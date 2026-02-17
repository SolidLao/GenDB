/*
 * TPC-H Q3: Shipping Priority Query (Iteration 2 - Load Pre-built Indexes, Lock-Free Aggregation)
 *
 * LOGICAL PLAN:
 * 1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows (20% selectivity)
 * 2. Join customer with orders via o_custkey + filter o_orderdate < '1995-03-15'
 * 3. Join with lineitem via l_orderkey + filter l_shipdate > '1995-03-15'
 * 4. Aggregate: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
 * 5. Sort: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10
 *
 * PHYSICAL PLAN (ITERATION 2):
 * 1. Parallel scan+filter customer → build hash set on c_custkey (~300K)
 * 2. Load pre-built orders_custkey_hash index via mmap (ZERO BUILD TIME)
 * 3. Parallel orders scan with zone-map pruning:
 *    - Use pre-built index to find all orders matching filtered customers
 *    - Apply o_orderdate < DATE filter
 *    - Build hash table on qualifying orderkeys → {o_orderdate, o_shippriority}
 * 4. Parallel lineitem scan with zone-map pruning + lock-free partitioned aggregation:
 *    - Filter l_shipdate > DATE
 *    - Probe orders hash table
 *    - Hash-partition aggregation (thread ownership, NO LOCKS)
 * 5. Merge partitions + partial sort (Top-K=10)
 *
 * ITERATION 2 KEY CHANGES:
 * - Load pre-built orders_custkey_hash index instead of building from scratch
 * - Use hash-partitioned aggregation with thread ownership (eliminate lock contention)
 * - Tighter memory layout for orders info
 *
 * DATE ENCODING: epoch days from 1970-01-01 (stored as int32_t)
 * DECIMAL: scaled integers (l_extendedprice, l_discount scaled by 100)
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cstdio>
#include <mutex>
#include <omp.h>

// ============================================================================
// Date utilities
// ============================================================================

int32_t date_to_epoch_days(int year, int month, int day) {
    // Compute days since 1970-01-01
    int32_t days = 0;
    // Add days for complete years
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months
    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; m++) {
        days += month_days[m];
        if (m == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) days++;
    }
    days += (day - 1);
    return days;
}

void epoch_days_to_date(int32_t epoch_days, int& year, int& month, int& day) {
    int32_t days = epoch_days;
    year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }
    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    month = 1;
    while (month <= 12) {
        int days_in_month = month_days[month];
        if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) days_in_month = 29;
        if (days < days_in_month) break;
        days -= days_in_month;
        month++;
    }
    day = days + 1;
}

// ============================================================================
// Open-addressing hash table
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ============================================================================
// Composite hash for aggregation
// ============================================================================

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        size_t h = (size_t)k.l_orderkey * 0x9E3779B97F4A7C15ULL;
        h ^= (size_t)k.o_orderdate * 0x517CC1B727220A95ULL;
        h ^= (size_t)k.o_shippriority * 0x85EBCA6B0C137B91ULL;
        return h;
    }
};

template<typename V>
struct CompactHashTableComposite {
    struct Entry {
        AggKey key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    AggKeyHash hasher;

    CompactHashTableComposite(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(const AggKey& key) const {
        return hasher(key);
    }

    V* find_or_insert(const AggKey& key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value = 0;
        table[idx].occupied = true;
        return &table[idx].value;
    }

    template<typename Callback>
    void iterate(Callback callback) const {
        for (const auto& entry : table) {
            if (entry.occupied) callback(entry.key, entry.value);
        }
    }
};

// ============================================================================
// Main Query Execution
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    const int32_t DATE_1995_03_15 = date_to_epoch_days(1995, 3, 15);

    // ------------------------------------------------------------------------
    // 1. Load all data columns and indexes
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string cust_dir = gendb_dir + "/customer/";
    std::string orders_dir = gendb_dir + "/orders/";
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string index_dir = gendb_dir + "/indexes/";

    // Customer columns
    int fd_c_custkey = open((cust_dir + "c_custkey.bin").c_str(), O_RDONLY);
    struct stat st_c_custkey; fstat(fd_c_custkey, &st_c_custkey);
    auto* c_custkey = (const int32_t*)mmap(nullptr, st_c_custkey.st_size, PROT_READ, MAP_PRIVATE, fd_c_custkey, 0);
    size_t c_count = st_c_custkey.st_size / sizeof(int32_t);
    close(fd_c_custkey);

    int fd_c_mktsegment = open((cust_dir + "c_mktsegment.bin").c_str(), O_RDONLY);
    struct stat st_c_mktsegment; fstat(fd_c_mktsegment, &st_c_mktsegment);
    auto* c_mktsegment = (const int32_t*)mmap(nullptr, st_c_mktsegment.st_size, PROT_READ, MAP_PRIVATE, fd_c_mktsegment, 0);
    close(fd_c_mktsegment);

    // Load c_mktsegment dictionary
    std::vector<std::string> c_mktsegment_dict;
    std::ifstream dict_file(cust_dir + "c_mktsegment_dict.txt");
    std::string line;
    while (std::getline(dict_file, line)) {
        c_mktsegment_dict.push_back(line);
    }
    dict_file.close();

    // Find 'BUILDING' code
    int32_t building_code = -1;
    for (size_t i = 0; i < c_mktsegment_dict.size(); i++) {
        if (c_mktsegment_dict[i] == "BUILDING") {
            building_code = (int32_t)i;
            break;
        }
    }

    // Orders columns
    int fd_o_orderkey = open((orders_dir + "o_orderkey.bin").c_str(), O_RDONLY);
    struct stat st_o_orderkey; fstat(fd_o_orderkey, &st_o_orderkey);
    auto* o_orderkey = (const int32_t*)mmap(nullptr, st_o_orderkey.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderkey, 0);
    size_t o_count = st_o_orderkey.st_size / sizeof(int32_t);
    close(fd_o_orderkey);

    // o_custkey not needed - using pre-built index instead

    int fd_o_orderdate = open((orders_dir + "o_orderdate.bin").c_str(), O_RDONLY);
    struct stat st_o_orderdate; fstat(fd_o_orderdate, &st_o_orderdate);
    auto* o_orderdate = (const int32_t*)mmap(nullptr, st_o_orderdate.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderdate, 0);
    close(fd_o_orderdate);

    int fd_o_shippriority = open((orders_dir + "o_shippriority.bin").c_str(), O_RDONLY);
    struct stat st_o_shippriority; fstat(fd_o_shippriority, &st_o_shippriority);
    auto* o_shippriority = (const int32_t*)mmap(nullptr, st_o_shippriority.st_size, PROT_READ, MAP_PRIVATE, fd_o_shippriority, 0);
    close(fd_o_shippriority);

    // Orders zone map not needed - using pre-built index for customer join

    // Load pre-built orders_custkey_hash index (hash_multi_value)
    // Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot
    int fd_orders_idx = open((index_dir + "orders_custkey_hash.bin").c_str(), O_RDONLY);
    struct stat st_orders_idx; fstat(fd_orders_idx, &st_orders_idx);
    auto* orders_idx_data = (const char*)mmap(nullptr, st_orders_idx.st_size, PROT_READ, MAP_PRIVATE, fd_orders_idx, 0);
    uint32_t orders_idx_table_size = *(const uint32_t*)(orders_idx_data + 4);

    struct OrdersCustkeyEntry { int32_t key; uint32_t offset; uint32_t count; };
    auto* orders_idx_table = (const OrdersCustkeyEntry*)(orders_idx_data + 8);
    auto* orders_idx_positions = (const uint32_t*)(orders_idx_data + 8 + orders_idx_table_size * 12);
    close(fd_orders_idx);

    // Lineitem columns
    int fd_l_orderkey = open((lineitem_dir + "l_orderkey.bin").c_str(), O_RDONLY);
    struct stat st_l_orderkey; fstat(fd_l_orderkey, &st_l_orderkey);
    auto* l_orderkey = (const int32_t*)mmap(nullptr, st_l_orderkey.st_size, PROT_READ, MAP_PRIVATE, fd_l_orderkey, 0);
    size_t l_count = st_l_orderkey.st_size / sizeof(int32_t);
    close(fd_l_orderkey);

    int fd_l_extendedprice = open((lineitem_dir + "l_extendedprice.bin").c_str(), O_RDONLY);
    struct stat st_l_extendedprice; fstat(fd_l_extendedprice, &st_l_extendedprice);
    auto* l_extendedprice = (const int64_t*)mmap(nullptr, st_l_extendedprice.st_size, PROT_READ, MAP_PRIVATE, fd_l_extendedprice, 0);
    close(fd_l_extendedprice);

    int fd_l_discount = open((lineitem_dir + "l_discount.bin").c_str(), O_RDONLY);
    struct stat st_l_discount; fstat(fd_l_discount, &st_l_discount);
    auto* l_discount = (const int64_t*)mmap(nullptr, st_l_discount.st_size, PROT_READ, MAP_PRIVATE, fd_l_discount, 0);
    close(fd_l_discount);

    int fd_l_shipdate = open((lineitem_dir + "l_shipdate.bin").c_str(), O_RDONLY);
    struct stat st_l_shipdate; fstat(fd_l_shipdate, &st_l_shipdate);
    auto* l_shipdate = (const int32_t*)mmap(nullptr, st_l_shipdate.st_size, PROT_READ, MAP_PRIVATE, fd_l_shipdate, 0);
    close(fd_l_shipdate);

    // Lineitem zone map
    int fd_lineitem_zone = open((index_dir + "lineitem_shipdate_zone.bin").c_str(), O_RDONLY);
    struct stat st_lineitem_zone; fstat(fd_lineitem_zone, &st_lineitem_zone);
    auto* lineitem_zone_data = (const char*)mmap(nullptr, st_lineitem_zone.st_size, PROT_READ, MAP_PRIVATE, fd_lineitem_zone, 0);
    uint32_t lineitem_num_zones = *(const uint32_t*)lineitem_zone_data;
    auto* lineitem_zones = (const int32_t*)(lineitem_zone_data + 4);
    close(fd_lineitem_zone);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ------------------------------------------------------------------------
    // 2. Parallel filter customer & build hash table
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    CompactHashTable<int32_t, bool> customer_ht(500000);
    std::atomic<size_t> cust_filtered{0};
    std::mutex ht_mutex;

    const int num_threads = omp_get_max_threads();

    #pragma omp parallel
    {
        std::vector<int32_t> local_custkeys;
        local_custkeys.reserve(c_count / num_threads + 1000);

        #pragma omp for nowait
        for (size_t i = 0; i < c_count; i++) {
            if (c_mktsegment[i] == building_code) {
                local_custkeys.push_back(c_custkey[i]);
            }
        }

        // Merge into global hash table
        std::lock_guard<std::mutex> lock(ht_mutex);
        for (auto key : local_custkeys) {
            customer_ht.insert(key, true);
        }
        cust_filtered.fetch_add(local_custkeys.size(), std::memory_order_relaxed);
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] scan_filter_customer: %.2f ms (%zu filtered)\n", cust_ms, cust_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 3. Use pre-built index to find orders matching filtered customers + date filter
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };
    CompactHashTable<int32_t, OrderInfo> orders_ht(2000000);
    std::atomic<size_t> orders_filtered{0};

    // Helper to probe pre-built hash index
    auto find_in_orders_idx = [&](int32_t custkey) -> const OrdersCustkeyEntry* {
        size_t hash = (size_t)custkey * 0x9E3779B97F4A7C15ULL;
        size_t idx = hash % orders_idx_table_size;
        while (orders_idx_table[idx].count > 0 || orders_idx_table[idx].offset > 0) {
            if (orders_idx_table[idx].key == custkey && orders_idx_table[idx].count > 0) {
                return &orders_idx_table[idx];
            }
            idx = (idx + 1) % orders_idx_table_size;
        }
        return nullptr;
    };

    #pragma omp parallel
    {
        std::vector<std::pair<int32_t, OrderInfo>> local_orders;
        local_orders.reserve(o_count / num_threads + 1000);

        // Iterate through filtered customers and use index to find their orders
        #pragma omp for nowait
        for (size_t slot = 0; slot < customer_ht.table.size(); slot++) {
            if (!customer_ht.table[slot].occupied) continue;

            int32_t custkey = customer_ht.table[slot].key;
            auto* idx_entry = find_in_orders_idx(custkey);
            if (!idx_entry) continue;

            // Iterate through all orders for this customer
            for (uint32_t j = 0; j < idx_entry->count; j++) {
                uint32_t order_pos = orders_idx_positions[idx_entry->offset + j];

                // Apply date filter
                if (o_orderdate[order_pos] < DATE_1995_03_15) {
                    local_orders.push_back({
                        o_orderkey[order_pos],
                        {o_orderdate[order_pos], o_shippriority[order_pos]}
                    });
                }
            }
        }

        // Merge into global hash table
        std::lock_guard<std::mutex> lock(ht_mutex);
        for (const auto& entry : local_orders) {
            orders_ht.insert(entry.first, entry.second);
        }
        orders_filtered.fetch_add(local_orders.size(), std::memory_order_relaxed);
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_join_orders: %.2f ms (%zu filtered)\n", orders_ms, orders_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 4. Parallel lineitem scan + join + aggregate (lock-free partitioned aggregation)
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    // Lock-free aggregation: each thread owns a partition
    const int NUM_AGG_THREADS = num_threads;
    std::vector<CompactHashTableComposite<int64_t>> thread_agg(NUM_AGG_THREADS, CompactHashTableComposite<int64_t>(50000));
    std::atomic<size_t> lineitem_matches{0};

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        auto& my_agg = thread_agg[thread_id];

        #pragma omp for schedule(dynamic, 1)
        for (uint32_t z = 0; z < lineitem_num_zones; z++) {
            int32_t zone_max = lineitem_zones[z * 2 + 1];

            // Skip zone if all values <= DATE_1995_03_15
            if (zone_max <= DATE_1995_03_15) continue;

            uint32_t start_row = z * 100000;
            uint32_t end_row = std::min((uint32_t)(z + 1) * 100000, (uint32_t)l_count);

            size_t local_matches = 0;

            for (uint32_t i = start_row; i < end_row; i++) {
                if (l_shipdate[i] > DATE_1995_03_15) {
                    auto* order_info = orders_ht.find(l_orderkey[i]);
                    if (order_info) {
                        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);
                        AggKey key{l_orderkey[i], order_info->o_orderdate, order_info->o_shippriority};

                        // Aggregate directly into thread-local hash table (no locks!)
                        *my_agg.find_or_insert(key) += revenue;
                        local_matches++;
                    }
                }
            }

            lineitem_matches.fetch_add(local_matches, std::memory_order_relaxed);
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (%zu matches)\n", lineitem_ms, lineitem_matches.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Merge thread-local aggregation results and partial sort (Top-10)
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct Result {
        int32_t l_orderkey;
        int64_t revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Merge thread-local hash tables into final result
    CompactHashTableComposite<int64_t> final_agg(lineitem_matches.load());

    for (int t = 0; t < NUM_AGG_THREADS; t++) {
        thread_agg[t].iterate([&](const AggKey& key, int64_t revenue) {
            *final_agg.find_or_insert(key) += revenue;
        });
    }

    // Collect final results
    std::vector<Result> results;
    results.reserve(lineitem_matches.load());

    final_agg.iterate([&](const AggKey& key, int64_t revenue) {
        results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    });

    // Partial sort for Top-10
    size_t topk = std::min((size_t)10, results.size());
    std::partial_sort(results.begin(), results.begin() + topk, results.end(),
        [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // ------------------------------------------------------------------------
    // 6. Write output
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q3.csv");
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (size_t i = 0; i < topk; i++) {
        int year, month, day;
        epoch_days_to_date(results[i].o_orderdate, year, month, day);

        // Revenue is scaled by 10000 (100 * 100)
        double revenue_val = results[i].revenue / 10000.0;

        char date_buf[16];
        snprintf(date_buf, sizeof(date_buf), "%04d-%02d-%02d", year, month, day);

        out << results[i].l_orderkey << ","
            << std::fixed << std::setprecision(2) << revenue_val << ","
            << date_buf << ","
            << results[i].o_shippriority << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
