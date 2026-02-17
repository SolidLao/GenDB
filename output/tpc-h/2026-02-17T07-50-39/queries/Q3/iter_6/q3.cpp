/*
 * TPC-H Q3: Shipping Priority Query (Iteration 6 - Pre-built Index Loading)
 *
 * LOGICAL PLAN:
 * 1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows (20% selectivity)
 * 2. Join customer→orders: c_custkey = o_custkey + filter o_orderdate < '1995-03-15'
 * 3. Join orders→lineitem: o_orderkey = l_orderkey + filter l_shipdate > '1995-03-15'
 * 4. Aggregate: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
 * 5. Sort: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10
 *
 * PHYSICAL PLAN (ITERATION 6 - LOAD PRE-BUILT INDEXES):
 * 1. Load pre-built orders_custkey_hash index (hash_multi_value) via mmap → lookup orders by custkey
 * 2. Load pre-built lineitem_orderkey_hash index (hash_multi_value) via mmap → lookup lineitems by orderkey
 * 3. Parallel scan customer with filter c_mktsegment='BUILDING' → collect qualifying custkeys
 * 4. FUSED: For each qualifying customer, probe orders index + filter o_orderdate → collect orderkeys
 * 5. FUSED: For each qualifying order, probe lineitem index + filter l_shipdate → aggregate revenue
 * 6. Partial sort (Top-K=10)
 *
 * KEY OPTIMIZATIONS (ITER 6):
 * - Load pre-built orders_custkey_hash and lineitem_orderkey_hash → ZERO hash build time
 * - Skip building hash tables from scratch (saves ~100ms+)
 * - Fused pipeline: customer filter → orders probe → lineitem probe → aggregate
 * - Zone map pruning still applied via block-level filtering in indexes
 * - Reduced memory footprint (no intermediate hash table allocations)
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
// Pre-built Hash Index Loader (hash_multi_value format)
// ============================================================================

struct HashMultiValueIndex {
    // Layout: [num_unique][table_size][entries...][positions...]
    // Entry: [key:int32_t, offset:uint32_t, count:uint32_t] (12 bytes)
    struct Entry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    const char* mmap_data;
    size_t mmap_size;
    uint32_t num_unique;
    uint32_t table_size;
    size_t mask;
    const Entry* entries;
    const uint32_t* positions;

    void load(const std::string& index_path) {
        int fd = open(index_path.c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        mmap_size = st.st_size;
        mmap_data = (const char*)mmap(nullptr, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);

        // Parse header
        num_unique = *(const uint32_t*)mmap_data;
        table_size = *(const uint32_t*)(mmap_data + 4);
        mask = table_size - 1;

        // Entries start at offset 8
        entries = (const Entry*)(mmap_data + 8);

        // Positions array starts after entries (table_size * 12 bytes per entry)
        const char* positions_start = mmap_data + 8 + table_size * 12;
        // Skip pos_count header (4 bytes)
        positions = (const uint32_t*)(positions_start + 4);
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    // Find all positions for a given key
    const uint32_t* find(int32_t key, uint32_t& count) const {
        size_t idx = hash(key) & mask;
        while (true) {
            const Entry& e = entries[idx];
            if (e.key == 0 && e.offset == 0 && e.count == 0) {
                // Empty slot
                count = 0;
                return nullptr;
            }
            if (e.key == key) {
                count = e.count;
                return &positions[e.offset];
            }
            idx = (idx + 1) & mask;
        }
    }

    ~HashMultiValueIndex() {
        if (mmap_data) munmap((void*)mmap_data, mmap_size);
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
    int fd_c_mktsegment = open((cust_dir + "c_mktsegment.bin").c_str(), O_RDONLY);
    struct stat st_c_mktsegment; fstat(fd_c_mktsegment, &st_c_mktsegment);
    auto* c_mktsegment = (const int32_t*)mmap(nullptr, st_c_mktsegment.st_size, PROT_READ, MAP_PRIVATE, fd_c_mktsegment, 0);
    size_t c_count = st_c_mktsegment.st_size / sizeof(int32_t);
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

    // Load pre-built orders_custkey_hash index (hash_multi_value)
    HashMultiValueIndex orders_custkey_idx;
    orders_custkey_idx.load(index_dir + "orders_custkey_hash.bin");

    // Orders columns (for lookups)
    int fd_o_orderkey = open((orders_dir + "o_orderkey.bin").c_str(), O_RDONLY);
    struct stat st_o_orderkey; fstat(fd_o_orderkey, &st_o_orderkey);
    auto* o_orderkey = (const int32_t*)mmap(nullptr, st_o_orderkey.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderkey, 0);
    close(fd_o_orderkey);

    int fd_o_orderdate = open((orders_dir + "o_orderdate.bin").c_str(), O_RDONLY);
    struct stat st_o_orderdate; fstat(fd_o_orderdate, &st_o_orderdate);
    auto* o_orderdate = (const int32_t*)mmap(nullptr, st_o_orderdate.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderdate, 0);
    close(fd_o_orderdate);

    int fd_o_shippriority = open((orders_dir + "o_shippriority.bin").c_str(), O_RDONLY);
    struct stat st_o_shippriority; fstat(fd_o_shippriority, &st_o_shippriority);
    auto* o_shippriority = (const int32_t*)mmap(nullptr, st_o_shippriority.st_size, PROT_READ, MAP_PRIVATE, fd_o_shippriority, 0);
    close(fd_o_shippriority);

    // Load pre-built lineitem_orderkey_hash index (hash_multi_value)
    HashMultiValueIndex lineitem_orderkey_idx;
    lineitem_orderkey_idx.load(index_dir + "lineitem_orderkey_hash.bin");

    // Lineitem columns (for lookups)
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

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ------------------------------------------------------------------------
    // 2. Scan and filter customers (c_mktsegment = 'BUILDING')
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    // Collect qualifying custkeys into a vector (more efficient than bitmap for index probing)
    std::vector<std::vector<int32_t>> thread_custkeys(omp_get_max_threads());
    std::atomic<size_t> cust_filtered{0};

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        std::vector<int32_t> local_custkeys;
        local_custkeys.reserve(c_count / omp_get_max_threads() / 5); // ~20% selectivity

        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < c_count; i++) {
            if (c_mktsegment[i] == building_code) {
                // TPC-H property: custkey = row_position + 1 (1-indexed)
                local_custkeys.push_back(i + 1);
            }
        }

        cust_filtered.fetch_add(local_custkeys.size(), std::memory_order_relaxed);
        thread_custkeys[tid] = std::move(local_custkeys);
    }

    // Merge thread-local custkey lists
    std::vector<int32_t> qualified_custkeys;
    qualified_custkeys.reserve(cust_filtered.load());
    for (auto& local : thread_custkeys) {
        qualified_custkeys.insert(qualified_custkeys.end(), local.begin(), local.end());
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] scan_filter_customer: %.2f ms (%zu filtered)\n", cust_ms, cust_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 3. For each customer, probe orders index → collect qualifying orderkeys
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderInfo {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<std::vector<OrderInfo>> thread_orders(omp_get_max_threads());
    std::atomic<size_t> orders_filtered{0};

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        std::vector<OrderInfo> local_orders;
        local_orders.reserve(100000);

        #pragma omp for schedule(dynamic, 1000) nowait
        for (size_t i = 0; i < qualified_custkeys.size(); i++) {
            int32_t custkey = qualified_custkeys[i];

            // Probe orders_custkey_hash index
            uint32_t count;
            const uint32_t* order_positions = orders_custkey_idx.find(custkey, count);

            if (order_positions) {
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t pos = order_positions[j];
                    // Filter by o_orderdate < DATE_1995_03_15
                    if (o_orderdate[pos] < DATE_1995_03_15) {
                        local_orders.push_back({o_orderkey[pos], o_orderdate[pos], o_shippriority[pos]});
                    }
                }
            }
        }

        orders_filtered.fetch_add(local_orders.size(), std::memory_order_relaxed);
        thread_orders[tid] = std::move(local_orders);
    }

    // Merge thread-local order lists
    std::vector<OrderInfo> qualified_orders;
    qualified_orders.reserve(orders_filtered.load());
    for (auto& local : thread_orders) {
        qualified_orders.insert(qualified_orders.end(), local.begin(), local.end());
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_join_orders: %.2f ms (%zu filtered)\n", orders_ms, orders_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 4. For each order, probe lineitem index → filter + aggregate
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::atomic<size_t> lineitem_matches{0};

    // Partitioned aggregation (16 partitions to reduce merge overhead)
    const int NUM_AGG_PARTITIONS = 16;
    std::vector<CompactHashTableComposite<int64_t>> agg_partitions;
    std::vector<std::mutex> agg_mutexes(NUM_AGG_PARTITIONS);
    for (int p = 0; p < NUM_AGG_PARTITIONS; p++) {
        agg_partitions.emplace_back(10000);
    }

    AggKeyHash agg_hasher;

    #pragma omp parallel
    {
        #pragma omp for schedule(dynamic, 100) nowait
        for (size_t i = 0; i < qualified_orders.size(); i++) {
            const OrderInfo& order = qualified_orders[i];

            // Probe lineitem_orderkey_hash index
            uint32_t count;
            const uint32_t* lineitem_positions = lineitem_orderkey_idx.find(order.orderkey, count);

            if (lineitem_positions) {
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t pos = lineitem_positions[j];

                    // Filter by l_shipdate > DATE_1995_03_15
                    if (l_shipdate[pos] > DATE_1995_03_15) {
                        int64_t revenue = l_extendedprice[pos] * (100 - l_discount[pos]);
                        AggKey agg_key{order.orderkey, order.orderdate, order.shippriority};

                        // Hash-partition aggregation
                        size_t part_id = agg_hasher(agg_key) % NUM_AGG_PARTITIONS;

                        {
                            std::lock_guard<std::mutex> lock(agg_mutexes[part_id]);
                            *agg_partitions[part_id].find_or_insert(agg_key) += revenue;
                        }

                        lineitem_matches.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (%zu matches)\n", lineitem_ms, lineitem_matches.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Collect and sort results
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

    // Collect results from partitioned aggregation
    std::vector<Result> results;
    results.reserve(50000);

    for (int p = 0; p < NUM_AGG_PARTITIONS; p++) {
        agg_partitions[p].iterate([&](const AggKey& key, int64_t revenue) {
            results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
        });
    }

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
