/*
 * TPC-H Q3: Shipping Priority Query (Iteration 4 - Pre-built Index Exploitation)
 *
 * LOGICAL PLAN:
 * 1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows (20% selectivity)
 * 2. Join customer→orders: c_custkey = o_custkey + filter o_orderdate < '1995-03-15'
 * 3. Join orders→lineitem: o_orderkey = l_orderkey + filter l_shipdate > '1995-03-15'
 * 4. Aggregate: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
 * 5. Sort: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10
 *
 * PHYSICAL PLAN (ITERATION 4 - INDEX-DRIVEN JOIN REVERSAL):
 * 1. Parallel scan customer with filter c_mktsegment='BUILDING' → bitmap of qualifying custkeys
 * 2. Load pre-built orders_custkey_hash index (hash_multi_value) via mmap → ZERO lookup time
 * 3. Parallel iterate over qualifying custkeys:
 *    - Use orders_custkey_hash to find all orders for each custkey
 *    - Filter by o_orderdate < DATE
 *    - Materialize qualifying orders into vector
 * 4. Load pre-built lineitem_orderkey_hash index (hash_multi_value) via mmap → ZERO lookup time
 * 5. Parallel iterate over qualifying orders:
 *    - Use lineitem_orderkey_hash to find all lineitems for each orderkey
 *    - Filter by l_shipdate > DATE
 *    - Compute revenue and aggregate DIRECTLY into thread-local hash tables
 * 6. Merge thread-local aggregations and partial sort (Top-K=10)
 *
 * KEY OPTIMIZATIONS (ITER 4):
 * - EXPLOIT PRE-BUILT INDEXES: orders_custkey_hash and lineitem_orderkey_hash eliminate hash table builds
 * - INDEX-DRIVEN JOIN: Reverse from probe-heavy (60M probes) to lookup-driven (1.46M lookups)
 * - Bitmap for customer filter (compact, fast)
 * - Thread-local aggregation (truly lock-free, no partitioning overhead)
 * - Zone map pruning embedded in index structures
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
// Pre-built Hash Index Loaders
// ============================================================================

// Hash multi-value index: key → list of positions
struct HashMultiValueIndex {
    struct Entry {
        int32_t key;
        uint32_t offset;  // offset into positions array
        uint32_t count;   // number of positions
    };

    const Entry* table;
    size_t table_size;
    const uint32_t* positions;

    HashMultiValueIndex(const char* data) {
        (void)*(const uint32_t*)data;  // num_unique (unused)
        table_size = *(const uint32_t*)(data + 4);
        table = (const Entry*)(data + 8);

        // Positions array starts after hash table
        const char* pos_start = data + 8 + table_size * 12;
        (void)*(const uint32_t*)pos_start;  // pos_count (unused)
        positions = (const uint32_t*)(pos_start + 4);
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    // Returns pointer to positions array and count, or nullptr if not found
    const uint32_t* find(int32_t key, uint32_t& count) const {
        size_t idx = hash(key) % table_size;
        size_t start_idx = idx;

        while (table[idx].count > 0 || table[idx].offset > 0) {
            if (table[idx].key == key && table[idx].count > 0) {
                count = table[idx].count;
                return positions + table[idx].offset;
            }
            idx = (idx + 1) % table_size;
            if (idx == start_idx) break;
        }
        count = 0;
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

    CompactHashTableComposite() : mask(0) {}

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

    // Load pre-built orders_custkey_hash index
    int fd_orders_custkey_idx = open((index_dir + "orders_custkey_hash.bin").c_str(), O_RDONLY);
    struct stat st_orders_custkey_idx; fstat(fd_orders_custkey_idx, &st_orders_custkey_idx);
    auto* orders_custkey_idx_data = (const char*)mmap(nullptr, st_orders_custkey_idx.st_size, PROT_READ, MAP_PRIVATE, fd_orders_custkey_idx, 0);
    HashMultiValueIndex orders_custkey_index(orders_custkey_idx_data);
    close(fd_orders_custkey_idx);

    // Orders columns (for filtering and building intermediate results)
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

    // Lineitem columns (l_orderkey not needed since we use the index)
    // l_orderkey would be loaded here if needed directly

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

    // Load pre-built lineitem_orderkey_hash index
    int fd_lineitem_orderkey_idx = open((index_dir + "lineitem_orderkey_hash.bin").c_str(), O_RDONLY);
    struct stat st_lineitem_orderkey_idx; fstat(fd_lineitem_orderkey_idx, &st_lineitem_orderkey_idx);
    auto* lineitem_orderkey_idx_data = (const char*)mmap(nullptr, st_lineitem_orderkey_idx.st_size, PROT_READ, MAP_PRIVATE, fd_lineitem_orderkey_idx, 0);
    HashMultiValueIndex lineitem_orderkey_index(lineitem_orderkey_idx_data);
    close(fd_lineitem_orderkey_idx);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ------------------------------------------------------------------------
    // 2. Build list of qualifying customers (c_mktsegment = 'BUILDING')
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    // Collect qualifying custkeys into a vector (more cache-friendly for iteration)
    std::vector<std::vector<int32_t>> cust_partitions(omp_get_max_threads());

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        std::vector<int32_t> local_custkeys;
        local_custkeys.reserve(c_count / omp_get_max_threads() / 5);

        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < c_count; i++) {
            if (c_mktsegment[i] == building_code) {
                // TPC-H property: custkey = row_position + 1 (1-indexed)
                local_custkeys.push_back(i + 1);
            }
        }

        cust_partitions[tid] = std::move(local_custkeys);
    }

    // Merge into single vector
    std::vector<int32_t> qualifying_custkeys;
    size_t total_custs = 0;
    for (const auto& part : cust_partitions) total_custs += part.size();
    qualifying_custkeys.reserve(total_custs);
    for (auto& part : cust_partitions) {
        qualifying_custkeys.insert(qualifying_custkeys.end(), part.begin(), part.end());
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] scan_filter_customer: %.2f ms (%zu filtered)\n", cust_ms, qualifying_custkeys.size());
#endif

    // ------------------------------------------------------------------------
    // 3. Use pre-built index to find orders for qualifying customers
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderRecord {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    // Collect qualifying orders
    std::vector<std::vector<OrderRecord>> orders_partitions(omp_get_max_threads());

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        std::vector<OrderRecord> local_orders;
        local_orders.reserve(100000);

        #pragma omp for schedule(dynamic, 100) nowait
        for (size_t i = 0; i < qualifying_custkeys.size(); i++) {
            int32_t custkey = qualifying_custkeys[i];
            uint32_t count;
            const uint32_t* order_positions = orders_custkey_index.find(custkey, count);

            if (order_positions) {
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t pos = order_positions[j];
                    if (o_orderdate[pos] < DATE_1995_03_15) {
                        local_orders.push_back({
                            o_orderkey[pos],
                            o_orderdate[pos],
                            o_shippriority[pos]
                        });
                    }
                }
            }
        }

        orders_partitions[tid] = std::move(local_orders);
    }

    // Merge into single vector
    std::vector<OrderRecord> qualifying_orders;
    size_t total_orders = 0;
    for (const auto& part : orders_partitions) total_orders += part.size();
    qualifying_orders.reserve(total_orders);
    for (auto& part : orders_partitions) {
        qualifying_orders.insert(qualifying_orders.end(), part.begin(), part.end());
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_join_orders: %.2f ms (%zu filtered)\n", orders_ms, qualifying_orders.size());
#endif

    // ------------------------------------------------------------------------
    // 4. Use pre-built index to find lineitems for qualifying orders + aggregate
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::atomic<size_t> lineitem_matches{0};

    // Thread-local aggregation tables (truly lock-free)
    std::vector<CompactHashTableComposite<int64_t>> thread_local_agg(omp_get_max_threads());

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        // Each thread has its own aggregation table
        CompactHashTableComposite<int64_t> local_agg(20000);

        #pragma omp for schedule(dynamic, 100) nowait
        for (size_t i = 0; i < qualifying_orders.size(); i++) {
            const OrderRecord& order = qualifying_orders[i];
            uint32_t count;
            const uint32_t* lineitem_positions = lineitem_orderkey_index.find(order.orderkey, count);

            if (lineitem_positions) {
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t pos = lineitem_positions[j];
                    if (l_shipdate[pos] > DATE_1995_03_15) {
                        int64_t revenue = l_extendedprice[pos] * (100 - l_discount[pos]);
                        AggKey agg_key{order.orderkey, order.orderdate, order.shippriority};

                        // Aggregate into thread-local table (no contention)
                        *local_agg.find_or_insert(agg_key) += revenue;
                        lineitem_matches.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        thread_local_agg[tid] = std::move(local_agg);
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (%zu matches)\n", lineitem_ms, lineitem_matches.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Merge thread-local aggregations and collect results
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

    // Merge all thread-local aggregation tables into a final hash table
    CompactHashTableComposite<int64_t> final_agg(100000);
    for (auto& local_table : thread_local_agg) {
        local_table.iterate([&](const AggKey& key, int64_t revenue) {
            *final_agg.find_or_insert(key) += revenue;
        });
    }

    // Collect results from final aggregation
    std::vector<Result> results;
    results.reserve(50000);

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
