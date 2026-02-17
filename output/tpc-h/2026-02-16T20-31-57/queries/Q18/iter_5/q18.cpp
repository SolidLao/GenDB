#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <omp.h>
#include <atomic>

/*
================================================================================
LOGICAL PLAN FOR Q18
================================================================================

Query: SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
              SUM(l_quantity) AS sum_qty
       FROM customer, orders, lineitem
       WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem
                            GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
             AND c_custkey = o_custkey
             AND o_orderkey = l_orderkey
       GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
       ORDER BY o_totalprice DESC, o_orderdate
       LIMIT 100

Step 1 (Subquery Decorrelation):
  - Subquery: SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
  - Cardinality: Extremely selective. lineitem has 59M rows. Grouping by l_orderkey gives 15M groups.
    After HAVING SUM(l_quantity) > 300 (scale factor 2, so > 600 in raw), very few orders qualify
    (estimated <5% = ~750K orderkeys)
  - Action: Pre-compute into hash set of qualifying l_orderkey values (semi-join reduction)

Step 2 (Filter orders):
  - Apply filter: o_orderkey IN qualifying_set (semi-join)
  - Input: 15M rows, Output: ~750K rows (~5% selectivity)

Step 3 (Filter lineitem):
  - Apply filter: l_orderkey IN qualifying_set (same semi-join set)
  - Input: 59M rows, Output: ~750K * avg_lines_per_order rows (~4-5M)
  - This is our build side (smaller than filtered orders initially, but we'll compute exact)

Step 4 (Join Strategy):
  - Build hash table on filtered orders (750K rows) keyed by (o_orderkey, o_custkey)
    - Why? Semi-join reduces both orders and lineitem. lineitem has more rows per order,
      so we build on orders and probe with lineitem
  - Probe with filtered lineitem, emit matching (o_custkey, l_orderkey, l_quantity) tuples

Step 5 (Join with customer):
  - Build hash table on filtered customer (1.5M rows) keyed by c_custkey
  - Probe with lineitem-orders intermediate result
  - Emit (c_custkey, c_name, o_orderkey, o_orderdate, o_totalprice, l_quantity)

Step 6 (Final Aggregation):
  - GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
  - Aggregate: SUM(l_quantity)
  - Cardinality: At most the intermediate result (~750K * avg_lines), but after grouping
    on both customer and order, roughly 750K unique groups (one per qualifying order)

Step 7 (Sort and Limit):
  - Sort by o_totalprice DESC, o_orderdate ASC
  - LIMIT 100

================================================================================
PHYSICAL PLAN FOR Q18
================================================================================

1. Subquery (Lineitem Aggregation):
   - Scan: Full scan of lineitem (59M rows)
   - Aggregate: GROUP BY l_orderkey, SUM(l_quantity)
   - Data structure: Open-addressing hash table (15M distinct groups expected, pre-size to 20M)
   - Filter: HAVING SUM(l_quantity) > 600 (threshold = 300 * scale_factor 2)
   - Parallelism: OpenMP parallel scan with thread-local aggregation buffers, then merge

2. Orders Join (Semi-join + Hash Join with Customer):
   - Scan: Full scan of orders (15M rows)
   - Apply semi-join filter: o_orderkey IN qualifying_orderkeys
   - Output: ~750K rows
   - Index: Use orders_custkey_hash multi-value index to speed up customer join later
   - Parallelism: Single scan, no parallelism needed (filtered down)

3. Lineitem Join:
   - Scan: Full scan of lineitem, filter l_orderkey IN qualifying_set
   - Build hash table: keyed by l_orderkey on filtered orders (750K rows)
   - Probe: emit (l_orderkey, l_quantity, o_custkey, o_orderdate, o_totalprice)
   - Parallelism: Parallel probe phase with thread-local buffers

4. Customer Join:
   - Build hash table on customer (1.5M rows) keyed by c_custkey (direct mmap of c_custkey.bin + c_name.bin)
   - Probe: with lineitem-orders intermediate
   - Emit: (c_custkey, c_name, o_orderkey, o_orderdate, o_totalprice, l_quantity)
   - Parallelism: Single pass (already filtered)

5. Final Aggregation:
   - GROUP BY (c_custkey, c_name, o_orderkey, o_orderdate, o_totalprice)
   - Aggregate: SUM(l_quantity)
   - Data structure: Open-addressing hash table (pre-size to 1M)
   - Composite key hash: (custkey, orderkey) tuple

6. Sort and Output:
   - Partial sort: TOP-100 by o_totalprice DESC, o_orderdate ASC
   - Algorithm: std::partial_sort on the final aggregation result
   - Output: CSV to results_dir/Q18.csv

================================================================================
IMPLEMENTATION NOTES
================================================================================

- DATE columns (o_orderdate): int32_t epoch days. No conversion needed for comparisons.
  Convert to YYYY-MM-DD only for CSV output (epoch day D → date calculation)
- DECIMAL columns (o_totalprice, l_quantity): int64_t scaled by 2
  - For HAVING: compare SUM(l_quantity) against 300 * 2 = 600 (raw value)
  - For output: format as value / 2 with 2 decimal places
- Dictionary encoding (c_name): Load from c_name_dict.txt at runtime, store int32_t codes
  - Decode only for output
- Hash joins and aggregations use open-addressing for cache efficiency
- Parallelism: OpenMP on large scans (>1M rows)

================================================================================
*/

// ============================================================================
// Helper: mmap file reading
// ============================================================================

void* mmap_file(const char* path, size_t& file_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    off_t size = lseek(fd, 0, SEEK_END);
    file_size = static_cast<size_t>(size);
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// ============================================================================
// Helper: Load dictionary file
// ============================================================================

std::unordered_map<std::string, int32_t> load_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    int32_t code = 0;
    while (std::getline(file, line)) {
        dict[line] = code++;
    }
    return dict;
}

// ============================================================================
// Helper: Reverse dictionary (code -> string)
// ============================================================================

std::vector<std::string> load_dict_reverse(const std::string& dict_path) {
    std::vector<std::string> rev;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return rev;
    }
    std::string line;
    while (std::getline(file, line)) {
        rev.push_back(line);
    }
    return rev;
}

// ============================================================================
// Helper: Convert epoch day to YYYY-MM-DD
// ============================================================================

std::string epoch_to_date(int32_t day) {
    // day 0 = 1970-01-01
    // day 10957 = 1999-12-31
    // Simple approach: iterate forward from 1970
    int year = 1970;
    int days_remaining = day;

    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_remaining < days_in_year) break;
        days_remaining -= days_in_year;
        year++;
    }

    // Now find month and day
    int month = 1;
    int day_in_year = days_remaining;
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[2] = 29;
    }

    for (month = 1; month <= 12; month++) {
        if (day_in_year < days_in_month[month]) {
            break;
        }
        day_in_year -= days_in_month[month];
    }

    int day_of_month = day_in_year + 1;  // 1-indexed

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month, day_of_month);
    return std::string(buf);
}

// ============================================================================
// Open-Addressing Hash Table for efficient aggregation
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;
    size_t num_entries;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
        num_entries = 0;
    }

    inline size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert_or_add(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
        num_entries++;
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    template<typename Func>
    void iterate(Func f) {
        for (auto& entry : table) {
            if (entry.occupied) f(entry.key, entry.value);
        }
    }
};

// ============================================================================
// Composite key for final aggregation
// ============================================================================

struct AggregateKey {
    int32_t custkey;
    int32_t orderkey;
    int32_t orderdate;
    int64_t totalprice;
    int32_t name_code;

    bool operator==(const AggregateKey& other) const {
        return custkey == other.custkey && orderkey == other.orderkey &&
               orderdate == other.orderdate && totalprice == other.totalprice &&
               name_code == other.name_code;
    }
};

inline size_t hash_aggregate_key(const AggregateKey& k) {
    // Combine all fields for a strong hash
    size_t h = 0;
    h ^= std::hash<int32_t>()(k.custkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<int32_t>()(k.orderkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<int32_t>()(k.orderdate) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<int64_t>()(k.totalprice) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<int32_t>()(k.name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
}

// Open-addressing hash table for composite keys
struct CompactHashTableComposite {
    struct Entry { AggregateKey key; int64_t value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;
    size_t num_entries;

    CompactHashTableComposite(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
        num_entries = 0;
    }

    void insert_or_add(const AggregateKey& key, int64_t value) {
        size_t idx = hash_aggregate_key(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
        num_entries++;
    }

    int64_t* find(const AggregateKey& key) {
        size_t idx = hash_aggregate_key(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    template<typename Func>
    void iterate(Func f) {
        for (auto& entry : table) {
            if (entry.occupied) f(entry.key, entry.value);
        }
    }
};

// ============================================================================
// Main Query Function
// ============================================================================

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load all data via mmap
    std::cout << "[LOAD] Mapping files..." << std::endl;

    size_t lineitem_size, orders_size, customer_size;
    size_t l_orderkey_size, l_quantity_size;
    size_t o_orderkey_size, o_custkey_size, o_orderdate_size, o_totalprice_size;
    size_t c_custkey_size, c_name_code_size;

    int32_t* l_orderkey = (int32_t*)mmap_file((gendb_dir + "/lineitem/l_orderkey.bin").c_str(), l_orderkey_size);
    int64_t* l_quantity = (int64_t*)mmap_file((gendb_dir + "/lineitem/l_quantity.bin").c_str(), l_quantity_size);

    int32_t* o_orderkey = (int32_t*)mmap_file((gendb_dir + "/orders/o_orderkey.bin").c_str(), o_orderkey_size);
    int32_t* o_custkey = (int32_t*)mmap_file((gendb_dir + "/orders/o_custkey.bin").c_str(), o_custkey_size);
    int32_t* o_orderdate = (int32_t*)mmap_file((gendb_dir + "/orders/o_orderdate.bin").c_str(), o_orderdate_size);
    int64_t* o_totalprice = (int64_t*)mmap_file((gendb_dir + "/orders/o_totalprice.bin").c_str(), o_totalprice_size);

    int32_t* c_custkey = (int32_t*)mmap_file((gendb_dir + "/customer/c_custkey.bin").c_str(), c_custkey_size);
    int32_t* c_name_code = (int32_t*)mmap_file((gendb_dir + "/customer/c_name.bin").c_str(), c_name_code_size);

    int64_t lineitem_rows = l_orderkey_size / sizeof(int32_t);
    int64_t orders_rows = o_orderkey_size / sizeof(int32_t);
    int64_t customer_rows = c_custkey_size / sizeof(int32_t);

    std::cout << "[LOAD] Lineitem rows: " << lineitem_rows << std::endl;
    std::cout << "[LOAD] Orders rows: " << orders_rows << std::endl;
    std::cout << "[LOAD] Customer rows: " << customer_rows << std::endl;

    // Load reverse dictionaries for decoding
    std::vector<std::string> c_name_dict = load_dict_reverse(gendb_dir + "/customer/c_name_dict.txt");
    std::cout << "[LOAD] Customer name dictionary loaded: " << c_name_dict.size() << " entries" << std::endl;

    // ========================================================================
    // STEP 1: Subquery - Compute qualifying orderkeys
    // GROUP BY l_orderkey HAVING SUM(l_quantity) > 300 (scaled: > 600)
    // Parallelized with thread-local aggregation buffers
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, int64_t>> thread_aggs;
    for (int t = 0; t < num_threads; t++) {
        thread_aggs.emplace_back(15000000 / num_threads + 1000);
    }

    // Parallel scan with thread-local aggregation (minimize contention)
    // Use restrict pointers to help compiler vectorization
    int32_t* __restrict l_orderkey_p = l_orderkey;
    int64_t* __restrict l_quantity_p = l_quantity;

    #pragma omp parallel for schedule(static, 100000)
    for (int64_t i = 0; i < lineitem_rows; i++) {
        int tid = omp_get_thread_num();
        int32_t key = l_orderkey_p[i];
        thread_aggs[tid].insert_or_add(key, l_quantity_p[i]);
    }

    // Merge phase: combine all thread-local aggregations
    CompactHashTable<int32_t, int64_t> lineitem_agg(15000000);
    for (int t = 0; t < num_threads; t++) {
        thread_aggs[t].iterate([&](int32_t key, int64_t value) {
            lineitem_agg.insert_or_add(key, value);
        });
    }

    // Filter by HAVING clause
    // Use hash set for O(1) qualification checks (binary search is too slow for 59M+ lookups)
    CompactHashTable<int32_t, bool> qualifying_set(1000000);
    int64_t qualifying_count = 0;
    lineitem_agg.iterate([&](int32_t key, int64_t agg_value) {
        // HAVING SUM(l_quantity) > 300
        // l_quantity is stored as value * 100 (2 decimal places)
        // So 300.00 = 30000, and we need SUM > 30000
        if (agg_value > 30000) {  // 300 * 100
            qualifying_set.insert_or_add(key, true);
            qualifying_count++;
        }
    });

    // Lambda for checking qualification (using hash set O(1) instead of binary search O(log N))
    auto is_qualifying = [&](int32_t key) {
        return qualifying_set.find(key) != nullptr;
    };

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery: %.2f ms\n", ms_subquery);
#endif

    std::cout << "[FILTER] Qualifying orderkeys: " << qualifying_count << " out of " << orders_rows << std::endl;

    // ========================================================================
    // STEP 2: Build hash table on filtered orders using open-addressing
    // Key: o_orderkey -> (custkey, orderdate, totalprice)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_build_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderData {
        int32_t custkey;
        int32_t orderdate;
        int64_t totalprice;
    };

    // Open-addressing hash table for orders (one entry per qualifying orderkey)
    struct OrderHashEntry { int32_t key; OrderData value; bool occupied = false; };
    std::vector<OrderHashEntry> orders_table;
    size_t orders_mask;
    {
        size_t sz = 1;
        while (sz < qualifying_count * 4 / 3) sz <<= 1;
        orders_table.resize(sz);
        orders_mask = sz - 1;
    }

    auto orders_hash_fn = [](int32_t key) { return (size_t)key * 0x9E3779B97F4A7C15ULL; };

    for (int64_t i = 0; i < orders_rows; i++) {
        if (is_qualifying(o_orderkey[i])) {
            int32_t key = o_orderkey[i];
            size_t idx = orders_hash_fn(key) & orders_mask;
            while (orders_table[idx].occupied && orders_table[idx].key != key) {
                idx = (idx + 1) & orders_mask;
            }
            if (!orders_table[idx].occupied) {
                orders_table[idx] = {key, {o_custkey[i], o_orderdate[i], o_totalprice[i]}, true};
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_build_end = std::chrono::high_resolution_clock::now();
    double ms_orders_build = std::chrono::duration<double, std::milli>(t_orders_build_end - t_orders_build_start).count();
    printf("[TIMING] orders_build: %.2f ms\n", ms_orders_build);
#endif

    // ========================================================================
    // STEP 3: Build hash table on customer using open-addressing
    // Key: c_custkey -> (c_name_code)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_customer_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Open-addressing hash table for customer (one entry per customer custkey)
    struct CustomerHashEntry { int32_t key; int32_t value; bool occupied = false; };
    std::vector<CustomerHashEntry> customer_table;
    size_t customer_mask;
    {
        size_t sz = 1;
        while (sz < customer_rows * 4 / 3) sz <<= 1;
        customer_table.resize(sz);
        customer_mask = sz - 1;
    }

    auto customer_hash_fn = [](int32_t key) { return (size_t)key * 0x9E3779B97F4A7C15ULL; };

    for (int64_t i = 0; i < customer_rows; i++) {
        int32_t key = c_custkey[i];
        size_t idx = customer_hash_fn(key) & customer_mask;
        while (customer_table[idx].occupied && customer_table[idx].key != key) {
            idx = (idx + 1) & customer_mask;
        }
        if (!customer_table[idx].occupied) {
            customer_table[idx] = {key, c_name_code[i], true};
        }
    }

#ifdef GENDB_PROFILE
    auto t_customer_build_end = std::chrono::high_resolution_clock::now();
    double ms_customer_build = std::chrono::duration<double, std::milli>(t_customer_build_end - t_customer_build_start).count();
    printf("[TIMING] customer_build: %.2f ms\n", ms_customer_build);
#endif

    // ========================================================================
    // STEP 4: Join lineitem with orders and customer, then aggregate
    // Parallelized with thread-local final_agg buffers
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation buffers - use smaller per-thread sizes for better cache
    std::vector<CompactHashTableComposite> thread_final_aggs;
    for (int t = 0; t < num_threads; t++) {
        thread_final_aggs.emplace_back(1000000 / num_threads + 10000);
    }

    int64_t matched_rows = 0;

    // Parallel lineitem scan with thread-local aggregation
    #pragma omp parallel for schedule(static, 100000)
    for (int64_t i = 0; i < lineitem_rows; i++) {
        int tid = omp_get_thread_num();
        int32_t orderkey = l_orderkey[i];

        // Check if orderkey qualifies (hash set lookup is O(1))
        if (!is_qualifying(orderkey)) continue;

        // Lookup order data from orders hash table
        size_t idx = orders_hash_fn(orderkey) & orders_mask;
        OrderData* order_data = nullptr;
        while (orders_table[idx].occupied) {
            if (orders_table[idx].key == orderkey) {
                order_data = &orders_table[idx].value;
                break;
            }
            idx = (idx + 1) & orders_mask;
        }
        if (!order_data) continue;

        int32_t custkey = order_data->custkey;

        // Lookup customer name from customer hash table
        size_t c_idx = customer_hash_fn(custkey) & customer_mask;
        int32_t* name_code_ptr = nullptr;
        while (customer_table[c_idx].occupied) {
            if (customer_table[c_idx].key == custkey) {
                name_code_ptr = &customer_table[c_idx].value;
                break;
            }
            c_idx = (c_idx + 1) & customer_mask;
        }
        if (!name_code_ptr) continue;

        int32_t name_code = *name_code_ptr;

        // Insert into thread-local final aggregation
        AggregateKey agg_key = {
            custkey,
            orderkey,
            order_data->orderdate,
            order_data->totalprice,
            name_code
        };

        thread_final_aggs[tid].insert_or_add(agg_key, l_quantity[i]);
        #pragma omp atomic
        matched_rows++;
    }

    // Merge phase: combine all thread-local final aggregations
    CompactHashTableComposite final_agg(qualifying_count + 10000);
    for (int t = 0; t < num_threads; t++) {
        thread_final_aggs[t].iterate([&](const AggregateKey& key, int64_t value) {
            final_agg.insert_or_add(key, value);
        });
    }

#ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double ms_join_agg = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_agg: %.2f ms\n", ms_join_agg);
#endif

    std::cout << "[JOIN] Matched rows: " << matched_rows << std::endl;
    std::cout << "[AGG] Aggregated groups: " << final_agg.num_entries << std::endl;

    // ========================================================================
    // STEP 5: Sort by o_totalprice DESC, o_orderdate ASC, and apply LIMIT 100
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct ResultRow {
        std::string c_name;
        int32_t c_custkey;
        int32_t o_orderkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
        int64_t sum_qty;
    };

    std::vector<ResultRow> results;
    final_agg.iterate([&](const AggregateKey& key, int64_t value) {
        std::string name = (key.name_code < c_name_dict.size())
            ? c_name_dict[key.name_code]
            : "UNKNOWN";
        results.push_back({
            name,
            key.custkey,
            key.orderkey,
            key.orderdate,
            key.totalprice,
            value
        });
    });

    // Apply LIMIT 100 with partial sort
    if (results.size() > 100) {
        std::partial_sort(results.begin(), results.begin() + 100, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice) {
                    return a.o_totalprice > b.o_totalprice;  // DESC
                }
                return a.o_orderdate < b.o_orderdate;  // ASC
            });
        results.resize(100);
    } else {
        // Still need to sort even if fewer than 100
        std::sort(results.begin(), results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice) {
                    return a.o_totalprice > b.o_totalprice;  // DESC
                }
                return a.o_orderdate < b.o_orderdate;  // ASC
            });
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ========================================================================
    // STEP 6: Write output CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream output(results_dir + "/Q18.csv");
    output << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";

    for (const auto& row : results) {
        std::string date_str = epoch_to_date(row.o_orderdate);
        // Format: c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice (scaled down), sum_qty (scaled down)
        output << row.c_name << ","
               << row.c_custkey << ","
               << row.o_orderkey << ","
               << date_str << ",";

        // Format o_totalprice: int64 is stored as value * 100 (2 decimal places)
        // So divide by 100.0 to get actual value
        double total_price = static_cast<double>(row.o_totalprice) / 100.0;
        char price_buf[32];
        snprintf(price_buf, 32, "%.2f", total_price);
        output << price_buf << ",";

        // Format sum_qty: int64 is stored as value * (scale_factor 2) = value * 100 (2 decimal places)
        // So divide by 100.0 to get actual value
        double sum_qty = static_cast<double>(row.sum_qty) / 100.0;
        char qty_buf[32];
        snprintf(qty_buf, 32, "%.2f", sum_qty);
        output << qty_buf << "\n";
    }

    output.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    std::cout << "[OUTPUT] Results written to " << results_dir << "/Q18.csv" << std::endl;
    std::cout << "[OUTPUT] Row count: " << results.size() << std::endl;

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
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
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
