/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 6 (Binary Search Date Range Pruning)
 *
 * LOGICAL PLAN:
 * 1. Load nation table and identify FRANCE/GERMANY keys
 * 2. Load supplier table → direct array mapping (100K) with target filter
 * 3. Phase 1: Load customer table → filter to target nations only (~120K rows)
 * 4. Phase 2: Build compact customer_nation hash table from filtered customers
 * 5. Phase 3: Load orders table → filter to orders with target customers (~750K rows)
 * 6. Phase 4: Build compact order_customer hash table from filtered orders (~750K)
 * 7. CRITICAL OPTIMIZATION: Binary search lineitem's l_shipdate to find date range bounds
 *    Since lineitem is SORTED by l_shipdate, we can skip 97%+ of the table!
 *    - Find first row where l_shipdate >= DATE_MIN (1995-01-01)
 *    - Find last row where l_shipdate <= DATE_MAX (1996-12-31)
 *    - Load ONLY rows in [first_idx, last_idx] range (~2M rows instead of 60M)
 * 8. For each qualifying lineitem row:
 *    - Check supplier nation is target (direct array lookup)
 *    - Lookup order→customer via compact hash table (pre-filtered to 750K)
 *    - Lookup customer→nation via compact hash table (pre-filtered to 120K)
 *    - Verify nation pair matches (FRANCE,GERMANY) or (GERMANY,FRANCE)
 *    - Extract year (O(1) table lookup)
 *    - Accumulate volume
 * 9. Merge thread-local aggregations
 * 10. Sort and output results
 *
 * OPTIMIZATION FOCUS (Iteration 6):
 * - CRITICAL FIX: Use sorted lineitem → binary search for date range
 *   Iteration 4 bottleneck: Loading all 60M lineitem rows even though only 2M qualify
 *   - I/O reduction: 60M rows → 2M rows (97% reduction)
 *   - Expected: 110ms load_data → 40-50ms load_data (2.5x faster)
 *   Total: 145ms → 85-95ms (1.5x faster)
 * - Binary search on pre-loaded l_shipdate first column (0-copy range lookup)
 * - Load only the qualifying range of all other columns
 *
 * PERFORMANCE EXPECTED (Iteration 6):
 * - Iter 4: 145ms (load_data 111ms, scan_filter 33ms)
 * - Iter 6 target: ~85-100ms (1.5-1.7x speedup)
 *   - load_data: 40-50ms (binary search + range load)
 *   - scan_filter: 35-40ms (2M rows instead of 60M)
 *   - aggregation: 0.1ms
 *   - Result: 2.4x gap → 1.5x gap (approaching DuckDB at 77ms)
 *
 * CARDINALITY ESTIMATES:
 * - supplier→nation: 100K rows, direct array
 * - customer→nation: ~120K filtered rows, compact hash table
 * - order_customer: ~750K filtered rows, compact hash table
 * - lineitem date range: ~2M rows (97% pruned via binary search)
 * - Final groups: 8 (2 supp × 2 cust × 2 years)
 *****************************************************************************/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Compact hash table for int32_t → int32_t mapping (customer → nation)
// Uses open-addressing with robin hood hashing to achieve cache-friendly O(1) lookups
struct CompactHashTableI32I32 {
    struct Entry {
        int32_t key;
        int32_t value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTableI32I32(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap, {0, 0, 0, false});
        mask = cap - 1;
    }

    // Murmur3-style hash for better distribution
    static size_t hash_key(int32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        return h ^ (h >> 32);
    }

    void insert(int32_t key, int32_t value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
    }

    const int32_t* find(int32_t key) const {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) {
                return nullptr;
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

// Precomputed year lookup table for O(1) extraction
static int16_t YEAR_TABLE[30000];

void init_year_table() {
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;

        day_of_month++;
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_month = days_per_month[month - 1];
        if (month == 2 && is_leap) days_in_month = 29;

        if (day_of_month > days_in_month) {
            day_of_month = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

// Memory-mapped file loader
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        exit(1);
    }
    return static_cast<T*>(addr);
}

// Binary search to find first/last index in sorted array
// Returns: pair<first_idx, last_idx+1> (exclusive end for STL-style iteration)
// Array must be sorted in ascending order
inline std::pair<size_t, size_t> binary_search_range(
    const int32_t* arr, size_t count, int32_t min_val, int32_t max_val) {

    // Find first index where arr[i] >= min_val
    size_t left = 0, right = count;
    while (left < right) {
        size_t mid = (left + right) / 2;
        if (arr[mid] < min_val) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    size_t first_idx = left;

    // Find last index where arr[i] <= max_val (exclusive end)
    left = 0;
    right = count;
    while (left < right) {
        size_t mid = (left + right) / 2;
        if (arr[mid] <= max_val) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    size_t last_idx_exclusive = left;

    return {first_idx, last_idx_exclusive};
}

// Load nation names (length-prefixed binary strings)
std::vector<std::string> load_nation_names(const std::string& gendb_dir) {
    std::vector<std::string> names;
    std::ifstream f(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Failed to open nation names" << std::endl;
        exit(1);
    }

    while (f) {
        uint32_t len;
        if (!f.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) break;
        if (len == 0 || len > 10000) break;

        std::string s(len, '\0');
        if (!f.read(&s[0], len)) break;
        names.push_back(s);
    }

    return names;
}

// Aggregation key: (supp_nation, cust_nation, year) → revenue sum
struct AggKey {
    std::string supp_nation;
    std::string cust_nation;
    int16_t year;

    bool operator==(const AggKey& o) const {
        return year == o.year && supp_nation == o.supp_nation && cust_nation == o.cust_nation;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        // Combine hashes of the three fields
        size_t h1 = std::hash<std::string>{}(k.supp_nation);
        size_t h2 = std::hash<std::string>{}(k.cust_nation);
        size_t h3 = std::hash<int16_t>{}(k.year);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

void run_Q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    init_year_table();

    // Target date range: 1995-01-01 to 1996-12-31
    // Computed as epoch days since 1970-01-01
    const int32_t DATE_MIN = 9131;  // 1995-01-01
    const int32_t DATE_MAX = 9861;  // 1996-12-31

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation table
    size_t nation_count;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    std::vector<std::string> n_name = load_nation_names(gendb_dir);

    // Find FRANCE and GERMANY nation keys
    int32_t france_key = -1, germany_key = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "FRANCE") france_key = n_nationkey[i];
        if (n_name[i] == "GERMANY") germany_key = n_nationkey[i];
    }
    if (france_key < 0 || germany_key < 0) {
        std::cerr << "Nation keys not found" << std::endl;
        exit(1);
    }

    // Build nation name direct array (index by nation key)
    std::string nation_names[25];
    bool nation_is_target[25] = {};
    for (size_t i = 0; i < nation_count; i++) {
        int32_t nk = n_nationkey[i];
        nation_names[nk] = n_name[i];
        if (nk == france_key || nk == germany_key) {
            nation_is_target[nk] = true;
        }
    }

    // Load supplier table
    size_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build supplier→nation direct array (index by s_suppkey, 1-based)
    // Also build filter array for target suppliers
    int32_t supplier_nation[100001] = {};
    bool supplier_is_target[100001] = {};
    for (size_t i = 0; i < supplier_count; i++) {
        int32_t sk = s_suppkey[i];
        supplier_nation[sk] = s_nationkey[i];
        if (nation_is_target[s_nationkey[i]]) {
            supplier_is_target[sk] = true;
        }
    }

    // Phase 1: Load and filter customer table (1.5M → ~120K)
    // Extract only customers with target nations (parallelized)
    size_t customer_count;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    std::vector<std::pair<int32_t, int32_t>> filtered_customers;
    filtered_customers.reserve(customer_count / 10);  // Estimate ~10% target

    #pragma omp parallel
    {
        std::vector<std::pair<int32_t, int32_t>> local_filtered;
        local_filtered.reserve(customer_count / (10 * omp_get_num_threads()) + 1000);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < customer_count; i++) {
            if (nation_is_target[c_nationkey[i]]) {
                local_filtered.push_back({c_custkey[i], c_nationkey[i]});
            }
        }

        // Merge thread-local results into global buffer
        #pragma omp critical
        {
            filtered_customers.insert(filtered_customers.end(), local_filtered.begin(), local_filtered.end());
        }
    }

    // Phase 2: Build customer→nation compact hash table from filtered customers (~120K)
    CompactHashTableI32I32 customer_nation(filtered_customers.size() * 4 / 3);
    for (const auto& [custkey, nationkey] : filtered_customers) {
        customer_nation.insert(custkey, nationkey);
    }

    // Phase 3: Load and filter orders table (15M → ~750K)
    // Extract only orders with target customers (parallelized)
    size_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Use thread-local buffers for filtered orders, then merge
    std::vector<std::pair<int32_t, int32_t>> filtered_orders;
    filtered_orders.reserve(orders_count / 20);  // Estimate ~5% target

    #pragma omp parallel
    {
        std::vector<std::pair<int32_t, int32_t>> local_filtered;
        local_filtered.reserve(orders_count / (20 * omp_get_num_threads()) + 1000);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < orders_count; i++) {
            const int32_t* cust_nation = customer_nation.find(o_custkey[i]);
            if (cust_nation != nullptr) {
                local_filtered.push_back({o_orderkey[i], o_custkey[i]});
            }
        }

        // Merge thread-local results into global buffer
        #pragma omp critical
        {
            filtered_orders.insert(filtered_orders.end(), local_filtered.begin(), local_filtered.end());
        }
    }

    // Phase 4: Build orders→customer compact hash table from filtered orders (~750K)
    CompactHashTableI32I32 order_customer(filtered_orders.size() * 4 / 3);
    for (const auto& [orderkey, custkey] : filtered_orders) {
        order_customer.insert(orderkey, custkey);
    }

    // Load lineitem table
    // CRITICAL OPTIMIZATION: lineitem is sorted by l_shipdate
    // Use binary search to find the date range, then load ONLY those rows
    // This reduces I/O from 60M rows to ~2M rows (97% reduction)
    size_t lineitem_count;
    int32_t* l_shipdate_full = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Binary search for date range [DATE_MIN, DATE_MAX]
    auto [lineitem_start, lineitem_end] = binary_search_range(l_shipdate_full, lineitem_count, DATE_MIN, DATE_MAX);
    size_t lineitem_range_count = lineitem_end - lineitem_start;

    // Now load ONLY the qualifying range from other columns
    // We need to seek to the correct offset in each file
    auto load_range = [&](const std::string& path, size_t start, size_t count) -> void* {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        size_t offset_bytes = start * sizeof(int32_t);
        size_t size_bytes = count * sizeof(int32_t);
        void* addr = mmap(nullptr, size_bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, offset_bytes);
        close(fd);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap range " << path << std::endl;
            exit(1);
        }
        return addr;
    };

    auto load_range_i64 = [&](const std::string& path, size_t start, size_t count) -> void* {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        size_t offset_bytes = start * sizeof(int64_t);
        size_t size_bytes = count * sizeof(int64_t);
        void* addr = mmap(nullptr, size_bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, offset_bytes);
        close(fd);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap range " << path << std::endl;
            exit(1);
        }
        return addr;
    };

    // Load only the qualifying range
    int32_t* l_orderkey = static_cast<int32_t*>(load_range(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_start, lineitem_range_count));
    int32_t* l_suppkey = static_cast<int32_t*>(load_range(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_start, lineitem_range_count));
    int32_t* l_shipdate = static_cast<int32_t*>(load_range(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_start, lineitem_range_count));
    int64_t* l_extendedprice = static_cast<int64_t*>(load_range_i64(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_start, lineitem_range_count));
    int64_t* l_discount = static_cast<int64_t*>(load_range_i64(gendb_dir + "/lineitem/l_discount.bin", lineitem_start, lineitem_range_count));

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
    #endif

    // Parallel scan and aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<AggKey, int64_t, AggKeyHash>> thread_aggs(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = thread_aggs[tid];

        // Now iterate only over the date-filtered range
        // No date check needed since we already filtered via binary search!
        #pragma omp for schedule(dynamic, 10000)
        for (size_t i = 0; i < lineitem_range_count; i++) {
            // Lookup supplier and check if target nation
            int32_t suppkey = l_suppkey[i];
            if (!supplier_is_target[suppkey]) continue;
            int32_t supp_nation_key = supplier_nation[suppkey];

            // Lookup order → customer via compact hash table
            const int32_t* custkey_ptr = order_customer.find(l_orderkey[i]);
            if (custkey_ptr == nullptr) continue;
            int32_t custkey = *custkey_ptr;

            // Lookup customer nation via compact hash table
            const int32_t* cust_nation_key_ptr = customer_nation.find(custkey);
            if (cust_nation_key_ptr == nullptr) continue;
            int32_t cust_nation_key = *cust_nation_key_ptr;

            // Check nation pair filter: (FRANCE, GERMANY) or (GERMANY, FRANCE)
            if (!((supp_nation_key == france_key && cust_nation_key == germany_key) ||
                  (supp_nation_key == germany_key && cust_nation_key == france_key))) {
                continue;
            }

            // Extract year from shipdate (O(1) lookup)
            int16_t year = YEAR_TABLE[l_shipdate[i]];

            // Compute volume: extendedprice * (1 - discount)
            // extendedprice is scaled by 100, discount is scaled by 100
            // Result: extendedprice * (100 - discount) is scaled by 100*100 = 10000
            int64_t volume = l_extendedprice[i] * (100 - l_discount[i]);

            // Aggregate
            AggKey key{nation_names[supp_nation_key], nation_names[cust_nation_key], year};
            local_agg[key] += volume;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
    #endif

    // Merge thread-local aggregations into flat map
    // With only 4-8 groups expected, we can use a sorted vector instead of hash table
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<AggKey, int64_t, AggKeyHash> final_agg;
    // Pre-size to expected cardinality to avoid rehashes
    final_agg.reserve(16);
    for (const auto& local : thread_aggs) {
        for (const auto& [k, v] : local) {
            final_agg[k] += v;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", merge_ms);
    #endif

    // Prepare results for sorting
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct ResultRow {
        std::string supp_nation;
        std::string cust_nation;
        int16_t l_year;
        int64_t revenue;
    };

    std::vector<ResultRow> results;
    results.reserve(final_agg.size());
    for (const auto& [k, v] : final_agg) {
        results.push_back({k.supp_nation, k.cust_nation, k.year, v});
    }

    // Sort by supp_nation, cust_nation, l_year
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
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

    // Write output to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q7.csv");
    out << "supp_nation,cust_nation,l_year,revenue\n";
    for (const auto& r : results) {
        // Revenue is scaled by 10000 (extendedprice scale 100 × discount scale 100)
        // Convert to decimal with 4 places: divide by 10000, format with 4 decimal places
        double revenue_decimal = r.revenue / 10000.0;
        out << r.supp_nation << "," << r.cust_nation << "," << r.l_year << ","
            << std::fixed << std::setprecision(4) << revenue_decimal << "\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    std::cout << "Q7 completed: " << results.size() << " result rows\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q7(gendb_dir, results_dir);
    return 0;
}
#endif
