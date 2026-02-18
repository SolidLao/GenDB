/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 10 (Critical Section Elimination)
 *
 * LOGICAL PLAN:
 * 1. Load nation table and identify FRANCE/GERMANY keys
 * 2. Load supplier table → direct array mapping (100K) with target filter
 * 3. Phase 1: Load customer table → filter to target nations (parallelized)
 *    - Pre-allocate output buffer of exact size
 *    - Use atomic counter to avoid critical section
 * 4. Phase 2: Build compact customer_nation hash table from filtered customers
 * 5. Phase 3: Load orders table → filter to target customers (parallelized)
 *    - Pre-allocate output buffer of exact size
 *    - Use atomic counter to avoid critical section
 * 6. Phase 4: Build compact order_customer hash table from filtered orders
 * 7. Filter lineitem by date range [1995-01-01, 1996-12-31]
 * 8. For each lineitem row:
 *    - Check supplier nation is target (direct array lookup)
 *    - Lookup order→customer via compact hash table
 *    - Lookup customer→nation via compact hash table
 *    - Verify nation pair matches (FRANCE,GERMANY) or (GERMANY,FRANCE)
 *    - Extract year (O(1) table lookup)
 *    - Accumulate volume
 * 9. Merge thread-local aggregations
 * 10. Sort and output results
 *
 * OPTIMIZATION FOCUS (Iteration 10):
 * - CRITICAL SECTION ELIMINATION: Replace #pragma omp critical with pre-sized output
 *   Iteration 8 bottleneck: omp critical sections blocking parallelism
 *     - Phase 1 filter (customer): 1.5M rows with critical merge cost ~15ms
 *     - Phase 3 filter (orders): 15M rows with critical merge cost ~20ms
 *   Iteration 10 approach: Pre-allocate exact-size output, use atomic counter
 *     - Estimate filtered size from column statistics
 *     - Allocate single output buffer for all threads
 *     - Each thread computes local output size → atomic increment for offset
 *     - Then write directly to output[offset:offset+local_size]
 *   Expected gain: 98ms → 70-75ms load_data (20-25% improvement)
 * - Keep compact hash table design
 * - Improve cache locality by reducing intermediate vector allocations
 *
 * PERFORMANCE TARGET:
 * - Iter 8: 121ms total (98ms load_data, 23ms scan_filter)
 * - Iter 10 target: ~90-95ms total (70-75ms load_data, 23ms scan_filter)
 *   - 1.3-1.4x speedup overall
 *   - Reduce 2.1x Umbra gap to ~1.8x
 *
 * CARDINALITY ESTIMATES:
 * - supplier→nation: 100K rows, direct array
 * - customer→nation: ~120K filtered rows, compact hash table
 * - order_customer: ~750K filtered rows, compact hash table
 * - Final groups: 4 (from TPC-H Q7)
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
#include <atomic>
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
    // Extract only customers with target nations (parallelized, NO CRITICAL SECTION)
    size_t customer_count;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // First pass: count filtered rows (parallel reduction, O(1.5M) with 64 threads = fast)
    size_t customer_filtered_count = 0;
    #pragma omp parallel for schedule(static) reduction(+:customer_filtered_count)
    for (size_t i = 0; i < customer_count; i++) {
        if (nation_is_target[c_nationkey[i]]) {
            customer_filtered_count++;
        }
    }

    // Pre-allocate output buffer to exact size (eliminates vector realloc/copy in critical section)
    std::vector<std::pair<int32_t, int32_t>> filtered_customers;
    filtered_customers.resize(customer_filtered_count);

    // Second pass: fill output buffer without critical section
    std::atomic<size_t> customer_output_offset(0);

    #pragma omp parallel
    {
        std::vector<std::pair<int32_t, int32_t>> local_buf;
        local_buf.reserve(customer_filtered_count / omp_get_num_threads() + 100);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < customer_count; i++) {
            if (nation_is_target[c_nationkey[i]]) {
                local_buf.push_back({c_custkey[i], c_nationkey[i]});
            }
        }

        // Each thread writes its output to a disjoint section using atomic offset
        // fetch_add is much cheaper than #pragma omp critical (no cache-line bouncing)
        if (!local_buf.empty()) {
            size_t offset = customer_output_offset.fetch_add(local_buf.size());
            std::copy(local_buf.begin(), local_buf.end(), filtered_customers.begin() + offset);
        }
    }

    // Phase 2: Build customer→nation compact hash table from filtered customers (~120K)
    CompactHashTableI32I32 customer_nation(filtered_customers.size() * 4 / 3);
    for (const auto& [custkey, nationkey] : filtered_customers) {
        customer_nation.insert(custkey, nationkey);
    }

    // Phase 3: Load and filter orders table (15M → ~750K)
    // Extract only orders with target customers (parallelized, NO CRITICAL SECTION)
    size_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // First pass: count filtered rows
    size_t orders_filtered_count = 0;
    #pragma omp parallel for schedule(static) reduction(+:orders_filtered_count)
    for (size_t i = 0; i < orders_count; i++) {
        const int32_t* cust_nation = customer_nation.find(o_custkey[i]);
        if (cust_nation != nullptr) {
            orders_filtered_count++;
        }
    }

    // Pre-allocate output buffer to exact size
    std::vector<std::pair<int32_t, int32_t>> filtered_orders;
    filtered_orders.resize(orders_filtered_count);

    // Second pass: fill output buffer without critical section
    std::atomic<size_t> orders_output_offset(0);

    #pragma omp parallel
    {
        std::vector<std::pair<int32_t, int32_t>> local_buf;
        local_buf.reserve(orders_filtered_count / omp_get_num_threads() + 100);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < orders_count; i++) {
            const int32_t* cust_nation = customer_nation.find(o_custkey[i]);
            if (cust_nation != nullptr) {
                local_buf.push_back({o_orderkey[i], o_custkey[i]});
            }
        }

        // Each thread writes its output to a disjoint section using atomic offset
        if (!local_buf.empty()) {
            size_t offset = orders_output_offset.fetch_add(local_buf.size());
            std::copy(local_buf.begin(), local_buf.end(), filtered_orders.begin() + offset);
        }
    }

    // Phase 4: Build orders→customer compact hash table from filtered orders (~750K)
    CompactHashTableI32I32 order_customer(filtered_orders.size() * 4 / 3);
    for (const auto& [orderkey, custkey] : filtered_orders) {
        order_customer.insert(orderkey, custkey);
    }

    // Load lineitem table
    size_t lineitem_count;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int32_t* l_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
    #endif

    // Parallel scan and aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::unordered_map<AggKey, int64_t, AggKeyHash>> thread_aggs(omp_get_max_threads());

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = thread_aggs[tid];

        #pragma omp for schedule(dynamic, 10000)
        for (size_t i = 0; i < lineitem_count; i++) {
            // Filter by date range
            int32_t ship_date = l_shipdate[i];
            if (ship_date < DATE_MIN || ship_date > DATE_MAX) continue;

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
            int16_t year = YEAR_TABLE[ship_date];

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
