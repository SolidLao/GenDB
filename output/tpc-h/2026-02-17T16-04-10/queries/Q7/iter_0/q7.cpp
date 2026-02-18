/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 0 (Optimized)
 *
 * LOGICAL PLAN:
 * 1. Load all required tables from binary column files
 * 2. Identify target nation keys (FRANCE=9, GERMANY=8 or similar)
 * 3. Build pre-filtered lookup structures:
 *    - supplier→nation mapping (direct array by s_suppkey)
 *    - customer→nation mapping (hash table for sparse lookups)
 *    - orders→customer mapping (hash table for sparse lookups)
 * 4. Filter lineitem by date range [1995-01-01, 1996-12-31] (9131..9861 epoch days)
 * 5. For each lineitem row:
 *    - Check supplier nation is target (FRANCE or GERMANY)
 *    - Lookup order→customer→customer nation
 *    - Verify nation pair matches (FRANCE,GERMANY) or (GERMANY,FRANCE)
 *    - Extract year from shipdate (O(1) via precomputed table)
 *    - Accumulate volume in aggregation key (supp_nation, cust_nation, year)
 * 6. Merge thread-local aggregations
 * 7. Sort results by (supp_nation, cust_nation, l_year)
 * 8. Output to CSV with proper decimal scaling
 *
 * PHYSICAL PLAN:
 * - Data structures:
 *   * Direct array for nation names (25 elements)
 *   * Direct array for supplier nations (100K elements, indexed by s_suppkey)
 *   * Hash table for customer nations (prefiltered to ~120K entries)
 *   * Hash table for order→customer mapping (prefiltered, ~3-4M entries)
 *   * Year lookup table for O(1) year extraction from epoch days
 *   * Aggregation: 8 small groups (2 nations × 2 directions × 2 years)
 *
 * - Joins:
 *   * Supplier: hash lookup by s_suppkey (pre-qualified in supplier_is_target array)
 *   * Orders: hash lookup by o_orderkey (only target orders pre-inserted)
 *   * Customer: hash lookup by c_custkey (only target customers pre-inserted)
 *   * Nation: array lookup by n_nationkey
 *
 * - Parallelism:
 *   * Parallel lineitem scan with dynamic scheduling (morsel size ~10K rows)
 *   * Thread-local aggregation hash tables to avoid contention
 *   * Sequential merge after scan (small overhead, <8 groups)
 *
 * CARDINALITY ESTIMATES:
 * - nation: 25 total, 2 target (FRANCE, GERMANY)
 * - supplier: 100K total, ~8K matching target nations (~8%)
 * - customer: 1.5M total, ~120K matching target nations (~8%)
 * - orders: 15M total, ~500K-1M with target customers
 * - lineitem: 60M total
 *   * Date filter [1995-01-01, 1996-12-31]: ~12M rows (~20%, 2 years of 7 year range)
 *   * After nation filter: ~100K-500K rows
 * - Final groups: 8 (2 supp nations × 2 cust nations × 2 years)
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

    // Load customer table
    size_t customer_count;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Build customer→nation hash table (only target customers)
    std::unordered_map<int32_t, int32_t> customer_nation;
    customer_nation.reserve(customer_count / 10);  // Estimate 10% are target
    for (size_t i = 0; i < customer_count; i++) {
        if (nation_is_target[c_nationkey[i]]) {
            customer_nation[c_custkey[i]] = c_nationkey[i];
        }
    }

    // Load orders table
    size_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Build orders→customer hash table (only for target customers)
    std::unordered_map<int32_t, int32_t> order_customer;
    order_customer.reserve(orders_count / 20);  // Estimate 5% are target
    for (size_t i = 0; i < orders_count; i++) {
        if (customer_nation.count(o_custkey[i]) > 0) {
            order_customer[o_orderkey[i]] = o_custkey[i];
        }
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

    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<AggKey, int64_t, AggKeyHash>> thread_aggs(num_threads);

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

            // Lookup order → customer
            auto oc_it = order_customer.find(l_orderkey[i]);
            if (oc_it == order_customer.end()) continue;
            int32_t custkey = oc_it->second;

            // Lookup customer nation
            auto cn_it = customer_nation.find(custkey);
            if (cn_it == customer_nation.end()) continue;
            int32_t cust_nation_key = cn_it->second;

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

    // Merge thread-local aggregations
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<AggKey, int64_t, AggKeyHash> final_agg;
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
