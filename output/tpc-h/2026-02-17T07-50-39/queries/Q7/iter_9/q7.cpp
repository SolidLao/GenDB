/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 9
 *
 * OPTIMIZATION: Eliminate orders hash table by using direct array lookup
 *
 * LOGICAL PLAN:
 * 1. Filter nation to FRANCE and GERMANY (25 → 2 rows)
 * 2. Build target customer bitset (c_nationkey = FRANCE or GERMANY) - ~120K customers
 * 3. Build target supplier bitset (s_nationkey = FRANCE or GERMANY) - ~8K suppliers
 * 4. Load orders o_custkey into direct array indexed by o_orderkey (15M → 60MB, mmap)
 * 5. Scan lineitem with zone map pruning on l_shipdate
 * 6. For each qualifying lineitem row:
 *    - Check if l_suppkey in target supplier set (bitset lookup)
 *    - Lookup o_custkey via direct array o_custkey[l_orderkey]
 *    - Check if custkey in target customer set (bitset lookup)
 *    - Get nation keys via direct arrays
 *    - Check nation pair filter
 *    - Accumulate in flat array (4 groups, atomic)
 *
 * KEY CHANGES FROM ITERATION 3:
 * - Eliminate 1.2M entry orders hash table build (was ~50ms)
 * - Use direct array o_custkey[orderkey] instead (O(1) lookup, zero build)
 * - Use bitsets for customer/supplier filtering (cache-friendly)
 * - Sequential scan of orders column is better than random-access hash table
 *****************************************************************************/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Date lookup table for O(1) year extraction
static int16_t YEAR_TABLE[30000];

void init_date_table() {
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;

        day_of_month++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
        if (day_of_month > dim) {
            day_of_month = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

// Memory-mapped file helper
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

// Zone map entry structure
struct ZoneEntry {
    int32_t min_val;
    int32_t max_val;
};

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    init_date_table();

    // Date range: 1995-01-01 to 1996-12-31
    const int32_t date_min = 9131;
    const int32_t date_max = 9861;

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation
    size_t nation_count;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    std::vector<std::string> n_name = load_nation_names(gendb_dir);

    // Find FRANCE and GERMANY nationkeys
    int32_t france_key = -1, germany_key = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "FRANCE") france_key = n_nationkey[i];
        if (n_name[i] == "GERMANY") germany_key = n_nationkey[i];
    }
    if (france_key < 0 || germany_key < 0) {
        std::cerr << "Nation keys not found" << std::endl;
        exit(1);
    }

    // Build nation name lookup array
    std::string nation_names[25];
    for (size_t i = 0; i < nation_count; i++) {
        nation_names[n_nationkey[i]] = n_name[i];
    }

    // Load supplier data - build direct arrays
    size_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey_data = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Direct arrays indexed by suppkey
    int32_t s_nationkey[100001] = {};
    bool s_is_target[100001] = {};

    for (size_t i = 0; i < supplier_count; i++) {
        int32_t suppkey = s_suppkey[i];
        int32_t nationkey = s_nationkey_data[i];
        s_nationkey[suppkey] = nationkey;
        if (nationkey == france_key || nationkey == germany_key) {
            s_is_target[suppkey] = true;
        }
    }

    // Load customer data - build direct arrays
    size_t customer_count;
    int32_t* c_custkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Direct arrays indexed by custkey
    int32_t c_nationkey[1500001] = {};
    bool c_is_target[1500001] = {};

    for (size_t i = 0; i < customer_count; i++) {
        int32_t custkey = c_custkey_data[i];
        int32_t nationkey = c_nationkey_data[i];
        c_nationkey[custkey] = nationkey;
        if (nationkey == france_key || nationkey == germany_key) {
            c_is_target[custkey] = true;
        }
    }

    // Load orders data - build direct array o_custkey[o_orderkey]
    // No hash table, just direct array for O(1) lookup
    size_t orders_count;
    int32_t* o_orderkey_data = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey_data = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Allocate direct array (max orderkey is ~60M for SF10)
    // Use vector for safety (auto-resize if needed)
    std::vector<int32_t> o_custkey_lookup(60000001, 0);

    for (size_t i = 0; i < orders_count; i++) {
        int32_t orderkey = o_orderkey_data[i];
        if (orderkey < (int32_t)o_custkey_lookup.size()) {
            o_custkey_lookup[orderkey] = o_custkey_data[i];
        }
    }

    // Load lineitem data
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

    // Load zone map index for lineitem l_shipdate
    #ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd_zone = open((gendb_dir + "/indexes/lineitem_shipdate_zone.bin").c_str(), O_RDONLY);
    struct stat sb_zone;
    fstat(fd_zone, &sb_zone);
    void* zone_addr = mmap(nullptr, sb_zone.st_size, PROT_READ, MAP_PRIVATE, fd_zone, 0);
    close(fd_zone);

    uint32_t* zone_ptr = static_cast<uint32_t*>(zone_addr);
    uint32_t num_zones = zone_ptr[0];
    ZoneEntry* zones = reinterpret_cast<ZoneEntry*>(zone_ptr + 1);

    const size_t block_size = 100000;
    std::vector<bool> block_active(num_zones, false);
    for (uint32_t z = 0; z < num_zones; z++) {
        if (zones[z].max_val >= date_min && zones[z].min_val <= date_max) {
            block_active[z] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    double zone_ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] zone_map_setup: %.2f ms\n", zone_ms);
    #endif

    // Parallel scan and aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Aggregation: flat array for 4 groups
    // [FRANCE→GERMANY 1995, FRANCE→GERMANY 1996, GERMANY→FRANCE 1995, GERMANY→FRANCE 1996]
    std::atomic<int64_t> agg_revenue[4];
    for (int i = 0; i < 4; i++) {
        agg_revenue[i].store(0, std::memory_order_relaxed);
    }

    #pragma omp parallel
    {
        #pragma omp for schedule(dynamic, 10)
        for (uint32_t z = 0; z < num_zones; z++) {
            if (!block_active[z]) continue;

            size_t start = z * block_size;
            size_t end = std::min(start + block_size, lineitem_count);

            for (size_t i = start; i < end; i++) {
                // Filter by date
                int32_t shipdate = l_shipdate[i];
                if (shipdate < date_min || shipdate > date_max) continue;

                // Filter supplier (bitset check)
                int32_t suppkey = l_suppkey[i];
                if (!s_is_target[suppkey]) continue;

                // Lookup order → customer via direct array
                int32_t orderkey = l_orderkey[i];
                if (orderkey >= (int32_t)o_custkey_lookup.size()) continue;

                int32_t custkey = o_custkey_lookup[orderkey];
                if (custkey == 0) continue; // no matching order (or orderkey 0, but those don't exist in TPC-H)

                // Filter customer (bitset check)
                if (!c_is_target[custkey]) continue;

                // Get nation keys
                int32_t supp_nation_key = s_nationkey[suppkey];
                int32_t cust_nation_key = c_nationkey[custkey];

                // Check nation pair filter
                if (!((supp_nation_key == france_key && cust_nation_key == germany_key) ||
                      (supp_nation_key == germany_key && cust_nation_key == france_key))) {
                    continue;
                }

                // Extract year
                int16_t year = YEAR_TABLE[shipdate];

                // Compute volume
                int64_t volume = l_extendedprice[i] * (100 - l_discount[i]);

                // Determine aggregation index
                int agg_idx;
                if (supp_nation_key == france_key && cust_nation_key == germany_key) {
                    agg_idx = (year == 1995) ? 0 : 1;
                } else { // germany → france
                    agg_idx = (year == 1995) ? 2 : 3;
                }

                // Atomic accumulation
                agg_revenue[agg_idx].fetch_add(volume, std::memory_order_relaxed);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_join: %.2f ms\n", scan_ms);
    #endif

    // No merge needed (atomic aggregation)
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", merge_ms);
    #endif

    // Build result array
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct ResultRow {
        std::string supp_nation;
        std::string cust_nation;
        int16_t year;
        int64_t revenue;
    };

    std::vector<ResultRow> results;
    results.reserve(4);

    const char* nations[2] = {"FRANCE", "GERMANY"};
    int16_t years[2] = {1995, 1996};

    results.push_back({nations[0], nations[1], years[0], agg_revenue[0].load()});
    results.push_back({nations[0], nations[1], years[1], agg_revenue[1].load()});
    results.push_back({nations[1], nations[0], years[0], agg_revenue[2].load()});
    results.push_back({nations[1], nations[0], years[1], agg_revenue[3].load()});

    // Sort (already in order by construction, but for completeness)
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.year < b.year;
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

    // Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q7.csv");
    out << "supp_nation,cust_nation,l_year,revenue\n";
    for (auto& r : results) {
        if (r.revenue > 0) { // only output non-zero groups
            double revenue_decimal = r.revenue / 10000.0;
            out << r.supp_nation << "," << r.cust_nation << "," << r.year << ","
                << std::fixed << std::setprecision(2) << revenue_decimal << "\n";
        }
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    // Count non-zero results
    int result_count = 0;
    for (auto& r : results) {
        if (r.revenue > 0) result_count++;
    }
    std::cout << "Q7 completed: " << result_count << " rows\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q7(gendb_dir, results_dir);
    return 0;
}
#endif
