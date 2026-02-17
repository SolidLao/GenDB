/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 5
 *
 * OPTIMIZATION: Use pre-built customer index + minimal orders hash
 *
 * LOGICAL PLAN:
 * 1. Filter nation to FRANCE and GERMANY (25 → 2 rows)
 * 2. Build supplier nation lookup array s_nationkey[s_suppkey]
 * 3. Load pre-built customer_custkey_hash index
 * 4. Load customer c_nationkey data
 * 5. Build compact filtered orders hash (only FRANCE/GERMANY customer orders)
 * 6. Scan lineitem with zone map pruning:
 *    - Filter by shipdate range
 *    - Probe orders hash (l_orderkey → o_custkey)
 *    - Probe customer hash (o_custkey → position → c_nationkey)
 *    - Check supplier nation (direct array)
 *    - Check nation pair filter
 *    - Accumulate in flat array (4 groups)
 *
 * KEY CHANGES FROM ITERATION 3:
 * - Use pre-built customer_custkey_hash instead of 1.5M-entry arrays
 * - Eliminate redundant customer data structures
 * - Tighter memory footprint in load phase
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

// Pre-built hash_single index structure
struct HashSingleEntry {
    int32_t key;
    uint32_t position;
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

    // Load supplier - build direct array s_nationkey[s_suppkey]
    size_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey_data = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    int32_t s_nationkey[100001] = {};
    for (size_t i = 0; i < supplier_count; i++) {
        s_nationkey[s_suppkey[i]] = s_nationkey_data[i];
    }

    // Load customer c_nationkey
    size_t customer_count;
    int32_t* c_nationkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Load pre-built customer_custkey_hash index
    int fd_cust = open((gendb_dir + "/indexes/customer_custkey_hash.bin").c_str(), O_RDONLY);
    if (fd_cust < 0) {
        std::cerr << "Failed to open customer_custkey_hash index" << std::endl;
        exit(1);
    }
    struct stat sb_cust;
    fstat(fd_cust, &sb_cust);
    void* cust_addr = mmap(nullptr, sb_cust.st_size, PROT_READ, MAP_PRIVATE, fd_cust, 0);
    close(fd_cust);

    uint32_t* cust_ptr = static_cast<uint32_t*>(cust_addr);
    uint32_t cust_table_size = cust_ptr[1];
    HashSingleEntry* customer_hash = reinterpret_cast<HashSingleEntry*>(cust_ptr + 2);

    // Build filtered customer set by scanning customer data
    size_t c_custkey_count;
    int32_t* c_custkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", c_custkey_count);

    // Use bitset for target customers (FRANCE/GERMANY only)
    const uint32_t max_custkey = 1500000;
    std::vector<uint8_t> c_is_target((max_custkey / 8) + 1, 0);

    for (size_t i = 0; i < customer_count; i++) {
        int32_t nationkey = c_nationkey_data[i];
        if (nationkey == france_key || nationkey == germany_key) {
            int32_t custkey = c_custkey_data[i];
            uint32_t byte_idx = custkey / 8;
            uint8_t bit_idx = custkey % 8;
            c_is_target[byte_idx] |= (1 << bit_idx);
        }
    }

    // Load orders and build filtered hash (only target customer orders)
    size_t orders_count;
    int32_t* o_orderkey_data = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Estimate ~1.2M filtered orders
    uint32_t orders_table_size = 1 << 22; // 4M slots
    HashSingleEntry* orders_hash = new HashSingleEntry[orders_table_size]();

    for (size_t i = 0; i < orders_count; i++) {
        int32_t custkey = o_custkey[i];

        // Check if customer is target (bitset lookup)
        uint32_t byte_idx = custkey / 8;
        uint8_t bit_idx = custkey % 8;
        if (byte_idx >= c_is_target.size() || !(c_is_target[byte_idx] & (1 << bit_idx))) {
            continue;
        }

        uint64_t hash = (uint64_t)o_orderkey_data[i] * 0x9E3779B97F4A7C15ULL;
        uint32_t slot = hash & (orders_table_size - 1);

        for (uint32_t probe = 0; probe < orders_table_size; probe++) {
            uint32_t idx = (slot + probe) & (orders_table_size - 1);
            if (orders_hash[idx].key == 0) {
                orders_hash[idx].key = o_orderkey_data[i];
                orders_hash[idx].position = custkey; // store custkey directly
                break;
            }
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
    struct ZoneEntry { int32_t min_val; int32_t max_val; };
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

    // Flat array for 4 groups: [FRANCE→GERMANY 1995, FRANCE→GERMANY 1996, GERMANY→FRANCE 1995, GERMANY→FRANCE 1996]
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
                // Filter by shipdate
                int32_t shipdate = l_shipdate[i];
                if (shipdate < date_min || shipdate > date_max) continue;

                // Lookup supplier nation
                int32_t suppkey = l_suppkey[i];
                int32_t supp_nation_key = s_nationkey[suppkey];
                if (supp_nation_key != france_key && supp_nation_key != germany_key) continue;

                // Probe orders hash
                int32_t orderkey = l_orderkey[i];
                uint64_t hash = (uint64_t)orderkey * 0x9E3779B97F4A7C15ULL;
                uint32_t slot = hash & (orders_table_size - 1);

                bool found = false;
                int32_t custkey = -1;

                for (uint32_t probe = 0; probe < orders_table_size; probe++) {
                    uint32_t idx = (slot + probe) & (orders_table_size - 1);
                    if (orders_hash[idx].key == orderkey) {
                        custkey = orders_hash[idx].position;
                        found = true;
                        break;
                    }
                    if (orders_hash[idx].key == 0) break;
                }

                if (!found) continue;

                // Probe customer hash via pre-built index
                uint64_t cust_hash = (uint64_t)custkey * 0x9E3779B97F4A7C15ULL;
                uint32_t cust_slot = cust_hash & (cust_table_size - 1);

                int32_t cust_nation_key = -1;
                for (uint32_t cust_probe = 0; cust_probe < cust_table_size; cust_probe++) {
                    uint32_t cust_idx = (cust_slot + cust_probe) & (cust_table_size - 1);
                    if (customer_hash[cust_idx].key == custkey) {
                        uint32_t cust_pos = customer_hash[cust_idx].position;
                        cust_nation_key = c_nationkey_data[cust_pos];
                        break;
                    }
                    if (customer_hash[cust_idx].key == 0) break;
                }

                if (cust_nation_key < 0) continue;

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
                } else {
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

    // Cleanup
    delete[] orders_hash;

    // Sort
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
        if (r.revenue > 0) {
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
