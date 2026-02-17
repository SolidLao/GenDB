/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 7
 *
 * OPTIMIZATION: Reverse join order + pre-built indexes (eliminate orders hash build)
 *
 * LOGICAL PLAN (Bottom-Up):
 * 1. Filter nation to FRANCE and GERMANY (25 → 2 rows)
 * 2. Filter customers to those 2 nations (~120K customers from 1.5M)
 * 3. Filter orders to those customers (~1.2M orders from 15M, 8% selectivity)
 * 4. For each qualifying order, lookup lineitem rows via pre-built lineitem_orderkey_hash
 * 5. Filter lineitem rows by shipdate + supplier nation
 * 6. Aggregate to 4 groups (2 nation pairs × 2 years)
 *
 * KEY CHANGES FROM ITERATION 3:
 * - ELIMINATE orders hash table build (was consuming 70%+ of load time)
 * - Use pre-built lineitem_orderkey_hash to reverse join direction
 * - Process qualifying orders (1.2M) instead of scanning lineitem (60M)
 * - Load supplier_suppkey_hash for O(1) supplier nation lookup
 * - Parallel processing over qualifying orders with OpenMP
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

// Pre-built hash_single index structure (customer, supplier)
struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

// Pre-built hash_multi_value index structure (lineitem_orderkey)
struct HashMultiEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
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

    // Load customer data and filter to FRANCE/GERMANY nations only
    size_t customer_count;
    int32_t* c_custkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey_data = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Build direct array for customer nation lookup
    int32_t c_nationkey_lookup[1500001] = {}; // max c_custkey
    std::vector<int32_t> target_customers;
    target_customers.reserve(150000); // ~120K expected

    for (size_t i = 0; i < customer_count; i++) {
        int32_t custkey = c_custkey_data[i];
        int32_t nationkey = c_nationkey_data[i];
        c_nationkey_lookup[custkey] = nationkey;
        if (nationkey == france_key || nationkey == germany_key) {
            target_customers.push_back(custkey);
        }
    }

    // Load supplier data via pre-built hash index
    int fd_supp_idx = open((gendb_dir + "/indexes/supplier_suppkey_hash.bin").c_str(), O_RDONLY);
    if (fd_supp_idx < 0) {
        std::cerr << "Failed to open supplier_suppkey_hash index" << std::endl;
        exit(1);
    }
    struct stat sb_supp;
    fstat(fd_supp_idx, &sb_supp);
    void* supp_idx_addr = mmap(nullptr, sb_supp.st_size, PROT_READ, MAP_PRIVATE, fd_supp_idx, 0);
    close(fd_supp_idx);

    uint32_t* supp_idx_ptr = static_cast<uint32_t*>(supp_idx_addr);
    uint32_t supp_table_size = supp_idx_ptr[1];
    HashSingleEntry* supp_hash_table = reinterpret_cast<HashSingleEntry*>(supp_idx_ptr + 2);

    // Load supplier nation data
    size_t supplier_count;
    int32_t* s_nationkey_data = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Load orders data and filter to target customers
    size_t orders_count;
    int32_t* o_orderkey_data = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Build set of target custkeys for fast lookup
    bool c_is_target[1500001] = {};
    for (int32_t custkey : target_customers) {
        c_is_target[custkey] = true;
    }

    // Filter orders to target customers only
    struct QualifyingOrder {
        int32_t orderkey;
        int32_t cust_nationkey;
    };
    std::vector<QualifyingOrder> qualifying_orders;
    qualifying_orders.reserve(1500000); // ~1.2M expected

    for (size_t i = 0; i < orders_count; i++) {
        int32_t custkey = o_custkey[i];
        if (c_is_target[custkey]) {
            qualifying_orders.push_back({o_orderkey_data[i], c_nationkey_lookup[custkey]});
        }
    }

    // Load pre-built lineitem_orderkey_hash index
    int fd_li_idx = open((gendb_dir + "/indexes/lineitem_orderkey_hash.bin").c_str(), O_RDONLY);
    if (fd_li_idx < 0) {
        std::cerr << "Failed to open lineitem_orderkey_hash index" << std::endl;
        exit(1);
    }
    struct stat sb_li;
    fstat(fd_li_idx, &sb_li);
    void* li_idx_addr = mmap(nullptr, sb_li.st_size, PROT_READ, MAP_PRIVATE, fd_li_idx, 0);
    close(fd_li_idx);

    uint32_t* li_idx_ptr = static_cast<uint32_t*>(li_idx_addr);
    uint32_t li_table_size = li_idx_ptr[1];
    HashMultiEntry* li_hash_table = reinterpret_cast<HashMultiEntry*>(li_idx_ptr + 2);

    // Position array starts after hash table
    uint32_t* li_positions_base = reinterpret_cast<uint32_t*>(li_hash_table + li_table_size);

    // Load lineitem columns
    size_t lineitem_count;
    int32_t* l_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    double zone_ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] zone_map_setup: %.2f ms\n", zone_ms);
    #endif

    // Process qualifying orders in parallel
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Aggregation: flat array for 4 groups
    // [FRANCE→GERMANY 1995, FRANCE→GERMANY 1996, GERMANY→FRANCE 1995, GERMANY→FRANCE 1996]
    std::atomic<int64_t> agg_revenue[4];
    for (int i = 0; i < 4; i++) {
        agg_revenue[i].store(0, std::memory_order_relaxed);
    }

    size_t num_qualifying_orders = qualifying_orders.size();

    #pragma omp parallel
    {
        #pragma omp for schedule(dynamic, 5000)
        for (size_t oi = 0; oi < num_qualifying_orders; oi++) {
            int32_t orderkey = qualifying_orders[oi].orderkey;
            int32_t cust_nationkey = qualifying_orders[oi].cust_nationkey;

            // Lookup lineitem rows for this order via pre-built index
            uint64_t hash = (uint64_t)orderkey * 0x9E3779B97F4A7C15ULL;
            uint32_t slot = hash & (li_table_size - 1);

            HashMultiEntry* entry = nullptr;
            for (uint32_t probe = 0; probe < li_table_size; probe++) {
                uint32_t idx = (slot + probe) & (li_table_size - 1);
                if (li_hash_table[idx].key == orderkey) {
                    entry = &li_hash_table[idx];
                    break;
                }
                if (li_hash_table[idx].key == 0) break; // empty slot
            }

            if (!entry) continue; // no lineitem rows for this order

            // Get position array for this orderkey
            uint32_t offset = entry->offset;
            uint32_t count = entry->count;
            uint32_t* positions = li_positions_base + offset + 1; // +1 to skip count field

            // Process each lineitem row for this order
            for (uint32_t pi = 0; pi < count; pi++) {
                uint32_t pos = positions[pi];

                // Filter by shipdate
                int32_t shipdate = l_shipdate[pos];
                if (shipdate < date_min || shipdate > date_max) continue;

                // Lookup supplier nation via pre-built index
                int32_t suppkey = l_suppkey[pos];

                uint64_t supp_hash = (uint64_t)suppkey * 0x9E3779B97F4A7C15ULL;
                uint32_t supp_slot = supp_hash & (supp_table_size - 1);

                uint32_t supp_pos = 0;
                bool found_supp = false;
                for (uint32_t probe = 0; probe < supp_table_size; probe++) {
                    uint32_t idx = (supp_slot + probe) & (supp_table_size - 1);
                    if (supp_hash_table[idx].key == suppkey) {
                        supp_pos = supp_hash_table[idx].position;
                        found_supp = true;
                        break;
                    }
                    if (supp_hash_table[idx].key == 0) break;
                }

                if (!found_supp) continue;

                int32_t supp_nationkey = s_nationkey_data[supp_pos];

                // Check nation pair filter
                if (!((supp_nationkey == france_key && cust_nationkey == germany_key) ||
                      (supp_nationkey == germany_key && cust_nationkey == france_key))) {
                    continue;
                }

                // Extract year
                int16_t year = YEAR_TABLE[shipdate];

                // Compute volume
                int64_t volume = l_extendedprice[pos] * (100 - l_discount[pos]);

                // Determine aggregation index
                int agg_idx;
                if (supp_nationkey == france_key && cust_nationkey == germany_key) {
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
