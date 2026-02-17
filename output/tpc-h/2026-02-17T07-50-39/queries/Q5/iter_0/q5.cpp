/*
 * Q5: Local Supplier Volume
 *
 * LOGICAL PLAN:
 * 1. Filter region: r_name = 'ASIA' → 1 region
 * 2. Join region → nation via n_regionkey → ~5 nations in ASIA
 * 3. Filter orders: o_orderdate IN [1994-01-01, 1995-01-01) → ~4.1M orders (using zone map)
 * 4. Join orders → customer via o_custkey (hash index)
 * 5. Join lineitem → orders via l_orderkey (multi-value hash index)
 * 6. Join lineitem → supplier via l_suppkey (hash index)
 * 7. Filter: c_nationkey = s_nationkey = n_nationkey (same nation in ASIA region)
 * 8. Aggregate: GROUP BY n_name, SUM(l_extendedprice * (1 - l_discount))
 * 9. Sort: ORDER BY revenue DESC
 *
 * PHYSICAL PLAN:
 * - Direct scan region (5 rows), filter to ASIA
 * - Direct scan nation (25 rows), build n_nationkey → n_name array
 * - Scan orders with zone map pruning on o_orderdate
 * - Use pre-built hash indexes: customer_custkey, supplier_suppkey, lineitem_orderkey
 * - Small hash aggregation (~5 groups)
 * - Final sort and output
 *
 * DATA STRUCTURES:
 * - Direct array: nation[25] for n_name lookup
 * - Hash set: valid_nations (5 entries for ASIA region)
 * - Hash aggregation: ~5 groups (nations in ASIA)
 *
 * PARALLELISM:
 * - Parallel orders scan with thread-local aggregation
 */

#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <omp.h>

// Helper: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    size_t size = expected_rows * sizeof(T);
    void* addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

// Helper: load string column (length-prefixed binary format)
std::vector<std::string> load_string_column(const std::string& path, size_t expected_rows) {
    std::ifstream file(path, std::ios::binary);
    std::vector<std::string> result;
    result.reserve(expected_rows);

    for (size_t i = 0; i < expected_rows; i++) {
        uint32_t len;
        file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (!file) break;

        std::string str(len, '\0');
        file.read(&str[0], len);
        result.push_back(str);
    }
    return result;
}

// Helper: compute epoch days from date components
int32_t date_to_epoch(int year, int month, int day) {
    // Days since 1970-01-01
    int days = 0;
    // Add days for complete years
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }
    // Add days for complete months in current year
    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap) month_days[1] = 29;
    for (int m = 1; m < month; m++) {
        days += month_days[m - 1];
    }
    // Add remaining days
    days += (day - 1);
    return days;
}

// Zone map structure for orders.o_orderdate
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
};

// Hash index structures
struct CustomerHashEntry {
    int32_t key;
    uint32_t position;
};

struct SupplierHashEntry {
    int32_t key;
    uint32_t position;
};

struct LineitemHashEntry {
    int32_t key;       // l_orderkey
    uint32_t offset;   // offset into positions array
    uint32_t count;    // number of positions for this key
};

// Aggregation key: nation name (string)
struct AggEntry {
    std::string n_name;
    int64_t revenue;
};

// Custom hash function (multiply-shift, avoid identity hash on integers)
inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
}

void run_q5(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Date range: 1994-01-01 to 1994-12-31 (inclusive of full year)
    int32_t date_start = date_to_epoch(1994, 1, 1);
    int32_t date_end = date_to_epoch(1995, 1, 1);  // exclusive

    // ========== STEP 1: Load region and filter to 'ASIA' ==========
#ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t region_rows = 5;
    auto region_r_regionkey = mmap_column<int32_t>(gendb_dir + "/region/r_regionkey.bin", region_rows);
    auto region_r_name = load_string_column(gendb_dir + "/region/r_name.bin", region_rows);

    int32_t asia_regionkey = -1;
    for (size_t i = 0; i < region_rows; i++) {
        if (region_r_name[i] == "ASIA") {
            asia_regionkey = region_r_regionkey[i];
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double ms_region = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] region_scan: %.2f ms\n", ms_region);
#endif

    // ========== STEP 2: Load nation and filter to ASIA region ==========
#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t nation_rows = 25;
    auto nation_n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);
    auto nation_n_regionkey = mmap_column<int32_t>(gendb_dir + "/nation/n_regionkey.bin", nation_rows);
    auto nation_n_name = load_string_column(gendb_dir + "/nation/n_name.bin", nation_rows);

    // Build direct array: nationkey → nation name
    std::string nation_names[25];
    std::unordered_set<int32_t> valid_nations;
    for (size_t i = 0; i < nation_rows; i++) {
        nation_names[nation_n_nationkey[i]] = nation_n_name[i];
        if (nation_n_regionkey[i] == asia_regionkey) {
            valid_nations.insert(nation_n_nationkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double ms_nation = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] nation_scan: %.2f ms\n", ms_nation);
#endif

    // ========== STEP 3: Load zone map for orders.o_orderdate ==========
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif

    int fd_zone = open((gendb_dir + "/indexes/orders_orderdate_zone.bin").c_str(), O_RDONLY);
    if (fd_zone < 0) {
        std::cerr << "Failed to open zone map" << std::endl;
        return;
    }
    uint32_t num_zones;
    read(fd_zone, &num_zones, sizeof(uint32_t));
    std::vector<ZoneMapEntry> zone_map(num_zones);
    read(fd_zone, zone_map.data(), num_zones * sizeof(ZoneMapEntry));
    close(fd_zone);

#ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double ms_zonemap = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zonemap_load: %.2f ms\n", ms_zonemap);
#endif

    // ========== STEP 4: Load customer hash index ==========
#ifdef GENDB_PROFILE
    auto t_cust_idx_start = std::chrono::high_resolution_clock::now();
#endif

    int fd_cust = open((gendb_dir + "/indexes/customer_custkey_hash.bin").c_str(), O_RDONLY);
    if (fd_cust < 0) {
        std::cerr << "Failed to open customer hash index" << std::endl;
        return;
    }
    uint32_t cust_num_entries, cust_table_size;
    read(fd_cust, &cust_num_entries, sizeof(uint32_t));
    read(fd_cust, &cust_table_size, sizeof(uint32_t));
    std::vector<CustomerHashEntry> cust_hash(cust_table_size);
    read(fd_cust, cust_hash.data(), cust_table_size * sizeof(CustomerHashEntry));
    close(fd_cust);

    const size_t customer_rows = 1500000;
    auto customer_c_nationkey = mmap_column<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_rows);

#ifdef GENDB_PROFILE
    auto t_cust_idx_end = std::chrono::high_resolution_clock::now();
    double ms_cust_idx = std::chrono::duration<double, std::milli>(t_cust_idx_end - t_cust_idx_start).count();
    printf("[TIMING] customer_index_load: %.2f ms\n", ms_cust_idx);
#endif

    // ========== STEP 5: Load supplier hash index ==========
#ifdef GENDB_PROFILE
    auto t_supp_idx_start = std::chrono::high_resolution_clock::now();
#endif

    int fd_supp = open((gendb_dir + "/indexes/supplier_suppkey_hash.bin").c_str(), O_RDONLY);
    if (fd_supp < 0) {
        std::cerr << "Failed to open supplier hash index" << std::endl;
        return;
    }
    uint32_t supp_num_entries, supp_table_size;
    read(fd_supp, &supp_num_entries, sizeof(uint32_t));
    read(fd_supp, &supp_table_size, sizeof(uint32_t));
    std::vector<SupplierHashEntry> supp_hash(supp_table_size);
    read(fd_supp, supp_hash.data(), supp_table_size * sizeof(SupplierHashEntry));
    close(fd_supp);

    const size_t supplier_rows = 100000;
    auto supplier_s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_rows);

#ifdef GENDB_PROFILE
    auto t_supp_idx_end = std::chrono::high_resolution_clock::now();
    double ms_supp_idx = std::chrono::duration<double, std::milli>(t_supp_idx_end - t_supp_idx_start).count();
    printf("[TIMING] supplier_index_load: %.2f ms\n", ms_supp_idx);
#endif

    // ========== STEP 6: Load lineitem hash index (multi-value) ==========
#ifdef GENDB_PROFILE
    auto t_line_idx_start = std::chrono::high_resolution_clock::now();
#endif

    int fd_line = open((gendb_dir + "/indexes/lineitem_orderkey_hash.bin").c_str(), O_RDONLY);
    if (fd_line < 0) {
        std::cerr << "Failed to open lineitem hash index" << std::endl;
        return;
    }
    uint32_t line_num_unique, line_table_size;
    read(fd_line, &line_num_unique, sizeof(uint32_t));
    read(fd_line, &line_table_size, sizeof(uint32_t));
    std::vector<LineitemHashEntry> line_hash(line_table_size);
    read(fd_line, line_hash.data(), line_table_size * sizeof(LineitemHashEntry));

    uint32_t line_pos_count;
    read(fd_line, &line_pos_count, sizeof(uint32_t));
    std::vector<uint32_t> line_positions(line_pos_count);
    read(fd_line, line_positions.data(), line_pos_count * sizeof(uint32_t));
    close(fd_line);

    const size_t lineitem_rows = 59986052;
    auto lineitem_l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_rows);
    auto lineitem_l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_rows);
    auto lineitem_l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_rows);

#ifdef GENDB_PROFILE
    auto t_line_idx_end = std::chrono::high_resolution_clock::now();
    double ms_line_idx = std::chrono::duration<double, std::milli>(t_line_idx_end - t_line_idx_start).count();
    printf("[TIMING] lineitem_index_load: %.2f ms\n", ms_line_idx);
#endif

    // ========== STEP 7: Scan orders with zone map pruning and aggregate ==========
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t orders_rows = 15000000;
    const size_t block_size = 100000;
    auto orders_o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_rows);
    auto orders_o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_rows);
    auto orders_o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_rows);

    // Thread-local aggregation maps
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::string, int64_t>> local_aggs(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = local_aggs[tid];

        #pragma omp for schedule(dynamic)
        for (size_t zone_idx = 0; zone_idx < num_zones; zone_idx++) {
            // Zone map pruning
            if (zone_map[zone_idx].max_value < date_start || zone_map[zone_idx].min_value >= date_end) {
                continue;  // Skip this zone
            }

            size_t block_start = zone_idx * block_size;
            size_t block_end = std::min(block_start + block_size, orders_rows);

            for (size_t i = block_start; i < block_end; i++) {
                int32_t o_date = orders_o_orderdate[i];
                if (o_date < date_start || o_date >= date_end) {
                    continue;
                }

                int32_t o_key = orders_o_orderkey[i];
                int32_t c_key = orders_o_custkey[i];

                // Lookup customer in hash index
                uint64_t c_hash = hash_int32(c_key);
                uint32_t c_slot = c_hash & (cust_table_size - 1);
                uint32_t c_pos = UINT32_MAX;
                for (uint32_t probe = 0; probe < cust_table_size; probe++) {
                    uint32_t idx = (c_slot + probe) & (cust_table_size - 1);
                    if (cust_hash[idx].key == c_key) {
                        c_pos = cust_hash[idx].position;
                        break;
                    }
                    if (cust_hash[idx].key == 0 && cust_hash[idx].position == 0) {
                        break;  // Empty slot, key not found
                    }
                }
                if (c_pos == UINT32_MAX) {
                    continue;  // Customer not found
                }

                int32_t c_nation = customer_c_nationkey[c_pos];
                if (valid_nations.find(c_nation) == valid_nations.end()) {
                    continue;  // Customer not in ASIA region
                }

                // Lookup lineitem rows for this order (multi-value)
                uint64_t l_hash = hash_int32(o_key);
                uint32_t l_slot = l_hash & (line_table_size - 1);
                uint32_t l_offset = UINT32_MAX;
                uint32_t l_count = 0;
                for (uint32_t probe = 0; probe < line_table_size; probe++) {
                    uint32_t idx = (l_slot + probe) & (line_table_size - 1);
                    if (line_hash[idx].key == o_key) {
                        l_offset = line_hash[idx].offset;
                        l_count = line_hash[idx].count;
                        break;
                    }
                    if (line_hash[idx].key == 0) {
                        break;  // Empty slot, key not found
                    }
                }
                if (l_offset == UINT32_MAX) {
                    continue;  // No lineitems for this order
                }

                // Process all lineitems for this order
                for (uint32_t j = 0; j < l_count; j++) {
                    uint32_t l_pos = line_positions[l_offset + j];
                    int32_t l_supp = lineitem_l_suppkey[l_pos];

                    // Lookup supplier in hash index
                    uint64_t s_hash = hash_int32(l_supp);
                    uint32_t s_slot = s_hash & (supp_table_size - 1);
                    uint32_t s_pos = UINT32_MAX;
                    for (uint32_t probe = 0; probe < supp_table_size; probe++) {
                        uint32_t idx = (s_slot + probe) & (supp_table_size - 1);
                        if (supp_hash[idx].key == l_supp) {
                            s_pos = supp_hash[idx].position;
                            break;
                        }
                        if (supp_hash[idx].key == 0 && supp_hash[idx].position == 0) {
                            break;
                        }
                    }
                    if (s_pos == UINT32_MAX) {
                        continue;  // Supplier not found
                    }

                    int32_t s_nation = supplier_s_nationkey[s_pos];
                    if (s_nation != c_nation) {
                        continue;  // Supplier and customer must be in same nation
                    }

                    // Compute revenue: l_extendedprice * (1 - l_discount)
                    int64_t price = lineitem_l_extendedprice[l_pos];
                    int64_t discount = lineitem_l_discount[l_pos];
                    int64_t revenue = price * (100 - discount);  // scaled by 100*100 = 10000

                    // Aggregate by nation name
                    std::string n_name = nation_names[c_nation];
                    local_agg[n_name] += revenue;
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_join_aggregate: %.2f ms\n", ms_scan);
#endif

    // ========== STEP 8: Merge local aggregations ==========
#ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<std::string, int64_t> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& kv : local) {
            global_agg[kv.first] += kv.second;
        }
    }

#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double ms_merge = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", ms_merge);
#endif

    // ========== STEP 9: Sort by revenue DESC ==========
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<AggEntry> results;
    results.reserve(global_agg.size());
    for (const auto& kv : global_agg) {
        results.push_back({kv.first, kv.second});
    }
    std::sort(results.begin(), results.end(), [](const AggEntry& a, const AggEntry& b) {
        return a.revenue > b.revenue;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    // ========== STEP 10: Write output ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q5.csv");
    out << "n_name,revenue\n";
    for (const auto& entry : results) {
        // Revenue is scaled by 10000 (100 from price * 100 from discount)
        double revenue_val = entry.revenue / 10000.0;
        out << entry.n_name << "," << std::fixed << std::setprecision(2) << revenue_val << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // Cleanup
    munmap(region_r_regionkey, region_rows * sizeof(int32_t));
    munmap(nation_n_nationkey, nation_rows * sizeof(int32_t));
    munmap(nation_n_regionkey, nation_rows * sizeof(int32_t));
    munmap(customer_c_nationkey, customer_rows * sizeof(int32_t));
    munmap(supplier_s_nationkey, supplier_rows * sizeof(int32_t));
    munmap(orders_o_orderkey, orders_rows * sizeof(int32_t));
    munmap(orders_o_custkey, orders_rows * sizeof(int32_t));
    munmap(orders_o_orderdate, orders_rows * sizeof(int32_t));
    munmap(lineitem_l_suppkey, lineitem_rows * sizeof(int32_t));
    munmap(lineitem_l_extendedprice, lineitem_rows * sizeof(int64_t));
    munmap(lineitem_l_discount, lineitem_rows * sizeof(int64_t));
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q5(gendb_dir, results_dir);
    return 0;
}
#endif
