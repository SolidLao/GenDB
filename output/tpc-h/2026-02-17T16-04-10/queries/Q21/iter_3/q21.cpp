#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <omp.h>

/*
================================================================================
QUERY PLAN: Q21 - Suppliers Who Kept Orders Waiting
================================================================================

LOGICAL PLAN:
  1. nation: filter by n_name = 'SAUDI ARABIA' → 1 row → n_nationkey = 16
  2. supplier: filter by s_nationkey = 16 → ~4K suppliers
  3. orders: filter by o_orderstatus = 'F' → ~11.25M orders
  4. lineitem l1: filter by l_receiptdate > l_commitdate → ~6M rows
  5. Subquery EXISTS l2: GROUP order keys by supplier count
     - For each (l_orderkey, l_suppkey) pair in l1:
       - Check if there EXISTS another supplier on same orderkey
       - Pre-compute: set of orderkeys with multiple suppliers
  6. Subquery NOT EXISTS l3: Anti-join
     - For each (l_orderkey, l_suppkey) pair in l1:
       - Check if NO other supplier has late delivery on same orderkey
       - Pre-compute: set of (orderkey, suppkey) pairs that should be excluded
  7. GROUP BY s_name, COUNT(*)
  8. ORDER BY numwait DESC, s_name
  9. LIMIT 100

PHYSICAL PLAN:
  1. Scan nation, filter, build hash set on n_nationkey
  2. Scan supplier, filter by nation_key lookup, build hash set
  3. Scan orders, filter by status='F', build hash set
  4. Scan lineitem l1:
     - Filter l_receiptdate > l_commitdate
     - Probe orders hash (o_orderkey)
     - Probe supplier hash (l_suppkey)
     - Fetch s_name via supplier hash
     - Fetch s_nationkey, join with nation
  5. Pre-compute EXISTS subquery:
     - For each l_orderkey in filtered l1, count distinct suppliers
     - Build set: orderkeys with >1 supplier
  6. Pre-compute NOT EXISTS subquery:
     - For each l_orderkey, identify suppliers with late delivery
     - For each l_orderkey, if ANY other supplier has late delivery, exclude current (orderkey, suppkey)
  7. Parallel aggregation: group by s_name, count(*)
  8. Sort by (numwait DESC, s_name ASC)
  9. Output top 100

OPTIMIZATION NOTES:
  - Nation table is tiny (25 rows), filter first for precision
  - Supplier is small (100K) after filtering to single nation (~4K)
  - Orders filter (status='F') is ~75% selectivity
  - Lineitem filter (late delivery) is ~10% selectivity
  - Pre-compute EXISTS and NOT EXISTS using single pass of l1
  - Use unordered_set for semi-join checks (1:N lookups)
  - Parallel aggregation with thread-local hash tables
================================================================================
*/

// Load column from binary file via mmap
template <typename T>
std::vector<T> load_column(const std::string& path, int64_t count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return {};
    }
    T* data = (T*)mmap(nullptr, count * sizeof(T), PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Error mmapping " << path << std::endl;
        close(fd);
        return {};
    }
    std::vector<T> result(data, data + count);
    munmap(data, count * sizeof(T));
    close(fd);
    return result;
}

// Load string column from binary file (format: [len(uint32_t), data...] repeated)
std::vector<std::string> load_string_column(const std::string& path, int64_t count = -1) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        std::cerr << "Error opening " << path << std::endl;
        return {};
    }
    std::vector<std::string> result;
    uint32_t len;
    while (f.read((char*)&len, sizeof(uint32_t))) {
        char* buf = new char[len + 1];
        f.read(buf, len);
        buf[len] = '\0';
        result.push_back(std::string(buf));
        delete[] buf;
        if (count > 0 && (int64_t)result.size() >= count) break;
    }
    return result;
}

// Load dictionary from *_dict.txt (format: "code=value\ncode=value\n...")
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f) return dict;
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days_since_epoch) {
    // Count days from 1970-01-01
    int year = 1970;
    int month = 1;
    int day = 1;

    // Rough approximation for fast conversion
    // This is a simplified approach; exact day-to-date conversion is complex
    // For now, we just use the epoch days directly if needed

    char buf[11];
    snprintf(buf, 11, "%d-%02d-%02d", year + days_since_epoch / 365, month, day);
    return std::string(buf);
}

void run_q21(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========== LOAD DATA ==========
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    auto nation_n_nationkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/nation/n_nationkey.bin", 25);
    auto nation_n_name = load_string_column(gendb_dir + "/../tpch_sf10.gendb/nation/n_name.bin", 25);

    auto supplier_s_suppkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/supplier/s_suppkey.bin", 100000);
    auto supplier_s_nationkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/supplier/s_nationkey.bin", 100000);
    auto supplier_s_name = load_string_column(gendb_dir + "/../tpch_sf10.gendb/supplier/s_name.bin", 100000);

    auto orders_o_orderkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/orders/o_orderkey.bin", 15000000);
    auto orders_o_orderstatus = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/orders/o_orderstatus.bin", 15000000);
    auto orderstatus_dict = load_dictionary(gendb_dir + "/../tpch_sf10.gendb/orders/o_orderstatus_dict.txt");

    auto lineitem_l_orderkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/lineitem/l_orderkey.bin", 59986052);
    auto lineitem_l_suppkey = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/lineitem/l_suppkey.bin", 59986052);
    auto lineitem_l_commitdate = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/lineitem/l_commitdate.bin", 59986052);
    auto lineitem_l_receiptdate = load_column<int32_t>(gendb_dir + "/../tpch_sf10.gendb/lineitem/l_receiptdate.bin", 59986052);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // ========== STEP 1: FILTER NATION ==========
    #ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t saudi_nationkey = -1;
    for (int i = 0; i < 25; i++) {
        if (nation_n_name[i] == "SAUDI ARABIA") {
            saudi_nationkey = nation_n_nationkey[i];
            break;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double nation_ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] filter_nation: %.2f ms\n", nation_ms);
    #endif

    // ========== STEP 2: FILTER SUPPLIER BY NATION ==========
    #ifdef GENDB_PROFILE
    auto t_supp_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_set<int32_t> supplier_set;
    for (int i = 0; i < 100000; i++) {
        if (supplier_s_nationkey[i] == saudi_nationkey) {
            supplier_set.insert(supplier_s_suppkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_supp_end = std::chrono::high_resolution_clock::now();
    double supp_ms = std::chrono::duration<double, std::milli>(t_supp_end - t_supp_start).count();
    printf("[TIMING] filter_supplier: %.2f ms (kept %zu suppliers)\n", supp_ms, supplier_set.size());
    #endif

    // ========== STEP 3: FILTER ORDERS BY STATUS ==========
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_set<int32_t> orders_f_set;
    int32_t f_code = -1;
    for (auto& [code, val] : orderstatus_dict) {
        if (val == "F") {
            f_code = code;
            break;
        }
    }

    for (int64_t i = 0; i < 15000000; i++) {
        if (orders_o_orderstatus[i] == f_code) {
            orders_f_set.insert(orders_o_orderkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (kept %zu orders)\n", orders_ms, orders_f_set.size());
    #endif

    // ========== STEP 4: PRE-COMPUTE EXISTS & NOT EXISTS SUBQUERIES ==========
    // OPTIMIZATION: Single-pass approach with inline deduplication
    // - EXISTS: Count distinct suppliers per orderkey in ONE pass
    // - NOT EXISTS: Find orders where MULTIPLE suppliers had late delivery in ONE pass
    #ifdef GENDB_PROFILE
    auto t_subq_start = std::chrono::high_resolution_clock::now();
    #endif

    // EXISTS: Build map: orderkey → set of distinct suppliers (to detect >1 supplier)
    std::unordered_map<int32_t, std::unordered_set<int32_t>> all_suppkeys_map;
    all_suppkeys_map.reserve(8000000);  // Pre-size to reduce rehashing

    for (int64_t i = 0; i < 59986052; i++) {
        int32_t ok = lineitem_l_orderkey[i];
        int32_t sk = lineitem_l_suppkey[i];
        all_suppkeys_map[ok].insert(sk);  // Inline deduplication via set
    }

    // NOT EXISTS: Build map: orderkey → set of distinct late suppliers
    std::unordered_map<int32_t, std::unordered_set<int32_t>> late_suppkeys_map;
    late_suppkeys_map.reserve(8000000);  // Pre-size to reduce rehashing

    for (int64_t i = 0; i < 59986052; i++) {
        int32_t receipt = lineitem_l_receiptdate[i];
        int32_t commit = lineitem_l_commitdate[i];
        if (receipt > commit) {
            int32_t ok = lineitem_l_orderkey[i];
            int32_t sk = lineitem_l_suppkey[i];
            late_suppkeys_map[ok].insert(sk);  // Inline deduplication
        }
    }

    // Build result structures
    std::unordered_set<int32_t> exists_orders;
    std::unordered_set<int64_t> not_exists_pairs;

    // EXISTS: Orderkeys with >1 distinct supplier
    for (auto& [ok, suppkeys] : all_suppkeys_map) {
        if (suppkeys.size() > 1) {
            exists_orders.insert(ok);
        }
    }

    // NOT EXISTS: Pairs where there are MULTIPLE distinct late suppliers on the same orderkey
    for (auto& [ok, late_suppkeys] : late_suppkeys_map) {
        if (late_suppkeys.size() > 1) {
            // Mark all these (orderkey, suppkey) pairs for exclusion
            for (int32_t sk : late_suppkeys) {
                int64_t pair = ((int64_t)ok << 32) | (sk & 0xFFFFFFFF);
                not_exists_pairs.insert(pair);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subq_end = std::chrono::high_resolution_clock::now();
    double subq_ms = std::chrono::duration<double, std::milli>(t_subq_end - t_subq_start).count();
    printf("[TIMING] precompute_subqueries: %.2f ms (exists=%zu, not_exists=%zu)\n",
           subq_ms, exists_orders.size(), not_exists_pairs.size());
    #endif

    // ========== STEP 5: BUILD SUPPLIER NAME LOOKUP ==========
    std::unordered_map<int32_t, std::string> suppkey_to_name;
    for (int i = 0; i < 100000; i++) {
        suppkey_to_name[supplier_s_suppkey[i]] = supplier_s_name[i];
    }

    // ========== STEP 6: SCAN LINEITEM WITH ALL FILTERS ==========
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<std::string, int>> filtered_results;

    for (int64_t i = 0; i < 59986052; i++) {
        int32_t l_orderkey = lineitem_l_orderkey[i];
        int32_t l_suppkey = lineitem_l_suppkey[i];
        int32_t l_receiptdate = lineitem_l_receiptdate[i];
        int32_t l_commitdate = lineitem_l_commitdate[i];

        // Filter 1: late delivery
        if (l_receiptdate <= l_commitdate) continue;

        // Filter 2: order has status 'F'
        if (!orders_f_set.count(l_orderkey)) continue;

        // Filter 3: supplier is in Saudi Arabia
        if (!supplier_set.count(l_suppkey)) continue;

        // Filter 4: EXISTS - order has another supplier
        if (!exists_orders.count(l_orderkey)) continue;

        // Filter 5: NOT EXISTS - no other supplier has late delivery
        int64_t pair = ((int64_t)l_orderkey << 32) | (l_suppkey & 0xFFFFFFFF);
        if (not_exists_pairs.count(pair)) continue;

        // Get supplier name
        auto it = suppkey_to_name.find(l_suppkey);
        if (it != suppkey_to_name.end()) {
            filtered_results.push_back({it->second, 1});
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms (passed %zu rows)\n", scan_ms, filtered_results.size());
    #endif

    // ========== STEP 7: AGGREGATION - GROUP BY s_name ==========
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<std::string, int> agg_map;
    for (auto& [s_name, cnt] : filtered_results) {
        agg_map[s_name] += cnt;
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms (%zu groups)\n", agg_ms, agg_map.size());
    #endif

    // ========== STEP 8: SORT BY (numwait DESC, s_name ASC) ==========
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<std::string, int>> sorted_results;
    for (auto& [name, cnt] : agg_map) {
        sorted_results.push_back({name, cnt});
    }

    std::sort(sorted_results.begin(), sorted_results.end(),
        [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
            if (a.second != b.second) return a.second > b.second;  // numwait DESC
            return a.first < b.first;  // s_name ASC
        });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    // ========== STEP 9: OUTPUT (LIMIT 100) ==========
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q21.csv";
    std::ofstream out(output_path);
    out << "s_name,numwait\n";

    int count = 0;
    for (auto& [s_name, numwait] : sorted_results) {
        if (count >= 100) break;
        out << s_name << "," << numwait << "\n";
        count++;
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q21(gendb_dir, results_dir);
    return 0;
}
#endif
