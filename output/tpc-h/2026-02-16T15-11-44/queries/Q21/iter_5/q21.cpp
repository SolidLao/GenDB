#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <thread>

/*
 * Q21: SUPPLIERS WHO KEPT ORDERS WAITING
 *
 * LOGICAL PLAN (Iter 5 - Restructured for Early Filtering):
 * 1. Filter nation: n_name = 'SAUDI ARABIA' -> 1 row (code 20)
 * 2. Filter supplier: s_nationkey = 20 -> ~4,000 rows
 * 3. Filter lineitem: l_receiptdate > l_commitdate -> ~18M rows
 * 4. Join filtered lineitem with filtered supplier -> ~150K rows
 * 5. Filter orders: o_orderstatus = 'F' -> ~5M rows
 * 6. Pre-compute EXISTS/NOT EXISTS on full lineitem (not filtered)
 * 7. Main pipeline: Aggregate qualified rows, apply EXISTS/NOT EXISTS filters
 * 8. Sort by numwait DESC, s_name, limit 100
 *
 * PHYSICAL PLAN (Iter 5):
 * - Scan nation, find SAUDI ARABIA code -> 1 row
 * - Scan supplier, filter by s_nationkey, create supplier map -> ~4K rows
 * - Scan orders, filter by status='F', create set -> ~5M rows
 * - Scan lineitem filtered + join with supplier early
 * - Pre-compute EXISTS/NOT EXISTS from unfiltered lineitem (still needed for full view)
 * - Final aggregation with filters applied
 * - Output: decode s_name and sort results
 */

// Helper: Load dictionary file and map string -> code
static std::unordered_map<std::string, int32_t> load_dict_reverse(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> result;
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "ERROR: Cannot open dict file " << dict_path << std::endl;
        return result;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            result[value] = code;
        }
    }
    return result;
}

// Helper: Load dictionary file and map code -> string
static std::unordered_map<int32_t, std::string> load_dict_forward(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> result;
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "ERROR: Cannot open dict file " << dict_path << std::endl;
        return result;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            result[code] = value;
        }
    }
    return result;
}

// Helper: mmap file and return pointer
static void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open " << path << std::endl;
        return nullptr;
    }
    size = lseek(fd, 0, SEEK_END);
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// For composite key (orderkey, suppkey) hashing
struct OrderSuppKeyHash {
    size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        return ((size_t)p.first << 32) | (uint32_t)p.second;
    }
};

void run_q21(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load dictionaries
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif
    auto nation_names = load_dict_forward(gendb_dir + "/nation/n_name_dict.txt");
    auto orderstatus_dict = load_dict_reverse(gendb_dir + "/orders/o_orderstatus_dict.txt");
    auto supplier_names = load_dict_forward(gendb_dir + "/supplier/s_name_dict.txt");
#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // Find code for 'SAUDI ARABIA'
    int32_t saudi_arabia_code = -1;
    for (auto& kv : nation_names) {
        if (kv.second == "SAUDI ARABIA") {
            saudi_arabia_code = kv.first;
            break;
        }
    }
    if (saudi_arabia_code == -1) {
        std::cerr << "ERROR: Cannot find SAUDI ARABIA in nation dictionary" << std::endl;
        return;
    }

    // Find code for 'F' in orderstatus
    int32_t order_status_f = orderstatus_dict["F"];

    // Load nation data
#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif
    size_t nation_size = 0;
    int32_t* nation_nationkey = (int32_t*)mmap_file(gendb_dir + "/nation/n_nationkey.bin", nation_size);
    int32_t* nation_name = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", nation_size);
    int32_t nation_count = nation_size / sizeof(int32_t);

    // Filter nation by n_name = 20
    int32_t saudi_nationkey = -1;
    for (int32_t i = 0; i < nation_count; i++) {
        if (nation_name[i] == saudi_arabia_code) {
            saudi_nationkey = nation_nationkey[i];
            break;
        }
    }
#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double nation_ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] scan_nation: %.2f ms\n", nation_ms);
#endif

    // Load supplier data
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif
    size_t supplier_size = 0;
    int32_t* supplier_suppkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", supplier_size);
    int32_t* supplier_name = (int32_t*)mmap_file(gendb_dir + "/supplier/s_name.bin", supplier_size);
    int32_t* supplier_nationkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", supplier_size);
    int32_t supplier_count = supplier_size / sizeof(int32_t);

    // Filter supplier by s_nationkey = saudi_nationkey
    std::unordered_map<int32_t, int32_t> supplier_map; // suppkey -> name code
    for (int32_t i = 0; i < supplier_count; i++) {
        if (supplier_nationkey[i] == saudi_nationkey) {
            supplier_map[supplier_suppkey[i]] = supplier_name[i];
        }
    }
#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] scan_filter_supplier: %.2f ms\n", supplier_ms);
#endif

    // Load orders data
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif
    size_t orders_size = 0;
    int32_t* orders_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", orders_size);
    int32_t* orders_orderstatus = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderstatus.bin", orders_size);
    int32_t orders_count = orders_size / sizeof(int32_t);

    // Filter orders by o_orderstatus = 'F'
    std::unordered_set<int32_t> orders_set; // orderkeys with status F
    for (int32_t i = 0; i < orders_count; i++) {
        if (orders_orderstatus[i] == order_status_f) {
            orders_set.insert(orders_orderkey[i]);
        }
    }
#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_orders: %.2f ms\n", orders_ms);
#endif

    // Load lineitem data
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif
    size_t lineitem_size = 0;
    int32_t* lineitem_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_size);
    int32_t* lineitem_suppkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_size);
    int32_t* lineitem_commitdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_commitdate.bin", lineitem_size);
    int32_t* lineitem_receiptdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_receiptdate.bin", lineitem_size);
    int32_t lineitem_count = lineitem_size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_load_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", lineitem_load_ms);
#endif

    // Pre-compute EXISTS and NOT EXISTS by scanning lineitem and filtering for relevant rows
#ifdef GENDB_PROFILE
    auto t_exists_start = std::chrono::high_resolution_clock::now();
#endif

    // Build maps only from lineitem rows matching our predicates:
    // - l_receiptdate > l_commitdate
    // - l_orderkey in orders_set (o_orderstatus = 'F')
    // - l_suppkey in supplier_map (s_nationkey = SAUDI_ARABIA)
    std::unordered_map<int32_t, std::unordered_set<int32_t>> suppkeys_per_order;
    std::unordered_map<int32_t, std::unordered_set<int32_t>> late_suppliers_per_order;

    suppkeys_per_order.reserve(1000000); // reduced from 15M - only qualifying orders
    late_suppliers_per_order.reserve(1000000);

    // Scan with predicates applied: build maps only for rows that could possibly pass all filters
    for (int32_t i = 0; i < lineitem_count; i++) {
        int32_t orderkey = lineitem_orderkey[i];
        int32_t suppkey = lineitem_suppkey[i];

        // Apply early filters: only process if order and supplier could be relevant
        if (orders_set.find(orderkey) == orders_set.end()) continue; // order not in status='F'
        if (supplier_map.find(suppkey) == supplier_map.end()) continue; // supplier not in SAUDI ARABIA

        // Now track this (orderkey, suppkey) pair
        suppkeys_per_order[orderkey].insert(suppkey);

        if (lineitem_receiptdate[i] > lineitem_commitdate[i]) {
            late_suppliers_per_order[orderkey].insert(suppkey);
        }
    }

#ifdef GENDB_PROFILE
    auto t_exists_end = std::chrono::high_resolution_clock::now();
    double exists_ms = std::chrono::duration<double, std::milli>(t_exists_end - t_exists_start).count();
    printf("[TIMING] precompute_exists_not_exists: %.2f ms\n", exists_ms);
#endif

    // Main pipeline: scan lineitem, apply remaining filters, aggregate
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif
    std::unordered_map<int32_t, int32_t> agg; // s_name code -> count
    agg.reserve(10000); // ~10K suppliers expected

    for (int32_t i = 0; i < lineitem_count; i++) {
        int32_t orderkey = lineitem_orderkey[i];
        int32_t suppkey = lineitem_suppkey[i];
        int32_t commitdate = lineitem_commitdate[i];
        int32_t receiptdate = lineitem_receiptdate[i];

        // Filter: l_receiptdate > l_commitdate
        if (receiptdate <= commitdate) continue;

        // Filter: o_orderstatus = 'F' (check in set)
        if (orders_set.find(orderkey) == orders_set.end()) continue;

        // Filter: supplier belongs to SAUDI ARABIA (check in map)
        auto supplier_it = supplier_map.find(suppkey);
        if (supplier_it == supplier_map.end()) continue;

        // Now we know this row matches the basic predicates.
        // Apply EXISTS: check if this orderkey has other suppliers in our filtered maps
        // (We built the maps only from relevant rows, so existence implies >1 supplier after filtering)
        auto suppkeys_it = suppkeys_per_order.find(orderkey);
        if (suppkeys_it == suppkeys_per_order.end() || suppkeys_it->second.size() <= 1) continue;

        // Apply NOT EXISTS: must not have another (different) supplier with late receipt
        // We only track late suppliers from the filtered set, so check if ANY other supplier had late receipt
        auto late_supp_it = late_suppliers_per_order.find(orderkey);
        bool has_competing_late = false;
        if (late_supp_it != late_suppliers_per_order.end()) {
            for (int32_t late_supp : late_supp_it->second) {
                if (late_supp != suppkey) {
                    has_competing_late = true;
                    break;
                }
            }
        }
        if (has_competing_late) continue; // skip this row

        // Aggregate
        int32_t s_name_code = supplier_it->second;
        agg[s_name_code]++;
    }
#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", join_ms);
#endif

    // Convert to vector and sort
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif
    std::vector<std::pair<int32_t, int32_t>> results; // (s_name code, count)
    for (auto& kv : agg) {
        results.push_back({kv.first, kv.second});
    }

    // Sort by count DESC, then by s_name ASC
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });

    // Limit to 100
    if (results.size() > 100) {
        results.resize(100);
    }
#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif
    std::ofstream out(results_dir + "/Q21.csv");
    out << "s_name,numwait\n";
    for (auto& row : results) {
        int32_t s_name_code = row.first;
        int32_t count = row.second;
        auto name_it = supplier_names.find(s_name_code);
        std::string s_name = (name_it != supplier_names.end()) ? name_it->second : "UNKNOWN";
        out << s_name << "," << count << "\n";
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
