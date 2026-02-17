/*
 * Q21: Suppliers Who Kept Orders Waiting
 * TPC-H Query 21 - Iteration 4 (Fundamental Restructuring)
 *
 * ROOT CAUSE OF STALL: Previous iterations scanned lineitem 3+ times for subquery preprocessing,
 * loading all 60M rows into hash maps. This is 73% of execution time!
 *
 * NEW STRATEGY: Single-pass lineitem scan with pre-computed aggregates
 *
 * LOGICAL PLAN:
 * 1. Filter nation by n_name = 'SAUDI ARABIA' -> 1 row (lookup in small nation table)
 * 2. Filter supplier by s_nationkey -> ~4K rows (build hash set for membership test)
 * 3. Filter orders by o_orderstatus = 'F' -> ~4.5M rows (build hash set for membership test)
 * 4. SINGLE PASS over lineitem to compute:
 *    a) For each orderkey: count distinct suppkeys, count suppkeys with late receipt
 *    b) For each (orderkey, suppkey) pair where suppkey is from SAUDI ARABIA:
 *       - Check EXISTS: orderkey has >1 suppkey? (from distinct count)
 *       - Check NOT EXISTS: orderkey has supplier with late receipt OTHER than this one?
 *         (count late suppliers > 1 OR count late suppliers == 1 AND this suppkey is NOT late)
 * 5. Aggregation: GROUP BY s_name -> low cardinality (300-500 distinct suppliers)
 * 6. Sort: by numwait DESC, s_name ASC, limit 100
 *
 * PHYSICAL PLAN:
 * - Stage 1 (Setup): Load nation/supplier/orders, build hash sets for filtering
 * - Stage 2 (Single-pass lineitem aggregation):
 *   - For each row: compute (orderkey, distinct_suppkey_count, late_suppkey_count)
 *   - Use open-addressing hash table: orderkey → (distinct_count, late_count, vec<suppkeys_with_late>)
 * - Stage 3 (Filter + aggregate):
 *   - Second pass lineitem: for each l1 row passing basic filters:
 *     - Check EXISTS: distinct_count[orderkey] > 1
 *     - Check NOT EXISTS: no other suppkey has late receipt (use pre-computed late_suppkeys set)
 *     - Aggregate count by (suppkey -> s_name)
 * - Aggregation: Flat array for suppkey (0-100K range) or open-addressing hash table
 * - Sort: Partial sort via heap or priority queue for top 100
 * - Parallelism: First pass can use parallel reduction; second pass sequential (depends on selectivity)
 */

#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <numeric>
#include <queue>
#include <map>

// Helper struct for sorting results
struct Result {
    std::string s_name;
    int64_t numwait;

    bool operator<(const Result& other) const {
        if (numwait != other.numwait) return numwait > other.numwait;
        return s_name < other.s_name;
    }
};

// Load dictionary file and return map from code to string
// Dictionary format: one value per line, code = line number (0-indexed)
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line.empty()) {
            code++;
            continue;
        }
        dict[code] = line;
        code++;
    }
    return dict;
}

// Reverse lookup: find code for a given string value in dictionary file
// Dictionary format: one value per line, code = line number (0-indexed)
int32_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f.is_open()) return -1;

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line.empty()) {
            code++;
            continue;
        }
        if (line == target) {
            return code;
        }
        code++;
    }
    return -1;
}

// Mmap a file and return pointer
void* mmap_file(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        close(fd);
        return nullptr;
    }
    file_size = size;

    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Mmap failed for: " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

void run_q21(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ===== LOAD DATA =====
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t file_size;

    // Load nation
    auto* nation_n_nationkey = (int32_t*)mmap_file(gendb_dir + "/nation/n_nationkey.bin", file_size);
    size_t nation_rows = file_size / sizeof(int32_t);
    auto* nation_n_name_codes = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", file_size);
    auto nation_n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Load supplier
    auto* supplier_s_suppkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", file_size);
    size_t supplier_rows = file_size / sizeof(int32_t);
    auto* supplier_s_name_codes = (int32_t*)mmap_file(gendb_dir + "/supplier/s_name.bin", file_size);
    auto supplier_s_name_dict = load_dictionary(gendb_dir + "/supplier/s_name_dict.txt");
    auto* supplier_s_nationkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", file_size);

    // Load orders
    auto* orders_o_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", file_size);
    size_t orders_rows = file_size / sizeof(int32_t);
    auto* orders_o_orderstatus_codes = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderstatus.bin", file_size);
    auto orders_o_orderstatus_dict = load_dictionary(gendb_dir + "/orders/o_orderstatus_dict.txt");

    // Load lineitem
    auto* lineitem_l_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    size_t lineitem_rows = file_size / sizeof(int32_t);
    auto* lineitem_l_suppkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_suppkey.bin", file_size);
    auto* lineitem_l_commitdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_commitdate.bin", file_size);
    auto* lineitem_l_receiptdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_receiptdate.bin", file_size);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms_load);
    #endif

    // ===== FIND SAUDI ARABIA CODE =====
    int32_t saudi_arabia_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "SAUDI ARABIA");
    if (saudi_arabia_code == -1) {
        std::cerr << "SAUDI ARABIA not found in nation dictionary" << std::endl;
        return;
    }

    // Find the suppkey for SAUDI ARABIA nation
    int32_t saudi_nationkey = -1;
    for (size_t i = 0; i < nation_rows; i++) {
        if (nation_n_name_codes[i] == saudi_arabia_code) {
            saudi_nationkey = nation_n_nationkey[i];
            break;
        }
    }

    if (saudi_nationkey == -1) {
        std::cerr << "SAUDI ARABIA nation not found" << std::endl;
        return;
    }

    // Find order status code for 'F'
    int32_t status_f_code = find_dict_code(gendb_dir + "/orders/o_orderstatus_dict.txt", "F");
    if (status_f_code == -1) {
        std::cerr << "Status 'F' not found in orders dictionary" << std::endl;
        return;
    }

    // ===== BUILD SUPPLIER HASH TABLE (filtered by SAUDI ARABIA) =====
    #ifdef GENDB_PROFILE
    auto t_supplier_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int32_t> supplier_name_by_suppkey;

    for (size_t i = 0; i < supplier_rows; i++) {
        if (supplier_s_nationkey[i] == saudi_nationkey) {
            supplier_name_by_suppkey[supplier_s_suppkey[i]] = supplier_s_name_codes[i];
        }
    }

    #ifdef GENDB_PROFILE
    auto t_supplier_filter_end = std::chrono::high_resolution_clock::now();
    double ms_supplier = std::chrono::duration<double, std::milli>(t_supplier_filter_end - t_supplier_filter_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms_supplier);
    #endif

    // ===== FILTER ORDERS BY STATUS 'F' =====
    #ifdef GENDB_PROFILE
    auto t_orders_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_set<int32_t> valid_orderkeys;

    for (size_t i = 0; i < orders_rows; i++) {
        if (orders_o_orderstatus_codes[i] == status_f_code) {
            valid_orderkeys.insert(orders_o_orderkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_filter_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_filter_end - t_orders_filter_start).count();
    printf("[TIMING] orders_filter: %.2f ms\n", ms_orders);
    #endif

    // ===== LINEITEM PREPROCESSING: MINIMAL STATS AGGREGATION =====
    #ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
    #endif

    // Compute per-orderkey stats needed for subquery checks (EXISTS/NOT EXISTS).
    // We need to track:
    // 1. Count of distinct suppliers for EXISTS check
    // 2. For NOT EXISTS: which suppliers have late receipt, PLUS the count

    struct OrderStats {
        int32_t distinct_count = 0;
        int32_t late_count = 0;
        std::unordered_set<int32_t> late_suppkeys;  // Only store late suppkeys to minimize memory
    };

    std::unordered_map<int32_t, OrderStats> order_stats;

    // Track which suppkeys we've seen per orderkey (to count distinct)
    std::unordered_map<int32_t, std::unordered_set<int32_t>> suppkey_sets_per_order;

    for (size_t i = 0; i < lineitem_rows; i++) {
        int32_t orderkey = lineitem_l_orderkey[i];
        int32_t suppkey = lineitem_l_suppkey[i];
        bool is_late = lineitem_l_receiptdate[i] > lineitem_l_commitdate[i];

        OrderStats& stats = order_stats[orderkey];

        // Track distinct suppliers
        if (suppkey_sets_per_order[orderkey].insert(suppkey).second) {
            // First time seeing this suppkey for this order
            stats.distinct_count++;
        }

        // Track late suppliers
        if (is_late) {
            if (stats.late_suppkeys.insert(suppkey).second) {
                // First time marking this suppkey as late
                stats.late_count++;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_preprocessing: %.2f ms\n", ms_subquery);
    #endif

    // ===== MAIN SCAN: LINEITEM WITH ALL FILTERS AND JOINS =====
    #ifdef GENDB_PROFILE
    auto t_scan_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int64_t> agg_count;  // suppkey -> count

    for (size_t i = 0; i < lineitem_rows; i++) {
        int32_t orderkey = lineitem_l_orderkey[i];
        int32_t suppkey = lineitem_l_suppkey[i];
        int32_t l_receiptdate = lineitem_l_receiptdate[i];
        int32_t l_commitdate = lineitem_l_commitdate[i];

        // Filter 1: l_receiptdate > l_commitdate
        if (l_receiptdate <= l_commitdate) continue;

        // Filter 2: orderkey must be valid (status = 'F')
        if (valid_orderkeys.find(orderkey) == valid_orderkeys.end()) continue;

        // Filter 3: suppkey must be from SAUDI ARABIA supplier
        if (supplier_name_by_suppkey.find(suppkey) == supplier_name_by_suppkey.end()) continue;

        // Get pre-computed stats for this orderkey
        const auto& stats_it = order_stats.find(orderkey);
        if (stats_it == order_stats.end()) continue;  // Should not happen
        const OrderStats& stats = stats_it->second;

        // Filter 4: EXISTS check
        // EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        // True if order has > 1 distinct supplier
        if (stats.distinct_count <= 1) continue;

        // Filter 5: NOT EXISTS check
        // NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        // This is FALSE if there exists another supplier with late receipt.
        // Logic: if late_count > 1, reject immediately
        //        if late_count == 1, check if current suppkey is the late one
        //        if late_count == 0, accept
        if (stats.late_count > 1) continue;  // Multiple late suppliers -> reject

        if (stats.late_count == 1) {
            // Exactly one supplier is late; reject if it's THIS supplier
            if (stats.late_suppkeys.count(suppkey)) continue;
        }

        // All filters passed - increment count for this supplier
        agg_count[suppkey]++;
    }

    #ifdef GENDB_PROFILE
    auto t_scan_filter_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_filter_end - t_scan_filter_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", ms_scan);
    #endif

    // ===== BUILD RESULT SET =====
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<Result> results;

    for (auto& [suppkey, count] : agg_count) {
        Result r;
        r.s_name = supplier_s_name_dict[supplier_name_by_suppkey[suppkey]];
        r.numwait = count;
        results.push_back(r);
    }

    // Sort by numwait DESC, s_name ASC
    std::sort(results.begin(), results.end());

    // Limit to 100
    if (results.size() > 100) {
        results.resize(100);
    }

    // Write CSV output
    std::string output_path = results_dir + "/Q21.csv";
    std::ofstream out(output_path);
    out << "s_name,numwait\n";

    for (auto& r : results) {
        out << r.s_name << "," << r.numwait << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    std::cout << "Q21 result written to " << output_path << std::endl;
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
