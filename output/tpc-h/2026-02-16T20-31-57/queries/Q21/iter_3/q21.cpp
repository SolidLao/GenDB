/*
 * Q21: Suppliers Who Kept Orders Waiting
 * TPC-H Query 21 - Iteration 3 (OPTIMIZED)
 *
 * LOGICAL PLAN:
 * 1. Filter nation by n_name = 'SAUDI ARABIA' -> 1 row
 * 2. Filter supplier by s_nationkey (from nation filter) -> ~4K rows
 * 3. Filter orders by o_orderstatus = 'F' -> ~4.5M rows
 * 4. Pre-compute EXISTS check: single-pass scan to find orderkeys with multiple suppliers
 * 5. Pre-compute NOT EXISTS check: single-pass scan to find (orderkey,suppkey) pairs to reject
 * 6. Main scan: Apply all filters (late receipt, valid order, Saudi supplier, EXISTS, NOT EXISTS)
 * 7. Aggregation: COUNT(*) grouped by suppkey -> final result set
 * 8. Decode supplier names and sort by numwait DESC, s_name ASC, limit 100
 *
 * PHYSICAL PLAN:
 * - Three parallel scans of lineitem (60M rows):
 *   a) Pass 1: Find orderkeys with multiple suppliers via hash set of (orderkey,suppkey) pairs
 *   b) Pass 2: Find (orderkey,suppkey) pairs to exclude via rejection set
 *   c) Pass 3: Main filtering with all conditions, parallel aggregation via thread-local buffers
 * - Supplier hash table: keyed by s_suppkey for O(1) lookup
 * - Orders hash set: keyed by o_orderkey for O(1) membership check
 * - Aggregation: Thread-local flat arrays (suppkey -> count), merged at end
 * - Parallelism: OpenMP parallel for all lineitem scans with thread-local results
 * - Join strategy: Hash join via lookup tables (no explicit join operator needed)
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
#include <omp.h>

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

    // ===== SUBQUERY PREPROCESSING: Simplified passes =====
    #ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
    #endif

    // Pass 1: For each orderkey, collect all (suppkey, is_late) pairs
    std::unordered_map<int32_t, std::vector<std::pair<int32_t, bool>>> lines_by_orderkey;

    for (size_t i = 0; i < lineitem_rows; i++) {
        int32_t orderkey = lineitem_l_orderkey[i];
        int32_t suppkey = lineitem_l_suppkey[i];
        bool is_late = lineitem_l_receiptdate[i] > lineitem_l_commitdate[i];
        lines_by_orderkey[orderkey].push_back({suppkey, is_late});
    }

    // Pass 2: Build exists_orderkeys and reject_pairs
    // EXISTS: order has multiple suppkeys
    // NOT EXISTS: for each line, check if there exists another suppkey with late receipt
    std::unordered_set<int32_t> exists_orderkeys;
    std::unordered_set<int64_t> reject_pairs;

    for (const auto& [orderkey, lines] : lines_by_orderkey) {
        // Check if order has multiple suppkeys
        std::unordered_set<int32_t> unique_suppkeys;
        std::unordered_set<int32_t> late_suppkeys;

        for (const auto& [suppkey, is_late] : lines) {
            unique_suppkeys.insert(suppkey);
            if (is_late) {
                late_suppkeys.insert(suppkey);
            }
        }

        // EXISTS condition: multiple suppkeys
        if (unique_suppkeys.size() > 1) {
            exists_orderkeys.insert(orderkey);

            // NOT EXISTS: for each suppkey, check if there's a late receipt from different suppkey
            for (const auto& [suppkey, is_late] : lines) {
                // Check if any OTHER suppkey has late receipt
                bool has_other_late = false;
                for (const auto& [other_suppkey, other_is_late] : lines) {
                    if (other_suppkey != suppkey && other_is_late) {
                        has_other_late = true;
                        break;
                    }
                }

                if (has_other_late) {
                    // This (orderkey, suppkey) pair should be rejected
                    int64_t pair_key = ((int64_t)orderkey << 32) | (uint32_t)suppkey;
                    reject_pairs.insert(pair_key);
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_preprocessing: %.2f ms\n", ms_subquery);
    #endif

    // ===== MAIN SCAN: LINEITEM WITH ALL FILTERS AND JOINS (PARALLEL) =====
    #ifdef GENDB_PROFILE
    auto t_scan_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation buffers (flat array indexed by suppkey for fast access)
    // Max suppkey is 100000, so we'll use an array
    const int MAX_SUPPKEY = 100001;
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<int64_t>> thread_agg(num_threads, std::vector<int64_t>(MAX_SUPPKEY, 0));

    #pragma omp parallel for schedule(static, 100000)
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

        // Filter 4: EXISTS check - there must be another line for this order with different suppkey
        if (exists_orderkeys.find(orderkey) == exists_orderkeys.end()) continue;

        // Filter 5: NOT EXISTS check - reject if this (orderkey, suppkey) pair is in reject set
        int64_t pair_key = ((int64_t)orderkey << 32) | (uint32_t)suppkey;
        if (reject_pairs.find(pair_key) != reject_pairs.end()) continue;

        // All filters passed - increment count for this supplier (thread-local)
        int thread_id = omp_get_thread_num();
        thread_agg[thread_id][suppkey]++;
    }

    // Merge thread-local aggregation buffers
    std::unordered_map<int32_t, int64_t> agg_count;
    for (int t = 0; t < num_threads; t++) {
        for (int32_t suppkey = 0; suppkey < MAX_SUPPKEY; suppkey++) {
            if (thread_agg[t][suppkey] > 0) {
                agg_count[suppkey] += thread_agg[t][suppkey];
            }
        }
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
