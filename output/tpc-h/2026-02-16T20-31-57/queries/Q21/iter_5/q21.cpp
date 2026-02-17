/*
 * Q21: Suppliers Who Kept Orders Waiting - Optimized Version
 *
 * KEY INSIGHT: The bottleneck is storing all suppkeys per orderkey.
 * NEW PLAN: Single-pass lineitem scan with on-the-fly subquery validation.
 *
 * Algorithm:
 * 1. Filter suppliers by nation = SAUDI ARABIA
 * 2. Filter orders by status = 'F'
 * 3. Single-pass lineitem scan:
 *    - Apply l_receiptdate > l_commitdate filter
 *    - For each qualifying row, validate EXISTS/NOT EXISTS on-the-fly
 *    - Count matching rows per suppkey
 * 4. Aggregate and output top 100
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

struct Result {
    std::string s_name;
    int64_t numwait;

    bool operator<(const Result& other) const {
        if (numwait != other.numwait) return numwait > other.numwait;
        return s_name < other.s_name;
    }
};

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

    // Load supplier
    auto* supplier_s_suppkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", file_size);
    size_t supplier_rows = file_size / sizeof(int32_t);
    auto* supplier_s_name_codes = (int32_t*)mmap_file(gendb_dir + "/supplier/s_name.bin", file_size);
    auto supplier_s_name_dict = load_dictionary(gendb_dir + "/supplier/s_name_dict.txt");
    auto* supplier_s_nationkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", file_size);

    // Load nation
    auto* nation_n_nationkey = (int32_t*)mmap_file(gendb_dir + "/nation/n_nationkey.bin", file_size);
    size_t nation_rows = file_size / sizeof(int32_t);
    auto* nation_n_name_codes = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", file_size);
    auto nation_n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

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

    // ===== FIND CODES =====
    int32_t saudi_arabia_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "SAUDI ARABIA");
    int32_t saudi_nationkey = -1;
    for (size_t i = 0; i < nation_rows; i++) {
        if (nation_n_name_codes[i] == saudi_arabia_code) {
            saudi_nationkey = nation_n_nationkey[i];
            break;
        }
    }

    int32_t status_f_code = find_dict_code(gendb_dir + "/orders/o_orderstatus_dict.txt", "F");
    if (status_f_code == -1 || saudi_nationkey == -1) {
        std::cerr << "Required codes not found" << std::endl;
        return;
    }

    // ===== BUILD SUPPLIER MAP (filtered by SAUDI ARABIA) =====
    #ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int32_t> supplier_name_by_suppkey;
    for (size_t i = 0; i < supplier_rows; i++) {
        if (supplier_s_nationkey[i] == saudi_nationkey) {
            supplier_name_by_suppkey[supplier_s_suppkey[i]] = supplier_s_name_codes[i];
        }
    }

    #ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double ms_supplier = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms_supplier);
    #endif

    // ===== BUILD ORDERS MAP (filtered by status='F') =====
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_set<int32_t> valid_orderkeys;
    for (size_t i = 0; i < orders_rows; i++) {
        if (orders_o_orderstatus_codes[i] == status_f_code) {
            valid_orderkeys.insert(orders_o_orderkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_filter: %.2f ms\n", ms_orders);
    #endif

    // ===== LINEITEM PREPROCESSING: Parallel scan to build suppkey sets =====
    #ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
    #endif

    // Mutex-protected maps for parallel insertion
    std::unordered_map<int32_t, std::unordered_set<int32_t>> suppkeys_by_ok;
    std::unordered_map<int32_t, std::unordered_set<int32_t>> late_suppkeys_by_ok;

    // Parallel scan with thread-local aggregation
    #pragma omp parallel
    {
        std::unordered_map<int32_t, std::unordered_set<int32_t>> local_suppkeys;
        std::unordered_map<int32_t, std::unordered_set<int32_t>> local_late_suppkeys;

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_rows; i++) {
            int32_t orderkey = lineitem_l_orderkey[i];
            if (valid_orderkeys.find(orderkey) == valid_orderkeys.end()) continue;

            int32_t suppkey = lineitem_l_suppkey[i];
            bool is_late = lineitem_l_receiptdate[i] > lineitem_l_commitdate[i];

            local_suppkeys[orderkey].insert(suppkey);
            if (is_late) {
                local_late_suppkeys[orderkey].insert(suppkey);
            }
        }

        // Merge local maps into global
        #pragma omp critical
        {
            for (auto& [ok, sks] : local_suppkeys) {
                for (int32_t sk : sks) {
                    suppkeys_by_ok[ok].insert(sk);
                }
            }
            for (auto& [ok, late_sks] : local_late_suppkeys) {
                for (int32_t sk : late_sks) {
                    late_suppkeys_by_ok[ok].insert(sk);
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_preprocessing: %.2f ms\n", ms_subquery);
    #endif

    // ===== MAIN SCAN: AGGREGATE WITH FILTERS (PARALLEL) =====
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int64_t> agg_count;

    #pragma omp parallel
    {
        std::unordered_map<int32_t, int64_t> local_agg_count;

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_rows; i++) {
            int32_t orderkey = lineitem_l_orderkey[i];
            int32_t suppkey = lineitem_l_suppkey[i];
            int32_t l_receiptdate = lineitem_l_receiptdate[i];
            int32_t l_commitdate = lineitem_l_commitdate[i];

            // Filter 1: l_receiptdate > l_commitdate
            if (l_receiptdate <= l_commitdate) continue;

            // Filter 2: orderkey must have status='F'
            if (valid_orderkeys.find(orderkey) == valid_orderkeys.end()) continue;

            // Filter 3: suppkey must be from SAUDI ARABIA
            if (supplier_name_by_suppkey.find(suppkey) == supplier_name_by_suppkey.end()) continue;

            // Get suppkey info for this orderkey
            const auto& sks = suppkeys_by_ok[orderkey];
            const auto& late_sks = late_suppkeys_by_ok[orderkey];

            // Filter 4: EXISTS - must have > 1 suppkey
            if (sks.size() <= 1) continue;

            // Filter 5: NOT EXISTS - no OTHER late suppkey
            bool valid = false;
            if (late_sks.size() == 0) {
                valid = true;  // No late suppkeys
            } else if (late_sks.size() == 1 && late_sks.count(suppkey) > 0) {
                valid = true;  // Only this suppkey is late
            }

            if (!valid) continue;

            // All filters passed
            local_agg_count[suppkey]++;
        }

        // Merge local aggregation into global
        #pragma omp critical
        {
            for (auto& [sk, count] : local_agg_count) {
                agg_count[sk] += count;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
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

    std::sort(results.begin(), results.end());

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
