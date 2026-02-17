#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <omp.h>

// ============================================================================
// LOGICAL QUERY PLAN for Q2
// ============================================================================
// SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
// WHERE:
//   - p_size = 15
//   - p_type LIKE '%BRASS'
//   - s_nationkey = n_nationkey
//   - n_regionkey = r_regionkey
//   - r_name = 'EUROPE'
//   - ps_supplycost = MIN(ps_supplycost) for parts in EUROPE region
//
// Logical execution order:
// 1. Filter region: r_name = 'EUROPE' → 1 row
// 2. Filter nation: n_regionkey IN europe_keys → ~5 rows (Eastern Europe, Russia, etc.)
// 3. Filter supplier: s_nationkey IN nation_keys → ~20K rows
// 4. Filter part: p_size = 15 AND p_type LIKE '%BRASS' → ~1-2K rows
// 5. Subquery: Compute MIN(ps_supplycost) for (partkey, suppkey) pairs where supplier is in Europe
// 6. Join partsupp with part on partkey, filter by size/type
// 7. Join result with supplier on suppkey, filter by nation
// 8. Join result with nation on nationkey
// 9. Join result with region on regionkey (should give 1 row per output)
// 10. Filter by ps_supplycost = min_cost for that partkey
// 11. Sort: s_acctbal DESC, n_name, s_name, p_partkey
// 12. LIMIT 100
//
// PHYSICAL PLAN:
// - All tables are small enough for hash joins (< 2M * 8 cols * 8 bytes = ~128 MB per table)
// - Use hash join: build on smaller side, probe with larger
// - Join order (smallest filtered first): region (1) → nation (5) → supplier (20K) → part (2K) → partsupp (8M)
// - Subquery: Pre-compute MIN per partkey across all Europe suppliers
// - Final filter: ps_supplycost = MIN[partkey]
// - Sort using std::sort on final result
// ============================================================================

// Helper: Load dictionary file for string columns
std::unordered_map<int32_t, std::string> load_dict(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
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

// Helper: Find dictionary code for a value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, val] : dict) {
        if (val == value) return code;
    }
    return -1;  // Not found
}

// Helper: Find all dictionary codes matching a condition (e.g., LIKE '%BRASS')
std::vector<int32_t> find_dict_codes_like(const std::unordered_map<int32_t, std::string>& dict, const std::string& pattern) {
    std::vector<int32_t> codes;
    for (const auto& [code, val] : dict) {
        if (val.find(pattern) != std::string::npos) {
            codes.push_back(code);
        }
    }
    return codes;
}

// Helper: Open and mmap a binary file
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    size = static_cast<size_t>(file_size);
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Helper: Get file size
size_t get_file_size(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f.is_open()) return 0;
    return f.tellg();
}

// Result structure for final output
struct Q2Result {
    int64_t s_acctbal;  // Scaled decimal (scale 100)
    std::string s_name;
    std::string n_name;
    int32_t p_partkey;
    std::string p_mfgr;
    std::string s_address;
    std::string s_phone;
    std::string s_comment;
};

// Comparison function for sorting
bool compare_q2_results(const Q2Result& a, const Q2Result& b) {
    // Sort by s_acctbal DESC
    if (a.s_acctbal != b.s_acctbal) {
        return a.s_acctbal > b.s_acctbal;
    }
    // Then by n_name ASC
    if (a.n_name != b.n_name) {
        return a.n_name < b.n_name;
    }
    // Then by s_name ASC
    if (a.s_name != b.s_name) {
        return a.s_name < b.s_name;
    }
    // Then by p_partkey ASC
    return a.p_partkey < b.p_partkey;
}

void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    // Timing
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // Step 1: Load dictionaries
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    auto r_name_dict = load_dict(gendb_dir + "/region/r_name_dict.txt");
    auto n_name_dict = load_dict(gendb_dir + "/nation/n_name_dict.txt");
    auto s_name_dict = load_dict(gendb_dir + "/supplier/s_name_dict.txt");
    auto s_address_dict = load_dict(gendb_dir + "/supplier/s_address_dict.txt");
    auto s_phone_dict = load_dict(gendb_dir + "/supplier/s_phone_dict.txt");
    auto s_comment_dict = load_dict(gendb_dir + "/supplier/s_comment_dict.txt");
    auto p_mfgr_dict = load_dict(gendb_dir + "/part/p_mfgr_dict.txt");
    auto p_type_dict = load_dict(gendb_dir + "/part/p_type_dict.txt");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
        printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
    }
#endif

    // Find dictionary code for "EUROPE"
    int32_t europe_code = find_dict_code(r_name_dict, "EUROPE");
    if (europe_code < 0) {
        std::cerr << "ERROR: EUROPE not found in region dictionary" << std::endl;
        return;
    }

    // Find dictionary codes for all types containing "BRASS"
    std::vector<int32_t> brass_type_codes = find_dict_codes_like(p_type_dict, "BRASS");
    if (brass_type_codes.empty()) {
        std::cerr << "ERROR: No types containing BRASS found" << std::endl;
        return;
    }

    // ========================================================================
    // Step 2: Load binary data (mmap)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t size_tmp;

    // Region
    void* region_r_regionkey_ptr = mmap_file(gendb_dir + "/region/r_regionkey.bin", size_tmp);
    int32_t* region_r_regionkey = static_cast<int32_t*>(region_r_regionkey_ptr);
    void* region_r_name_ptr = mmap_file(gendb_dir + "/region/r_name.bin", size_tmp);
    int32_t* region_r_name = static_cast<int32_t*>(region_r_name_ptr);
    size_t region_count = get_file_size(gendb_dir + "/region/r_regionkey.bin") / sizeof(int32_t);

    // Nation
    void* nation_n_nationkey_ptr = mmap_file(gendb_dir + "/nation/n_nationkey.bin", size_tmp);
    int32_t* nation_n_nationkey = static_cast<int32_t*>(nation_n_nationkey_ptr);
    void* nation_n_regionkey_ptr = mmap_file(gendb_dir + "/nation/n_regionkey.bin", size_tmp);
    int32_t* nation_n_regionkey = static_cast<int32_t*>(nation_n_regionkey_ptr);
    size_t nation_count = get_file_size(gendb_dir + "/nation/n_nationkey.bin") / sizeof(int32_t);

    // Supplier
    void* supplier_s_suppkey_ptr = mmap_file(gendb_dir + "/supplier/s_suppkey.bin", size_tmp);
    int32_t* supplier_s_suppkey = static_cast<int32_t*>(supplier_s_suppkey_ptr);
    void* supplier_s_name_ptr = mmap_file(gendb_dir + "/supplier/s_name.bin", size_tmp);
    int32_t* supplier_s_name = static_cast<int32_t*>(supplier_s_name_ptr);
    void* supplier_s_address_ptr = mmap_file(gendb_dir + "/supplier/s_address.bin", size_tmp);
    int32_t* supplier_s_address = static_cast<int32_t*>(supplier_s_address_ptr);
    void* supplier_s_phone_ptr = mmap_file(gendb_dir + "/supplier/s_phone.bin", size_tmp);
    int32_t* supplier_s_phone = static_cast<int32_t*>(supplier_s_phone_ptr);
    void* supplier_s_comment_ptr = mmap_file(gendb_dir + "/supplier/s_comment.bin", size_tmp);
    int32_t* supplier_s_comment = static_cast<int32_t*>(supplier_s_comment_ptr);
    void* supplier_s_nationkey_ptr = mmap_file(gendb_dir + "/supplier/s_nationkey.bin", size_tmp);
    int32_t* supplier_s_nationkey = static_cast<int32_t*>(supplier_s_nationkey_ptr);
    void* supplier_s_acctbal_ptr = mmap_file(gendb_dir + "/supplier/s_acctbal.bin", size_tmp);
    int64_t* supplier_s_acctbal = static_cast<int64_t*>(supplier_s_acctbal_ptr);
    size_t supplier_count = get_file_size(gendb_dir + "/supplier/s_suppkey.bin") / sizeof(int32_t);

    // Part
    void* part_p_partkey_ptr = mmap_file(gendb_dir + "/part/p_partkey.bin", size_tmp);
    int32_t* part_p_partkey = static_cast<int32_t*>(part_p_partkey_ptr);
    void* part_p_mfgr_ptr = mmap_file(gendb_dir + "/part/p_mfgr.bin", size_tmp);
    int32_t* part_p_mfgr = static_cast<int32_t*>(part_p_mfgr_ptr);
    void* part_p_type_ptr = mmap_file(gendb_dir + "/part/p_type.bin", size_tmp);
    int32_t* part_p_type = static_cast<int32_t*>(part_p_type_ptr);
    void* part_p_size_ptr = mmap_file(gendb_dir + "/part/p_size.bin", size_tmp);
    int32_t* part_p_size = static_cast<int32_t*>(part_p_size_ptr);
    size_t part_count = get_file_size(gendb_dir + "/part/p_partkey.bin") / sizeof(int32_t);

    // Partsupp
    void* partsupp_ps_partkey_ptr = mmap_file(gendb_dir + "/partsupp/ps_partkey.bin", size_tmp);
    int32_t* partsupp_ps_partkey = static_cast<int32_t*>(partsupp_ps_partkey_ptr);
    void* partsupp_ps_suppkey_ptr = mmap_file(gendb_dir + "/partsupp/ps_suppkey.bin", size_tmp);
    int32_t* partsupp_ps_suppkey = static_cast<int32_t*>(partsupp_ps_suppkey_ptr);
    void* partsupp_ps_supplycost_ptr = mmap_file(gendb_dir + "/partsupp/ps_supplycost.bin", size_tmp);
    int64_t* partsupp_ps_supplycost = static_cast<int64_t*>(partsupp_ps_supplycost_ptr);
    size_t partsupp_count = get_file_size(gendb_dir + "/partsupp/ps_partkey.bin") / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
        printf("[TIMING] load_data: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 3: Filter region (r_name = 'EUROPE')
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_filter_region_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t europe_regionkey = -1;
    for (size_t i = 0; i < region_count; i++) {
        if (region_r_name[i] == europe_code) {
            europe_regionkey = region_r_regionkey[i];
            break;
        }
    }

    if (europe_regionkey < 0) {
        std::cerr << "ERROR: EUROPE region not found" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_filter_region_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_filter_region_end - t_filter_region_start).count();
        printf("[TIMING] filter_region: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 4: Filter nation (n_regionkey = europe_regionkey)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_filter_nation_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> europe_nations;  // n_nationkey -> n_nationkey
    for (size_t i = 0; i < nation_count; i++) {
        if (nation_n_regionkey[i] == europe_regionkey) {
            europe_nations[nation_n_nationkey[i]] = nation_n_nationkey[i];
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_nation_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_filter_nation_end - t_filter_nation_start).count();
        printf("[TIMING] filter_nation: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 5: Build set of Europe supplier keys (FAST LOOKUP)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    // Build a set of Europe supplier keys for fast O(1) checking
    std::unordered_set<int32_t> europe_suppliers;  // Set of suppkey values in Europe
    for (size_t i = 0; i < supplier_count; i++) {
        int32_t nationkey = supplier_s_nationkey[i];
        if (europe_nations.find(nationkey) != europe_nations.end()) {
            europe_suppliers.insert(supplier_s_suppkey[i]);
        }
    }

    // ========================================================================
    // Step 5b: Precompute MIN(ps_supplycost) for each partkey in Europe (PARALLEL)
    // ========================================================================
    // Parallel computation: each thread builds its own local min_cost map
    std::vector<std::unordered_map<int32_t, int64_t>> thread_local_min_cost;
    int num_threads = std::min(64, (int)std::thread::hardware_concurrency());
    thread_local_min_cost.resize(num_threads);

#pragma omp parallel for num_threads(64) schedule(dynamic, 10000)
    for (size_t i = 0; i < partsupp_count; i++) {
        int32_t suppkey = partsupp_ps_suppkey[i];
        int32_t partkey = partsupp_ps_partkey[i];
        int64_t cost = partsupp_ps_supplycost[i];

        // Quick check: is this supplier in Europe? (Fast set lookup)
        if (europe_suppliers.find(suppkey) == europe_suppliers.end()) continue;

        // Update min cost for this partkey in thread-local map
        int tid = omp_get_thread_num();
        auto it = thread_local_min_cost[tid].find(partkey);
        if (it == thread_local_min_cost[tid].end()) {
            thread_local_min_cost[tid][partkey] = cost;
        } else {
            it->second = std::min(it->second, cost);
        }
    }

    // Merge thread-local results into global map
    std::unordered_map<int32_t, int64_t> min_cost_per_partkey;
    for (int tid = 0; tid < num_threads; tid++) {
        for (const auto& [partkey, cost] : thread_local_min_cost[tid]) {
            auto it = min_cost_per_partkey.find(partkey);
            if (it == min_cost_per_partkey.end()) {
                min_cost_per_partkey[partkey] = cost;
            } else {
                it->second = std::min(it->second, cost);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
        printf("[TIMING] subquery_min_cost: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 6: Filter part (p_size = 15 AND p_type LIKE '%BRASS')
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_filter_part_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> valid_partkeys;  // partkey -> partkey
    for (size_t i = 0; i < part_count; i++) {
        if (part_p_size[i] == 15) {
            // Check if type is a BRASS type
            bool is_brass = false;
            for (int32_t brass_code : brass_type_codes) {
                if (part_p_type[i] == brass_code) {
                    is_brass = true;
                    break;
                }
            }
            if (is_brass && min_cost_per_partkey.find(part_p_partkey[i]) != min_cost_per_partkey.end()) {
                valid_partkeys[part_p_partkey[i]] = part_p_partkey[i];
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_part_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_filter_part_end - t_filter_part_start).count();
        printf("[TIMING] filter_part: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 7: Build hash map for parts (partkey -> part_idx)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> partkey_to_idx;
    for (size_t i = 0; i < part_count; i++) {
        partkey_to_idx[part_p_partkey[i]] = i;
    }

    // ========================================================================
    // Step 8: Main join and filtering loop (PARALLEL with thread-local results)
    // ========================================================================
    // Use thread-local vectors to avoid synchronization contention
    std::vector<std::vector<Q2Result>> thread_local_results(num_threads);

#pragma omp parallel for num_threads(64) schedule(dynamic, 10000)
    for (size_t i = 0; i < partsupp_count; i++) {
        int32_t ps_partkey = partsupp_ps_partkey[i];
        int32_t ps_suppkey = partsupp_ps_suppkey[i];
        int64_t ps_cost = partsupp_ps_supplycost[i];

        // Filter 1: partkey must be in valid list
        if (valid_partkeys.find(ps_partkey) == valid_partkeys.end()) continue;

        // Filter 2: supplier must be in Europe
        // Note: suppliers are 1-indexed (suppkey) but arrays are 0-indexed
        if (ps_suppkey < 1 || ps_suppkey > 100000) continue;
        int32_t s_nationkey = supplier_s_nationkey[ps_suppkey - 1];
        if (s_nationkey < 0 || s_nationkey >= 25) continue;
        if (europe_nations.find(s_nationkey) == europe_nations.end()) continue;

        // Filter 3: supplycost must equal minimum for this partkey
        if (min_cost_per_partkey[ps_partkey] != ps_cost) continue;

        // All filters passed, construct result
        // Note: ps_suppkey is 1-indexed, arrays are 0-indexed
        int32_t supp_idx = ps_suppkey - 1;

        // Lookup part index via hash map (O(1) instead of O(part_count))
        auto it = partkey_to_idx.find(ps_partkey);
        if (it == partkey_to_idx.end()) continue;  // Part not found (shouldn't happen)
        int32_t part_idx = it->second;

        Q2Result row;
        row.s_acctbal = supplier_s_acctbal[supp_idx];
        row.s_name = s_name_dict.count(supplier_s_name[supp_idx]) ? s_name_dict[supplier_s_name[supp_idx]] : "";
        row.n_name = n_name_dict.count(s_nationkey) ? n_name_dict[s_nationkey] : "";
        row.p_partkey = ps_partkey;
        row.p_mfgr = p_mfgr_dict.count(part_p_mfgr[part_idx]) ? p_mfgr_dict[part_p_mfgr[part_idx]] : "";
        row.s_address = s_address_dict.count(supplier_s_address[supp_idx]) ? s_address_dict[supplier_s_address[supp_idx]] : "";
        row.s_phone = s_phone_dict.count(supplier_s_phone[supp_idx]) ? s_phone_dict[supplier_s_phone[supp_idx]] : "";
        row.s_comment = s_comment_dict.count(supplier_s_comment[supp_idx]) ? s_comment_dict[supplier_s_comment[supp_idx]] : "";

        int tid = omp_get_thread_num();
        thread_local_results[tid].push_back(row);
    }

    // Merge thread-local results into global results vector
    std::vector<Q2Result> results;
    for (int tid = 0; tid < num_threads; tid++) {
        for (const auto& row : thread_local_results[tid]) {
            results.push_back(row);
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
        printf("[TIMING] join_filter: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 9: Sort and limit
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), compare_q2_results);

    if (results.size() > 100) {
        results.resize(100);
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
        printf("[TIMING] sort_limit: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Step 10: Output results to CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q2.csv");
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << results_dir << "/Q2.csv" << std::endl;
        return;
    }

    // Helper function to escape CSV field (quote if contains comma, newline, or quote)
    auto escape_csv = [](const std::string& field) -> std::string {
        bool needs_quoting = field.find(',') != std::string::npos ||
                             field.find('\n') != std::string::npos ||
                             field.find('"') != std::string::npos;
        if (!needs_quoting) return field;

        std::string result = "\"";
        for (char c : field) {
            if (c == '"') result += "\"\"";  // Escape quote as double quote
            else result += c;
        }
        result += "\"";
        return result;
    };

    // Write header
    out << "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n";

    // Write rows
    for (const auto& row : results) {
        // s_acctbal: decimal with scale 100, output with 2 decimal places
        double acctbal = static_cast<double>(row.s_acctbal) / 100.0;
        out << acctbal << ",";
        out << row.s_name << ",";
        out << row.n_name << ",";
        out << row.p_partkey << ",";
        out << row.p_mfgr << ",";
        out << escape_csv(row.s_address) << ",";
        out << row.s_phone << ",";
        out << escape_csv(row.s_comment) << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
        printf("[TIMING] output: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // Total timing
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    {
        double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
        printf("[TIMING] total: %.2f ms\n", ms_total);
    }
#endif

    // Cleanup (optional - OS will clean up on exit)
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
