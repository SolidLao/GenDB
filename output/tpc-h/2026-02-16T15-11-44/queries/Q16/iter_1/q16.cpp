/*
=============================================================================
Q16: Parts/Supplier Relationship - TPC-H Query 16
=============================================================================

LOGICAL PLAN:
============

Step 1: Single-table predicates and filtering
  - supplier: Filter on s_comment LIKE '%Customer%Complaints%' → pre-compute set of excluded suppkeys
    Estimated: ~5000 rows (out of 100K) match the pattern → ~95K excluded suppliers
    Action: Full scan, decode dictionary, pattern match, collect into hash set

  - part: Apply all single-table predicates
    * p_brand <> 'Brand#45' → exclude 1 brand out of 25 → ~1.92M rows
    * p_type NOT LIKE 'MEDIUM POLISHED%' → exclude ~10 types out of 150 → ~1.87M rows
    * p_size IN (49, 14, 23, 45, 19, 3, 36, 9) → 8 out of 50 sizes → ~0.30M rows
    Estimated: ~0.30M rows after all filters (from 2M)
    Action: Full scan, apply all predicates in single loop

Step 2: Join execution (smallest filtered result first)
  - Build phase: Create hash table on filtered part (0.30M rows)
    Key: p_partkey, Value: (p_brand, p_type, p_size)

  - Probe phase 1: Probe partsupp (8M rows) against part hash table
    For each ps_partkey, find matching p_partkey in hash table
    Output intermediate: (p_brand, p_type, p_size, ps_suppkey)
    Estimated intermediate: ~0.30M rows × 4 (avg suppkeys per part) = 1.2M rows

  - Filter intermediate: Exclude ps_suppkey if in excluded supplier set
    Estimated: ~1.2M rows → ~1.1M rows after exclusion

Step 3: Aggregation (GROUP BY p_brand, p_type, p_size)
  - Count distinct ps_suppkey per group
  - Estimated groups: ~0.30M / 4 = ~75K groups (upper bound, many parts have <4 suppliers)
  - Data structure: hash_map<(brand_code, type_code, size), set<suppkey>>
  - OR: hash_map<(brand_code, type_code, size), bitset<100K>> for fast unique counting

Step 4: Sort and output
  - ORDER BY supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC
  - Output: (p_brand_string, p_type_string, p_size, supplier_cnt)

PHYSICAL PLAN:
==============

Scan strategy:
  - Supplier: Full scan (100K rows, small table) - no parallelism benefit
  - Part: Full scan with predicate pushdown (2M rows) - parallelize with OpenMP
  - Partsupp: Full scan with join (8M rows) - parallelize join probe with OpenMP

Join strategy:
  - part→partsupp: Hash join, build on part (smaller after filtering), probe with partsupp
  - Parallelization: Build phase sequential, probe phase parallel with morsel-driven chunking

Aggregation:
  - Use hash_map with custom hash for (brand_code, type_code, size) tuple key
  - Value: std::set<int32_t> for distinct count (or bitset for faster unique counting)
  - Pre-size with estimated 75K groups

Sort:
  - Collect results into vector, std::sort with custom comparator

Implementation details:
  1. Load dictionaries at startup (p_brand, p_type, s_comment)
  2. Load mmap binary columns for all required fields
  3. Parallel scan of supplier to build excluded_suppkeys set
  4. Parallel scan of part with filter predicates, build hash table
  5. Parallel probe of partsupp, join with part hash table, filter by excluded set
  6. Single-threaded aggregation (intermediate result small enough)
  7. Sort results and output to CSV

Parallelism:
  - OpenMP for: supplier scan (100K), part scan (2M), partsupp probe (8M)
  - Thread-local buffers for aggregation in probe phase if intermediate >10M rows
  - Critical sections: hash table insert during build phase

=============================================================================
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <bitset>
#include <omp.h>
#include <atomic>

// Dictionary loading utilities
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
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

// Check if string matches LIKE pattern (LIKE '%Customer%Complaints%' or LIKE 'MEDIUM POLISHED%')
bool matches_pattern(const std::string& s, const std::string& pattern, bool is_prefix = false) {
    if (is_prefix) {
        return s.find(pattern) == 0;
    } else {
        // Pattern like '%Customer%Complaints%' or '%substring%'
        size_t pos = 0;
        std::string remaining = pattern;

        // Split by '%' and check each part
        std::vector<std::string> parts;
        size_t start = 0;
        while ((pos = remaining.find('%', start)) != std::string::npos) {
            if (pos > start) {
                parts.push_back(remaining.substr(start, pos - start));
            }
            start = pos + 1;
        }
        if (start < remaining.length()) {
            parts.push_back(remaining.substr(start));
        }

        // If pattern is '%X%Y%', check that X and Y appear in order
        pos = 0;
        for (const auto& part : parts) {
            if (part.empty()) continue;
            size_t found = s.find(part, pos);
            if (found == std::string::npos) return false;
            pos = found + part.length();
        }
        return true;
    }
}

// File memory mapping helper
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    count = sb.st_size / sizeof(T);
    T* data = (T*) mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Error mmapping " << path << std::endl;
        return nullptr;
    }

    return data;
}

struct ResultRow {
    std::string p_brand;
    std::string p_type;
    int32_t p_size;
    int32_t supplier_cnt;

    bool operator<(const ResultRow& other) const {
        if (supplier_cnt != other.supplier_cnt) return supplier_cnt > other.supplier_cnt;
        if (p_brand != other.p_brand) return p_brand < other.p_brand;
        if (p_type != other.p_type) return p_type < other.p_type;
        return p_size < other.p_size;
    }
};

void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    const std::string supplier_dir = gendb_dir + "/supplier/";
    const std::string part_dir = gendb_dir + "/part/";
    const std::string partsupp_dir = gendb_dir + "/partsupp/";

    // Load dictionaries
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    auto p_brand_dict = load_dictionary(part_dir + "p_brand_dict.txt");
    auto p_type_dict = load_dictionary(part_dir + "p_type_dict.txt");
    auto s_comment_dict = load_dictionary(supplier_dir + "s_comment_dict.txt");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // ========== STEP 1: Build excluded supplier set ==========
#ifdef GENDB_PROFILE
    auto t_supplier_scan_start = std::chrono::high_resolution_clock::now();
#endif

    size_t supplier_count = 0;
    int32_t* s_suppkey = mmap_file<int32_t>(supplier_dir + "s_suppkey.bin", supplier_count);
    int32_t* s_comment = mmap_file<int32_t>(supplier_dir + "s_comment.bin", supplier_count);

    std::unordered_set<int32_t> excluded_suppkeys;

    for (size_t i = 0; i < supplier_count; i++) {
        auto it = s_comment_dict.find(s_comment[i]);
        if (it != s_comment_dict.end()) {
            if (matches_pattern(it->second, "%Customer%Complaints%", false)) {
                excluded_suppkeys.insert(s_suppkey[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_supplier_scan_end = std::chrono::high_resolution_clock::now();
    double supplier_scan_ms = std::chrono::duration<double, std::milli>(t_supplier_scan_end - t_supplier_scan_start).count();
    printf("[TIMING] supplier_scan_and_filter: %.2f ms (excluded: %zu)\n", supplier_scan_ms, excluded_suppkeys.size());
#endif

    // ========== STEP 2: Scan and filter part table ==========
#ifdef GENDB_PROFILE
    auto t_part_scan_start = std::chrono::high_resolution_clock::now();
#endif

    size_t part_count = 0;
    int32_t* p_partkey = mmap_file<int32_t>(part_dir + "p_partkey.bin", part_count);
    int32_t* p_brand = mmap_file<int32_t>(part_dir + "p_brand.bin", part_count);
    int32_t* p_type = mmap_file<int32_t>(part_dir + "p_type.bin", part_count);
    int32_t* p_size = mmap_file<int32_t>(part_dir + "p_size.bin", part_count);

    // Find codes for Brand#45 and MEDIUM POLISHED% in dictionaries
    int32_t brand45_code = -1;
    for (auto& [code, value] : p_brand_dict) {
        if (value == "Brand#45") {
            brand45_code = code;
            break;
        }
    }

    // Build hash table on filtered part (build on smaller relation)
    struct PartValue {
        int32_t brand_code;
        int32_t type_code;
        int32_t size;
    };

    std::unordered_map<int32_t, std::vector<PartValue>> part_hash;
    part_hash.reserve(part_count / 4);  // Estimated: ~0.3M unique keys from 2M rows

    std::vector<int32_t> size_filter = {49, 14, 23, 45, 19, 3, 36, 9};
    std::unordered_set<int32_t> size_set(size_filter.begin(), size_filter.end());

    int32_t filtered_part_count = 0;

    for (size_t i = 0; i < part_count; i++) {
        // Apply predicates
        // 1. p_brand <> 'Brand#45'
        if (p_brand[i] == brand45_code) continue;

        // 2. p_type NOT LIKE 'MEDIUM POLISHED%'
        auto type_it = p_type_dict.find(p_type[i]);
        if (type_it != p_type_dict.end()) {
            if (matches_pattern(type_it->second, "MEDIUM POLISHED", true)) {
                continue;
            }
        }

        // 3. p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        if (size_set.find(p_size[i]) == size_set.end()) continue;

        // All predicates passed, add to hash table
        part_hash[p_partkey[i]].push_back({p_brand[i], p_type[i], p_size[i]});
        filtered_part_count++;
    }

#ifdef GENDB_PROFILE
    auto t_part_scan_end = std::chrono::high_resolution_clock::now();
    double part_scan_ms = std::chrono::duration<double, std::milli>(t_part_scan_end - t_part_scan_start).count();
    printf("[TIMING] part_scan_and_filter: %.2f ms (filtered: %d)\n", part_scan_ms, filtered_part_count);
#endif

    // ========== STEP 3: Probe partsupp and aggregate ==========
#ifdef GENDB_PROFILE
    auto t_partsupp_start = std::chrono::high_resolution_clock::now();
#endif

    size_t partsupp_count = 0;
    int32_t* ps_partkey = mmap_file<int32_t>(partsupp_dir + "ps_partkey.bin", partsupp_count);
    int32_t* ps_suppkey = mmap_file<int32_t>(partsupp_dir + "ps_suppkey.bin", partsupp_count);

    // Aggregation: GROUP BY (p_brand_code, p_type_code, p_size) -> sorted vector for distinct counting
    struct GroupKey {
        int32_t brand_code;
        int32_t type_code;
        int32_t size;

        bool operator==(const GroupKey& other) const {
            return brand_code == other.brand_code &&
                   type_code == other.type_code &&
                   size == other.size;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            return ((size_t)k.brand_code << 40) | ((size_t)k.type_code << 20) | (size_t)k.size;
        }
    };

    // Use sorted vector for efficient distinct counting
    std::unordered_map<GroupKey, std::vector<int32_t>, GroupKeyHash> aggregation;
    aggregation.reserve(filtered_part_count);

    int32_t filtered_ps_count = 0;

    // Parallel probe with thread-local buffers
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<GroupKey, std::vector<int32_t>, GroupKeyHash>> local_agg(num_threads);
    for (int t = 0; t < num_threads; t++) {
        local_agg[t].reserve(filtered_part_count / num_threads + 1);
    }

    std::atomic<int32_t> filtered_ps_count_atomic(0);

    #pragma omp parallel for schedule(static) num_threads(num_threads)
    for (size_t i = 0; i < partsupp_count; i++) {
        int thread_id = omp_get_thread_num();

        // Lookup part by ps_partkey
        auto it = part_hash.find(ps_partkey[i]);
        if (it == part_hash.end()) continue;  // Part doesn't match filters

        // Check if supplier is excluded
        if (excluded_suppkeys.find(ps_suppkey[i]) != excluded_suppkeys.end()) continue;

        // Add to thread-local aggregation
        for (const auto& pv : it->second) {
            GroupKey gk = {pv.brand_code, pv.type_code, pv.size};
            local_agg[thread_id][gk].push_back(ps_suppkey[i]);
        }
        filtered_ps_count_atomic++;
    }

    filtered_ps_count = filtered_ps_count_atomic;

    // Merge thread-local aggregations into global
    for (int t = 0; t < num_threads; t++) {
        for (auto& [gk, suppkeys] : local_agg[t]) {
            auto& global_suppkeys = aggregation[gk];
            global_suppkeys.insert(global_suppkeys.end(), suppkeys.begin(), suppkeys.end());
        }
    }

    // Sort each suppkey vector and deduplicate for final distinct count
    for (auto& [gk, suppkeys] : aggregation) {
        std::sort(suppkeys.begin(), suppkeys.end());
        suppkeys.erase(std::unique(suppkeys.begin(), suppkeys.end()), suppkeys.end());
    }

#ifdef GENDB_PROFILE
    auto t_partsupp_end = std::chrono::high_resolution_clock::now();
    double partsupp_ms = std::chrono::duration<double, std::milli>(t_partsupp_end - t_partsupp_start).count();
    printf("[TIMING] partsupp_join_and_aggregate: %.2f ms (matched: %d)\n", partsupp_ms, filtered_ps_count);
#endif

    // ========== STEP 4: Convert aggregation to result rows and sort ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    results.reserve(aggregation.size());

    for (const auto& [gk, suppkeys] : aggregation) {
        auto brand_it = p_brand_dict.find(gk.brand_code);
        auto type_it = p_type_dict.find(gk.type_code);

        if (brand_it != p_brand_dict.end() && type_it != p_type_dict.end()) {
            results.push_back({
                brand_it->second,
                type_it->second,
                gk.size,
                (int32_t)suppkeys.size()  // Distinct count after dedup
            });
        }
    }

    // Sort: supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC
    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_output_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // ========== STEP 5: Write CSV output ==========
#ifdef GENDB_PROFILE
    auto t_csv_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q16.csv";
    std::ofstream out(output_path);

    // Write header
    out << "p_brand,p_type,p_size,supplier_cnt\n";

    // Write rows
    for (const auto& row : results) {
        out << row.p_brand << ","
            << row.p_type << ","
            << row.p_size << ","
            << row.supplier_cnt << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_csv_end = std::chrono::high_resolution_clock::now();
    double csv_ms = std::chrono::duration<double, std::milli>(t_csv_end - t_csv_start).count();
    printf("[TIMING] output: %.2f ms\n", csv_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q16 completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q16(gendb_dir, results_dir);
    return 0;
}
#endif
