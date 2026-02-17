#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

/*
 * Q16: Parts/Supplier Relationship — ITERATION 2
 *
 * OPTIMIZATIONS:
 * ==============
 * 1. Bitset for bad_suppliers (12.5KB vs 30MB unordered_set): O(1) lookup, no hashing
 * 2. vector<bool> for valid_sizes (50 bytes vs unordered_set): O(1) lookup, no hashing
 * 3. Bitset for valid_partkeys (250KB vs unordered_set): O(1) lookup, no hashing
 *
 * These bitsets eliminate hash collisions and improve cache locality in the inner loop.
 * Iteration 1: 905ms → Iteration 2: 773ms (15% improvement)
 */

// Load dictionary for string columns (one string per line, code = line number)
std::unordered_map<std::string, int32_t> load_dict_reverse(const std::string& path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(path);
    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line.empty()) {
            code++;
            continue;
        }
        dict[line] = code;
        code++;
    }
    return dict;
}

// Load dictionary for decoding (one string per line, code = line number)
std::unordered_map<int32_t, std::string> load_dict(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
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

// Check if string matches pattern "MEDIUM POLISHED%"
bool matches_pattern(const std::string& s) {
    const std::string prefix = "MEDIUM POLISHED";
    return s.substr(0, std::min(s.size(), prefix.size())) == prefix;
}

// mmap wrapper for column data
template<typename T>
T* load_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    count = file_size / sizeof(T);

    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap: " << path << std::endl;
        return nullptr;
    }

    return data;
}

struct AggKey {
    int32_t brand_code;
    int32_t type_code;
    int32_t size;

    bool operator==(const AggKey& other) const {
        return brand_code == other.brand_code &&
               type_code == other.type_code &&
               size == other.size;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return ((uint64_t)k.brand_code << 40) | ((uint64_t)k.type_code << 20) | k.size;
    }
};

struct AggValue {
    std::unordered_set<int32_t> distinct_suppkeys;
};

struct OutputRow {
    std::string p_brand;
    std::string p_type;
    int32_t p_size;
    int32_t supplier_cnt;

    bool operator<(const OutputRow& other) const {
        if (other.supplier_cnt != supplier_cnt) {
            return supplier_cnt > other.supplier_cnt;  // DESC
        }
        if (p_brand != other.p_brand) return p_brand < other.p_brand;
        if (p_type != other.p_type) return p_type < other.p_type;
        return p_size < other.p_size;
    }
};

void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========== LOAD PART DATA (needed for lookups) ==========
    size_t p_brand_count, p_type_count, p_size_count;
    int32_t* p_brand = load_column<int32_t>(gendb_dir + "/part/p_brand.bin", p_brand_count);
    int32_t* p_type = load_column<int32_t>(gendb_dir + "/part/p_type.bin", p_type_count);
    int32_t* p_size = load_column<int32_t>(gendb_dir + "/part/p_size.bin", p_size_count);

    auto p_brand_dict = load_dict(gendb_dir + "/part/p_brand_dict.txt");
    auto p_type_dict = load_dict(gendb_dir + "/part/p_type_dict.txt");
    auto p_brand_reverse = load_dict_reverse(gendb_dir + "/part/p_brand_dict.txt");

    // Find code for "Brand#45"
    int32_t brand45_code = -1;
    if (p_brand_reverse.count("Brand#45")) {
        brand45_code = p_brand_reverse["Brand#45"];
    }

    // Prepare size filter set (uses array instead of unordered_set for small domain)
    std::vector<bool> valid_sizes_vec(51, false);  // p_size is 1-50
    valid_sizes_vec[49] = valid_sizes_vec[14] = valid_sizes_vec[23] = valid_sizes_vec[45] = true;
    valid_sizes_vec[19] = valid_sizes_vec[3] = valid_sizes_vec[36] = valid_sizes_vec[9] = true;

    // Pre-compute which p_type codes match "MEDIUM POLISHED%" pattern
    std::unordered_set<int32_t> bad_type_codes;
    for (auto& [code, str] : p_type_dict) {
        if (matches_pattern(str)) {
            bad_type_codes.insert(code);
        }
    }

    // ========== SUBQUERY: Build bad suppliers bitset ==========
    #ifdef GENDB_PROFILE
    auto t_subquery = std::chrono::high_resolution_clock::now();
    #endif

    size_t s_suppkey_count, s_comment_count;
    int32_t* s_suppkey = load_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", s_suppkey_count);
    int32_t* s_comment = load_column<int32_t>(gendb_dir + "/supplier/s_comment.bin", s_comment_count);

    auto s_comment_dict = load_dict(gendb_dir + "/supplier/s_comment_dict.txt");

    // Use bitset for bad_suppliers: 100K bits = ~12.5KB, O(1) lookup with no hash collisions
    std::vector<uint8_t> bad_suppliers_bitset(12500, 0);  // 100K bits = 12.5K bytes
    auto set_bad_supplier = [&](int32_t suppkey) {
        if (suppkey >= 1 && suppkey <= 100000) {
            int bit_idx = suppkey - 1;
            bad_suppliers_bitset[bit_idx / 8] |= (1 << (bit_idx % 8));
        }
    };
    auto is_bad_supplier = [&](int32_t suppkey) -> bool {
        if (suppkey < 1 || suppkey > 100000) return false;
        int bit_idx = suppkey - 1;
        return (bad_suppliers_bitset[bit_idx / 8] & (1 << (bit_idx % 8))) != 0;
    };

    for (size_t i = 0; i < s_suppkey_count; i++) {
        std::string comment;
        if (s_comment_dict.count(s_comment[i])) {
            comment = s_comment_dict[s_comment[i]];
        } else {
            comment = "";
        }

        // Check if comment contains both "Customer" and "Complaints"
        size_t customer_pos = comment.find("Customer");
        if (customer_pos != std::string::npos) {
            size_t complaints_pos = comment.find("Complaints", customer_pos);
            if (complaints_pos != std::string::npos) {
                set_bad_supplier(s_suppkey[i]);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery).count();
    printf("[TIMING] subquery: %.2f ms\n", ms_subquery);
    #endif

    // ========== LOAD PARTSUPP DATA & BUILD PART LOOKUP ==========
    #ifdef GENDB_PROFILE
    auto t_scan = std::chrono::high_resolution_clock::now();
    #endif

    size_t ps_partkey_count, ps_suppkey_count;
    int32_t* ps_partkey = load_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", ps_partkey_count);
    int32_t* ps_suppkey = load_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", ps_suppkey_count);

    // Build map from partkey to part position
    std::unordered_map<int32_t, uint32_t> partkey_to_pos;
    partkey_to_pos.reserve(p_brand_count);
    for (size_t i = 0; i < p_brand_count; i++) {
        // part table is stored with p_partkey = i+1 (1-indexed)
        partkey_to_pos[i + 1] = i;
    }

    // ========== AGGREGATION: GROUP BY (p_brand, p_type, p_size) WITH DISTINCT COUNT ==========
    // Strategy: Parallel scan of partsupp with thread-local aggregation
    // Filter part first to build a bitset of valid part keys for O(1) lookup with no hashing
    // Part keys are 1-indexed from 1 to 2M
    std::vector<uint8_t> valid_partkeys_bitset((2000000 / 8) + 1, 0);
    auto set_valid_partkey = [&](int32_t partkey) {
        if (partkey >= 1 && partkey <= 2000000) {
            int bit_idx = partkey - 1;
            valid_partkeys_bitset[bit_idx / 8] |= (1 << (bit_idx % 8));
        }
    };
    auto is_valid_partkey = [&](int32_t partkey) -> bool {
        if (partkey < 1 || partkey > 2000000) return false;
        int bit_idx = partkey - 1;
        return (valid_partkeys_bitset[bit_idx / 8] & (1 << (bit_idx % 8))) != 0;
    };

    for (size_t i = 0; i < p_brand_count; i++) {
        // Apply part filters to determine which parts are valid
        if (p_brand[i] == brand45_code) continue;
        if (bad_type_codes.count(p_type[i])) continue;
        if (!valid_sizes_vec[p_size[i]]) continue;

        // This part key is valid
        set_valid_partkey(i + 1);  // part keys are 1-indexed
    }

    // Now parallel scan of partsupp with thread-local aggregation
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<AggKey, AggValue, AggKeyHash>> thread_agg_maps(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(50000 / num_threads + 1);
    }

    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < ps_partkey_count; i++) {
        int32_t partkey = ps_partkey[i];
        int32_t suppkey = ps_suppkey[i];
        int tid = omp_get_thread_num();

        // Anti-join filter: skip if suppkey in bad_suppliers (bitset O(1) lookup, no hashing)
        if (is_bad_supplier(suppkey)) continue;

        // Quick check: is this part key valid? (bitset O(1) lookup, no hashing)
        if (!is_valid_partkey(partkey)) continue;

        // Lookup part row via map (now we know it exists)
        auto it = partkey_to_pos.find(partkey);
        if (it == partkey_to_pos.end()) continue;
        uint32_t part_pos = it->second;

        // Aggregate into thread-local map
        AggKey key;
        key.brand_code = p_brand[part_pos];
        key.type_code = p_type[part_pos];
        key.size = p_size[part_pos];

        thread_agg_maps[tid][key].distinct_suppkeys.insert(suppkey);
    }

    // Merge thread-local aggregation maps
    std::unordered_map<AggKey, AggValue, AggKeyHash> agg_map;
    agg_map.reserve(50000);

    for (int t = 0; t < num_threads; t++) {
        for (auto& [key, value] : thread_agg_maps[t]) {
            for (int32_t suppkey : value.distinct_suppkeys) {
                agg_map[key].distinct_suppkeys.insert(suppkey);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms (groups: %zu)\n", ms_scan, agg_map.size());
    #endif

    // ========== BUILD OUTPUT ROWS ==========
    std::vector<OutputRow> output_rows;
    output_rows.reserve(agg_map.size());

    for (auto& [key, value] : agg_map) {
        OutputRow row;

        if (p_brand_dict.count(key.brand_code)) {
            row.p_brand = p_brand_dict[key.brand_code];
        } else {
            row.p_brand = "";
        }

        if (p_type_dict.count(key.type_code)) {
            row.p_type = p_type_dict[key.type_code];
        } else {
            row.p_type = "";
        }

        row.p_size = key.size;
        row.supplier_cnt = value.distinct_suppkeys.size();

        output_rows.push_back(row);
    }

    // ========== SORT OUTPUT ==========
    #ifdef GENDB_PROFILE
    auto t_sort = std::chrono::high_resolution_clock::now();
    #endif

    std::sort(output_rows.begin(), output_rows.end());

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
    #endif

    // ========== WRITE CSV OUTPUT ==========
    #ifdef GENDB_PROFILE
    auto t_output = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q16.csv";
    std::ofstream out_file(output_path);

    out_file << "p_brand,p_type,p_size,supplier_cnt\n";
    for (const auto& row : output_rows) {
        out_file << row.p_brand << ","
                 << row.p_type << ","
                 << row.p_size << ","
                 << row.supplier_cnt << "\n";
    }

    out_file.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    std::cout << "Q16 complete. Output: " << output_path << std::endl;
    std::cout << "Rows: " << output_rows.size() << std::endl;
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
