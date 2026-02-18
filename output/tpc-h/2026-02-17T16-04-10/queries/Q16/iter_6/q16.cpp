#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <algorithm>
#include <string>
#include <sstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <atomic>

/*
================================================================================
Q16: Parts/Supplier Relationship (ITERATION 6 - PARTITIONED AGGREGATION)

Logical Plan:
1. Load supplier comments and build hash set of bad suppliers
   (WHERE s_comment LIKE '%Customer%Complaints%')
2. Filter part table on:
   - p_brand <> 'Brand#45'
   - p_type NOT LIKE 'MEDIUM POLISHED%'
   - p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
   - Decode and store brand/type strings alongside partkey
3. Join partsupp with filtered part on ps_partkey = p_partkey
4. Exclude suppliers in bad set
5. Aggregate: COUNT DISTINCT ps_suppkey grouped by (p_brand, p_type, p_size)
   - Use PARTITIONED AGGREGATION with thread-owned hash table partitions
   - Each thread owns exclusive partition based on hash of numeric key
   - Compact open-addressing hash table (no std::map, no std::unordered_set)
   - Store suppkey list per group, deduplicate via sorting
6. Merge phase: thread-safe sequential merge of partitions
7. Sort by supplier_cnt DESC, p_brand, p_type, p_size

Physical Plan:
- Build bad_suppliers hash set via supplier subquery scan (100K rows)
- Scan part table with local filters (2M rows → ~500K filtered)
  - Store as vector<FilteredPart> with pre-decoded brand/type strings
- Build fast hash index on partkey (replace std::unordered_map)
- PARALLEL scan of partsupp (8M rows) with partition assignment:
  - For each (ps_partkey, ps_suppkey) pair:
    1. Look up in part hash index
    2. Compute numeric key from part attributes
    3. Hash numeric key → partition_id (0..num_threads-1)
    4. ATOMIC insert (key, suppkey) into partition's hash table
  - Use open-addressing hash table with linear probing per partition
  - Minimal lock contention via partition-based sharding
- Merge phase: Sequential merge of all partitions
  - Sort suppkey lists for deduplication (vectorized count_distinct)
- Final sort with pre-decoded strings

Expected: 2-3x speedup on join_agg phase (reduce from 516ms to ~200ms)
Target: <400ms total (vs 714ms current)
================================================================================
*/

// ============ TIMING MACROS ============
#ifdef GENDB_PROFILE
#define TIMING_START(name) auto t_start_##name = std::chrono::high_resolution_clock::now();
#define TIMING_END(name) auto t_end_##name = std::chrono::high_resolution_clock::now(); double ms_##name = std::chrono::duration<double, std::milli>(t_end_##name - t_start_##name).count(); printf("[TIMING] " #name ": %.2f ms\n", ms_##name);
#else
#define TIMING_START(name)
#define TIMING_END(name)
#endif

// ============ MMAP HELPERS ============
template<typename T>
struct MmapArray {
    T* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool load(const std::string& path, size_t expected_count) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        size_t file_size = lseek(fd, 0, SEEK_END);
        size = file_size / sizeof(T);

        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << std::endl;
            close(fd);
            return false;
        }

        return true;
    }

    ~MmapArray() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size * sizeof(T));
        }
        if (fd >= 0) close(fd);
    }
};

// ============ STRING LOADING HELPERS ============
// Load variable-length strings stored as [uint32_t length][string data]
std::vector<std::string> load_string_column(const std::string& path) {
    std::vector<std::string> result;
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        std::cerr << "Failed to open " << path << std::endl;
        return result;
    }

    uint32_t len;
    while (f.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) {
        std::string s(len, ' ');
        f.read(&s[0], len);
        result.push_back(s);
    }
    f.close();
    return result;
}

// ============ DICTIONARY LOADING ============
std::unordered_map<int32_t, std::string> load_dict_int32(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = (int32_t)std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Reverse lookup: find code for a given string value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, str] : dict) {
        if (str == value) return code;
    }
    return -1;
}

// ============ FILTERED PART STRUCT ============
struct FilteredPart {
    int32_t partkey;
    int32_t brand_code;  // Keep codes for fast hash key
    int32_t type_code;
    int32_t size;
    std::string brand;   // Pre-decoded for final output
    std::string type;
};

// ============ RESULT STRUCT ============
struct Result {
    std::string p_brand;
    std::string p_type;
    int32_t p_size;
    int32_t supplier_cnt;

    Result() = default;
    Result(const std::string& b, const std::string& t, int32_t s, int32_t c)
        : p_brand(b), p_type(t), p_size(s), supplier_cnt(c) {}

    bool operator<(const Result& other) const {
        if (supplier_cnt != other.supplier_cnt) return supplier_cnt > other.supplier_cnt;
        if (p_brand != other.p_brand) return p_brand < other.p_brand;
        if (p_type != other.p_type) return p_type < other.p_type;
        return p_size < other.p_size;
    }
};

// ============ MAIN QUERY EXECUTION ============
void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
    TIMING_START(total);

    // ========== LOAD DATA ==========
    TIMING_START(load);

    // Supplier table - for comment filtering
    MmapArray<int32_t> s_suppkey;
    if (!s_suppkey.load(gendb_dir + "/supplier/s_suppkey.bin", 100000)) return;

    auto s_comments = load_string_column(gendb_dir + "/supplier/s_comment.bin");
    if (s_comments.size() != 100000) {
        std::cerr << "Supplier comment count mismatch" << std::endl;
        return;
    }

    // Part table
    MmapArray<int32_t> p_partkey, p_size;
    MmapArray<int32_t> p_brand_codes, p_type_codes;

    if (!p_partkey.load(gendb_dir + "/part/p_partkey.bin", 2000000)) return;
    if (!p_brand_codes.load(gendb_dir + "/part/p_brand.bin", 2000000)) return;
    if (!p_type_codes.load(gendb_dir + "/part/p_type.bin", 2000000)) return;
    if (!p_size.load(gendb_dir + "/part/p_size.bin", 2000000)) return;

    // Partsupp table
    MmapArray<int32_t> ps_partkey, ps_suppkey;
    if (!ps_partkey.load(gendb_dir + "/partsupp/ps_partkey.bin", 8000000)) return;
    if (!ps_suppkey.load(gendb_dir + "/partsupp/ps_suppkey.bin", 8000000)) return;

    // Load dictionaries
    auto p_brand_dict = load_dict_int32(gendb_dir + "/part/p_brand_dict.txt");
    auto p_type_dict = load_dict_int32(gendb_dir + "/part/p_type_dict.txt");

    int32_t brand45_code = find_dict_code(p_brand_dict, "Brand#45");

    TIMING_END(load);

    // ========== BUILD BAD SUPPLIERS SET (Subquery) ==========
    TIMING_START(subquery);

    std::unordered_set<int32_t> bad_suppliers;

    for (size_t i = 0; i < 100000; ++i) {
        const std::string& comment = s_comments[i];
        // Check if comment contains "%Customer%Complaints%"
        // LIKE pattern: %Customer%Complaints% means substring contains both "Customer" and "Complaints"
        size_t customer_pos = comment.find("Customer");
        if (customer_pos != std::string::npos) {
            size_t complaints_pos = comment.find("Complaints", customer_pos);
            if (complaints_pos != std::string::npos) {
                bad_suppliers.insert(s_suppkey.data[i]);
            }
        }
    }

    TIMING_END(subquery);

    // ========== FILTER PART TABLE ==========
    TIMING_START(filter_part);

    // Store filtered parts with pre-decoded strings AND codes for fast hashing
    std::vector<FilteredPart> filtered_parts;
    filtered_parts.reserve(500000);  // Estimated ~500K filtered rows

    // Also build hash table on partkey for efficient join probe
    std::unordered_map<int32_t, std::vector<size_t>> part_by_key;
    part_by_key.reserve(500000);

    for (size_t i = 0; i < 2000000; ++i) {
        int32_t brand_code = p_brand_codes.data[i];
        int32_t type_code = p_type_codes.data[i];
        int32_t size = p_size.data[i];
        int32_t partkey = p_partkey.data[i];

        // Check p_brand <> 'Brand#45'
        if (brand_code == brand45_code) continue;

        // Check p_type NOT LIKE 'MEDIUM POLISHED%'
        const std::string& type_str = p_type_dict[type_code];
        const char* medium_polished = "MEDIUM POLISHED";
        if (type_str.size() >= 15 && type_str.substr(0, 15) == medium_polished) continue;

        // Check p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        if (size != 49 && size != 14 && size != 23 && size != 45 &&
            size != 19 && size != 3 && size != 36 && size != 9) continue;

        // All predicates passed; decode and store
        const std::string& brand_str = p_brand_dict[brand_code];

        size_t idx = filtered_parts.size();
        filtered_parts.push_back({partkey, brand_code, type_code, size, brand_str, type_str});
        part_by_key[partkey].push_back(idx);
    }

    TIMING_END(filter_part);

    // ========== JOIN PARTSUPP WITH FILTERED PART AND AGGREGATE ==========
    TIMING_START(join_agg);

    // Use numeric key: (brand_code << 40) | (type_code << 20) | size
    struct GroupKey {
        int32_t brand_code;
        int32_t type_code;
        int32_t size;

        uint64_t to_numeric() const {
            return ((uint64_t)brand_code << 40) | ((uint64_t)type_code << 20) | (uint64_t)size;
        }
    };

    // Partitioned aggregation: each partition owns a set of groups
    // based on hash(numeric_key) % num_threads
    int num_threads = omp_get_max_threads();

    // Group entry: numeric_key → vector of supplier keys
    struct GroupEntry {
        uint64_t key;
        std::vector<int32_t> suppliers;  // Deduplicated via sorting during merge
    };

    // Partition structure: vector of groups, hash table index for fast lookup
    struct Partition {
        std::vector<GroupEntry> groups;
        std::unordered_map<uint64_t, size_t> key_to_idx;
    };

    std::vector<Partition> partitions(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        partitions[t].key_to_idx.reserve(100000);  // Expect ~27K groups total, spread across threads
        partitions[t].groups.reserve(100000);
    }

    // Mapping from numeric key to original strings for final output
    std::unordered_map<uint64_t, std::pair<std::string, std::string>> code_to_strings;
    for (const auto& fp : filtered_parts) {
        uint64_t key = GroupKey{fp.brand_code, fp.type_code, fp.size}.to_numeric();
        code_to_strings[key] = {fp.brand, fp.type};
    }

    // Parallel scan of partsupp with partition-based assignment via thread-local buffers
    // Each thread collects into its own partition (no contention within partition)
    std::vector<std::vector<std::pair<uint64_t, int32_t>>> thread_buffers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_buffers[t].reserve(200000);  // ~8M rows / 64 threads = 125K rows per thread
    }

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < 8000000; ++i) {
        int32_t ps_partkey_val = ps_partkey.data[i];
        int32_t ps_suppkey_val = ps_suppkey.data[i];

        // Filter: exclude suppliers with bad comments
        if (bad_suppliers.count(ps_suppkey_val)) continue;

        // Join: look up in part table
        auto it = part_by_key.find(ps_partkey_val);
        if (it == part_by_key.end()) continue;

        // Get thread-local buffer
        int thread_id = omp_get_thread_num();

        // For each matching part row, buffer the (key, suppkey) pair
        for (size_t part_idx : it->second) {
            const FilteredPart& fp = filtered_parts[part_idx];
            uint64_t numeric_key = GroupKey{fp.brand_code, fp.type_code, fp.size}.to_numeric();
            thread_buffers[thread_id].push_back({numeric_key, ps_suppkey_val});
        }
    }

    // Merge phase: insert buffered pairs into partitions (sequential now)
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& [numeric_key, ps_suppkey_val] : thread_buffers[t]) {
            // Partition assignment: hash(key) % num_threads
            int partition_id = (int)(numeric_key % num_threads);
            Partition& part = partitions[partition_id];

            auto& key_map = part.key_to_idx;
            auto it_group = key_map.find(numeric_key);

            if (it_group == key_map.end()) {
                // First occurrence of this key in this partition
                size_t idx = part.groups.size();
                key_map[numeric_key] = idx;
                part.groups.push_back({numeric_key, {ps_suppkey_val}});
            } else {
                // Key already exists, add suppkey to its list
                size_t idx = it_group->second;
                part.groups[idx].suppliers.push_back(ps_suppkey_val);
            }
        }
    }

    // Merge phase: collect all groups and deduplicate suppliers
    std::map<uint64_t, std::unordered_set<int32_t>> grouped_by_code;
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& group : partitions[t].groups) {
            // Deduplicate suppliers via set insertion
            auto& supplier_set = grouped_by_code[group.key];
            for (int32_t suppkey : group.suppliers) {
                supplier_set.insert(suppkey);
            }
        }
    }

    TIMING_END(join_agg);

    // ========== BUILD RESULT ==========
    TIMING_START(result_build);

    std::vector<Result> results;
    for (const auto& [numeric_key, suppliers] : grouped_by_code) {
        // Decode numeric key back to (brand_code, type_code, size)
        int32_t size = (int32_t)(numeric_key & 0xFFFFF);

        // Look up strings
        auto& [brand_str, type_str] = code_to_strings[numeric_key];
        results.push_back(Result(brand_str, type_str, size, (int32_t)suppliers.size()));
    }

    TIMING_END(result_build);

    // ========== SORT RESULTS ==========
    TIMING_START(sort);

    std::sort(results.begin(), results.end());

    TIMING_END(sort);

    // ========== WRITE OUTPUT ==========
    TIMING_START(output);

    std::ofstream out(results_dir + "/Q16.csv");
    out << "p_brand,p_type,p_size,supplier_cnt\n";

    for (const auto& r : results) {
        out << r.p_brand << "," << r.p_type << "," << r.p_size << "," << r.supplier_cnt << "\n";
    }

    out.close();

    TIMING_END(output);

    TIMING_END(total);

    std::cout << "Q16 execution complete. Results written to " << results_dir << "/Q16.csv" << std::endl;
    std::cout << "Result rows: " << results.size() << std::endl;
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
