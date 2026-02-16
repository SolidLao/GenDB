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
#include <bitset>

namespace {

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

// ============ DICTIONARY LOADING ============
std::unordered_map<int8_t, std::string> load_dict_int8(const std::string& dict_path) {
    std::unordered_map<int8_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int8_t code = (int8_t)std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

std::unordered_map<int16_t, std::string> load_dict_int16(const std::string& dict_path) {
    std::unordered_map<int16_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int16_t code = (int16_t)std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Reverse lookup: find code for a given string value
int8_t find_brand_code(const std::unordered_map<int8_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, str] : dict) {
        if (str == value) return code;
    }
    return -1;
}

int16_t find_type_code(const std::unordered_map<int16_t, std::string>& dict, const std::string& prefix) {
    for (const auto& [code, str] : dict) {
        if (str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix) {
            return code;
        }
    }
    return -1;
}

// ============ HASH MULTI-VALUE INDEX LOADING ============
// Generic hash multi-value index for any key type
template<typename KeyType>
struct HashMultiValueIndex {
    std::vector<KeyType> keys;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> counts;
    std::vector<uint32_t> positions;
    uint32_t table_size = 0;

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index " << path << std::endl;
            return false;
        }

        // Read header
        uint32_t num_unique;
        read(fd, &num_unique, sizeof(num_unique));
        read(fd, &table_size, sizeof(table_size));

        keys.resize(table_size);
        offsets.resize(table_size);
        counts.resize(table_size);

        // Read hash entries
        for (uint32_t i = 0; i < table_size; ++i) {
            KeyType key;
            uint32_t offset, count;
            if (read(fd, &key, sizeof(key)) < 0) break;
            if (read(fd, &offset, sizeof(offset)) < 0) break;
            if (read(fd, &count, sizeof(count)) < 0) break;
            keys[i] = key;
            offsets[i] = offset;
            counts[i] = count;
        }

        // Read positions array
        uint32_t pos_count;
        if (read(fd, &pos_count, sizeof(pos_count)) >= 0) {
            positions.resize(pos_count);
            read(fd, positions.data(), pos_count * sizeof(uint32_t));
        }

        close(fd);
        return true;
    }

    // Look up key in hash table, return offset and count (or {UINT32_MAX, 0} if not found)
    std::pair<uint32_t, uint32_t> lookup(KeyType key) const {
        // Simple linear probing hash lookup
        // Assumes keys[i] = 0 means empty slot (reserve this convention)
        uint32_t hash_val = std::hash<KeyType>()(key);
        uint32_t idx = hash_val % table_size;

        for (uint32_t i = 0; i < table_size; ++i) {
            uint32_t probe_idx = (idx + i) % table_size;
            // In a valid hash table index, non-empty slots have valid offset/count
            // For int32_t keys, we assume key == 0 could be valid, so check counts instead
            if (counts[probe_idx] > 0 || offsets[probe_idx] != UINT32_MAX) {
                if (keys[probe_idx] == key) {
                    return {offsets[probe_idx], counts[probe_idx]};
                }
            }
        }
        return {UINT32_MAX, 0};
    }
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
} // end anonymous namespace

void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
    TIMING_START(total);

    // ========== LOAD DATA ==========
    TIMING_START(load);

    // Part table
    MmapArray<int32_t> p_partkey, p_size;
    MmapArray<int8_t> p_brand_codes;
    MmapArray<int16_t> p_type_codes;

    if (!p_partkey.load(gendb_dir + "/part/p_partkey.bin", 2000000)) return;
    if (!p_brand_codes.load(gendb_dir + "/part/p_brand.bin", 2000000)) return;
    if (!p_type_codes.load(gendb_dir + "/part/p_type.bin", 2000000)) return;
    if (!p_size.load(gendb_dir + "/part/p_size.bin", 2000000)) return;

    // Partsupp table
    MmapArray<int32_t> ps_partkey, ps_suppkey;
    if (!ps_partkey.load(gendb_dir + "/partsupp/ps_partkey.bin", 8000000)) return;
    if (!ps_suppkey.load(gendb_dir + "/partsupp/ps_suppkey.bin", 8000000)) return;

    // Supplier table - for comment filtering
    MmapArray<int32_t> s_suppkey;
    if (!s_suppkey.load(gendb_dir + "/supplier/s_suppkey.bin", 100000)) return;

    // Load supplier comments for filtering
    // For TPC-H Q16: exclude suppliers with "%Customer%Complaints%" LIKE pattern
    // Use a simple boolean array for O(1) lookup (supplier IDs are 1-based, 1-100000)
    std::vector<bool> bad_suppliers(100001, false);  // Index by supplier ID directly

    {
        // Load supplier comments: format is [num_suppliers:uint32_t] [offsets:uint32_t[num_suppliers]] [strings]
        int fd = open((gendb_dir + "/supplier/s_comment.bin").c_str(), O_RDONLY);
        if (fd >= 0) {
            lseek(fd, 0, SEEK_END);
            off_t file_size = lseek(fd, 0, SEEK_CUR);
            lseek(fd, 0, SEEK_SET);

            if (file_size > 0) {
                char* comment_data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
                if (comment_data != MAP_FAILED) {
                    uint32_t num_suppliers_in_file = *(uint32_t*)comment_data;
                    uint32_t* offset_array = (uint32_t*)(comment_data + 4);
                    char* string_data = comment_data + 4 + num_suppliers_in_file * 4;

                    // Check each supplier
                    for (uint32_t i = 0; i < num_suppliers_in_file; i++) {
                        uint32_t str_offset = offset_array[i];
                        uint32_t next_offset = (i < num_suppliers_in_file - 1) ?
                            offset_array[i+1] :
                            (file_size - (4 + num_suppliers_in_file * 4));
                        uint32_t str_len = next_offset - str_offset;

                        // Check if comment contains both "Customer" and "Complaints" using raw string ops
                        // Avoid string object creation overhead
                        const char* comment_str = string_data + str_offset;
                        const char* customer_pos = std::search(comment_str, comment_str + str_len,
                                                               "Customer", "Customer" + 8);
                        const char* complaint_pos = std::search(comment_str, comment_str + str_len,
                                                               "Complaints", "Complaints" + 10);

                        if (customer_pos != comment_str + str_len &&
                            complaint_pos != comment_str + str_len) {
                            // Supplier ID is 1-indexed (supplier 1 has s_suppkey 1, etc.)
                            bad_suppliers[i + 1] = true;
                        }
                    }

                    munmap(comment_data, file_size);
                }
            }
            close(fd);
        }
    }

    // Load dictionaries
    auto p_brand_dict = load_dict_int8(gendb_dir + "/part/p_brand_dict.txt");
    auto p_type_dict = load_dict_int16(gendb_dir + "/part/p_type_dict.txt");

    int8_t brand45_code = find_brand_code(p_brand_dict, "Brand#45");

    TIMING_END(load);

    // ========== BUILD HASH TABLE ON PART (filtered) ==========
    TIMING_START(filter_part);

    // Pre-compute LIKE pattern rejection set for types
    // Cache which type codes match "MEDIUM POLISHED%" to avoid repeated string lookups
    std::unordered_set<int16_t> rejected_types;
    for (const auto& [code, type_str] : p_type_dict) {
        if (type_str.size() >= 16 && type_str.substr(0, 15) == "MEDIUM POLISHED") {
            rejected_types.insert(code);
        }
    }

    // Pre-compute size filter set for fast membership test
    static const std::unordered_set<int32_t> valid_sizes = {49, 14, 23, 45, 19, 3, 36, 9};

    // Parallel filter with thread-local accumulation
    int num_filter_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, std::vector<size_t>>> local_part_by_key(num_filter_threads);
    for (int t = 0; t < num_filter_threads; ++t) {
        local_part_by_key[t].reserve(500000 / num_filter_threads + 1);
    }

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < 2000000; ++i) {
        int8_t brand = p_brand_codes.data[i];
        int16_t type_code = p_type_codes.data[i];
        int32_t size = p_size.data[i];
        int32_t partkey = p_partkey.data[i];

        // Check p_brand <> 'Brand#45'
        if (brand == brand45_code) continue;

        // Check p_type NOT LIKE 'MEDIUM POLISHED%' (using pre-computed set)
        if (rejected_types.count(type_code)) continue;

        // Check p_size IN (49, 14, 23, 45, 19, 3, 36, 9) (using pre-computed set)
        if (!valid_sizes.count(size)) continue;

        int thread_id = omp_get_thread_num();
        local_part_by_key[thread_id][partkey].push_back(i);
    }

    // Merge thread-local results into global map
    std::unordered_map<int32_t, std::vector<size_t>> part_by_key;
    part_by_key.reserve(500000);
    for (int t = 0; t < num_filter_threads; ++t) {
        for (auto& [partkey, indices] : local_part_by_key[t]) {
            auto& global_indices = part_by_key[partkey];
            if (global_indices.empty()) {
                global_indices = std::move(indices);
            } else {
                global_indices.insert(global_indices.end(), indices.begin(), indices.end());
            }
        }
    }

    TIMING_END(filter_part);

    // ========== PROCESS PARTSUPP AND JOIN WITH PART ==========
    TIMING_START(join);

    // Use a flat map structure: (encoded key) → set of suppkeys
    // Encoded key = (int8_t brand, int16_t type, int32_t size)
    // This avoids expensive string conversions in the hot path
    struct EncodedKey {
        int8_t brand_code;
        int16_t type_code;
        int32_t size;

        bool operator==(const EncodedKey& other) const {
            return brand_code == other.brand_code &&
                   type_code == other.type_code &&
                   size == other.size;
        }

        bool operator<(const EncodedKey& other) const {
            if (brand_code != other.brand_code) return brand_code < other.brand_code;
            if (type_code != other.type_code) return type_code < other.type_code;
            return size < other.size;
        }
    };

    struct EncodedKeyHash {
        size_t operator()(const EncodedKey& k) const {
            // Combine three fields into a single hash
            size_t h1 = std::hash<int8_t>()(k.brand_code);
            size_t h2 = std::hash<int16_t>()(k.type_code);
            size_t h3 = std::hash<int32_t>()(k.size);
            return h1 ^ (h2 << 16) ^ (h3 << 32);
        }
    };

    // Use a vector-based set for suppkeys to reduce memory allocation overhead
    // Store suppkeys in sorted vectors instead of unordered_set for cache efficiency
    using SuppkeySet = std::vector<int32_t>;

    // Use a single global unordered_map with pre-sized capacity
    // Key: EncodedKey, Value: vector of suppkeys (DISTINCT supplier count)
    std::unordered_map<EncodedKey, SuppkeySet, EncodedKeyHash> encoded_grouped;
    encoded_grouped.reserve(200000);  // Pre-allocate for expected groups

    // Parallel scan of partsupp with atomic-safe aggregation
    int num_threads = omp_get_max_threads();

    // Thread-local aggregation to reduce contention
    std::vector<std::unordered_map<EncodedKey, std::vector<int32_t>, EncodedKeyHash>> local_grouped(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        local_grouped[t].reserve(200000 / num_threads + 1);
    }

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < 8000000; ++i) {
        int32_t ps_partkey_val = ps_partkey.data[i];
        int32_t ps_suppkey_val = ps_suppkey.data[i];

        // Check if supplier is in bad list (O(1) array lookup)
        if (ps_suppkey_val > 0 && ps_suppkey_val <= 100000 && bad_suppliers[ps_suppkey_val]) continue;

        // Look up in part table
        auto it = part_by_key.find(ps_partkey_val);
        if (it == part_by_key.end()) continue;

        // For each matching part row
        int thread_id = omp_get_thread_num();
        for (size_t part_idx : it->second) {
            int8_t brand_code = p_brand_codes.data[part_idx];
            int16_t type_code = p_type_codes.data[part_idx];
            int32_t size = p_size.data[part_idx];

            EncodedKey key{brand_code, type_code, size};
            local_grouped[thread_id][key].push_back(ps_suppkey_val);
        }
    }

    // Merge thread-local results into global map with deduplication
    for (int t = 0; t < num_threads; ++t) {
        for (auto& [enc_key, local_suppliers] : local_grouped[t]) {
            // Sort and deduplicate local suppliers
            std::sort(local_suppliers.begin(), local_suppliers.end());
            local_suppliers.erase(std::unique(local_suppliers.begin(), local_suppliers.end()), local_suppliers.end());

            auto& global_suppliers = encoded_grouped[enc_key];

            // Merge sorted vectors efficiently
            if (global_suppliers.empty()) {
                global_suppliers = std::move(local_suppliers);
            } else {
                // Merge two sorted vectors
                std::vector<int32_t> merged;
                merged.reserve(global_suppliers.size() + local_suppliers.size());
                std::set_union(global_suppliers.begin(), global_suppliers.end(),
                               local_suppliers.begin(), local_suppliers.end(),
                               std::back_inserter(merged));
                global_suppliers = std::move(merged);
            }
        }
    }

    TIMING_END(join);

    // ========== BUILD RESULT ==========
    TIMING_START(agg);

    std::vector<Result> results;
    results.reserve(encoded_grouped.size());  // Pre-allocate

    for (const auto& [enc_key, suppliers] : encoded_grouped) {
        // Decode keys to strings for output
        std::string brand = p_brand_dict[enc_key.brand_code];
        std::string type = p_type_dict[enc_key.type_code];
        int32_t size = enc_key.size;
        // suppliers is now a vector of sorted, deduplicated suppkeys
        int32_t supplier_cnt = (int32_t)suppliers.size();

        results.push_back(Result(brand, type, size, supplier_cnt));
    }

    TIMING_END(agg);

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
    std::cout << "Total rows: " << results.size() << std::endl;
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
