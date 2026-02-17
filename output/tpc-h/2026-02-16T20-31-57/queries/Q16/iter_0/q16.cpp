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
 * Q16: Parts/Supplier Relationship
 *
 * LOGICAL PLAN:
 * ============
 * 1. Subquery: Pre-compute supplier keys with "%Customer%Complaints%" in s_comment
 *    - Supplier table (100K rows) → dictionary decode s_comment → pattern match → hash set of bad suppkeys
 *    - Estimated: ~10K suppliers match pattern
 *
 * 2. Part table scan & filter:
 *    - Full scan part (2M rows)
 *    - Filter: p_brand != 'Brand#45' (dictionary code lookup)
 *    - Filter: p_type NOT LIKE 'MEDIUM POLISHED%' (pattern match on decoded strings)
 *    - Filter: p_size IN (49, 14, 23, 45, 19, 3, 36, 9) (direct integer match)
 *    - Estimated cardinality: ~400K rows pass filters (selectivity ~20%)
 *
 * 3. Join with partsupp:
 *    - Use pre-built partsupp_partkey_hash index (multi-value hash for 8M rows)
 *    - For each qualifying part key, lookup all partsupp entries
 *    - Apply anti-join filter: ps_suppkey NOT IN bad_suppliers
 *
 * 4. Aggregation:
 *    - GROUP BY (p_brand, p_type, p_size) with DISTINCT COUNT(ps_suppkey)
 *    - Use compound key hash map: (brand_code, type_code, size) → set of distinct suppkeys
 *    - Estimated groups: ~10K (low selectivity cardinality)
 *
 * 5. Sort output:
 *    - ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
 *    - ~10K rows to sort (small enough for single-threaded sort)
 *
 * PHYSICAL PLAN:
 * ==============
 * - Subquery: Hash anti-join using unordered_set<int32_t> for suppkeys
 * - Part scan: Parallel full scan with dictionary lookups, OpenMP for 64 cores
 * - Join: Use mmap'd partsupp_partkey_hash index, linear probe for ps_suppkey entries
 * - Aggregation: Hash map from (brand, type, size) → unordered_set<suppkey> for DISTINCT count
 * - Output: Sort struct array by supplier_cnt DESC, then lexicographic
 * - Parallelism: Part scan, partsupp traversal within part scan loop (nested parallel regions)
 */

struct HashMultiValueIndex {
    struct Entry {
        int32_t key;
        uint32_t offset;  // offset into positions array
        uint32_t count;   // number of matching positions
    };

    uint32_t num_unique;
    uint32_t table_size;
    Entry* entries;       // [table_size] slots
    uint32_t* positions;  // packed position list
    uint32_t positions_count;

    // Load from mmap'd file
    static HashMultiValueIndex* load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index: " << path << std::endl;
            return nullptr;
        }

        size_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);

        void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);

        if (mapped == MAP_FAILED) {
            std::cerr << "Failed to mmap index: " << path << std::endl;
            return nullptr;
        }

        auto idx = new HashMultiValueIndex();
        uint8_t* ptr = (uint8_t*)mapped;

        idx->num_unique = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);
        idx->table_size = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);

        // Entries: [key, offset, count] = 12 bytes each
        idx->entries = (Entry*)ptr;
        ptr += idx->table_size * sizeof(Entry);

        idx->positions_count = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);
        idx->positions = (uint32_t*)ptr;

        return idx;
    }

    // Get all position indices for a key using linear probe
    std::vector<uint32_t> get(int32_t key) const {
        std::vector<uint32_t> result;
        uint32_t slot = hash_fn(key) % table_size;

        for (uint32_t i = 0; i < table_size; i++) {
            uint32_t idx_slot = (slot + i) % table_size;
            if (entries[idx_slot].key == key) {
                result.reserve(entries[idx_slot].count);
                for (uint32_t j = 0; j < entries[idx_slot].count; j++) {
                    result.push_back(positions[entries[idx_slot].offset + j]);
                }
                break;
            }
        }

        return result;
    }

private:
    static uint32_t hash_fn(int32_t key) {
        // MurmurHash-like
        uint32_t h = key;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
};

struct HashSingleIndex {
    struct Entry {
        int32_t key;
        uint32_t pos;
    };

    uint32_t num_entries;
    Entry* entries;
    void* mapped_data;
    size_t mapped_size;

    static HashSingleIndex* load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return nullptr;
        }

        size_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);

        void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);

        if (mapped == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            return nullptr;
        }

        auto idx = new HashSingleIndex();
        idx->mapped_data = mapped;
        idx->mapped_size = file_size;

        uint8_t* ptr = (uint8_t*)mapped;
        idx->num_entries = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);
        idx->entries = (Entry*)ptr;
        return idx;
    }

    uint32_t lookup(int32_t key) const {
        // Linear probe
        uint32_t slot = hash_fn(key) % num_entries;
        for (uint32_t i = 0; i < num_entries; i++) {
            uint32_t idx_slot = (slot + i) % num_entries;
            if (entries[idx_slot].key == key) {
                return entries[idx_slot].pos;
            }
        }
        return UINT32_MAX;
    }

private:
    static uint32_t hash_fn(int32_t key) {
        uint32_t h = key;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
};

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

    // Prepare size filter set
    std::unordered_set<int32_t> valid_sizes = {49, 14, 23, 45, 19, 3, 36, 9};

    // Pre-compute which p_type codes match "MEDIUM POLISHED%" pattern
    std::unordered_set<int32_t> bad_type_codes;
    for (auto& [code, str] : p_type_dict) {
        if (matches_pattern(str)) {
            bad_type_codes.insert(code);
        }
    }

    // ========== SUBQUERY: Build bad suppliers set ==========
    #ifdef GENDB_PROFILE
    auto t_subquery = std::chrono::high_resolution_clock::now();
    #endif

    size_t s_suppkey_count, s_comment_count;
    int32_t* s_suppkey = load_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", s_suppkey_count);
    int32_t* s_comment = load_column<int32_t>(gendb_dir + "/supplier/s_comment.bin", s_comment_count);

    auto s_comment_dict = load_dict(gendb_dir + "/supplier/s_comment_dict.txt");

    std::unordered_set<int32_t> bad_suppliers;
    bad_suppliers.reserve(100000);

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
                bad_suppliers.insert(s_suppkey[i]);
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
    // Strategy: scan partsupp, lookup part columns via index, apply part filters, aggregate
    std::unordered_map<AggKey, AggValue, AggKeyHash> agg_map;
    agg_map.reserve(50000);

    for (size_t i = 0; i < ps_partkey_count; i++) {
        int32_t partkey = ps_partkey[i];
        int32_t suppkey = ps_suppkey[i];

        // Anti-join filter: skip if suppkey in bad_suppliers
        if (bad_suppliers.count(suppkey)) continue;

        // Lookup part row via map
        auto it = partkey_to_pos.find(partkey);
        if (it == partkey_to_pos.end()) continue;
        uint32_t part_pos = it->second;

        // Apply part filters
        if (p_brand[part_pos] == brand45_code) continue;

        // Filter on p_type NOT LIKE 'MEDIUM POLISHED%'
        if (bad_type_codes.count(p_type[part_pos])) continue;

        // Filter on p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        if (!valid_sizes.count(p_size[part_pos])) continue;

        // Aggregate
        AggKey key;
        key.brand_code = p_brand[part_pos];
        key.type_code = p_type[part_pos];
        key.size = p_size[part_pos];

        agg_map[key].distinct_suppkeys.insert(suppkey);
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
