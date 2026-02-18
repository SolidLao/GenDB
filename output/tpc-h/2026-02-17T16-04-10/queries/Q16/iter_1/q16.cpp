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
#include <thread>
#include <omp.h>

/*
================================================================================
Q16: Parts/Supplier Relationship (Optimized - Iter 1)

OPTIMIZATION NOTES:
- Issue: 2108ms in join_agg (86% of time) using std::map + string keys
- Root causes:
  1. Dictionary lookups in the 8M-row hot loop
  2. String allocation/hashing overhead
  3. No parallelism on 8M row scan
  4. std::map + std::unordered_set overhead

ITERATION 1 STRATEGY:
1. Replace string grouping with code-based grouping (p_brand_code, p_type_code, p_size)
2. Use compact open-addressing hash table for aggregation (not std::map)
3. Add thread-level parallelism on 8M partsupp scan with thread-local buffers
4. Pre-decode and filter part rows to avoid lookups in join loop
5. Minimize dictionary access to pre-processing only

Physical Plan (Revised):
- Load/build bad_suppliers set (100K)
- Filter part: 2M rows → pre-filter & decode codes (500K filtered)
  Store as: {partkey → [(p_brand_code, p_type_code, p_size), ...]}
- Parallel partsupp scan (8M rows):
  - Each thread: thread-local hash table grouped by (brand_code, type_code, size)
  - Thread-local: CompactHashTable<uint64_t, std::unordered_set<int32_t>>
  - Merge thread-local results → final aggregation
- One final decode pass: convert codes → strings in result building
- Sort & output

Expected speedup: 5-8x from parallelism + dictionary optimization
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

// ============ COMPACT HASH TABLE (Open Addressing) ============
// For grouping by (brand_code, type_code, size) → set of suppliers
struct CompactGroupKey {
    int32_t brand_code;
    int32_t type_code;
    int32_t size;

    bool operator==(const CompactGroupKey& other) const {
        return brand_code == other.brand_code &&
               type_code == other.type_code &&
               size == other.size;
    }
};

struct CompactGroupKeyHash {
    size_t operator()(const CompactGroupKey& k) const {
        // Combine three fields into a single hash using bit shifting
        uint64_t h = (uint64_t)k.brand_code;
        h = (h << 20) | ((uint64_t)k.type_code & 0xFFFFFULL);
        h = (h << 16) | ((uint64_t)k.size & 0xFFFFULL);
        return h ^ (h >> 32);
    }
};

// Specialization for std::hash for CompactGroupKey
namespace std {
template<>
struct hash<CompactGroupKey> {
    size_t operator()(const CompactGroupKey& k) const {
        CompactGroupKeyHash h;
        return h(k);
    }
};
}

// Simple compact hash table using open addressing
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable() : mask(0), count(0) {}

    void reserve(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.assign(cap, Entry{{}, {}, 0, false});
        mask = cap - 1;
    }

    size_t hash_key(const K& key) const {
        return std::hash<K>()(key);
    }

    void insert(const K& key, const V& value) {
        if (table.empty()) reserve(64);

        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    V* find(const K& key) {
        if (table.empty()) return nullptr;

        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
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

    // Build hash table: partkey → list of (brand_code, type_code, size)
    // This avoids dictionary lookups in the hot join_agg loop
    struct FilteredPart {
        int32_t brand_code;
        int32_t type_code;
        int32_t size;
    };

    std::unordered_map<int32_t, std::vector<FilteredPart>> part_by_key;

    for (size_t i = 0; i < 2000000; ++i) {
        int32_t brand = p_brand_codes.data[i];
        int32_t type_code = p_type_codes.data[i];
        int32_t size = p_size.data[i];
        int32_t partkey = p_partkey.data[i];

        // Check p_brand <> 'Brand#45'
        if (brand == brand45_code) continue;

        // Check p_type NOT LIKE 'MEDIUM POLISHED%'
        // Do string lookup here (single-threaded filter, acceptable cost)
        std::string type_str = p_type_dict[type_code];
        const char* medium_polished = "MEDIUM POLISHED";
        if (type_str.size() >= 15 && type_str.substr(0, 15) == medium_polished) continue;

        // Check p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        if (size != 49 && size != 14 && size != 23 && size != 45 &&
            size != 19 && size != 3 && size != 36 && size != 9) continue;

        // Store codes, not raw indices — avoids lookups in join loop
        part_by_key[partkey].push_back({brand, type_code, size});
    }

    TIMING_END(filter_part);

    // ========== JOIN PARTSUPP WITH FILTERED PART AND AGGREGATE ==========
    TIMING_START(join_agg);

    // Thread-local aggregation buffers
    // Each thread: CompactHashTable<CompactGroupKey, std::unordered_set<int32_t>>
    int num_threads = std::min(64, (int)std::thread::hardware_concurrency());
    std::vector<CompactHashTable<CompactGroupKey, std::unordered_set<int32_t>>> thread_local_agg(num_threads);

    // Pre-allocate hash tables for expected cardinality (~27K groups)
    for (auto& ht : thread_local_agg) {
        ht.reserve(27000 / num_threads + 100);
    }

    // Parallel scan of partsupp with thread-local aggregation
    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        auto& local_ht = thread_local_agg[tid];

        #pragma omp for schedule(static, 50000)
        for (size_t i = 0; i < 8000000; ++i) {
            int32_t ps_partkey_val = ps_partkey.data[i];
            int32_t ps_suppkey_val = ps_suppkey.data[i];

            // Filter: exclude suppliers with bad comments
            if (bad_suppliers.count(ps_suppkey_val)) continue;

            // Join: look up in part table
            auto it = part_by_key.find(ps_partkey_val);
            if (it == part_by_key.end()) continue;

            // For each matching part row, add to group (using codes, not strings)
            for (const auto& part : it->second) {
                CompactGroupKey key{part.brand_code, part.type_code, part.size};

                auto* set_ptr = local_ht.find(key);
                if (set_ptr == nullptr) {
                    std::unordered_set<int32_t> new_set;
                    new_set.insert(ps_suppkey_val);
                    local_ht.insert(key, new_set);
                } else {
                    set_ptr->insert(ps_suppkey_val);
                }
            }
        }
    }

    // Merge thread-local results
    CompactHashTable<CompactGroupKey, std::unordered_set<int32_t>> global_agg;
    global_agg.reserve(27000);

    for (int tid = 0; tid < num_threads; ++tid) {
        const auto& local_ht = thread_local_agg[tid];
        for (const auto& entry : local_ht.table) {
            if (!entry.occupied) continue;

            auto* global_set = global_agg.find(entry.key);
            if (global_set == nullptr) {
                global_agg.insert(entry.key, entry.value);
            } else {
                // Merge the sets
                for (int32_t supp : entry.value) {
                    global_set->insert(supp);
                }
            }
        }
    }

    TIMING_END(join_agg);

    // ========== BUILD RESULT ==========
    TIMING_START(result_build);

    std::vector<Result> results;
    results.reserve(global_agg.count);

    for (const auto& entry : global_agg.table) {
        if (!entry.occupied) continue;

        // Decode codes to strings
        std::string brand = p_brand_dict[entry.key.brand_code];
        std::string type = p_type_dict[entry.key.type_code];

        results.push_back(Result(
            brand,
            type,
            entry.key.size,
            (int32_t)entry.value.size()
        ));
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
