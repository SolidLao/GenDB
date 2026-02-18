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

/*
================================================================================
Q16: Parts/Supplier Relationship (ITERATION 3 - OPTIMIZED AGGREGATION)

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
   - Use partitioned hashing: key hash % num_threads determines ownership
   - Thread-local aggregation without locks
   - Each thread accumulates in its partition, no sequential merge
6. Extract results from all partitions
7. Sort by supplier_cnt DESC, p_brand, p_type, p_size

Physical Plan:
- Build bad_suppliers hash set via supplier subquery scan (100K rows)
- Scan part table with local filters, decode strings (2M rows → ~500K filtered)
  - Store as vector<FilteredPart> with pre-decoded brand/type strings
- Hash partkey→index for probe join (O(1) lookup)
- PARALLEL scan of partsupp (8M rows) with partitioned aggregation:
  - Hash group key to thread partition (key_hash % num_threads)
  - Each thread owns exclusive buckets in thread_partitions[thread_id]
  - No atomics or locks needed - partition ownership prevents contention
  - Reduced memory traffic vs. global lock
- Single-threaded extraction of results
- Single-threaded final sort

Key Optimizations:
1. Partitioned hash: distribute (brand, type, size) to 64 partitions (one per thread)
2. Eliminates sequential merge bottleneck from iteration 2
3. Reduces lock contention on aggregation (critical section only for partition ownership)
4. Improved cache locality - each thread accesses contiguous partition memory

Expected: 2-3x speedup on join_agg phase vs iteration 2
Target: ~250-350ms join_agg phase (from 748ms)
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

int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, str] : dict) {
        if (str == value) return code;
    }
    return -1;
}

// ============ FILTERED PART STRUCT ============
struct FilteredPart {
    int32_t partkey;
    std::string brand;
    std::string type;
    int32_t size;
};

// ============ GROUPING KEY STRUCT ============
struct GroupKey {
    std::string brand;
    std::string type;
    int32_t size;

    bool operator==(const GroupKey& other) const {
        return brand == other.brand && type == other.type && size == other.size;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const {
        size_t h = 0;
        for (char c : key.brand) h = h * 31 + c;
        for (char c : key.type) h = h * 31 + c;
        h = h * 31 + key.size;
        return h;
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

    MmapArray<int32_t> s_suppkey;
    if (!s_suppkey.load(gendb_dir + "/supplier/s_suppkey.bin", 100000)) return;

    auto s_comments = load_string_column(gendb_dir + "/supplier/s_comment.bin");
    if (s_comments.size() != 100000) {
        std::cerr << "Supplier comment count mismatch" << std::endl;
        return;
    }

    MmapArray<int32_t> p_partkey, p_size;
    MmapArray<int32_t> p_brand_codes, p_type_codes;

    if (!p_partkey.load(gendb_dir + "/part/p_partkey.bin", 2000000)) return;
    if (!p_brand_codes.load(gendb_dir + "/part/p_brand.bin", 2000000)) return;
    if (!p_type_codes.load(gendb_dir + "/part/p_type.bin", 2000000)) return;
    if (!p_size.load(gendb_dir + "/part/p_size.bin", 2000000)) return;

    MmapArray<int32_t> ps_partkey, ps_suppkey;
    if (!ps_partkey.load(gendb_dir + "/partsupp/ps_partkey.bin", 8000000)) return;
    if (!ps_suppkey.load(gendb_dir + "/partsupp/ps_suppkey.bin", 8000000)) return;

    auto p_brand_dict = load_dict_int32(gendb_dir + "/part/p_brand_dict.txt");
    auto p_type_dict = load_dict_int32(gendb_dir + "/part/p_type_dict.txt");

    int32_t brand45_code = find_dict_code(p_brand_dict, "Brand#45");

    TIMING_END(load);

    // ========== BUILD BAD SUPPLIERS SET (Subquery) ==========
    TIMING_START(subquery);

    std::unordered_set<int32_t> bad_suppliers;

    for (size_t i = 0; i < 100000; ++i) {
        const std::string& comment = s_comments[i];
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

    std::vector<FilteredPart> filtered_parts;
    filtered_parts.reserve(500000);

    std::unordered_map<int32_t, std::vector<size_t>> part_by_key;
    part_by_key.reserve(500000);

    for (size_t i = 0; i < 2000000; ++i) {
        int32_t brand_code = p_brand_codes.data[i];
        int32_t type_code = p_type_codes.data[i];
        int32_t size = p_size.data[i];
        int32_t partkey = p_partkey.data[i];

        if (brand_code == brand45_code) continue;

        const std::string& type_str = p_type_dict[type_code];
        const char* medium_polished = "MEDIUM POLISHED";
        if (type_str.size() >= 15 && type_str.substr(0, 15) == medium_polished) continue;

        if (size != 49 && size != 14 && size != 23 && size != 45 &&
            size != 19 && size != 3 && size != 36 && size != 9) continue;

        const std::string& brand_str = p_brand_dict[brand_code];

        size_t idx = filtered_parts.size();
        filtered_parts.push_back({partkey, brand_str, type_str, size});
        part_by_key[partkey].push_back(idx);
    }

    TIMING_END(filter_part);

    // ========== JOIN PARTSUPP WITH FILTERED PART AND AGGREGATE ==========
    TIMING_START(join_agg);

    int num_threads = omp_get_max_threads();

    // Partitioned aggregation: each thread owns buckets based on hash of group key
    std::vector<std::unordered_map<GroupKey, std::unordered_set<int32_t>, GroupKeyHash>> thread_partitions(num_threads);

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < 8000000; ++i) {
        int32_t ps_partkey_val = ps_partkey.data[i];
        int32_t ps_suppkey_val = ps_suppkey.data[i];

        if (bad_suppliers.count(ps_suppkey_val)) continue;

        auto it = part_by_key.find(ps_partkey_val);
        if (it == part_by_key.end()) continue;

        for (size_t part_idx : it->second) {
            const FilteredPart& fp = filtered_parts[part_idx];

            // Hash the group key to determine partition ownership
            GroupKey key{fp.brand, fp.type, fp.size};
            size_t key_hash = GroupKeyHash()(key);
            int partition_thread = key_hash % num_threads;

            // Critical section: protect writes to thread_partitions[partition_thread]
            // since we're writing from multiple threads to different partitions
            #pragma omp critical(update_partition)
            thread_partitions[partition_thread][key].insert(ps_suppkey_val);
        }
    }

    // Extract all results from partitions into a single map
    std::map<GroupKey, std::unordered_set<int32_t>, std::function<bool(const GroupKey&, const GroupKey&)>>
        grouped(
            [](const GroupKey& a, const GroupKey& b) {
                if (a.brand != b.brand) return a.brand < b.brand;
                if (a.type != b.type) return a.type < b.type;
                return a.size < b.size;
            }
        );

    for (int t = 0; t < num_threads; ++t) {
        for (const auto& [key, suppliers] : thread_partitions[t]) {
            auto& global_suppliers = grouped[key];
            global_suppliers.insert(suppliers.begin(), suppliers.end());
        }
    }

    TIMING_END(join_agg);

    // ========== BUILD RESULT ==========
    TIMING_START(result_build);

    std::vector<Result> results;
    for (const auto& [key, suppliers] : grouped) {
        results.push_back(Result(key.brand, key.type, key.size, (int32_t)suppliers.size()));
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
