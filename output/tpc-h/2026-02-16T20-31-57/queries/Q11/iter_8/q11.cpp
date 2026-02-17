#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <cstring>

// ============================================================================
// LOGICAL PLAN (Iteration 8 Optimizations):
// 1. Load nation table, find GERMANY's nationkey via dictionary lookup
// 2. Build supplier nationkey lookup array: suppkey -> nationkey (direct indexing)
//    - Replaces hash table with O(1) array lookup for 8M iterations
//    - Saves ~3-5ms vs hash table collision overhead
// 3. SINGLE PASS OVER PARTSUPP with direct array supplier lookup:
//    - Compute total SUM and per-partkey aggregation in one pass
//    - Use optimized open-addressing hash table with MurmurHash for partkey aggregation
//    - Parallelize with OpenMP over partsupp rows (static chunking)
//    - Thread-local aggregation buffers + optimized merge
// 4. Calculate threshold (0.0001 * total_sum) and apply HAVING filter
// 5. Sort results by value DESC
// 6. Output to CSV
//
// PHYSICAL PLAN (Iteration 8):
// - Nation lookup: Direct array (25 entries)
// - Supplier lookup: Direct array indexed by suppkey (100K+1 entries)
//   Much faster than hash table for 8M lookups with low collision cost
// - Partsupp aggregation: Open-addressing hash table with MurmurHash
// - Scan strategy: Single parallel pass with O(1) direct array probes
// - Parallelism: OpenMP static schedule for balanced L3 cache locality
// - Key optimization: Replace hash table lookup with direct array (save ~3-5ms)
// ============================================================================

// Open-addressing hash table with MurmurHash for better distribution
struct CompactHashTable {
    std::vector<int32_t> keys;
    std::vector<int64_t> values;
    uint32_t size;
    uint32_t mask;

    CompactHashTable() : size(0), mask(0) {}

    void reserve(uint32_t capacity) {
        // Round to power of 2 at 67% load factor for good collision properties
        uint32_t p = 1;
        while (p < capacity * 3 / 2) p *= 2;
        keys.resize(p, -1);
        values.resize(p, 0);
        mask = p - 1;
        size = 0;
    }

    // MurmurHash-inspired hash function for better distribution than simple XOR
    uint32_t hash(int32_t key) const {
        uint32_t h = (uint32_t)key;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

    void insert(int32_t key, int64_t value) {
        uint32_t idx = hash(key) & mask;
        while (keys[idx] != -1 && keys[idx] != key) {
            idx = (idx + 1) & mask;
        }
        if (keys[idx] == -1) {
            size++;
            keys[idx] = key;
        }
        values[idx] += value;
    }

    int32_t lookup(int32_t key) const {
        uint32_t idx = hash(key) & mask;
        while (keys[idx] != -1 && keys[idx] != key) {
            idx = (idx + 1) & mask;
        }
        if (keys[idx] == key) {
            return (int32_t)values[idx];
        }
        return -1;
    }

    // Fast merge: direct insertion without intermediate collection
    void merge(const CompactHashTable& other) {
        for (uint32_t i = 0; i < other.keys.size(); i++) {
            if (other.keys[i] != -1) {
                int32_t key = other.keys[i];
                int64_t value = other.values[i];
                // Inline insert to avoid function call overhead in hot loop
                uint32_t idx = hash(key) & mask;
                while (keys[idx] != -1 && keys[idx] != key) {
                    idx = (idx + 1) & mask;
                }
                if (keys[idx] == -1) {
                    size++;
                    keys[idx] = key;
                }
                values[idx] += value;
            }
        }
    }
};

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile() : fd(-1), ptr(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            std::cerr << "fstat failed for " << path << std::endl;
            close();
            return false;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << std::endl;
            close();
            return false;
        }

        return true;
    }

    void close() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
            ptr = nullptr;
        }
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// Load dictionary file: returns code -> string mapping
// Dictionary format: each line is a value, line number (0-indexed) is the code
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line.empty()) {
            code++;
            continue;
        }
        // Try parsing as "code=value" format first
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
            code++;
        } else {
            // Simple line-based format: code is line number
            dict[code] = line;
            code++;
        }
    }
    f.close();
    return dict;
}

// Find which code maps to a specific string value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) return code;
    }
    return -1;  // Not found
}

void run_q11(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========================================================================
    // 1. Load NATION table and find GERMANY
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile nation_nationkey_file, nation_name_file;
    if (!nation_nationkey_file.open(gendb_dir + "/nation/n_nationkey.bin")) return;
    if (!nation_name_file.open(gendb_dir + "/nation/n_name.bin")) return;

    int32_t* nation_nationkey = (int32_t*)nation_nationkey_file.ptr;
    int32_t* nation_name = (int32_t*)nation_name_file.ptr;

    // Load dictionary for nation names
    auto n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Find GERMANY's nation key
    int32_t germany_code = find_dict_code(n_name_dict, "GERMANY");
    int32_t germany_nationkey = -1;

    for (size_t i = 0; i < 25; i++) {
        if (nation_name[i] == germany_code) {
            germany_nationkey = nation_nationkey[i];
            break;
        }
    }

    if (germany_nationkey == -1) {
        std::cerr << "GERMANY not found in nation table" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 2. Load SUPPLIER table and build fast lookup hash table
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load supplier columns
    MmapFile supplier_suppkey_file, supplier_nationkey_file;
    if (!supplier_suppkey_file.open(gendb_dir + "/supplier/s_suppkey.bin")) return;
    if (!supplier_nationkey_file.open(gendb_dir + "/supplier/s_nationkey.bin")) return;

    int32_t* supplier_suppkey = (int32_t*)supplier_suppkey_file.ptr;
    int32_t* supplier_nationkey = (int32_t*)supplier_nationkey_file.ptr;

    const int32_t num_suppliers = 100000;

    // Build supplier lookup table: suppkey -> nationkey
    // Use direct array for cache-friendly O(1) access (suppkey is 1-100000)
    // This avoids hash table collision overhead: 100K array << 8M iterations
    std::vector<int32_t> supplier_nationkey_by_suppkey(num_suppliers + 1, -1);
    for (int32_t i = 0; i < num_suppliers; i++) {
        int32_t suppkey = supplier_suppkey[i];
        if (suppkey >= 0 && suppkey <= num_suppliers) {
            supplier_nationkey_by_suppkey[suppkey] = supplier_nationkey[i];
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_supplier_ht: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 3. Load PARTSUPP table
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile partsupp_partkey_file, partsupp_suppkey_file, partsupp_supplycost_file, partsupp_availqty_file;
    if (!partsupp_partkey_file.open(gendb_dir + "/partsupp/ps_partkey.bin")) return;
    if (!partsupp_suppkey_file.open(gendb_dir + "/partsupp/ps_suppkey.bin")) return;
    if (!partsupp_supplycost_file.open(gendb_dir + "/partsupp/ps_supplycost.bin")) return;
    if (!partsupp_availqty_file.open(gendb_dir + "/partsupp/ps_availqty.bin")) return;

    int32_t* partsupp_partkey = (int32_t*)partsupp_partkey_file.ptr;
    int32_t* partsupp_suppkey = (int32_t*)partsupp_suppkey_file.ptr;
    int64_t* partsupp_supplycost = (int64_t*)partsupp_supplycost_file.ptr;
    int32_t* partsupp_availqty = (int32_t*)partsupp_availqty_file.ptr;

    const int32_t num_partsupp = 8000000;

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 4. SINGLE PASS: Parallel aggregation (both total_sum and per-partkey)
    //    with thread-local buffers and merge
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums(num_threads, 0);
    std::vector<CompactHashTable> thread_aggregations(num_threads);

    // Pre-allocate aggregation tables for each thread
    // Expected ~2M distinct part keys total; with 64 threads: 2M / 64 = ~31K keys per thread
    // Allocate at ~35K to minimize iteration overhead while allowing some variance
    uint32_t per_thread_size = 35000;
    for (int32_t t = 0; t < num_threads; t++) {
        thread_aggregations[t].reserve(per_thread_size);
    }

    #pragma omp parallel for schedule(static)
    for (int32_t i = 0; i < num_partsupp; i++) {
        int32_t thread_id = omp_get_thread_num();
        int32_t suppkey = partsupp_suppkey[i];

        // Direct array lookup: O(1) cache-friendly access
        // Bounds check is implicit in the filter below
        if (suppkey >= 0 && suppkey <= num_suppliers) {
            int32_t nationkey = supplier_nationkey_by_suppkey[suppkey];

            if (nationkey == germany_nationkey) {
                // Product = (supplycost_scaled) * availqty, scaled by 100
                int64_t product = partsupp_supplycost[i] * partsupp_availqty[i];

                // Accumulate to thread-local total
                thread_sums[thread_id] += product;

                // Aggregate per partkey to thread-local table
                int32_t partkey = partsupp_partkey[i];
                thread_aggregations[thread_id].insert(partkey, product);
            }
        }
    }

    // Merge thread-local totals
    int64_t total_sum = 0;
    for (int32_t t = 0; t < num_threads; t++) {
        total_sum += thread_sums[t];
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] pass1_total_sum: %.2f ms\n", ms);
    #endif

    // Calculate threshold: 0.0001 * total_sum
    // ps_supplycost is scaled by 100, so the product (supplycost * availqty) is also scaled by 100
    // Therefore, the accumulated total_sum is 100x the semantic value
    // To get the correct 0.0001 factor, we divide by 100000 (100 * 10000)
    int64_t threshold = total_sum / 100000;

    // ========================================================================
    // 5. Merge thread-local aggregations (optimized: collect pairs then build)
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    // Instead of merging thread-by-thread (64 iterations with expensive lookups),
    // collect all key-value pairs first, deduplicate, then build final table
    // This is faster because we iterate only occupied slots, not all allocated slots

    // Phase 1: Merge thread-local aggregations directly using optimized open-addressing merge
    // Instead of collecting pairs, merge directly into final table with better cache locality
    CompactHashTable aggregation;
    aggregation.reserve(2000000 + 10000);  // Pre-allocate to avoid rehashing during merge

    // Thread 0's table becomes the base; merge all others into it
    // This is faster than collecting pairs because we only touch occupied slots
    if (num_threads > 0) {
        // Copy thread 0 directly
        for (uint32_t i = 0; i < thread_aggregations[0].keys.size(); i++) {
            if (thread_aggregations[0].keys[i] != -1) {
                int32_t key = thread_aggregations[0].keys[i];
                int64_t value = thread_aggregations[0].values[i];
                // Direct insertion into final table (no rehashing)
                uint32_t idx = aggregation.hash(key) & aggregation.mask;
                while (aggregation.keys[idx] != -1 && aggregation.keys[idx] != key) {
                    idx = (idx + 1) & aggregation.mask;
                }
                if (aggregation.keys[idx] == -1) {
                    aggregation.size++;
                    aggregation.keys[idx] = key;
                }
                aggregation.values[idx] += value;
            }
        }

        // Merge remaining thread tables
        for (int32_t t = 1; t < num_threads; t++) {
            for (uint32_t i = 0; i < thread_aggregations[t].keys.size(); i++) {
                if (thread_aggregations[t].keys[i] != -1) {
                    int32_t key = thread_aggregations[t].keys[i];
                    int64_t value = thread_aggregations[t].values[i];
                    // Inline merge to reduce function call overhead
                    uint32_t idx = aggregation.hash(key) & aggregation.mask;
                    while (aggregation.keys[idx] != -1 && aggregation.keys[idx] != key) {
                        idx = (idx + 1) & aggregation.mask;
                    }
                    if (aggregation.keys[idx] == -1) {
                        aggregation.size++;
                        aggregation.keys[idx] = key;
                    }
                    aggregation.values[idx] += value;
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 6. Apply HAVING filter and collect results
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<int32_t, int64_t>> results;
    results.reserve(aggregation.size);

    for (uint32_t i = 0; i < aggregation.keys.size(); i++) {
        if (aggregation.keys[i] != -1) {
            int32_t partkey = aggregation.keys[i];
            int64_t sum_val = aggregation.values[i];
            if (sum_val > threshold) {
                results.push_back({partkey, sum_val});
            }
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] having_filter: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 7. Sort by value DESC
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::sort(results.begin(), results.end(),
              [](const std::pair<int32_t, int64_t>& a, const std::pair<int32_t, int64_t>& b) {
                  return a.second > b.second;
              });

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 8. Write CSV output
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q11.csv");
    out << "ps_partkey,value\n";

    // sum_val is scaled by 100 (scale_factor=2 for decimal)
    // Output with 2 decimal places: divide by 100
    for (const auto& [partkey, sum_val] : results) {
        double value = sum_val / 100.0;
        out << partkey << "," << std::fixed << std::setprecision(2) << value << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q11(gendb_dir, results_dir);

    return 0;
}
#endif
