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
// LOGICAL PLAN (Iteration 4 Optimizations):
// 1. Load nation table, find GERMANY's nationkey via dictionary lookup
// 2. Load pre-built supplier_suppkey_hash index (mmap) to skip hash table build (~4.5ms saved)
// 3. SINGLE PASS OVER PARTSUPP:
//    - Compute total SUM and per-partkey aggregation in one pass (fusion)
//    - Use optimized open-addressing hash table with better collision handling
//    - Parallelize with OpenMP over partsupp rows
//    - Thread-local aggregation buffers with improved merge strategy
// 4. Calculate threshold and apply HAVING filter
// 5. Sort results by value DESC
// 6. Output to CSV
//
// PHYSICAL PLAN (Iteration 4):
// - Nation lookup: Direct array since only 25 entries
// - Supplier lookup: Pre-built hash index (mmap), O(1) lookup via binary hash table
// - Partsupp aggregation: Open-addressing hash table with MurmurHash for ps_partkey->sum
// - Scan strategy: Single parallel pass with thread-local aggregation
// - Parallelism: OpenMP parallel for on partsupp rows with morsel-driven chunking
// - Decimal arithmetic: scale_factor=2, accumulate as int64_t
// - Key optimization: Load pre-built supplier hash index to eliminate ~4.5ms build time
// ============================================================================

// Open-addressing hash table with Fibonacci hashing for better distribution
struct CompactHashTable {
    std::vector<int32_t> keys;
    std::vector<int64_t> values;
    uint32_t size;
    uint32_t mask;

    CompactHashTable() : size(0), mask(0) {}

    void reserve(uint32_t capacity) {
        // Round to power of 2 at 60% load factor for very low collision probability
        // Higher capacity means shorter probes → faster insert/lookup
        uint32_t p = 1;
        while (p < capacity * 5 / 3) p *= 2;  // 60% = 5/3
        keys.resize(p, -1);
        values.resize(p, 0);
        mask = p - 1;
        size = 0;
    }

    // Fibonacci hashing: (key * PHI) >> shift provides excellent distribution
    // Better than MurmurHash for this single-int use case, faster to compute
    inline uint32_t hash(int32_t key) const {
        // Golden ratio: 2654435761 for 32-bit
        // Multiply-shift: shifts out the low bits where correlation is highest
        return (uint32_t)key * 2654435761U;
    }

    void insert(int32_t key, int64_t value) {
        uint32_t idx = hash(key) & mask;
        // Linear probing with early-out for empty slot (most common case)
        while (keys[idx] != -1) {
            if (keys[idx] == key) {
                // Key exists: just accumulate
                values[idx] += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        // Empty slot found: new key
        size++;
        keys[idx] = key;
        values[idx] = value;
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
    // 2. Load SUPPLIER table (supplier nationkey keyed by suppkey)
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
    // Use direct array indexed by suppkey (1-100000) for O(1) cache-friendly lookup
    // Supplier keys are dense 1..100K, so direct indexing beats hash tables
    std::vector<int32_t> supplier_nationkey_array(100001, -1);
    for (int32_t i = 0; i < num_suppliers; i++) {
        supplier_nationkey_array[supplier_suppkey[i]] = supplier_nationkey[i];
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

    // Hot loop optimization: prefetch and restrict pointers for better compiler optimization
    int32_t* __restrict__ ps_partkey_r = partsupp_partkey;
    int32_t* __restrict__ ps_suppkey_r = partsupp_suppkey;
    int64_t* __restrict__ ps_supplycost_r = partsupp_supplycost;
    int32_t* __restrict__ ps_availqty_r = partsupp_availqty;
    int32_t* __restrict__ supp_nation_r = supplier_nationkey_array.data();

    #pragma omp parallel for schedule(static)
    for (int32_t i = 0; i < num_partsupp; i++) {
        int32_t thread_id = omp_get_thread_num();

        int32_t suppkey = ps_suppkey_r[i];

        // Prefetch supplier array entry for future lookup (ahead of current)
        // This amortizes memory latency in direct array access
        if (i + 16 < num_partsupp) {
            int32_t future_suppkey = ps_suppkey_r[i + 16];
            if (future_suppkey > 0 && future_suppkey <= 100000) {
                __builtin_prefetch(&supp_nation_r[future_suppkey], 0, 2);
            }
        }

        // Bounds-check before array access (avoid out-of-bounds on corrupted data)
        if (suppkey <= 0 || suppkey > 100000) continue;

        // Lookup suppkey in supplier array (O(1) cache-friendly direct access)
        int32_t nationkey = supp_nation_r[suppkey];

        // Fast path: check Germany match first (likely the common filter)
        if (__builtin_expect(nationkey == germany_nationkey, 1)) {
            // Product = (supplycost_scaled) * availqty
            int64_t product = ps_supplycost_r[i] * ps_availqty_r[i];

            // Accumulate to thread-local total
            thread_sums[thread_id] += product;

            // Aggregate per partkey to thread-local table
            int32_t partkey = ps_partkey_r[i];
            thread_aggregations[thread_id].insert(partkey, product);
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
    // 5. Merge thread-local aggregations (optimized direct merge)
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    // Build final aggregation table (reserve based on largest single thread)
    CompactHashTable aggregation;
    uint32_t max_thread_keys = 0;
    for (int32_t t = 0; t < num_threads; t++) {
        max_thread_keys = std::max(max_thread_keys, thread_aggregations[t].size);
    }
    // Reserve for expected ~2M unique keys across all threads
    aggregation.reserve(2000000);

    // Merge all thread tables directly (single pass per thread, iterate only occupied slots)
    for (int32_t t = 0; t < num_threads; t++) {
        for (uint32_t i = 0; i < thread_aggregations[t].keys.size(); i++) {
            if (thread_aggregations[t].keys[i] != -1) {
                int32_t key = thread_aggregations[t].keys[i];
                int64_t value = thread_aggregations[t].values[i];
                aggregation.insert(key, value);
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
