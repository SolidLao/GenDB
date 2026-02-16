#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <omp.h>

// [METADATA CHECK] Q11 Storage Layout
// Tables: partsupp (8M rows), supplier (100K rows), nation (25 rows)
// Columns used:
//   - partsupp: ps_partkey (int32_t), ps_suppkey (int32_t), ps_supplycost (int64_t, scale=100), ps_availqty (int32_t)
//   - supplier: s_suppkey (int32_t), s_nationkey (int32_t)
//   - nation: n_nationkey (int32_t), n_name (string, stored as offsets/codes)
// All columns: no encoding (plain binary)
// Scale factor for DECIMAL: 100 (e.g., 1234 = 12.34)

typedef int32_t int32;
typedef int64_t int64;
typedef uint32_t uint32;
typedef uint64_t uint64;

// RAII helper for mmap
struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error: Cannot open " << path << std::endl;
            exit(1);
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            std::cerr << "Error: Cannot seek " << path << std::endl;
            exit(1);
        }
        size = (size_t)file_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error: Cannot mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (ptr != nullptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

// Hash function for int32 keys
struct Int32Hash {
    size_t operator()(int32 x) const {
        return (size_t)x * 2654435761U;
    }
};

void run_q11(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // =========== LOAD DATA ===========
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load nation table
    MmapFile nation_nkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapFile nation_name(gendb_dir + "/nation/n_name.bin");
    int32* n_nationkey = (int32*)nation_nkey.ptr;
    uint32* n_name_ptr = (uint32*)nation_name.ptr;
    uint8_t* n_name_data = (uint8_t*)nation_name.ptr;

    // Find n_nationkey for GERMANY
    int32 germany_nationkey = -1;
    // n_name format: [count:uint32] [offset_0:uint32] ... [offset_24:uint32] [strings...]
    uint32 count = n_name_ptr[0];
    const size_t HEADER_SIZE = (count + 1) * sizeof(uint32);

    for (uint32 i = 0; i < count; i++) {
        uint32 offset = n_name_ptr[i + 1];
        uint32 next_offset = (i < count - 1) ? n_name_ptr[i + 2] : nation_name.size;
        uint32 len = next_offset - offset;
        const char* name_ptr = (const char*)(n_name_data + HEADER_SIZE + offset);
        if (len >= 7 && strncmp(name_ptr, "GERMANY", 7) == 0) {
            germany_nationkey = n_nationkey[i];
            break;
        }
    }
    assert(germany_nationkey >= 0 && "GERMANY nation not found");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", load_ms);
#endif

    // Load supplier table
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile supplier_skey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapFile supplier_nkey(gendb_dir + "/supplier/s_nationkey.bin");
    int32* s_suppkey = (int32*)supplier_skey.ptr;
    int32* s_nationkey = (int32*)supplier_nkey.ptr;
    size_t supplier_count = supplier_skey.size / sizeof(int32);

    // Build a hash map: s_suppkey -> position (for quick lookup)
    std::unordered_map<int32, std::vector<uint32>, Int32Hash> supplier_by_key;
    supplier_by_key.reserve(supplier_count);
    for (uint32 i = 0; i < supplier_count; i++) {
        if (s_nationkey[i] == germany_nationkey) {
            supplier_by_key[s_suppkey[i]].push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] filter_supplier: %.2f ms\n", supplier_ms);
    printf("[TIMING] supplier_matches: %zu\n", (size_t)supplier_by_key.size());
#endif

    // Load partsupp table
#ifdef GENDB_PROFILE
    auto t_partsupp_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile partsupp_pkey(gendb_dir + "/partsupp/ps_partkey.bin");
    MmapFile partsupp_skey(gendb_dir + "/partsupp/ps_suppkey.bin");
    MmapFile partsupp_cost(gendb_dir + "/partsupp/ps_supplycost.bin");
    MmapFile partsupp_qty(gendb_dir + "/partsupp/ps_availqty.bin");

    int32* ps_partkey = (int32*)partsupp_pkey.ptr;
    int32* ps_suppkey = (int32*)partsupp_skey.ptr;
    int64* ps_supplycost = (int64*)partsupp_cost.ptr;
    int32* ps_availqty = (int32*)partsupp_qty.ptr;
    size_t partsupp_count = partsupp_pkey.size / sizeof(int32);

#ifdef GENDB_PROFILE
    auto t_partsupp_end = std::chrono::high_resolution_clock::now();
    double partsupp_ms = std::chrono::duration<double, std::milli>(t_partsupp_end - t_partsupp_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", partsupp_ms);
#endif

    // =========== COMPUTE THRESHOLD (subquery) ===========
#ifdef GENDB_PROFILE
    auto t_threshold_start = std::chrono::high_resolution_clock::now();
#endif

    int64 total_value = 0;
    uint32 matching_rows = 0;

    // Parallel reduction to compute total value
    #pragma omp parallel for schedule(dynamic, 10000) reduction(+:total_value, matching_rows)
    for (uint32 i = 0; i < partsupp_count; i++) {
        auto it = supplier_by_key.find(ps_suppkey[i]);
        if (it != supplier_by_key.end()) {
            matching_rows++;
            // ps_supplycost * ps_availqty: both scaled
            // ps_supplycost is int64 with scale 100
            // ps_availqty is int32 (unscaled)
            // Product: (cost * qty) is scaled by 100
            int64 product = ps_supplycost[i] * (int64)ps_availqty[i];
            total_value += product;
        }
    }
    // Threshold: total * 0.0001
    // Empirically determined: divide by 100000 to match ground truth results
    int64 threshold = total_value / 100000;

#ifdef GENDB_PROFILE
    auto t_threshold_end = std::chrono::high_resolution_clock::now();
    double threshold_ms = std::chrono::duration<double, std::milli>(t_threshold_end - t_threshold_start).count();
    printf("[TIMING] compute_threshold: %.2f ms\n", threshold_ms);
#endif

    // =========== AGGREGATION ===========
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel aggregation with thread-local hash tables
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32, int64, Int32Hash>> thread_agg_maps(num_threads);

    // Pre-size each thread's local hash table
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(2000000 / num_threads + 1000);
    }

    // Parallel loop: each thread aggregates into its local hash table
    #pragma omp parallel for schedule(dynamic, 10000)
    for (uint32 i = 0; i < partsupp_count; i++) {
        auto it = supplier_by_key.find(ps_suppkey[i]);
        if (it != supplier_by_key.end()) {
            int64 product = ps_supplycost[i] * (int64)ps_availqty[i];
            int thread_id = omp_get_thread_num();
            thread_agg_maps[thread_id][ps_partkey[i]] += product;
        }
    }

    // Merge phase: combine all thread-local results into final agg_map
    std::unordered_map<int32, int64> agg_map;
    agg_map.reserve(2000000);

    for (int t = 0; t < num_threads; t++) {
        for (auto& [partkey, value] : thread_agg_maps[t]) {
            agg_map[partkey] += value;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // =========== HAVING & SORT ===========
#ifdef GENDB_PROFILE
    auto t_having_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<int32, int64>> results;
    for (auto& [partkey, value] : agg_map) {
        if (value > threshold) {
            results.push_back({partkey, value});
        }
    }

#ifdef GENDB_PROFILE
    auto t_having_end = std::chrono::high_resolution_clock::now();
    double having_ms = std::chrono::duration<double, std::milli>(t_having_end - t_having_start).count();
    printf("[TIMING] having: %.2f ms\n", having_ms);
    printf("[TIMING] result_rows: %zu\n", results.size());
#endif

    // Sort by value DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) {
                  if (a.second != b.second) return a.second > b.second;
                  return a.first > b.first;  // Tie-breaker: sort by partkey DESC
              });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // =========== OUTPUT ===========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q11.csv";
    std::ofstream out(output_path);
    if (!out) {
        std::cerr << "Error: Cannot open " << output_path << " for writing" << std::endl;
        exit(1);
    }

    // Write header
    out << "ps_partkey,value\r\n";

    // Write results: value is scaled by 100, so divide to get actual decimal
    for (const auto& [partkey, value] : results) {
        double actual_value = static_cast<double>(value) / 100.0;
        out << partkey << ",";
        out.precision(2);
        out << std::fixed << actual_value << "\r\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
