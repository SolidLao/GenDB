#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <omp.h>
#include <vector>
#include <iomanip>
#include <algorithm>

/*
QUERY PLAN FOR Q6: ITERATION 5 - STALL RECOVERY REWRITE

ROOT CAUSE OF 22.7x GAP (131ms vs 6ms):
  1. Division in hot loop: each matched row does (double / 10000.0) → ~20 cycles per division
  2. All 60M rows scanned, no early filtering: 60M branches on random predicates
  3. Zone map loaded but not effectively used to skip non-matching blocks

ARCHITECTURE REDESIGN:
  1. INTEGER-ONLY TIGHT LOOP: Accumulate as int64_t, divide once at end
  2. BLOCK-LEVEL ZONE MAP PRUNING: Skip entire 100K-row blocks with one min/max check
  3. SELECTIVE PREDICATE ORDERING: Most selective first (quantity < 24 filters ~95%)
  4. BITWISE PREDICATE FUSION: Use & instead of && to avoid branch misprediction
  5. BLOCK-LEVEL PARALLELIZATION: Each OpenMP thread handles independent blocks

Logical Plan:
  - Table: lineitem (59,986,052 rows, sorted by l_shipdate)
  - Predicates (in selectivity order):
    1. l_shipdate BETWEEN 1994-01-01 AND 1995-01-01 [ZONE MAP: block-level skip]
    2. l_quantity < 24 [high selectivity: ~5% pass, filter early]
    3. l_discount BETWEEN 0.05 AND 0.07 [medium selectivity: ~20% of remaining]
  - Aggregation: SUM(l_extendedprice * l_discount) as int64_t, convert to double at end
  - Single result row, no GROUP BY

Physical Plan:
  1. Load zone map for l_shipdate: binary format [uint32_t num_blocks] + [int32_t min, max] per block
  2. Load 4 columns via mmap: l_shipdate, l_discount, l_quantity, l_extendedprice
  3. Block-level loop: for each block, check zone map before scanning rows
     a. IF block's max_date < 1994-01-01 OR min_date >= 1995-01-01: SKIP entire block
     b. ELSE: scan 100K rows in block, apply remaining predicates
  4. Tight inner loop: fused integer predicates, accumulate products as int64_t
  5. Parallel execution: OpenMP distributes blocks across threads
  6. Merge: thread-local int64_t sums → final double result
  7. Output: divide by 10000 once

Data Encoding:
  - l_shipdate: int32_t (epoch days)
  - l_discount: int64_t (scaled by 100)
  - l_quantity: int64_t (scaled by 100)
  - l_extendedprice: int64_t (scaled by 100)
  - Result: (int64_t sum) / 10000.0 → double

Zone Map Binary Format:
  [uint32_t num_blocks] followed by [int32_t min_val, int32_t max_val] per block
  Each zone covers one contiguous 100,000-row block

Key Optimizations:
  1. Integer accumulation: NO division in loop → 10-20x speedup
  2. Block skipping: Skip 5-10% of blocks → 5-10% faster
  3. Bitwise predicates: Reduces branch misprediction
  4. Block-level parallelism: Each thread handles independent blocks
*/

// Epoch day constants
constexpr int32_t DATE_1994_01_01 = 8766;
constexpr int32_t DATE_1995_01_01 = 9131;

// Discount bounds (scaled by 100)
constexpr int64_t DISCOUNT_MIN = 5;    // 0.05
constexpr int64_t DISCOUNT_MAX = 7;    // 0.07

// Quantity bound (scaled by 100)
constexpr int64_t QUANTITY_MAX = 2400;  // 24.00

// Block size (from storage guide)
constexpr uint32_t BLOCK_SIZE = 100000;

// Zone map entry (from storage guide)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
};

// Helper function to load column via mmap
template<typename T>
T* load_column(const std::string& file_path, size_t& num_rows) {
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << file_path << std::endl;
        exit(1);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat " << file_path << std::endl;
        close(fd);
        exit(1);
    }

    size_t file_size = st.st_size;
    num_rows = file_size / sizeof(T);

    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << file_path << std::endl;
        close(fd);
        exit(1);
    }

    close(fd);
    return data;
}

// Helper function to unmap column
template<typename T>
void unload_column(T* data, size_t num_rows) {
    if (data && data != MAP_FAILED) {
        munmap(data, num_rows * sizeof(T));
    }
}

// Load zone map: binary format is [uint32_t num_blocks] + [int32_t min, max] per block
struct ZoneMap {
    uint32_t num_blocks;
    ZoneMapEntry* entries;
    uint8_t* raw_data;  // Keep for cleanup
    size_t file_size;
};

ZoneMap load_zone_map(const std::string& zonemap_file) {
    int fd = open(zonemap_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open zone map " << zonemap_file << std::endl;
        exit(1);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat zone map " << zonemap_file << std::endl;
        close(fd);
        exit(1);
    }

    size_t file_size = st.st_size;
    uint8_t* raw_data = (uint8_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (raw_data == MAP_FAILED) {
        std::cerr << "Failed to mmap zone map " << zonemap_file << std::endl;
        close(fd);
        exit(1);
    }

    close(fd);

    // Parse: [uint32_t num_blocks] then [int32_t min, int32_t max] per block
    uint32_t num_blocks = *(uint32_t*)raw_data;
    ZoneMapEntry* entries = (ZoneMapEntry*)(raw_data + sizeof(uint32_t));

    return {num_blocks, entries, raw_data, file_size};
}

void unload_zone_map(const ZoneMap& zm) {
    if (zm.raw_data) {
        munmap(zm.raw_data, zm.file_size);
    }
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load columns and zone map
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_rows = 0;
    int32_t* l_shipdate = load_column<int32_t>(
        gendb_dir + "/lineitem/l_shipdate.bin", num_rows);
    int64_t* l_discount = load_column<int64_t>(
        gendb_dir + "/lineitem/l_discount.bin", num_rows);
    int64_t* l_quantity = load_column<int64_t>(
        gendb_dir + "/lineitem/l_quantity.bin", num_rows);
    int64_t* l_extendedprice = load_column<int64_t>(
        gendb_dir + "/lineitem/l_extendedprice.bin", num_rows);

    // Load zone map for l_shipdate
    ZoneMap zm = load_zone_map(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Parallel scan + filter + aggregate with block-level zone map pruning
    // Accumulate as int64_t to avoid division in tight loop
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums(num_threads, 0);

    // Block-level parallelism: each block processed independently
    #pragma omp parallel for schedule(dynamic, 1)
    for (uint32_t block_idx = 0; block_idx < zm.num_blocks; block_idx++) {
        // Zone map pruning: skip blocks entirely outside the date range
        // Skip condition: block_max < DATE_1994_01_01 OR block_min >= DATE_1995_01_01
        if (zm.entries[block_idx].max_val < DATE_1994_01_01 ||
            zm.entries[block_idx].min_val >= DATE_1995_01_01) {
            continue;  // Entire block is outside date range, skip
        }

        // Compute row range for this block
        uint32_t block_start = block_idx * BLOCK_SIZE;
        uint32_t block_end = std::min(block_idx * BLOCK_SIZE + BLOCK_SIZE, (uint32_t)num_rows);

        // Thread-local accumulator (no synchronization needed)
        int tid = omp_get_thread_num();
        int64_t local_sum = thread_sums[tid];

        // Tight inner loop: integer-only predicates, no division
        // Predicate order (most selective first): quantity < 24, then discount range, then shipdate (partially filtered by zone map)
        for (uint32_t i = block_start; i < block_end; i++) {
            // Fused predicates using bitwise AND to avoid branch misprediction
            // This is equivalent to (a && b && c && d && e) but avoids short-circuit branches
            int64_t shipdate_ok = (l_shipdate[i] >= DATE_1994_01_01) && (l_shipdate[i] < DATE_1995_01_01);
            int64_t quantity_ok = (l_quantity[i] < QUANTITY_MAX);
            int64_t discount_ok = (l_discount[i] >= DISCOUNT_MIN) && (l_discount[i] <= DISCOUNT_MAX);

            if (shipdate_ok & quantity_ok & discount_ok) {
                // Product: l_extendedprice * l_discount
                // Both scaled by 100, so product is scaled by 10000
                // Keep as int64_t, divide at the end for 10-20x speedup
                local_sum += l_extendedprice[i] * l_discount[i];
            }
        }

        thread_sums[tid] = local_sum;
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
#endif

    // Merge thread-local results
#ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t final_sum_int = 0;
    for (int t = 0; t < num_threads; t++) {
        final_sum_int += thread_sums[t];
    }

    // Convert result: divide by 10000 once at the end
    double final_sum = (double)final_sum_int / 10000.0;

#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", merge_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q6.csv";
    std::ofstream out(output_file);
    out << "revenue\n";
    out << std::fixed << std::setprecision(4) << final_sum << "\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // Cleanup
    unload_column(l_shipdate, num_rows);
    unload_column(l_discount, num_rows);
    unload_column(l_quantity, num_rows);
    unload_column(l_extendedprice, num_rows);
    unload_zone_map(zm);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
