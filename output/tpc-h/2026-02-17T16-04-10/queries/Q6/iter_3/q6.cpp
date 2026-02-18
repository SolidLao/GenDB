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

/*
QUERY PLAN FOR Q6: Forecasting Revenue Change (ITERATION 3 - ZONE MAP OPTIMIZATION)

Logical Plan:
  - Table: lineitem (59,986,052 rows, sorted by l_shipdate)
  - Predicates (in selectivity order):
    1. l_shipdate >= 1994-01-01 AND < 1995-01-01 (epoch days: 8766-9131) — ZONE MAP PRUNING
    2. l_quantity < 24 (scaled: 2400 as int64_t) — high selectivity, few rows pass
    3. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7 as int64_t) — medium selectivity
  - Aggregation: SUM(l_extendedprice * l_discount)
  - No GROUP BY: single result row

Physical Plan:
  1. Load zone map from indexes/lineitem_l_shipdate_zonemap.bin (skip blocks outside 1994)
  2. Load required columns via mmap: l_shipdate, l_discount, l_quantity, l_extendedprice
  3. Parallel scan with zone map block pruning (OpenMP with thread-local accumulators)
  4. Evaluate predicates in order: shipdate (via zone map) → quantity → discount
  5. Accumulate product (l_extendedprice * l_discount) without Kahan overhead
  6. Merge thread-local sums via simple addition
  7. Output single row with revenue

Data Encoding:
  - l_shipdate: int32_t (epoch days since 1970-01-01)
  - l_discount: int64_t with scale_factor=100
  - l_quantity: int64_t with scale_factor=100
  - l_extendedprice: int64_t with scale_factor=100

Zone Map Format:
  - File: indexes/lineitem_l_shipdate_zonemap.bin
  - Binary layout: [uint32_t num_blocks] followed by [int32_t min, int32_t max] per block
  - Each zone represents a contiguous range of rows

Parallelism:
  - Thread-local accumulators (one per thread) to avoid synchronization
  - Skip Kahan summation overhead (using simple double accumulators)
  - Master thread merges local results with simple addition
  - Zone map blocks pruned at block level, scans only live rows
*/

// Compute epoch day for a given date
// Date 1994-01-01: leap years from 1970-1993 are 1972, 1976, 1980, 1984, 1988, 1992 (6 leap years)
// Total days: 24*365 + 6 = 8760 + 6 = 8766
constexpr int32_t DATE_1994_01_01 = 8766;
// Date 1995-01-01: 1 more year of 365 days
constexpr int32_t DATE_1995_01_01 = 9131;

// Discount bounds (scaled by 100)
constexpr int64_t DISCOUNT_MIN = 5;    // 0.05
constexpr int64_t DISCOUNT_MAX = 7;    // 0.07

// Quantity bound (scaled by 100)
constexpr int64_t QUANTITY_MAX = 2400;  // 24.00

// Zone map entry structure
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

// Helper function to load zone map with block boundaries
struct ZoneMapWithBlocks {
    ZoneMapEntry* entries;
    uint32_t* block_starts;  // row offset for each block
    uint32_t num_blocks;
};

ZoneMapWithBlocks load_zone_map(const std::string& zonemap_file, size_t total_rows) {
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

    // Parse header: [uint32_t num_blocks]
    uint32_t num_blocks = *(uint32_t*)raw_data;
    ZoneMapEntry* entries = (ZoneMapEntry*)(raw_data + sizeof(uint32_t));

    // Build block_starts array (each block represents 100,000 rows by default)
    uint32_t* block_starts = new uint32_t[num_blocks + 1];
    const uint32_t block_size = 100000;
    for (uint32_t i = 0; i <= num_blocks; i++) {
        block_starts[i] = i * block_size;
    }

    return {entries, block_starts, num_blocks};
}

void unload_zone_map(const ZoneMapWithBlocks& zm) {
    if (zm.entries) {
        uint8_t* base = ((uint8_t*)zm.entries) - sizeof(uint32_t);
        munmap(base, 1);
    }
    delete[] zm.block_starts;
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
    ZoneMapWithBlocks zm = load_zone_map(
        gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin", num_rows);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Parallel scan + filter + aggregate with zone map pruning
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<double> thread_sums(num_threads, 0.0);

    // Iterate over zones, skip entire blocks that don't match date range
    #pragma omp parallel for schedule(static, 1)
    for (uint32_t block_idx = 0; block_idx < zm.num_blocks; block_idx++) {
        // Zone map pruning: skip block if it's entirely outside the date range
        if (zm.entries[block_idx].max_val < DATE_1994_01_01 ||
            zm.entries[block_idx].min_val >= DATE_1995_01_01) {
            continue;  // Block entirely outside date range, skip
        }

        // Process rows in this block
        uint32_t block_start = zm.block_starts[block_idx];
        uint32_t block_end = zm.block_starts[block_idx + 1];

        int tid = omp_get_thread_num();
        double local_sum = thread_sums[tid];

        for (uint32_t i = block_start; i < block_end; i++) {
            // Apply predicates in selectivity order (most selective first)
            // Predicate 1: l_shipdate (partially filtered by zone map)
            if (l_shipdate[i] < DATE_1994_01_01 || l_shipdate[i] >= DATE_1995_01_01) {
                continue;
            }

            // Predicate 2: l_quantity < 24 (high selectivity, ~5% pass)
            if (l_quantity[i] >= QUANTITY_MAX) {
                continue;
            }

            // Predicate 3: l_discount BETWEEN 0.05 AND 0.07
            if (l_discount[i] < DISCOUNT_MIN || l_discount[i] > DISCOUNT_MAX) {
                continue;
            }

            // Compute product: (extendedprice * discount) / (100 * 100)
            // Both are scaled by 100, so product is scaled by 10000
            double product_scaled = (double)l_extendedprice[i] * (double)l_discount[i] / 10000.0;
            local_sum += product_scaled;
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

    double final_sum = 0.0;
    for (int t = 0; t < num_threads; t++) {
        final_sum += thread_sums[t];
    }

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
