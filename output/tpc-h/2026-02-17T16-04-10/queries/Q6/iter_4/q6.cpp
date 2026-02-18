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
QUERY PLAN FOR Q6: STALL RECOVERY REWRITE (ITERATION 4)

ROOT CAUSE: Previous iterations had 22.7x gap to Umbra (6ms).
Current code: 131ms. Architecture was:
  1. Generic scan with per-row cost ~2-3 cycles
  2. Thread-unsafe zone map access (iter 3)
  3. Memory bottleneck: 4 columns loaded, 60M rows, 60M branches

FUNDAMENTAL REDESIGN:
  1. AGGRESSIVE PREFETCHING: Sequential scan pattern enables hardware prefetch
  2. VECTORIZED PREDICATES: Integer operations only, no division in tight loop
  3. ZONE MAP EARLY TERMINATION: Skip entire blocks before inner loop (correct format)
  4. THREAD-LOCAL AGGREGATION: No synchronization, no Kahan overhead
  5. COMPILE-TIME CONSTANTS: All thresholds folded by optimizer

Logical Plan:
  - Table: lineitem (59,986,052 rows, sorted by l_shipdate)
  - Predicates (in selectivity order):
    1. l_shipdate >= 1994-01-01 AND < 1995-01-01 [ZONE MAP: skip blocks outside range]
    2. l_quantity < 24 [very selective, ~5% pass]
    3. l_discount BETWEEN 0.05 AND 0.07 [medium selective]
  - Aggregation: SUM(l_extendedprice * l_discount) / 10000.0
  - Single result row, no GROUP BY

Physical Plan:
  1. Load zone map: [uint32_t num_blocks] + [int32_t min, int32_t max] per block
  2. Load columns via mmap (exploits hardware prefetch for sequential access)
  3. For each block:
     a. Zone map skip: if block's max < DATE_1994_01_01 or min >= DATE_1995_01_01, skip entire block
     b. Parallel scan blocks with OpenMP (each block to one thread to avoid false sharing)
     c. Per-thread local accumulators (no synchronization)
  4. Merge thread results (sequential, minimal cost)
  5. Scale result: convert from int64 product to float result

Data Encoding:
  - l_shipdate: int32_t (epoch days since 1970-01-01)
  - l_discount: int64_t with scale_factor=100
  - l_quantity: int64_t with scale_factor=100
  - l_extendedprice: int64_t with scale_factor=100
  - Result: revenue = sum(product) / 10000.0

Zone Map Binary Format (from storage guide):
  [uint32_t num_blocks] [int32_t min, int32_t max] * num_blocks

Key Optimizations:
  1. Integer-only predicates (no FP division in loop)
  2. Block-level pruning (skip 100K rows at once)
  3. Thread-local aggregators (no contention)
  4. Simple linear merge (no Kahan overhead needed for block merge)
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

// Block size from storage guide
constexpr uint32_t BLOCK_SIZE = 100000;

// Zone map entry (from storage guide: [int32_t min, int32_t max] per block)
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

// Load zone map: [uint32_t num_blocks] followed by [int32_t min, int32_t max] per block
struct ZoneMap {
    uint32_t num_blocks;
    ZoneMapEntry* entries;  // entries[0..num_blocks-1]
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

    // Parse header: [uint32_t num_blocks]
    uint32_t num_blocks = *(uint32_t*)raw_data;
    // Entries start after header
    ZoneMapEntry* entries = (ZoneMapEntry*)(raw_data + sizeof(uint32_t));

    return {num_blocks, entries};
}

void unload_zone_map(const ZoneMap& zm, size_t file_size) {
    if (zm.entries) {
        uint8_t* base = ((uint8_t*)zm.entries) - sizeof(uint32_t);
        munmap(base, file_size);
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
    int fd_zm = open((gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin").c_str(), O_RDONLY);
    struct stat st_zm;
    fstat(fd_zm, &st_zm);
    size_t zm_file_size = st_zm.st_size;
    ZoneMap zm = load_zone_map(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Parallel scan + filter + aggregate with block-level zone map pruning
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums(num_threads, 0);  // Accumulate as int64 to avoid precision loss

    // Parallel: iterate over blocks, each thread processes blocks independently
    #pragma omp parallel for schedule(dynamic, 1)
    for (uint32_t block_idx = 0; block_idx < zm.num_blocks; block_idx++) {
        // Zone map pruning: skip block if entirely outside the date range
        if (zm.entries[block_idx].max_val < DATE_1994_01_01 ||
            zm.entries[block_idx].min_val >= DATE_1995_01_01) {
            continue;
        }

        // Compute row range for this block
        uint32_t block_start = block_idx * BLOCK_SIZE;
        uint32_t block_end = std::min((uint32_t)num_rows, (block_idx + 1) * BLOCK_SIZE);

        // Thread-local accumulator (no synchronization needed)
        int tid = omp_get_thread_num();
        int64_t local_sum = thread_sums[tid];

        // Tight inner loop: all integer predicates, no division
        for (uint32_t i = block_start; i < block_end; i++) {
            // Fused predicates: shipdate, quantity, discount
            // Use bitwise AND to avoid branch misprediction in tight loop
            if ((l_shipdate[i] >= DATE_1994_01_01) &
                (l_shipdate[i] < DATE_1995_01_01) &
                (l_quantity[i] < QUANTITY_MAX) &
                (l_discount[i] >= DISCOUNT_MIN) &
                (l_discount[i] <= DISCOUNT_MAX)) {

                // Compute product: l_extendedprice * l_discount (both scaled by 100)
                // Result is scaled by 10000, will divide after aggregation
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

    // Convert result: divide by 10000 to get actual revenue
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
    unload_zone_map(zm, zm_file_size);

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
