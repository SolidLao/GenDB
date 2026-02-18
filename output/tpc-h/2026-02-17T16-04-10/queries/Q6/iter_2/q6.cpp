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
QUERY PLAN FOR Q6: Forecasting Revenue Change (Iteration 2 — Zone Map Pruning)

Logical Plan:
  - Table: lineitem (59,986,052 rows)
  - Predicates:
    1. l_shipdate >= 1994-01-01 (epoch day: 8766)
    2. l_shipdate < 1995-01-01 (epoch day: 9131)
    3. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7 as int64_t)
    4. l_quantity < 24 (scaled: 2400 as int64_t)
  - Aggregation: SUM(l_extendedprice * l_discount)
  - No GROUP BY: single result row

Physical Plan:
  1. Load zone map for l_shipdate to identify qualifying blocks (~1.5M out of 60M rows)
  2. Load all required columns via mmap: l_shipdate, l_discount, l_quantity, l_extendedprice
  3. Iterate through zone map entries, skip blocks outside date range
  4. For qualifying blocks only: parallel scan with morsel-driven approach (OpenMP)
  5. Apply all predicates in tight loop (fused scan+filter)
  6. Accumulate product (l_extendedprice * l_discount) using Kahan summation
  7. Output single row with revenue

Data Encoding:
  - l_shipdate: int32_t (epoch days since 1970-01-01)
  - l_discount: int64_t with scale_factor=100
  - l_quantity: int64_t with scale_factor=100
  - l_extendedprice: int64_t with scale_factor=100

Zone Map Layout:
  - File: indexes/lineitem_l_shipdate_zonemap.bin
  - Header: uint32_t num_blocks
  - Entries: [int32_t min, int32_t max] per block (16 bytes each)

Parallelism:
  - Thread-local accumulators (one per thread) to avoid synchronization
  - Kahan summation to maintain precision during parallel aggregation
  - Master thread merges local results after parallel phase
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

// Zone map entry: [min:int32_t, max:int32_t] per block
struct ZoneMapBlock {
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

// Helper function to load zone map
struct ZoneMapIndex {
    uint32_t num_blocks;
    ZoneMapBlock* blocks;
};

ZoneMapIndex load_zonemap(const std::string& zonemap_path) {
    int fd = open(zonemap_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open zonemap " << zonemap_path << std::endl;
        exit(1);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat zonemap " << zonemap_path << std::endl;
        close(fd);
        exit(1);
    }

    size_t file_size = st.st_size;

    // Read header: uint32_t num_blocks
    uint32_t* header = (uint32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (header == MAP_FAILED) {
        std::cerr << "Failed to mmap zonemap " << zonemap_path << std::endl;
        close(fd);
        exit(1);
    }

    uint32_t num_blocks = *header;

    // Zone map entries follow the header
    ZoneMapBlock* blocks = (ZoneMapBlock*)((uint8_t*)header + sizeof(uint32_t));

    close(fd);

    ZoneMapIndex idx;
    idx.num_blocks = num_blocks;
    idx.blocks = blocks;
    return idx;
}

// Helper function to unmap column
template<typename T>
void unload_column(T* data, size_t num_rows) {
    if (data && data != MAP_FAILED) {
        munmap(data, num_rows * sizeof(T));
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
    ZoneMapIndex zonemap = load_zonemap(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Parallel scan + filter + aggregate using Kahan summation
    // First, identify qualifying blocks using zone map
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<double> thread_sums(num_threads, 0.0);
    std::vector<double> thread_c(num_threads, 0.0);  // Kahan compensation

    // Build a list of row ranges for qualifying blocks (zone map pruning)
    std::vector<std::pair<int64_t, int64_t>> block_ranges;
    const int64_t BLOCK_SIZE = 100000;  // From storage guide

    for (uint32_t b = 0; b < zonemap.num_blocks; b++) {
        // Skip blocks entirely outside the date range
        if (zonemap.blocks[b].max_val < DATE_1994_01_01 ||
            zonemap.blocks[b].min_val >= DATE_1995_01_01) {
            continue;  // Block has no qualifying rows
        }

        // This block may have qualifying rows
        int64_t block_start = (int64_t)b * BLOCK_SIZE;
        int64_t block_end = std::min(block_start + BLOCK_SIZE, (int64_t)num_rows);
        block_ranges.push_back({block_start, block_end});
    }

    // Process qualifying blocks in parallel using work-sharing
    #pragma omp parallel for schedule(dynamic, 1) collapse(1)
    for (int64_t br = 0; br < (int64_t)block_ranges.size(); br++) {
        int64_t block_start = block_ranges[br].first;
        int64_t block_end = block_ranges[br].second;

        for (int64_t i = block_start; i < block_end; i++) {
            // Apply all predicates
            if (l_shipdate[i] >= DATE_1994_01_01 &&
                l_shipdate[i] < DATE_1995_01_01 &&
                l_discount[i] >= DISCOUNT_MIN &&
                l_discount[i] <= DISCOUNT_MAX &&
                l_quantity[i] < QUANTITY_MAX) {

                // Compute product: (extendedprice * discount) / (100 * 100)
                // Both are scaled by 100, so product is scaled by 10000
                // To preserve precision, convert to double before division
                double product_scaled = (double)l_extendedprice[i] * (double)l_discount[i] / 10000.0;

                // Kahan summation for precision
                int tid = omp_get_thread_num();
                double y = product_scaled - thread_c[tid];
                double t = thread_sums[tid] + y;
                thread_c[tid] = (t - thread_sums[tid]) - y;
                thread_sums[tid] = t;
            }
        }
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
    double final_c = 0.0;
    for (int t = 0; t < num_threads; t++) {
        double y = thread_sums[t] - final_c;
        double sum = final_sum + y;
        final_c = (sum - final_sum) - y;
        final_sum = sum;
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
