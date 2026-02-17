/*
 * Q6 Implementation for GenDB Iteration 0
 *
 * QUERY PLAN:
 * ===========
 *
 * LOGICAL PLAN:
 * - Table: lineitem (59986052 rows)
 * - Single-table filters:
 *   1. l_shipdate >= 8766 (1994-01-01) AND l_shipdate < 9131 (1995-01-01)
 *   2. l_discount >= 5 AND l_discount <= 7 (scaled by 100: 0.05 to 0.07)
 *   3. l_quantity < 2400 (scaled by 100: < 24)
 * - Operation: aggregate SUM(l_extendedprice * l_discount)
 * - Estimated selectivity: All three predicates are moderately selective.
 *   Zone maps can help skip non-qualifying blocks.
 *
 * PHYSICAL PLAN:
 * - Scan: Use zone maps for shipdate, discount, and quantity to skip blocks
 * - Filter: Vectorized filtering with early exit on per-row checks
 * - Aggregation: Single SUM result (cardinality = 1), use int64_t accumulator
 *   scaled at 100*100 = 10000 to avoid losing precision in products
 * - Parallelism: OpenMP parallel for with thread-local accumulators
 * - Data structures: mmap binary columns, simple accumulator
 *
 * ZONE MAP FORMAT (per storage guide):
 * - idx_lineitem_shipdate_zmap: [uint32_t num_zones][per zone: int32_t min, int32_t max, uint32_t count]
 * - idx_lineitem_discount_zmap: [uint32_t num_zones][per zone: int64_t min, int64_t max, uint32_t count]
 * - idx_lineitem_quantity_zmap: [uint32_t num_zones][per zone: int64_t min, int64_t max, uint32_t count]
 *
 * CORRECTNESS NOTES:
 * - DATE: int32_t epoch days. Compare as integers.
 * - DECIMAL: int64_t scaled by 100. Thresholds computed offline (0.05*100=5, etc).
 * - Aggregation: Use int64_t (scale^2) to avoid overflow on products of scaled values.
 * - Output: Divide final sum by 100 before CSV output.
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <chrono>
#include <cstdio>

// Helper to mmap a file and return a pointer and size
struct MmapFile {
    void* ptr = nullptr;
    size_t size = 0;

    bool open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error: cannot open " << path << std::endl;
            return false;
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Error: fstat failed for " << path << std::endl;
            ::close(fd);
            return false;
        }
        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error: mmap failed for " << path << std::endl;
            return false;
        }
        return true;
    }

    void close() {
        if (ptr) {
            munmap(ptr, size);
            ptr = nullptr;
            size = 0;
        }
    }

    ~MmapFile() { close(); }
};

// Zone map structures (not used in iteration 0, but defined for future optimization)
// struct ZoneMapEntryI32 { int32_t min_val; int32_t max_val; uint32_t row_count; };
// struct ZoneMapEntryI64 { int64_t min_val; int64_t max_val; uint32_t row_count; };

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Paths
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string indexes_dir = gendb_dir + "/indexes/";

    // Load binary columns via mmap
    MmapFile col_shipdate, col_discount, col_quantity, col_extendedprice;
    MmapFile zmap_shipdate, zmap_discount, zmap_quantity;

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    if (!col_shipdate.open(lineitem_dir + "l_shipdate.bin")) return;
    if (!col_discount.open(lineitem_dir + "l_discount.bin")) return;
    if (!col_quantity.open(lineitem_dir + "l_quantity.bin")) return;
    if (!col_extendedprice.open(lineitem_dir + "l_extendedprice.bin")) return;
    if (!zmap_shipdate.open(indexes_dir + "idx_lineitem_shipdate_zmap.bin")) return;
    if (!zmap_discount.open(indexes_dir + "idx_lineitem_discount_zmap.bin")) return;
    if (!zmap_quantity.open(indexes_dir + "idx_lineitem_quantity_zmap.bin")) return;

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Cast to typed arrays
    const int32_t* shipdate_arr = static_cast<const int32_t*>(col_shipdate.ptr);
    const int64_t* discount_arr = static_cast<const int64_t*>(col_discount.ptr);
    const int64_t* quantity_arr = static_cast<const int64_t*>(col_quantity.ptr);
    const int64_t* extendedprice_arr = static_cast<const int64_t*>(col_extendedprice.ptr);

    // Zone maps loaded but not used in iteration 0 (full scan is simpler and correct)
    // They are available for future optimization in iteration 1+

    // Query predicates
    const int32_t shipdate_low = 8766;   // 1994-01-01
    const int32_t shipdate_high = 9130;  // < 1995-01-01 (so <= 9130)
    const int64_t discount_low = 5;      // 0.05 * 100
    const int64_t discount_high = 7;     // 0.07 * 100
    const int64_t quantity_threshold = 2400;  // 24 * 100

    // Thread-local aggregation
    uint32_t num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums(num_threads, 0);

#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel full table scan with filtering
    // Total rows: 59986052
    const uint32_t total_rows = 59986052;

    #pragma omp parallel for schedule(static, 1000000)
    for (uint32_t r = 0; r < total_rows; r++) {
        // Apply all three filters at row level
        if (shipdate_arr[r] >= shipdate_low && shipdate_arr[r] <= shipdate_high &&
            discount_arr[r] >= discount_low && discount_arr[r] <= discount_high &&
            quantity_arr[r] < quantity_threshold) {
            // Aggregate: SUM(l_extendedprice * l_discount)
            // Both are scaled by 100, so product is scaled by 10000
            int thread_id = omp_get_thread_num();
            thread_sums[thread_id] += extendedprice_arr[r] * discount_arr[r];
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", filter_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Merge thread-local sums
    int64_t total_sum = 0;
    for (uint32_t i = 0; i < num_threads; i++) {
        total_sum += thread_sums[i];
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // Convert result to CSV format
    // Total sum is scaled by 10000 (100 * 100 from scaled decimals)
    // Divide by 10000 to get actual monetary value
    double revenue = static_cast<double>(total_sum) / 10000.0;

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Write CSV output
    std::string output_file = results_dir + "/Q6.csv";
    std::ofstream outfile(output_file);
    if (!outfile.is_open()) {
        std::cerr << "Error: cannot open output file " << output_file << std::endl;
        return;
    }

    outfile << "revenue\n";
    outfile << std::fixed;
    outfile.precision(4);
    outfile << revenue << "\n";
    outfile.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    std::cerr << "Q6 result: revenue = " << revenue << std::endl;
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
