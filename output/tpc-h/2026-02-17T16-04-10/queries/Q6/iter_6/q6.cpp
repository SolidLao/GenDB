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
QUERY PLAN FOR Q6: Forecasting Revenue Change (STALL RECOVERY - CLEAN INT64_T ACCUMULATION)

Logical Plan:
  - Table: lineitem (59,986,052 rows)
  - Predicates:
    1. l_shipdate >= 1994-01-01 (epoch day: 8766)
    2. l_shipdate < 1995-01-01 (epoch day: 9131)
    3. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7 as int64_t)
    4. l_quantity < 24 (scaled: 2400 as int64_t)
  - Aggregation: SUM(l_extendedprice * l_discount) with NO GROUP BY

Physical Plan (STALL RECOVERY - Key Optimization):
  1. Load all columns via mmap (zero-copy access)
  2. Single-pass fused scan + filter + aggregate
  3. Accumulate as int64_t (avoids per-row double conversion overhead)
  4. Convert final result to double once (not 60M times)
  5. Simple OpenMP parallelization with reduction

Key Optimization Principle:
  - Previous code did (double)product_scaled per row = 60M double conversions
  - New code: accumulate int64_t, single conversion at end
  - This alone should give ~2-3x speedup for the arithmetic-heavy inner loop
  - Total predicates: date (14% selectivity) + discount (27%) + quantity (96%)
  - Expected qualifying rows: ~2.2M (3.7% of dataset)

Data Encoding:
  - l_shipdate: int32_t (epoch days)
  - All other columns: int64_t with scale_factor=100
*/

// Epoch date constants
constexpr int32_t DATE_1994_01_01 = 8766;
constexpr int32_t DATE_1995_01_01 = 9131;

// Discount bounds (scaled by 100)
constexpr int64_t DISCOUNT_MIN = 5;    // 0.05
constexpr int64_t DISCOUNT_MAX = 7;    // 0.07

// Quantity bound (scaled by 100)
constexpr int64_t QUANTITY_MAX = 2400;  // 24.00

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

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load columns
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

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Parallel scan + filter + aggregate
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t total_sum = 0;

    // Core optimization: accumulate as int64_t, convert to double ONCE at the end
    // This eliminates 60M expensive double conversions from the inner loop
    #pragma omp parallel for schedule(static) reduction(+: total_sum)
    for (int64_t i = 0; i < (int64_t)num_rows; i++) {
        if (l_shipdate[i] >= DATE_1994_01_01 &&
            l_shipdate[i] < DATE_1995_01_01 &&
            l_discount[i] >= DISCOUNT_MIN &&
            l_discount[i] <= DISCOUNT_MAX &&
            l_quantity[i] < QUANTITY_MAX) {
            // Accumulate as int64_t: both scaled by 100, product scaled by 10000
            total_sum += l_extendedprice[i] * l_discount[i];
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

    // Single conversion from int64_t to double
    double final_sum = (double)total_sum / 10000.0;

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
