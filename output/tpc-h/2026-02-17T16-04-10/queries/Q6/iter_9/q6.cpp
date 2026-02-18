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
#include <cmath>
#include <immintrin.h>

/*
QUERY PLAN FOR Q6: Forecasting Revenue Change (ITERATION 9 - BRANCH-FREE VECTORIZATION)

Logical Plan:
  - Table: lineitem (59,986,052 rows)
  - Predicates (combined with branch-free arithmetic):
    1. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7 as int64_t) - ~27% selectivity
    2. l_shipdate >= 1994-01-01 AND < 1995-01-01 (epoch days: 8766-9131) - ~14% selectivity
    3. l_quantity < 24 (scaled: 2400 as int64_t) - ~96% selectivity
  - Aggregation: SUM(l_extendedprice * l_discount), accumulate as int64_t

Physical Plan (ITERATION 9 - Vectorization + Branch-Free Evaluation):
  1. Load all four required columns: l_discount, l_shipdate, l_quantity, l_extendedprice
  2. Parallel scan with branch-free predicate evaluation
  3. Use arithmetic AND (bitwise) instead of conditional branches to combine predicates
  4. Vectorization through compiler auto-optimization with `-O3 -march=native`
  5. OpenMP parallel for with static scheduling for optimal cache utilization

Key Changes from Iter 8:
  - Remove explicit 4-way unrolling: let compiler auto-vectorize
  - Use branch-free arithmetic to combine predicates for better CPU pipeline usage
  - Simpler code → better compiler optimization opportunities
  - Direct `omp parallel for reduction` without nested loops
  - All predicate evaluation at native register speed

Expected Performance:
  - Full scan with branch-free filtering: tight loop
  - Compiler can auto-vectorize: 4 int64 operations per SIMD instruction (AVX2)
  - Effective throughput: scan + 3 comparisons + conditional accumulate per row
  - Target: Reduce scan_filter_aggregate from 39ms to 12-15ms through vectorization

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

// Load zone map for date-based block pruning
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
};

struct ZoneMapData {
    ZoneMapBlock* blocks;
    size_t num_blocks;
    void* mmap_ptr;
    size_t mmap_size;
};

ZoneMapData load_zone_map(const std::string& file_path) {
    ZoneMapData result = {nullptr, 0, nullptr, 0};

    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        return result;  // Zone map may not exist
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return result;
    }

    size_t file_size = st.st_size;
    uint32_t* header = (uint32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (header == MAP_FAILED) {
        close(fd);
        return result;
    }

    close(fd);

    // Zone map format: [uint32_t num_blocks] followed by [int32_t min, int32_t max] per block
    result.num_blocks = *header;
    result.blocks = (ZoneMapBlock*)(header + 1);
    result.mmap_ptr = (void*)header;
    result.mmap_size = file_size;

    return result;
}

void unload_zone_map(ZoneMapData& zone_map) {
    if (zone_map.mmap_ptr) {
        munmap(zone_map.mmap_ptr, zone_map.mmap_size);
        zone_map.blocks = nullptr;
        zone_map.num_blocks = 0;
        zone_map.mmap_ptr = nullptr;
        zone_map.mmap_size = 0;
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

    // Parallel scan + filter + aggregate with SIMD-vectorized filtering
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t total_sum = 0;

    // Branch-free filtering for better CPU prediction and vectorization
    // Process all rows, but use arithmetic to avoid branches
    #pragma omp parallel reduction(+: total_sum)
    {
        int64_t local_sum = 0;

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < (int64_t)num_rows; i++) {
            // Check all predicates: non-branching evaluation
            int discount_ok = (l_discount[i] >= DISCOUNT_MIN && l_discount[i] <= DISCOUNT_MAX);
            int date_ok = (l_shipdate[i] >= DATE_1994_01_01 && l_shipdate[i] < DATE_1995_01_01);
            int quantity_ok = (l_quantity[i] < QUANTITY_MAX);

            // Combine predicates: if all true, add to sum (using arithmetic AND)
            int all_pass = discount_ok & date_ok & quantity_ok;
            local_sum += all_pass * (l_extendedprice[i] * l_discount[i]);
        }

        total_sum += local_sum;
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
