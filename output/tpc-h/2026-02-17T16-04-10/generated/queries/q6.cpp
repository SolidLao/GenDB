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

namespace {

/*
QUERY PLAN FOR Q6: Forecasting Revenue Change (ITERATION 10 - ELIMINATE REDUNDANT DATE CHECKS)

Logical Plan:
  - Table: lineitem (59,986,052 rows, sorted by l_shipdate)
  - Predicates (ordered by selectivity):
    1. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7 as int64_t) - ~27% selectivity
    2. l_quantity < 24 (scaled: 2400 as int64_t) - ~96% selectivity
    3. l_shipdate >= 1994-01-01 AND < 1995-01-01 (epoch days: 8766-9131) - ~14% selectivity
  - Aggregation: SUM(l_extendedprice * l_discount)

Physical Plan (ITERATION 10 - Optimize Zone Map Pruning):
  1. Load zone map for l_shipdate to skip blocks completely outside date range
  2. Classify blocks into 3 categories:
     - SKIP: block entirely outside date range → skip completely
     - PARTIAL: block overlaps date range → check date per-row
     - FULL: block entirely within date range → no date checks needed (skip redundant predicate)
  3. For FULL blocks: only check discount and quantity predicates
  4. For PARTIAL blocks: check all predicates including date
  5. Vectorized 4-element filtering on hot path (discount first, then quantity, then date if needed)
  6. Accumulate as int64_t, single conversion to double at end
  7. OpenMP with block-level parallelism

Key Optimization Changes from Iter 7:
  - Eliminate redundant date checks on FULL blocks: ~14% time savings
  - Better predicate ordering for PARTIAL blocks: discount > quantity > date
  - Maintain zone map pruning and SIMD vectorization
  - Block classification avoids branch misprediction

Expected Performance:
  - Redundant date check elimination: ~5-7ms savings (14% of 39ms)
  - Zone map skip: ~86% of blocks pruned
  - Target: ~32ms for scan_filter_aggregate (from 39ms)
  - Total target: ~69ms

Data Encoding:
  - l_shipdate: int32_t (epoch days), sorted, block_size=100K
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

} // end anonymous namespace

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

    // Load zone map for date-based block pruning
    ZoneMapData zone_map = load_zone_map(
        gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

    // Parallel scan + filter + aggregate with block-level morsel parallelism
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t total_sum = 0;
    const int32_t BLOCK_SIZE = 100000;  // Fixed block size per storage guide

    // Strategy: classify blocks into SKIP/PARTIAL/FULL based on zone map
    // - SKIP: completely outside date range
    // - PARTIAL: overlaps date range, need per-row date checks
    // - FULL: completely within date range, skip redundant date checks

    if (zone_map.blocks && zone_map.num_blocks > 0) {
        // Zone map exists: use block classification for optimal filtering
        #pragma omp parallel reduction(+: total_sum)
        {
            int64_t local_sum = 0;

            // Each thread processes qualifying blocks
            #pragma omp for schedule(dynamic)
            for (size_t block_idx = 0; block_idx < zone_map.num_blocks; block_idx++) {
                int32_t block_min = zone_map.blocks[block_idx].min_val;
                int32_t block_max = zone_map.blocks[block_idx].max_val;

                // Classify block: skip if entirely outside date range
                if (block_max < DATE_1994_01_01 || block_min >= DATE_1995_01_01) {
                    continue;  // SKIP: block entirely outside date range
                }

                int64_t block_start = block_idx * BLOCK_SIZE;
                int64_t block_end = std::min(block_start + BLOCK_SIZE, (int64_t)num_rows);

                // Determine if this block is FULL (no date checks needed) or PARTIAL (date checks needed)
                bool block_is_full = (block_min >= DATE_1994_01_01 && block_max < DATE_1995_01_01);

                if (block_is_full) {
                    // FULL block: skip date checks, only filter on discount and quantity
                    int64_t i = block_start;

                    // Vectorized loop: 4 rows per iteration (discount check first)
                    for (; i + 3 < block_end; i += 4) {
                        // Check discount predicate: >= DISCOUNT_MIN && <= DISCOUNT_MAX
                        bool d0 = (l_discount[i] >= DISCOUNT_MIN && l_discount[i] <= DISCOUNT_MAX);
                        bool d1 = (l_discount[i + 1] >= DISCOUNT_MIN && l_discount[i + 1] <= DISCOUNT_MAX);
                        bool d2 = (l_discount[i + 2] >= DISCOUNT_MIN && l_discount[i + 2] <= DISCOUNT_MAX);
                        bool d3 = (l_discount[i + 3] >= DISCOUNT_MIN && l_discount[i + 3] <= DISCOUNT_MAX);

                        // Short-circuit if all discount predicates fail
                        if (!d0 && !d1 && !d2 && !d3) continue;

                        // Check remaining predicates (discount + quantity, NO date check)
                        if (d0 && l_quantity[i] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i] * l_discount[i];
                        }
                        if (d1 && l_quantity[i + 1] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 1] * l_discount[i + 1];
                        }
                        if (d2 && l_quantity[i + 2] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 2] * l_discount[i + 2];
                        }
                        if (d3 && l_quantity[i + 3] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 3] * l_discount[i + 3];
                        }
                    }

                    // Scalar tail: remaining rows in FULL block (no date check)
                    for (; i < block_end; i++) {
                        if (l_discount[i] >= DISCOUNT_MIN &&
                            l_discount[i] <= DISCOUNT_MAX &&
                            l_quantity[i] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i] * l_discount[i];
                        }
                    }
                } else {
                    // PARTIAL block: need full predicate checks including date
                    int64_t i = block_start;

                    // Vectorized loop: 4 rows per iteration (all predicates checked)
                    for (; i + 3 < block_end; i += 4) {
                        // Check discount predicate: >= DISCOUNT_MIN && <= DISCOUNT_MAX
                        bool d0 = (l_discount[i] >= DISCOUNT_MIN && l_discount[i] <= DISCOUNT_MAX);
                        bool d1 = (l_discount[i + 1] >= DISCOUNT_MIN && l_discount[i + 1] <= DISCOUNT_MAX);
                        bool d2 = (l_discount[i + 2] >= DISCOUNT_MIN && l_discount[i + 2] <= DISCOUNT_MAX);
                        bool d3 = (l_discount[i + 3] >= DISCOUNT_MIN && l_discount[i + 3] <= DISCOUNT_MAX);

                        // Short-circuit if all discount predicates fail
                        if (!d0 && !d1 && !d2 && !d3) continue;

                        // Check all remaining predicates (discount + quantity + date)
                        if (d0 && l_shipdate[i] >= DATE_1994_01_01 && l_shipdate[i] < DATE_1995_01_01 &&
                            l_quantity[i] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i] * l_discount[i];
                        }
                        if (d1 && l_shipdate[i + 1] >= DATE_1994_01_01 && l_shipdate[i + 1] < DATE_1995_01_01 &&
                            l_quantity[i + 1] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 1] * l_discount[i + 1];
                        }
                        if (d2 && l_shipdate[i + 2] >= DATE_1994_01_01 && l_shipdate[i + 2] < DATE_1995_01_01 &&
                            l_quantity[i + 2] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 2] * l_discount[i + 2];
                        }
                        if (d3 && l_shipdate[i + 3] >= DATE_1994_01_01 && l_shipdate[i + 3] < DATE_1995_01_01 &&
                            l_quantity[i + 3] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i + 3] * l_discount[i + 3];
                        }
                    }

                    // Scalar tail: remaining rows in PARTIAL block (full predicate check)
                    for (; i < block_end; i++) {
                        if (l_discount[i] >= DISCOUNT_MIN &&
                            l_discount[i] <= DISCOUNT_MAX &&
                            l_shipdate[i] >= DATE_1994_01_01 &&
                            l_shipdate[i] < DATE_1995_01_01 &&
                            l_quantity[i] < QUANTITY_MAX) {
                            local_sum += l_extendedprice[i] * l_discount[i];
                        }
                    }
                }
            }

            total_sum += local_sum;
        }
    } else {
        // Fallback: no zone map, full scan with vectorization
        #pragma omp parallel for schedule(static) reduction(+: total_sum)
        for (int64_t i = 0; i < (int64_t)num_rows; i++) {
            if (l_discount[i] >= DISCOUNT_MIN &&
                l_discount[i] <= DISCOUNT_MAX &&
                l_shipdate[i] >= DATE_1994_01_01 &&
                l_shipdate[i] < DATE_1995_01_01 &&
                l_quantity[i] < QUANTITY_MAX) {
                total_sum += l_extendedprice[i] * l_discount[i];
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
    unload_zone_map(zone_map);

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
