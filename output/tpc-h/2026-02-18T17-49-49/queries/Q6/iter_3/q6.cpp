#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <algorithm>

#include <immintrin.h>  // AVX2 intrinsics
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07  (scale=100: [5,7])
//   AND l_quantity < 24
//
// Physical Plan (Iteration 3):
//   - Zone map prune on l_shipdate (sorted column) → skip ~75% of blocks
//   - OpenMP parallel morsel scan over surviving zones
//   - AVX2 SIMD: batch-vectorize shipdate filter (8×int32 per instruction)
//   - Branchless accumulation for discount/qty on shipdate survivors
//   - Thread-local int64 accumulator → sequential merge (trivial, 64 values)
//
// Key differences vs iter 2:
//   - OpenMP (lower thread overhead) instead of std::thread
//   - Explicit AVX2 for the dominant shipdate filter (25% sel → 75% skip)
//   - 2-stage filter: vectorized shipdate pass produces selection vector,
//     then scalar loop over selection vector for discount/qty/accumulate
//   - MADV_SEQUENTIAL already set by MmapColumn; also add prefetch-ahead
//   - Batch size 4096 rows for L1 cache working set

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Correctness anchors (DO NOT MODIFY)
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");  // 8766
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");  // 9131

    // Discount filter: BETWEEN 0.05 AND 0.07 => scale=100 => [5, 7]
    const int64_t DISC_LO = 5;
    const int64_t DISC_HI = 7;

    // Quantity filter: < 24
    const int64_t QTY_MAX = 24;

    // Open columns via mmap (zero-copy, MADV_SEQUENTIAL applied automatically)
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns into page cache concurrently
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    const size_t total_rows = col_shipdate.size();

    // Load zone map for l_shipdate
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Get qualifying zones
    std::vector<std::pair<uint32_t, uint32_t>> zone_ranges;
    {
        GENDB_PHASE("zone_map_prune");
        zone_map.qualifying_ranges(DATE_LO, DATE_HI, zone_ranges);
    }

    const int NUM_THREADS = 64;
    const int nzones = (int)zone_ranges.size();

    // Per-thread accumulators (cache-line padded to avoid false sharing)
    struct alignas(64) ThreadAcc {
        int64_t sum = 0;
        char pad[56];  // pad to 64 bytes
    };
    ThreadAcc thread_sums[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) thread_sums[i].sum = 0;

    // Raw pointers for hot loop
    const int32_t* __restrict__ shipdate = col_shipdate.data;
    const int64_t* __restrict__ discount = col_discount.data;
    const int64_t* __restrict__ quantity = col_quantity.data;
    const int64_t* __restrict__ extprice = col_extprice.data;

    // AVX2 constants for vectorized shipdate filter
    // We compare int32 values: DATE_LO <= sd < DATE_HI
    // sd >= DATE_LO  ↔  sd > DATE_LO-1  →  _mm256_cmpgt_epi32(sd, DATE_LO-1)
    // sd < DATE_HI   ↔  sd <= DATE_HI-1 →  _mm256_cmpgt_epi32(DATE_HI, sd)
    const __m256i vDATE_HI   = _mm256_set1_epi32(DATE_HI - 1);  // use <= (DATE_HI-1)
    const __m256i vDATE_LOM1 = _mm256_set1_epi32(DATE_LO - 1);  // use > (DATE_LO-1)

    {
        GENDB_PHASE("main_scan");

        std::atomic<int> zone_cursor{0};

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            int64_t local_sum = 0;

            // Selection vector buffer: batch size 4096 rows
            static const int BATCH = 4096;
            // Use a local array on the stack for the selection vector
            uint32_t sel_vec[BATCH];

            while (true) {
                int zi = zone_cursor.fetch_add(1, std::memory_order_relaxed);
                if (zi >= nzones) break;

                uint32_t row_start = zone_ranges[zi].first;
                uint32_t row_end   = zone_ranges[zi].second;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;

                // Process zone in batches of BATCH rows
                for (uint32_t batch_start = row_start; batch_start < row_end; batch_start += BATCH) {
                    uint32_t batch_end = std::min(batch_start + (uint32_t)BATCH, row_end);
                    uint32_t batch_len = batch_end - batch_start;

                    // -------------------------------------------------------
                    // Stage 1: AVX2 vectorized shipdate filter
                    // Produces a selection vector of row indices passing sd filter
                    // -------------------------------------------------------
                    int sel_count = 0;
                    const int32_t* sd_ptr = shipdate + batch_start;

                    // Process 8 int32 at a time with AVX2
                    uint32_t vi = 0;
                    uint32_t avx_len = (batch_len / 8) * 8;

                    for (; vi < avx_len; vi += 8) {
                        __m256i sd8 = _mm256_loadu_si256((const __m256i*)(sd_ptr + vi));
                        // sd >= DATE_LO  ↔  sd > DATE_LO - 1
                        __m256i ge_lo = _mm256_cmpgt_epi32(sd8, vDATE_LOM1);
                        // sd <= DATE_HI-1  ↔  sd < DATE_HI
                        __m256i le_hi = _mm256_cmpgt_epi32(vDATE_HI, sd8);
                        // Wait: _mm256_cmpgt_epi32(a,b) returns ~0 where a > b
                        // le_hi: vDATE_HI > sd8, i.e., sd < DATE_HI ✓
                        __m256i pass  = _mm256_and_si256(ge_lo, le_hi);
                        // Extract 8-bit mask
                        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(pass));
                        // Scatter passing lane indices into sel_vec
                        while (mask) {
                            int lane = __builtin_ctz(mask);
                            sel_vec[sel_count++] = batch_start + vi + lane;
                            mask &= mask - 1;
                        }
                    }
                    // Scalar tail
                    for (; vi < batch_len; vi++) {
                        int32_t sd = sd_ptr[vi];
                        if (sd >= DATE_LO && sd < DATE_HI) {
                            sel_vec[sel_count++] = batch_start + vi;
                        }
                    }

                    // -------------------------------------------------------
                    // Stage 2: Scalar loop over selection vector
                    // Apply discount and quantity filters, accumulate revenue
                    // -------------------------------------------------------
                    for (int s = 0; s < sel_count; s++) {
                        uint32_t idx = sel_vec[s];
                        int64_t disc = discount[idx];
                        // Branchless: disc in [5,7] AND qty < 24
                        // Use bitwise AND to avoid branch misprediction
                        int64_t qty  = quantity[idx];
                        int pass = (disc >= DISC_LO) & (disc <= DISC_HI) & (qty < QTY_MAX);
                        local_sum += pass * (extprice[idx] * disc);
                    }
                }
            }

            thread_sums[tid].sum = local_sum;
        }
    }

    // Merge thread-local accumulators
    int64_t total_sum = 0;
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < NUM_THREADS; t++) {
            total_sum += thread_sums[t].sum;
        }
    }

    // Output result
    {
        GENDB_PHASE("output");
        // Scale: extendedprice (scale=100) * discount (scale=100) = scale=10000
        double revenue = static_cast<double>(total_sum) / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        std::FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", revenue);
        std::fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
