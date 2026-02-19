// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24
//
// ARCHITECTURE (iter 3 — full rewrite for STALL recovery):
// ==========================================================
// Zone maps cover all 600 blocks (data not clustered by shipdate).
// No block pruning is possible. Must scan all 59.9M rows.
//
// Bottleneck analysis:
//   - 59.9M rows × 28 bytes/row = 1.68 GB of data
//   - 64 threads, 2 NUMA nodes, AVX-512 available
//   - Previous approach: manual std::thread + branchy inner loop
//   - This approach: OpenMP + branchless AVX-512 auto-vectorizable loop
//
// Key changes vs prior iterations:
//   1. OpenMP parallel for with reduction — compiler emits AVX-512 vectorized code
//   2. Fully branchless inner loop: mask = (d>=5)&(d<=7)&(q<2400)&(s>=8766)&(s<9131)
//      then local_sum += (int64_t)mask * (ep * d)  — no branches, fully vectorizable
//   3. Tighter morsel size (schedule dynamic, 4 blocks = 400K rows) to reduce scheduling overhead
//   4. Separate outer scan loop over raw pointers — helps compiler see no aliasing
//   5. Predicate constants as constexpr int64_t to unify types (avoid mixed int32/int64)
//
// Correctness anchors (DO NOT MODIFY):
//   QTY_HI = 2400  (quantity < 24.00 scaled by 100)
//   DISC_LO = 5, DISC_HI = 7  (0.05 and 0.07 scaled by 100)
//   SHIPDATE_LO = 8766, SHIPDATE_HI = 9131  (days since epoch)
//   Revenue = global_sum / 10000  (extprice * discount both scaled by 100)

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <fstream>
#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

// Correctness anchor — DO NOT CHANGE
static constexpr int64_t SHIPDATE_LO = 8766;   // 1994-01-01 as epoch days
static constexpr int64_t SHIPDATE_HI = 9131;   // 1995-01-01 as epoch days
static constexpr int64_t DISC_LO     = 5;      // 0.05 * 100
static constexpr int64_t DISC_HI     = 7;      // 0.07 * 100
static constexpr int64_t QTY_HI      = 2400;   // 24.00 * 100 (threshold_constant from anchors)

static constexpr size_t BLOCK_SIZE   = 100000;
// Morsel: 4 blocks = 400K rows fits well in L2 cache per thread (400K * 28B = 11MB)
// With 64 threads and 600 blocks, schedule(dynamic,4) gives ~150 tasks, good load balance
static constexpr int    MORSEL_BLOCKS = 4;

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Phase 1: Memory-map all columns (zero-copy)
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;

    {
        GENDB_PHASE("load_columns");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        // Prefetch all columns into page cache concurrently (MADV_WILLNEED)
        mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);
    }

    const size_t total_rows = col_shipdate.count;
    const size_t num_blocks = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // Raw pointers — allows compiler to reason about no aliasing
    const int32_t* __restrict__ shipdate_ptr = col_shipdate.data;
    const int64_t* __restrict__ discount_ptr = col_discount.data;
    const int64_t* __restrict__ quantity_ptr = col_quantity.data;
    const int64_t* __restrict__ extprice_ptr = col_extprice.data;

    // -----------------------------------------------------------------------
    // Phase 2: Parallel branchless scan via OpenMP
    //
    // The inner loop is fully branchless so the compiler can auto-vectorize
    // with AVX-512 (16 int32 or 8 int64 per instruction).
    //
    // Branchless pattern:
    //   int64_t pass = ((int64_t)(s >= SHIPDATE_LO) & (int64_t)(s < SHIPDATE_HI) &
    //                   (d >= DISC_LO) & (d <= DISC_HI) & (q < QTY_HI));
    //   local_sum += pass * (ep * d);
    //
    // This avoids branch misprediction (~19% selectivity means ~80% taken branches)
    // and allows AVX-512 SIMD to process 8 int64 rows simultaneously.
    // -----------------------------------------------------------------------
    int64_t global_sum = 0;

    {
        GENDB_PHASE("main_scan");

        // Use static num_blocks_int for OpenMP loop bound
        const int nb = (int)num_blocks;

        // OpenMP parallel for with reduction on global_sum
        // schedule(dynamic, MORSEL_BLOCKS): each thread pulls 4-block morsels
        // to balance load while minimizing scheduling overhead
        #pragma omp parallel for schedule(dynamic, MORSEL_BLOCKS) reduction(+:global_sum) \
                num_threads(64) default(none) \
                shared(shipdate_ptr, discount_ptr, quantity_ptr, extprice_ptr, total_rows, nb)
        for (int blk = 0; blk < nb; blk++) {
            const size_t row_start = (size_t)blk * BLOCK_SIZE;
            const size_t row_end   = (row_start + BLOCK_SIZE < total_rows)
                                     ? row_start + BLOCK_SIZE
                                     : total_rows;
            const size_t n = row_end - row_start;

            const int32_t* __restrict__ sd  = shipdate_ptr + row_start;
            const int64_t* __restrict__ dis = discount_ptr + row_start;
            const int64_t* __restrict__ qty = quantity_ptr + row_start;
            const int64_t* __restrict__ ep  = extprice_ptr + row_start;

            int64_t local_sum = 0;

            // Fully branchless inner loop — enables AVX-512 auto-vectorization
            // Predicate evaluation produces 0 or 1; multiply zeroes out non-matches
            // Compiler sees: no branches, no loop-carried dependencies, __restrict__
            // → emits AVX-512 VPCMPQ, VPMULLQ, VPADDQ instructions
            #pragma omp simd reduction(+:local_sum)
            for (size_t i = 0; i < n; i++) {
                const int64_t s   = (int64_t)sd[i];
                const int64_t d   = dis[i];
                const int64_t q   = qty[i];
                const int64_t e   = ep[i];

                // All predicates combined into single bitmask (0 or 1)
                const int64_t pass =
                    ((int64_t)(s >= SHIPDATE_LO)) &
                    ((int64_t)(s <  SHIPDATE_HI)) &
                    ((int64_t)(d >= DISC_LO))     &
                    ((int64_t)(d <= DISC_HI))     &
                    ((int64_t)(q <  QTY_HI));

                local_sum += pass * (e * d);
            }

            global_sum += local_sum;
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Output results
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // global_sum = SUM(extprice * discount), both scaled by 100
        // → actual revenue = global_sum / 10000
        const int64_t integer_part = global_sum / 10000LL;
        const int64_t frac_part    = global_sum % 10000LL;

        char buf[64];
        std::snprintf(buf, sizeof(buf), "%lld.%04lld",
                      (long long)integer_part, (long long)frac_part);

        const std::string out_path = results_dir + "/Q6.csv";
        std::ofstream ofs(out_path);
        ofs << "revenue\n" << buf << "\n";
        ofs.close();

        std::printf("revenue = %s\n", buf);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
