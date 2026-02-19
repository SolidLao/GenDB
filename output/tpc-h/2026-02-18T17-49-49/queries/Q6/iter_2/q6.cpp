#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <algorithm>
#include <fstream>
#include <immintrin.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24
//
// Physical Plan:
//   1. Zone-map prune on l_shipdate: skip ~75% of blocks
//   2. Parallel scan over surviving zones (64 threads via OpenMP)
//   3. Per-zone: tight inner loop with branchless SIMD-friendly predicates
//      Predicate order: shipdate (25%) → discount (27%) → quantity (46%)
//   4. Accumulate extprice * discount into thread-local int64 sum
//   5. OpenMP reduction merges thread-local sums

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Date thresholds — encoded as epoch days (CORRECTNESS ANCHORS: DO NOT MODIFY)
    // 1994-01-01 = 8766, 1995-01-01 = 9131
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");

    // Discount filter: BETWEEN 0.05 AND 0.07 => scale=100 => [5, 7]
    const int64_t DISC_LO = 5;
    const int64_t DISC_HI = 7;

    // Quantity filter: < 24, scale=1
    const int64_t QTY_MAX = 24;

    // Open columns via mmap (zero-copy)
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns to page cache concurrently (MADV_WILLNEED)
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    const size_t total_rows = col_shipdate.size();

    // Load zone map index for l_shipdate
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Get qualifying zones (ranges that overlap the date predicate)
    std::vector<std::pair<uint32_t, uint32_t>> zone_ranges;
    {
        GENDB_PHASE("zone_map_prune");
        zone_map.qualifying_ranges(DATE_LO, DATE_HI, zone_ranges);
    }

    // Flatten zone ranges into a single contiguous list of (start, end) pairs
    // for easy OpenMP chunking
    const int nzones = (int)zone_ranges.size();

    // Raw column pointers for inner loop (avoids repeated struct dereference)
    const int32_t* __restrict__ shipdate = col_shipdate.data;
    const int64_t* __restrict__ discount = col_discount.data;
    const int64_t* __restrict__ quantity = col_quantity.data;
    const int64_t* __restrict__ extprice = col_extprice.data;

    int64_t total_sum = 0;

    {
        GENDB_PHASE("main_scan");

        // OpenMP parallel for with reduction — lets compiler generate optimal
        // vectorized reduction code with per-thread private accumulators.
        // dynamic,1 schedule: each thread grabs one zone at a time (work-stealing),
        // which is fine since zones are large (~100K rows each) and we have ~150 zones.
#pragma omp parallel for schedule(dynamic, 1) reduction(+:total_sum) num_threads(64)
        for (int zi = 0; zi < nzones; zi++) {
            uint32_t row_start = zone_ranges[zi].first;
            uint32_t row_end   = zone_ranges[zi].second;
            if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;

            int64_t local_sum = 0;

            // Two-pass approach within each zone:
            // Pass 1: Apply shipdate + discount + quantity filters, writing
            //         qualifying indices into a local selection vector.
            //         This separates the branchy filter from the multiply-accumulate,
            //         enabling better auto-vectorization of each phase.
            //
            // For zones of ~100K rows, the selection vector fits in L2 cache.
            // Using uint32_t indices (4B each): 100K * 4 = 400KB — may spill to L3.
            // Use uint32_t relative offsets to keep the vector compact.
            //
            // Actually, for a tight single-pass with __builtin_expect hints,
            // a branchless scalar loop is often faster than two-pass on modern OOO CPUs.
            // Use branchless bit-masking approach to accumulate:
            //   pass = (date_ok & disc_ok & qty_ok)  [0 or 1]
            //   local_sum += pass * extprice[i] * discount[i]
            // This eliminates all branches inside the loop body.

            const uint32_t len = row_end - row_start;
            const int32_t*  __restrict__ sd  = shipdate + row_start;
            const int64_t*  __restrict__ di  = discount + row_start;
            const int64_t*  __restrict__ qi  = quantity + row_start;
            const int64_t*  __restrict__ ep  = extprice + row_start;

            // Branchless fused loop: compiler can auto-vectorize this pattern.
            // All comparisons produce 0/1, multiplied together = conjunction.
            // The final multiply by the conjunction mask avoids any branch.
#pragma omp simd reduction(+:local_sum)
            for (uint32_t i = 0; i < len; i++) {
                // Date predicate: sd >= DATE_LO AND sd < DATE_HI
                int32_t s = sd[i];
                int64_t date_ok = (int64_t)((uint32_t)(s - DATE_LO) < (uint32_t)(DATE_HI - DATE_LO));

                // Discount predicate: disc >= DISC_LO AND disc <= DISC_HI
                int64_t d = di[i];
                int64_t disc_ok = (int64_t)((uint64_t)(d - DISC_LO) <= (uint64_t)(DISC_HI - DISC_LO));

                // Quantity predicate: qty < QTY_MAX
                int64_t q = qi[i];
                int64_t qty_ok = (int64_t)(q < QTY_MAX);

                // Branchless conjunction: all three must pass
                int64_t pass = date_ok & disc_ok & qty_ok;

                // Accumulate: only non-zero when pass == 1
                local_sum += pass * ep[i] * d;
            }

            total_sum += local_sum;
        }
    }

    // Aggregate merge is implicit via OpenMP reduction above
    {
        GENDB_PHASE("aggregation_merge");
        // No-op: reduction handled by OpenMP
    }

    // Output result
    {
        GENDB_PHASE("output");
        // Scale: extendedprice (scale=100) * discount (scale=100) = scale=10000
        // Revenue = total_sum / 10000.0
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
