#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <algorithm>
#include <immintrin.h>
#include <omp.h>

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
//   1. Zone map prune on l_shipdate → skip 75% of blocks
//   2. Parallel OpenMP scan across surviving zones
//   3. Branchless vectorized filter: all 3 predicates combined with bitwise AND
//   4. Accumulate SUM(l_extendedprice * l_discount) per thread, reduce at end

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Date thresholds — encoded as epoch days (Correctness Anchors: DO NOT MODIFY)
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

    // Prefetch all columns to page cache concurrently
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

    const int NUM_THREADS = 64;

    // Per-thread accumulators, cache-line padded to avoid false sharing
    struct alignas(64) ThreadAcc {
        int64_t sum = 0;
        char pad[56];  // 64 - 8 = 56
    };
    ThreadAcc thread_sums[64];
    for (int i = 0; i < NUM_THREADS; i++) thread_sums[i].sum = 0;

    const int32_t* __restrict__ shipdate = col_shipdate.data;
    const int64_t* __restrict__ discount = col_discount.data;
    const int64_t* __restrict__ quantity = col_quantity.data;
    const int64_t* __restrict__ extprice = col_extprice.data;

    const int num_zones = (int)zone_ranges.size();

    {
        GENDB_PHASE("main_scan");

        // Static distribution of zones across threads: each thread gets a contiguous slice
        // of zones. Better than atomic counter: no cross-thread synchronization, sequential
        // zone access per thread is cache-friendly.
        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = 0;
            #ifdef _OPENMP
            tid = omp_get_thread_num();
            #endif
            int64_t local_sum = 0;

            // Static partition: each thread handles a contiguous range of zones
            int zone_start = (int64_t)num_zones * tid / NUM_THREADS;
            int zone_end   = (int64_t)num_zones * (tid + 1) / NUM_THREADS;

            for (int zi = zone_start; zi < zone_end; zi++) {
                uint32_t row_start = zone_ranges[zi].first;
                uint32_t row_end   = zone_ranges[zi].second;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;
                const uint32_t len = row_end - row_start;

                // Process the zone in a vectorizable branchless loop.
                // The bitwise AND of boolean results avoids branches and lets the
                // compiler auto-vectorize with AVX2 across multiple elements.
                //
                // For ~6% combined selectivity the unconditional multiply is cheap
                // since it is masked by the pass bit (0 * x = 0).
                const uint32_t base = row_start;

                // Process 4 int64 rows at a time (AVX2: 4×int64 = 256-bit)
                // We use a branchless fused approach for the inner body.
                uint32_t i = 0;
                for (; i < len; i++) {
                    uint32_t ri = base + i;
                    int32_t sd   = shipdate[ri];
                    int64_t disc = discount[ri];
                    int64_t qty  = quantity[ri];
                    // Branchless: combine all predicates into a single bitmask
                    // Each comparison produces 0 or 1; AND combines them
                    int64_t pass = (int64_t)(sd >= DATE_LO)
                                 & (int64_t)(sd <  DATE_HI)
                                 & (int64_t)(disc >= DISC_LO)
                                 & (int64_t)(disc <= DISC_HI)
                                 & (int64_t)(qty  <  QTY_MAX);
                    // Multiply pass (0 or 1) ensures no branch — compiler can vectorize
                    local_sum += pass * (extprice[ri] * disc);
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
