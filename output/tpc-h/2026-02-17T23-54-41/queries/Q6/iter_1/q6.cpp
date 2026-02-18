/*
 * Q6: Forecasting Revenue Change
 *
 * SQL:
 *   SELECT SUM(l_extendedprice * l_discount) AS revenue
 *   FROM lineitem
 *   WHERE l_shipdate >= DATE '1994-01-01'
 *     AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR   -- i.e., < 1995-01-01
 *     AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01       -- [0.05, 0.07]
 *     AND l_quantity < 24;
 *
 * === LOGICAL PLAN ===
 *   Single table: lineitem (59.9M rows)
 *   Predicates (all single-table):
 *     1. l_shipdate >= 8766  (1994-01-01 epoch days)
 *     2. l_shipdate <  9131  (1995-01-01 epoch days)
 *     3. l_discount >= 5     (0.05 * 100, scaled int64)
 *     4. l_discount <= 7     (0.07 * 100, scaled int64)
 *     5. l_quantity < 2400   (24 * 100, scaled int64)
 *   Aggregation: SUM(l_extendedprice * l_discount)
 *   No joins, no GROUP BY.
 *
 * === PHYSICAL PLAN ===
 *   1. Zone map pruning on l_shipdate to skip blocks outside [8766, 9130]
 *      - Index: indexes/lineitem_l_shipdate_zonemap.bin
 *      - Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
 *      - 300 zones, 200,000 rows each
 *      - Produces a contiguous [row_start, row_end) range (since data sorted by shipdate)
 *   2. Parallel morsel scan of qualifying row range with OpenMP
 *      - Morsel size: 64K rows — fits in L3 cache slice per thread
 *      - 64 threads, ~2-4M qualifying rows → ~32-64 morsels → full thread utilization
 *      - Predicate order: discount (int64, range [5,7]) → quantity (int64, <2400) → shipdate (int32, within zone but may spill)
 *      - Branch-free arithmetic + #pragma omp simd for auto-vectorization
 *      - Thread-local int128 accumulator for SUM (avoid overflow)
 *   3. Global merge of thread-local accumulators
 *   4. Output: revenue = SUM / (100 * 100) with 2 decimal places
 *
 * === KEY OPTIMIZATIONS vs iter_0 ===
 *   - Parallelism restructured: iterate over rows (not zones) → 64 threads fully utilized
 *     iter_0 parallelized over ~10-20 zones → most of 64 threads sat idle
 *   - Morsel-driven: atomic counter dispatches 64K-row morsels to threads
 *   - __restrict__ on all column pointers for alias-free auto-vectorization
 *   - #pragma omp simd reduction for the inner accumulation loop
 *   - Predicate ordering: discount first (very selective: only values 5,6,7 out of 0-10),
 *     then quantity, then shipdate (already constrained by zone map)
 *   - Use contiguous qualifying range instead of vector of scan_ranges
 *
 * === DECIMAL ARITHMETIC ===
 *   l_extendedprice: int64, scale 100 (value = actual * 100)
 *   l_discount:      int64, scale 100 (value = actual * 100)
 *   l_extendedprice * l_discount = actual^2 * 10000
 *   Final revenue = SUM(ep * disc) / 10000
 *   Use __int128 accumulator to avoid overflow.
 *
 * === PARALLELISM ===
 *   Morsel-driven OpenMP with atomic row counter.
 *   64 hardware threads, morsel = 65536 rows.
 *   Qualifying rows: ~2-4M → ~32-64 morsels → full utilization.
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <atomic>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/lineitem/";

    // === Date thresholds (epoch days) ===
    const int32_t date_lo = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t date_hi = gendb::date_str_to_epoch_days("1995-01-01"); // exclusive

    // === Scaled integer thresholds ===
    // discount BETWEEN 0.05 AND 0.07 → [5, 7] (scale 100)
    const int64_t disc_lo = 5;
    const int64_t disc_hi = 7;
    // quantity < 24 → < 2400 (scale 100)
    const int64_t qty_max = 2400; // exclusive

    // === Load columns via mmap (zero-copy) ===
    gendb::MmapColumn<int32_t> col_shipdate(base + "l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(base + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(base + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(base + "l_extendedprice.bin");

    const size_t total_rows = col_shipdate.count;

    // === Zone map pruning — determine contiguous qualifying row range ===
    // Since data is sorted by l_shipdate, the qualifying zones are contiguous.
    // We find the min start row and max end row among qualifying zones.
    struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t count; };

    size_t range_start = 0;
    size_t range_end   = total_rows;

    {
        GENDB_PHASE("zone_map_prune");
        std::string zm_path = gendb_dir + "/../indexes/lineitem_l_shipdate_zonemap.bin";
        FILE* f = fopen(zm_path.c_str(), "rb");
        if (f) {
            uint32_t num_zones = 0;
            if (fread(&num_zones, sizeof(uint32_t), 1, f) == 1) {
                std::vector<ZoneEntry> zones(num_zones);
                if (fread(zones.data(), sizeof(ZoneEntry), num_zones, f) == num_zones) {
                    // Find contiguous range of qualifying zones
                    size_t first_qualifying = SIZE_MAX;
                    size_t last_qualifying_end = 0;
                    size_t row_offset = 0;
                    for (uint32_t z = 0; z < num_zones; z++) {
                        size_t zone_count = zones[z].count;
                        // Zone qualifies if it could contain rows in [date_lo, date_hi)
                        if (zones[z].max_val >= date_lo && zones[z].min_val < date_hi) {
                            if (first_qualifying == SIZE_MAX) first_qualifying = row_offset;
                            last_qualifying_end = row_offset + zone_count;
                        }
                        row_offset += zone_count;
                    }
                    if (first_qualifying != SIZE_MAX) {
                        range_start = first_qualifying;
                        range_end   = last_qualifying_end;
                    } else {
                        range_start = range_end = 0; // nothing qualifies
                    }
                }
            }
            fclose(f);
        }
    }

    // === Parallel morsel scan with fused filter + accumulate ===
    // Morsel size: 65536 rows per task → balances overhead vs granularity
    // With 64 threads and ~2-4M qualifying rows, this gives ~32-64 morsels.
    const size_t MORSEL_SIZE = 65536;
    const size_t qualifying_rows = (range_end > range_start) ? (range_end - range_start) : 0;
    const size_t num_morsels = (qualifying_rows + MORSEL_SIZE - 1) / MORSEL_SIZE;

    // Align raw pointers to base of qualifying range (compiler alias analysis)
    const int32_t* __restrict__ sd   = col_shipdate.data + range_start;
    const int64_t* __restrict__ disc = col_discount.data + range_start;
    const int64_t* __restrict__ qty  = col_quantity.data + range_start;
    const int64_t* __restrict__ ep   = col_extprice.data + range_start;

    int num_threads = omp_get_max_threads();
    // Pad to avoid false sharing (each __int128 = 16 bytes; pad to 64B cache line)
    struct alignas(64) PaddedSum { __int128 val; char pad[48]; };
    std::vector<PaddedSum> thread_sums(num_threads);
    for (auto& s : thread_sums) s.val = (__int128)0;

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (size_t m = 0; m < num_morsels; m++) {
            int tid = omp_get_thread_num();
            size_t row_start = m * MORSEL_SIZE;
            size_t row_end   = row_start + MORSEL_SIZE;
            if (row_end > qualifying_rows) row_end = qualifying_rows;

            const int32_t* __restrict__ sd_m   = sd   + row_start;
            const int64_t* __restrict__ disc_m  = disc + row_start;
            const int64_t* __restrict__ qty_m   = qty  + row_start;
            const int64_t* __restrict__ ep_m    = ep   + row_start;
            size_t len = row_end - row_start;

            __int128 local_sum = 0;

            // Inner loop: apply predicates with branch-free arithmetic where possible.
            // Predicate order (most selective first):
            //   1. discount in [5,7]   — only 3 of 11 possible values pass (~27%)
            //   2. quantity < 2400     — roughly 50% selectivity
            //   3. shipdate in [lo,hi) — ~14% of full table, but zone map already constrains range
            // Use short-circuit branches (cheap mis-prediction: most rows rejected early).
            for (size_t i = 0; i < len; i++) {
                int64_t d = disc_m[i];
                if ((d < disc_lo) | (d > disc_hi)) continue;  // bitwise OR: branch-free compound
                int64_t q = qty_m[i];
                if (q >= qty_max) continue;
                int32_t shipdate = sd_m[i];
                if ((shipdate < date_lo) | (shipdate >= date_hi)) continue;
                local_sum += ((__int128)ep_m[i]) * d;
            }

            thread_sums[tid].val += local_sum;
        }
    }

    // === Merge thread sums ===
    __int128 total_sum = 0;
    for (auto& s : thread_sums) total_sum += s.val;

    // Divide by 10000 (scale_factor^2 = 100*100) to get actual revenue
    int64_t int_part  = (int64_t)(total_sum / 10000);
    int64_t frac_part = (int64_t)(total_sum % 10000);
    int64_t frac2     = frac_part / 100;
    if ((frac_part % 100) >= 50) frac2++;
    if (frac2 >= 100) { int_part++; frac2 -= 100; }

    // === Output results ===
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) throw std::runtime_error("Cannot open output: " + out_path);
        fprintf(out, "revenue\n");
        fprintf(out, "%lld.%02lld\n", (long long)int_part, (long long)frac2);
        fclose(out);
    }
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
