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
 * === PHYSICAL PLAN (iter 9) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *   2. Parallel morsel scan with three-stage late materialization:
 *
 *      STAGE A — Bulk filter on disc+qty via AVX2 int64 SIMD (4-lane × 2 groups = 8 rows/iter):
 *        * disc [5,7] and qty < 2400: ~12% combined selectivity
 *        * Emit passing row indices into a local selection vector (stack-allocated, fixed size)
 *        * This stage avoids loading shipdate and ep for 88% of rows
 *
 *      STAGE B — Scatter-load shipdate for qualifying indices, apply date filter:
 *        * Load shipdate[sel[j]] for each index in the selection vector
 *        * Compact survivors to a second selection vector (date_sel)
 *        * ~14% of disc+qty survivors pass the date filter (1 year / 7 year range)
 *        * Further reduces ep loads to ~1.7% of input rows
 *
 *      STAGE C — Accumulate ep*disc for rows passing all three filters:
 *        * Load ep[sel[j]] and disc[sel[j]] for qualifying rows only
 *        * Branchless scalar multiply into __int128 local sum
 *
 *      - Selection vector approach avoids false-sharing and branch misprediction
 *      - SIMD constants precomputed outside hot loop
 *      - schedule(static) with MORSEL_SIZE=131072 → minimal OpenMP overhead
 *      - madvise(MADV_SEQUENTIAL) on all columns for prefetch hint
 *
 *   3. Global merge of thread-local __int128 sums
 *   4. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === KEY OPTIMIZATIONS vs iter_4/8 ===
 *   - Late materialization: shipdate only loaded for ~12% of rows (those passing disc+qty)
 *   - ep only loaded for ~1.7% of rows (those passing all three filters)
 *   - Selection vector pattern eliminates branch misprediction in accumulation
 *   - No store-to-stack-array + bit-check pattern for ep/disc accumulation
 *   - Direct scalar index-based load from ep and disc columns for qualifying rows
 *
 * === DECIMAL ARITHMETIC ===
 *   l_extendedprice: int64, scale 100
 *   l_discount:      int64, scale 100
 *   Revenue = SUM(ep * disc) / 10000
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
#include <sys/mman.h>
#include <immintrin.h>
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
    const int64_t disc_lo = 5;    // 0.05 * 100
    const int64_t disc_hi = 7;    // 0.07 * 100
    const int64_t qty_max = 2400; // 24.00 * 100, exclusive

    // === Load columns via mmap (zero-copy) ===
    gendb::MmapColumn<int32_t> col_shipdate(base + "l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(base + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(base + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(base + "l_extendedprice.bin");

    const size_t total_rows = col_shipdate.count;

    // === Advise OS for sequential access ===
    // This helps the kernel prefetcher for HDD-backed mmap pages.
    madvise((void*)col_shipdate.data, total_rows * sizeof(int32_t), MADV_SEQUENTIAL);
    madvise((void*)col_discount.data, total_rows * sizeof(int64_t), MADV_SEQUENTIAL);
    madvise((void*)col_quantity.data, total_rows * sizeof(int64_t), MADV_SEQUENTIAL);
    madvise((void*)col_extprice.data, total_rows * sizeof(int64_t), MADV_SEQUENTIAL);

    // === Zone map pruning ===
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
                    size_t first_qualifying = SIZE_MAX;
                    size_t last_qualifying_end = 0;
                    size_t row_offset = 0;
                    for (uint32_t z = 0; z < num_zones; z++) {
                        size_t zone_count = zones[z].count;
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
                        range_start = range_end = 0;
                    }
                }
            }
            fclose(f);
        }
    }

    // === Parallel morsel scan ===
    const size_t MORSEL_SIZE = 131072;
    const size_t qualifying_rows = (range_end > range_start) ? (range_end - range_start) : 0;
    const size_t num_morsels = (qualifying_rows + MORSEL_SIZE - 1) / MORSEL_SIZE;

    const int32_t* __restrict__ sd   = col_shipdate.data + range_start;
    const int64_t* __restrict__ disc = col_discount.data + range_start;
    const int64_t* __restrict__ qty  = col_quantity.data + range_start;
    const int64_t* __restrict__ ep   = col_extprice.data + range_start;

    int num_threads = omp_get_max_threads();
    struct alignas(64) PaddedSum { __int128 val; char pad[48]; };
    std::vector<PaddedSum> thread_sums(num_threads);
    for (auto& s : thread_sums) s.val = (__int128)0;

    // === Precompute ALL SIMD constants outside the hot loop ===
    // int64 constants for discount and quantity (4-lane AVX2)
    const __m256i v_disc_lo_m1 = _mm256_set1_epi64x(disc_lo - 1); // disc > (disc_lo-1) ↔ disc >= disc_lo
    const __m256i v_disc_hi_p1 = _mm256_set1_epi64x(disc_hi + 1); // disc < (disc_hi+1) ↔ disc <= disc_hi
    const __m256i v_qty_max    = _mm256_set1_epi64x(qty_max);      // qty_max > qty ↔ qty < qty_max

    // Shipdate filter is applied scalar after disc+qty selection (late materialization).
    // date_lo and date_hi are used as scalar comparisons in stages B+C.
    (void)date_lo; (void)date_hi; // suppress potential unused-var if compiler inlines below

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(static) num_threads(num_threads)
        for (size_t m = 0; m < num_morsels; m++) {
            int tid = omp_get_thread_num();
            size_t row_start = m * MORSEL_SIZE;
            size_t row_end   = row_start + MORSEL_SIZE;
            if (row_end > qualifying_rows) row_end = qualifying_rows;

            const int32_t* __restrict__ sd_m   = sd   + row_start;
            const int64_t* __restrict__ disc_m  = disc + row_start;
            const int64_t* __restrict__ qty_m   = qty  + row_start;
            const int64_t* __restrict__ ep_m    = ep   + row_start;
            const size_t len = row_end - row_start;

            __int128 local_sum = 0;

            // Selection vector: disc+qty survivors (indices within morsel)
            // Max ~12% of MORSEL_SIZE = ~15728, but we cap at MORSEL_SIZE for safety
            // Use uint32_t indices since morsel size fits in 32 bits
            static const size_t SEL_MAX = 16384; // max expected survivors per morsel (12% of 131072)
            uint32_t sel[SEL_MAX + 16];  // +16 for SIMD overwrite safety
            size_t sel_count = 0;

            // === STAGE A: SIMD filter on disc+qty → selection vector ===
            // Process 8 rows per iteration (two 4-lane int64 groups).
            // For each passing lane, record the row index.
            size_t i = 0;
            for (; i + 8 <= len; i += 8) {
                __m256i d_lo = _mm256_loadu_si256((const __m256i*)(disc_m + i));
                __m256i d_hi = _mm256_loadu_si256((const __m256i*)(disc_m + i + 4));
                __m256i q_lo = _mm256_loadu_si256((const __m256i*)(qty_m  + i));
                __m256i q_hi = _mm256_loadu_si256((const __m256i*)(qty_m  + i + 4));

                __m256i dge_lo = _mm256_cmpgt_epi64(d_lo, v_disc_lo_m1);
                __m256i dge_hi = _mm256_cmpgt_epi64(d_hi, v_disc_lo_m1);
                __m256i dle_lo = _mm256_cmpgt_epi64(v_disc_hi_p1, d_lo);
                __m256i dle_hi = _mm256_cmpgt_epi64(v_disc_hi_p1, d_hi);
                __m256i qlt_lo = _mm256_cmpgt_epi64(v_qty_max, q_lo);
                __m256i qlt_hi = _mm256_cmpgt_epi64(v_qty_max, q_hi);

                __m256i mask64_lo = _mm256_and_si256(_mm256_and_si256(dge_lo, dle_lo), qlt_lo);
                __m256i mask64_hi = _mm256_and_si256(_mm256_and_si256(dge_hi, dle_hi), qlt_hi);

                // Extract movemask (1 bit per byte per lane → 4 bytes per lane = 4 bits set per passing lane)
                int mv_lo = _mm256_movemask_epi8(mask64_lo);
                int mv_hi = _mm256_movemask_epi8(mask64_hi);

                // Early skip for common case (88% rejection)
                if (__builtin_expect((mv_lo | mv_hi) == 0, 1)) continue;

                // Emit passing indices into selection vector
                // Each 64-bit lane produces 8 identical bytes in movemask (all-0 or all-1 lane)
                // Check each of the 4 groups of 8 bits
                uint32_t base_i = (uint32_t)i;
                if (mv_lo & 0x000000FF) { sel[sel_count++] = base_i + 0; }
                if (mv_lo & 0x0000FF00) { sel[sel_count++] = base_i + 1; }
                if (mv_lo & 0x00FF0000) { sel[sel_count++] = base_i + 2; }
                if (mv_lo & 0xFF000000) { sel[sel_count++] = base_i + 3; }  // NOLINT
                if (mv_hi & 0x000000FF) { sel[sel_count++] = base_i + 4; }
                if (mv_hi & 0x0000FF00) { sel[sel_count++] = base_i + 5; }
                if (mv_hi & 0x00FF0000) { sel[sel_count++] = base_i + 6; }
                if (mv_hi & 0xFF000000) { sel[sel_count++] = base_i + 7; }  // NOLINT

                // Flush selection vector if getting large to avoid cache overflow
                if (__builtin_expect(sel_count >= SEL_MAX, 0)) {
                    // Process current selection vector in stages B+C
                    for (size_t j = 0; j < sel_count; j++) {
                        uint32_t idx = sel[j];
                        int32_t shipdate = sd_m[idx];
                        if (shipdate < date_lo || shipdate >= date_hi) continue;
                        local_sum += (__int128)ep_m[idx] * disc_m[idx];
                    }
                    sel_count = 0;
                }
            }

            // === Scalar tail for remaining rows (< 8) ===
            for (; i < len; i++) {
                int64_t d = disc_m[i];
                if (d < disc_lo || d > disc_hi) continue;
                int64_t q = qty_m[i];
                if (q >= qty_max) continue;
                int32_t shipdate = sd_m[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                local_sum += (__int128)ep_m[i] * d;
            }

            // === STAGE B+C: Apply shipdate filter + accumulate ep*disc ===
            // Process remaining selection vector
            // This loop loads shipdate[sel[j]] and ep[sel[j]] only for disc+qty survivors (~12%)
            // Shipdate filter further reduces to ~1.7% of original rows for ep load.
            for (size_t j = 0; j < sel_count; j++) {
                uint32_t idx = sel[j];
                int32_t shipdate = sd_m[idx];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                local_sum += (__int128)ep_m[idx] * disc_m[idx];
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
