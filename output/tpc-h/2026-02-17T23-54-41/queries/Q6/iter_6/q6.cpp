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
 * === PHYSICAL PLAN (iter 6) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *   2. Parallel morsel scan with fused filter + accumulate
 *      - Morsel size: 131072 rows per task, schedule(static)
 *      - Predicate order optimized: SHIPDATE FIRST (8-lane int32, cheaper) then disc+qty
 *
 *      STAGE A — Shipdate filter (8 int32 lanes, cheapest):
 *        * Check all 8 rows in one AVX2 pass: sd >= date_lo AND sd < date_hi
 *        * If none pass → skip all expensive int64 operations (early exit)
 *        * movemask to get 8-bit lane bitmask (one bit per int32 lane)
 *
 *      STAGE B — Discount + Quantity filter on passing rows only:
 *        * Only for rows where shipdate passes (~14% selectivity from zone-pruned range)
 *        * disc [5,7]: int64 4-lane SIMD, two groups
 *        * qty < 2400: int64 4-lane SIMD, two groups
 *        * Produce final 8-bit combined bitmask
 *
 *      STAGE C — Accumulate ep*disc for rows passing all filters:
 *        * Scalar multiply into __int128 local sum
 *        * Extract int64 values directly from loaded SIMD registers (no store-reload)
 *
 *      - All SIMD constants precomputed OUTSIDE the loop
 *      - Scalar tail for remainder rows
 *      - schedule(static) with large morsels → minimal OpenMP overhead
 *   3. Global merge of thread-local __int128 sums
 *   4. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === KEY OPTIMIZATIONS vs iter_4 ===
 *   - Predicate reorder: SHIPDATE FIRST (int32, 8-lane) → skip expensive int64 ops early
 *     * ~86% of rows in the zone-pruned range fail the shipdate filter (outside 1994)
 *       NOTE: zone-map gives us blocks that OVERLAP [1994,1995), not just 1994 rows.
 *       Many rows in edge blocks are outside the year range → early shipdate exit saves work.
 *   - Eliminate store-reload penalty: no _mm256_storeu_si256 round-trip for ep/disc arrays
 *     * Extract int64 values directly from loaded SIMD registers using _mm_extract_epi64
 *       and _mm256_extracti128_si256 → values stay in registers
 *   - Compact 8-bit shipdate mask: use _mm256_movemask_ps on view-cast to get per-lane bits
 *     efficiently for int32 lanes
 *   - Fused disc+qty mask into 8-bit form efficiently using 4-lane movemask_epi8 condensed
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
#include <immintrin.h>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Extract the i-th int64 from a __m256i without store-reload
static inline int64_t extract64_0(__m256i v) {
    return _mm256_extract_epi64(v, 0);
}
static inline int64_t extract64_1(__m256i v) {
    return _mm256_extract_epi64(v, 1);
}
static inline int64_t extract64_2(__m256i v) {
    return _mm256_extract_epi64(v, 2);
}
static inline int64_t extract64_3(__m256i v) {
    return _mm256_extract_epi64(v, 3);
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/lineitem/";

    // === Date thresholds (epoch days) ===
    const int32_t date_lo = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t date_hi = gendb::date_str_to_epoch_days("1995-01-01"); // exclusive

    // === Scaled integer thresholds ===
    const int64_t disc_lo = 5;   // 0.05 * 100
    const int64_t disc_hi = 7;   // 0.07 * 100
    const int64_t qty_max = 2400; // 24.00 * 100, exclusive

    // === Load columns via mmap (zero-copy) ===
    gendb::MmapColumn<int32_t> col_shipdate(base + "l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(base + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(base + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(base + "l_extendedprice.bin");

    const size_t total_rows = col_shipdate.count;

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
    // int32 constants for shipdate (8-lane AVX2) — checked FIRST for early exit
    const __m256i v_date_lo_m1 = _mm256_set1_epi32(date_lo - 1);  // sd > (date_lo-1) ↔ sd >= date_lo
    const __m256i v_date_hi    = _mm256_set1_epi32(date_hi);       // date_hi > sd ↔ sd < date_hi

    // int64 constants for discount and quantity (4-lane AVX2) — checked only if shipdate passes
    const __m256i v_disc_lo_m1 = _mm256_set1_epi64x(disc_lo - 1); // disc > (disc_lo-1) ↔ disc >= disc_lo
    const __m256i v_disc_hi_p1 = _mm256_set1_epi64x(disc_hi + 1); // disc < (disc_hi+1) ↔ disc <= disc_hi
    const __m256i v_qty_max    = _mm256_set1_epi64x(qty_max);      // qty_max > qty ↔ qty < qty_max

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
            size_t len = row_end - row_start;

            __int128 local_sum = 0;

            // === SIMD inner loop ===
            // STAGE A: Shipdate filter FIRST (int32, 8-lane) — cheapest, highest early exit rate
            // STAGE B: Disc+Qty filter on passing rows only (int64, 4-lane x2)
            // STAGE C: Accumulate ep*disc, extract directly from loaded SIMD regs
            size_t i = 0;

            for (; i + 8 <= len; i += 8) {
                // --- STAGE A: Shipdate check (8 int32 lanes) ---
                __m256i s32 = _mm256_loadu_si256((__m256i*)(sd_m + i));
                // sd >= date_lo: sd > (date_lo - 1)
                __m256i sdge = _mm256_cmpgt_epi32(s32, v_date_lo_m1);
                // sd < date_hi: date_hi > sd
                __m256i sdlt = _mm256_cmpgt_epi32(v_date_hi, s32);
                __m256i sd_mask32 = _mm256_and_si256(sdge, sdlt);

                // Get 8-bit shipdate bitmask (one bit per int32 lane)
                // _mm256_movemask_ps treats 256 bits as 8×float, checks sign bit of each 32-bit element
                int sd_bits = _mm256_movemask_ps(_mm256_castsi256_ps(sd_mask32));
                if (sd_bits == 0) continue; // No rows pass shipdate — skip expensive int64 work

                // --- STAGE B: Discount + Quantity (int64, two groups of 4 lanes) ---
                __m256i d_lo = _mm256_loadu_si256((__m256i*)(disc_m + i));     // disc[0..3]
                __m256i d_hi = _mm256_loadu_si256((__m256i*)(disc_m + i + 4)); // disc[4..7]
                __m256i q_lo = _mm256_loadu_si256((__m256i*)(qty_m  + i));
                __m256i q_hi = _mm256_loadu_si256((__m256i*)(qty_m  + i + 4));

                // disc >= disc_lo: disc > (disc_lo - 1)
                __m256i dge_lo = _mm256_cmpgt_epi64(d_lo, v_disc_lo_m1);
                __m256i dge_hi = _mm256_cmpgt_epi64(d_hi, v_disc_lo_m1);
                // disc <= disc_hi: (disc_hi + 1) > disc
                __m256i dle_lo = _mm256_cmpgt_epi64(v_disc_hi_p1, d_lo);
                __m256i dle_hi = _mm256_cmpgt_epi64(v_disc_hi_p1, d_hi);
                // qty < qty_max: qty_max > qty
                __m256i qlt_lo = _mm256_cmpgt_epi64(v_qty_max, q_lo);
                __m256i qlt_hi = _mm256_cmpgt_epi64(v_qty_max, q_hi);

                // Combine disc+qty masks
                __m256i dq_lo = _mm256_and_si256(_mm256_and_si256(dge_lo, dle_lo), qlt_lo);
                __m256i dq_hi = _mm256_and_si256(_mm256_and_si256(dge_hi, dle_hi), qlt_hi);

                // Convert 4-lane int64 masks to 4-bit form each:
                // movemask_epi8 on a 4-lane int64 mask gives 4×8 = 32 bits, with 8 bits per lane.
                // Lane i passes if bits [8i..8i+7] are all set → check byte 0,8,16,24.
                int dq_mv_lo = _mm256_movemask_epi8(dq_lo); // bits 0,8,16,24 indicate lanes 0-3
                int dq_mv_hi = _mm256_movemask_epi8(dq_hi); // bits 0,8,16,24 indicate lanes 0-3

                // Extract per-lane bits for lanes 0-3 from movemask_epi8 output (8 bits per lane):
                // lane k passes if (dq_mv >> (8*k)) & 0xFF == 0xFF
                // Compress to 4-bit form using nibble extraction:
                // bit k = (dq_mv >> (k*8)) & 1
                int dq_bits_lo = ((dq_mv_lo & 0x01))       |
                                 ((dq_mv_lo >> 7) & 0x02)  |
                                 ((dq_mv_lo >> 14) & 0x04) |
                                 ((dq_mv_lo >> 21) & 0x08);
                int dq_bits_hi = ((dq_mv_hi & 0x01))       |
                                 ((dq_mv_hi >> 7) & 0x02)  |
                                 ((dq_mv_hi >> 14) & 0x04) |
                                 ((dq_mv_hi >> 21) & 0x08);

                // Combine disc+qty bits (8-bit, lanes 0-7) with sd_bits (8-bit, lanes 0-7)
                // dq_bits_lo covers lanes 0-3, dq_bits_hi covers lanes 4-7
                int dq_bits = dq_bits_lo | (dq_bits_hi << 4);
                int final_bits = sd_bits & dq_bits;
                if (final_bits == 0) continue;

                // --- STAGE C: Accumulate ep*disc for passing rows ---
                // Load ep; extract values directly from SIMD registers (no store-reload)
                __m256i ep_lo = _mm256_loadu_si256((__m256i*)(ep_m + i));
                __m256i ep_hi = _mm256_loadu_si256((__m256i*)(ep_m + i + 4));

                if (final_bits & 0x01) local_sum += (__int128)extract64_0(ep_lo) * extract64_0(d_lo);
                if (final_bits & 0x02) local_sum += (__int128)extract64_1(ep_lo) * extract64_1(d_lo);
                if (final_bits & 0x04) local_sum += (__int128)extract64_2(ep_lo) * extract64_2(d_lo);
                if (final_bits & 0x08) local_sum += (__int128)extract64_3(ep_lo) * extract64_3(d_lo);
                if (final_bits & 0x10) local_sum += (__int128)extract64_0(ep_hi) * extract64_0(d_hi);
                if (final_bits & 0x20) local_sum += (__int128)extract64_1(ep_hi) * extract64_1(d_hi);
                if (final_bits & 0x40) local_sum += (__int128)extract64_2(ep_hi) * extract64_2(d_hi);
                if (final_bits & 0x80) local_sum += (__int128)extract64_3(ep_hi) * extract64_3(d_hi);
            }

            // === Scalar tail ===
            for (; i < len; i++) {
                int32_t shipdate = sd_m[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                int64_t d = disc_m[i];
                if (d < disc_lo || d > disc_hi) continue;
                int64_t q = qty_m[i];
                if (q >= qty_max) continue;
                local_sum += (__int128)ep_m[i] * d;
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
