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
 * === PHYSICAL PLAN (iter 7) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *   2. Parallel morsel scan with fused filter + accumulate
 *      - Morsel size: 131072 rows per task, schedule(static)
 *      - Predicate strategy:
 *        * Since data sorted by l_shipdate and zone-map prunes to 1994 range,
 *          most rows in pruned window pass shipdate filter. Check disc+qty first
 *          (most selective together: ~12% pass), then shipdate.
 *
 *      SIMD LOOP (processes 8 rows per iteration):
 *        1. Load disc[0..3], disc[4..7] (int64, 4 lanes each)
 *        2. Load qty[0..3], qty[4..7]  (int64, 4 lanes each)
 *        3. Compute disc+qty combined mask (int64 SIMD)
 *        4. Early exit if zero rows pass disc+qty
 *        5. Load shipdate[0..7] (int32, 8 lanes) → split to two int64 masks
 *        6. Final combined mask
 *        7. Early exit if zero rows pass all three filters
 *        8. Accumulate ep*disc for passing lanes:
 *           - Extract disc values directly from d_lo/d_hi via _mm256_extract_epi64
 *             (avoids store-to-stack round-trip for disc values)
 *           - Read ep_m[i+k] directly (ep column loaded only for qualifying rows)
 *           - Prefetch ep for next iteration to hide latency
 *
 *      Scalar tail for remainder rows.
 *
 *   3. Global merge of thread-local __int128 sums
 *   4. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === KEY OPTIMIZATIONS vs iter_4 ===
 *   - Eliminated store-to-stack for disc: use _mm256_extract_epi64 on d_lo/d_hi registers
 *   - Eliminated unconditional ep SIMD load: only read ep_m[i+k] when row actually qualifies
 *   - Software prefetch of ep column: __builtin_prefetch ahead by PREFETCH_DIST rows
 *   - Conditional disc_m prefetch: helps for discount/qty columns
 *   - Tighter inner loop: fewer memory operations when most 8-row groups are rejected early
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

// Prefetch distance in rows (64-byte cache lines, int64 = 8 bytes → 8 per line)
// Tune: 16 cache lines ahead = 128 rows. Adjust based on memory latency (~100ns HDD/RAM).
static constexpr int PREFETCH_DIST = 256;

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
    const __m256i v_disc_lo_m1 = _mm256_set1_epi64x(disc_lo - 1); // disc > (disc_lo-1) ↔ disc >= disc_lo
    const __m256i v_disc_hi_p1 = _mm256_set1_epi64x(disc_hi + 1); // (disc_hi+1) > disc ↔ disc <= disc_hi
    const __m256i v_qty_max    = _mm256_set1_epi64x(qty_max);      // qty_max > qty ↔ qty < qty_max
    const __m256i v_date_lo_m1 = _mm256_set1_epi32(date_lo - 1);  // sd > (date_lo-1) ↔ sd >= date_lo
    const __m256i v_date_hi    = _mm256_set1_epi32(date_hi);       // date_hi > sd ↔ sd < date_hi

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

            size_t i = 0;

            for (; i + 8 <= len; i += 8) {
                // Prefetch ep column data ahead to hide latency.
                // ep is only accessed for qualifying rows; prefetching ensures it's
                // in cache when needed without paying full memory latency.
                if (i + PREFETCH_DIST < len) {
                    __builtin_prefetch(ep_m + i + PREFETCH_DIST,     0, 1);
                    __builtin_prefetch(ep_m + i + PREFETCH_DIST + 4, 0, 1);
                }

                // --- Discount + Quantity: two groups of 4 int64 lanes ---
                __m256i d_lo = _mm256_loadu_si256((__m256i*)(disc_m + i));     // disc[0..3]
                __m256i d_hi = _mm256_loadu_si256((__m256i*)(disc_m + i + 4)); // disc[4..7]
                __m256i q_lo = _mm256_loadu_si256((__m256i*)(qty_m  + i));
                __m256i q_hi = _mm256_loadu_si256((__m256i*)(qty_m  + i + 4));

                // disc >= disc_lo AND disc <= disc_hi AND qty < qty_max
                __m256i dge_lo = _mm256_cmpgt_epi64(d_lo, v_disc_lo_m1);
                __m256i dge_hi = _mm256_cmpgt_epi64(d_hi, v_disc_lo_m1);
                __m256i dle_lo = _mm256_cmpgt_epi64(v_disc_hi_p1, d_lo);
                __m256i dle_hi = _mm256_cmpgt_epi64(v_disc_hi_p1, d_hi);
                __m256i qlt_lo = _mm256_cmpgt_epi64(v_qty_max, q_lo);
                __m256i qlt_hi = _mm256_cmpgt_epi64(v_qty_max, q_hi);

                __m256i mask64_lo = _mm256_and_si256(_mm256_and_si256(dge_lo, dle_lo), qlt_lo);
                __m256i mask64_hi = _mm256_and_si256(_mm256_and_si256(dge_hi, dle_hi), qlt_hi);

                // Early exit if no rows pass disc+qty (most rows rejected here: ~88%)
                int mv_lo = _mm256_movemask_epi8(mask64_lo);
                int mv_hi = _mm256_movemask_epi8(mask64_hi);
                if ((mv_lo | mv_hi) == 0) continue;

                // --- Shipdate: 8 int32 lanes ---
                __m256i s32 = _mm256_loadu_si256((__m256i*)(sd_m + i)); // 8 int32
                __m256i sdge = _mm256_cmpgt_epi32(s32, v_date_lo_m1);
                __m256i sdlt = _mm256_cmpgt_epi32(v_date_hi, s32);
                __m256i sd_mask32 = _mm256_and_si256(sdge, sdlt); // 8 int32 mask

                // Sign-extend each int32 lane to int64 for ANDing with int64 masks
                __m128i sd_lo128 = _mm256_castsi256_si128(sd_mask32);
                __m128i sd_hi128 = _mm256_extracti128_si256(sd_mask32, 1);
                __m256i sd_mask64_lo = _mm256_cvtepi32_epi64(sd_lo128);
                __m256i sd_mask64_hi = _mm256_cvtepi32_epi64(sd_hi128);

                // Final combined masks
                __m256i final_lo = _mm256_and_si256(mask64_lo, sd_mask64_lo);
                __m256i final_hi = _mm256_and_si256(mask64_hi, sd_mask64_hi);

                int fmv_lo = _mm256_movemask_epi8(final_lo);
                int fmv_hi = _mm256_movemask_epi8(final_hi);

                if ((fmv_lo | fmv_hi) == 0) continue;

                // Accumulate ep*disc for passing lanes.
                // Extract disc directly from the already-loaded d_lo/d_hi SIMD registers
                // using _mm_extract_epi64 — avoids a store-to-stack + reload round-trip.
                // Read ep_m[i+k] directly (conditional load only for qualifying rows).
                // This is more cache-friendly than unconditionally storing 8 ep values to stack.
                if (fmv_lo & 0x000000FF) local_sum += (__int128)ep_m[i+0] * (int64_t)_mm256_extract_epi64(d_lo, 0);
                if (fmv_lo & 0x0000FF00) local_sum += (__int128)ep_m[i+1] * (int64_t)_mm256_extract_epi64(d_lo, 1);
                if (fmv_lo & 0x00FF0000) local_sum += (__int128)ep_m[i+2] * (int64_t)_mm256_extract_epi64(d_lo, 2);
                if (fmv_lo & 0xFF000000) local_sum += (__int128)ep_m[i+3] * (int64_t)_mm256_extract_epi64(d_lo, 3);
                if (fmv_hi & 0x000000FF) local_sum += (__int128)ep_m[i+4] * (int64_t)_mm256_extract_epi64(d_hi, 0);
                if (fmv_hi & 0x0000FF00) local_sum += (__int128)ep_m[i+5] * (int64_t)_mm256_extract_epi64(d_hi, 1);
                if (fmv_hi & 0x00FF0000) local_sum += (__int128)ep_m[i+6] * (int64_t)_mm256_extract_epi64(d_hi, 2);
                if (fmv_hi & 0xFF000000) local_sum += (__int128)ep_m[i+7] * (int64_t)_mm256_extract_epi64(d_hi, 3);
            }

            // === Scalar tail ===
            for (; i < len; i++) {
                int64_t d = disc_m[i];
                if (d < disc_lo || d > disc_hi) continue;
                int64_t q = qty_m[i];
                if (q >= qty_max) continue;
                int32_t shipdate = sd_m[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
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
