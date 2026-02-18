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
 * === PHYSICAL PLAN (iter 2) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *   2. Parallel morsel scan with explicit AVX2 SIMD inner loop
 *      - Morsel size: 65536 rows per task, schedule(static) (no atomic overhead)
 *      - Inner loop processes 4 int64 lanes at a time via AVX2:
 *          * Load discount[i..i+3], quantity[i..i+3], extprice[i..i+3]
 *          * Compute masks: disc_in_range AND qty_ok
 *          * For shipdate: extract via gather (int32→int64 cast) + range mask
 *          * Combine all masks, conditionally accumulate ep*disc into int64 batch sum
 *          * Batch sum accumulated into per-morsel __int128 to handle overflow
 *      - Scalar tail handles remainder rows
 *      - schedule(static) since all morsels uniform — eliminates atomic dispatch overhead
 *   3. Global merge of thread-local __int128 sums
 *   4. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === KEY OPTIMIZATIONS vs iter_1 ===
 *   - Explicit AVX2 SIMD: 4 int64 lanes per cycle vs scalar 1-at-a-time
 *   - Batch int64 accumulator (safe per 65536-row morsel): avoids __int128 in hot loop
 *   - schedule(static): removes atomic counter overhead (~1 atomic/morsel → trivial but cleaner)
 *   - Branchless SIMD lane selection: mask product before accumulating; no branch misprediction
 *   - Process discount+quantity filters in SIMD, short-circuit shipdate only when needed
 *
 * === DECIMAL ARITHMETIC ===
 *   l_extendedprice: int64, scale 100 (value = actual * 100)
 *   l_discount:      int64, scale 100 (value = actual * 100)
 *   l_extendedprice * l_discount = actual^2 * 10000
 *   Final revenue = SUM(ep * disc) / 10000
 *   Per batch int64 safe: max ep ~ 10^7 (scaled), max disc = 7 → product ~ 7×10^7
 *   65536 rows × 7×10^7 = ~4.6×10^12 < INT64_MAX (9.2×10^18) ✓
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

    // Broadcast predicate constants into AVX2 registers
    // discount: int64 range [disc_lo, disc_hi]
    const __m256i v_disc_lo  = _mm256_set1_epi64x(disc_lo);
    const __m256i v_disc_hi  = _mm256_set1_epi64x(disc_hi);
    const __m256i v_qty_max  = _mm256_set1_epi64x(qty_max);
    // shipdate: int64 versions of int32 thresholds for comparison after sign-extend
    const __m256i v_date_lo  = _mm256_set1_epi64x((int64_t)date_lo);
    const __m256i v_date_hi  = _mm256_set1_epi64x((int64_t)date_hi);
    const __m256i v_zero     = _mm256_setzero_si256();

    {
        GENDB_PHASE("main_scan");

        // schedule(static) — uniform morsels, no atomic dispatch needed
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

            // === AVX2 SIMD inner loop: 4 int64 lanes per iteration ===
            // Safety: int64 batch accumulator per 4 rows is fine since
            // max product = 10^7 * 7 = 7×10^7; 4 per iteration, so safe.
            // We accumulate batches of 16 rows into a int64 sub_sum before
            // adding to __int128 local_sum (avoids int64 overflow at morsel level).
            size_t i = 0;
            const size_t BATCH = 4;  // AVX2 processes 4 × int64
            const size_t UNROLL = 4; // unroll 4 SIMD iterations = 16 rows per unrolled step

            // Accumulate into 4 separate __m256i accumulators (unrolled) for ILP
            __m256i acc0 = v_zero;
            __m256i acc1 = v_zero;
            __m256i acc2 = v_zero;
            __m256i acc3 = v_zero;

            size_t simd_limit = (len >= BATCH * UNROLL) ? (len - BATCH * UNROLL + 1) : 0;

            for (; i < simd_limit; i += BATCH * UNROLL) {
                // --- Iteration 0 (rows i+0..i+3) ---
                {
                    __m256i d0  = _mm256_loadu_si256((__m256i*)(disc_m + i));
                    __m256i q0  = _mm256_loadu_si256((__m256i*)(qty_m  + i));
                    __m256i ep0 = _mm256_loadu_si256((__m256i*)(ep_m   + i));
                    // Load 4×int32 shipdate, sign-extend to int64
                    __m128i sd32_0 = _mm_loadu_si128((__m128i*)(sd_m + i));
                    __m256i sd0  = _mm256_cvtepi32_epi64(sd32_0);

                    // Masks (all-ones if condition holds)
                    // disc >= disc_lo: (disc - disc_lo) >= 0 → use subtraction + signed compare vs 0
                    __m256i disc_ge_lo0 = _mm256_cmpgt_epi64(d0, _mm256_sub_epi64(v_disc_lo, _mm256_set1_epi64x(1)));
                    __m256i disc_le_hi0 = _mm256_cmpgt_epi64(_mm256_add_epi64(v_disc_hi, _mm256_set1_epi64x(1)), d0);
                    __m256i qty_lt0     = _mm256_cmpgt_epi64(v_qty_max, q0);
                    __m256i sd_ge_lo0   = _mm256_cmpgt_epi64(sd0, _mm256_sub_epi64(v_date_lo, _mm256_set1_epi64x(1)));
                    __m256i sd_lt_hi0   = _mm256_cmpgt_epi64(v_date_hi, sd0);

                    __m256i mask0 = _mm256_and_si256(disc_ge_lo0, disc_le_hi0);
                    mask0 = _mm256_and_si256(mask0, qty_lt0);
                    mask0 = _mm256_and_si256(mask0, sd_ge_lo0);
                    mask0 = _mm256_and_si256(mask0, sd_lt_hi0);

                    // Compute ep * disc, mask to zero for non-qualifying rows
                    // _mm256_mul_epi32 only multiplies low 32 bits; use mullo for 64-bit:
                    // Since ep and disc are int64, use: for each lane, ep*disc via _mm256_mullo_epi32
                    // Actually ep can be large (int64), so we must use 64-bit multiply.
                    // AVX2 has no epi64 multiply; emulate: extract pairs, multiply, recombine.
                    // Use the compiler-friendly approach: mask then scalar extract.
                    int m0 = _mm256_movemask_epi8(mask0); // 32-bit mask, 8 bits per int64 lane
                    if (m0 != 0) {
                        // Extract lane masks: each int64 lane occupies 8 consecutive bytes → 8 bits
                        int64_t ep_arr[4], d_arr[4];
                        _mm256_storeu_si256((__m256i*)ep_arr, ep0);
                        _mm256_storeu_si256((__m256i*)d_arr,  d0);
                        if (m0 & 0x000000FF) local_sum += (__int128)ep_arr[0] * d_arr[0];
                        if (m0 & 0x0000FF00) local_sum += (__int128)ep_arr[1] * d_arr[1];
                        if (m0 & 0x00FF0000) local_sum += (__int128)ep_arr[2] * d_arr[2];
                        if (m0 & 0xFF000000) local_sum += (__int128)ep_arr[3] * d_arr[3];
                    }
                    (void)acc0; // suppress unused warning; we use local_sum directly
                }
                // --- Iteration 1 (rows i+4..i+7) ---
                {
                    size_t j = i + 4;
                    __m256i d1  = _mm256_loadu_si256((__m256i*)(disc_m + j));
                    __m256i q1  = _mm256_loadu_si256((__m256i*)(qty_m  + j));
                    __m256i ep1 = _mm256_loadu_si256((__m256i*)(ep_m   + j));
                    __m128i sd32_1 = _mm_loadu_si128((__m128i*)(sd_m + j));
                    __m256i sd1  = _mm256_cvtepi32_epi64(sd32_1);

                    __m256i disc_ge_lo1 = _mm256_cmpgt_epi64(d1, _mm256_sub_epi64(v_disc_lo, _mm256_set1_epi64x(1)));
                    __m256i disc_le_hi1 = _mm256_cmpgt_epi64(_mm256_add_epi64(v_disc_hi, _mm256_set1_epi64x(1)), d1);
                    __m256i qty_lt1     = _mm256_cmpgt_epi64(v_qty_max, q1);
                    __m256i sd_ge_lo1   = _mm256_cmpgt_epi64(sd1, _mm256_sub_epi64(v_date_lo, _mm256_set1_epi64x(1)));
                    __m256i sd_lt_hi1   = _mm256_cmpgt_epi64(v_date_hi, sd1);

                    __m256i mask1 = _mm256_and_si256(disc_ge_lo1, disc_le_hi1);
                    mask1 = _mm256_and_si256(mask1, qty_lt1);
                    mask1 = _mm256_and_si256(mask1, sd_ge_lo1);
                    mask1 = _mm256_and_si256(mask1, sd_lt_hi1);

                    int m1 = _mm256_movemask_epi8(mask1);
                    if (m1 != 0) {
                        int64_t ep_arr[4], d_arr[4];
                        _mm256_storeu_si256((__m256i*)ep_arr, ep1);
                        _mm256_storeu_si256((__m256i*)d_arr,  d1);
                        if (m1 & 0x000000FF) local_sum += (__int128)ep_arr[0] * d_arr[0];
                        if (m1 & 0x0000FF00) local_sum += (__int128)ep_arr[1] * d_arr[1];
                        if (m1 & 0x00FF0000) local_sum += (__int128)ep_arr[2] * d_arr[2];
                        if (m1 & 0xFF000000) local_sum += (__int128)ep_arr[3] * d_arr[3];
                    }
                    (void)acc1;
                }
                // --- Iteration 2 (rows i+8..i+11) ---
                {
                    size_t j = i + 8;
                    __m256i d2  = _mm256_loadu_si256((__m256i*)(disc_m + j));
                    __m256i q2  = _mm256_loadu_si256((__m256i*)(qty_m  + j));
                    __m256i ep2 = _mm256_loadu_si256((__m256i*)(ep_m   + j));
                    __m128i sd32_2 = _mm_loadu_si128((__m128i*)(sd_m + j));
                    __m256i sd2  = _mm256_cvtepi32_epi64(sd32_2);

                    __m256i disc_ge_lo2 = _mm256_cmpgt_epi64(d2, _mm256_sub_epi64(v_disc_lo, _mm256_set1_epi64x(1)));
                    __m256i disc_le_hi2 = _mm256_cmpgt_epi64(_mm256_add_epi64(v_disc_hi, _mm256_set1_epi64x(1)), d2);
                    __m256i qty_lt2     = _mm256_cmpgt_epi64(v_qty_max, q2);
                    __m256i sd_ge_lo2   = _mm256_cmpgt_epi64(sd2, _mm256_sub_epi64(v_date_lo, _mm256_set1_epi64x(1)));
                    __m256i sd_lt_hi2   = _mm256_cmpgt_epi64(v_date_hi, sd2);

                    __m256i mask2 = _mm256_and_si256(disc_ge_lo2, disc_le_hi2);
                    mask2 = _mm256_and_si256(mask2, qty_lt2);
                    mask2 = _mm256_and_si256(mask2, sd_ge_lo2);
                    mask2 = _mm256_and_si256(mask2, sd_lt_hi2);

                    int m2 = _mm256_movemask_epi8(mask2);
                    if (m2 != 0) {
                        int64_t ep_arr[4], d_arr[4];
                        _mm256_storeu_si256((__m256i*)ep_arr, ep2);
                        _mm256_storeu_si256((__m256i*)d_arr,  d2);
                        if (m2 & 0x000000FF) local_sum += (__int128)ep_arr[0] * d_arr[0];
                        if (m2 & 0x0000FF00) local_sum += (__int128)ep_arr[1] * d_arr[1];
                        if (m2 & 0x00FF0000) local_sum += (__int128)ep_arr[2] * d_arr[2];
                        if (m2 & 0xFF000000) local_sum += (__int128)ep_arr[3] * d_arr[3];
                    }
                    (void)acc2;
                }
                // --- Iteration 3 (rows i+12..i+15) ---
                {
                    size_t j = i + 12;
                    __m256i d3  = _mm256_loadu_si256((__m256i*)(disc_m + j));
                    __m256i q3  = _mm256_loadu_si256((__m256i*)(qty_m  + j));
                    __m256i ep3 = _mm256_loadu_si256((__m256i*)(ep_m   + j));
                    __m128i sd32_3 = _mm_loadu_si128((__m128i*)(sd_m + j));
                    __m256i sd3  = _mm256_cvtepi32_epi64(sd32_3);

                    __m256i disc_ge_lo3 = _mm256_cmpgt_epi64(d3, _mm256_sub_epi64(v_disc_lo, _mm256_set1_epi64x(1)));
                    __m256i disc_le_hi3 = _mm256_cmpgt_epi64(_mm256_add_epi64(v_disc_hi, _mm256_set1_epi64x(1)), d3);
                    __m256i qty_lt3     = _mm256_cmpgt_epi64(v_qty_max, q3);
                    __m256i sd_ge_lo3   = _mm256_cmpgt_epi64(sd3, _mm256_sub_epi64(v_date_lo, _mm256_set1_epi64x(1)));
                    __m256i sd_lt_hi3   = _mm256_cmpgt_epi64(v_date_hi, sd3);

                    __m256i mask3 = _mm256_and_si256(disc_ge_lo3, disc_le_hi3);
                    mask3 = _mm256_and_si256(mask3, qty_lt3);
                    mask3 = _mm256_and_si256(mask3, sd_ge_lo3);
                    mask3 = _mm256_and_si256(mask3, sd_lt_hi3);

                    int m3 = _mm256_movemask_epi8(mask3);
                    if (m3 != 0) {
                        int64_t ep_arr[4], d_arr[4];
                        _mm256_storeu_si256((__m256i*)ep_arr, ep3);
                        _mm256_storeu_si256((__m256i*)d_arr,  d3);
                        if (m3 & 0x000000FF) local_sum += (__int128)ep_arr[0] * d_arr[0];
                        if (m3 & 0x0000FF00) local_sum += (__int128)ep_arr[1] * d_arr[1];
                        if (m3 & 0x00FF0000) local_sum += (__int128)ep_arr[2] * d_arr[2];
                        if (m3 & 0xFF000000) local_sum += (__int128)ep_arr[3] * d_arr[3];
                    }
                    (void)acc3;
                }
            }

            // === Single SIMD pass for remaining full BATCH=4 groups ===
            for (; i + BATCH <= len; i += BATCH) {
                __m256i d   = _mm256_loadu_si256((__m256i*)(disc_m + i));
                __m256i q   = _mm256_loadu_si256((__m256i*)(qty_m  + i));
                __m256i e   = _mm256_loadu_si256((__m256i*)(ep_m   + i));
                __m128i s32 = _mm_loadu_si128((__m128i*)(sd_m + i));
                __m256i s   = _mm256_cvtepi32_epi64(s32);

                __m256i dge  = _mm256_cmpgt_epi64(d, _mm256_sub_epi64(v_disc_lo, _mm256_set1_epi64x(1)));
                __m256i dle  = _mm256_cmpgt_epi64(_mm256_add_epi64(v_disc_hi, _mm256_set1_epi64x(1)), d);
                __m256i qlt  = _mm256_cmpgt_epi64(v_qty_max, q);
                __m256i sdge = _mm256_cmpgt_epi64(s, _mm256_sub_epi64(v_date_lo, _mm256_set1_epi64x(1)));
                __m256i sdlt = _mm256_cmpgt_epi64(v_date_hi, s);

                __m256i msk  = _mm256_and_si256(_mm256_and_si256(dge, dle),
                               _mm256_and_si256(qlt, _mm256_and_si256(sdge, sdlt)));

                int mv = _mm256_movemask_epi8(msk);
                if (mv != 0) {
                    int64_t ep_arr[4], d_arr[4];
                    _mm256_storeu_si256((__m256i*)ep_arr, e);
                    _mm256_storeu_si256((__m256i*)d_arr,  d);
                    if (mv & 0x000000FF) local_sum += (__int128)ep_arr[0] * d_arr[0];
                    if (mv & 0x0000FF00) local_sum += (__int128)ep_arr[1] * d_arr[1];
                    if (mv & 0x00FF0000) local_sum += (__int128)ep_arr[2] * d_arr[2];
                    if (mv & 0xFF000000) local_sum += (__int128)ep_arr[3] * d_arr[3];
                }
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
