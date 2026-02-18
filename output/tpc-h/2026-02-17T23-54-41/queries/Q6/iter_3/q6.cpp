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
 *   Single table: lineitem (59.9M rows), sorted by l_shipdate
 *   Predicates (all single-table):
 *     1. l_shipdate >= 8766  (1994-01-01 epoch days, int32_t)
 *     2. l_shipdate <  9131  (1995-01-01 epoch days, int32_t)
 *     3. l_discount >= 5     (0.05 * 100, scaled int64)
 *     4. l_discount <= 7     (0.07 * 100, scaled int64)
 *     5. l_quantity < 2400   (24 * 100, scaled int64)
 *   Aggregation: SUM(l_extendedprice * l_discount)
 *   No joins, no GROUP BY.
 *
 * === PHYSICAL PLAN (iter 3) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *   2. Parallel morsel scan — AVX2 8-wide int32 inner loop (key improvement over iter 2):
 *
 *   CRITICAL INSIGHT vs iter 2:
 *     - iter 2 used AVX2 with 4×int64 lanes but fell back to store→scalar-branch for
 *       accumulation (no AVX2 epi64 multiply exists), defeating SIMD throughput.
 *     - iter 3 exploits that ALL columns fit in int32 for purposes of this filter+multiply:
 *         * l_discount: max value 7 (fits easily in int32)
 *         * l_quantity: max value 5000 (scale 100, fits in int32)
 *         * l_extendedprice: max ~$104,950 → scaled = 10,495,000 → fits in int32 (< 2^31)
 *         * l_shipdate: already int32
 *       Therefore we can use _mm256_mullo_epi32 to multiply ep×disc in 8-wide SIMD!
 *       Products fit in int32 (max: 10,495,000 × 7 = 73,465,000 < 2^31 ✓).
 *
 *   INNER LOOP STRATEGY:
 *     a) Load 8 int32_t shipdates from l_shipdate (one full AVX2 register)
 *     b) Load 8 int64_t discount values; pack lower 32 bits of each into one __m256i via
 *        _mm256_packs_epi32 / _mm256_permutevar8x32 → 8 int32 discount values
 *     c) Load 8 int64_t quantity values; same narrowing → 8 int32
 *     d) Load 8 int64_t extprice values; same narrowing → 8 int32
 *     e) Apply all 5 predicates as int32 comparisons (8-wide)
 *     f) Mask ep×disc products: _mm256_mullo_epi32(ep32, disc32) & mask
 *     g) Zero-extend masked products to int64, add to two 4-wide int64 accumulators
 *        (lo 4 lanes + hi 4 lanes separately since no 8-wide int64 accumulator needed)
 *     h) Periodically drain int64 accumulators into __int128 local_sum
 *
 *   BENEFIT: Fully branchless, fully vectorized inner loop — no store/load roundtrip,
 *   no branch on mask. 8 rows per iteration vs 4 in iter 2, true SIMD multiply.
 *
 *   3. schedule(static) parallel for, MORSEL_SIZE tuned for L3 / threads / 4 columns
 *   4. Global merge of thread-local __int128 sums
 *   5. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === DECIMAL ARITHMETIC ===
 *   l_extendedprice: int64, scale 100. Typical max (SF10): ~10,495,000 (scaled) < 2^31 ✓
 *   l_discount:      int64, scale 100. Max = 7 (0.07 × 100) < 2^31 ✓
 *   Product ep×disc: max = 73,465,000 → fits in int32 ✓ (enables _mm256_mullo_epi32)
 *   Final revenue = SUM(ep * disc) / 10000
 *
 * === INT64 ACCUMULATOR DRAIN SAFETY ===
 *   Per-drain budget (8 rows × max_product = 8 × 73,465,000 ≈ 587,720,000)
 *   We drain every 4096 iterations (= 32,768 rows):
 *   32,768 × 73,465,000 ≈ 2.41×10^12 < INT64_MAX (9.22×10^18) ✓
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

// Helper: narrow 8 int64 values (from two AVX2 registers) to 8 int32 values in one AVX2 register.
// Takes lo=lower 4 int64, hi=upper 4 int64.
// Each int64 must have value fitting in int32 (caller guarantees this).
// Strategy: use _mm256_shuffle_epi32 to pick dword 0 of each qword, then pack.
static inline __m256i narrow_int64x8_to_int32x8(__m256i lo4, __m256i hi4) {
    // Extract the lower 32 bits of each 64-bit lane.
    // _mm256_shuffle_epi32 with imm=0b10001000 = 0x88: picks elements 0,2 from each 128-bit lane.
    // lo4: [a0_lo, a0_hi, a1_lo, a1_hi, a2_lo, a2_hi, a3_lo, a3_hi] (int32 view)
    // After shuffle 0x88: [a0_lo, a1_lo, 0, 0, a2_lo, a3_lo, 0, 0]
    __m256i lo_narrow = _mm256_shuffle_epi32(lo4, 0x88); // [a0,a1,_,_,a2,a3,_,_]
    __m256i hi_narrow = _mm256_shuffle_epi32(hi4, 0x88); // [b0,b1,_,_,b2,b3,_,_]

    // Permute to consolidate: we want [a0,a1,a2,a3,b0,b1,b2,b3] as 32-bit words
    // lo_narrow 128-bit lanes: lane0=[a0,a1,?,?], lane1=[a2,a3,?,?]
    // Use _mm256_permute4x64_epi64 to rearrange 64-bit chunks:
    //   lo_narrow: qwords = [a0a1, ??, a2a3, ??]
    //   after permute(0b10001000=0x88): [a0a1, a2a3, a0a1, a2a3] → nope
    // Better: use _mm256_permutevar8x32_epi32 to select specific 32-bit lanes
    // lo_narrow 32-bit indices: [a0=0, a1=1, _=2, _=3, a2=4, a3=5, _=6, _=7]
    // hi_narrow 32-bit indices: [b0=0, b1=1, _=2, _=3, b2=4, b3=5, _=6, _=7]

    // Combine: put a0,a1,a2,a3 in positions 0-3, b0,b1,b2,b3 in positions 4-7
    // Use _mm256_unpacklo_epi64 and permute
    // Simpler: permute lo to get [a0,a1,a2,a3,a0,a1,a2,a3] then blend with hi
    const __m256i perm_lo = _mm256_permutevar8x32_epi32(lo_narrow,
        _mm256_set_epi32(5,4,1,0, 5,4,1,0)); // [a0,a1,a2,a3,a0,a1,a2,a3]
    const __m256i perm_hi = _mm256_permutevar8x32_epi32(hi_narrow,
        _mm256_set_epi32(5,4,1,0, 5,4,1,0)); // [b0,b1,b2,b3,b0,b1,b2,b3]
    // Blend: take low 128-bits from perm_lo, high 128-bits from perm_hi
    return _mm256_blend_epi32(perm_lo, perm_hi, 0xF0); // 0xF0 = 11110000 → hi 4 from perm_hi
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/lineitem/";

    // === Date thresholds (epoch days) ===
    const int32_t date_lo = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t date_hi = gendb::date_str_to_epoch_days("1995-01-01"); // exclusive

    // === Scaled integer thresholds ===
    // discount BETWEEN 0.05 AND 0.07 → [5, 7] (scale 100)
    const int32_t disc_lo32 = 5;
    const int32_t disc_hi32 = 7;
    // quantity < 24 → < 2400 (scale 100)
    const int32_t qty_max32 = 2400; // exclusive

    // === Load columns via mmap (zero-copy) ===
    gendb::MmapColumn<int32_t> col_shipdate(base + "l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(base + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(base + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(base + "l_extendedprice.bin");

    const size_t total_rows = col_shipdate.count;

    // === Zone map pruning — determine contiguous qualifying row range ===
    // Since data is sorted by l_shipdate, the qualifying zones are contiguous.
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

    // === Parallel morsel scan with fused filter + accumulate ===
    // MORSEL_SIZE: 44MB L3 / 64 threads / 4 columns × 8B avg = ~22K rows. Use 32768 (power of 2).
    const size_t MORSEL_SIZE = 32768;
    const size_t qualifying_rows = (range_end > range_start) ? (range_end - range_start) : 0;
    const size_t num_morsels = (qualifying_rows + MORSEL_SIZE - 1) / MORSEL_SIZE;

    // Raw pointers to base of qualifying range
    const int32_t* __restrict__ sd   = col_shipdate.data + range_start;
    const int64_t* __restrict__ disc = col_discount.data + range_start;
    const int64_t* __restrict__ qty  = col_quantity.data + range_start;
    const int64_t* __restrict__ ep   = col_extprice.data + range_start;

    int num_threads = omp_get_max_threads();
    struct alignas(64) PaddedSum { __int128 val; char pad[48]; };
    std::vector<PaddedSum> thread_sums(num_threads);
    for (auto& s : thread_sums) s.val = (__int128)0;

    // Precompute SIMD constants (int32 versions — 8-wide)
    const __m256i v_disc_lo  = _mm256_set1_epi32(disc_lo32);
    const __m256i v_disc_hi  = _mm256_set1_epi32(disc_hi32);
    const __m256i v_qty_max  = _mm256_set1_epi32(qty_max32);
    const __m256i v_date_lo  = _mm256_set1_epi32(date_lo);
    const __m256i v_date_hi  = _mm256_set1_epi32(date_hi);
    const __m256i v_one32    = _mm256_set1_epi32(1);
    const __m256i v_zero     = _mm256_setzero_si256();

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

            // int64 accumulators for vectorized products (lo 4 lanes, hi 4 lanes)
            // Drained every DRAIN_INTERVAL iterations (each processes 8 rows)
            // Safety: DRAIN_INTERVAL * 8 * max_product = 4096*8*73,465,000 ≈ 2.4e12 < INT64_MAX ✓
            __m256i vacc_lo = v_zero; // accumulates products for lanes 0-3 (int64)
            __m256i vacc_hi = v_zero; // accumulates products for lanes 4-7 (int64)
            const size_t DRAIN_INTERVAL = 4096;
            size_t drain_count = 0;

            size_t i = 0;
            const size_t STEP = 8; // process 8 rows per SIMD iteration

            for (; i + STEP <= len; i += STEP) {
                // --- Load shipdate: 8 × int32 directly ---
                __m256i sd_vec = _mm256_loadu_si256((__m256i*)(sd_m + i));

                // --- Load discount, quantity, extprice: 8 × int64 each, narrow to int32 ---
                // Each pair of 256-bit loads covers 4 int64 (lo half) + 4 int64 (hi half)
                __m256i disc_lo4 = _mm256_loadu_si256((__m256i*)(disc_m + i));
                __m256i disc_hi4 = _mm256_loadu_si256((__m256i*)(disc_m + i + 4));
                __m256i disc_vec = narrow_int64x8_to_int32x8(disc_lo4, disc_hi4);

                __m256i qty_lo4  = _mm256_loadu_si256((__m256i*)(qty_m  + i));
                __m256i qty_hi4  = _mm256_loadu_si256((__m256i*)(qty_m  + i + 4));
                __m256i qty_vec  = narrow_int64x8_to_int32x8(qty_lo4,  qty_hi4);

                __m256i ep_lo4   = _mm256_loadu_si256((__m256i*)(ep_m   + i));
                __m256i ep_hi4   = _mm256_loadu_si256((__m256i*)(ep_m   + i + 4));
                __m256i ep_vec   = narrow_int64x8_to_int32x8(ep_lo4,   ep_hi4);

                // --- Apply all 5 predicates (8-wide int32 comparisons) ---
                // disc >= disc_lo: cmpgt(disc, disc_lo-1)
                __m256i disc_ge  = _mm256_cmpgt_epi32(disc_vec, _mm256_sub_epi32(v_disc_lo, v_one32));
                // disc <= disc_hi: cmpgt(disc_hi+1, disc)
                __m256i disc_le  = _mm256_cmpgt_epi32(_mm256_add_epi32(v_disc_hi, v_one32), disc_vec);
                // qty < qty_max: cmpgt(qty_max, qty)
                __m256i qty_lt   = _mm256_cmpgt_epi32(v_qty_max, qty_vec);
                // shipdate >= date_lo: cmpgt(sd, date_lo-1)
                __m256i sd_ge    = _mm256_cmpgt_epi32(sd_vec, _mm256_sub_epi32(v_date_lo, v_one32));
                // shipdate < date_hi: cmpgt(date_hi, sd)
                __m256i sd_lt    = _mm256_cmpgt_epi32(v_date_hi, sd_vec);

                // Combine all predicates
                __m256i mask     = _mm256_and_si256(disc_ge,
                                   _mm256_and_si256(disc_le,
                                   _mm256_and_si256(qty_lt,
                                   _mm256_and_si256(sd_ge, sd_lt))));

                // --- Compute ep * disc (int32 multiply, branchless) ---
                __m256i prod32   = _mm256_mullo_epi32(ep_vec, disc_vec);

                // --- Mask product: zero out non-qualifying lanes ---
                __m256i masked   = _mm256_and_si256(prod32, mask);

                // --- Zero-extend masked int32 products to int64 for accumulation ---
                // Extract lo 128-bit lane and hi 128-bit lane, then cvtepi32_epi64
                __m128i masked_lo128 = _mm256_castsi256_si128(masked);
                __m128i masked_hi128 = _mm256_extracti128_si256(masked, 1);

                // _mm256_cvtepi32_epi64: sign-extends 4 int32 → 4 int64
                // Since mask is 0 or -1 (0xFFFFFFFF), and products are non-negative,
                // masked product lanes are either 0 or positive int32 → sign-extend is correct.
                // But mask is 0xFFFFFFFF for passing lanes — and after AND, masked = prod32 & mask.
                // prod32 values are positive (ep,disc both positive). mask lanes are 0 or 0xFFFFFFFF.
                // So masked product = prod32 (positive) when pass, 0 when fail. Both fit in int32. ✓
                __m256i wide_lo  = _mm256_cvtepi32_epi64(masked_lo128);
                __m256i wide_hi  = _mm256_cvtepi32_epi64(masked_hi128);

                vacc_lo = _mm256_add_epi64(vacc_lo, wide_lo);
                vacc_hi = _mm256_add_epi64(vacc_hi, wide_hi);

                // --- Drain int64 accumulators periodically ---
                drain_count++;
                if (__builtin_expect(drain_count == DRAIN_INTERVAL, 0)) {
                    drain_count = 0;
                    int64_t tmp[4];
                    _mm256_storeu_si256((__m256i*)tmp, vacc_lo);
                    local_sum += (__int128)tmp[0] + tmp[1] + tmp[2] + tmp[3];
                    _mm256_storeu_si256((__m256i*)tmp, vacc_hi);
                    local_sum += (__int128)tmp[0] + tmp[1] + tmp[2] + tmp[3];
                    vacc_lo = v_zero;
                    vacc_hi = v_zero;
                }
            }

            // --- Final drain of SIMD accumulators ---
            {
                int64_t tmp[4];
                _mm256_storeu_si256((__m256i*)tmp, vacc_lo);
                local_sum += (__int128)tmp[0] + tmp[1] + tmp[2] + tmp[3];
                _mm256_storeu_si256((__m256i*)tmp, vacc_hi);
                local_sum += (__int128)tmp[0] + tmp[1] + tmp[2] + tmp[3];
            }

            // === Scalar tail for remaining rows ===
            for (; i < len; i++) {
                int32_t d32 = (int32_t)disc_m[i];
                if (d32 < disc_lo32 || d32 > disc_hi32) continue;
                int32_t q32 = (int32_t)qty_m[i];
                if (q32 >= qty_max32) continue;
                int32_t shipdate = sd_m[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                local_sum += (__int128)(int32_t)ep_m[i] * d32;
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
