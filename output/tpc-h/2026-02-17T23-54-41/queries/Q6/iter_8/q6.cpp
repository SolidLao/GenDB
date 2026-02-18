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
 *   Single table: lineitem (59.9M rows, sorted by l_shipdate)
 *   Predicates:
 *     1. l_shipdate >= 8766  (1994-01-01 epoch days)
 *     2. l_shipdate <  9131  (1995-01-01 epoch days)
 *     3. l_discount >= 5     (0.05 * 100, scaled int64, only 3 values: 5,6,7 pass)
 *     4. l_discount <= 7     (0.07 * 100)
 *     5. l_quantity < 2400   (24 * 100, ~48% pass rate)
 *   Aggregation: SUM(l_extendedprice * l_discount)
 *
 * === PHYSICAL PLAN (iter 8) ===
 *   1. Zone map pruning on l_shipdate → contiguous [range_start, range_end) row range
 *      (table sorted by shipdate → single contiguous zone range)
 *
 *   2. Parallel morsel scan with LATE MATERIALIZATION of l_extendedprice:
 *      - Phase A: Scan disc (int64) + qty (int64) + shipdate (int32) with AVX2
 *        * Process 8 rows per SIMD iteration (shipdate int32 = 8 lanes)
 *        * disc & qty: two groups of 4-lane int64 SIMD
 *        * Early exit if no row passes disc+qty (dominant filter)
 *        * Combine with shipdate mask
 *        * Write qualifying row indices to a thread-local selection vector (int32[])
 *      - Phase B: For each qualifying index in the selection vector:
 *        * Load ep[idx] and disc[idx] (late materialization of ep)
 *        * Accumulate into __int128 local sum
 *      - Morsel size: 65536 rows (fits disc+qty+sd in ~3MB → good L3 reuse)
 *      - schedule(static) for minimal OpenMP overhead
 *
 *   3. Global merge of thread-local __int128 sums
 *   4. Output: revenue = SUM / 10000 with 2 decimal places
 *
 * === KEY INSIGHT vs iter 4 ===
 *   Late materialization of l_extendedprice: combined selectivity ~1.8%
 *   → 98.2% of ep memory accesses avoided
 *   → 3 columns scanned per morsel instead of 4 in phase A
 *   → ep loaded only for ~1.8% of rows in phase B (fully cache-friendly via sel vector)
 *   → Selection vector is small (int32 indices, ~1.8% × morsel_size entries)
 *   → Phase B ep+disc loads are irregular but tiny in total count
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

    // === Parallel morsel scan with late materialization ===
    // Morsel: 65536 rows. 3 active columns: disc(8B) + qty(8B) + sd(4B) = 20B/row
    // → 65536*20 = 1.3MB fits well in L3 per thread.
    // Selection vector: max 65536 int32_t entries = 256KB (but actual fill ~1.8% = ~4KB)
    const size_t MORSEL_SIZE = 65536;
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
    const __m256i v_disc_lo_m1 = _mm256_set1_epi64x(disc_lo - 1);
    const __m256i v_disc_hi_p1 = _mm256_set1_epi64x(disc_hi + 1);
    const __m256i v_qty_max    = _mm256_set1_epi64x(qty_max);

    // int32 constants for shipdate (8-lane AVX2)
    const __m256i v_date_lo_m1 = _mm256_set1_epi32(date_lo - 1);
    const __m256i v_date_hi    = _mm256_set1_epi32(date_hi);

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

            // === Phase A: SIMD filter → selection vector ===
            // Use a thread-local selection vector (stack-allocated, small)
            // Max entries: len (but typically ~1.8% fill → very few entries)
            // Using int32_t offsets relative to morsel start
            int32_t sel_vec[MORSEL_SIZE];
            int32_t sel_count = 0;

            size_t i = 0;

            // SIMD loop: 8 rows per iteration
            for (; i + 8 <= len; i += 8) {
                // --- Discount + Quantity: two groups of 4 int64 lanes ---
                __m256i d_lo = _mm256_loadu_si256((const __m256i*)(disc_m + i));
                __m256i d_hi = _mm256_loadu_si256((const __m256i*)(disc_m + i + 4));
                __m256i q_lo = _mm256_loadu_si256((const __m256i*)(qty_m  + i));
                __m256i q_hi = _mm256_loadu_si256((const __m256i*)(qty_m  + i + 4));

                // disc >= disc_lo AND disc <= disc_hi AND qty < qty_max
                __m256i dge_lo = _mm256_cmpgt_epi64(d_lo, v_disc_lo_m1);
                __m256i dge_hi = _mm256_cmpgt_epi64(d_hi, v_disc_lo_m1);
                __m256i dle_lo = _mm256_cmpgt_epi64(v_disc_hi_p1, d_lo);
                __m256i dle_hi = _mm256_cmpgt_epi64(v_disc_hi_p1, d_hi);
                __m256i qlt_lo = _mm256_cmpgt_epi64(v_qty_max, q_lo);
                __m256i qlt_hi = _mm256_cmpgt_epi64(v_qty_max, q_hi);

                __m256i mask64_lo = _mm256_and_si256(_mm256_and_si256(dge_lo, dle_lo), qlt_lo);
                __m256i mask64_hi = _mm256_and_si256(_mm256_and_si256(dge_hi, dle_hi), qlt_hi);

                int mv_lo = _mm256_movemask_epi8(mask64_lo);
                int mv_hi = _mm256_movemask_epi8(mask64_hi);

                // Early exit: if no rows pass disc+qty filters, skip shipdate check
                if ((mv_lo | mv_hi) == 0) continue;

                // --- Shipdate: 8 int32 lanes ---
                __m256i s32 = _mm256_loadu_si256((const __m256i*)(sd_m + i));
                __m256i sdge = _mm256_cmpgt_epi32(s32, v_date_lo_m1);
                __m256i sdlt = _mm256_cmpgt_epi32(v_date_hi, s32);
                __m256i sd_mask32 = _mm256_and_si256(sdge, sdlt);

                // Expand int32 mask to int64 mask (sign-extend each int32 lane to int64)
                __m128i sd_lo128 = _mm256_castsi256_si128(sd_mask32);
                __m128i sd_hi128 = _mm256_extracti128_si256(sd_mask32, 1);
                __m256i sd_mask64_lo = _mm256_cvtepi32_epi64(sd_lo128);
                __m256i sd_mask64_hi = _mm256_cvtepi32_epi64(sd_hi128);

                // Combined masks
                __m256i final_lo = _mm256_and_si256(mask64_lo, sd_mask64_lo);
                __m256i final_hi = _mm256_and_si256(mask64_hi, sd_mask64_hi);

                int fmv_lo = _mm256_movemask_epi8(final_lo);
                int fmv_hi = _mm256_movemask_epi8(final_hi);

                if ((fmv_lo | fmv_hi) == 0) continue;

                // Write qualifying indices to selection vector (branchless per lane)
                // Each int64 lane = 8 bytes → mask bits 0,8,16,24 (bytes 0,8,16,24)
                int32_t base_i = (int32_t)i;
                if (fmv_lo & 0x000000FF) sel_vec[sel_count++] = base_i + 0;
                if (fmv_lo & 0x0000FF00) sel_vec[sel_count++] = base_i + 1;
                if (fmv_lo & 0x00FF0000) sel_vec[sel_count++] = base_i + 2;
                if (fmv_lo & 0xFF000000) sel_vec[sel_count++] = base_i + 3;
                if (fmv_hi & 0x000000FF) sel_vec[sel_count++] = base_i + 4;
                if (fmv_hi & 0x0000FF00) sel_vec[sel_count++] = base_i + 5;
                if (fmv_hi & 0x00FF0000) sel_vec[sel_count++] = base_i + 6;
                if (fmv_hi & 0xFF000000) sel_vec[sel_count++] = base_i + 7;
            }

            // Scalar tail for remainder rows
            for (; i < len; i++) {
                int64_t d = disc_m[i];
                if (d < disc_lo || d > disc_hi) continue;
                int64_t q = qty_m[i];
                if (q >= qty_max) continue;
                int32_t shipdate = sd_m[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                sel_vec[sel_count++] = (int32_t)i;
            }

            // === Phase B: Late materialization — load ep only for qualifying rows ===
            // sel_count is ~1.8% of len → minimal ep memory traffic
            for (int32_t s = 0; s < sel_count; s++) {
                int32_t idx = sel_vec[s];
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
