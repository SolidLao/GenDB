/*
 * Q6: Forecasting Revenue Change — Iter 8 Optimizations
 *
 * STRATEGY:
 * 1. Zone map index for l_shipdate: qualifying_ranges() prunes ~75% of blocks.
 * 2. SPLIT zones into two categories:
 *    a) FULLY-CONTAINED zones (zone.min >= DATE_LO AND zone.max < DATE_HI):
 *       ALL rows pass the date predicate. Skip loading l_shipdate entirely.
 *       Scan only 3 columns (discount, quantity, extprice) — saves 25% bandwidth.
 *       Use AVX-512 fused discount+quantity filter + accumulate.
 *    b) BOUNDARY zones (zone overlaps but not fully contained):
 *       Load all 4 columns, apply per-row date check (same as iter 5).
 *       Only ~2 boundary zones exist (one at each end of the date range).
 * 3. Lane-accumulator pattern: maintain 8 int64 SIMD lanes, reduce once per zone.
 * 4. All SIMD constants hoisted outside loops.
 * 5. OpenMP parallel scan with static scheduling across 64 threads.
 * 6. Software prefetch for HDD latency hiding.
 *
 * KEY IMPROVEMENT OVER ITER 5:
 * - Late materialization: skip l_shipdate load for fully-contained zones.
 *   Lineitem has ~600 zones; ~25% qualify (150 zones); of these ~148 are
 *   fully-contained, only ~2 are boundary zones. So ~99% of scan work avoids
 *   loading l_shipdate, reducing column reads from 4 to 3 (25% less I/O).
 * - The 3-column inner loop is tighter and allows better CPU pipelining.
 *
 * CORRECTNESS ANCHORS (DO NOT MODIFY):
 *   date_literal: 1994-01-01 → epoch_days = 8766
 *   date_literal: 1995-01-01 → epoch_days = 9131
 *   discount: BETWEEN 0.05 AND 0.07, scale=100 → int64 [5, 7]
 *   quantity: < 24, scale=1 → int64 < 24
 *   revenue = SUM(l_extendedprice * l_discount) / 10000.0
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

#include <immintrin.h>   // AVX-512 intrinsics
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// AVX-512 kernel for FULLY-CONTAINED zones (no date check needed).
// Only 3 columns accessed: discount, quantity, extprice.
// This saves ~20-25% memory bandwidth vs the 4-column version.
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_zone_nodate(
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ extprice,
    uint32_t row_start,
    uint32_t row_end,
    __m512i v_disc_lo,
    __m512i v_disc_hi,
    __m512i v_qty_max)
{
    __m512i lane_acc = _mm512_setzero_si512();

    uint32_t i = row_start;
    const uint32_t n = row_end;
    static const int PF_DIST = 128; // rows ahead to prefetch

    // 2x unrolled: 16 rows per iteration
    const uint32_t n16 = row_start + ((n - row_start) & ~15u);
    for (; i < n16; i += 16) {
        __builtin_prefetch(discount + i + PF_DIST, 0, 1);
        __builtin_prefetch(quantity + i + PF_DIST, 0, 1);
        __builtin_prefetch(extprice + i + PF_DIST, 0, 1);

        // Batch A: rows i..i+7
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i));
            __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
            __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
            __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            __mmask8 final_m   = m_disc_lo & m_disc_hi & m_qty;
            if (__builtin_expect(final_m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
            }
        }

        // Batch B: rows i+8..i+15
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 8));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i + 8));
            __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
            __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
            __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            __mmask8 final_m   = m_disc_lo & m_disc_hi & m_qty;
            if (__builtin_expect(final_m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 8));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
            }
        }
    }

    // Handle remaining 8-row batch
    const uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i));
        __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
        __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
        __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
        __mmask8 final_m   = m_disc_lo & m_disc_hi & m_qty;
        if (__builtin_expect(final_m == 0, 1)) continue;

        __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
        __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
        lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
    }

    // Reduce lane accumulator to scalar, then scalar tail
    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Extract scalar constants for tail
    alignas(64) int64_t disc_lo_arr[8], disc_hi_arr[8], qty_arr[8];
    _mm512_store_si512((__m512i*)disc_lo_arr, v_disc_lo);
    _mm512_store_si512((__m512i*)disc_hi_arr, v_disc_hi);
    _mm512_store_si512((__m512i*)qty_arr, v_qty_max);
    const int64_t disc_lo_s = disc_lo_arr[0];
    const int64_t disc_hi_s = disc_hi_arr[0];
    const int64_t qty_max_s = qty_arr[0];

    for (; i < n; i++) {
        int64_t disc = discount[i];
        if (disc < disc_lo_s || disc > disc_hi_s) continue;
        int64_t qty = quantity[i];
        if (qty >= qty_max_s) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// AVX-512 kernel for BOUNDARY zones (need per-row date check).
// All 4 columns loaded. Identical to iter 5's avx512_scan_zone.
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_zone_withdate(
    const int32_t* __restrict__ shipdate,
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ extprice,
    uint32_t row_start,
    uint32_t row_end,
    __m256i v_dlo_m1,
    __m256i v_dhi,
    __m512i v_disc_lo,
    __m512i v_disc_hi,
    __m512i v_qty_max)
{
    __m512i lane_acc = _mm512_setzero_si512();

    uint32_t i = row_start;
    const uint32_t n = row_end;
    static const int PF_DIST = 128;

    // 2x unrolled: 16 rows per iteration
    const uint32_t n16 = row_start + ((n - row_start) & ~15u);
    for (; i < n16; i += 16) {
        __builtin_prefetch(shipdate + i + PF_DIST, 0, 1);
        __builtin_prefetch(discount + i + PF_DIST, 0, 1);
        __builtin_prefetch(quantity + i + PF_DIST, 0, 1);
        __builtin_prefetch(extprice + i + PF_DIST, 0, 1);

        // Batch A: rows i..i+7
        {
            __m256i vdate   = _mm256_loadu_si256((__m256i*)(shipdate + i));
            __m256i cmp_lo  = _mm256_cmpgt_epi32(vdate, v_dlo_m1);
            __m256i cmp_hi  = _mm256_cmpgt_epi32(v_dhi, vdate);
            __m256i dmask   = _mm256_and_si256(cmp_lo, cmp_hi);
            uint8_t dpmask  = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(dmask));

            if (__builtin_expect(dpmask != 0, 0)) {
                __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
                __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i));
                __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
                __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
                __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
                __mmask8 final_m   = dpmask & m_disc_lo & m_disc_hi & m_qty;
                if (__builtin_expect(final_m != 0, 0)) {
                    __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
                    __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                    lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
                }
            }
        }

        // Batch B: rows i+8..i+15
        {
            __m256i vdate   = _mm256_loadu_si256((__m256i*)(shipdate + i + 8));
            __m256i cmp_lo  = _mm256_cmpgt_epi32(vdate, v_dlo_m1);
            __m256i cmp_hi  = _mm256_cmpgt_epi32(v_dhi, vdate);
            __m256i dmask   = _mm256_and_si256(cmp_lo, cmp_hi);
            uint8_t dpmask  = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(dmask));

            if (__builtin_expect(dpmask != 0, 0)) {
                __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 8));
                __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i + 8));
                __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
                __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
                __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
                __mmask8 final_m   = dpmask & m_disc_lo & m_disc_hi & m_qty;
                if (__builtin_expect(final_m != 0, 0)) {
                    __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 8));
                    __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                    lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
                }
            }
        }
    }

    // Handle remaining 8-row batch
    const uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        __m256i vdate   = _mm256_loadu_si256((__m256i*)(shipdate + i));
        __m256i cmp_lo  = _mm256_cmpgt_epi32(vdate, v_dlo_m1);
        __m256i cmp_hi  = _mm256_cmpgt_epi32(v_dhi, vdate);
        __m256i dmask   = _mm256_and_si256(cmp_lo, cmp_hi);
        uint8_t dpmask  = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(dmask));
        if (__builtin_expect(dpmask == 0, 1)) continue;

        __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i));
        __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
        __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
        __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
        __mmask8 final_m   = dpmask & m_disc_lo & m_disc_hi & m_qty;
        if (__builtin_expect(final_m == 0, 1)) continue;

        __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
        __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
        lane_acc = _mm512_mask_add_epi64(lane_acc, final_m, lane_acc, vprod);
    }

    // Scalar constants for tail
    int32_t date_lo_scalar = _mm256_extract_epi32(v_dlo_m1, 0) + 1;
    int32_t date_hi_scalar = _mm256_extract_epi32(v_dhi, 0);
    alignas(64) int64_t disc_lo_arr[8], disc_hi_arr[8], qty_arr[8];
    _mm512_store_si512((__m512i*)disc_lo_arr, v_disc_lo);
    _mm512_store_si512((__m512i*)disc_hi_arr, v_disc_hi);
    _mm512_store_si512((__m512i*)qty_arr, v_qty_max);
    const int64_t disc_lo_s = disc_lo_arr[0];
    const int64_t disc_hi_s = disc_hi_arr[0];
    const int64_t qty_max_s = qty_arr[0];

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);
    for (; i < n; i++) {
        int32_t sd = shipdate[i];
        if (sd < date_lo_scalar || sd >= date_hi_scalar) continue;
        int64_t disc = discount[i];
        if (disc < disc_lo_s || disc > disc_hi_s) continue;
        int64_t qty = quantity[i];
        if (qty >= qty_max_s) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // CORRECTNESS ANCHORS — DO NOT MODIFY
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");  // 8766
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");  // 9131
    const int64_t DISC_LO = 5;     // 0.05 * 100
    const int64_t DISC_HI = 7;     // 0.07 * 100
    const int64_t QTY_MAX = 24;    // < 24

    // Pre-compute SIMD constants ONCE (hoisted from inner loop)
    const __m256i v_dlo_m1  = _mm256_set1_epi32(DATE_LO - 1);
    const __m256i v_dhi     = _mm256_set1_epi32(DATE_HI);
    const __m512i v_disc_lo = _mm512_set1_epi64(DISC_LO);
    const __m512i v_disc_hi = _mm512_set1_epi64(DISC_HI);
    const __m512i v_qty_max = _mm512_set1_epi64(QTY_MAX);

    // Zero-copy mmap access to all 4 columns
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns concurrently — overlap I/O with setup
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    // Load zone map index for l_shipdate
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Get qualifying row ranges AND classify as boundary vs fully-contained.
    // Since l_shipdate is SORTED within each zone:
    //   - Fully-contained: zone.min >= DATE_LO AND zone.max < DATE_HI
    //     → ALL rows pass date predicate → skip l_shipdate per-row check
    //   - Boundary: zone overlaps the range but is not fully contained
    //     → Need per-row date check (only a few zones at each end)
    struct ZoneInfo {
        uint32_t row_start;
        uint32_t row_end;
        bool     fully_contained;
    };
    std::vector<ZoneInfo> zones;
    {
        GENDB_PHASE("zone_map_prune");
        for (const auto& z : zone_map.zones) {
            if (z.max < DATE_LO || z.min >= DATE_HI) continue; // fully outside
            bool fc = (z.min >= DATE_LO && z.max < DATE_HI);
            zones.push_back({z.row_offset, z.row_offset + z.row_count, fc});
        }
    }

    const int NUM_THREADS = 64;
    const int num_zones = (int)zones.size();

    // Per-thread accumulators (cache-line padded to prevent false sharing)
    struct alignas(64) PaddedAcc {
        int64_t val;
        char _pad[56];
    };
    PaddedAcc thread_sums[NUM_THREADS] = {};

    {
        GENDB_PHASE("main_scan");

        const int32_t* __restrict__ shipdate = col_shipdate.data;
        const int64_t* __restrict__ discount  = col_discount.data;
        const int64_t* __restrict__ quantity  = col_quantity.data;
        const int64_t* __restrict__ extprice  = col_extprice.data;

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            int64_t local_sum = 0;

            #pragma omp for schedule(static)
            for (int zi = 0; zi < num_zones; zi++) {
                const uint32_t row_start = zones[zi].row_start;
                const uint32_t row_end   = zones[zi].row_end;

                if (zones[zi].fully_contained) {
                    // No date check needed — all rows in range.
                    // Scan only 3 columns: discount, quantity, extprice.
                    // ~25% less memory bandwidth vs 4-column scan.
                    local_sum += avx512_scan_zone_nodate(
                        discount, quantity, extprice,
                        row_start, row_end,
                        v_disc_lo, v_disc_hi, v_qty_max);
                } else {
                    // Boundary zone: need per-row date filter.
                    local_sum += avx512_scan_zone_withdate(
                        shipdate, discount, quantity, extprice,
                        row_start, row_end,
                        v_dlo_m1, v_dhi,
                        v_disc_lo, v_disc_hi, v_qty_max);
                }
            }

            thread_sums[tid].val = local_sum;
        }
    }

    // Merge thread-local accumulators
    int64_t total_sum = 0;
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < NUM_THREADS; t++) {
            total_sum += thread_sums[t].val;
        }
    }

    // Output result
    {
        GENDB_PHASE("output");
        // Revenue = SUM(extprice * discount) / (100 * 100)  [both scale=100]
        double revenue = static_cast<double>(total_sum) / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        std::FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "revenue\n%.2f\n", revenue);
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
