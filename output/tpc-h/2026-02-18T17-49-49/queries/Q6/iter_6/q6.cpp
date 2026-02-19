/*
 * Q6: Forecasting Revenue Change — Iter 6 Optimizations
 *
 * STRATEGY:
 * 1. Zone map index for l_shipdate: qualifying_ranges() prunes ~75% of blocks.
 * 2. AVX-512 vectorized SIMD scan.
 * 3. Fused single-pass filter+accumulate over 4 columns.
 * 4. OpenMP parallel scan over qualifying zone ranges.
 * 5. Thread-local int64 accumulators — no false sharing.
 *
 * ITER 6 IMPROVEMENTS over iter 5:
 * a) Zone interior fast path: For zones fully contained in [DATE_LO, DATE_HI),
 *    skip the date comparison entirely. Only boundary zones need per-row date
 *    filtering. Since l_shipdate is sorted and zones are 100K rows, most
 *    qualifying zones are interior zones with ALL dates in range — eliminating
 *    the costliest per-row comparison from the hot path.
 * b) Branchless inner loop for interior zones: No dpmask branches since all
 *    dates pass. Process discount+quantity filters directly with AVX-512,
 *    then multiply+accumulate. Removes 2 conditional branches per 8-row batch.
 * c) MAP_POPULATE equivalent: Use madvise(MADV_WILLNEED) + MADV_SEQUENTIAL
 *    with posix_fadvise(POSIX_FADV_WILLNEED) for more aggressive prefetch.
 * d) 4x unrolled inner loop for interior zones (32 rows per iteration) to
 *    maximize instruction-level parallelism and hide memory latency further.
 * e) Finer-grained zone classification: track zone type (boundary/interior)
 *    during qualifying_ranges to avoid checking zone.min/max twice.
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
#include <fcntl.h>       // posix_fadvise
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// INTERIOR ZONE kernel: all rows pass date filter → skip date comparison.
// Only applies discount and quantity filters, then accumulates.
// 4x unrolled (32 rows per iteration) for maximum ILP.
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_interior(
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

    static const int PF_DIST = 128;

    // 4x unrolled main loop: 32 rows per iteration
    uint32_t n32 = row_start + ((n - row_start) & ~31u);
    for (; i < n32; i += 32) {
        __builtin_prefetch(discount + i + PF_DIST, 0, 1);
        __builtin_prefetch(quantity + i + PF_DIST, 0, 1);
        __builtin_prefetch(extprice + i + PF_DIST, 0, 1);

        // Batch A: rows i..i+7
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i));
            __mmask8 m = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            if (__builtin_expect(m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc, vprod);
            }
        }
        // Batch B: rows i+8..i+15
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 8));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i + 8));
            __mmask8 m = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            if (__builtin_expect(m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 8));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc, vprod);
            }
        }
        // Batch C: rows i+16..i+23
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 16));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i + 16));
            __mmask8 m = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            if (__builtin_expect(m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 16));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc, vprod);
            }
        }
        // Batch D: rows i+24..i+31
        {
            __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 24));
            __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i + 24));
            __mmask8 m = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
            if (__builtin_expect(m != 0, 0)) {
                __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 24));
                __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc, vprod);
            }
        }
    }

    // Handle remaining 8-row batches
    uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i));
        __mmask8 m = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                   & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                   & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
        if (__builtin_expect(m != 0, 0)) {
            __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
            __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
            lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc, vprod);
        }
    }

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Scalar tail
    for (; i < n; i++) {
        int64_t disc = discount[i];
        if (disc < 5 || disc > 7) continue;
        if (quantity[i] >= 24) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// BOUNDARY ZONE kernel: per-row date filter required.
// Lane accumulator + 2x unrolled (16 rows per iteration).
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_boundary(
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
    uint32_t n16 = row_start + ((n - row_start) & ~15u);
    for (; i < n16; i += 16) {
        __builtin_prefetch(shipdate + i + PF_DIST, 0, 1);
        __builtin_prefetch(discount + i + PF_DIST, 0, 1);
        __builtin_prefetch(quantity + i + PF_DIST, 0, 1);
        __builtin_prefetch(extprice + i + PF_DIST, 0, 1);

        // Batch A: rows i..i+7
        {
            __m256i vdate = _mm256_loadu_si256((__m256i*)(shipdate + i));
            __m256i date_mask = _mm256_and_si256(
                _mm256_cmpgt_epi32(vdate, v_dlo_m1),
                _mm256_cmpgt_epi32(v_dhi, vdate));
            uint8_t dpmask = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_mask));
            if (__builtin_expect(dpmask != 0, 0)) {
                __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
                __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i));
                __mmask8 final = dpmask
                               & _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                               & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                               & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
                if (__builtin_expect(final != 0, 0)) {
                    __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
                    __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                    lane_acc = _mm512_mask_add_epi64(lane_acc, final, lane_acc, vprod);
                }
            }
        }
        // Batch B: rows i+8..i+15
        {
            __m256i vdate = _mm256_loadu_si256((__m256i*)(shipdate + i + 8));
            __m256i date_mask = _mm256_and_si256(
                _mm256_cmpgt_epi32(vdate, v_dlo_m1),
                _mm256_cmpgt_epi32(v_dhi, vdate));
            uint8_t dpmask = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_mask));
            if (__builtin_expect(dpmask != 0, 0)) {
                __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i + 8));
                __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i + 8));
                __mmask8 final = dpmask
                               & _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                               & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                               & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
                if (__builtin_expect(final != 0, 0)) {
                    __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i + 8));
                    __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
                    lane_acc = _mm512_mask_add_epi64(lane_acc, final, lane_acc, vprod);
                }
            }
        }
    }

    // 8-row tail
    uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        __m256i vdate = _mm256_loadu_si256((__m256i*)(shipdate + i));
        __m256i date_mask = _mm256_and_si256(
            _mm256_cmpgt_epi32(vdate, v_dlo_m1),
            _mm256_cmpgt_epi32(v_dhi, vdate));
        uint8_t dpmask = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_mask));
        if (__builtin_expect(dpmask == 0, 1)) continue;

        __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity  + i));
        __mmask8 final = dpmask
                       & _mm512_cmpge_epi64_mask(vdisc, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vdisc, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vqty,  v_qty_max);
        if (__builtin_expect(final == 0, 1)) continue;

        __m512i vext  = _mm512_loadu_si512((__m512i*)(extprice + i));
        __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
        lane_acc = _mm512_mask_add_epi64(lane_acc, final, lane_acc, vprod);
    }

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Scalar tail
    for (; i < n; i++) {
        int32_t sd = shipdate[i];
        if (sd < 8766 || sd >= 9131) continue;
        int64_t disc = discount[i];
        if (disc < 5 || disc > 7) continue;
        if (quantity[i] >= 24) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// Zone descriptor: row range + whether date filter is needed
// ---------------------------------------------------------------------------
struct ZoneDesc {
    uint32_t row_start;
    uint32_t row_end;
    bool     is_interior;   // true → all dates in range, skip date filter
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // CORRECTNESS ANCHORS — DO NOT MODIFY
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");
    const int64_t DISC_LO = 5;      // 0.05 * 100
    const int64_t DISC_HI = 7;      // 0.07 * 100
    const int64_t QTY_MAX = 24;     // < 24

    // Pre-compute SIMD constants ONCE
    const __m256i v_dlo_m1  = _mm256_set1_epi32(DATE_LO - 1);
    const __m256i v_dhi     = _mm256_set1_epi32(DATE_HI);
    const __m512i v_disc_lo = _mm512_set1_epi64(DISC_LO);
    const __m512i v_disc_hi = _mm512_set1_epi64(DISC_HI);
    const __m512i v_qty_max = _mm512_set1_epi64(QTY_MAX);

    // Zero-copy mmap access
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns into page cache (async, overlaps with zone map processing)
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    // Load zone map index
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Build zone descriptors with interior classification
    // Interior zone: zone.min >= DATE_LO && zone.max < DATE_HI
    // → all rows pass date filter, skip per-row date comparison
    std::vector<ZoneDesc> zones;
    {
        GENDB_PHASE("zone_map_prune");
        zones.reserve(zone_map.zones.size());
        for (const auto& z : zone_map.zones) {
            if (z.max < DATE_LO || z.min >= DATE_HI) continue;
            ZoneDesc d;
            d.row_start   = z.row_offset;
            d.row_end     = z.row_offset + z.row_count;
            d.is_interior = (z.min >= DATE_LO && z.max < DATE_HI);
            zones.push_back(d);
        }
    }

    const int NUM_THREADS = 64;
    const int num_zones   = (int)zones.size();

    // Per-thread accumulators (cache-line padded)
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
                const ZoneDesc& z = zones[zi];

                if (z.is_interior) {
                    // Hot path: skip date comparison entirely
                    local_sum += avx512_scan_interior(
                        discount, quantity, extprice,
                        z.row_start, z.row_end,
                        v_disc_lo, v_disc_hi, v_qty_max);
                } else {
                    // Boundary zone: date filter required
                    local_sum += avx512_scan_boundary(
                        shipdate, discount, quantity, extprice,
                        z.row_start, z.row_end,
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

    // Output
    {
        GENDB_PHASE("output");
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
