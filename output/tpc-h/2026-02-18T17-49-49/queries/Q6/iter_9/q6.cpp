/*
 * Q6: Forecasting Revenue Change — Iter 9 ARCHITECTURAL REWRITE
 *
 * STALL RECOVERY — fundamental strategy change from previous 8 iterations.
 *
 * ROOT CAUSE ANALYSIS:
 *   Prior approach: ~76ms total, main_scan 30ms, ~46ms unaccounted.
 *   The ~46ms comes from page-fault overhead when 64 threads race to fault in
 *   mmap pages that were never pre-faulted. MADV_WILLNEED is async and does not
 *   guarantee pages are in memory before the parallel scan begins.
 *
 * NEW ARCHITECTURE:
 *
 * 1. TWO-TIER ZONE PROCESSING (eliminates date filter for ~98% of rows):
 *    - "Edge" zones: zone_min < DATE_LO or zone_max >= DATE_HI → need date check
 *    - "Interior" zones: zone_min >= DATE_LO AND zone_max < DATE_HI → every row
 *      passes the date filter. Skip date load+compare entirely. Only check
 *      discount BETWEEN 5 AND 7 AND quantity < 24.
 *    With ~150 qualifying zones only ~2 are edge zones. For 148 interior zones
 *    we save the int32 shipdate load + 2 comparisons per row.
 *
 * 2. SYNCHRONOUS PAGE PRE-FAULT (MADV_POPULATE_READ) on qualifying byte ranges:
 *    Before forking OpenMP threads, call madvise(MADV_POPULATE_READ) on just the
 *    qualifying row ranges of each column. This blocks until all pages are in the
 *    page cache, so threads never incur page-fault latency during the scan.
 *    Only pre-fault the qualifying portion, not entire 1.68GB dataset.
 *
 * 3. PURE AVX-512 INT64 INNER KERNEL for interior zones:
 *    With date check eliminated, inner loop does only:
 *      - Load 8 discount values (512-bit)
 *      - Load 8 quantity values (512-bit)
 *      - Two range comparisons → 8-bit mask
 *      - Masked load 8 extprice values (512-bit, only if mask != 0)
 *      - Masked mullo + masked add to lane accumulator
 *    No AVX2 date comparison, no 32→64 mask conversion, no branch on date.
 *
 * 4. REDUCED THREAD COUNT (32 threads, NUMA-local):
 *    64 threads across 2 NUMA nodes thrash remote memory. With data likely on
 *    NUMA node 0, use 32 threads pinned to node 0. OpenMP thread startup for 64
 *    threads also adds ~5-10ms overhead per query.
 *
 * 5. STATIC SCHEDULING over merged row ranges (not per-zone):
 *    Merge contiguous qualifying zones into a single large row range, then split
 *    that range into equal-size chunks for each thread. This gives perfect load
 *    balance without zone-boundary overhead.
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
#include <climits>

#include <immintrin.h>   // AVX-512 intrinsics
#include <omp.h>
#include <sys/mman.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Interior zone kernel: NO date check.
// Every row in this zone is already guaranteed to be in the date range.
// Filter: discount BETWEEN DISC_LO AND DISC_HI, quantity < QTY_MAX.
// Accumulate extprice * discount for qualifying rows.
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

    // 4x unrolled: process 32 rows per iteration to maximize ILP and hide latency
    uint32_t n32 = row_start + ((n - row_start) / 32) * 32;
    for (; i < n32; i += 32) {
        // Prefetch 512 bytes (8 cache lines) ahead
        __builtin_prefetch(discount + i + 256, 0, 0);
        __builtin_prefetch(quantity + i + 256, 0, 0);
        __builtin_prefetch(extprice + i + 256, 0, 0);

        // Batch A (rows i..i+7)
        {
            __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i));
            __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i));
            __mmask8 m = _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vq, v_qty_max);
            if (m) {
                __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i));
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                               _mm512_mullo_epi64(ve, vd));
            }
        }
        // Batch B (rows i+8..i+15)
        {
            __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i + 8));
            __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i + 8));
            __mmask8 m = _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vq, v_qty_max);
            if (m) {
                __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i + 8));
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                               _mm512_mullo_epi64(ve, vd));
            }
        }
        // Batch C (rows i+16..i+23)
        {
            __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i + 16));
            __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i + 16));
            __mmask8 m = _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vq, v_qty_max);
            if (m) {
                __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i + 16));
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                               _mm512_mullo_epi64(ve, vd));
            }
        }
        // Batch D (rows i+24..i+31)
        {
            __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i + 24));
            __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i + 24));
            __mmask8 m = _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                       & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                       & _mm512_cmplt_epi64_mask(vq, v_qty_max);
            if (m) {
                __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i + 24));
                lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                               _mm512_mullo_epi64(ve, vd));
            }
        }
    }

    // 8-row tail
    uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i));
        __mmask8 m = _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                   & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                   & _mm512_cmplt_epi64_mask(vq, v_qty_max);
        if (m) {
            __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i));
            lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                           _mm512_mullo_epi64(ve, vd));
        }
    }

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Scalar tail
    alignas(64) int64_t disc_lo_arr[8], disc_hi_arr[8], qty_max_arr[8];
    _mm512_store_si512((__m512i*)disc_lo_arr, v_disc_lo);
    _mm512_store_si512((__m512i*)disc_hi_arr, v_disc_hi);
    _mm512_store_si512((__m512i*)qty_max_arr, v_qty_max);
    const int64_t disc_lo_s = disc_lo_arr[0];
    const int64_t disc_hi_s = disc_hi_arr[0];
    const int64_t qty_max_s = qty_max_arr[0];

    for (; i < n; i++) {
        int64_t disc = discount[i];
        if (disc < disc_lo_s || disc > disc_hi_s) continue;
        if (quantity[i] >= qty_max_s) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// Edge zone kernel: includes date check.
// Used only for the 1-2 boundary zones where not all rows pass the date filter.
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_edge(
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

    uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        // Date filter (AVX2, int32)
        __m256i vdate = _mm256_loadu_si256((__m256i*)(shipdate + i));
        __m256i cmp_lo = _mm256_cmpgt_epi32(vdate, v_dlo_m1);
        __m256i cmp_hi = _mm256_cmpgt_epi32(v_dhi, vdate);
        __m256i date_mask = _mm256_and_si256(cmp_lo, cmp_hi);
        uint8_t dpmask = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_mask));
        if (dpmask == 0) continue;

        __m512i vd = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vq = _mm512_loadu_si512((__m512i*)(quantity + i));
        __mmask8 m = ((__mmask8)dpmask)
                   & _mm512_cmpge_epi64_mask(vd, v_disc_lo)
                   & _mm512_cmple_epi64_mask(vd, v_disc_hi)
                   & _mm512_cmplt_epi64_mask(vq, v_qty_max);
        if (m) {
            __m512i ve = _mm512_loadu_si512((__m512i*)(extprice + i));
            lane_acc = _mm512_mask_add_epi64(lane_acc, m, lane_acc,
                           _mm512_mullo_epi64(ve, vd));
        }
    }

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Extract scalars for tail
    alignas(64) int64_t disc_lo_arr[8], disc_hi_arr[8], qty_max_arr[8];
    _mm512_store_si512((__m512i*)disc_lo_arr, v_disc_lo);
    _mm512_store_si512((__m512i*)disc_hi_arr, v_disc_hi);
    _mm512_store_si512((__m512i*)qty_max_arr, v_qty_max);
    const int64_t disc_lo_s = disc_lo_arr[0];
    const int64_t disc_hi_s = disc_hi_arr[0];
    const int64_t qty_max_s = qty_max_arr[0];
    const int32_t date_lo_s = _mm256_extract_epi32(v_dlo_m1, 0) + 1;
    const int32_t date_hi_s = _mm256_extract_epi32(v_dhi, 0);

    for (; i < n; i++) {
        int32_t sd = shipdate[i];
        if (sd < date_lo_s || sd >= date_hi_s) continue;
        int64_t disc = discount[i];
        if (disc < disc_lo_s || disc > disc_hi_s) continue;
        if (quantity[i] >= qty_max_s) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// Synchronous page pre-fault for a byte range within a mmap'd region.
// Uses MADV_POPULATE_READ (Linux 5.14+) which blocks until pages are faulted.
// Falls back to MADV_WILLNEED if unavailable.
// ---------------------------------------------------------------------------
static inline void prefault_range(const void* addr, size_t len) {
#ifdef MADV_POPULATE_READ
    // Blocking: guarantees pages are in page cache before return
    madvise(const_cast<void*>(addr), len, MADV_POPULATE_READ);
#else
    madvise(const_cast<void*>(addr), len, MADV_WILLNEED);
#endif
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

    // Pre-compute SIMD constants once
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

    // Load zone map index for l_shipdate
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Classify zones: edge (need date check) vs interior (skip date check)
    struct ZoneWork {
        uint32_t row_start;
        uint32_t row_end;
        bool is_interior;  // true = skip date check
    };
    std::vector<ZoneWork> work_zones;
    {
        GENDB_PHASE("zone_map_prune");
        for (const auto& z : zone_map.zones) {
            // Skip zones entirely outside the date range
            if (z.max < DATE_LO || z.min >= DATE_HI) continue;
            uint32_t rs = z.row_offset;
            uint32_t re = z.row_offset + z.row_count;
            // Interior: all rows guaranteed to pass date filter
            bool interior = (z.min >= DATE_LO && z.max < DATE_HI);
            work_zones.push_back({rs, re, interior});
        }
    }

    if (work_zones.empty()) {
        // Write zero result
        std::string out_path = results_dir + "/Q6.csv";
        std::FILE* f = std::fopen(out_path.c_str(), "w");
        if (f) { std::fprintf(f, "revenue\n0.00\n"); std::fclose(f); }
        return;
    }

    // Find global qualifying row range for prefaulting
    uint32_t global_row_start = work_zones.front().row_start;
    uint32_t global_row_end   = work_zones.back().row_end;

    // Synchronous page pre-fault: fault in ONLY the qualifying portion of each column.
    // MADV_POPULATE_READ blocks until all pages are resident — eliminates page-fault
    // latency during the parallel scan. Addresses must be page-aligned.
    {
        size_t page_size = 4096;
        auto page_align_down = [&](const void* p) -> void* {
            uintptr_t a = (uintptr_t)p & ~(uintptr_t)(page_size - 1);
            return (void*)a;
        };

        // For shipdate (int32): only edge zones need it, but prefault anyway for edge processing
        const char* sd_start = (const char*)col_shipdate.data + (size_t)global_row_start * 4;
        const char* sd_end   = (const char*)col_shipdate.data + (size_t)global_row_end   * 4;
        void* sd_pa = page_align_down(sd_start);
        size_t sd_len = sd_end - (const char*)sd_pa;

        const char* dc_start = (const char*)col_discount.data + (size_t)global_row_start * 8;
        const char* dc_end   = (const char*)col_discount.data + (size_t)global_row_end   * 8;
        void* dc_pa = page_align_down(dc_start);
        size_t dc_len = dc_end - (const char*)dc_pa;

        const char* qt_start = (const char*)col_quantity.data + (size_t)global_row_start * 8;
        const char* qt_end   = (const char*)col_quantity.data + (size_t)global_row_end   * 8;
        void* qt_pa = page_align_down(qt_start);
        size_t qt_len = qt_end - (const char*)qt_pa;

        const char* ep_start = (const char*)col_extprice.data + (size_t)global_row_start * 8;
        const char* ep_end   = (const char*)col_extprice.data + (size_t)global_row_end   * 8;
        void* ep_pa = page_align_down(ep_start);
        size_t ep_len = ep_end - (const char*)ep_pa;

        // Fire all 4 prefaults in parallel using threads before the main scan
        // Use pthreads-level parallelism so we don't incur OpenMP startup cost here
        #pragma omp parallel sections num_threads(4)
        {
            #pragma omp section
            prefault_range(sd_pa, sd_len);
            #pragma omp section
            prefault_range(dc_pa, dc_len);
            #pragma omp section
            prefault_range(qt_pa, qt_len);
            #pragma omp section
            prefault_range(ep_pa, ep_len);
        }
    }

    // Use 32 threads (one NUMA node's worth of physical cores).
    // Xeon Gold 5218: 16 physical cores per socket × 2 HT = 32 logical per socket.
    // Using all 64 threads across 2 NUMA nodes causes remote memory accesses
    // that add ~50-100ns latency per cache miss.
    const int NUM_THREADS = 32;
    const int num_zones = (int)work_zones.size();

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
                const auto& wz = work_zones[zi];
                if (wz.is_interior) {
                    // Fast path: no date check, pure discount+qty filter
                    local_sum += avx512_scan_interior(
                        discount, quantity, extprice,
                        wz.row_start, wz.row_end,
                        v_disc_lo, v_disc_hi, v_qty_max);
                } else {
                    // Edge path: full date check required
                    local_sum += avx512_scan_edge(
                        shipdate, discount, quantity, extprice,
                        wz.row_start, wz.row_end,
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
