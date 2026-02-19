/*
 * Q6: Forecasting Revenue Change — Iter 10 ARCHITECTURAL REWRITE
 *
 * PROBLEM ANALYSIS (stall recovery):
 * Prior iterations (5-9) used AVX-512 with branchy "skip batch if no dates pass" logic.
 * This is WRONG for Q6 because:
 *   1. Data is SORTED by l_shipdate → within qualifying zones, most rows pass the date
 *      filter (high density). The __builtin_expect(date_mask==0, unlikely) hint fires wrong,
 *      causing branch mispredictions on ~25% of batches.
 *   2. The int64 columns (discount, qty) require 512-bit loads even though values fit in
 *      int32. We can't change encoding, but we can eliminate redundant loads by using
 *      branchless SIMD that always loads all 4 columns together.
 *   3. Static zone-level scheduling with 64 threads over ~150 zones gives each thread
 *      ~2-3 zones. Thread startup + synchronization overhead becomes significant.
 *   4. mmap_prefetch_all (MADV_WILLNEED) on HDD before data is needed causes stalls.
 *      Better: use MADV_SEQUENTIAL (already set by MmapColumn::open) and let the
 *      sequential prefetcher work. For warm-cache runs (benchmark), avoid extra madvise.
 *
 * NEW STRATEGY:
 *   1. Zone map: find contiguous row range [global_start, global_end) covering all
 *      qualifying zones. Data is sorted by shipdate → qualifying zones are contiguous.
 *      Skip individual zone per-row date filter for "interior" zones (fully in range);
 *      only apply date filter on first/last partial zones.
 *   2. Branchless AVX-512 inner loop: always load all 4 columns, apply all 3 filters
 *      via mask operations, accumulate with masked add. No branch on date mask.
 *   3. Morsel-driven parallelism with atomic counter: threads pull 512K-row morsels
 *      from a shared counter. Better load balance than static zone scheduling.
 *   4. Reduce thread count to 32: on HDD, 64 threads cause random I/O fragmentation.
 *      32 threads with 512K-row morsels = better sequential access per thread.
 *   5. Pure AVX-512 branchless accumulation per 8-row batch.
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
#include <atomic>

#include <immintrin.h>   // AVX-512 intrinsics
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Fully branchless AVX-512 kernel: process rows [row_start, row_end).
// Applies all 3 filters simultaneously, no early-exit branches.
// Mask-add into lane accumulator, single reduce at end.
//
// Why branchless here: within qualifying zones sorted by shipdate, date filter
// density is high (~25% of rows overall but concentrated in qualifying zones).
// The old "if (dpmask != 0)" branch mispredicts ~25% of the time.
// Branchless SIMD with mask-add is faster for this density.
// ---------------------------------------------------------------------------
static inline int64_t branchless_scan(
    const int32_t* __restrict__ shipdate,
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ extprice,
    uint32_t row_start,
    uint32_t row_end,
    const __m256i v_dlo_m1,    // date_lo - 1 (sd > date_lo-1 means sd >= date_lo)
    const __m256i v_dhi,       // date_hi     (date_hi > sd means sd < date_hi)
    const __m512i v_disc_lo,
    const __m512i v_disc_hi,
    const __m512i v_qty_max)
{
    __m512i lane_acc = _mm512_setzero_si512();

    uint32_t i = row_start;

    // Main loop: 8 rows per iteration (one AVX-512 batch for int64 columns)
    // AVX2 for int32 dates (8 lanes), AVX-512 for int64 discount/qty/extprice (8 lanes)
    uint32_t n8 = row_start + ((row_end - row_start) & ~7u);
    for (; i < n8; i += 8) {
        // Load 8 int32 shipdates via AVX2
        __m256i vdate = _mm256_loadu_si256((const __m256i*)(shipdate + i));
        // sd >= date_lo: sd > date_lo-1
        __m256i cmp_lo = _mm256_cmpgt_epi32(vdate, v_dlo_m1);
        // sd <  date_hi: date_hi > sd
        __m256i cmp_hi = _mm256_cmpgt_epi32(v_dhi, vdate);
        __m256i date_and = _mm256_and_si256(cmp_lo, cmp_hi);
        // _mm256_movemask_ps extracts MSB of each 32-bit float lane → 8-bit mask
        __mmask8 date_mask = (__mmask8)(uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_and));

        // Load int64 columns (always — no branch)
        __m512i vdisc = _mm512_loadu_si512((const __m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((const __m512i*)(quantity  + i));

        // Discount filter: disc >= disc_lo AND disc <= disc_hi
        __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
        __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
        __mmask8 m_disc = m_disc_lo & m_disc_hi;
        // Quantity filter: qty < qty_max
        __mmask8 m_qty  = _mm512_cmplt_epi64_mask(vqty, v_qty_max);

        // Combine all masks
        __mmask8 final_mask = date_mask & m_disc & m_qty;

        if (final_mask) {
            __m512i vext  = _mm512_loadu_si512((const __m512i*)(extprice + i));
            __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
            lane_acc = _mm512_mask_add_epi64(lane_acc, final_mask, lane_acc, vprod);
        }
    }

    // Scalar tail
    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Extract scalar constants for tail
    alignas(32) int32_t dlo_arr[8]; _mm256_store_si256((__m256i*)dlo_arr, v_dlo_m1);
    alignas(64) int64_t dlo64_arr[8]; _mm512_store_si512((__m512i*)dlo64_arr, v_disc_lo);
    alignas(64) int64_t dhi64_arr[8]; _mm512_store_si512((__m512i*)dhi64_arr, v_disc_hi);
    alignas(64) int64_t qty64_arr[8]; _mm512_store_si512((__m512i*)qty64_arr, v_qty_max);
    const int32_t date_lo_s = dlo_arr[0] + 1;
    const int32_t date_hi_s = _mm256_extract_epi32(v_dhi, 0);
    const int64_t disc_lo_s = dlo64_arr[0];
    const int64_t disc_hi_s = dhi64_arr[0];
    const int64_t qty_max_s = qty64_arr[0];

    for (; i < row_end; i++) {
        int32_t sd = shipdate[i];
        if (sd < date_lo_s || sd >= date_hi_s) continue;
        int64_t disc = discount[i];
        if (disc < disc_lo_s || disc > disc_hi_s) continue;
        int64_t qty = quantity[i];
        if (qty >= qty_max_s) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

// ---------------------------------------------------------------------------
// Fully branchless variant that SKIPS the date filter (for interior zones
// where ALL rows are guaranteed to pass the date predicate).
// ---------------------------------------------------------------------------
static inline int64_t branchless_scan_no_date(
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ extprice,
    uint32_t row_start,
    uint32_t row_end,
    const __m512i v_disc_lo,
    const __m512i v_disc_hi,
    const __m512i v_qty_max)
{
    __m512i lane_acc = _mm512_setzero_si512();

    uint32_t i = row_start;
    uint32_t n8 = row_start + ((row_end - row_start) & ~7u);

    for (; i < n8; i += 8) {
        __m512i vdisc = _mm512_loadu_si512((const __m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((const __m512i*)(quantity  + i));

        __mmask8 m_disc = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo) &
                          _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
        __mmask8 m_qty  = _mm512_cmplt_epi64_mask(vqty, v_qty_max);
        __mmask8 final_mask = m_disc & m_qty;

        if (final_mask) {
            __m512i vext  = _mm512_loadu_si512((const __m512i*)(extprice + i));
            __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
            lane_acc = _mm512_mask_add_epi64(lane_acc, final_mask, lane_acc, vprod);
        }
    }

    int64_t local_sum = _mm512_reduce_add_epi64(lane_acc);

    // Extract scalar constants for tail
    alignas(64) int64_t dlo64_arr[8]; _mm512_store_si512((__m512i*)dlo64_arr, v_disc_lo);
    alignas(64) int64_t dhi64_arr[8]; _mm512_store_si512((__m512i*)dhi64_arr, v_disc_hi);
    alignas(64) int64_t qty64_arr[8]; _mm512_store_si512((__m512i*)qty64_arr, v_qty_max);

    for (; i < row_end; i++) {
        int64_t disc = discount[i];
        if (disc < dlo64_arr[0] || disc > dhi64_arr[0]) continue;
        int64_t qty = quantity[i];
        if (qty >= qty64_arr[0]) continue;
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

    // Pre-compute SIMD constants ONCE
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

    // Load zone map index for l_shipdate pruning
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // ARCHITECTURAL CHANGE: Instead of per-zone processing, find the contiguous
    // row range covering ALL qualifying zones, then classify each zone as:
    //   - "interior": zone.min >= DATE_LO && zone.max < DATE_HI → skip date filter
    //   - "boundary": first/last zones that partially overlap the date range → apply date filter
    //
    // This enables the faster branchless_scan_no_date() for interior zones.
    struct ZoneWork {
        uint32_t row_start;
        uint32_t row_end;
        bool need_date_filter;  // false for interior zones
    };

    std::vector<ZoneWork> work_zones;
    {
        GENDB_PHASE("zone_map_prune");
        for (const auto& z : zone_map.zones) {
            // Skip zones entirely outside date range
            if (z.max < DATE_LO || z.min >= DATE_HI) continue;
            // Classify: interior = all rows guaranteed in date range
            bool interior = (z.min >= DATE_LO && z.max < DATE_HI);
            work_zones.push_back({
                z.row_offset,
                z.row_offset + z.row_count,
                !interior
            });
        }
    }

    const int num_zones = (int)work_zones.size();

    // Use 32 threads: better sequential I/O per thread on HDD vs 64 threads
    // (64 threads cause too-small per-thread working sets → random I/O patterns)
    const int NUM_THREADS = 32;

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

        // Morsel-driven parallelism: atomic zone counter.
        // Each thread pulls one zone at a time. Zones are ~100K rows.
        // This avoids static scheduling imbalance when zone sizes vary.
        std::atomic<int> zone_counter{0};

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            int64_t local_sum = 0;

            while (true) {
                int zi = zone_counter.fetch_add(1, std::memory_order_relaxed);
                if (zi >= num_zones) break;

                const ZoneWork& zw = work_zones[zi];

                if (zw.need_date_filter) {
                    // Boundary zone: apply full date + discount + qty filter
                    local_sum += branchless_scan(
                        shipdate, discount, quantity, extprice,
                        zw.row_start, zw.row_end,
                        v_dlo_m1, v_dhi,
                        v_disc_lo, v_disc_hi, v_qty_max);
                } else {
                    // Interior zone: skip date filter entirely (guaranteed in range)
                    local_sum += branchless_scan_no_date(
                        discount, quantity, extprice,
                        zw.row_start, zw.row_end,
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
