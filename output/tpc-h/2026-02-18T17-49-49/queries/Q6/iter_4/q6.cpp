/*
 * Q6: Forecasting Revenue Change — Complete Architectural Rewrite (Iter 4)
 *
 * STRATEGY:
 * 1. Zone map index for l_shipdate: qualifying_ranges() prunes ~70-80% of blocks
 *    by skipping zones where block_max < DATE_LO or block_min >= DATE_HI.
 * 2. AVX-512 vectorized SIMD scan: process 16 int32 (date) or 8 int64 (disc/qty) per cycle.
 * 3. Two-phase late materialization:
 *    Phase A: SIMD scan of 3 filter columns (shipdate, discount, quantity) → selection vector
 *    Phase B: gather extendedprice only for surviving rows → accumulate
 * 4. OpenMP parallel scan with contiguous row-range morsels (better HW prefetch).
 * 5. Thread-local int64 accumulators — no false sharing.
 *
 * PREDICATE ORDER (most selective first):
 *   1. l_shipdate in [DATE_LO, DATE_HI)  — ~25% selectivity
 *   2. l_discount in [5, 7]              — ~27% selectivity
 *   3. l_quantity < 24                   — ~46% selectivity
 *   Combined: ~6% qualifying rows (~3.6M out of 60M)
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
#include <atomic>
#include <algorithm>

#include <immintrin.h>   // AVX-512 intrinsics
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// AVX-512 vectorized filter + accumulate kernel
// Processes 8 int64 elements per iteration using AVX-512.
// Filters: discount in [disc_lo, disc_hi], quantity < qty_max
// Accumulates: extprice[i] * discount[i] for qualifying rows
// ---------------------------------------------------------------------------
static inline int64_t avx512_scan_zone(
    const int32_t* __restrict__ shipdate,
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ extprice,
    uint32_t row_start,
    uint32_t row_end,
    int32_t date_lo,
    int32_t date_hi,
    int64_t disc_lo,
    int64_t disc_hi,
    int64_t qty_max)
{
    int64_t local_sum = 0;

    // AVX-512 constants for int64 comparisons (8 × int64 per zmm register)
    const __m512i v_disc_lo  = _mm512_set1_epi64(disc_lo);
    const __m512i v_disc_hi  = _mm512_set1_epi64(disc_hi);
    const __m512i v_qty_max  = _mm512_set1_epi64(qty_max);

    uint32_t i = row_start;
    uint32_t n = row_end;

    // Main SIMD loop: 8 rows at a time (matching int64 width)
    uint32_t n8 = row_start + ((n - row_start) & ~7u);
    for (; i < n8; i += 8) {
        // Load 8 int32 dates (use 256-bit ymm load, cheaper than 512-bit)
        __m256i vdate32 = _mm256_loadu_si256((__m256i*)(shipdate + i));
        // Widen to int64 for comparison with int64 thresholds? No: compare as int32.
        // Actually keep as int32: use AVX2 for date comparison (8 int32 in ymm)
        // date_lo <= sd < date_hi  =>  sd >= date_lo AND sd < date_hi
        // AVX2: _mm256_cmpgt_epi32 gives sd > val, so:
        //   pass_lo = sd >= date_lo  ≡  NOT (sd < date_lo)  ≡  NOT (date_lo > sd) ≡ NOT cmpgt(date_lo, sd)
        __m256i v_dlo = _mm256_set1_epi32(date_lo);
        __m256i v_dhi = _mm256_set1_epi32(date_hi - 1);
        // pass if date_lo <= sd AND sd <= date_hi-1
        __m256i cmp_lo = _mm256_cmpgt_epi32(vdate32, _mm256_sub_epi32(v_dlo, _mm256_set1_epi32(1))); // sd > date_lo-1 == sd >= date_lo
        __m256i cmp_hi = _mm256_cmpgt_epi32(_mm256_add_epi32(v_dhi, _mm256_set1_epi32(1)), vdate32); // date_hi > sd == sd < date_hi
        __m256i date_mask256 = _mm256_and_si256(cmp_lo, cmp_hi);
        // _mm256_movemask_ps gives 1 bit per float (= 1 bit per int32 lane) = 8 bits total
        uint8_t date_pmask8 = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(date_mask256));

        if (__builtin_expect(date_pmask8 == 0, 1)) continue; // all 8 fail date filter

        // Load discount and quantity (int64, 8 elements = 512 bits)
        __m512i vdisc = _mm512_loadu_si512((__m512i*)(discount + i));
        __m512i vqty  = _mm512_loadu_si512((__m512i*)(quantity + i));

        // disc >= disc_lo: use _mm512_cmpge_epi64_mask
        __mmask8 m_disc_lo = _mm512_cmpge_epi64_mask(vdisc, v_disc_lo);
        // disc <= disc_hi
        __mmask8 m_disc_hi = _mm512_cmple_epi64_mask(vdisc, v_disc_hi);
        // qty < qty_max
        __mmask8 m_qty     = _mm512_cmplt_epi64_mask(vqty, v_qty_max);

        // Combine all masks
        __mmask8 final_mask = date_pmask8 & m_disc_lo & m_disc_hi & m_qty;

        if (__builtin_expect(final_mask == 0, 1)) continue;

        // Load extprice and accumulate masked
        __m512i vext = _mm512_loadu_si512((__m512i*)(extprice + i));
        // Compute extprice * discount for all 8 rows
        // AVX-512 int64 multiply (low 64 bits of 64x64 product)
        // _mm512_mullo_epi64 requires AVX-512DQ (we have it)
        __m512i vprod = _mm512_mullo_epi64(vext, vdisc);
        // Masked reduce sum: sum only lanes where final_mask is set
        __m512i masked_prod = _mm512_mask_mov_epi64(_mm512_setzero_si512(), final_mask, vprod);
        // Horizontal sum
        local_sum += _mm512_reduce_add_epi64(masked_prod);
    }

    // Scalar tail
    for (; i < n; i++) {
        int32_t sd = shipdate[i];
        if (sd < date_lo || sd >= date_hi) continue;
        int64_t disc = discount[i];
        if (disc < disc_lo || disc > disc_hi) continue;
        int64_t qty = quantity[i];
        if (qty >= qty_max) continue;
        local_sum += extprice[i] * disc;
    }

    return local_sum;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // CORRECTNESS ANCHORS — DO NOT MODIFY
    // Use date_str_to_epoch_days() for reproducibility — single source of truth
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");  // 8766
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");  // 9131
    const int64_t DISC_LO = 5;     // 0.05 * 100
    const int64_t DISC_HI = 7;     // 0.07 * 100
    const int64_t QTY_MAX = 24;    // < 24

    // Zero-copy mmap access to all 4 columns
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns concurrently — overlap I/O with setup
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    // Load zone map index for l_shipdate — prunes ~70-80% of blocks on date range predicates
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Get qualifying row ranges (blocks where block_min <= DATE_HI and block_max >= DATE_LO)
    std::vector<std::pair<uint32_t, uint32_t>> zone_ranges;
    {
        GENDB_PHASE("zone_map_prune");
        zone_map.qualifying_ranges(DATE_LO, DATE_HI, zone_ranges);
    }

    // Morsel-driven parallel scan over qualifying zones only
    const int NUM_THREADS = 64;

    // Per-thread accumulators (cache-line padded to prevent false sharing)
    struct alignas(64) PaddedAcc {
        int64_t val;
        char _pad[56];
    };
    PaddedAcc thread_sums[NUM_THREADS] = {};

    {
        GENDB_PHASE("main_scan");

        // Raw pointers for SIMD — restrict hints for alias analysis
        const int32_t* __restrict__ shipdate = col_shipdate.data;
        const int64_t* __restrict__ discount  = col_discount.data;
        const int64_t* __restrict__ quantity  = col_quantity.data;
        const int64_t* __restrict__ extprice  = col_extprice.data;

        const uint32_t num_zones = (uint32_t)zone_ranges.size();
        std::atomic<uint32_t> zone_idx{0};

        // Launch threads via OpenMP — each thread claims qualifying zones atomically
        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            int64_t local_sum = 0;

            while (true) {
                uint32_t zi = zone_idx.fetch_add(1, std::memory_order_relaxed);
                if (zi >= num_zones) break;

                uint32_t row_start = zone_ranges[zi].first;
                uint32_t row_end   = zone_ranges[zi].second;

                local_sum += avx512_scan_zone(
                    shipdate, discount, quantity, extprice,
                    row_start, row_end,
                    DATE_LO, DATE_HI, DISC_LO, DISC_HI, QTY_MAX);
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
