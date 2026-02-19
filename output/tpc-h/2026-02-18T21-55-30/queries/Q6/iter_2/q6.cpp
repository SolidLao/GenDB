// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24
//
// Physical Plan:
// 1. Observations from profiling:
//    - Zone maps give 0% pruning (all 600 blocks have full-range data — not sorted by shipdate)
//    - Must scan all 60M rows; bottleneck is pure scan throughput
//    - Hardware: AVX-512 available, 64 cores / 2 NUMA nodes (32 each)
// 2. Strategy: OpenMP parallel scan with AVX-512 explicit SIMD inner loop
//    - Process 8 int64 elements per AVX-512 iteration for discount/qty/extprice
//    - Handle shipdate (int32) with AVX-512: 16 per vector, check 8 at a time (interleaved with int64)
//    - Minimize pointer chasing; use __restrict__, aligned loop structure
// 3. Correctness anchors (DO NOT MODIFY):
//    - SHIPDATE_LO = 8766, SHIPDATE_HI = 9131 (days since epoch)
//    - DISC_LO = 5, DISC_HI = 7  (stored * 100)
//    - QTY_HI = 24               (l_quantity scale_factor=1, raw value IS quantity)
//    - revenue = global_sum / 10000  (ep and discount both scaled *100)

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <fstream>
#include <immintrin.h>

#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Correctness anchors — DO NOT MODIFY
// ---------------------------------------------------------------------------
static constexpr int32_t  SHIPDATE_LO  = 8766;   // 1994-01-01
static constexpr int32_t  SHIPDATE_HI  = 9131;   // 1995-01-01
static constexpr int64_t  DISC_LO      = 5;       // 0.05 * 100
static constexpr int64_t  DISC_HI      = 7;       // 0.07 * 100
static constexpr int64_t  QTY_HI       = 24;      // l_quantity scale_factor=1, raw value IS quantity

static constexpr size_t   BLOCK_SIZE   = 100000;

// ---------------------------------------------------------------------------
// AVX-512 hot loop: scan a contiguous row range, accumulate sum
// Processes 8 rows per iteration (8 x int64 per AVX-512 register)
// ---------------------------------------------------------------------------
static int64_t scan_range_avx512(
    const int32_t* __restrict__ sd,   // shipdate (int32_t)
    const int64_t* __restrict__ dis,  // discount (int64_t)
    const int64_t* __restrict__ qty,  // quantity (int64_t)
    const int64_t* __restrict__ ep,   // extendedprice (int64_t)
    size_t n)
{
    int64_t total = 0;

#ifdef __AVX512F__
    // Broadcast constants into AVX-512 registers
    const __m512i v_disc_lo = _mm512_set1_epi64(DISC_LO);
    const __m512i v_disc_hi = _mm512_set1_epi64(DISC_HI);
    const __m512i v_qty_hi  = _mm512_set1_epi64(QTY_HI);
    const __m256i v_sd_lo   = _mm256_set1_epi32(SHIPDATE_LO);
    const __m256i v_sd_hi   = _mm256_set1_epi32(SHIPDATE_HI);     // use SHIPDATE_HI > s  <=>  s < SHIPDATE_HI

    // Accumulators: 8 x int64 partial sums (avoid overflow with i64)
    __m512i acc = _mm512_setzero_si512();

    size_t i = 0;
    // Process 8 rows at a time (8 x int64)
    for (; i + 8 <= n; i += 8) {
        // Load 8 discount values (int64_t)
        __m512i vd = _mm512_loadu_si512((__m512i const*)(dis + i));

        // Discount filter: d >= DISC_LO && d <= DISC_HI
        // _mm512_cmpge_epi64_mask, _mm512_cmple_epi64_mask
        __mmask8 m_disc = _mm512_cmpge_epi64_mask(vd, v_disc_lo) &
                          _mm512_cmple_epi64_mask(vd, v_disc_hi);
        if (m_disc == 0) continue;

        // Load 8 quantity values (int64_t)
        __m512i vq = _mm512_loadu_si512((__m512i const*)(qty + i));

        // Quantity filter: q < QTY_HI
        __mmask8 m_qty = _mm512_cmplt_epi64_mask(vq, v_qty_hi);
        __mmask8 m_dq = m_disc & m_qty;
        if (m_dq == 0) continue;

        // Load 8 shipdate values (int32_t) — only need 8, load as 256-bit
        __m256i vs = _mm256_loadu_si256((__m256i const*)(sd + i));

        // Shipdate filter: s >= SHIPDATE_LO && s < SHIPDATE_HI
        // i.e., s >= 8766 && s <= 9130
        // AVX2 int32 comparisons (signed): only cmpgt available
        // s >= SHIPDATE_LO  <=>  s > (SHIPDATE_LO - 1)
        // s < SHIPDATE_HI   <=>  SHIPDATE_HI > s  (direct cmpgt, no adjustment needed)
        __m256i above_lo = _mm256_cmpgt_epi32(vs, _mm256_sub_epi32(v_sd_lo, _mm256_set1_epi32(1)));
        __m256i below_hi = _mm256_cmpgt_epi32(v_sd_hi, vs);
        __m256i sd_pass  = _mm256_and_si256(above_lo, below_hi);

        // Convert 8-wide int32 mask to uint8 mask
        // Each int32 mask lane is 0xFFFFFFFF or 0x00000000 — extract sign bit
        uint32_t sd_mask32 = (uint32_t)_mm256_movemask_epi8(sd_pass); // 32 bits, 4 per lane
        // We have 8 lanes × 4 bytes = 32 bytes mask; bit pattern: bits 0,4,8,12,16,20,24,28 are the top bits
        // Compact to 8-bit mask: take bits 3,7,11,15,19,23,27,31 (MSB of each 4-byte group)
        uint8_t m_sd = (uint8_t)(
            ((sd_mask32 >> 3)  & 0x01) |
            ((sd_mask32 >> 6)  & 0x02) |
            ((sd_mask32 >> 9)  & 0x04) |
            ((sd_mask32 >> 12) & 0x08) |
            ((sd_mask32 >> 15) & 0x10) |
            ((sd_mask32 >> 18) & 0x20) |
            ((sd_mask32 >> 21) & 0x40) |
            ((sd_mask32 >> 24) & 0x80)
        );

        __mmask8 m_all = m_dq & (__mmask8)m_sd;
        if (m_all == 0) continue;

        // Load extprice for the 8 rows
        __m512i vep = _mm512_loadu_si512((__m512i const*)(ep + i));

        // Compute ep * d for qualifying rows (masked multiply)
        // We need ep[i] * dis[i] for rows where m_all is set
        // Use masked mullo — but int64 mullo (_mm512_mullo_epi64) requires AVX512DQ
        __m512i prod = _mm512_mullo_epi64(vep, vd);

        // Masked accumulate: only add where all predicates pass
        acc = _mm512_mask_add_epi64(acc, m_all, acc, prod);
    }

    // Reduce accumulator
    total = _mm512_reduce_add_epi64(acc);

    // Scalar tail
    for (; i < n; i++) {
        int64_t d = dis[i];
        if (d < DISC_LO || d > DISC_HI) continue;
        if (qty[i] >= QTY_HI) continue;
        int32_t s = sd[i];
        if (s < SHIPDATE_LO || s >= SHIPDATE_HI) continue;
        total += ep[i] * d;
    }

#else
    // Scalar fallback
    for (size_t i = 0; i < n; i++) {
        int64_t d = dis[i];
        if (d < DISC_LO || d > DISC_HI) continue;
        if (qty[i] >= QTY_HI) continue;
        int32_t s = sd[i];
        if (s < SHIPDATE_LO || s >= SHIPDATE_HI) continue;
        total += ep[i] * d;
    }
#endif

    return total;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Phase 1: Load columns via mmap (zero-copy)
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;

    {
        GENDB_PHASE("load_columns");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        // Prefetch all columns into page cache concurrently
        mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);
    }

    const size_t total_rows = col_shipdate.count;
    const size_t num_blocks = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // -----------------------------------------------------------------------
    // Phase 2: Parallel scan — OpenMP static schedule, 64 threads
    // -----------------------------------------------------------------------
    int64_t global_sum = 0;

    {
        GENDB_PHASE("main_scan");

        const int32_t*  shipdate_ptr = col_shipdate.data;
        const int64_t*  discount_ptr = col_discount.data;
        const int64_t*  quantity_ptr = col_quantity.data;
        const int64_t*  extprice_ptr = col_extprice.data;

        // Use OpenMP with reduction for minimal overhead
        // Static schedule gives equal work partitions and zero atomic overhead
        #pragma omp parallel for schedule(static) num_threads(64) reduction(+:global_sum)
        for (size_t blk = 0; blk < num_blocks; blk++) {
            size_t row_start = blk * BLOCK_SIZE;
            size_t row_end   = row_start + BLOCK_SIZE;
            if (row_end > total_rows) row_end = total_rows;
            size_t n = row_end - row_start;

            global_sum += scan_range_avx512(
                shipdate_ptr + row_start,
                discount_ptr + row_start,
                quantity_ptr + row_start,
                extprice_ptr + row_start,
                n
            );
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Output results
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // global_sum = SUM(extprice * discount) where both are scaled by 100
        // Actual revenue = global_sum / 10000
        int64_t integer_part = global_sum / 10000LL;
        int64_t frac_part    = global_sum % 10000LL;

        char buf[64];
        std::snprintf(buf, sizeof(buf), "%lld.%04lld",
                      (long long)integer_part, (long long)frac_part);

        std::string out_path = results_dir + "/Q6.csv";
        std::ofstream ofs(out_path);
        ofs << "revenue\n";
        ofs << buf << "\n";
        ofs.close();

        std::printf("revenue = %s\n", buf);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
