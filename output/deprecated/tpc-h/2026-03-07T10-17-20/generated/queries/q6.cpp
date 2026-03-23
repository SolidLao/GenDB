// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;
//
// Strategy:
//   1. Zone-map prune: skip ~508/600 blocks using l_shipdate_zone_map.bin
//   2. Parallel morsel scan over ~92 qualifying blocks (static OMP)
//   3. Full-block flag: skip per-row date check for blocks fully inside range
//   4. AVX-512 branchless masked accumulate for full blocks (8 doubles/cycle)
//   5. Scalar branchless for partial blocks (boundary blocks needing date check)
//
// Usage: ./q6 <gendb_dir> <results_dir>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <climits>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <immintrin.h>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

// ---------------------------------------------------------------------------
// Filter constants (compile-time)
// ---------------------------------------------------------------------------
static constexpr int32_t kShipdateLo = 8766;   // 1994-01-01 (days since 1970-01-01)
static constexpr int32_t kShipdateHi = 9131;   // 1995-01-01 (exclusive)
static constexpr double  kDiscLo     = 0.05;
static constexpr double  kDiscHi     = 0.07;
static constexpr double  kQtyMax     = 24.0;
static constexpr size_t  kBlockSize  = 100000;

// ---------------------------------------------------------------------------
// Qualifying block descriptor
// ---------------------------------------------------------------------------
struct QBlock {
    int32_t block_id;
    bool    full;       // true = block entirely inside date range, skip per-row date check
};

// ---------------------------------------------------------------------------
// AVX-512 inner loop for a FULL block (no date predicate needed)
// Computes SUM(l_extendedprice * l_discount) where l_discount in [kDiscLo, kDiscHi]
// AND l_quantity < kQtyMax.
// ---------------------------------------------------------------------------
static inline double scan_full_block_avx512(
        const double* __restrict__ disc,
        const double* __restrict__ qty,
        const double* __restrict__ ext,
        size_t len)
{
#if defined(__AVX512F__)
    const __m512d vDiscLo = _mm512_set1_pd(kDiscLo);
    const __m512d vDiscHi = _mm512_set1_pd(kDiscHi);
    const __m512d vQtyMax = _mm512_set1_pd(kQtyMax);

    __m512d sum8 = _mm512_setzero_pd();

    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        __m512d vd = _mm512_loadu_pd(disc + i);
        __m512d vq = _mm512_loadu_pd(qty  + i);
        __m512d ve = _mm512_loadu_pd(ext  + i);

        __mmask8 m = _mm512_cmp_pd_mask(vd, vDiscLo, _CMP_GE_OQ);
        m &= _mm512_cmp_pd_mask(vd, vDiscHi, _CMP_LE_OQ);
        m &= _mm512_cmp_pd_mask(vq, vQtyMax, _CMP_LT_OQ);

        __m512d prod = _mm512_mul_pd(ve, vd);
        sum8 = _mm512_mask_add_pd(sum8, m, sum8, prod);
    }

    double block_sum = _mm512_reduce_add_pd(sum8);

    // Scalar tail
    for (; i < len; ++i) {
        if (disc[i] >= kDiscLo & disc[i] <= kDiscHi & qty[i] < kQtyMax) {
            block_sum += ext[i] * disc[i];
        }
    }
    return block_sum;
#else
    // Fallback: branchless scalar (compiler will auto-vectorize)
    double block_sum = 0.0;
    for (size_t i = 0; i < len; ++i) {
        bool pass = (disc[i] >= kDiscLo) & (disc[i] <= kDiscHi) & (qty[i] < kQtyMax);
        block_sum += pass ? ext[i] * disc[i] : 0.0;
    }
    return block_sum;
#endif
}

// ---------------------------------------------------------------------------
// Scalar inner loop for a PARTIAL block (need per-row date check)
// ---------------------------------------------------------------------------
static inline double scan_partial_block(
        const int32_t* __restrict__ ship,
        const double*  __restrict__ disc,
        const double*  __restrict__ qty,
        const double*  __restrict__ ext,
        size_t len)
{
    double block_sum = 0.0;
    for (size_t i = 0; i < len; ++i) {
        bool pass = (ship[i] >= kShipdateLo) & (ship[i] < kShipdateHi)
                  & (disc[i] >= kDiscLo)     & (disc[i] <= kDiscHi)
                  & (qty[i]  < kQtyMax);
        block_sum += pass ? ext[i] * disc[i] : 0.0;
    }
    return block_sum;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Phase: data_loading — read zone map, build qualifying block list
    // -------------------------------------------------------------------------
    std::vector<QBlock> qblocks;
    qblocks.reserve(128);

    {
        GENDB_PHASE("data_loading");

        const std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";

        // Zone map layout (int32_t packed):
        //   [0]          num_blocks
        //   [1]          block_size
        //   [2..601]     min[600]
        //   [602..1201]  max[600]
        MmapColumn<int32_t> zm(zm_path);

        const int32_t* zd = zm.data;
        const int32_t num_blocks = zd[0];
        // block_size = zd[1] (= 100000, already a constant)
        const int32_t* zm_min = zd + 2;
        const int32_t* zm_max = zd + 2 + num_blocks;

        for (int32_t b = 0; b < num_blocks; ++b) {
            if (zm_max[b] < kShipdateLo) continue;   // block before range
            if (zm_min[b] >= kShipdateHi) break;     // sorted: all later blocks past range
            // Block overlaps the range
            bool full = (zm_min[b] >= kShipdateLo) && (zm_max[b] < kShipdateHi);
            qblocks.push_back({b, full});
        }
    }

    if (qblocks.empty()) {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        FILE* f = std::fopen((results_dir + "/Q6.csv").c_str(), "w");
        std::fprintf(f, "revenue\n0.00\n");
        std::fclose(f);
        return 0;
    }

    // -------------------------------------------------------------------------
    // Mmap the four columns
    // -------------------------------------------------------------------------
    const std::string li_dir = gendb_dir + "/lineitem";
    MmapColumn<int32_t> col_shipdate(li_dir + "/l_shipdate.bin");
    MmapColumn<double>  col_discount(li_dir + "/l_discount.bin");
    MmapColumn<double>  col_quantity(li_dir + "/l_quantity.bin");
    MmapColumn<double>  col_extprice(li_dir + "/l_extendedprice.bin");

    const size_t total_rows = col_shipdate.size();

    // Prefetch qualifying row ranges only (avoid wasted I/O for skipped blocks)
    {
        int32_t first_b = qblocks.front().block_id;
        int32_t last_b  = qblocks.back().block_id;
        size_t row_lo = static_cast<size_t>(first_b) * kBlockSize;
        size_t row_hi = std::min(static_cast<size_t>(last_b + 1) * kBlockSize, total_rows);

        madvise(const_cast<void*>((const void*)(col_shipdate.data + row_lo)),
                (row_hi - row_lo) * sizeof(int32_t), MADV_WILLNEED);
        madvise(const_cast<void*>((const void*)(col_discount.data + row_lo)),
                (row_hi - row_lo) * sizeof(double), MADV_WILLNEED);
        madvise(const_cast<void*>((const void*)(col_quantity.data + row_lo)),
                (row_hi - row_lo) * sizeof(double), MADV_WILLNEED);
        madvise(const_cast<void*>((const void*)(col_extprice.data + row_lo)),
                (row_hi - row_lo) * sizeof(double), MADV_WILLNEED);
    }

    // -------------------------------------------------------------------------
    // Phase: main_scan — parallel morsel scan over qualifying blocks
    // -------------------------------------------------------------------------
    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        const int32_t*  __restrict__ ship = col_shipdate.data;
        const double*   __restrict__ disc = col_discount.data;
        const double*   __restrict__ qty  = col_quantity.data;
        const double*   __restrict__ ext  = col_extprice.data;

        const int nq = static_cast<int>(qblocks.size());

        #pragma omp parallel reduction(+:global_revenue) num_threads(64)
        {
            double local_sum = 0.0;

            #pragma omp for schedule(static) nowait
            for (int qi = 0; qi < nq; ++qi) {
                const QBlock& qb = qblocks[qi];
                size_t row_lo = static_cast<size_t>(qb.block_id) * kBlockSize;
                size_t row_hi = std::min(row_lo + kBlockSize, total_rows);
                size_t len    = row_hi - row_lo;

                if (qb.full) {
                    local_sum += scan_full_block_avx512(
                        disc + row_lo, qty + row_lo, ext + row_lo, len);
                } else {
                    local_sum += scan_partial_block(
                        ship + row_lo, disc + row_lo, qty + row_lo, ext + row_lo, len);
                }
            }

            global_revenue += local_sum;
        }
    }

    // -------------------------------------------------------------------------
    // Phase: output — write CSV result
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q6.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output: %s\n", out_path.c_str());
            return 1;
        }
        std::fprintf(f, "revenue\n%.2f\n", global_revenue);
        std::fclose(f);
        std::printf("revenue: %.2f\n", global_revenue);
    }

    return 0;
}
