// Q6: Forecasting Revenue Change
// SUM(l_extendedprice * l_discount) WHERE shipdate in [1994-01-01, 1995-01-01)
//   AND discount BETWEEN 0.05 AND 0.07 AND quantity < 24

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <immintrin.h>
#include <omp.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "timing_utils.h"

// MADV_POPULATE_READ: blocking madvise that faults all pages before returning (Linux 5.14+)
#ifndef MADV_POPULATE_READ
#define MADV_POPULATE_READ 22
#endif

// ─── constants ───────────────────────────────────────────────────────────────
static constexpr int32_t  DATE_LO   = 8766;   // 1994-01-01
static constexpr int32_t  DATE_HI   = 9131;   // 1995-01-01 (exclusive)
static constexpr double   DISC_LO   = 0.05;
static constexpr double   DISC_HI   = 0.07;
static constexpr double   QTY_MAX   = 24.0;   // quantity < 24
static constexpr uint32_t BLOCK_SZ  = 100000;

// ─── mmap helper ─────────────────────────────────────────────────────────────
template<typename T>
struct Col {
    const T* data  = nullptr;
    size_t   n     = 0;
    size_t   bytes = 0;
    int      fd    = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = (size_t)st.st_size;
        // No MAP_POPULATE: don't fault all pages; use selective prefetch after zone-map prune
        data  = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); std::exit(1); }
        n = bytes / sizeof(T);
    }

    // Blocking prefetch: faults all pages in the qualifying range before returning.
    // MADV_POPULATE_READ (Linux 5.14+) ensures pages are resident when this returns.
    void prefetch_rows(size_t r0, size_t r1) const {
        size_t b0 = r0 * sizeof(T);
        size_t b1 = std::min(r1 * sizeof(T), bytes);
        if (b1 <= b0) return;
        madvise((void*)((const char*)data + b0), b1 - b0, MADV_POPULATE_READ);
    }
};

// ─── zone map structs ────────────────────────────────────────────────────────
struct ZMInt   { int32_t mn, mx; };
struct ZMDbl   { double  mn, mx; };

template<typename Entry>
struct ZoneMap {
    uint32_t      num_blocks = 0;
    const Entry*  entries    = nullptr;
    size_t        bytes      = 0;
    int           fd         = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = (size_t)st.st_size;
        const char* raw = reinterpret_cast<const char*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (raw == MAP_FAILED) { perror("mmap zm"); std::exit(1); }
        num_blocks = *reinterpret_cast<const uint32_t*>(raw);
        entries    =  reinterpret_cast<const Entry*>(raw + sizeof(uint32_t));
    }
};

// ─── main query function ──────────────────────────────────────────────────────
void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string base = gendb_dir + "/lineitem/";
    const std::string idx  = gendb_dir + "/indexes/";

    // ── Phase 0: Open / mmap all files ───────────────────────────────────────
    ZoneMap<ZMInt> zm_ship;
    ZoneMap<ZMDbl> zm_disc, zm_qty;
    Col<int32_t>   c_ship;
    Col<double>    c_disc, c_qty, c_ep;
    uint32_t first_blk, last_blk;

    {
        GENDB_PHASE("data_loading");

        // Step 1: Load tiny zone maps first (MAP_POPULATE OK — they're small)
        zm_ship.open(idx + "lineitem_shipdate_zonemap.bin");
        zm_disc.open(idx + "lineitem_discount_zonemap.bin");
        zm_qty .open(idx + "lineitem_quantity_zonemap.bin");

        // Step 2: Binary search on shipdate zone map → qualifying block range
        // (lineitem is sorted by l_shipdate → contiguous range, max pruning)
        uint32_t nb = zm_ship.num_blocks;
        {
            // first_blk: first block where block_max >= DATE_LO
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mx >= DATE_LO) hi = mid;
                else                                    lo = mid + 1;
            }
            first_blk = lo;
        }
        {
            // last_blk: last block where block_min < DATE_HI
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mn < DATE_HI) lo = mid + 1;
                else                                   hi = mid;
            }
            last_blk = (lo > 0) ? lo - 1 : 0;
        }

        // Step 3: Open column files WITHOUT MAP_POPULATE (lazy mmap — no page faults yet)
        c_ship.open(base + "l_shipdate.bin");
        c_disc.open(base + "l_discount.bin");
        c_qty .open(base + "l_quantity.bin");
        c_ep  .open(base + "l_extendedprice.bin");

        // Step 4: Blocking prefetch ONLY the qualifying ~15.6% of each column.
        // 16 threads: 4 columns × 4 sub-range chunks each — maximizes I/O parallelism.
        // MADV_POPULATE_READ guarantees pages are resident before the scan begins,
        // so main_scan runs with fully warm cache (no page faults during compute).
        if (first_blk <= last_blk) {
            size_t r0 = (size_t)first_blk * BLOCK_SZ;
            size_t r1 = std::min((size_t)(last_blk + 1) * BLOCK_SZ, c_ship.n);
            size_t row_span  = r1 - r0;
            size_t chunk_len = (row_span + 3) / 4;

            #pragma omp parallel for num_threads(16) schedule(static)
            for (int slot = 0; slot < 16; ++slot) {
                int    col   = slot / 4;
                int    chunk = slot % 4;
                size_t cr0   = r0 + (size_t)chunk * chunk_len;
                size_t cr1   = std::min(cr0 + chunk_len, r1);
                if (cr0 >= cr1) continue;
                switch (col) {
                    case 0: c_ship.prefetch_rows(cr0, cr1); break;
                    case 1: c_disc.prefetch_rows(cr0, cr1); break;
                    case 2: c_qty .prefetch_rows(cr0, cr1); break;
                    case 3: c_ep  .prefetch_rows(cr0, cr1); break;
                }
            }
        }
    }

    // ── Phase 2: Parallel scan with fused filter + accumulate ─────────────────
    double global_sum = 0.0;
    {
        GENDB_PHASE("main_scan");

        const int32_t*  ship = c_ship.data;
        const double*   disc = c_disc.data;
        const double*   qty  = c_qty.data;
        const double*   ep   = c_ep.data;
        const size_t    total_rows = c_ship.n;

        #pragma omp parallel num_threads(64) reduction(+:global_sum)
        {
            double local_sum = 0.0;

            // Hoist SIMD constants outside the block loop — one init per thread.
#ifdef __AVX512F__
            const __m512d vDISC_LO_512 = _mm512_set1_pd(DISC_LO);
            const __m512d vDISC_HI_512 = _mm512_set1_pd(DISC_HI);
            const __m512d vQTY_MAX_512 = _mm512_set1_pd(QTY_MAX);
#elif defined(__AVX2__)
            const __m256d vDISC_LO_256 = _mm256_set1_pd(DISC_LO);
            const __m256d vDISC_HI_256 = _mm256_set1_pd(DISC_HI);
            const __m256d vQTY_MAX_256 = _mm256_set1_pd(QTY_MAX);
#endif

            #pragma omp for schedule(static)
            for (uint32_t blk = first_blk; blk <= last_blk; ++blk) {
                // Discount zone map pruning
                const ZMDbl& zd = zm_disc.entries[blk];
                if (zd.mx < DISC_LO || zd.mn > DISC_HI) continue;

                // Quantity zone map pruning
                const ZMDbl& zq = zm_qty.entries[blk];
                if (zq.mn >= QTY_MAX) continue;

                size_t row_start = (size_t)blk * BLOCK_SZ;
                size_t row_end   = std::min(row_start + BLOCK_SZ, total_rows);

                // Determine if this is an interior block (date predicate always true)
                bool interior = (zm_ship.entries[blk].mn >= DATE_LO &&
                                 zm_ship.entries[blk].mx <  DATE_HI);

                if (interior) {
                    // Interior block: skip shipdate check — fused discount+qty+ep loop.
                    size_t i = row_start;
                    size_t n = row_end - row_start;

#ifdef __AVX512F__
                    // AVX-512: 8 doubles per iteration
                    __m512d vacc = _mm512_setzero_pd();
                    size_t i_end8 = row_start + (n & ~(size_t)7);
                    for (; i < i_end8; i += 8) {
                        __m512d vd = _mm512_loadu_pd(disc + i);
                        __m512d vq = _mm512_loadu_pd(qty  + i);
                        __m512d ve = _mm512_loadu_pd(ep   + i);
                        __mmask8 mask =
                            _mm512_cmp_pd_mask(vd, vDISC_LO_512, _CMP_GE_OQ) &
                            _mm512_cmp_pd_mask(vd, vDISC_HI_512, _CMP_LE_OQ) &
                            _mm512_cmp_pd_mask(vq, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc = _mm512_mask_add_pd(vacc, mask, vacc, _mm512_mul_pd(ve, vd));
                    }
                    local_sum += _mm512_reduce_add_pd(vacc);
#elif defined(__AVX2__)
                    // AVX2: 4 doubles per iteration
                    __m256d vacc = _mm256_setzero_pd();
                    size_t i_end4 = row_start + (n & ~(size_t)3);
                    for (; i < i_end4; i += 4) {
                        __m256d vd = _mm256_loadu_pd(disc + i);
                        __m256d vq = _mm256_loadu_pd(qty  + i);
                        __m256d ve = _mm256_loadu_pd(ep   + i);
                        __m256d m1   = _mm256_cmp_pd(vd, vDISC_LO_256, _CMP_GE_OQ);
                        __m256d m2   = _mm256_cmp_pd(vd, vDISC_HI_256, _CMP_LE_OQ);
                        __m256d m3   = _mm256_cmp_pd(vq, vQTY_MAX_256, _CMP_LT_OQ);
                        __m256d mask = _mm256_and_pd(_mm256_and_pd(m1, m2), m3);
                        __m256d prod = _mm256_and_pd(_mm256_mul_pd(ve, vd), mask);
                        vacc = _mm256_add_pd(vacc, prod);
                    }
                    {
                        double tmp[4]; _mm256_storeu_pd(tmp, vacc);
                        local_sum += tmp[0] + tmp[1] + tmp[2] + tmp[3];
                    }
#endif
                    // Scalar tail
                    for (; i < row_end; ++i) {
                        double d = disc[i];
                        if (d >= DISC_LO && d <= DISC_HI && qty[i] < QTY_MAX)
                            local_sum += ep[i] * d;
                    }
                } else {
                    // Boundary block (at most 2): per-row shipdate check required.
                    for (size_t i = row_start; i < row_end; ++i) {
                        int32_t s = ship[i];
                        if (s >= DATE_LO && s < DATE_HI) {
                            double d = disc[i];
                            if (d >= DISC_LO && d <= DISC_HI && qty[i] < QTY_MAX)
                                local_sum += ep[i] * d;
                        }
                    }
                }
            }

            global_sum += local_sum;
        }
    }

    // ── Phase 3: Output ───────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        std::ofstream out(out_path);
        if (!out) { std::cerr << "Cannot open " << out_path << std::endl; std::exit(1); }
        out << "revenue\n";
        out << std::fixed;
        out.precision(2);
        out << global_sum << "\n";
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
