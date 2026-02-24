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

    // Prefetch qualifying row range. Combination of:
    //   MADV_SEQUENTIAL  → aggressive kernel readahead for sequential scans
    //   MADV_HUGEPAGE    → use 2MB THP: 512x fewer page faults (~64K→~132 for 263MB)
    //   MADV_WILLNEED    → async I/O kick-off (non-blocking: I/O overlaps with scan)
    //   POSIX_FADV_WILLNEED → belt-and-suspenders for kernel file readahead
    void prefetch_rows(size_t r0, size_t r1) const {
        size_t b0 = r0 * sizeof(T);
        size_t b1 = std::min(r1 * sizeof(T), bytes);
        if (b1 <= b0) return;
        void* ptr = (void*)((const char*)data + b0);
        size_t len = b1 - b0;
        madvise(ptr, len, MADV_SEQUENTIAL);   // large readahead windows
        madvise(ptr, len, MADV_HUGEPAGE);     // 2MB THP: 512x fewer page faults
        madvise(ptr, len, MADV_WILLNEED);     // async prefetch (non-blocking)
        posix_fadvise(fd, (off_t)b0, (off_t)len, POSIX_FADV_WILLNEED);
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

        // Step 4: Selectively prefetch ONLY the qualifying ~15.6% of each column in parallel.
        // l_shipdate is NOT prefetched: interior blocks skip the shipdate check entirely,
        // and boundary blocks (1-2) access only ~100K rows of shipdate (400KB) which faults
        // in quickly via sequential readahead. Saving ~37MB of cold I/O.
        if (first_blk <= last_blk) {
            size_t r0 = (size_t)first_blk * BLOCK_SZ;
            size_t r1 = std::min((size_t)(last_blk + 1) * BLOCK_SZ, c_ship.n);
            #pragma omp parallel sections num_threads(3)
            {
                #pragma omp section
                c_disc.prefetch_rows(r0, r1);
                #pragma omp section
                c_qty .prefetch_rows(r0, r1);
                #pragma omp section
                c_ep  .prefetch_rows(r0, r1);
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

            // Hoist SIMD constants outside block loop — one init per thread
#ifdef __AVX512F__
            const __m512d vDISC_LO_512 = _mm512_set1_pd(DISC_LO);
            const __m512d vDISC_HI_512 = _mm512_set1_pd(DISC_HI);
            const __m512d vQTY_MAX_512 = _mm512_set1_pd(QTY_MAX);
#elif defined(__AVX2__)
            const __m256d vDISC_LO = _mm256_set1_pd(DISC_LO);
            const __m256d vDISC_HI = _mm256_set1_pd(DISC_HI);
            const __m256d vQTY_MAX = _mm256_set1_pd(QTY_MAX);
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
                size_t n_rows    = row_end - row_start;

                // Interior block: date predicate always true → skip shipdate check
                bool interior = (zm_ship.entries[blk].mn >= DATE_LO &&
                                 zm_ship.entries[blk].mx <  DATE_HI);

                if (interior) {
                    size_t i = row_start;

#ifdef __AVX512F__
                    // AVX-512: 4-way unrolled with independent FMA accumulators.
                    // 4 accumulators hide the 4-cycle FMA latency, maximizing throughput.
                    // _mm512_mask3_fmadd_pd(a,b,c,k): where k → c = a*b+c; else c unchanged.
                    // No software prefetch: hardware prefetcher handles sequential streams.
                    __m512d vacc0 = _mm512_setzero_pd();
                    __m512d vacc1 = _mm512_setzero_pd();
                    __m512d vacc2 = _mm512_setzero_pd();
                    __m512d vacc3 = _mm512_setzero_pd();

                    size_t i_end32 = row_start + (n_rows & ~(size_t)31);
                    for (; i < i_end32; i += 32) {
                        __m512d vd0 = _mm512_loadu_pd(disc + i);
                        __m512d vq0 = _mm512_loadu_pd(qty  + i);
                        __m512d ve0 = _mm512_loadu_pd(ep   + i);
                        __mmask8 m0 = _mm512_cmp_pd_mask(vd0, vDISC_LO_512, _CMP_GE_OQ) &
                                      _mm512_cmp_pd_mask(vd0, vDISC_HI_512, _CMP_LE_OQ) &
                                      _mm512_cmp_pd_mask(vq0, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc0 = _mm512_mask3_fmadd_pd(ve0, vd0, vacc0, m0);

                        __m512d vd1 = _mm512_loadu_pd(disc + i + 8);
                        __m512d vq1 = _mm512_loadu_pd(qty  + i + 8);
                        __m512d ve1 = _mm512_loadu_pd(ep   + i + 8);
                        __mmask8 m1 = _mm512_cmp_pd_mask(vd1, vDISC_LO_512, _CMP_GE_OQ) &
                                      _mm512_cmp_pd_mask(vd1, vDISC_HI_512, _CMP_LE_OQ) &
                                      _mm512_cmp_pd_mask(vq1, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc1 = _mm512_mask3_fmadd_pd(ve1, vd1, vacc1, m1);

                        __m512d vd2 = _mm512_loadu_pd(disc + i + 16);
                        __m512d vq2 = _mm512_loadu_pd(qty  + i + 16);
                        __m512d ve2 = _mm512_loadu_pd(ep   + i + 16);
                        __mmask8 m2 = _mm512_cmp_pd_mask(vd2, vDISC_LO_512, _CMP_GE_OQ) &
                                      _mm512_cmp_pd_mask(vd2, vDISC_HI_512, _CMP_LE_OQ) &
                                      _mm512_cmp_pd_mask(vq2, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc2 = _mm512_mask3_fmadd_pd(ve2, vd2, vacc2, m2);

                        __m512d vd3 = _mm512_loadu_pd(disc + i + 24);
                        __m512d vq3 = _mm512_loadu_pd(qty  + i + 24);
                        __m512d ve3 = _mm512_loadu_pd(ep   + i + 24);
                        __mmask8 m3 = _mm512_cmp_pd_mask(vd3, vDISC_LO_512, _CMP_GE_OQ) &
                                      _mm512_cmp_pd_mask(vd3, vDISC_HI_512, _CMP_LE_OQ) &
                                      _mm512_cmp_pd_mask(vq3, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc3 = _mm512_mask3_fmadd_pd(ve3, vd3, vacc3, m3);
                    }
                    // Handle remaining 8-wide chunks
                    size_t i_end8 = row_start + (n_rows & ~(size_t)7);
                    for (; i < i_end8; i += 8) {
                        __m512d vd = _mm512_loadu_pd(disc + i);
                        __m512d vq = _mm512_loadu_pd(qty  + i);
                        __m512d ve = _mm512_loadu_pd(ep   + i);
                        __mmask8 mask =
                            _mm512_cmp_pd_mask(vd, vDISC_LO_512, _CMP_GE_OQ) &
                            _mm512_cmp_pd_mask(vd, vDISC_HI_512, _CMP_LE_OQ) &
                            _mm512_cmp_pd_mask(vq, vQTY_MAX_512, _CMP_LT_OQ);
                        vacc0 = _mm512_mask3_fmadd_pd(ve, vd, vacc0, mask);
                    }
                    // Reduce 4 accumulators into local_sum
                    local_sum += _mm512_reduce_add_pd(
                        _mm512_add_pd(_mm512_add_pd(vacc0, vacc1),
                                      _mm512_add_pd(vacc2, vacc3)));
#elif defined(__AVX2__)
                    // AVX2: 4-way unrolled, 4 doubles per lane, 16 per iteration.
                    // 4 independent accumulators hide FMA latency.
                    __m256d vacc0 = _mm256_setzero_pd();
                    __m256d vacc1 = _mm256_setzero_pd();
                    __m256d vacc2 = _mm256_setzero_pd();
                    __m256d vacc3 = _mm256_setzero_pd();
                    size_t i_end16 = row_start + (n_rows & ~(size_t)15);
                    for (; i < i_end16; i += 16) {
                        auto do_lane = [&](size_t off, __m256d& acc) {
                            __m256d vd = _mm256_loadu_pd(disc + i + off);
                            __m256d vq = _mm256_loadu_pd(qty  + i + off);
                            __m256d ve = _mm256_loadu_pd(ep   + i + off);
                            __m256d mask = _mm256_and_pd(
                                _mm256_and_pd(_mm256_cmp_pd(vd, vDISC_LO, _CMP_GE_OQ),
                                              _mm256_cmp_pd(vd, vDISC_HI, _CMP_LE_OQ)),
                                _mm256_cmp_pd(vq, vQTY_MAX, _CMP_LT_OQ));
                            acc = _mm256_add_pd(acc, _mm256_and_pd(_mm256_mul_pd(ve, vd), mask));
                        };
                        do_lane(0,  vacc0);
                        do_lane(4,  vacc1);
                        do_lane(8,  vacc2);
                        do_lane(12, vacc3);
                    }
                    // Remaining 4-wide chunks
                    size_t i_end4 = row_start + (n_rows & ~(size_t)3);
                    for (; i < i_end4; i += 4) {
                        __m256d vd = _mm256_loadu_pd(disc + i);
                        __m256d vq = _mm256_loadu_pd(qty  + i);
                        __m256d ve = _mm256_loadu_pd(ep   + i);
                        __m256d mask = _mm256_and_pd(
                            _mm256_and_pd(_mm256_cmp_pd(vd, vDISC_LO, _CMP_GE_OQ),
                                          _mm256_cmp_pd(vd, vDISC_HI, _CMP_LE_OQ)),
                            _mm256_cmp_pd(vq, vQTY_MAX, _CMP_LT_OQ));
                        vacc0 = _mm256_add_pd(vacc0, _mm256_and_pd(_mm256_mul_pd(ve, vd), mask));
                    }
                    {
                        __m256d vtot = _mm256_add_pd(_mm256_add_pd(vacc0, vacc1),
                                                     _mm256_add_pd(vacc2, vacc3));
                        double tmp[4];
                        _mm256_storeu_pd(tmp, vtot);
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
                    // Boundary block (1-2 total): per-row shipdate check required
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
