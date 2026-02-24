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
        data  = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); std::exit(1); }
        posix_fadvise(fd, 0, (off_t)bytes, POSIX_FADV_SEQUENTIAL);
        n = bytes / sizeof(T);
    }

    void prefetch_rows(size_t r0, size_t r1) const {
        size_t b0 = r0 * sizeof(T);
        size_t b1 = std::min(r1 * sizeof(T), bytes);
        if (b1 > b0)
            madvise((void*)((const char*)data + b0), b1 - b0, MADV_WILLNEED);
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

    {
        GENDB_PHASE("data_loading");

        // Zone maps first (tiny; needed for block range determination)
        zm_ship.open(idx + "lineitem_shipdate_zonemap.bin");
        zm_disc.open(idx + "lineitem_discount_zonemap.bin");
        zm_qty .open(idx + "lineitem_quantity_zonemap.bin");

        // Open column files (mmap + MAP_POPULATE; posix_fadvise sequential)
        c_ship.open(base + "l_shipdate.bin");
        c_disc.open(base + "l_discount.bin");
        c_qty .open(base + "l_quantity.bin");
        c_ep  .open(base + "l_extendedprice.bin");

        // Binary search on shipdate zone map to find qualifying block range
        // first_block: first b where zm_ship.entries[b].mx >= DATE_LO
        uint32_t nb = zm_ship.num_blocks;
        uint32_t first_blk = nb, last_blk = 0;

        {
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mx >= DATE_LO) hi = mid;
                else                                    lo = mid + 1;
            }
            first_blk = lo;
        }
        {
            // last_blk: last b where zm_ship.entries[b].mn < DATE_HI
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mn < DATE_HI) lo = mid + 1;
                else                                   hi = mid;
            }
            last_blk = (lo > 0) ? lo - 1 : 0;
        }

        // Selective prefetch: only qualifying block ranges
        if (first_blk <= last_blk) {
            size_t r0 = (size_t)first_blk * BLOCK_SZ;
            size_t r1 = std::min((size_t)(last_blk + 1) * BLOCK_SZ, c_ship.n);
            #pragma omp parallel sections num_threads(4)
            {
                #pragma omp section
                c_ship.prefetch_rows(r0, r1);
                #pragma omp section
                c_disc.prefetch_rows(r0, r1);
                #pragma omp section
                c_qty .prefetch_rows(r0, r1);
                #pragma omp section
                c_ep  .prefetch_rows(r0, r1);
            }
        }

        // Store for use in scan (re-derive inside scan_phase below)
        // We'll re-derive from zone map inside the scan phase
        (void)first_blk; (void)last_blk;
    }

    // ── Phase 1: Determine qualifying block range ─────────────────────────────
    uint32_t nb = zm_ship.num_blocks;
    uint32_t first_blk, last_blk;
    {
        {
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mx >= DATE_LO) hi = mid;
                else                                    lo = mid + 1;
            }
            first_blk = lo;
        }
        {
            uint32_t lo = 0, hi = nb;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zm_ship.entries[mid].mn < DATE_HI) lo = mid + 1;
                else                                   hi = mid;
            }
            last_blk = (lo > 0) ? lo - 1 : 0;
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

            #pragma omp for schedule(dynamic, 1)
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
                    // Interior block: skip shipdate check; discount + quantity + ep multiply
                    size_t i = row_start;

#ifdef __AVX2__
                    // AVX2: process 4 doubles at a time
                    const __m256d vDISC_LO = _mm256_set1_pd(DISC_LO);
                    const __m256d vDISC_HI = _mm256_set1_pd(DISC_HI);
                    const __m256d vQTY_MAX = _mm256_set1_pd(QTY_MAX);
                    __m256d vacc = _mm256_setzero_pd();

                    size_t i_end4 = row_start + ((row_end - row_start) & ~(size_t)3);
                    for (; i < i_end4; i += 4) {
                        __m256d vd = _mm256_loadu_pd(disc + i);
                        __m256d vq = _mm256_loadu_pd(qty  + i);
                        __m256d ve = _mm256_loadu_pd(ep   + i);

                        // disc mask: DISC_LO <= d <= DISC_HI
                        __m256d m1 = _mm256_cmp_pd(vd, vDISC_LO, _CMP_GE_OQ);
                        __m256d m2 = _mm256_cmp_pd(vd, vDISC_HI, _CMP_LE_OQ);
                        // qty mask:  q < QTY_MAX
                        __m256d m3 = _mm256_cmp_pd(vq, vQTY_MAX, _CMP_LT_OQ);

                        __m256d mask = _mm256_and_pd(_mm256_and_pd(m1, m2), m3);

                        // product = ep * disc; zero out non-qualifying lanes
                        __m256d prod = _mm256_mul_pd(ve, vd);
                        __m256d contrib = _mm256_and_pd(prod, mask);
                        vacc = _mm256_add_pd(vacc, contrib);
                    }

                    // horizontal sum of vacc
                    {
                        double tmp[4];
                        _mm256_storeu_pd(tmp, vacc);
                        local_sum += tmp[0] + tmp[1] + tmp[2] + tmp[3];
                    }
// AVX2 done
#endif
                    // Scalar tail (or full scalar if no AVX2)
                    for (; i < row_end; ++i) {
                        double d = disc[i];
                        if (d >= DISC_LO && d <= DISC_HI && qty[i] < QTY_MAX) {
                            local_sum += ep[i] * d;
                        }
                    }
                } else {
                    // Boundary block: per-row shipdate check required
                    size_t i = row_start;

#ifdef __AVX2__
                    const __m256d vDISC_LO = _mm256_set1_pd(DISC_LO);
                    const __m256d vDISC_HI = _mm256_set1_pd(DISC_HI);
                    const __m256d vQTY_MAX = _mm256_set1_pd(QTY_MAX);

                    // Boundary blocks are few (1-2 total); scalar is fine
                    (void)vDISC_LO; (void)vDISC_HI; (void)vQTY_MAX;
#endif
                    for (; i < row_end; ++i) {
                        int32_t s = ship[i];
                        if (s >= DATE_LO && s < DATE_HI) {
                            double d = disc[i];
                            if (d >= DISC_LO && d <= DISC_HI && qty[i] < QTY_MAX) {
                                local_sum += ep[i] * d;
                            }
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
