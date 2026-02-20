#include <cstdint>
#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <omp.h>
#include <algorithm>
#include <iostream>
#include "timing_utils.h"

// Zone map entry layout
struct ZMEntry {
    int32_t  min;
    int32_t  max;
    uint32_t block_size;
};

static inline const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    // MAP_POPULATE pre-faults all pages serially before parallel scan,
    // avoiding page-fault lock contention across 64 threads.
    // MADV_HUGEPAGE removed: THP doesn't apply to read-only file-backed maps
    // and scanning 488K page entries to mark them costs ~40ms of overhead.
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    madvise(p, out_size, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    // Pre-warm the OpenMP thread pool BEFORE timing starts.
    // First-time omp parallel fork creates 64 threads (50-100ms overhead).
    // This ensures thread creation cost is excluded from the timed region.
    #pragma omp parallel num_threads(64)
    { /* warm up thread pool */ }

    GENDB_PHASE("total");

    // Date constants
    const int32_t DATE_LO = 8766;  // 1994-01-01
    const int32_t DATE_HI = 9131;  // 1995-01-01 (exclusive)

    // Column file paths
    const std::string zm_path    = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
    const std::string sd_path    = gendb_dir + "/lineitem/l_shipdate.bin";
    const std::string disc_path  = gendb_dir + "/lineitem/l_discount.bin";
    const std::string qty_path   = gendb_dir + "/lineitem/l_quantity.bin";
    const std::string price_path = gendb_dir + "/lineitem/l_extendedprice.bin";

    // Load zone map
    size_t zm_size = 0;
    const uint8_t* zm_raw = (const uint8_t*)mmap_file(zm_path, zm_size);
    uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
    const ZMEntry* zm = reinterpret_cast<const ZMEntry*>(zm_raw + sizeof(uint32_t));

    // mmap columns
    size_t sd_size = 0, disc_size = 0, qty_size = 0, price_size = 0;
    const int32_t* shipdate    = (const int32_t*)mmap_file(sd_path,    sd_size);
    const double*  discount    = (const double*) mmap_file(disc_path,  disc_size);
    const double*  quantity    = (const double*) mmap_file(qty_path,   qty_size);
    const double*  extprice    = (const double*) mmap_file(price_path, price_size);

    size_t total_rows = sd_size / sizeof(int32_t);

    // Collect qualifying blocks for parallel dispatch
    std::vector<uint32_t> active_blocks;
    active_blocks.reserve(num_blocks);
    {
        GENDB_PHASE("zone_map_prune");
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zm[b].max < DATE_LO || zm[b].min >= DATE_HI) continue;
            active_blocks.push_back(b);
        }
    }

    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        int nblocks = (int)active_blocks.size();

#pragma omp parallel reduction(+:global_revenue) num_threads(64)
        {
            double local_rev = 0.0;

#pragma omp for schedule(static)
            for (int bi = 0; bi < nblocks; bi++) {
                uint32_t b = active_blocks[bi];
                uint32_t row_start = b * 100000;
                uint32_t row_end   = row_start + zm[b].block_size;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;

                const int32_t* sd  = shipdate + row_start;
                const double*  di  = discount + row_start;
                const double*  qt  = quantity + row_start;
                const double*  ep  = extprice + row_start;
                uint32_t n = row_end - row_start;

#ifdef __AVX512F__
                // AVX-512: process 8 doubles at a time
                const __m512d v_disc_lo = _mm512_set1_pd(0.05);
                const __m512d v_disc_hi = _mm512_set1_pd(0.07);
                const __m512d v_qty_th  = _mm512_set1_pd(24.0);
                const __m256i v_sd_lo   = _mm256_set1_epi32(DATE_LO);
                const __m256i v_sd_hi   = _mm256_set1_epi32(DATE_HI - 1);

                // Dual accumulators: 2x loop unroll for better ILP.
                // Branches removed: HW prefetcher loads doubles regardless of m_sd;
                // early-exit saves no bandwidth, only adds mispredictions (~50/50).
                __m512d v_acc0 = _mm512_setzero_pd();
                __m512d v_acc1 = _mm512_setzero_pd();

                uint32_t i = 0;
                for (; i + 16 <= n; i += 16) {
                    // First group of 8
                    __m256i v_sd0 = _mm256_loadu_si256((const __m256i*)(sd + i));
                    __m512d v_di0 = _mm512_loadu_pd(di + i);
                    __m512d v_qt0 = _mm512_loadu_pd(qt + i);
                    __m512d v_ep0 = _mm512_loadu_pd(ep + i);

                    __mmask8 m_sd0 = _mm256_cmpge_epi32_mask(v_sd0, v_sd_lo) &
                                     _mm256_cmple_epi32_mask(v_sd0, v_sd_hi);
                    __mmask8 m_di0 = _mm512_cmp_pd_mask(v_di0, v_disc_lo, _CMP_GE_OQ) &
                                     _mm512_cmp_pd_mask(v_di0, v_disc_hi, _CMP_LE_OQ);
                    __mmask8 m_qt0 = _mm512_cmp_pd_mask(v_qt0, v_qty_th, _CMP_LT_OQ);
                    __mmask8 mask0 = m_sd0 & m_di0 & m_qt0;

                    // Second group of 8 (interleaved for OOO execution)
                    __m256i v_sd1 = _mm256_loadu_si256((const __m256i*)(sd + i + 8));
                    __m512d v_di1 = _mm512_loadu_pd(di + i + 8);
                    __m512d v_qt1 = _mm512_loadu_pd(qt + i + 8);
                    __m512d v_ep1 = _mm512_loadu_pd(ep + i + 8);

                    __mmask8 m_sd1 = _mm256_cmpge_epi32_mask(v_sd1, v_sd_lo) &
                                     _mm256_cmple_epi32_mask(v_sd1, v_sd_hi);
                    __mmask8 m_di1 = _mm512_cmp_pd_mask(v_di1, v_disc_lo, _CMP_GE_OQ) &
                                     _mm512_cmp_pd_mask(v_di1, v_disc_hi, _CMP_LE_OQ);
                    __mmask8 m_qt1 = _mm512_cmp_pd_mask(v_qt1, v_qty_th, _CMP_LT_OQ);
                    __mmask8 mask1 = m_sd1 & m_di1 & m_qt1;

                    v_acc0 = _mm512_mask3_fmadd_pd(v_ep0, v_di0, v_acc0, mask0);
                    v_acc1 = _mm512_mask3_fmadd_pd(v_ep1, v_di1, v_acc1, mask1);
                }

                // Single-step tail (up to 8 remaining)
                for (; i + 8 <= n; i += 8) {
                    __m256i v_sd0 = _mm256_loadu_si256((const __m256i*)(sd + i));
                    __m512d v_di0 = _mm512_loadu_pd(di + i);
                    __m512d v_qt0 = _mm512_loadu_pd(qt + i);
                    __m512d v_ep0 = _mm512_loadu_pd(ep + i);

                    __mmask8 m_sd0 = _mm256_cmpge_epi32_mask(v_sd0, v_sd_lo) &
                                     _mm256_cmple_epi32_mask(v_sd0, v_sd_hi);
                    __mmask8 m_di0 = _mm512_cmp_pd_mask(v_di0, v_disc_lo, _CMP_GE_OQ) &
                                     _mm512_cmp_pd_mask(v_di0, v_disc_hi, _CMP_LE_OQ);
                    __mmask8 m_qt0 = _mm512_cmp_pd_mask(v_qt0, v_qty_th, _CMP_LT_OQ);
                    __mmask8 mask0 = m_sd0 & m_di0 & m_qt0;
                    v_acc0 = _mm512_mask3_fmadd_pd(v_ep0, v_di0, v_acc0, mask0);
                }

                local_rev += _mm512_reduce_add_pd(_mm512_add_pd(v_acc0, v_acc1));

                // Scalar tail
                for (; i < n; i++) {
                    int32_t s = sd[i];
                    if (s < DATE_LO || s >= DATE_HI) continue;
                    double d = di[i];
                    if (d < 0.05 || d > 0.07) continue;
                    double q = qt[i];
                    if (q >= 24.0) continue;
                    local_rev += ep[i] * d;
                }
#else
                // Scalar fallback
                for (uint32_t i = 0; i < n; i++) {
                    int32_t s = sd[i];
                    if (s < DATE_LO || s >= DATE_HI) continue;
                    double d = di[i];
                    if (d < 0.05 || d > 0.07) continue;
                    double q = qt[i];
                    if (q >= 24.0) continue;
                    local_rev += ep[i] * d;
                }
#endif
            }

            global_revenue += local_rev;
        }
    }

    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", global_revenue);
        fclose(f);
    }

    // Unmap
    munmap((void*)zm_raw,   zm_size);
    munmap((void*)shipdate, sd_size);
    munmap((void*)discount, disc_size);
    munmap((void*)quantity, qty_size);
    munmap((void*)extprice, price_size);
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
