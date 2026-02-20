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
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
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

#pragma omp for schedule(dynamic, 4)
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

                __m512d v_acc = _mm512_setzero_pd();

                uint32_t i = 0;
                for (; i + 8 <= n; i += 8) {
                    // Load 8 int32 shipdates
                    __m256i v_sd = _mm256_loadu_si256((const __m256i*)(sd + i));
                    // Compare: sd >= DATE_LO && sd <= DATE_HI-1
                    __mmask8 m_sd = _mm256_cmpge_epi32_mask(v_sd, v_sd_lo) &
                                    _mm256_cmple_epi32_mask(v_sd, v_sd_hi);
                    if (m_sd == 0) continue;

                    // Load discount, quantity, extprice
                    __m512d v_di = _mm512_loadu_pd(di + i);
                    __m512d v_qt = _mm512_loadu_pd(qt + i);
                    __m512d v_ep = _mm512_loadu_pd(ep + i);

                    // Discount filter: disc >= 0.05 && disc <= 0.07
                    __mmask8 m_di = _mm512_cmp_pd_mask(v_di, v_disc_lo, _CMP_GE_OQ) &
                                    _mm512_cmp_pd_mask(v_di, v_disc_hi, _CMP_LE_OQ);
                    // Quantity filter: qty < 24.0
                    __mmask8 m_qt = _mm512_cmp_pd_mask(v_qt, v_qty_th, _CMP_LT_OQ);

                    __mmask8 mask = m_sd & m_di & m_qt;
                    if (mask == 0) continue;

                    // Masked multiply-add: ep * di where mask is set
                    __m512d v_prod = _mm512_mul_pd(v_ep, v_di);
                    v_acc = _mm512_mask_add_pd(v_acc, mask, v_acc, v_prod);
                }

                local_rev += _mm512_reduce_add_pd(v_acc);

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
