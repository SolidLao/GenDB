#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include "timing_utils.h"

// ── column mmap helper ──────────────────────────────────────────────────────
struct MmapCol {
    void*  ptr  = nullptr;
    size_t size = 0;
    int    fd   = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st);
        size = st.st_size;
        ptr  = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); std::exit(1); }
        posix_fadvise(fd, 0, size, POSIX_FADV_SEQUENTIAL);
    }
    ~MmapCol() {
        if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

// ── zone-map block descriptor ───────────────────────────────────────────────
struct ZMBlock {
    int32_t  mn;
    int32_t  mx;
    uint32_t nr;
};

// ── cache-line-aligned thread-local accumulator ─────────────────────────────
struct alignas(64) ThreadAcc {
    double val = 0.0;
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const int32_t LO = 8766;   // 1994-01-01
    const int32_t HI = 9131;   // 1995-01-01

    // ── Phase 1: load zone map and build qualifying block list ───────────────
    std::vector<std::pair<uint32_t,uint32_t>> qual_blocks; // (row_start, row_end)
    {
        GENDB_PHASE("dim_filter");

        MmapCol zm;
        zm.open(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin");

        const uint8_t* zm_ptr = reinterpret_cast<const uint8_t*>(zm.ptr);
        uint32_t nb = *reinterpret_cast<const uint32_t*>(zm_ptr);
        const ZMBlock* blocks = reinterpret_cast<const ZMBlock*>(zm_ptr + 4);

        qual_blocks.reserve(128);
        for (uint32_t b = 0; b < nb; b++) {
            if (blocks[b].mx < LO) continue;
            if (blocks[b].mn >= HI) break;  // sorted → stop early
            uint32_t row_start = b * 100000u;
            uint32_t row_end   = row_start + blocks[b].nr;
            qual_blocks.emplace_back(row_start, row_end);
        }
    }

    // ── Phase 2: mmap columns ────────────────────────────────────────────────
    MmapCol col_shipdate, col_discount, col_quantity, col_extprice;
    {
        GENDB_PHASE("build_joins");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    }

    const int32_t* __restrict__ shipdate =
        reinterpret_cast<const int32_t*>(col_shipdate.ptr);
    const double*  __restrict__ discount =
        reinterpret_cast<const double*>(col_discount.ptr);
    const double*  __restrict__ quantity =
        reinterpret_cast<const double*>(col_quantity.ptr);
    const double*  __restrict__ extprice =
        reinterpret_cast<const double*>(col_extprice.ptr);

    // ── Phase 3: parallel morsel scan ───────────────────────────────────────
    double global_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        const int nthreads = 64;
        ThreadAcc thread_acc[nthreads];
        std::atomic<uint32_t> morsel_idx{0};
        const uint32_t nblocks = static_cast<uint32_t>(qual_blocks.size());

        auto worker = [&](int tid) {
            double local = 0.0;
            uint32_t b;
            while ((b = morsel_idx.fetch_add(1, std::memory_order_relaxed)) < nblocks) {
                auto [row_start, row_end] = qual_blocks[b];
                const int32_t* __restrict__ sd = shipdate + row_start;
                const double*  __restrict__ di = discount  + row_start;
                const double*  __restrict__ qt = quantity  + row_start;
                const double*  __restrict__ ep = extprice  + row_start;
                uint32_t n = row_end - row_start;

                #pragma omp simd reduction(+:local)
                for (uint32_t i = 0; i < n; i++) {
                    int32_t s = sd[i];
                    double  d = di[i];
                    double  q = qt[i];
                    if (s >= LO && s < HI &&
                        d >= 0.04999 && d <= 0.07001 &&
                        q < 24.0) {
                        local += ep[i] * d;
                    }
                }
            }
            thread_acc[tid].val = local;
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();

        for (int t = 0; t < nthreads; t++)
            global_revenue += thread_acc[t].val;
    }

    // ── Phase 4: output ──────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); std::exit(1); }
        fprintf(f, "revenue\n%.2f\n", global_revenue);
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
