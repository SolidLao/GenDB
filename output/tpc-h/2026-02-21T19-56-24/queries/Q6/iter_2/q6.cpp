#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <algorithm>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// ── Zone-map entry ──────────────────────────────────────────────────────────
struct ZoneEntry { int32_t min_val, max_val; };

// ── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    void*  ptr  = MAP_FAILED;
    size_t size = 0;
    int    fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        // MAP_POPULATE removed: do NOT load entire file eagerly from HDD
        ptr  = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); return false; }
        // Hint kernel to not readahead beyond our selective range
        posix_fadvise(fd, 0, size, POSIX_FADV_RANDOM);
        return true;
    }

    void advise_range(size_t byte_off, size_t byte_len) {
        if (ptr != MAP_FAILED && byte_len > 0) {
            // Sequential readahead within the qualifying range only
            madvise((char*)ptr + byte_off, byte_len, MADV_SEQUENTIAL);
            madvise((char*)ptr + byte_off, byte_len, MADV_WILLNEED);
            posix_fadvise(fd, byte_off, byte_len, POSIX_FADV_WILLNEED);
        }
    }

    ~MmapFile() {
        if (ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

// ── Query runner ─────────────────────────────────────────────────────────────
void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    constexpr int32_t  DATE_LO    = 8766;   // 1994-01-01
    constexpr int32_t  DATE_HI    = 9131;   // 1995-01-01 (exclusive)
    constexpr double   DISC_LO    = 0.05;
    constexpr double   DISC_HI    = 0.07;
    constexpr double   QTY_LO     = 24.0;   // strictly less than
    constexpr uint32_t BLOCK_SIZE = 100000u;
    constexpr uint32_t TOTAL_ROWS = 59986052u;
    constexpr int      NTHREADS   = 64;

    // ── Phase 0: Data loading ─────────────────────────────────────────────
    MmapFile f_shipdate, f_discount, f_quantity, f_extprice;
    uint32_t num_blocks = 0;
    std::vector<ZoneEntry> zones;

    {
        GENDB_PHASE("data_loading");

        // Load zone map (tiny — 4804 bytes)
        {
            std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
            int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
            if (zm_fd < 0) { perror(zm_path.c_str()); return; }
            uint32_t nb = 0;
            if (read(zm_fd, &nb, sizeof(nb)) != sizeof(nb)) { close(zm_fd); return; }
            num_blocks = nb;
            zones.resize(nb);
            if (read(zm_fd, zones.data(), nb * sizeof(ZoneEntry)) != (ssize_t)(nb * sizeof(ZoneEntry))) {
                close(zm_fd); return;
            }
            close(zm_fd);
        }

        // Identify qualifying block range (data sorted → contiguous)
        uint32_t first_qual = num_blocks, last_qual = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val < DATE_LO) continue;
            if (zones[b].min_val >= DATE_HI) break;
            if (first_qual == num_blocks) first_qual = b;
            last_qual = b;
        }

        // mmap the four column files
        f_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        f_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        f_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        f_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        // Prefetch qualifying byte ranges from main thread (hints are async kernel ops).
        // No separate threads needed — avoids thread-create/join overhead on hot path.
        if (first_qual <= last_qual) {
            size_t row_off    = (size_t)first_qual * BLOCK_SIZE;
            uint32_t last_end = std::min((last_qual + 1) * BLOCK_SIZE, TOTAL_ROWS);
            size_t n_rows     = last_end - row_off;

            f_shipdate.advise_range(row_off * sizeof(int32_t), n_rows * sizeof(int32_t));
            f_discount.advise_range(row_off * sizeof(double),  n_rows * sizeof(double));
            f_quantity.advise_range(row_off * sizeof(double),  n_rows * sizeof(double));
            f_extprice.advise_range(row_off * sizeof(double),  n_rows * sizeof(double));
        }
    }

    // Typed pointers
    const int32_t* shipdate  = reinterpret_cast<const int32_t*>(f_shipdate.ptr);
    const double*  discount  = reinterpret_cast<const double*>(f_discount.ptr);
    const double*  quantity  = reinterpret_cast<const double*>(f_quantity.ptr);
    const double*  extprice  = reinterpret_cast<const double*>(f_extprice.ptr);

    // Collect qualifying blocks
    std::vector<uint32_t> qual_blocks;
    qual_blocks.reserve(128);
    for (uint32_t b = 0; b < num_blocks; b++) {
        if (zones[b].max_val < DATE_LO) continue;
        if (zones[b].min_val >= DATE_HI) break;
        qual_blocks.push_back(b);
    }

    // ── Phase 1: Parallel scan (OpenMP morsel-driven) ────────────────────
    double revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        const int nq = (int)qual_blocks.size();

        #pragma omp parallel for schedule(dynamic, 1) reduction(+:revenue) num_threads(NTHREADS)
        for (int bi = 0; bi < nq; bi++) {
            uint32_t b         = qual_blocks[bi];
            uint32_t row_start = b * BLOCK_SIZE;
            uint32_t row_end   = std::min(row_start + BLOCK_SIZE, TOTAL_ROWS);
            uint32_t n         = row_end - row_start;

            // Restrict-qualified pointers into each column at block start.
            // Enables auto-vectorization (compiler sees no aliasing).
            const int32_t* __restrict__ sd = shipdate + row_start;
            const double*  __restrict__ dc = discount  + row_start;
            const double*  __restrict__ qt = quantity  + row_start;
            const double*  __restrict__ ep = extprice  + row_start;

            double block_sum = 0.0;

            // Interior blocks: sorted data guarantees ALL rows pass date filter.
            // Skip the int32 shipdate load entirely → saves 400KB/block bandwidth.
            bool interior = (zones[b].min_val >= DATE_LO && zones[b].max_val < DATE_HI);
            if (interior) {
                // Only discount and quantity filters needed (both double → clean SIMD).
                for (uint32_t r = 0; r < n; r++) {
                    int ok = (dc[r] >= DISC_LO) & (dc[r] <= DISC_HI) & (qt[r] < QTY_LO);
                    block_sum += ok ? (ep[r] * dc[r]) : 0.0;
                }
            } else {
                // Boundary block: need date filter too.
                for (uint32_t r = 0; r < n; r++) {
                    int ok = (sd[r] >= DATE_LO) & (sd[r] < DATE_HI) &
                             (dc[r] >= DISC_LO) & (dc[r] <= DISC_HI) &
                             (qt[r] < QTY_LO);
                    block_sum += ok ? (ep[r] * dc[r]) : 0.0;
                }
            }
            revenue += block_sum;
        }
    }

    // ── Phase 2: Output ──────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "revenue\n%.2f\n", revenue);
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
