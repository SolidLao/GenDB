#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <thread>
#include <vector>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// ── Zone-map entry ──────────────────────────────────────────────────────────
struct ZoneEntry { int32_t min_val, max_val; };

// ── pread a column's qualifying range into an aligned buffer ─────────────────
static void pread_range(const char* path, void* buf, off_t byte_off, size_t byte_len) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return; }
    posix_fadvise(fd, byte_off, byte_len, POSIX_FADV_SEQUENTIAL);
    char* dst = static_cast<char*>(buf);
    size_t rem = byte_len;
    off_t  off = byte_off;
    while (rem > 0) {
        ssize_t nr = pread(fd, dst, rem, off);
        if (nr <= 0) break;
        dst += nr; off += nr; rem -= nr;
    }
    close(fd);
}

// ── Query runner ─────────────────────────────────────────────────────────────
void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    constexpr int32_t  DATE_LO    = 8766;    // 1994-01-01
    constexpr int32_t  DATE_HI    = 9131;    // 1995-01-01 (exclusive)
    constexpr double   DISC_LO    = 0.05;
    constexpr double   DISC_HI    = 0.07;
    constexpr double   QTY_LO     = 24.0;
    constexpr uint32_t BLOCK_SIZE = 100000u;
    constexpr uint32_t TOTAL_ROWS = 59986052u;
    constexpr int      NTHREADS   = 64;

    // ── Phase 0: Data loading ─────────────────────────────────────────────
    // Strategy: pread qualifying range into aligned buffers.
    // Eliminates page-fault overhead (~35ms) during main_scan by
    // bringing all data into user-space memory before the scan begins.
    int32_t* shipdate_buf = nullptr;
    double*  discount_buf = nullptr;
    double*  quantity_buf = nullptr;
    double*  extprice_buf = nullptr;
    size_t   n_rows       = 0;
    size_t   row_off      = 0;

    {
        GENDB_PHASE("data_loading");

        // 1. Load zone map (4804 bytes → fits in L1)
        uint32_t num_blocks = 0;
        std::vector<ZoneEntry> zones;
        {
            std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
            int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
            if (zm_fd < 0) { perror(zm_path.c_str()); return; }
            if (::read(zm_fd, &num_blocks, sizeof(num_blocks)) != sizeof(num_blocks)) { close(zm_fd); return; }
            zones.resize(num_blocks);
            if (::read(zm_fd, zones.data(), num_blocks * sizeof(ZoneEntry))
                    != (ssize_t)(num_blocks * sizeof(ZoneEntry))) { close(zm_fd); return; }
            close(zm_fd);
        }

        // 2. Find qualifying block range (data sorted → contiguous run)
        uint32_t first_qual = num_blocks, last_qual = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val < DATE_LO) continue;
            if (zones[b].min_val >= DATE_HI) break;
            if (first_qual == num_blocks) first_qual = b;
            last_qual = b;
        }
        if (first_qual == num_blocks) {
            // No qualifying blocks — output zero
            std::string out = results_dir + "/Q6.csv";
            FILE* f = fopen(out.c_str(), "w");
            if (f) { fprintf(f, "revenue\n0.00\n"); fclose(f); }
            return;
        }

        row_off        = (size_t)first_qual * BLOCK_SIZE;
        size_t row_end = std::min((size_t)(last_qual + 1) * BLOCK_SIZE, (size_t)TOTAL_ROWS);
        n_rows         = row_end - row_off;

        // 3. Allocate 64-byte aligned buffers (AVX-512 friendly)
        auto alloc64 = [](size_t bytes) -> void* {
            size_t aligned = (bytes + 63) & ~(size_t)63;
            return aligned_alloc(64, aligned);
        };
        shipdate_buf = static_cast<int32_t*>(alloc64(n_rows * sizeof(int32_t)));
        discount_buf = static_cast<double*> (alloc64(n_rows * sizeof(double)));
        quantity_buf = static_cast<double*> (alloc64(n_rows * sizeof(double)));
        extprice_buf = static_cast<double*> (alloc64(n_rows * sizeof(double)));

        // 4. pread qualifying ranges in parallel across 4 threads.
        //    pread from warm page-cache is memcpy-speed — no page faults in scan.
        std::string sd_path = gendb_dir + "/lineitem/l_shipdate.bin";
        std::string dc_path = gendb_dir + "/lineitem/l_discount.bin";
        std::string qt_path = gendb_dir + "/lineitem/l_quantity.bin";
        std::string ep_path = gendb_dir + "/lineitem/l_extendedprice.bin";

        std::thread t1([&]{ pread_range(sd_path.c_str(), shipdate_buf,
                                (off_t)(row_off * sizeof(int32_t)), n_rows * sizeof(int32_t)); });
        std::thread t2([&]{ pread_range(dc_path.c_str(), discount_buf,
                                (off_t)(row_off * sizeof(double)),  n_rows * sizeof(double)); });
        std::thread t3([&]{ pread_range(qt_path.c_str(), quantity_buf,
                                (off_t)(row_off * sizeof(double)),  n_rows * sizeof(double)); });
        std::thread t4([&]{ pread_range(ep_path.c_str(), extprice_buf,
                                (off_t)(row_off * sizeof(double)),  n_rows * sizeof(double)); });
        t1.join(); t2.join(); t3.join(); t4.join();
    }

    // ── Phase 1: Parallel vectorized scan ────────────────────────────────
    // Branchless inner loop enables auto-vectorization (AVX-512).
    // All data is already in user-space buffers — zero page faults.
    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        const int32_t* __restrict__ sd_col = shipdate_buf;
        const double*  __restrict__ dc_col = discount_buf;
        const double*  __restrict__ qt_col = quantity_buf;
        const double*  __restrict__ ep_col = extprice_buf;

        // row_off: offset within qualifying range where DATE filter boundary may
        // require per-element shipdate check (boundary blocks only; interior blocks
        // have all rows in range, but we always check for correctness).
        constexpr uint32_t DATE_SPAN = (uint32_t)(DATE_HI - DATE_LO);

        #pragma omp parallel for num_threads(NTHREADS) reduction(+:revenue) schedule(static)
        for (size_t r = 0; r < n_rows; r++) {
            int32_t sd = sd_col[r];
            double  dc = dc_col[r];
            double  qt = qt_col[r];
            double  ep = ep_col[r];

            // Branchless predicate — all conditions as integer mask
            // Unsigned range trick: (sd - DATE_LO) < DATE_SPAN == sd in [DATE_LO, DATE_HI)
            int pass = ((uint32_t)(sd - DATE_LO) < DATE_SPAN)
                     & (dc >= DISC_LO) & (dc <= DISC_HI)
                     & (qt < QTY_LO);

            revenue += pass ? (ep * dc) : 0.0;
        }
    }

    free(shipdate_buf);
    free(discount_buf);
    free(quantity_buf);
    free(extprice_buf);

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
