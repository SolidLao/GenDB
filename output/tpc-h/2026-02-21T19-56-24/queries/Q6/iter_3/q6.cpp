#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include "timing_utils.h"

// ── Zone-map entry ──────────────────────────────────────────────────────────
struct ZoneEntry { int32_t min_val, max_val; };

// ── Cache-line-aligned double: prevents false sharing across threads ─────────
struct alignas(64) PaddedDouble { double val = 0.0; };

// ── Sub-range mmap with MAP_POPULATE ────────────────────────────────────────
// Maps ONLY the qualifying row range [row_start, row_start+n_rows) from a
// column file. MAP_POPULATE faults in every page synchronously, so the compute
// phase runs with zero page faults (pure L3/DRAM bandwidth, no fault overhead).
struct SubrangeMmap {
    int    fd        = -1;
    void*  base_ptr  = MAP_FAILED;
    size_t mmap_size = 0;
    size_t page_extra = 0;   // alignment padding at start of mapping

    bool load(const std::string& path, size_t row_start,
              size_t n_rows, size_t elem_size) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        size_t byte_off     = row_start * elem_size;
        constexpr size_t PAGE = 4096;
        size_t aligned_off  = (byte_off / PAGE) * PAGE;
        page_extra          = byte_off - aligned_off;
        mmap_size           = n_rows * elem_size + page_extra;
        // MAP_POPULATE: block until all pages are resident — eliminates page
        // faults during the parallel scan phase entirely.
        base_ptr = mmap(nullptr, mmap_size, PROT_READ,
                        MAP_PRIVATE | MAP_POPULATE, fd, (off_t)aligned_off);
        if (base_ptr == MAP_FAILED) { perror("mmap"); return false; }
        return true;
    }

    // Returns pointer to element at index 0 relative to row_start.
    // Access row r (absolute) as: data<T>()[r - row_start_global]
    template<typename T>
    const T* data() const {
        return reinterpret_cast<const T*>((char*)base_ptr + page_extra);
    }

    ~SubrangeMmap() {
        if (base_ptr != MAP_FAILED) munmap(base_ptr, mmap_size);
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
    SubrangeMmap f_shipdate, f_discount, f_quantity, f_extprice;
    uint32_t num_blocks = 0;
    std::vector<ZoneEntry> zones;
    uint32_t first_qual = UINT32_MAX, last_qual = 0;
    uint32_t row_start_global = 0, row_end_global = 0;

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

        // Identify qualifying block range (data sorted → contiguous, early exit)
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val < DATE_LO) continue;
            if (zones[b].min_val >= DATE_HI) break;
            if (first_qual == UINT32_MAX) first_qual = b;
            last_qual = b;
        }
        if (first_qual == UINT32_MAX) return;   // no qualifying blocks

        row_start_global = first_qual * BLOCK_SIZE;
        row_end_global   = std::min((last_qual + 1) * BLOCK_SIZE, TOTAL_ROWS);
        uint32_t n_rows  = row_end_global - row_start_global;

        // Sub-range MAP_POPULATE: map ONLY qualifying rows in 4 parallel threads.
        // MAP_POPULATE faults all pages in synchronously → zero page faults in
        // main_scan, which then runs as pure compute (no OS overhead).
        std::string sd_path = gendb_dir + "/lineitem/l_shipdate.bin";
        std::string dc_path = gendb_dir + "/lineitem/l_discount.bin";
        std::string qt_path = gendb_dir + "/lineitem/l_quantity.bin";
        std::string ep_path = gendb_dir + "/lineitem/l_extendedprice.bin";

        std::thread t1([&]{ f_shipdate.load(sd_path, row_start_global, n_rows, sizeof(int32_t)); });
        std::thread t2([&]{ f_discount.load(dc_path, row_start_global, n_rows, sizeof(double));  });
        std::thread t3([&]{ f_quantity.load(qt_path, row_start_global, n_rows, sizeof(double));  });
        std::thread t4([&]{ f_extprice.load(ep_path, row_start_global, n_rows, sizeof(double));  });
        t1.join(); t2.join(); t3.join(); t4.join();
    }

    // Typed pointers — index as ptr[r - row_start_global] for row r
    const int32_t* shipdate = f_shipdate.data<int32_t>();
    const double*  discount = f_discount.data<double>();
    const double*  quantity = f_quantity.data<double>();
    const double*  extprice = f_extprice.data<double>();

    // Collect qualifying blocks
    std::vector<uint32_t> qual_blocks;
    qual_blocks.reserve(128);
    for (uint32_t b = first_qual; b <= last_qual; b++) {
        if (zones[b].max_val >= DATE_LO && zones[b].min_val < DATE_HI)
            qual_blocks.push_back(b);
    }

    // ── Phase 1: Parallel scan (morsel-driven) ───────────────────────────
    // PaddedDouble: each thread writes to a separate cache line → no false sharing
    PaddedDouble partial[NTHREADS];
    std::atomic<uint32_t> block_idx{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            double local_sum = 0.0;
            while (true) {
                uint32_t bi = block_idx.fetch_add(1, std::memory_order_relaxed);
                if (bi >= (uint32_t)qual_blocks.size()) break;
                uint32_t b = qual_blocks[bi];
                uint32_t row_s = b * BLOCK_SIZE;
                uint32_t row_e = std::min(row_s + BLOCK_SIZE, TOTAL_ROWS);

                // Relative indices into sub-range mmap arrays
                uint32_t rel_s = row_s - row_start_global;
                uint32_t rel_e = row_e - row_start_global;

                // For fully-interior blocks (all rows in date range), skip
                // shipdate check → avoids loading the int32 column per row,
                // reducing memory bandwidth by ~12%.
                const bool full_block =
                    (zones[b].min_val >= DATE_LO && zones[b].max_val < DATE_HI);

                if (full_block) {
                    for (uint32_t r = rel_s; r < rel_e; r++) {
                        double dc = discount[r];
                        if (dc < DISC_LO || dc > DISC_HI) continue;
                        double qt = quantity[r];
                        if (qt >= QTY_LO) continue;
                        local_sum += extprice[r] * dc;
                    }
                } else {
                    for (uint32_t r = rel_s; r < rel_e; r++) {
                        int32_t sd = shipdate[r];
                        if (sd < DATE_LO || sd >= DATE_HI) continue;
                        double dc = discount[r];
                        if (dc < DISC_LO || dc > DISC_HI) continue;
                        double qt = quantity[r];
                        if (qt >= QTY_LO) continue;
                        local_sum += extprice[r] * dc;
                    }
                }
            }
            partial[tid].val = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();
    }

    // Final reduction
    double revenue = 0.0;
    for (int t = 0; t < NTHREADS; t++)
        revenue += partial[t].val;

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
