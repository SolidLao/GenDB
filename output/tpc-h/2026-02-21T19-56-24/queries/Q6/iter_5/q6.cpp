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

// ── Range-only mmap: maps and pre-faults ONLY qualifying byte range ──────────
// MAP_POPULATE is synchronous — blocks until all pages are resident.
// This avoids the async-WILLNEED race where the scan begins before pages arrive.
struct RangeMmap {
    void*  base      = MAP_FAILED;
    size_t map_size  = 0;
    size_t row_delta = 0;   // byte offset from base to first qualifying row

    // Maps file[fd] rows [row_first, row_last) with MAP_POPULATE (synchronous pre-fault).
    template<typename T>
    bool open(int fd, size_t row_first, size_t row_last) {
        constexpr size_t PAGE = 4096;
        size_t byte_first = row_first * sizeof(T);
        size_t byte_last  = row_last  * sizeof(T);
        size_t aligned_off = byte_first & ~(PAGE - 1);
        size_t aligned_end = (byte_last + PAGE - 1) & ~(PAGE - 1);
        map_size  = aligned_end - aligned_off;
        row_delta = byte_first - aligned_off;
        // MAP_POPULATE: kernel synchronously faults in all pages before returning.
        // With warm page cache this is a fast page-table population (no disk I/O).
        base = mmap(nullptr, map_size, PROT_READ,
                    MAP_PRIVATE | MAP_POPULATE, fd, (off_t)aligned_off);
        return base != MAP_FAILED;
    }

    template<typename T>
    const T* data() const {
        return reinterpret_cast<const T*>(static_cast<const char*>(base) + row_delta);
    }

    ~RangeMmap() {
        if (base != MAP_FAILED) munmap(base, map_size);
    }
};

// ── Query runner ─────────────────────────────────────────────────────────────
void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    constexpr int32_t  DATE_LO    = 8766;    // 1994-01-01
    constexpr int32_t  DATE_HI    = 9131;    // 1995-01-01 (exclusive)
    constexpr double   DISC_LO    = 0.05;
    constexpr double   DISC_HI    = 0.07;
    constexpr double   QTY_MAX    = 24.0;
    constexpr uint32_t BLOCK_SIZE = 100000u;
    constexpr uint32_t TOTAL_ROWS = 59986052u;
    constexpr int      NTHREADS   = 64;

    // ── data_loading ──────────────────────────────────────────────────────
    uint32_t num_blocks = 0;
    std::vector<ZoneEntry> zones;
    uint32_t first_qual = 0, last_qual = 0;

    RangeMmap m_sd, m_dc, m_qty, m_ext;

    {
        GENDB_PHASE("data_loading");

        // 1. Load zone map (4804 bytes — fits entirely in L1 cache)
        {
            std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
            int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
            if (zm_fd < 0) { perror(zm_path.c_str()); return; }
            if (::read(zm_fd, &num_blocks, sizeof(num_blocks)) != sizeof(num_blocks)) {
                close(zm_fd); return;
            }
            zones.resize(num_blocks);
            if (::read(zm_fd, zones.data(), num_blocks * sizeof(ZoneEntry))
                    != (ssize_t)(num_blocks * sizeof(ZoneEntry))) {
                close(zm_fd); return;
            }
            close(zm_fd);
        }

        // 2. Find qualifying block range (data sorted ascending → contiguous range)
        first_qual = num_blocks;
        last_qual  = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val < DATE_LO) continue;
            if (zones[b].min_val >= DATE_HI) break;
            if (first_qual == num_blocks) first_qual = b;
            last_qual = b;
        }
        if (first_qual >= num_blocks) {
            // No qualifying blocks — output zero
            std::string out_path = results_dir + "/Q6.csv";
            FILE* f = fopen(out_path.c_str(), "w");
            if (f) { fprintf(f, "revenue\n0.00\n"); fclose(f); }
            return;
        }

        // 3. Qualifying row range
        size_t row_first = (size_t)first_qual * BLOCK_SIZE;
        size_t row_last  = std::min((size_t)(last_qual + 1) * BLOCK_SIZE,
                                    (size_t)TOTAL_ROWS);

        // 4. Open 4 column file descriptors
        int fd_sd  = ::open((gendb_dir + "/lineitem/l_shipdate.bin").c_str(),     O_RDONLY);
        int fd_dc  = ::open((gendb_dir + "/lineitem/l_discount.bin").c_str(),     O_RDONLY);
        int fd_qty = ::open((gendb_dir + "/lineitem/l_quantity.bin").c_str(),     O_RDONLY);
        int fd_ext = ::open((gendb_dir + "/lineitem/l_extendedprice.bin").c_str(), O_RDONLY);

        // 5. MAP_POPULATE qualifying range of all 4 columns in parallel.
        //    MAP_POPULATE is synchronous: when mmap() returns, all pages are resident.
        //    4 threads → parallel page-table population across 4 files.
        //    Total I/O: ~35MB (shipdate) + 3×~70MB (doubles) ≈ 245MB  (vs 1.9GB full load)
        std::thread t1([&]{ m_sd.open<int32_t>(fd_sd,  row_first, row_last); });
        std::thread t2([&]{ m_dc.open<double> (fd_dc,  row_first, row_last); });
        std::thread t3([&]{ m_qty.open<double>(fd_qty, row_first, row_last); });
        std::thread t4([&]{ m_ext.open<double>(fd_ext, row_first, row_last); });
        t1.join(); t2.join(); t3.join(); t4.join();

        close(fd_sd); close(fd_dc); close(fd_qty); close(fd_ext);
    }

    // Typed pointers — index [0] == row (first_qual * BLOCK_SIZE)
    // Access row r via ptr[r - first_qual*BLOCK_SIZE]
    const size_t row_base = (size_t)first_qual * BLOCK_SIZE;
    const int32_t* sd  = m_sd.data<int32_t>();
    const double*  dc  = m_dc.data<double>();
    const double*  qty = m_qty.data<double>();
    const double*  ext = m_ext.data<double>();

    // Collect qualifying blocks (zone-map secondary filter)
    std::vector<uint32_t> qual_blocks;
    qual_blocks.reserve(128);
    for (uint32_t b = first_qual; b <= last_qual; b++) {
        if (zones[b].max_val < DATE_LO || zones[b].min_val >= DATE_HI) continue;
        qual_blocks.push_back(b);
    }

    // ── main_scan: morsel-driven parallel scan ────────────────────────────
    // Data is fully pre-faulted → zero page-fault latency during scan.
    alignas(64) double partial[NTHREADS] = {};
    std::atomic<uint32_t> block_idx{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            double local_sum = 0.0;
            while (true) {
                uint32_t bi = block_idx.fetch_add(1, std::memory_order_relaxed);
                if (bi >= (uint32_t)qual_blocks.size()) break;

                uint32_t b   = qual_blocks[bi];
                uint32_t rs  = b * BLOCK_SIZE;
                uint32_t re  = std::min(rs + BLOCK_SIZE, TOTAL_ROWS);
                uint32_t off = (uint32_t)(rs - row_base);  // index into range-mapped arrays
                uint32_t len = re - rs;

                const int32_t* s = sd  + off;
                const double*  d = dc  + off;
                const double*  q = qty + off;
                const double*  e = ext + off;

                // Fused filter + accumulate — compiler will auto-vectorize with -march=native
                for (uint32_t i = 0; i < len; i++) {
                    if (s[i] < DATE_LO || s[i] >= DATE_HI) continue;
                    double dv = d[i];
                    if (dv < DISC_LO || dv > DISC_HI) continue;
                    if (q[i] >= QTY_MAX) continue;
                    local_sum += e[i] * dv;
                }
            }
            partial[tid] = local_sum;
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
        revenue += partial[t];

    // ── output ────────────────────────────────────────────────────────────
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
