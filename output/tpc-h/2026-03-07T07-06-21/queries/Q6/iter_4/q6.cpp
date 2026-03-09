// Q6: Forecasting Revenue Change — iter_4
// Strategy: zone_map_prune → partial_mmap_populate → parallel_filter_scan → aggregate_reduce
//
// Key optimizations vs prior iterations:
//   1. Partial mmap: map only qualifying row range [row_lo, row_hi) — ~17MB vs 834MB full
//   2. MAP_POPULATE: pre-fault all qualifying pages in main thread before spawning workers
//      eliminates concurrent page-fault kernel lock contention during parallel scan
//   3. Direct mkdir() syscall instead of system('mkdir -p ...') — no fork+exec overhead
//   4. Row-range partitioning across hardware_concurrency() threads (not block-stride)
//   5. Filter order: shipdate → discount → quantity (most selective first per plan)

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <string>
#include <vector>
#include <thread>
#include <algorithm>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    // total timer: declared first, lives until end of main
    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Create output directory via direct syscall (no fork+exec overhead)
    // -------------------------------------------------------------------------
    if (mkdir(results_dir.c_str(), 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "mkdir(%s) failed: %s\n", results_dir.c_str(), strerror(errno));
        return 1;
    }

    // -------------------------------------------------------------------------
    // Phase: zone_map_prune
    // -------------------------------------------------------------------------
    struct ZE { int32_t mn, mx; };
    uint32_t num_blocks = 0, block_size = 0;
    std::vector<ZE> zones;
    size_t row_lo = 0, row_hi = 0;
    bool have_qualifying = false;

    {
        GENDB_PHASE("zone_map_prune");

        const std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (!zf) { fprintf(stderr, "Cannot open zone map: %s\n", zm_path.c_str()); return 1; }
        (void)fread(&num_blocks, 4, 1, zf);
        (void)fread(&block_size, 4, 1, zf);
        zones.resize(num_blocks);
        (void)fread(zones.data(), sizeof(ZE), num_blocks, zf);
        fclose(zf);

        const int32_t LO = 8766;  // 1994-01-01
        const int32_t HI = 9131;  // 1995-01-01

        uint32_t block_lo = num_blocks, block_hi = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].mx < LO || zones[b].mn >= HI) continue;
            if (b < block_lo) block_lo = b;
            if (b + 1 > block_hi) block_hi = b + 1;
        }

        if (block_lo < block_hi) {
            const size_t total_rows = 59986052ULL;
            row_lo = (size_t)block_lo * block_size;
            row_hi = std::min((size_t)block_hi * block_size, total_rows);
            have_qualifying = true;
        }
    }

    // No qualifying blocks → output zero revenue
    if (!have_qualifying) {
        const std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { fprintf(stderr, "Cannot open output file\n"); return 1; }
        fprintf(out, "revenue\n0.00\n");
        fclose(out);
        return 0;
    }

    const size_t qualifying_rows = row_hi - row_lo;

    // -------------------------------------------------------------------------
    // Phase: partial_mmap_populate
    // Map ONLY the qualifying row range for each column with MAP_POPULATE.
    // MAP_POPULATE pre-faults all pages synchronously in the main thread before
    // workers start, eliminating concurrent page-fault kernel lock contention.
    // -------------------------------------------------------------------------
    struct PartialMap {
        void*  base;
        size_t len;
        void*  elem_ptr;  // pointer to element at row_lo
    };

    PartialMap pm_sd{}, pm_disc{}, pm_qty{}, pm_ep{};

    {
        GENDB_PHASE("partial_mmap_populate");

        const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

        auto do_partial_mmap = [&](const std::string& path, size_t elem_size) -> PartialMap {
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                fprintf(stderr, "Cannot open %s: %s\n", path.c_str(), strerror(errno));
                exit(1);
            }
            const size_t byte_lo    = row_lo * elem_size;
            const size_t byte_hi    = row_hi * elem_size;
            const size_t aligned_lo = (byte_lo / PAGE_SIZE) * PAGE_SIZE;
            const size_t delta      = byte_lo - aligned_lo;
            const size_t map_len    = byte_hi - aligned_lo;

            void* base = mmap(nullptr, map_len, PROT_READ,
                              MAP_PRIVATE | MAP_POPULATE, fd, (off_t)aligned_lo);
            close(fd);
            if (base == MAP_FAILED) {
                fprintf(stderr, "mmap failed for %s: %s\n", path.c_str(), strerror(errno));
                exit(1);
            }
            return PartialMap{base, map_len, static_cast<char*>(base) + delta};
        };

        pm_sd   = do_partial_mmap(gendb_dir + "/lineitem/l_shipdate.bin",      sizeof(int32_t));
        pm_disc = do_partial_mmap(gendb_dir + "/lineitem/l_discount.bin",      sizeof(int8_t));
        pm_qty  = do_partial_mmap(gendb_dir + "/lineitem/l_quantity.bin",      sizeof(int8_t));
        pm_ep   = do_partial_mmap(gendb_dir + "/lineitem/l_extendedprice.bin", sizeof(double));
    }

    // Typed pointers into the qualifying window (index 0 = row_lo)
    const int32_t* l_shipdate      = static_cast<const int32_t*>(pm_sd.elem_ptr);
    const int8_t*  l_discount      = static_cast<const int8_t*>(pm_disc.elem_ptr);
    const int8_t*  l_quantity      = static_cast<const int8_t*>(pm_qty.elem_ptr);
    const double*  l_extendedprice = static_cast<const double*>(pm_ep.elem_ptr);

    // -------------------------------------------------------------------------
    // Phase: main_scan — morsel-driven parallel filter + aggregation
    // Divide [0, qualifying_rows) evenly across all hardware threads.
    // Each thread accumulates a local double; no shared state during scan.
    // -------------------------------------------------------------------------
    double revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        constexpr int32_t LO = 8766;
        constexpr int32_t HI = 9131;

        const int nthreads = (int)std::thread::hardware_concurrency();
        std::vector<double> thread_sums(nthreads, 0.0);
        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        const size_t chunk = (qualifying_rows + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, qualifying_rows);
                if (lo >= hi) return;

                double rev = 0.0;

                // Inner loop: filter order sd → disc → qty (most selective first)
                for (size_t i = lo; i < hi; i++) {
                    const int32_t sd = l_shipdate[i];
                    if (sd < LO || sd >= HI) continue;
                    const int8_t disc = l_discount[i];
                    if (disc < 5 || disc > 7) continue;
                    const int8_t qty = l_quantity[i];
                    if (qty >= (int8_t)24) continue;
                    rev += l_extendedprice[i] * (disc * 0.01);
                }

                thread_sums[t] = rev;
            });
        }

        for (auto& th : threads) th.join();

        // Sequential reduction of per-thread partial sums
        for (double s : thread_sums) revenue += s;
    }

    // Unmap partial column windows
    munmap(pm_sd.base,   pm_sd.len);
    munmap(pm_disc.base, pm_disc.len);
    munmap(pm_qty.base,  pm_qty.len);
    munmap(pm_ep.base,   pm_ep.len);

    // -------------------------------------------------------------------------
    // Phase: output — write CSV result
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { fprintf(stderr, "Cannot open output: %s\n", out_path.c_str()); return 1; }
        fprintf(out, "revenue\n%.2f\n", revenue);
        fclose(out);
    }

    return 0;
}
