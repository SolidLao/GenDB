// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24
//
// Strategy:
//   - Zone-map pruning on l_shipdate (86 of 600 blocks survive)
//   - Parallel morsel-driven scan (64 threads, atomic block counter)
//   - Thread-local int64 accumulators merged at end
//   - Interior blocks: branchless SIMD-vectorized inner loop (loads all 4 cols)
//   - Edge blocks: branchy with per-row shipdate check + late-mat extendedprice
//   - Result scale: extendedprice*discount has implicit scale 100*100=10000

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <iostream>
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"

// Predicate constants (all values in scaled integer representation, scale_factor=100)
static constexpr int32_t DATE_LOW  = 8766;  // 1994-01-01 epoch days
static constexpr int32_t DATE_HIGH = 9131;  // 1995-01-01 epoch days
static constexpr int64_t DISC_LOW  = 5;     // 0.05 * 100
static constexpr int64_t DISC_HIGH = 7;     // 0.07 * 100
static constexpr int64_t QTY_MAX   = 24;    // 24 raw value (scale_factor=1, NO scaling)

struct ZoneMapEntry {
    int32_t  min;
    int32_t  max;
    uint32_t block_size;
};

static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = static_cast<size_t>(st.st_size);
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Phase 1: mmap all columns
    // -------------------------------------------------------------------------
    size_t sz_shipdate, sz_discount, sz_quantity, sz_extprice, sz_zonemap;

    const int32_t* col_shipdate = reinterpret_cast<const int32_t*>(
        mmap_file(gendb_dir + "/lineitem/l_shipdate.bin",      sz_shipdate));
    const int64_t* col_discount = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_discount.bin",      sz_discount));
    const int64_t* col_quantity = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_quantity.bin",      sz_quantity));
    const int64_t* col_extprice = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", sz_extprice));

    // -------------------------------------------------------------------------
    // Phase 2: Load zone map and build surviving block list
    // -------------------------------------------------------------------------
    const uint8_t* zm_raw = reinterpret_cast<const uint8_t*>(
        mmap_file(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin", sz_zonemap));

    uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
    const ZoneMapEntry* zm = reinterpret_cast<const ZoneMapEntry*>(zm_raw + 4);

    std::vector<uint32_t> surviving;
    std::vector<size_t>   block_start;

    {
        GENDB_PHASE("zone_map_prune");

        surviving.reserve(num_blocks);
        block_start.resize(num_blocks + 1);

        size_t off = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            block_start[b] = off;
            off += zm[b].block_size;
            // Prune blocks entirely outside [DATE_LOW, DATE_HIGH)
            if (zm[b].max < DATE_LOW || zm[b].min >= DATE_HIGH) continue;
            surviving.push_back(b);
        }
        block_start[num_blocks] = off;

        // Sequential access hint for all columns
        madvise(const_cast<int32_t*>(col_shipdate), sz_shipdate, MADV_SEQUENTIAL);
        madvise(const_cast<int64_t*>(col_discount),  sz_discount,  MADV_SEQUENTIAL);
        madvise(const_cast<int64_t*>(col_quantity),  sz_quantity,  MADV_SEQUENTIAL);
        madvise(const_cast<int64_t*>(col_extprice),  sz_extprice,  MADV_SEQUENTIAL);
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel morsel-driven scan
    // -------------------------------------------------------------------------
    static constexpr int N_THREADS = 64;
    std::atomic<uint32_t> morsel_counter{0};

    // Pad thread sums to avoid false sharing (64-byte cache lines)
    alignas(64) int64_t thread_sums[N_THREADS] = {};

    {
        GENDB_PHASE("main_scan");

        uint32_t n_surviving = static_cast<uint32_t>(surviving.size());

        auto worker = [&](int tid) {
            int64_t local_sum = 0;

            while (true) {
                uint32_t idx = morsel_counter.fetch_add(1, std::memory_order_relaxed);
                if (idx >= n_surviving) break;

                uint32_t b     = surviving[idx];
                size_t   rs    = block_start[b];
                size_t   re    = block_start[b + 1];
                size_t   count = re - rs;

                int32_t bmin     = zm[b].min;
                int32_t bmax     = zm[b].max;
                bool    interior = (bmin >= DATE_LOW && bmax < DATE_HIGH);

                const int32_t* __restrict__ sd = col_shipdate + rs;
                const int64_t* __restrict__ di = col_discount  + rs;
                const int64_t* __restrict__ qt = col_quantity   + rs;
                const int64_t* __restrict__ ep = col_extprice   + rs;

                if (interior) {
                    // Interior block: all rows pass the date filter — skip shipdate check.
                    // Late materialization: only load ep for the ~12.5% of rows that pass
                    // both disc and qty filters, reducing ep bandwidth by ~87.5%.
                    // disc filter (~27% pass rate) applied first for maximum early rejection.
                    for (size_t i = 0; i < count; i++) {
                        int64_t disc = di[i];
                        if (disc < DISC_LOW || disc > DISC_HIGH) continue;
                        if (qt[i] >= QTY_MAX) continue;
                        local_sum += ep[i] * disc;
                    }
                } else {
                    // Edge block: per-row shipdate check required.
                    for (size_t i = 0; i < count; i++) {
                        int32_t ship = sd[i];
                        if (ship < DATE_LOW || ship >= DATE_HIGH) continue;
                        int64_t disc = di[i];
                        if (disc < DISC_LOW || disc > DISC_HIGH) continue;
                        if (qt[i] >= QTY_MAX) continue;
                        local_sum += ep[i] * disc;
                    }
                }
            }

            thread_sums[tid] = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(N_THREADS);
        for (int t = 0; t < N_THREADS; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();
    }

    // -------------------------------------------------------------------------
    // Phase 4: Merge and output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        int64_t total = 0;
        for (int t = 0; t < N_THREADS; t++) total += thread_sums[t];

        // Scale: extendedprice * discount has implicit scale 100 * 100 = 10000
        // Divide by 10000 to get the actual decimal value.
        double revenue = static_cast<double>(total) / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", revenue);
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
