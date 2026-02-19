#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"
#include "timing_utils.h"

// Zone map entry: [double min_val, double max_val, uint32_t row_count, uint32_t _pad] = 24 bytes
struct ZoneMapEntry {
    double   min_val;
    double   max_val;
    uint32_t row_count;
    uint32_t _pad;
};

static inline const void* mmap_file(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_size = st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return ptr;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---------------------------------------------------------------------------
    // Phase 1: Load zone map and identify qualifying blocks
    // ---------------------------------------------------------------------------
    std::vector<uint32_t> qualifying_blocks;
    size_t total_rows = 0;

    {
        GENDB_PHASE("dim_filter");

        std::string zm_path = gendb_dir + "/lineitem/indexes/l_shipdate_zonemap.bin";
        size_t zm_size = 0;
        const void* zm_raw = mmap_file(zm_path.c_str(), zm_size);
        if (!zm_raw) return;

        const uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapEntry* entries = reinterpret_cast<const ZoneMapEntry*>(
            static_cast<const char*>(zm_raw) + sizeof(uint32_t));

        qualifying_blocks.reserve(128);
        for (uint32_t i = 0; i < num_blocks; i++) {
            // Skip if block is entirely outside [8766, 9131)
            if (entries[i].max_val < 8766.0 || entries[i].min_val >= 9131.0) continue;
            qualifying_blocks.push_back(i);
        }

        // Compute total rows from last block
        // Row offset of block i = i * 100000; last block may be partial
        // We need total_rows for bounds checking
        {
            std::string sd_path = gendb_dir + "/lineitem/l_shipdate.bin";
            struct stat st;
            int fd = open(sd_path.c_str(), O_RDONLY);
            fstat(fd, &st);
            close(fd);
            total_rows = st.st_size / sizeof(int32_t);
        }

        munmap(const_cast<void*>(zm_raw), zm_size);
    }

    // ---------------------------------------------------------------------------
    // Phase 2: Parallel scan of qualifying blocks
    // ---------------------------------------------------------------------------
    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        // mmap all 4 columns
        size_t sd_size = 0, disc_size = 0, qty_size = 0, ep_size = 0;
        const int32_t* col_shipdate      = reinterpret_cast<const int32_t*>(
            mmap_file((gendb_dir + "/lineitem/l_shipdate.bin").c_str(), sd_size));
        const double*  col_discount      = reinterpret_cast<const double*>(
            mmap_file((gendb_dir + "/lineitem/l_discount.bin").c_str(), disc_size));
        const double*  col_quantity      = reinterpret_cast<const double*>(
            mmap_file((gendb_dir + "/lineitem/l_quantity.bin").c_str(), qty_size));
        const double*  col_extendedprice = reinterpret_cast<const double*>(
            mmap_file((gendb_dir + "/lineitem/l_extendedprice.bin").c_str(), ep_size));

        if (!col_shipdate || !col_discount || !col_quantity || !col_extendedprice) return;

        const uint32_t BLOCK_SIZE = 100000;
        const uint32_t num_qualifying = (uint32_t)qualifying_blocks.size();

        // Dynamic block scheduling via atomic counter
        std::atomic<uint32_t> next_block{0};

        int nthreads = (int)std::thread::hardware_concurrency();
        if (nthreads <= 0) nthreads = 64;
        // Cap threads to number of qualifying blocks to avoid idle threads
        if (nthreads > (int)num_qualifying) nthreads = (int)num_qualifying;
        if (nthreads < 1) nthreads = 1;

        std::vector<double> partial_sums(nthreads, 0.0);

        auto worker = [&](int tid) {
            double local_sum = 0.0;
            const double DISC_LO = 0.05 - 1e-9;
            const double DISC_HI = 0.07 + 1e-9;
            const double QTY_MAX = 24.0;
            const int32_t SD_LO  = 8766;
            const int32_t SD_HI  = 9131;

            while (true) {
                uint32_t bi = next_block.fetch_add(1, std::memory_order_relaxed);
                if (bi >= num_qualifying) break;

                uint32_t block_id = qualifying_blocks[bi];
                size_t   row_start = (size_t)block_id * BLOCK_SIZE;
                size_t   row_end   = row_start + BLOCK_SIZE;
                if (row_end > total_rows) row_end = total_rows;

                const int32_t* sd  = col_shipdate      + row_start;
                const double*  dis = col_discount       + row_start;
                const double*  qty = col_quantity       + row_start;
                const double*  ep  = col_extendedprice  + row_start;
                size_t n = row_end - row_start;

                // Fused filter + accumulate — compiler can auto-vectorize
                for (size_t i = 0; i < n; i++) {
                    int32_t shipdate = sd[i];
                    if (shipdate < SD_LO | shipdate >= SD_HI) continue;
                    double disc = dis[i];
                    if (disc < DISC_LO | disc > DISC_HI) continue;
                    if (qty[i] >= QTY_MAX) continue;
                    local_sum += ep[i] * disc;
                }
            }
            partial_sums[tid] = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();

        for (int t = 0; t < nthreads; t++)
            global_revenue += partial_sums[t];

        munmap(const_cast<int32_t*>(col_shipdate), sd_size);
        munmap(const_cast<double*>(col_discount), disc_size);
        munmap(const_cast<double*>(col_quantity), qty_size);
        munmap(const_cast<double*>(col_extendedprice), ep_size);
    }

    // ---------------------------------------------------------------------------
    // Phase 3: Output results
    // ---------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", global_revenue);
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
