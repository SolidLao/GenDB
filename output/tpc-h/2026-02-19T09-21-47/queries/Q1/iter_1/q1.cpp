#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <vector>
#include <fstream>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ============================================================
// Q1: Pricing Summary Report — Optimized iter_1
//
// Root cause of 222ms (target ~83ms):
//   The 7 mmap() calls used MAP_POPULATE, forcing ~570K single-threaded minor
//   page faults for 2.28 GB of column data on the main thread (~170ms wasted).
//   Additionally, long double (80-bit x87) in AggSlot prevented AVX2
//   auto-vectorization of the inner loop.
//
// Key changes:
//   1. Removed MAP_POPULATE: page faults deferred to parallel scan threads,
//      amortized across 64 threads → ~170ms overhead eliminated.
//   2. Added MADV_SEQUENTIAL: enables aggressive kernel readahead.
//   3. long double → double in AggSlot: enables AVX2 arithmetic vectorization;
//      reduces slot size from ~88B to 48B (fits in one cache line).
//   4. alignas(64) AggSlot: cache-line alignment prevents false sharing.
//   5. fully_qualified block split: ~97% of blocks (those where max_val <=
//      threshold) skip l_shipdate reads AND the per-row filter branch, saving
//      ~232 MB of memory reads and eliminating branch mispredictions in hot path.
//   6. OpenMP parallel for schedule(static): lower overhead than std::thread+atomic.
// ============================================================

struct ZoneMapEntry {
    double   min_val;
    double   max_val;
    uint32_t row_count;
    uint32_t _pad;
};

// 48 bytes of data, padded to 64-byte cache line to prevent false sharing
// and keep all 6 slots for a thread hot in L1 cache (6 * 64 = 384 bytes).
struct alignas(64) AggSlot {
    double  sum_qty        = 0.0;
    double  sum_base_price = 0.0;
    double  sum_disc_price = 0.0;
    double  sum_charge     = 0.0;
    double  sum_discount   = 0.0;
    int64_t count_order    = 0;
};

static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    // NO MAP_POPULATE: avoids ~170ms of single-threaded minor page faults on main
    // thread. Page faults happen lazily and in parallel across scan threads.
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr != MAP_FAILED) {
        madvise(ptr, out_size, MADV_SEQUENTIAL); // aggressive kernel readahead
    }
    close(fd);
    return ptr;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty()) dict.push_back(line);
    }
    return dict;
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const int32_t SHIPDATE_THRESHOLD = 10471; // 1998-09-02

    // ---- Load dictionaries ----
    auto rf_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto ls_dict = load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");

    // ---- Phase 1: Zone map → active block list with fully_qualified flag ----
    struct BlockRange {
        size_t row_start;
        size_t row_end;
        bool   fully_qualified; // true if max_val <= threshold: all rows pass filter
    };
    std::vector<BlockRange> active_blocks;
    {
        GENDB_PHASE("dim_filter");
        size_t zm_size = 0;
        const void* zm_ptr = mmap_file(
            gendb_dir + "/lineitem/indexes/l_shipdate_zonemap.bin", zm_size);
        const uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_ptr);
        const ZoneMapEntry* entries = reinterpret_cast<const ZoneMapEntry*>(
            (const char*)zm_ptr + sizeof(uint32_t));

        size_t row_offset = 0;
        for (uint32_t i = 0; i < num_blocks; i++) {
            const size_t block_rows = entries[i].row_count;
            // l_shipdate sorted ascending — early exit once min_val > threshold
            if (entries[i].min_val > (double)SHIPDATE_THRESHOLD) break;
            // fully_qualified: entire block passes (no per-row filter needed)
            bool fq = (entries[i].max_val <= (double)SHIPDATE_THRESHOLD);
            active_blocks.push_back({row_offset, row_offset + block_rows, fq});
            row_offset += block_rows;
        }
        munmap(const_cast<void*>(zm_ptr), zm_size);
    }

    { GENDB_PHASE("build_joins"); } // no joins in Q1

    // ---- Phase 2: mmap all needed columns — NO MAP_POPULATE ----
    size_t sz_shipdate = 0, sz_rf = 0, sz_ls = 0, sz_qty = 0;
    size_t sz_price = 0, sz_disc = 0, sz_tax = 0;
    const int32_t* col_shipdate = (const int32_t*)mmap_file(
        gendb_dir + "/lineitem/l_shipdate.bin",      sz_shipdate);
    const int8_t*  col_rf       = (const int8_t*) mmap_file(
        gendb_dir + "/lineitem/l_returnflag.bin",    sz_rf);
    const int8_t*  col_ls       = (const int8_t*) mmap_file(
        gendb_dir + "/lineitem/l_linestatus.bin",    sz_ls);
    const double*  col_qty      = (const double*) mmap_file(
        gendb_dir + "/lineitem/l_quantity.bin",      sz_qty);
    const double*  col_price    = (const double*) mmap_file(
        gendb_dir + "/lineitem/l_extendedprice.bin", sz_price);
    const double*  col_disc     = (const double*) mmap_file(
        gendb_dir + "/lineitem/l_discount.bin",      sz_disc);
    const double*  col_tax      = (const double*) mmap_file(
        gendb_dir + "/lineitem/l_tax.bin",           sz_tax);

    // ---- Phase 3: Parallel scan with thread-local flat_array[6] ----
    // Group key: rf_code * 2 + ls_code → max index 4 (rf=2, ls=0); 6 slots safe
    const int N_SLOTS = 6;
    int nthreads = omp_get_max_threads();
    if (nthreads < 1) nthreads = 64;

    // Per-thread AggSlot arrays; alignas(64) prevents false sharing
    std::vector<std::array<AggSlot, N_SLOTS>> thread_agg(nthreads);
    for (auto& arr : thread_agg)
        for (auto& s : arr) s = AggSlot{};

    {
        GENDB_PHASE("main_scan");
        const int total_blocks = (int)active_blocks.size();

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            // Stack-allocated, cache-line aligned local accumulators — stay in L1
            alignas(64) AggSlot local[N_SLOTS];
            for (int s = 0; s < N_SLOTS; s++) local[s] = AggSlot{};

            #pragma omp for schedule(static)
            for (int bi = 0; bi < total_blocks; bi++) {
                const BlockRange& blk = active_blocks[bi];
                const size_t row_start = blk.row_start;
                const size_t n         = blk.row_end - row_start;

                // __restrict__: no aliasing — aids AVX2 auto-vectorization of arithmetic
                const int8_t* __restrict__ rf  = col_rf    + row_start;
                const int8_t* __restrict__ ls  = col_ls    + row_start;
                const double* __restrict__ qty = col_qty   + row_start;
                const double* __restrict__ pr  = col_price + row_start;
                const double* __restrict__ di  = col_disc  + row_start;
                const double* __restrict__ tx  = col_tax   + row_start;

                if (blk.fully_qualified) {
                    // ---- Hot path (~97% of blocks) ----
                    // No filter needed, no l_shipdate read — saves ~232 MB memory reads.
                    // double + __restrict__ enables AVX2 arithmetic vectorization.
                    for (size_t i = 0; i < n; i++) {
                        const int idx = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                        const double disc_price = pr[i] * (1.0 - di[i]);
                        local[idx].sum_qty        += qty[i];
                        local[idx].sum_base_price += pr[i];
                        local[idx].sum_disc_price += disc_price;
                        local[idx].sum_charge     += disc_price * (1.0 + tx[i]);
                        local[idx].sum_discount   += di[i];
                        local[idx].count_order    += 1;
                    }
                } else {
                    // ---- Cold path (~3% of blocks, boundary blocks) ----
                    // Apply per-row l_shipdate filter
                    const int32_t* __restrict__ sd = col_shipdate + row_start;
                    for (size_t i = 0; i < n; i++) {
                        if (sd[i] > SHIPDATE_THRESHOLD) continue;
                        const int idx = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                        const double disc_price = pr[i] * (1.0 - di[i]);
                        local[idx].sum_qty        += qty[i];
                        local[idx].sum_base_price += pr[i];
                        local[idx].sum_disc_price += disc_price;
                        local[idx].sum_charge     += disc_price * (1.0 + tx[i]);
                        local[idx].sum_discount   += di[i];
                        local[idx].count_order    += 1;
                    }
                }
            }

            // Store thread-local results to global array
            for (int s = 0; s < N_SLOTS; s++)
                thread_agg[tid][s] = local[s];
        }
    }

    // ---- Phase 4: Merge thread-local results (O(64*6) = trivial) ----
    AggSlot global_agg[N_SLOTS];
    for (int s = 0; s < N_SLOTS; s++) global_agg[s] = AggSlot{};
    for (int t = 0; t < nthreads; t++) {
        for (int s = 0; s < N_SLOTS; s++) {
            global_agg[s].sum_qty        += thread_agg[t][s].sum_qty;
            global_agg[s].sum_base_price += thread_agg[t][s].sum_base_price;
            global_agg[s].sum_disc_price += thread_agg[t][s].sum_disc_price;
            global_agg[s].sum_charge     += thread_agg[t][s].sum_charge;
            global_agg[s].sum_discount   += thread_agg[t][s].sum_discount;
            global_agg[s].count_order    += thread_agg[t][s].count_order;
        }
    }

    // ---- Phase 5: Output ----
    {
        GENDB_PHASE("output");

        struct OutputRow { int rf_code, ls_code; AggSlot agg; };
        std::vector<OutputRow> rows;
        for (int rf = 0; rf < (int)rf_dict.size(); rf++) {
            for (int ls = 0; ls < (int)ls_dict.size(); ls++) {
                int idx = rf * 2 + ls;
                if (global_agg[idx].count_order > 0)
                    rows.push_back({rf, ls, global_agg[idx]});
            }
        }
        std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.rf_code != b.rf_code) return a.rf_code < b.rf_code;
            return a.ls_code < b.ls_code;
        });

        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                   "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
        for (const auto& r : rows) {
            const double cnt = (double)r.agg.count_order;
            fprintf(f, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                rf_dict[r.rf_code].c_str(),
                ls_dict[r.ls_code].c_str(),
                r.agg.sum_qty,
                r.agg.sum_base_price,
                r.agg.sum_disc_price,
                r.agg.sum_charge,
                r.agg.sum_qty        / cnt,
                r.agg.sum_base_price / cnt,
                r.agg.sum_discount   / cnt,
                r.agg.count_order);
        }
        fclose(f);
    }

    // ---- Cleanup mmaps ----
    munmap(const_cast<int32_t*>(col_shipdate), sz_shipdate);
    munmap(const_cast<int8_t*>(col_rf),        sz_rf);
    munmap(const_cast<int8_t*>(col_ls),        sz_ls);
    munmap(const_cast<double*>(col_qty),        sz_qty);
    munmap(const_cast<double*>(col_price),      sz_price);
    munmap(const_cast<double*>(col_disc),       sz_disc);
    munmap(const_cast<double*>(col_tax),        sz_tax);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    run_Q1(argv[1], argc > 2 ? argv[2] : ".");
    return 0;
}
#endif
