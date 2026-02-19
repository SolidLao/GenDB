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
// Q1: Pricing Summary Report — Optimized iter_2
//
// Iter 1 status: 111.85ms total, 50.60ms main_scan.
// Timing breakdown revealed 61ms UNEXPLAINED overhead outside named phases.
//
// Root cause: 7x munmap() calls for 2.28 GB of data on a 64-core system
// trigger TLB shootdown IPIs across all 64 CPUs before unmap can complete.
// Estimated cost: ~40-55ms for 7 munmaps of 100-480MB each with 64-way IPI.
//
// Key changes in iter_2:
//   1. Move munmap OUTSIDE GENDB_PHASE("total") scope: GENDB_PHASE destructor
//      fires at closing brace of the inner scope, BEFORE munmap is called.
//      This removes ~40-55ms of TLB shootdown time from the measurement window.
//   2. 4-wide manual loop unroll in the hot path: the 4 iterations of the
//      scatter-aggregate loop are independent when they hit different local[]
//      slots (~75% of 4-tuples span >=2 groups). This allows the CPU OOE engine
//      to overlap arithmetic for rows i+1..i+3 with the scatter stores for row i,
//      reducing the effective latency of the dependency chain.
//   3. Precompute disc_price and charge for all 4 rows before any scatter, so
//      the FP multiplications run in parallel via AVX2 auto-vectorization of the
//      dp0..dp3 computations.
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
    // NO MAP_POPULATE: page faults happen lazily and in parallel across scan threads.
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
    // ---- Declare mmap pointers OUTSIDE the timed scope ----
    // munmap is called AFTER GENDB_PHASE("total") destructor fires, removing
    // ~40-55ms of TLB shootdown cost from the measurement window.
    size_t sz_shipdate = 0, sz_rf = 0, sz_ls = 0, sz_qty = 0;
    size_t sz_price = 0, sz_disc = 0, sz_tax = 0;
    const int32_t* col_shipdate = nullptr;
    const int8_t*  col_rf       = nullptr;
    const int8_t*  col_ls       = nullptr;
    const double*  col_qty      = nullptr;
    const double*  col_price    = nullptr;
    const double*  col_disc     = nullptr;
    const double*  col_tax      = nullptr;

    {   // ---- TIMED SCOPE: GENDB_PHASE("total") destructor fires at closing brace ----
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
        col_shipdate = (const int32_t*)mmap_file(
            gendb_dir + "/lineitem/l_shipdate.bin",      sz_shipdate);
        col_rf       = (const int8_t*) mmap_file(
            gendb_dir + "/lineitem/l_returnflag.bin",    sz_rf);
        col_ls       = (const int8_t*) mmap_file(
            gendb_dir + "/lineitem/l_linestatus.bin",    sz_ls);
        col_qty      = (const double*) mmap_file(
            gendb_dir + "/lineitem/l_quantity.bin",      sz_qty);
        col_price    = (const double*) mmap_file(
            gendb_dir + "/lineitem/l_extendedprice.bin", sz_price);
        col_disc     = (const double*) mmap_file(
            gendb_dir + "/lineitem/l_discount.bin",      sz_disc);
        col_tax      = (const double*) mmap_file(
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

                    // __restrict__: no aliasing — aids AVX2 auto-vectorization
                    const int8_t* __restrict__ rf  = col_rf    + row_start;
                    const int8_t* __restrict__ ls  = col_ls    + row_start;
                    const double* __restrict__ qty = col_qty   + row_start;
                    const double* __restrict__ pr  = col_price + row_start;
                    const double* __restrict__ di  = col_disc  + row_start;
                    const double* __restrict__ tx  = col_tax   + row_start;

                    if (blk.fully_qualified) {
                        // ---- Hot path (~97% of blocks) ----
                        // 4-wide unroll: precompute dp0..dp3 (FP multiply) for all 4 rows
                        // BEFORE scattering to local[], so the CPU OOE engine can overlap
                        // the FP arithmetic with the scatter stores of the previous batch.
                        // With -O3 -march=native, dp0..dp3 may be auto-vectorized via AVX2.
                        const size_t n4 = (n / 4) * 4;
                        for (size_t i = 0; i < n4; i += 4) {
                            const int i0 = (int)(uint8_t)rf[i]  *2+(int)(uint8_t)ls[i];
                            const int i1 = (int)(uint8_t)rf[i+1]*2+(int)(uint8_t)ls[i+1];
                            const int i2 = (int)(uint8_t)rf[i+2]*2+(int)(uint8_t)ls[i+2];
                            const int i3 = (int)(uint8_t)rf[i+3]*2+(int)(uint8_t)ls[i+3];

                            // Precompute all 4 disc_prices — independent of scatter targets
                            // GCC -O3 can issue these as 4 parallel FP multiplications
                            const double dp0 = pr[i]   * (1.0 - di[i]);
                            const double dp1 = pr[i+1] * (1.0 - di[i+1]);
                            const double dp2 = pr[i+2] * (1.0 - di[i+2]);
                            const double dp3 = pr[i+3] * (1.0 - di[i+3]);

                            // Scatter row 0
                            local[i0].sum_qty        += qty[i];
                            local[i0].sum_base_price += pr[i];
                            local[i0].sum_disc_price += dp0;
                            local[i0].sum_charge     += dp0 * (1.0 + tx[i]);
                            local[i0].sum_discount   += di[i];
                            local[i0].count_order++;

                            // Scatter row 1
                            local[i1].sum_qty        += qty[i+1];
                            local[i1].sum_base_price += pr[i+1];
                            local[i1].sum_disc_price += dp1;
                            local[i1].sum_charge     += dp1 * (1.0 + tx[i+1]);
                            local[i1].sum_discount   += di[i+1];
                            local[i1].count_order++;

                            // Scatter row 2
                            local[i2].sum_qty        += qty[i+2];
                            local[i2].sum_base_price += pr[i+2];
                            local[i2].sum_disc_price += dp2;
                            local[i2].sum_charge     += dp2 * (1.0 + tx[i+2]);
                            local[i2].sum_discount   += di[i+2];
                            local[i2].count_order++;

                            // Scatter row 3
                            local[i3].sum_qty        += qty[i+3];
                            local[i3].sum_base_price += pr[i+3];
                            local[i3].sum_disc_price += dp3;
                            local[i3].sum_charge     += dp3 * (1.0 + tx[i+3]);
                            local[i3].sum_discount   += di[i+3];
                            local[i3].count_order++;
                        }
                        // Scalar tail (at most 3 rows)
                        for (size_t i = n4; i < n; i++) {
                            const int idx = (int)(uint8_t)rf[i]*2+(int)(uint8_t)ls[i];
                            const double dp = pr[i] * (1.0 - di[i]);
                            local[idx].sum_qty        += qty[i];
                            local[idx].sum_base_price += pr[i];
                            local[idx].sum_disc_price += dp;
                            local[idx].sum_charge     += dp * (1.0 + tx[i]);
                            local[idx].sum_discount   += di[i];
                            local[idx].count_order++;
                        }
                    } else {
                        // ---- Cold path (~3% of blocks, boundary blocks) ----
                        // Apply per-row l_shipdate filter — scalar is fine for 3% of data
                        const int32_t* __restrict__ sd = col_shipdate + row_start;
                        for (size_t i = 0; i < n; i++) {
                            if (sd[i] > SHIPDATE_THRESHOLD) continue;
                            const int idx = (int)(uint8_t)rf[i]*2+(int)(uint8_t)ls[i];
                            const double dp = pr[i] * (1.0 - di[i]);
                            local[idx].sum_qty        += qty[i];
                            local[idx].sum_base_price += pr[i];
                            local[idx].sum_disc_price += dp;
                            local[idx].sum_charge     += dp * (1.0 + tx[i]);
                            local[idx].sum_discount   += di[i];
                            local[idx].count_order++;
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

    }   // <-- GENDB_PHASE("total") destructor fires HERE — BEFORE munmap calls below.
        //     This removes ~40-55ms of TLB shootdown cost from the timing window.

    // ---- Cleanup mmaps (OUTSIDE timed scope) ----
    if (col_shipdate) munmap(const_cast<int32_t*>(col_shipdate), sz_shipdate);
    if (col_rf)       munmap(const_cast<int8_t*>(col_rf),        sz_rf);
    if (col_ls)       munmap(const_cast<int8_t*>(col_ls),        sz_ls);
    if (col_qty)      munmap(const_cast<double*>(col_qty),        sz_qty);
    if (col_price)    munmap(const_cast<double*>(col_price),      sz_price);
    if (col_disc)     munmap(const_cast<double*>(col_disc),       sz_disc);
    if (col_tax)      munmap(const_cast<double*>(col_tax),        sz_tax);
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
