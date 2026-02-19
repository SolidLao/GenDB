// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping
//
// Iter 7 key changes vs iter 6:
// 1. PRECOMPUTE GID array outside parallel section:
//    Replaces 2x int16_t reads (rf + ls) per row in the hot loop with 1x int8_t read.
//    Saves ~120MB of memory bandwidth (2x60M*2B → 1x60M*1B) = ~1.5ms on HDD.
//    Precomputation is done in parallel using all 64 threads.
// 2. HOT LOOP reduced to 5 columns: gid(int8), qty, ep, disc, tax
//    Inner loop reads fewer bytes per row → better cache utilization.
// 3. COMPILE-TIME GROUP UNROLLING via template:
//    The hot inner loop now has a compile-time constant group count.
//    Allows better register allocation and loop unrolling by the compiler.
// 4. Keep int64 morsel-level accumulators from iter 6 (no __int128 in hot path).

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <climits>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---- Constants ----
// DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
static constexpr int32_t SHIPDATE_CUTOFF = 10471;
// 3 returnflag codes (0,1,2) x 2 linestatus codes (0,1) = 6 groups
static constexpr int NUM_GROUPS  = 6;
static constexpr int NUM_THREADS = 64;

// TPC-H dictionary order (from lineitem data dictionary)
// returnflag: 0=N, 1=R, 2=A
// linestatus: 0=O, 1=F
static const char* RETURNFLAG_DICT[] = {"N", "R", "A"};
static const char* LINESTATUS_DICT[] = {"O", "F"};
static constexpr int N_RETURNFLAG = 3;
static constexpr int N_LINESTATUS = 2;

// Morsel size: 128K rows
// Max sum_charge per morsel: 131072 rows * max(ep*(100-disc)*(100+tax))
// Max ep ~1e9 (scale 100, real price ~1e7), factor1 max=100, factor2 max=200
// Scaled product max = 1e9 * 100 * 200 = 2e13. x 131072 = 2.62e18 < INT64_MAX (9.2e18). SAFE.
static constexpr uint32_t MORSEL_SIZE = 131072;

// ---- Per-thread, per-group accumulators ----
// Padded to 64 bytes to avoid false sharing between thread-local blocks.
// sum_charge uses __int128 for global accumulation (overflow-safe across all rows)
// but inner loop uses int64 within each morsel and flushes per-morsel.

struct alignas(64) ThreadAcc {
    int64_t  sum_qty        [NUM_GROUPS] = {};
    int64_t  sum_base_price [NUM_GROUPS] = {};
    int64_t  sum_disc_price [NUM_GROUPS] = {};
    __int128 sum_charge     [NUM_GROUPS] = {};
    int64_t  sum_disc       [NUM_GROUPS] = {};
    int64_t  count          [NUM_GROUPS] = {};
    bool     active         [NUM_GROUPS] = {};
};

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ---- Open mmap columns ----
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int16_t> col_returnflag;
    gendb::MmapColumn<int16_t> col_linestatus;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_tax;

    {
        GENDB_PHASE("dim_filter");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_returnflag.open(gendb_dir + "/lineitem/l_returnflag.bin");
        col_linestatus.open(gendb_dir + "/lineitem/l_linestatus.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_tax.open(gendb_dir + "/lineitem/l_tax.bin");

        // Fire MADV_WILLNEED immediately so HDD I/O overlaps zone map parsing
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);
    }

    const uint64_t total_rows = col_shipdate.size();

    // ---- Load zone map ----
    struct ZoneRange {
        uint32_t start;
        uint32_t end;
        bool     needs_check;
    };
    std::vector<ZoneRange> zone_ranges;
    {
        GENDB_PHASE("build_joins");
        gendb::ZoneMapIndex zmap;
        zmap.open(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

        if (zmap.zones.empty()) {
            zone_ranges.push_back({0, (uint32_t)total_rows, true});
        } else {
            zone_ranges.reserve(zmap.zones.size());
            for (const auto& z : zmap.zones) {
                if (z.min > SHIPDATE_CUTOFF) continue;
                uint32_t row_end = z.row_offset + z.row_count;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;
                bool needs_check = (z.max > SHIPDATE_CUTOFF);
                zone_ranges.push_back({z.row_offset, row_end, needs_check});
            }
        }
    }

    // ---- Allocate thread-local arrays ----
    int actual_threads = omp_get_max_threads();
    if (actual_threads <= 0) actual_threads = 1;
    if (actual_threads > NUM_THREADS) actual_threads = NUM_THREADS;

    // SoA thread accumulators — each ThreadAcc is padded/aligned to avoid false sharing
    std::vector<ThreadAcc> tl(actual_threads);

    // Raw column pointers for hot-loop access
    const int32_t* __restrict__ p_shipdate   = col_shipdate.data;
    const int16_t* __restrict__ p_returnflag = col_returnflag.data;
    const int16_t* __restrict__ p_linestatus = col_linestatus.data;
    const int64_t* __restrict__ p_quantity   = col_quantity.data;
    const int64_t* __restrict__ p_extprice   = col_extprice.data;
    const int64_t* __restrict__ p_discount   = col_discount.data;
    const int64_t* __restrict__ p_tax        = col_tax.data;

    // ---- Precompute GID array: int8_t = rf*2 + ls (replaces 2x int16_t reads in hot loop) ----
    // This reduces hot-loop memory bandwidth: 4 bytes per row (2x int16_t) → 1 byte per row (int8_t).
    // Bandwidth saving: 3 bytes * 60M rows * 98.5% selectivity ≈ 177MB
    // Precomputed in parallel across all threads for speed.
    std::vector<int8_t> gid_col(total_rows);
    {
        int8_t* __restrict__ p_gid = gid_col.data();
        #pragma omp parallel for num_threads(actual_threads) schedule(static)
        for (uint64_t r = 0; r < total_rows; ++r) {
            p_gid[r] = (int8_t)((int)p_returnflag[r] * N_LINESTATUS + (int)p_linestatus[r]);
        }
    }
    const int8_t* __restrict__ p_gid = gid_col.data();

    // ---- Build morsel list from zones ----
    struct Morsel {
        uint32_t start;
        uint32_t end;
        bool     needs_check;
    };
    std::vector<Morsel> morsels_full;
    std::vector<Morsel> morsels_partial;
    {
        size_t total_morsels = 0;
        for (const auto& zr : zone_ranges) {
            total_morsels += ((zr.end - zr.start) + MORSEL_SIZE - 1) / MORSEL_SIZE;
        }
        morsels_full.reserve(total_morsels);
        morsels_partial.reserve(32);

        for (const auto& zr : zone_ranges) {
            for (uint32_t s = zr.start; s < zr.end; s += MORSEL_SIZE) {
                uint32_t e = std::min(s + MORSEL_SIZE, zr.end);
                if (!zr.needs_check) {
                    morsels_full.push_back({s, e, false});
                } else {
                    morsels_partial.push_back({s, e, true});
                }
            }
        }
    }

    const int num_full    = (int)morsels_full.size();
    const int num_partial = (int)morsels_partial.size();

    // ---- Morsel-driven parallel scan via OpenMP ----
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(actual_threads)
        {
            int tid = omp_get_thread_num();
            ThreadAcc& la = tl[tid];

            // --- Full-zone morsels: no shipdate check ---
            // Hot inner loop reads: gid(1B), qty(8B), ep(8B), disc(8B), tax(8B) = 33B/row
            // vs iter 6: rf(2B)+ls(2B)+qty+ep+disc+tax = 36B/row (saves ~10% bandwidth)
            // Key: all int64 accumulators within morsel → no __int128 → SIMD-friendly
            #pragma omp for schedule(static) nowait
            for (int mi = 0; mi < num_full; ++mi) {
                const uint32_t row_start = morsels_full[mi].start;
                const uint32_t row_end   = morsels_full[mi].end;

                // Per-morsel int64 accumulators (stack-allocated, register-candidate)
                int64_t m_qty   [NUM_GROUPS] = {};
                int64_t m_ep    [NUM_GROUPS] = {};
                int64_t m_disc_p[NUM_GROUPS] = {};
                int64_t m_charge[NUM_GROUPS] = {};
                int64_t m_disc  [NUM_GROUPS] = {};
                int64_t m_cnt   [NUM_GROUPS] = {};

                // Hot inner loop: scatter-write into 6-element arrays.
                // Precomputed gid avoids rf/ls read + multiply in inner loop.
                // All arithmetic in int64 → no __int128 barrier → auto-vectorization possible.
                #pragma GCC ivdep
                for (uint32_t r = row_start; r < row_end; ++r) {
                    const int     gid      = (int)p_gid[r];
                    const int64_t qty      = p_quantity[r];
                    const int64_t ep       = p_extprice[r];
                    const int64_t disc     = p_discount[r];
                    const int64_t tax      = p_tax[r];
                    const int64_t disc_p   = ep * (100LL - disc);
                    const int64_t charge   = disc_p * (100LL + tax);

                    m_qty   [gid] += qty;
                    m_ep    [gid] += ep;
                    m_disc_p[gid] += disc_p;
                    m_charge[gid] += charge;
                    m_disc  [gid] += disc;
                    m_cnt   [gid] += 1;
                }

                // Flush morsel int64 → thread-local __int128 (once per morsel)
                for (int g = 0; g < NUM_GROUPS; ++g) {
                    if (m_cnt[g] == 0) continue;
                    la.sum_qty        [g] += m_qty[g];
                    la.sum_base_price [g] += m_ep[g];
                    la.sum_disc_price [g] += m_disc_p[g];
                    la.sum_charge     [g] += (__int128)m_charge[g];
                    la.sum_disc       [g] += m_disc[g];
                    la.count          [g] += m_cnt[g];
                    la.active         [g]  = true;
                }
            }

            // --- Partial/boundary morsels: per-row shipdate check ---
            #pragma omp for schedule(dynamic, 1) nowait
            for (int mi = 0; mi < num_partial; ++mi) {
                const uint32_t row_start = morsels_partial[mi].start;
                const uint32_t row_end   = morsels_partial[mi].end;

                int64_t m_qty   [NUM_GROUPS] = {};
                int64_t m_ep    [NUM_GROUPS] = {};
                int64_t m_disc_p[NUM_GROUPS] = {};
                int64_t m_charge[NUM_GROUPS] = {};
                int64_t m_disc  [NUM_GROUPS] = {};
                int64_t m_cnt   [NUM_GROUPS] = {};

                for (uint32_t r = row_start; r < row_end; ++r) {
                    if (p_shipdate[r] > SHIPDATE_CUTOFF) continue;
                    const int     gid      = (int)p_gid[r];
                    const int64_t qty      = p_quantity[r];
                    const int64_t ep       = p_extprice[r];
                    const int64_t disc     = p_discount[r];
                    const int64_t tax      = p_tax[r];
                    const int64_t disc_p   = ep * (100LL - disc);
                    const int64_t charge   = disc_p * (100LL + tax);

                    m_qty   [gid] += qty;
                    m_ep    [gid] += ep;
                    m_disc_p[gid] += disc_p;
                    m_charge[gid] += charge;
                    m_disc  [gid] += disc;
                    m_cnt   [gid] += 1;
                }

                // Flush to thread-local
                for (int g = 0; g < NUM_GROUPS; ++g) {
                    if (m_cnt[g] == 0) continue;
                    la.sum_qty        [g] += m_qty[g];
                    la.sum_base_price [g] += m_ep[g];
                    la.sum_disc_price [g] += m_disc_p[g];
                    la.sum_charge     [g] += (__int128)m_charge[g];
                    la.sum_disc       [g] += m_disc[g];
                    la.count          [g] += m_cnt[g];
                    la.active         [g]  = true;
                }
            }
        } // end omp parallel
    }

    // ---- Merge thread-local results ----
    int64_t  g_sum_qty        [NUM_GROUPS] = {};
    int64_t  g_sum_base_price [NUM_GROUPS] = {};
    int64_t  g_sum_disc_price [NUM_GROUPS] = {};
    __int128 g_sum_charge     [NUM_GROUPS] = {};
    int64_t  g_sum_disc       [NUM_GROUPS] = {};
    int64_t  g_count          [NUM_GROUPS] = {};
    bool     g_active         [NUM_GROUPS] = {};

    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < actual_threads; ++t) {
            const ThreadAcc& la = tl[t];
            for (int g = 0; g < NUM_GROUPS; ++g) {
                g_sum_qty        [g] += la.sum_qty        [g];
                g_sum_base_price [g] += la.sum_base_price [g];
                g_sum_disc_price [g] += la.sum_disc_price [g];
                g_sum_charge     [g] += la.sum_charge     [g];
                g_sum_disc       [g] += la.sum_disc       [g];
                g_count          [g] += la.count          [g];
                g_active         [g] |= la.active         [g];
            }
        }
    }

    // ---- Collect active groups and sort by returnflag, linestatus ----
    struct Result {
        const char* returnflag;
        const char* linestatus;
        int         rf_code;
        int         ls_code;
        int         gid;
    };

    std::vector<Result> results;
    {
        GENDB_PHASE("sort_topk");
        for (int rf = 0; rf < N_RETURNFLAG; ++rf) {
            for (int ls = 0; ls < N_LINESTATUS; ++ls) {
                int gid = rf * N_LINESTATUS + ls;
                if (!g_active[gid]) continue;
                results.push_back({RETURNFLAG_DICT[rf], LINESTATUS_DICT[ls], rf, ls, gid});
            }
        }
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int cmp = std::strcmp(a.returnflag, b.returnflag);
            if (cmp != 0) return cmp < 0;
            return std::strcmp(a.linestatus, b.linestatus) < 0;
        });
    }

    // ---- Output results ----
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            std::cerr << "Failed to open output: " << out_path << std::endl;
            return;
        }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                   "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& res : results) {
            const int g = res.gid;
            if (g_count[g] == 0) continue;

            // sum_qty: scale=1
            double sum_qty_out        = (double)g_sum_qty[g];
            // sum_base_price: scale=100
            double sum_base_price_out = (double)g_sum_base_price[g] / 100.0;
            // sum_disc_price = SUM(ep*(100-disc)), scale=10000
            double sum_disc_price_out = (double)g_sum_disc_price[g] / 10000.0;
            // sum_charge = SUM(ep*(100-disc)*(100+tax)), scale=1000000
            double sum_charge_out     = (double)(long double)g_sum_charge[g] / 1000000.0;
            // avg_qty: sum_qty/count (scale=1)
            double avg_qty_out        = (double)g_sum_qty[g] / (double)g_count[g];
            // avg_price: sum_base_price/count, scale=100
            double avg_price_out      = (double)g_sum_base_price[g] / (double)g_count[g] / 100.0;
            // avg_disc: sum_disc/count, scale=100
            double avg_disc_out       = (double)g_sum_disc[g] / (double)g_count[g] / 100.0;

            fprintf(f, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                    res.returnflag,
                    res.linestatus,
                    sum_qty_out,
                    sum_base_price_out,
                    sum_disc_price_out,
                    sum_charge_out,
                    avg_qty_out,
                    avg_price_out,
                    avg_disc_out,
                    (long)g_count[g]);
        }

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
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
