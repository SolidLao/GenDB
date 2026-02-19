// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping
//
// Optimizations in iter_4:
// 1. Branchless per-group accumulation: eliminate indirect scatter (gid-indexed writes)
//    that prevents auto-vectorization. Instead, for each row compute a branchless mask
//    (0 or 1) per group via == comparison, multiply by value, and accumulate.
//    With only 6 groups this is 6x cheaper than a mispredicted branch on gid.
// 2. sum_disc_price in pure int64 (no __int128): ep*(100-disc) max ~9.3e9 per row,
//    59M rows => max 5.5e17 << INT64_MAX. Safe.
// 3. sum_charge batched: accumulate ep*(100-disc)*(100+tax) in int64 per batch of 512
//    rows (max batch ~2e11 * 512 = 1.0e14 << INT64_MAX), promote once per batch.
// 4. Static schedule on full-zone morsels, dynamic on boundary morsels.
// 5. MADV_WILLNEED fired immediately to overlap HDD I/O with zone map parsing.
// 6. Reduced morsel size to 64K (from 128K) to improve load balance across 64 threads
//    when total scan work is uneven.

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

// ---- Per-thread, per-group accumulators ----
// SoA layout: separate arrays per field to allow vectorized merge.
// sum_disc_price: ep*(100-disc) — pure int64, max per row ~9.3e9, total ~5.5e17 < INT64_MAX.
// sum_charge: ep*(100-disc)*(100+tax) — __int128 for global; batched int64 accumulation in-loop.
// Padded to 64 bytes to avoid false sharing between thread-local blocks.

struct alignas(64) ThreadAcc {
    int64_t  sum_qty        [NUM_GROUPS] = {};
    int64_t  sum_base_price [NUM_GROUPS] = {};
    int64_t  sum_disc_price [NUM_GROUPS] = {};
    __int128 sum_charge     [NUM_GROUPS] = {};
    int64_t  sum_disc       [NUM_GROUPS] = {};
    int64_t  count          [NUM_GROUPS] = {};
    bool     active         [NUM_GROUPS] = {};
};

// Batch size for int64 charge accumulation before promoting to __int128.
// Per-row charge max: ep*(100-disc)*(100+tax) <= 1e7 * 100 * 200 = 2e11 (fits int64).
// Batch of 512: 512 * 2e11 = 1.02e14 << INT64_MAX (9.2e18). Safe.
static constexpr uint32_t CHARGE_BATCH = 512;

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

    // ---- Build morsel list from zones ----
    // Morsel size 64K: better load balance across 64 threads; ~930 morsels total
    static constexpr uint32_t MORSEL_SIZE = 65536;
    struct Morsel {
        uint32_t start;
        uint32_t end;
        bool     needs_check;
    };
    std::vector<Morsel> morsels_full;    // full zones: no shipdate check
    std::vector<Morsel> morsels_partial; // boundary zones: per-row check
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
    // Key optimization: branchless per-group accumulation.
    // Instead of: la.sum_qty[gid] += qty  (indirect scatter, kills vectorization)
    // We do:      for each group g: la.sum_qty[g] += qty * (gid == g)
    // This turns the irregular scatter into 6 branchless conditional adds per row.
    // The compiler can vectorize the outer (row) loop with SIMD because the
    // group index loop is fully unrolled (only 6 iterations, compile-time constant).
    //
    // sum_charge uses batched int64 accumulation (CHARGE_BATCH rows) to avoid
    // per-row __int128 multiply while keeping correctness.
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(actual_threads)
        {
            int tid = omp_get_thread_num();
            ThreadAcc& la = tl[tid];

            // --- Full-zone morsels: no shipdate check, static schedule ---
            #pragma omp for schedule(static) nowait
            for (int mi = 0; mi < num_full; ++mi) {
                const uint32_t row_start = morsels_full[mi].start;
                const uint32_t row_end   = morsels_full[mi].end;

                // Process in batches of CHARGE_BATCH for int64 charge accumulation
                for (uint32_t rb = row_start; rb < row_end; rb += CHARGE_BATCH) {
                    const uint32_t batch_end = (rb + CHARGE_BATCH < row_end) ? rb + CHARGE_BATCH : row_end;

                    // Per-batch int64 charge accumulators (reset per batch)
                    int64_t batch_charge[NUM_GROUPS] = {};

                    for (uint32_t r = rb; r < batch_end; ++r) {
                        const int     rf    = (int)p_returnflag[r];
                        const int     ls    = (int)p_linestatus[r];
                        const int64_t qty   = p_quantity[r];
                        const int64_t ep    = p_extprice[r];
                        const int64_t disc  = p_discount[r];
                        const int64_t tax   = p_tax[r];
                        const int64_t f1    = 100LL - disc;         // (100 - disc)
                        const int64_t dp    = ep * f1;              // ep*(100-disc): int64, fits
                        const int64_t chg   = dp * (100LL + tax);   // per-row charge: ~2e11, fits int64

                        // Branchless scatter: replaces la.field[gid] += val
                        // with a fully unrolled 6-way branchless update.
                        // Compiler unrolls the inner loop (NUM_GROUPS=6 constant).
                        // The mask (rf == rfk && ls == lsk) is 0 or 1 — branchless.
                        #pragma GCC unroll 6
                        for (int g = 0; g < NUM_GROUPS; ++g) {
                            const int rfk = g / N_LINESTATUS;
                            const int lsk = g % N_LINESTATUS;
                            const int64_t mask = (int64_t)((rf == rfk) & (ls == lsk));
                            la.sum_qty        [g] += qty  * mask;
                            la.sum_base_price [g] += ep   * mask;
                            la.sum_disc_price [g] += dp   * mask;
                            batch_charge      [g] += chg  * mask;
                            la.sum_disc       [g] += disc * mask;
                            la.count          [g] += mask;
                        }
                    }

                    // Promote batch int64 charge to __int128 (once per batch)
                    #pragma GCC unroll 6
                    for (int g = 0; g < NUM_GROUPS; ++g) {
                        la.sum_charge[g] += (__int128)batch_charge[g];
                        if (batch_charge[g] != 0) la.active[g] = true;
                    }
                }
            }

            // --- Partial/boundary morsels: per-row shipdate check, dynamic schedule ---
            #pragma omp for schedule(dynamic, 1) nowait
            for (int mi = 0; mi < num_partial; ++mi) {
                const uint32_t row_start = morsels_partial[mi].start;
                const uint32_t row_end   = morsels_partial[mi].end;

                for (uint32_t rb = row_start; rb < row_end; rb += CHARGE_BATCH) {
                    const uint32_t batch_end = (rb + CHARGE_BATCH < row_end) ? rb + CHARGE_BATCH : row_end;

                    int64_t batch_charge[NUM_GROUPS] = {};

                    for (uint32_t r = rb; r < batch_end; ++r) {
                        if (p_shipdate[r] > SHIPDATE_CUTOFF) continue;
                        const int     rf    = (int)p_returnflag[r];
                        const int     ls    = (int)p_linestatus[r];
                        const int64_t qty   = p_quantity[r];
                        const int64_t ep    = p_extprice[r];
                        const int64_t disc  = p_discount[r];
                        const int64_t tax   = p_tax[r];
                        const int64_t f1    = 100LL - disc;
                        const int64_t dp    = ep * f1;
                        const int64_t chg   = dp * (100LL + tax);

                        #pragma GCC unroll 6
                        for (int g = 0; g < NUM_GROUPS; ++g) {
                            const int rfk = g / N_LINESTATUS;
                            const int lsk = g % N_LINESTATUS;
                            const int64_t mask = (int64_t)((rf == rfk) & (ls == lsk));
                            la.sum_qty        [g] += qty  * mask;
                            la.sum_base_price [g] += ep   * mask;
                            la.sum_disc_price [g] += dp   * mask;
                            batch_charge      [g] += chg  * mask;
                            la.sum_disc       [g] += disc * mask;
                            la.count          [g] += mask;
                        }
                    }

                    #pragma GCC unroll 6
                    for (int g = 0; g < NUM_GROUPS; ++g) {
                        la.sum_charge[g] += (__int128)batch_charge[g];
                        if (batch_charge[g] != 0) la.active[g] = true;
                    }
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
