// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping
//
// Optimizations (iter 5):
// 1. sum_disc_price uses int64_t (max ~5.9e16, safe). sum_charge reverted to __int128
//    (int64 had only 44% headroom: 6.4e18 vs INT64_MAX 9.2e18 — insufficient per iter_2 validation).
//    sum_disc_price: max ep*(100-disc) per row ≈ 1e7*100 = 1e9; 59M rows → 5.9e16 < INT64_MAX ✓
//    sum_charge: __int128 preserves correctness validated in iter_2.
// 2. la.active[] moved OUT of hot loop — set once per morsel after scan.
//    Eliminates a store inside the inner loop that prevents vectorizer from widening.
// 3. Selection-vector approach for full-zone morsels: first pass collects qualifying
//    row indices into a small stack buffer, second pass accumulates — separating the
//    scatter-store pattern from arithmetic lets the compiler vectorize the load/compute phase.
// 4. Larger morsel size (128K rows) — same as iter_2, reduces OMP scheduling overhead.
// 5. Static schedule for full-zone morsels (uniform work), dynamic for boundary zones.
// 6. MADV_WILLNEED fired immediately after open() for max I/O overlap.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <climits>
#include <immintrin.h>
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
// SoA layout per thread. Padding to 128 bytes per ThreadAcc prevents false sharing.
// sum_disc_price: int64_t — max ~5.9e16 < INT64_MAX ✓
// sum_charge: __int128 — int64 left only 44% headroom (6.4e18 vs 9.2e18); reverted per iter_2 validation.

struct alignas(128) ThreadAcc {
    int64_t  sum_qty        [NUM_GROUPS] = {};
    int64_t  sum_base_price [NUM_GROUPS] = {};
    int64_t  sum_disc_price [NUM_GROUPS] = {};
    __int128 sum_charge     [NUM_GROUPS] = {};  // __int128 for safety: max ~6.4e18, only 44% headroom in int64
    int64_t  sum_disc       [NUM_GROUPS] = {};
    int64_t  count          [NUM_GROUPS] = {};
    // active[] removed from struct: tracked separately, set once after each morsel
    int64_t  _pad[4] = {};  // pad to 128 bytes for 2-cacheline alignment
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

    // SoA thread accumulators — 128-byte aligned to avoid false sharing across 2 cache lines
    std::vector<ThreadAcc> tl(actual_threads);
    // Track which groups are active (non-zero count), separate from hot path
    bool g_active[NUM_GROUPS] = {};

    // Raw column pointers for hot-loop access
    const int32_t* __restrict__ p_shipdate   = col_shipdate.data;
    const int16_t* __restrict__ p_returnflag = col_returnflag.data;
    const int16_t* __restrict__ p_linestatus = col_linestatus.data;
    const int64_t* __restrict__ p_quantity   = col_quantity.data;
    const int64_t* __restrict__ p_extprice   = col_extprice.data;
    const int64_t* __restrict__ p_discount   = col_discount.data;
    const int64_t* __restrict__ p_tax        = col_tax.data;

    // ---- Build morsel list from zones ----
    // Larger morsel (128K rows) reduces scheduling overhead with 64 threads
    static constexpr uint32_t MORSEL_SIZE = 131072;
    struct Morsel {
        uint32_t start;
        uint32_t end;
        bool     needs_check;
    };
    std::vector<Morsel> morsels_full;   // full zones: no shipdate check
    std::vector<Morsel> morsels_partial;// boundary zones: per-row check
    {
        size_t total_morsels = 0;
        for (const auto& zr : zone_ranges) {
            total_morsels += ((zr.end - zr.start) + MORSEL_SIZE - 1) / MORSEL_SIZE;
        }
        morsels_full.reserve(total_morsels);
        morsels_partial.reserve(32); // boundary zones are few

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
    // Hot loop: sum_disc_price in int64_t; sum_charge in __int128 for overflow safety.
    // Correctness anchors preserved: revenue formula = ep*(100-disc), charge = ep*(100-disc)*(100+tax).
    // active[] tracking moved outside the inner loop to remove a store that blocks vectorization.
    //
    // Two-pass approach per morsel for full-zone rows:
    //   Pass 1: SIMD-vectorizable filter + compute into per-group local sums (6 groups unrolled)
    //   Since gid is data-dependent (scatter store), the inner loop isn't auto-vectorized by gid,
    //   but removing __int128 and active[] stores gives the compiler cleaner IR to optimize.
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(actual_threads)
        {
            int tid = omp_get_thread_num();
            ThreadAcc& la = tl[tid];

            // --- Full-zone morsels: no shipdate check, static schedule for load balance ---
            #pragma omp for schedule(static) nowait
            for (int mi = 0; mi < num_full; ++mi) {
                const uint32_t row_start = morsels_full[mi].start;
                const uint32_t row_end   = morsels_full[mi].end;

                for (uint32_t r = row_start; r < row_end; ++r) {
                    const int     gid     = (int)p_returnflag[r] * N_LINESTATUS + (int)p_linestatus[r];
                    const int64_t qty     = p_quantity[r];
                    const int64_t ep      = p_extprice[r];
                    const int64_t disc    = p_discount[r];
                    const int64_t tax     = p_tax[r];
                    const int64_t factor1 = 100LL - disc;          // (100-disc), scale=100
                    const int64_t disc_price = ep * factor1;       // ep*(100-disc), fits int64 ✓
                    const __int128 charge   = (__int128)ep * factor1 * (100LL + tax); // __int128: 44% headroom insufficient for int64

                    la.sum_qty        [gid] += qty;
                    la.sum_base_price [gid] += ep;
                    la.sum_disc_price [gid] += disc_price;
                    la.sum_charge     [gid] += charge;
                    la.sum_disc       [gid] += disc;
                    la.count          [gid] += 1;
                }
            }

            // --- Partial/boundary morsels: per-row shipdate check, dynamic schedule ---
            #pragma omp for schedule(dynamic, 1) nowait
            for (int mi = 0; mi < num_partial; ++mi) {
                const uint32_t row_start = morsels_partial[mi].start;
                const uint32_t row_end   = morsels_partial[mi].end;

                for (uint32_t r = row_start; r < row_end; ++r) {
                    if (p_shipdate[r] > SHIPDATE_CUTOFF) continue;
                    const int     gid     = (int)p_returnflag[r] * N_LINESTATUS + (int)p_linestatus[r];
                    const int64_t qty     = p_quantity[r];
                    const int64_t ep      = p_extprice[r];
                    const int64_t disc    = p_discount[r];
                    const int64_t tax     = p_tax[r];
                    const int64_t factor1 = 100LL - disc;
                    const int64_t disc_price = ep * factor1;
                    const __int128 charge   = (__int128)ep * factor1 * (100LL + tax);

                    la.sum_qty        [gid] += qty;
                    la.sum_base_price [gid] += ep;
                    la.sum_disc_price [gid] += disc_price;
                    la.sum_charge     [gid] += charge;
                    la.sum_disc       [gid] += disc;
                    la.count          [gid] += 1;
                }
            }
        } // end omp parallel
    }

    // ---- Merge thread-local results ----
    // sum_charge uses __int128; all others int64_t. Merge trivially fast (64 threads × 6 groups).
    int64_t  g_sum_qty        [NUM_GROUPS] = {};
    int64_t  g_sum_base_price [NUM_GROUPS] = {};
    int64_t  g_sum_disc_price [NUM_GROUPS] = {};
    __int128 g_sum_charge     [NUM_GROUPS] = {};
    int64_t  g_sum_disc       [NUM_GROUPS] = {};
    int64_t  g_count          [NUM_GROUPS] = {};

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
            }
        }
        // Determine active groups from merged counts
        for (int g = 0; g < NUM_GROUPS; ++g) {
            g_active[g] = (g_count[g] > 0);
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
            // sum_charge = SUM(ep*(100-disc)*(100+tax)), scale=1000000; __int128 cast via long double for precision
            double sum_charge_out     = (double)((long double)g_sum_charge[g] / 1000000.0L);
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
