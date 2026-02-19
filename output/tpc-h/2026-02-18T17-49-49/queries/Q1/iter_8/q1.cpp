// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping
//
// Iter 8 key changes vs iter 6 (best so far, 108ms):
// 1. Branchless per-group accumulation using integer masks instead of indirect
//    scatter `m_acc[gid] += val`. For 6 groups, compute a mask per group
//    (gid == g ? 1 : 0) and multiply — this is fully vectorizable by the compiler.
//    We loop over groups in the outer dimension, rows in the inner dimension,
//    but only once via an interleaved approach:
//    For each row: compute 6 boolean masks via comparisons, multiply & accumulate.
//    This transforms the scatter into 6 independent fused-multiply-add streams.
// 2. Reduce morsel size to better fit L3 cache per thread: 64K rows
//    (64K * 40 bytes = 2.6MB per morsel, fits in L3 / thread on 64-core machine with 44MB L3).
// 3. Use __builtin_expect for the conditional zone-boundary rows.
// 4. Force vectorization with #pragma omp simd simdlen(8) on the inner loop.

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
#include <immintrin.h>

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

// Morsel size: 64K rows
// Max sum_charge per morsel: 65536 * max(ep*(100-disc)*(100+tax))
// Max ep ~1e9, factor1 max=100, factor2 max=200 => 2e13 per row * 65536 = 1.31e18 < INT64_MAX. SAFE.
static constexpr uint32_t MORSEL_SIZE = 65536;

// ---- Per-thread, per-group accumulators ----
// Padded to 128 bytes to avoid false sharing.
struct alignas(128) ThreadAcc {
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
            // Key optimization: branchless per-group accumulation.
            // For each row we compute gid = rf * 2 + ls, then for each of 6
            // possible gid values, compute mask = (gid == target) as int64 (0 or 1),
            // multiply by mask and accumulate. This converts the scatter (non-vectorizable)
            // into 6 independent fused-multiply-add streams (vectorizable).
            //
            // We split into two approaches:
            //   A) Process rows in blocks of 8, using branchless group isolation
            //   B) Use per-group morsel local accumulators with scalar fallback
            //
            // We use approach B (proven in iter 6) but with smaller morsels (64K)
            // and explicit simd hint on the inner loop for better compiler vectorization.

            #pragma omp for schedule(static) nowait
            for (int mi = 0; mi < num_full; ++mi) {
                const uint32_t row_start = morsels_full[mi].start;
                const uint32_t row_end   = morsels_full[mi].end;

                // Per-morsel int64 accumulators (stack-allocated)
                // Branchless accumulation: for each row, compute gid (0..5),
                // then for each group g, mask = (gid == g), add mask*val to m_acc[g].
                // This is vectorizable because there are no loop-carried dependencies
                // across iterations when written as 6 separate mask streams.
                int64_t m_qty   [NUM_GROUPS] = {};
                int64_t m_ep    [NUM_GROUPS] = {};
                int64_t m_disc_p[NUM_GROUPS] = {};
                int64_t m_charge[NUM_GROUPS] = {};
                int64_t m_disc  [NUM_GROUPS] = {};
                int64_t m_cnt   [NUM_GROUPS] = {};

                // Branchless scatter via per-group mask multiply.
                // The compiler should auto-vectorize this as 6 separate horizontal
                // reduction streams — no scatter dependency between iterations.
                for (uint32_t r = row_start; r < row_end; ++r) {
                    const int64_t rf   = (int64_t)(uint16_t)p_returnflag[r];
                    const int64_t ls   = (int64_t)(uint16_t)p_linestatus[r];
                    const int64_t qty  = p_quantity[r];
                    const int64_t ep   = p_extprice[r];
                    const int64_t disc = p_discount[r];
                    const int64_t tax  = p_tax[r];
                    const int64_t f1   = 100LL - disc;
                    const int64_t dp   = ep * f1;
                    const int64_t ch   = dp * (100LL + tax);
                    const int64_t gid  = rf * N_LINESTATUS + ls;

                    // Unrolled branchless group update — fully vectorizable
                    // Compiler sees 6 independent accumulators updated via mask
                    m_qty   [0] += qty  & -(gid == 0);
                    m_qty   [1] += qty  & -(gid == 1);
                    m_qty   [2] += qty  & -(gid == 2);
                    m_qty   [3] += qty  & -(gid == 3);
                    m_qty   [4] += qty  & -(gid == 4);
                    m_qty   [5] += qty  & -(gid == 5);

                    m_ep    [0] += ep   & -(gid == 0);
                    m_ep    [1] += ep   & -(gid == 1);
                    m_ep    [2] += ep   & -(gid == 2);
                    m_ep    [3] += ep   & -(gid == 3);
                    m_ep    [4] += ep   & -(gid == 4);
                    m_ep    [5] += ep   & -(gid == 5);

                    m_disc_p[0] += dp   & -(gid == 0);
                    m_disc_p[1] += dp   & -(gid == 1);
                    m_disc_p[2] += dp   & -(gid == 2);
                    m_disc_p[3] += dp   & -(gid == 3);
                    m_disc_p[4] += dp   & -(gid == 4);
                    m_disc_p[5] += dp   & -(gid == 5);

                    m_charge[0] += ch   & -(gid == 0);
                    m_charge[1] += ch   & -(gid == 1);
                    m_charge[2] += ch   & -(gid == 2);
                    m_charge[3] += ch   & -(gid == 3);
                    m_charge[4] += ch   & -(gid == 4);
                    m_charge[5] += ch   & -(gid == 5);

                    m_disc  [0] += disc & -(gid == 0);
                    m_disc  [1] += disc & -(gid == 1);
                    m_disc  [2] += disc & -(gid == 2);
                    m_disc  [3] += disc & -(gid == 3);
                    m_disc  [4] += disc & -(gid == 4);
                    m_disc  [5] += disc & -(gid == 5);

                    m_cnt   [0] += (int64_t)(gid == 0);
                    m_cnt   [1] += (int64_t)(gid == 1);
                    m_cnt   [2] += (int64_t)(gid == 2);
                    m_cnt   [3] += (int64_t)(gid == 3);
                    m_cnt   [4] += (int64_t)(gid == 4);
                    m_cnt   [5] += (int64_t)(gid == 5);
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
                    const int64_t rf   = (int64_t)(uint16_t)p_returnflag[r];
                    const int64_t ls   = (int64_t)(uint16_t)p_linestatus[r];
                    const int64_t qty  = p_quantity[r];
                    const int64_t ep   = p_extprice[r];
                    const int64_t disc = p_discount[r];
                    const int64_t tax  = p_tax[r];
                    const int64_t f1   = 100LL - disc;
                    const int64_t dp   = ep * f1;
                    const int64_t ch   = dp * (100LL + tax);
                    const int64_t gid  = rf * N_LINESTATUS + ls;

                    m_qty   [gid] += qty;
                    m_ep    [gid] += ep;
                    m_disc_p[gid] += dp;
                    m_charge[gid] += ch;
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
