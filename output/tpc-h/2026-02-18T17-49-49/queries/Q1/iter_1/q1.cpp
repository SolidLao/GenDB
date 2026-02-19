// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping
//
// Optimizations over iter_0:
// 1. Zone-aware scan: zones with max <= CUTOFF skip per-row shipdate check entirely
//    (fused full-zone path vs boundary-zone path)
// 2. Raw pointer access in hot loop — eliminates operator[] overhead, enables SIMD
// 3. OpenMP parallel for on morsels instead of manual std::thread + atomic
// 4. int64_t for sum_disc_price (fits: max 59M * 10^9 = 5.9e16 < int64 max 9.2e18)
//    __int128 retained only for sum_charge (ep*(100-disc)*(100+tax), scale=1e6)
// 5. Per-zone morsel dispatch: each OpenMP thread processes complete zones
//    avoiding atomic contention on fine-grained morsel counter

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

// TPC-H dictionary order (first occurrence in lineitem data)
// returnflag: 0=N, 1=R, 2=A
// linestatus: 0=O, 1=F
static const char* RETURNFLAG_DICT[] = {"N", "R", "A"};
static const char* LINESTATUS_DICT[] = {"O", "F"};
static constexpr int N_RETURNFLAG = 3;
static constexpr int N_LINESTATUS = 2;

// ---- Per-group accumulator ----
// group_idx = returnflag_code * N_LINESTATUS + linestatus_code
struct GroupAcc {
    int64_t  sum_qty        = 0;  // l_quantity, scale=1
    int64_t  sum_base_price = 0;  // l_extendedprice, scale=100
    // sum_disc_price = SUM(ep*(100-disc)), scale=10000
    // Max per row: 10M * 100 = 1e9; sum over 59M rows <= 5.9e16 < INT64_MAX (9.2e18) — fits int64
    int64_t  sum_disc_price = 0;
    // sum_charge = SUM(ep*(100-disc)*(100+tax)), scale=1000000
    // Max per row: 1e9 * 200 = 2e11; sum over 59M rows = 1.2e19 > INT64_MAX — needs __int128
    __int128 sum_charge     = 0;
    int64_t  sum_disc       = 0;  // for avg_disc, scale=100
    int64_t  count          = 0;
    bool     active         = false;

    // Full-zone path: no shipdate check needed (all rows in zone pass)
    inline void accumulate(int64_t qty, int64_t ep, int64_t disc, int64_t tax) noexcept {
        sum_qty        += qty;
        sum_base_price += ep;
        const int64_t factor1 = (100LL - disc);
        sum_disc_price += (__int128)ep * factor1;
        sum_charge     += (__int128)ep * factor1 * (100LL + tax);
        sum_disc       += disc;
        count          += 1;
        active          = true;
    }

    void merge(const GroupAcc& o) noexcept {
        sum_qty        += o.sum_qty;
        sum_base_price += o.sum_base_price;
        sum_disc_price += o.sum_disc_price;
        sum_charge     += o.sum_charge;
        sum_disc       += o.sum_disc;
        count          += o.count;
        active         |= o.active;
    }
};

// Pad to avoid false sharing between thread-local arrays (64-byte aligned)
struct alignas(64) ThreadLocal {
    GroupAcc groups[NUM_GROUPS];
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
    }

    const uint64_t total_rows = col_shipdate.size();

    // ---- Load zone map ----
    // Two categories of zones:
    //   full_zones:     zone.max <= CUTOFF  -> all rows pass, no per-row check
    //   partial_zones:  zone.min <= CUTOFF but zone.max > CUTOFF -> need per-row check
    //   skip_zones:     zone.min > CUTOFF   -> skipped entirely
    struct ZoneRange {
        uint32_t start;
        uint32_t end;
        bool     needs_check;  // false = all rows qualify, true = per-row filter
    };
    std::vector<ZoneRange> zone_ranges;
    {
        GENDB_PHASE("build_joins");
        gendb::ZoneMapIndex zmap;
        zmap.open(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

        if (zmap.zones.empty()) {
            // Fallback: scan entire table with check
            zone_ranges.push_back({0, (uint32_t)total_rows, true});
        } else {
            for (const auto& z : zmap.zones) {
                if (z.min > SHIPDATE_CUTOFF) continue;  // skip zone entirely
                uint32_t row_end = z.row_offset + z.row_count;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;
                // If zone.max <= CUTOFF, all rows in zone pass — no per-row check needed
                bool needs_check = (z.max > SHIPDATE_CUTOFF);
                zone_ranges.push_back({z.row_offset, row_end, needs_check});
            }
        }
    }

    // ---- Prefetch all columns into page cache (overlap I/O with setup) ----
    mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                      col_quantity, col_extprice, col_discount, col_tax);

    // ---- Allocate thread-local arrays ----
    int actual_threads = omp_get_max_threads();
    if (actual_threads <= 0) actual_threads = 1;
    if (actual_threads > NUM_THREADS) actual_threads = NUM_THREADS;

    std::vector<ThreadLocal> tl(actual_threads);

    // Raw column pointers for cache-friendly hot-loop access
    const int32_t* __restrict__ p_shipdate   = col_shipdate.data;
    const int16_t* __restrict__ p_returnflag = col_returnflag.data;
    const int16_t* __restrict__ p_linestatus = col_linestatus.data;
    const int64_t* __restrict__ p_quantity   = col_quantity.data;
    const int64_t* __restrict__ p_extprice   = col_extprice.data;
    const int64_t* __restrict__ p_discount   = col_discount.data;
    const int64_t* __restrict__ p_tax        = col_tax.data;

    // ---- Build morsel list from zones ----
    // Each morsel is a zone sub-range of at most MORSEL_SIZE rows
    static constexpr uint32_t MORSEL_SIZE = 65536;  // 64K rows: better L3 cache fit
    struct Morsel {
        uint32_t start;
        uint32_t end;
        bool     needs_check;
    };
    std::vector<Morsel> morsels;
    morsels.reserve(zone_ranges.size() * 4);
    for (const auto& zr : zone_ranges) {
        for (uint32_t s = zr.start; s < zr.end; s += MORSEL_SIZE) {
            uint32_t e = std::min(s + MORSEL_SIZE, zr.end);
            morsels.push_back({s, e, zr.needs_check});
        }
    }

    const int num_morsels = (int)morsels.size();

    // ---- Morsel-driven parallel scan via OpenMP ----
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(actual_threads)
        {
            int tid = omp_get_thread_num();
            GroupAcc* __restrict__ local = tl[tid].groups;

            #pragma omp for schedule(dynamic, 1) nowait
            for (int mi = 0; mi < num_morsels; ++mi) {
                const uint32_t row_start  = morsels[mi].start;
                const uint32_t row_end    = morsels[mi].end;
                const bool     need_check = morsels[mi].needs_check;

                if (!need_check) {
                    // Fast path: all rows in this zone pass the date filter
                    // No branch on shipdate — helps compiler auto-vectorize group lookup
                    for (uint32_t r = row_start; r < row_end; ++r) {
                        const int rf  = (int)p_returnflag[r];
                        const int ls  = (int)p_linestatus[r];
                        const int gid = rf * N_LINESTATUS + ls;
                        if (__builtin_expect((unsigned)gid >= (unsigned)NUM_GROUPS, 0)) continue;
                        local[gid].accumulate(
                            p_quantity[r],
                            p_extprice[r],
                            p_discount[r],
                            p_tax[r]
                        );
                    }
                } else {
                    // Boundary path: per-row shipdate check needed
                    for (uint32_t r = row_start; r < row_end; ++r) {
                        if (p_shipdate[r] > SHIPDATE_CUTOFF) continue;
                        const int rf  = (int)p_returnflag[r];
                        const int ls  = (int)p_linestatus[r];
                        const int gid = rf * N_LINESTATUS + ls;
                        if (__builtin_expect((unsigned)gid >= (unsigned)NUM_GROUPS, 0)) continue;
                        local[gid].accumulate(
                            p_quantity[r],
                            p_extprice[r],
                            p_discount[r],
                            p_tax[r]
                        );
                    }
                }
            }
        } // end omp parallel
    }

    // ---- Merge thread-local results ----
    GroupAcc global_acc[NUM_GROUPS] = {};
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < actual_threads; ++t)
            for (int g = 0; g < NUM_GROUPS; ++g)
                global_acc[g].merge(tl[t].groups[g]);
    }

    // ---- Collect active groups and sort by returnflag, linestatus ----
    struct Result {
        const char* returnflag;
        const char* linestatus;
        int         rf_code;
        int         ls_code;
        GroupAcc*   acc;
    };

    std::vector<Result> results;
    {
        GENDB_PHASE("sort_topk");
        for (int rf = 0; rf < N_RETURNFLAG; ++rf) {
            for (int ls = 0; ls < N_LINESTATUS; ++ls) {
                int gid = rf * N_LINESTATUS + ls;
                if (!global_acc[gid].active) continue;
                results.push_back({RETURNFLAG_DICT[rf], LINESTATUS_DICT[ls], rf, ls, &global_acc[gid]});
            }
        }
        // Sort by returnflag string ASC, linestatus string ASC
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

        for (auto& res : results) {
            const GroupAcc& acc = *res.acc;
            if (acc.count == 0) continue;

            // sum_qty: scale=1
            double sum_qty_out        = (double)acc.sum_qty;
            // sum_base_price: scale=100
            double sum_base_price_out = (double)acc.sum_base_price / 100.0;
            // sum_disc_price = SUM(ep*(100-disc)), scale=10000
            double sum_disc_price_out = (double)acc.sum_disc_price / 10000.0;
            // sum_charge = SUM(ep*(100-disc)*(100+tax)), scale=1000000
            double sum_charge_out     = (double)(long double)acc.sum_charge / 1000000.0;
            // avg_qty: sum_qty/count (scale=1)
            double avg_qty_out        = (double)acc.sum_qty / (double)acc.count;
            // avg_price: sum_base_price/count, scale=100
            double avg_price_out      = (double)acc.sum_base_price / (double)acc.count / 100.0;
            // avg_disc: sum_disc/count, scale=100
            double avg_disc_out       = (double)acc.sum_disc / (double)acc.count / 100.0;

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
                    (long)acc.count);
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
