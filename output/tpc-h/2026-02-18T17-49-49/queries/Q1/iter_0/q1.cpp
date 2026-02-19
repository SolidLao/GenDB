// Q1: Pricing Summary Report
// Morsel-driven parallel scan with flat-array aggregation (6 groups max)
// Zone map pruning on l_shipdate for block skipping

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <algorithm>
#include <climits>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---- Constants ----
// DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
static constexpr int32_t SHIPDATE_CUTOFF = 10471;
// 3 returnflag codes (0,1,2) x 2 linestatus codes (0,1) = 6 groups
static constexpr int NUM_GROUPS  = 6;
static constexpr int NUM_THREADS = 64;

// TPC-H dictionary: determined by first-occurrence order in the data
// returnflag: 0=N, 1=R, 2=A  (order of first occurrence in lineitem data)
// linestatus: 0=O, 1=F
// No _dict.txt files present; verified against raw tbl data.
static const char* RETURNFLAG_DICT[] = {"N", "R", "A"};
static const char* LINESTATUS_DICT[] = {"O", "F"};
static constexpr int N_RETURNFLAG = 3;
static constexpr int N_LINESTATUS = 2;

// ---- Per-group accumulator ----
// group_idx = returnflag_code * N_LINESTATUS + linestatus_code
struct GroupAcc {
    int64_t  sum_qty        = 0;  // l_quantity, scale=1
    int64_t  sum_base_price = 0;  // l_extendedprice, scale=100
    // sum_disc_price = SUM(ep * (100-disc)), scale=10000; divide by 10000 at output
    __int128 sum_disc_price = 0;
    // sum_charge = SUM(ep * (100-disc) * (100+tax)), scale=1000000; divide by 1e6 at output
    __int128 sum_charge     = 0;
    int64_t  sum_disc       = 0;  // for avg_disc, scale=100
    int64_t  count          = 0;
    bool     active         = false;

    inline void accumulate(int64_t qty, int64_t ep, int64_t disc, int64_t tax) noexcept {
        sum_qty        += qty;
        sum_base_price += ep;
        int64_t factor1 = (100LL - disc);
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

// Pad to avoid false sharing between thread-local arrays
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

    // ---- Load zone map and compute qualifying ranges ----
    // Using zone map to skip zones where zone.min > SHIPDATE_CUTOFF
    std::vector<std::pair<uint32_t,uint32_t>> ranges; // (start_row, end_row) exclusive
    {
        GENDB_PHASE("build_joins");
        gendb::ZoneMapIndex zmap;
        zmap.open(gendb_dir + "/indexes/lineitem_l_shipdate.bin");
        // For l_shipdate <= SHIPDATE_CUTOFF: pass lo=INT32_MIN, hi=SHIPDATE_CUTOFF+1
        zmap.qualifying_ranges(INT32_MIN, SHIPDATE_CUTOFF + 1, ranges);
        if (ranges.empty()) {
            // Fallback: scan entire table
            ranges.push_back({0, (uint32_t)total_rows});
        }
    }

    // ---- Allocate thread-local arrays ----
    int actual_threads = (int)std::thread::hardware_concurrency();
    if (actual_threads <= 0) actual_threads = 1;
    if (actual_threads > NUM_THREADS) actual_threads = NUM_THREADS;

    std::vector<ThreadLocal> tl(actual_threads);

    // ---- Morsel-driven parallel scan ----
    {
        GENDB_PHASE("main_scan");

        static constexpr uint32_t MORSEL_SIZE = 100000;

        // Build morsel list from qualifying ranges
        struct Morsel { uint32_t start, end; };
        std::vector<Morsel> morsels;
        morsels.reserve(ranges.size() * 2);
        for (auto& [rs, re] : ranges) {
            uint32_t r_end = (re == 0 || re > (uint32_t)total_rows) ? (uint32_t)total_rows : re;
            for (uint32_t s = rs; s < r_end; s += MORSEL_SIZE) {
                uint32_t e = std::min(s + MORSEL_SIZE, r_end);
                morsels.push_back({s, e});
            }
        }

        std::atomic<uint32_t> morsel_idx{0};
        const uint32_t num_morsels = (uint32_t)morsels.size();

        // Prefetch all columns for sequential access
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);

        auto worker = [&](int tid) {
            GroupAcc* __restrict__ local = tl[tid].groups;
            while (true) {
                uint32_t mi = morsel_idx.fetch_add(1, std::memory_order_relaxed);
                if (mi >= num_morsels) break;
                const uint32_t row_start = morsels[mi].start;
                const uint32_t row_end   = morsels[mi].end;

                for (uint32_t r = row_start; r < row_end; ++r) {
                    if (col_shipdate[r] > SHIPDATE_CUTOFF) continue;

                    const int16_t rf  = col_returnflag[r];
                    const int16_t ls  = col_linestatus[r];
                    const int     gid = (int)rf * N_LINESTATUS + (int)ls;

                    // Bounds check; valid: rf in [0,2], ls in [0,1]
                    if (__builtin_expect((unsigned)gid >= (unsigned)NUM_GROUPS, 0)) continue;

                    local[gid].accumulate(
                        col_quantity[r],
                        col_extprice[r],
                        col_discount[r],
                        col_tax[r]
                    );
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(actual_threads);
        for (int t = 0; t < actual_threads; ++t)
            threads.emplace_back(worker, t);
        for (auto& th : threads) th.join();
    }

    // ---- Merge thread-local results ----
    GroupAcc global[NUM_GROUPS] = {};
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < actual_threads; ++t)
            for (int g = 0; g < NUM_GROUPS; ++g)
                global[g].merge(tl[t].groups[g]);
    }

    // ---- Collect active groups and sort ----
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
                if (!global[gid].active) continue;
                results.push_back({RETURNFLAG_DICT[rf], LINESTATUS_DICT[ls], rf, ls, &global[gid]});
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
            double sum_disc_price_out = (double)(long double)acc.sum_disc_price / 10000.0;
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
