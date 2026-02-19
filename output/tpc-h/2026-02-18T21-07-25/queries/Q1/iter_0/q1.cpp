#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================
// Q1: Pricing Summary Report
//
// SELECT l_returnflag, l_linestatus,
//        SUM(l_quantity), SUM(l_extendedprice),
//        SUM(l_extendedprice * (1 - l_discount)),
//        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
//        COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus
//
// Strategy:
//   - Zone-map prune trailing zones where min(l_shipdate) > 10471
//   - Morsel-driven parallel scan with thread-local AggState[6] flat arrays
//   - Composite group key = l_returnflag * 2 + l_linestatus (max 6 groups)
//   - All arithmetic in scaled int64_t (scale=100), divide at output only
// ============================================================

// DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = 10471 days since epoch
static constexpr int32_t SHIPDATE_CUTOFF = 10471;

// Number of groups: returnflag (3 values) x linestatus (2 values) = 6 max
static constexpr int NUM_GROUPS = 6;

// Aggregation state for one (returnflag, linestatus) group
// All sums are in scaled integers (scale=100 for price/qty/discount/tax)
// disc_price = sum(extendedprice * (100 - discount))  -> scale = 100*100 = 10000
// charge     = sum(extendedprice * (100 - discount) * (100 + tax)) -> scale = 100*100*100 = 1000000
struct AggState {
    int64_t sum_qty        = 0;   // scale=100
    int64_t sum_price      = 0;   // scale=100
    // Use __int128 for disc_price and charge to avoid overflow on 60M rows
    __int128 sum_disc_price = 0;  // scale=10000
    __int128 sum_charge     = 0;  // scale=1000000
    int64_t sum_discount   = 0;   // scale=100 (for avg_disc)
    int64_t count          = 0;
    // pad to avoid false sharing (128 bytes total)
    char _pad[128 - (sizeof(int64_t)*4 + sizeof(__int128)*2)];
};
static_assert(sizeof(AggState) == 128, "AggState must be 128 bytes");

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ----------------------------------------------------------------
    // Phase 1: Load columns via mmap (zero-copy)
    // ----------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate   (gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int32_t> col_returnflag (gendb_dir + "/lineitem/l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus (gendb_dir + "/lineitem/l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity   (gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice   (gendb_dir + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount   (gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax        (gendb_dir + "/lineitem/l_tax.bin");

    // ----------------------------------------------------------------
    // Phase 2: Load zone map and determine zone ranges
    // ----------------------------------------------------------------
    // Determine which zones to scan: skip zones where min > SHIPDATE_CUTOFF
    // full-pass zones where max <= SHIPDATE_CUTOFF
    // partial zones otherwise
    struct ZoneRange {
        uint32_t row_start;
        uint32_t row_end;    // exclusive
        bool     full_pass;  // true = all rows pass filter
    };

    std::vector<ZoneRange> zone_ranges;
    zone_ranges.reserve(600);

    {
        GENDB_PHASE("zone_map_load");
        // Use gendb::ZoneMapIndex which has the correct 16-byte ZoneEntry layout:
        // [int32_t min, int32_t max, uint32_t row_count, uint32_t row_offset]
        gendb::ZoneMapIndex zm(gendb_dir + "/indexes/idx_lineitem_shipdate.bin");

        for (const auto& z : zm.zones) {
            // skip: all rows in zone exceed cutoff
            if (z.min > SHIPDATE_CUTOFF) continue;

            uint32_t row_start = z.row_offset;
            uint32_t row_end   = z.row_offset + z.row_count;
            bool full = (z.max <= SHIPDATE_CUTOFF);
            zone_ranges.push_back({row_start, row_end, full});
        }
    }

    // Build flat list of morsel descriptors (per-zone for now; threads pull atomically)
    // We'll use atomic zone index counter; each thread processes one zone at a time.
    // This is fine since zones are ~100K rows each and we have 64 threads.
    // For finer granularity we could split partial zones further, but zones ~100K is good.

    // ----------------------------------------------------------------
    // Phase 3: Parallel morsel-driven scan + aggregation
    // ----------------------------------------------------------------
    const int NUM_THREADS = static_cast<int>(std::thread::hardware_concurrency());
    const int nthreads    = std::min(NUM_THREADS, 64);

    // Thread-local AggState arrays; laid out contiguously to allow easy merge
    // Use aligned allocation to avoid false sharing (each AggState is 128 bytes)
    std::vector<std::vector<AggState>> tl_agg(nthreads, std::vector<AggState>(NUM_GROUPS));

    // Pointers for raw column data (MmapColumn::data is a public field, not a method)
    const int32_t* p_shipdate   = col_shipdate.data;
    const int32_t* p_returnflag = col_returnflag.data;
    const int32_t* p_linestatus = col_linestatus.data;
    const int64_t* p_quantity   = col_quantity.data;
    const int64_t* p_extprice   = col_extprice.data;
    const int64_t* p_discount   = col_discount.data;
    const int64_t* p_tax        = col_tax.data;

    std::atomic<uint32_t> zone_cursor{0};
    const uint32_t total_zones = static_cast<uint32_t>(zone_ranges.size());

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            AggState* local = tl_agg[tid].data();

            while (true) {
                uint32_t zi = zone_cursor.fetch_add(1, std::memory_order_relaxed);
                if (zi >= total_zones) break;

                const ZoneRange& zr = zone_ranges[zi];
                const uint32_t   rs = zr.row_start;
                const uint32_t   re = zr.row_end;

                if (zr.full_pass) {
                    // All rows in zone pass the date filter — no per-row check
                    for (uint32_t r = rs; r < re; ++r) {
                        int32_t rf = p_returnflag[r];
                        int32_t ls = p_linestatus[r];
                        int32_t g  = rf * 2 + ls;

                        int64_t qty      = p_quantity[r];
                        int64_t price    = p_extprice[r];
                        int64_t disc     = p_discount[r];
                        int64_t tax      = p_tax[r];

                        // disc_price = price * (100 - disc)  [scale = 100*100 = 10000]
                        int64_t disc_price = price * (100 - disc);
                        // charge = disc_price * (100 + tax)  [scale = 10000*100 = 1000000]
                        __int128 charge = static_cast<__int128>(disc_price) * (100 + tax);

                        AggState& ag = local[g];
                        ag.sum_qty        += qty;
                        ag.sum_price      += price;
                        ag.sum_disc_price += disc_price;
                        ag.sum_charge     += charge;
                        ag.sum_discount   += disc;
                        ag.count          += 1;
                    }
                } else {
                    // Partial zone: check per row
                    for (uint32_t r = rs; r < re; ++r) {
                        if (p_shipdate[r] > SHIPDATE_CUTOFF) continue;

                        int32_t rf = p_returnflag[r];
                        int32_t ls = p_linestatus[r];
                        int32_t g  = rf * 2 + ls;

                        int64_t qty      = p_quantity[r];
                        int64_t price    = p_extprice[r];
                        int64_t disc     = p_discount[r];
                        int64_t tax      = p_tax[r];

                        int64_t disc_price = price * (100 - disc);
                        __int128 charge    = static_cast<__int128>(disc_price) * (100 + tax);

                        AggState& ag = local[g];
                        ag.sum_qty        += qty;
                        ag.sum_price      += price;
                        ag.sum_disc_price += disc_price;
                        ag.sum_charge     += charge;
                        ag.sum_discount   += disc;
                        ag.count          += 1;
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; ++t)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();
    }

    // ----------------------------------------------------------------
    // Phase 4: Merge thread-local aggregation results
    // ----------------------------------------------------------------
    std::vector<AggState> agg(NUM_GROUPS);
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; ++t) {
            for (int g = 0; g < NUM_GROUPS; ++g) {
                agg[g].sum_qty        += tl_agg[t][g].sum_qty;
                agg[g].sum_price      += tl_agg[t][g].sum_price;
                agg[g].sum_disc_price += tl_agg[t][g].sum_disc_price;
                agg[g].sum_charge     += tl_agg[t][g].sum_charge;
                agg[g].sum_discount   += tl_agg[t][g].sum_discount;
                agg[g].count          += tl_agg[t][g].count;
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 5: Load dictionaries and build result rows
    // ----------------------------------------------------------------
    // Load returnflag dictionary
    std::vector<std::string> rf_dict, ls_dict;
    {
        std::ifstream f(gendb_dir + "/lineitem/l_returnflag_dict.txt");
        std::string line;
        while (std::getline(f, line)) rf_dict.push_back(line);
    }
    {
        std::ifstream f(gendb_dir + "/lineitem/l_linestatus_dict.txt");
        std::string line;
        while (std::getline(f, line)) ls_dict.push_back(line);
    }

    // Collect non-empty groups
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int32_t     rf_code;
        int32_t     ls_code;
        int64_t     sum_qty;
        int64_t     sum_price;
        __int128    sum_disc_price;
        __int128    sum_charge;
        int64_t     sum_discount;
        int64_t     count;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_GROUPS);

    for (int rf = 0; rf < static_cast<int>(rf_dict.size()); ++rf) {
        for (int ls = 0; ls < static_cast<int>(ls_dict.size()); ++ls) {
            int g = rf * 2 + ls;
            if (g >= NUM_GROUPS) continue;
            if (agg[g].count == 0) continue;

            ResultRow row;
            row.returnflag    = rf_dict[rf];
            row.linestatus    = ls_dict[ls];
            row.rf_code       = rf;
            row.ls_code       = ls;
            row.sum_qty       = agg[g].sum_qty;
            row.sum_price     = agg[g].sum_price;
            row.sum_disc_price= agg[g].sum_disc_price;
            row.sum_charge    = agg[g].sum_charge;
            row.sum_discount  = agg[g].sum_discount;
            row.count         = agg[g].count;
            results.push_back(std::move(row));
        }
    }

    // ORDER BY l_returnflag, l_linestatus
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // ----------------------------------------------------------------
    // Phase 6: Write CSV output
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) {
            std::cerr << "Failed to open output file: " << out_path << std::endl;
            return;
        }

        // Header
        fprintf(out,
            "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
            "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : results) {
            int64_t cnt = r.count;

            // sum_qty: scale=100, display with 2 decimal places
            double sum_qty_d   = static_cast<double>(r.sum_qty)   / 100.0;
            double sum_price_d = static_cast<double>(r.sum_price)  / 100.0;

            // sum_disc_price: scale=10000
            double sum_disc_d  = static_cast<double>((__int128)r.sum_disc_price) / 10000.0;

            // sum_charge: scale=1000000
            double sum_charge_d= static_cast<double>((__int128)r.sum_charge)    / 1000000.0;

            // avg_qty: sum_qty/count (l_quantity scale_factor=1, so no 100 divisor)
            double avg_qty_d   = static_cast<double>(r.sum_qty)      / cnt;
            double avg_price_d = static_cast<double>(r.sum_price)    / (100.0 * cnt);
            double avg_disc_d  = static_cast<double>(r.sum_discount) / (100.0 * cnt);

            fprintf(out, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                r.returnflag.c_str(),
                r.linestatus.c_str(),
                sum_qty_d,
                sum_price_d,
                sum_disc_d,
                sum_charge_d,
                avg_qty_d,
                avg_price_d,
                avg_disc_d,
                (long)cnt);
        }

        fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
