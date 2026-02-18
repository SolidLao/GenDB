/*
 * Q1: Pricing Summary Report
 *
 * === LOGICAL PLAN ===
 * Table: lineitem (~60M rows)
 * Predicate: l_shipdate <= 10471 (epoch days for 1998-09-02 = DATE '1998-12-01' - 90 days)
 * Group By: l_returnflag (3 values: A,N,R), l_linestatus (2 values: F,O) => max 6 groups
 * Aggregations: sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count
 * Order By: l_returnflag, l_linestatus
 *
 * === PHYSICAL PLAN ===
 * Scan Strategy: Zone map pruning on l_shipdate (sorted column) - skip blocks with min > threshold
 *   - lineitem is sorted by l_shipdate, so late blocks can all be skipped
 * Aggregation: Flat array [returnflag_code * MAX_LINESTATUS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with local accumulators
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *   - 64 cores available, target ~10K-100K row morsels
 * Late Materialization: Not needed (all needed columns are integer)
 * Decimal handling: int64_t at scale 100; accumulate at scale^2 for products, divide once for output
 *
 * === KEY IMPLEMENTATION DETAILS ===
 * - l_extendedprice * (1 - l_discount): both scaled by 100, so product is scale 10000
 *   => disc_price = extprice * (100 - discount)  [scale: 100 * 100 = 10000]
 * - l_extendedprice * (1 - l_discount) * (1 + l_tax): add tax
 *   => charge = extprice * (100 - discount) * (100 + tax)  [scale: 100 * 100 * 100 = 1000000]
 * - Output: divide by scale factor before printing (2 decimal places)
 * - Zone map: skip blocks where max(shipdate) < threshold (ALL rows in block are > threshold)
 *   Wait: since sorted ascending, blocks with min > threshold are all > threshold => skip those.
 *   More precisely: skip block if block_min > threshold (all rows fail predicate).
 *   Actually for upper bound predicate <=: skip block if block_min > threshold.
 *   But we want rows WHERE shipdate <= threshold. Skip if block_min > threshold (all rows too large).
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iostream>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Group key: composite of (returnflag_code, linestatus_code)
// Max 3 * 2 = 6 groups => flat array of size 3*2=6

struct AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    // sum_disc_price = sum(extprice * (100 - discount)) => scale 10000
    __int128 sum_disc_price = 0;
    // sum_charge = sum(extprice * (100 - discount) * (100 + tax)) => scale 1000000
    __int128 sum_charge     = 0;
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    int returnflag_code    = -1;
    int linestatus_code    = -1;
    bool active            = false;
};

static constexpr int MAX_RF = 8;   // returnflag distinct values
static constexpr int MAX_LS = 8;   // linestatus distinct values

// Zone map entry layout as per storage guide:
// [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count] = 20 bytes
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
} __attribute__((packed));

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string lineitem_dir = gendb_dir + "/lineitem/";
    const std::string indexes_dir  = gendb_dir + "/indexes/";

    // Threshold: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
    const int32_t shipdate_threshold = 10471;

    // -----------------------------------------------------------------------
    // Phase 1: Load dictionary files to get string values for output
    // -----------------------------------------------------------------------
    std::vector<std::string> rf_dict, ls_dict;
    {
        GENDB_PHASE("dim_filter");

        // Load returnflag dictionary
        {
            std::ifstream f(lineitem_dir + "l_returnflag_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                if (!line.empty()) rf_dict.push_back(line);
            }
        }
        // Load linestatus dictionary
        {
            std::ifstream f(lineitem_dir + "l_linestatus_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                if (!line.empty()) ls_dict.push_back(line);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Load zone map and determine block ranges to scan
    // -----------------------------------------------------------------------
    std::vector<std::pair<uint64_t, uint64_t>> scan_ranges; // [row_start, row_end)
    uint64_t total_rows = 59986052ULL;

    {
        GENDB_PHASE("build_joins");

        // Read zone map header: uint32_t num_blocks
        std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            fread(&num_blocks, sizeof(uint32_t), 1, zf);

            // Read all entries
            std::vector<ZoneMapEntry> entries(num_blocks);
            fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf);
            fclose(zf);

            // Since lineitem is sorted by l_shipdate ascending,
            // once we see a block where min > threshold, all subsequent blocks also fail.
            // But to be safe, also handle any gaps.
            uint64_t range_start = UINT64_MAX;
            uint64_t range_end   = 0;

            for (uint32_t i = 0; i < num_blocks; i++) {
                const auto& e = entries[i];
                // Skip block if ALL rows exceed threshold (min > threshold)
                if (e.min_val > shipdate_threshold) {
                    // All rows in this block have shipdate > threshold, skip
                    continue;
                }
                // This block has at least some rows that might qualify
                uint64_t bstart = e.row_start;
                uint64_t bend   = e.row_start + e.row_count;
                if (range_start == UINT64_MAX) {
                    range_start = bstart;
                    range_end   = bend;
                } else {
                    // Extend range (since sorted, contiguous)
                    range_end = std::max(range_end, bend);
                }
            }

            if (range_start != UINT64_MAX) {
                scan_ranges.push_back({range_start, std::min(range_end, total_rows)});
            }
        } else {
            // No zone map available, scan everything
            scan_ranges.push_back({0, total_rows});
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // -----------------------------------------------------------------------
    // Flat array: agg[rf_code][ls_code]
    // Thread-local accumulators, merge at end

    const int num_threads = omp_get_max_threads();

    // Thread-local aggregation: [thread][rf][ls]
    // Using flat layout: thread * MAX_RF * MAX_LS + rf * MAX_LS + ls
    const int group_stride = MAX_RF * MAX_LS;
    std::vector<AggGroup> thread_aggs(num_threads * group_stride);

    // Open columns
    gendb::MmapColumn<int32_t> col_returnflag, col_linestatus, col_shipdate;
    gendb::MmapColumn<int64_t> col_quantity, col_extprice, col_discount, col_tax;

    {
        GENDB_PHASE("main_scan");

        col_shipdate.open(lineitem_dir + "l_shipdate.bin");
        col_returnflag.open(lineitem_dir + "l_returnflag.bin");
        col_linestatus.open(lineitem_dir + "l_linestatus.bin");
        col_quantity.open(lineitem_dir + "l_quantity.bin");
        col_extprice.open(lineitem_dir + "l_extendedprice.bin");
        col_discount.open(lineitem_dir + "l_discount.bin");
        col_tax.open(lineitem_dir + "l_tax.bin");

        // Prefetch all columns
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

        for (auto& [rstart, rend] : scan_ranges) {
            const uint64_t range_size = rend - rstart;

            // Get raw pointers for fast access
            const int32_t* p_shipdate    = &col_shipdate[rstart];
            const int32_t* p_returnflag  = &col_returnflag[rstart];
            const int32_t* p_linestatus  = &col_linestatus[rstart];
            const int64_t* p_quantity    = &col_quantity[rstart];
            const int64_t* p_extprice    = &col_extprice[rstart];
            const int64_t* p_discount    = &col_discount[rstart];
            const int64_t* p_tax         = &col_tax[rstart];

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                AggGroup* local_agg = &thread_aggs[tid * group_stride];

                #pragma omp for schedule(static, 65536)
                for (int64_t i = 0; i < (int64_t)range_size; i++) {
                    // Apply predicate
                    if (p_shipdate[i] > shipdate_threshold) continue;

                    const int rf = p_returnflag[i];
                    const int ls = p_linestatus[i];
                    AggGroup& g  = local_agg[rf * MAX_LS + ls];

                    if (!g.active) {
                        g.active          = true;
                        g.returnflag_code = rf;
                        g.linestatus_code = ls;
                    }

                    const int64_t qty       = p_quantity[i];
                    const int64_t extprice  = p_extprice[i];
                    const int64_t discount  = p_discount[i];
                    const int64_t tax       = p_tax[i];

                    g.sum_qty        += qty;
                    g.sum_base_price += extprice;
                    g.sum_discount   += discount;
                    g.count          += 1;

                    // disc_price = extprice * (100 - discount) [scale: 10000]
                    const int64_t disc_price = extprice * (100LL - discount);
                    g.sum_disc_price += disc_price;

                    // charge = extprice * (100 - discount) * (100 + tax) [scale: 1000000]
                    g.sum_charge += (__int128)disc_price * (100LL + tax);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local accumulators
    // -----------------------------------------------------------------------
    AggGroup merged[MAX_RF][MAX_LS] = {};

    for (int t = 0; t < num_threads; t++) {
        AggGroup* local_agg = &thread_aggs[t * group_stride];
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                AggGroup& src = local_agg[rf * MAX_LS + ls];
                if (!src.active) continue;
                AggGroup& dst = merged[rf][ls];
                if (!dst.active) {
                    dst.active          = true;
                    dst.returnflag_code = src.returnflag_code;
                    dst.linestatus_code = src.linestatus_code;
                }
                dst.sum_qty        += src.sum_qty;
                dst.sum_base_price += src.sum_base_price;
                dst.sum_disc_price += src.sum_disc_price;
                dst.sum_charge     += src.sum_charge;
                dst.sum_discount   += src.sum_discount;
                dst.count          += src.count;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 5: Collect active groups and sort
    // -----------------------------------------------------------------------
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int rf_code;
        int ls_code;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<ResultRow> results;

    for (int rf = 0; rf < MAX_RF; rf++) {
        if (rf >= (int)rf_dict.size()) break;
        for (int ls = 0; ls < MAX_LS; ls++) {
            if (ls >= (int)ls_dict.size()) break;
            AggGroup& g = merged[rf][ls];
            if (!g.active) continue;

            ResultRow row;
            row.rf_code      = rf;
            row.ls_code      = ls;
            row.returnflag   = rf_dict[rf];
            row.linestatus   = ls_dict[ls];
            row.sum_qty        = (double)g.sum_qty / 100.0;
            row.sum_base_price = (double)g.sum_base_price / 100.0;
            // sum_disc_price is at scale 10000
            row.sum_disc_price = (double)(int64_t)(g.sum_disc_price / 1LL) / 10000.0;
            // sum_charge is at scale 1000000
            row.sum_charge     = (double)(int64_t)(g.sum_charge / 1LL) / 1000000.0;
            row.avg_qty        = (double)g.sum_qty / 100.0 / (double)g.count;
            row.avg_price      = (double)g.sum_base_price / 100.0 / (double)g.count;
            row.avg_disc       = (double)g.sum_discount / 100.0 / (double)g.count;
            row.count_order    = g.count;
            results.push_back(row);
        }
    }

    // Sort by returnflag, then linestatus (ORDER BY l_returnflag, l_linestatus)
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // -----------------------------------------------------------------------
    // Phase 6: Output CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) {
            fprintf(stderr, "Failed to open output file: %s\n", out_path.c_str());
            return;
        }

        // Header
        fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& row : results) {
            fprintf(out, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                row.returnflag.c_str(),
                row.linestatus.c_str(),
                row.sum_qty,
                row.sum_base_price,
                row.sum_disc_price,
                row.sum_charge,
                row.avg_qty,
                row.avg_price,
                row.avg_disc,
                (long)row.count_order);
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
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
