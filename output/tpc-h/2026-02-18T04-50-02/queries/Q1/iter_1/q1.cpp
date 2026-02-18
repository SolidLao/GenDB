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
 * Scan Strategy: Zone map pruning on l_shipdate (sorted column).
 *   - lineitem is sorted ascending by l_shipdate.
 *   - Binary search zone map entries to find exact cutoff block where min > threshold.
 *   - All blocks from that point onward are skipped (single contiguous range to scan).
 * Aggregation: Flat array [returnflag_code * MAX_LINESTATUS + linestatus_code]
 *   - Only 6 groups max => flat array, zero-contention with thread-local accumulators.
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end.
 *   - 64 cores, morsel size 131072 rows (good L3 cache fit).
 * Data types: ALL accumulators use int64_t (NOT __int128).
 *   - disc_price max = 10^5 * 100 = 10^7 per row; 60M rows => 6×10^14 << INT64_MAX(9.2×10^18) ✓
 *   - charge max = 10^7 * 110 = 1.1×10^9 per row; 60M rows => 6.6×10^16 << INT64_MAX ✓
 *   - Using int64_t enables compiler auto-vectorization (AVX2: 4 int64 per instruction).
 * False sharing prevention: AggGroup padded to full cache line (64 bytes per group entry).
 *
 * === KEY IMPLEMENTATION DETAILS ===
 * - disc_price = extprice * (100 - discount)  [scale: 10000]
 * - charge = extprice * (100 - discount) * (100 + tax)  [scale: 1000000]
 * - hot loop: no branch for group activation check — handled outside hot path
 * - __restrict__ pointers + simple loop body → helps compiler SIMD auto-vectorize
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
// Max 3 * 2 = 6 groups => flat array of size MAX_RF * MAX_LS

// int64_t range check for accumulators (SF=10):
//   sum_disc_price: max extprice ~103950 (scale 100), max (100-disc)=100
//     => per row: 103950 * 100 = 1.04e7 ; 60M rows: 6.2e14 < 9.2e18 (INT64_MAX) ✓
//   sum_charge: max disc_price ~1.04e7, max (100+tax)=110
//     => per row: 1.04e7 * 110 = 1.14e9 ; 60M rows: 6.9e16 < 9.2e18 (INT64_MAX) ✓
// All accumulators safely use int64_t — avoids __int128 which blocks vectorization.

struct AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000; fits in int64_t (see above)
    int64_t sum_charge     = 0;  // scale 1000000; fits in int64_t (see above)
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    int returnflag_code    = -1;
    int linestatus_code    = -1;
    bool active            = false;
    // Pad struct to 64 bytes (cache line) to prevent false sharing.
    // Layout: 6×int64(48) + 2×int32(8) + 1×bool(1) = 57 bytes → pad 7 bytes → 64 bytes total.
    char _pad[7];
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
    uint64_t scan_start = 0;
    uint64_t scan_end   = 59986052ULL;
    const uint64_t total_rows = 59986052ULL;

    {
        GENDB_PHASE("build_joins");

        // Read zone map header: uint32_t num_blocks
        std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            (void)fread(&num_blocks, sizeof(uint32_t), 1, zf);

            // Read all entries
            std::vector<ZoneMapEntry> entries(num_blocks);
            (void)fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf);
            fclose(zf);

            // lineitem is sorted ascending by l_shipdate.
            // Find the first block where min_val > threshold — all subsequent blocks can be skipped.
            // Binary search since blocks are sorted by date.
            int32_t lo = 0, hi = (int32_t)num_blocks;
            while (lo < hi) {
                int32_t mid = lo + (hi - lo) / 2;
                if (entries[mid].min_val <= shipdate_threshold)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            // lo = index of first block where min_val > threshold
            // All blocks [0, lo) may have qualifying rows (scan up to the end of block lo-1).
            // Also handle boundary block lo-1 which may be partial (mixed rows).
            if (lo > 0) {
                scan_start = entries[0].row_start;
                // end of last qualifying block
                scan_end = std::min(entries[lo - 1].row_start + (uint64_t)entries[lo - 1].row_count,
                                    total_rows);
                // If block lo exists and its max <= threshold, it also qualifies; but
                // binary search already handles this since we keyed on min_val.
                // For blocks with min<=threshold but max>threshold, we still scan them
                // and rely on per-row predicate. That's correct.
            } else {
                // No block has min <= threshold — entire table fails predicate
                scan_start = 0;
                scan_end   = 0;
            }
        }
        // else: scan full table (defaults already set)
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // -----------------------------------------------------------------------
    // Flat array: agg[rf_code][ls_code]
    // Thread-local accumulators padded to avoid false sharing, merge at end.

    const int num_threads = omp_get_max_threads();

    // Thread-local aggregation: [thread][rf][ls]
    // group_stride = MAX_RF * MAX_LS entries per thread
    const int group_stride = MAX_RF * MAX_LS;
    // Allocate with alignment to prevent false sharing across threads
    std::vector<AggGroup> thread_aggs(num_threads * group_stride);

    // Open columns via mmap for zero-copy access
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

        // Prefetch all columns into OS page cache
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

        if (scan_end > scan_start) {
            const int64_t range_size = (int64_t)(scan_end - scan_start);

            // Raw pointers with __restrict__ for auto-vectorization
            const int32_t* __restrict__ p_shipdate   = &col_shipdate[scan_start];
            const int32_t* __restrict__ p_returnflag = &col_returnflag[scan_start];
            const int32_t* __restrict__ p_linestatus = &col_linestatus[scan_start];
            const int64_t* __restrict__ p_quantity   = &col_quantity[scan_start];
            const int64_t* __restrict__ p_extprice   = &col_extprice[scan_start];
            const int64_t* __restrict__ p_discount   = &col_discount[scan_start];
            const int64_t* __restrict__ p_tax        = &col_tax[scan_start];

            // Morsel size: 131072 rows (~1MB per int32 column, good L3 cache utilization)
            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                AggGroup* __restrict__ local_agg = &thread_aggs[tid * group_stride];

                // Initialize group codes in thread-local array once
                for (int rf = 0; rf < MAX_RF; rf++) {
                    for (int ls = 0; ls < MAX_LS; ls++) {
                        local_agg[rf * MAX_LS + ls].returnflag_code = rf;
                        local_agg[rf * MAX_LS + ls].linestatus_code = ls;
                    }
                }

                #pragma omp for schedule(static, 131072)
                for (int64_t i = 0; i < range_size; i++) {
                    // Per-row predicate: skip rows where shipdate > threshold
                    if (p_shipdate[i] > shipdate_threshold) continue;

                    const int rf = p_returnflag[i];
                    const int ls = p_linestatus[i];
                    AggGroup& g  = local_agg[rf * MAX_LS + ls];

                    const int64_t qty      = p_quantity[i];
                    const int64_t extprice = p_extprice[i];
                    const int64_t discount = p_discount[i];
                    const int64_t tax      = p_tax[i];

                    g.sum_qty        += qty;
                    g.sum_base_price += extprice;
                    g.sum_discount   += discount;
                    g.count          += 1;

                    // disc_price = extprice * (100 - discount) [scale: 10000]
                    // int64_t is safe: max 103950*100 * 60M < 9.2e18
                    const int64_t disc_price = extprice * (100LL - discount);
                    g.sum_disc_price += disc_price;

                    // charge = disc_price * (100 + tax) [scale: 1000000]
                    // int64_t is safe: max 1.04e7 * 110 * 60M ~ 6.9e16 < 9.2e18
                    g.sum_charge += disc_price * (100LL + tax);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local accumulators
    // -----------------------------------------------------------------------
    AggGroup merged[MAX_RF][MAX_LS] = {};
    // Pre-set group codes in merged array
    for (int rf = 0; rf < MAX_RF; rf++)
        for (int ls = 0; ls < MAX_LS; ls++) {
            merged[rf][ls].returnflag_code = rf;
            merged[rf][ls].linestatus_code = ls;
        }

    for (int t = 0; t < num_threads; t++) {
        AggGroup* local_agg = &thread_aggs[t * group_stride];
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                AggGroup& src = local_agg[rf * MAX_LS + ls];
                if (src.count == 0) continue;  // skip unused groups
                AggGroup& dst = merged[rf][ls];
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
            if (g.count == 0) continue;  // skip unused groups

            ResultRow row;
            row.rf_code      = rf;
            row.ls_code      = ls;
            row.returnflag   = rf_dict[rf];
            row.linestatus   = ls_dict[ls];
            row.sum_qty        = (double)g.sum_qty / 100.0;
            row.sum_base_price = (double)g.sum_base_price / 100.0;
            // sum_disc_price is at scale 10000 (int64_t)
            row.sum_disc_price = (double)g.sum_disc_price / 10000.0;
            // sum_charge is at scale 1000000 (int64_t)
            row.sum_charge     = (double)g.sum_charge / 1000000.0;
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
