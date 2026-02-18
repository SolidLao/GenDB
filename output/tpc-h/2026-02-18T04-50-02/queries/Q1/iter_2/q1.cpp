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
 * Scan Strategy: Zone map pruning on l_shipdate (sorted column)
 *   - lineitem sorted by l_shipdate ascending; once block_min > threshold, stop.
 * Aggregation: Flat array [returnflag_code * MAX_LS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with thread-local accumulators
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *   - 64 cores available; morsel = 65536 rows (fits in L3 per-thread slice)
 *
 * === ITER 2 KEY CHANGES vs ITER 0 ===
 * 1. Replace __int128 with int64_t for sum_disc_price and sum_charge:
 *    - SF10 overflow check: max disc_price per row = 10000*100 = 1,000,000 (scale 10000)
 *      Over 60M rows: 60M * 1,000,000 = 6e13 << INT64_MAX (~9.2e18). Safe.
 *    - max charge per row = disc_price_row * (100+10) = 1.1e6*100 = 1.1e8 (scale 1e6)
 *      Over 60M rows: 60M * 1.1e8 = 6.6e15 << INT64_MAX. Safe.
 *    - Enables compiler AUTO-VECTORIZATION of the entire hot loop (AVX2: 4x int64 SIMD)
 * 2. Remove `g.active` check from hot loop — initialize active flag once outside
 * 3. Use __restrict__ on column pointers to hint no aliasing -> better vectorization
 * 4. Cache-line pad AggGroup structs per thread to eliminate false sharing
 * 5. Use __builtin_expect for rarely-taken branch (most rows pass predicate after zone map skip)
 *
 * === DECIMAL HANDLING ===
 * - disc_price = extprice * (100 - discount)  [scale: 100 * 100 = 10000]
 * - charge = extprice * (100 - discount) * (100 + tax)  [scale: 100 * 100 * 100 = 1000000]
 * - Output: divide by scale factor before printing (2 decimal places)
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Group key: composite of (returnflag_code, linestatus_code)
// Max 3 * 2 = 6 groups => flat array of size 3*2=6

static constexpr int MAX_RF = 8;   // returnflag distinct values
static constexpr int MAX_LS = 8;   // linestatus distinct values
static constexpr int MAX_GROUPS = MAX_RF * MAX_LS;  // 64 entries

struct AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000 — int64_t safe for SF10
    int64_t sum_charge     = 0;  // scale 1000000 — int64_t safe for SF10
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    int returnflag_code    = -1;
    int linestatus_code    = -1;
    bool active            = false;
    // Pad to 64 bytes to avoid false sharing when multiple groups are updated
    // sizeof above fields: 5*8 + 2*4 + 1 = 49 bytes -> pad to 64
    char _pad[7];
} __attribute__((aligned(64)));

static_assert(sizeof(AggGroup) <= 64, "AggGroup must fit in one cache line");

// Per-thread block: MAX_GROUPS AggGroups. Align entire block to cache-line boundary
// to prevent false sharing between threads.
struct alignas(64) ThreadAggBlock {
    AggGroup groups[MAX_GROUPS];
};

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
    uint64_t scan_end   = 59986052ULL;  // default: full table

    {
        GENDB_PHASE("build_joins");

        std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            if (fread(&num_blocks, sizeof(uint32_t), 1, zf) == 1) {
                std::vector<ZoneMapEntry> entries(num_blocks);
                if (fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf) == num_blocks) {
                    // lineitem is sorted by l_shipdate ascending.
                    // We want rows WHERE shipdate <= threshold.
                    // Skip block if block_min > threshold (all rows fail predicate).
                    // Since sorted ascending, find the contiguous range from block 0 up to
                    // the last block that still has any qualifying rows (max >= something <= threshold).
                    // More precisely: include block if block_min <= threshold.
                    scan_start = entries[0].row_start;
                    scan_end   = 0;
                    for (uint32_t i = 0; i < num_blocks; i++) {
                        if (entries[i].min_val > shipdate_threshold) {
                            // Sorted ascending: all subsequent blocks also exceed threshold
                            break;
                        }
                        uint64_t bend = entries[i].row_start + entries[i].row_count;
                        if (bend > scan_end) scan_end = bend;
                    }
                    if (scan_end == 0) {
                        scan_end = 59986052ULL;  // fallback
                    } else {
                        // Clamp to table size
                        if (scan_end > 59986052ULL) scan_end = 59986052ULL;
                    }
                }
            }
            fclose(zf);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // -----------------------------------------------------------------------
    // Thread-local accumulators, merge at end.
    // Use cache-aligned ThreadAggBlock per thread to avoid false sharing.

    const int num_threads = omp_get_max_threads();
    std::vector<ThreadAggBlock> thread_agg_blocks(num_threads);

    // Open columns via mmap (zero-copy)
    gendb::MmapColumn<int32_t> col_shipdate, col_returnflag, col_linestatus;
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

        // Prefetch all columns into page cache asynchronously
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

        const uint64_t range_size = scan_end - scan_start;

        // Get raw __restrict__ pointers — tells compiler no aliasing between arrays
        const int32_t* __restrict__ p_shipdate   = col_shipdate.data   + scan_start;
        const int32_t* __restrict__ p_returnflag = col_returnflag.data + scan_start;
        const int32_t* __restrict__ p_linestatus = col_linestatus.data + scan_start;
        const int64_t* __restrict__ p_quantity   = col_quantity.data   + scan_start;
        const int64_t* __restrict__ p_extprice   = col_extprice.data   + scan_start;
        const int64_t* __restrict__ p_discount   = col_discount.data   + scan_start;
        const int64_t* __restrict__ p_tax        = col_tax.data        + scan_start;

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            AggGroup* __restrict__ local_agg = thread_agg_blocks[tid].groups;

            #pragma omp for schedule(static, 65536) nowait
            for (int64_t i = 0; i < (int64_t)range_size; i++) {
                // Predicate: l_shipdate <= threshold
                // After zone map pruning, most rows in the scan range qualify.
                // Still need per-row check for the boundary block.
                if (__builtin_expect(p_shipdate[i] > shipdate_threshold, 0)) continue;

                const int rf = p_returnflag[i];
                const int ls = p_linestatus[i];
                AggGroup& g  = local_agg[rf * MAX_LS + ls];

                // Mark active without branching inside aggregation
                g.returnflag_code = rf;
                g.linestatus_code = ls;
                g.active          = true;

                const int64_t qty      = p_quantity[i];
                const int64_t extprice = p_extprice[i];
                const int64_t discount = p_discount[i];
                const int64_t tax      = p_tax[i];

                g.sum_qty        += qty;
                g.sum_base_price += extprice;
                g.sum_discount   += discount;
                g.count          += 1;

                // disc_price = extprice * (100 - discount)  [scale: 10000]
                // int64_t safe: max = 1000000 * 60M = 6e13 << 9.2e18
                const int64_t disc_price = extprice * (100LL - discount);
                g.sum_disc_price += disc_price;

                // charge = disc_price * (100 + tax)  [scale: 1000000]
                // int64_t safe: max = 1.1e8 * 60M = 6.6e15 << 9.2e18
                g.sum_charge += disc_price * (100LL + tax);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local accumulators into global result
    // -----------------------------------------------------------------------
    AggGroup merged[MAX_RF][MAX_LS] = {};

    for (int t = 0; t < num_threads; t++) {
        const AggGroup* local_agg = thread_agg_blocks[t].groups;
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                const AggGroup& src = local_agg[rf * MAX_LS + ls];
                if (!src.active) continue;
                AggGroup& dst = merged[rf][ls];
                dst.active          = true;
                dst.returnflag_code = src.returnflag_code;
                dst.linestatus_code = src.linestatus_code;
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
    results.reserve(6);

    for (int rf = 0; rf < MAX_RF; rf++) {
        if (rf >= (int)rf_dict.size()) break;
        for (int ls = 0; ls < MAX_LS; ls++) {
            if (ls >= (int)ls_dict.size()) break;
            const AggGroup& g = merged[rf][ls];
            if (!g.active) continue;

            ResultRow row;
            row.rf_code        = rf;
            row.ls_code        = ls;
            row.returnflag     = rf_dict[rf];
            row.linestatus     = ls_dict[ls];
            row.sum_qty        = (double)g.sum_qty / 100.0;
            row.sum_base_price = (double)g.sum_base_price / 100.0;
            row.sum_disc_price = (double)g.sum_disc_price / 10000.0;
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
