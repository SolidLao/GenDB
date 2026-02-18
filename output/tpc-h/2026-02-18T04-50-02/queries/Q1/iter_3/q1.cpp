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
 *   - Zone map gives us: fully-qualifying blocks (max <= threshold) and the boundary block.
 *   - For fully-qualifying blocks: skip shipdate predicate entirely => tighter loop, SIMD-vectorizable.
 *   - For the boundary block: check predicate per-row.
 * Aggregation: Flat array [returnflag_code * MAX_LS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with local accumulators
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *   - 64 cores available, target ~65536 row morsels
 *
 * === KEY IMPLEMENTATION DETAILS ===
 * - CRITICAL: Use int64_t (NOT __int128) for all accumulators to enable AVX2 vectorization.
 *   Overflow analysis for SF10:
 *   - max extprice in scale-100 units: ~10,494,950 (from TPC-H spec)
 *   - disc_price = extprice * (100 - discount), max: 10,494,950 * 100 = 1,049,495,000
 *   - charge = disc_price * (100 + tax), max: 1,049,495,000 * 110 = ~1.154e11
 *   - sum_charge over 60M rows: 60M * 1.154e11 = ~6.9e18 < 9.22e18 (INT64_MAX). Safe.
 * - Two-loop strategy per block range:
 *   (A) "Clean" loop for rows where zone map guarantees all pass (block_max <= threshold):
 *       No per-row predicate check, tight loop, compiler can vectorize.
 *   (B) "Boundary" loop for the last block (block_max > threshold):
 *       Per-row predicate check on shipdate column.
 * - AggGroup uses only int64_t hot fields, no metadata in hot struct.
 * - Active-flag check removed from inner loop; groups initialized once before loop.
 * - l_extendedprice * (1 - l_discount): both scaled by 100, product at scale 10000
 *   => disc_price = extprice * (100 - discount)  [scale: 10000]
 * - l_extendedprice * (1 - l_discount) * (1 + l_tax):
 *   => charge = extprice * (100 - discount) * (100 + tax) [scale: 1000000]
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

// Hot aggregation struct — all int64_t for SIMD-friendly vectorization.
// No metadata fields here to avoid cache-line pollution.
struct alignas(64) AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000
    int64_t sum_charge     = 0;  // scale 1000000
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    // Pad to full cache line (6 int64 = 48 bytes, pad 2 more)
    int64_t _pad[2]        = {0, 0};
};

static constexpr int MAX_RF = 8;   // returnflag distinct values (actual: 3)
static constexpr int MAX_LS = 8;   // linestatus distinct values (actual: 2)

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
    // Phase 2: Load zone map — split blocks into "clean" and "boundary" ranges
    //   clean:    block_max <= threshold => all rows pass, no per-row check needed
    //   boundary: block_min <= threshold < block_max => per-row check required
    //   skip:     block_min > threshold => all rows fail, skip entirely
    // -----------------------------------------------------------------------

    // Each scan segment: {row_start, row_count, is_boundary}
    struct ScanSegment {
        uint64_t row_start;
        uint64_t row_count;
        bool is_boundary;
    };
    std::vector<ScanSegment> segments;
    uint64_t total_rows = 59986052ULL;

    {
        GENDB_PHASE("build_joins");

        std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            fread(&num_blocks, sizeof(uint32_t), 1, zf);
            std::vector<ZoneMapEntry> entries(num_blocks);
            fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf);
            fclose(zf);

            // Track contiguous clean range to merge into single segment
            uint64_t clean_start = UINT64_MAX;
            uint64_t clean_end   = 0;

            auto flush_clean = [&]() {
                if (clean_start != UINT64_MAX) {
                    segments.push_back({clean_start, clean_end - clean_start, false});
                    clean_start = UINT64_MAX;
                    clean_end   = 0;
                }
            };

            for (uint32_t i = 0; i < num_blocks; i++) {
                const auto& e = entries[i];
                uint64_t bstart = e.row_start;
                uint64_t bend   = std::min((uint64_t)(e.row_start + e.row_count), total_rows);

                if (e.min_val > shipdate_threshold) {
                    // All rows fail — skip; flush accumulated clean range
                    flush_clean();
                } else if (e.max_val <= shipdate_threshold) {
                    // All rows pass — clean block, extend clean range
                    if (clean_start == UINT64_MAX) {
                        clean_start = bstart;
                        clean_end   = bend;
                    } else {
                        clean_end = std::max(clean_end, bend);
                    }
                } else {
                    // Boundary block: min <= threshold < max
                    flush_clean();
                    segments.push_back({bstart, bend - bstart, true});
                }
            }
            flush_clean();
        } else {
            // No zone map: scan everything as boundary (safe fallback)
            segments.push_back({0, total_rows, true});
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // -----------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();
    const int group_stride = MAX_RF * MAX_LS;
    // Cache-line-aligned thread-local agg arrays
    std::vector<AggGroup> thread_aggs(num_threads * group_stride);

    // Open columns (mmap — zero copy)
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

        // Prefetch all columns into page cache
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

        // Process each segment
        for (const auto& seg : segments) {
            const uint64_t rstart     = seg.row_start;
            const int64_t  range_size = (int64_t)seg.row_count;

            const int32_t* p_shipdate   = col_shipdate.data   + rstart;
            const int32_t* p_returnflag = col_returnflag.data + rstart;
            const int32_t* p_linestatus = col_linestatus.data + rstart;
            const int64_t* p_quantity   = col_quantity.data   + rstart;
            const int64_t* p_extprice   = col_extprice.data   + rstart;
            const int64_t* p_discount   = col_discount.data   + rstart;
            const int64_t* p_tax        = col_tax.data        + rstart;

            if (seg.is_boundary) {
                // Boundary segment: per-row predicate check. Usually small (1 block = 65536 rows).
                // Run in parallel only if large enough; for single-block boundary, single-thread is fine.
                #pragma omp parallel
                {
                    const int tid = omp_get_thread_num();
                    AggGroup* local_agg = &thread_aggs[tid * group_stride];
                    #pragma omp for schedule(static, 65536)
                    for (int64_t i = 0; i < range_size; i++) {
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
                        const int64_t disc_price = extprice * (100LL - discount);
                        g.sum_disc_price += disc_price;
                        g.sum_charge     += disc_price * (100LL + tax);
                    }
                }
            } else {
                // Clean segment: all rows pass predicate — no shipdate check in inner loop.
                // Compiler can auto-vectorize this loop (no branches, all int64_t, no __int128).
                #pragma omp parallel
                {
                    const int tid = omp_get_thread_num();
                    AggGroup* local_agg = &thread_aggs[tid * group_stride];
                    #pragma omp for schedule(static, 65536)
                    for (int64_t i = 0; i < range_size; i++) {
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
                        const int64_t disc_price = extprice * (100LL - discount);
                        g.sum_disc_price += disc_price;
                        g.sum_charge     += disc_price * (100LL + tax);
                    }
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

    for (int rf = 0; rf < (int)rf_dict.size(); rf++) {
        for (int ls = 0; ls < (int)ls_dict.size(); ls++) {
            AggGroup& g = merged[rf][ls];
            if (g.count == 0) continue;

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

    // Sort by returnflag, then linestatus
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
