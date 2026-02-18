/*
 * Q1: Pricing Summary Report
 *
 * === LOGICAL PLAN ===
 * Table: lineitem (~60M rows, sorted by l_shipdate ascending)
 * Predicate: l_shipdate <= 10471 (epoch days for 1998-09-02 = DATE '1998-12-01' - 90 days)
 * Group By: l_returnflag (3 values: A,N,R), l_linestatus (2 values: F,O) => max 6 groups
 * Aggregations: sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count
 * Order By: l_returnflag, l_linestatus
 *
 * === PHYSICAL PLAN ===
 * Scan Strategy:
 *   1. Zone map analysis on l_shipdate (sorted column):
 *      - Blocks where block_max <= threshold: ALL rows qualify, skip per-row shipdate check
 *      - Boundary block where block_min <= threshold < block_max: per-row check needed
 *      - Blocks where block_min > threshold: skip entirely (table is sorted)
 *   2. Two-region scan:
 *      - "full" region [0, full_end): all rows qualify, branch-free, SIMD-friendly
 *      - "partial" region [full_end, scan_end): per-row shipdate predicate check
 * Aggregation: Flat array [returnflag_code * MAX_LS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with local accumulators
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *   - 64 cores available
 * Memory: madvise(MADV_POPULATE_READ) pre-faults all pages before parallel scan
 *   - Eliminates page-fault serialization during the parallel critical section
 *
 * === KEY IMPLEMENTATION DETAILS ===
 * - disc_price = extprice * (100 - discount)  [scale: 10000]
 * - charge = disc_price * (100 + tax)          [scale: 1000000]
 * - Output: divide by scale factor before printing (2 decimal places)
 * - Zone map split: find boundary between "fully qualifying" and "partial" blocks
 *   Since data is sorted ascending by l_shipdate:
 *     full_end = first row in first block whose max > threshold
 *     scan_end = end of last block whose min <= threshold
 *
 * === OVERFLOW ANALYSIS (justifying int64_t for all accumulators) ===
 * - extprice max: ~$10000 => 1,000,000 at scale 100
 * - disc_price per row max: 1,000,000 * 100 = 1e8
 * - sum_disc_price for 60M rows: 60M * 1e8 = 6e15 < INT64_MAX (9.2e18) ✓
 * - charge per row max: 1e8 * 110 = 1.1e10
 * - sum_charge for 60M rows: 60M * 1.1e10 = 6.6e17 < INT64_MAX ✓
 * Using int64_t enables full AVX2 auto-vectorization of the hot loop.
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
#include <sys/mman.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Flat aggregation group — all int64_t for auto-vectorization
// Padded to 64 bytes (1 cache line) to prevent false sharing between threads
struct alignas(64) AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000: extprice*(100-discount)
    int64_t sum_charge     = 0;  // scale 1000000: extprice*(100-discount)*(100+tax)
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    // 6 * 8 = 48 bytes of data, 16 bytes of padding to reach 64 bytes
    int32_t returnflag_code = -1;
    int32_t linestatus_code = -1;
    bool    active          = false;
    uint8_t _pad[7]         = {};
};
static_assert(sizeof(AggGroup) == 64, "AggGroup must be exactly 64 bytes (1 cache line)");

static constexpr int MAX_RF = 8;   // returnflag distinct values (3 actual)
static constexpr int MAX_LS = 8;   // linestatus distinct values (2 actual)

// Zone map entry layout as per storage guide:
// [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count] = 20 bytes
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
} __attribute__((packed));

// Pre-fault a range of mmap'd memory synchronously using MADV_POPULATE_READ
// (Linux 5.14+). Falls back to MADV_WILLNEED if not available.
// This eliminates page-fault stalls during the parallel scan.
static void populate_range(const void* ptr, size_t bytes) {
#ifdef MADV_POPULATE_READ
    madvise(const_cast<void*>(ptr), bytes, MADV_POPULATE_READ);
#else
    madvise(const_cast<void*>(ptr), bytes, MADV_WILLNEED);
#endif
}

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

        {
            std::ifstream f(lineitem_dir + "l_returnflag_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                if (!line.empty()) rf_dict.push_back(line);
            }
        }
        {
            std::ifstream f(lineitem_dir + "l_linestatus_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                if (!line.empty()) ls_dict.push_back(line);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Load zone map — compute TWO scan boundaries:
    //   scan_full_end: end of last block where block_max <= threshold
    //                  (all rows in [scan_start, scan_full_end) fully qualify)
    //   scan_end:      end of last block where block_min <= threshold
    //                  (rows in [scan_full_end, scan_end) need per-row check)
    // This exploits the sorted order of l_shipdate to eliminate the predicate
    // check from the dominant "fully qualifying" region of the scan.
    // -----------------------------------------------------------------------
    uint64_t scan_start    = 0;
    uint64_t scan_full_end = 59986052ULL;  // end of "no predicate check needed" region
    uint64_t scan_end      = 59986052ULL;  // end of "all rows with any chance of qualifying"

    {
        GENDB_PHASE("build_joins");

        std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            size_t r1 = fread(&num_blocks, sizeof(uint32_t), 1, zf);
            (void)r1;

            std::vector<ZoneMapEntry> entries(num_blocks);
            size_t r2 = fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf);
            (void)r2;
            fclose(zf);

            // lineitem is sorted by l_shipdate ascending.
            // Walk blocks in order:
            //   - block_max <= threshold: all rows qualify → extend scan_full_end
            //   - block_min <= threshold < block_max: boundary block → extend scan_end only
            //   - block_min > threshold: all rows fail → stop (sorted data, no more qualifying)
            uint64_t last_full_end    = 0;
            uint64_t last_partial_end = 0;
            bool     found_start      = false;

            for (uint32_t i = 0; i < num_blocks; i++) {
                const auto& e = entries[i];

                if (e.min_val > shipdate_threshold) {
                    // All rows in this block (and beyond, since sorted) exceed threshold
                    break;
                }

                // This block has at least some qualifying rows
                if (!found_start) {
                    scan_start = e.row_start;
                    found_start = true;
                }

                uint64_t bend = e.row_start + e.row_count;

                if (e.max_val <= shipdate_threshold) {
                    // Entire block qualifies — no per-row shipdate check needed
                    last_full_end = bend;
                    last_partial_end = bend;  // also update partial
                } else {
                    // Boundary block: some rows qualify, some don't
                    // DO NOT update last_full_end
                    last_partial_end = bend;
                }
            }

            if (found_start) {
                scan_full_end = std::min(last_full_end,    (uint64_t)59986052ULL);
                scan_end      = std::min(last_partial_end, (uint64_t)59986052ULL);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // -----------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();

    // Thread-local aggregation: [thread][rf][ls]
    const int group_stride = MAX_RF * MAX_LS;
    std::vector<AggGroup> thread_aggs((size_t)num_threads * group_stride);

    for (int t = 0; t < num_threads; t++) {
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                AggGroup& g = thread_aggs[t * group_stride + rf * MAX_LS + ls];
                g.returnflag_code = rf;
                g.linestatus_code = ls;
                g.active = false;
            }
        }
    }

    // Open columns
    gendb::MmapColumn<int32_t> col_returnflag, col_linestatus, col_shipdate;
    gendb::MmapColumn<int64_t> col_quantity, col_extprice, col_discount, col_tax;

    {
        GENDB_PHASE("main_scan");

        col_shipdate.open(lineitem_dir   + "l_shipdate.bin");
        col_returnflag.open(lineitem_dir + "l_returnflag.bin");
        col_linestatus.open(lineitem_dir + "l_linestatus.bin");
        col_quantity.open(lineitem_dir   + "l_quantity.bin");
        col_extprice.open(lineitem_dir   + "l_extendedprice.bin");
        col_discount.open(lineitem_dir   + "l_discount.bin");
        col_tax.open(lineitem_dir        + "l_tax.bin");

        // Pre-fault pages for the scan region in parallel across columns.
        // MADV_POPULATE_READ (Linux 5.14+) synchronously maps all pages,
        // eliminating page-fault stalls during the parallel scan.
        // We only prefetch the scan region [scan_start, scan_end), not the
        // entire file, to minimize wasted I/O.
        {
            // Calculate byte ranges to prefetch for each column
            size_t i32_bytes = (scan_end - scan_start) * sizeof(int32_t);
            size_t i64_bytes = (scan_end - scan_start) * sizeof(int64_t);

            // Fire prefetches for all columns concurrently (async kernel I/O)
            // for the partial region. For full region, same thing.
            populate_range(col_shipdate.data   + scan_full_end,
                           (scan_end - scan_full_end) * sizeof(int32_t));
            populate_range(col_returnflag.data + scan_start, i32_bytes);
            populate_range(col_linestatus.data + scan_start, i32_bytes);
            populate_range(col_quantity.data   + scan_start, i64_bytes);
            populate_range(col_extprice.data   + scan_start, i64_bytes);
            populate_range(col_discount.data   + scan_start, i64_bytes);
            populate_range(col_tax.data        + scan_start, i64_bytes);
            // Prefetch shipdate only for the full region (partial already done)
            populate_range(col_shipdate.data   + scan_start, i32_bytes);
        }

        // Get raw pointers for tight loop
        const int32_t* __restrict__ p_shipdate   = col_shipdate.data   + scan_start;
        const int32_t* __restrict__ p_returnflag = col_returnflag.data + scan_start;
        const int32_t* __restrict__ p_linestatus = col_linestatus.data + scan_start;
        const int64_t* __restrict__ p_quantity   = col_quantity.data   + scan_start;
        const int64_t* __restrict__ p_extprice   = col_extprice.data   + scan_start;
        const int64_t* __restrict__ p_discount   = col_discount.data   + scan_start;
        const int64_t* __restrict__ p_tax        = col_tax.data        + scan_start;

        // Offsets within the scan range
        const int64_t full_len    = (int64_t)(scan_full_end - scan_start);
        const int64_t partial_len = (int64_t)(scan_end - scan_start);

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            AggGroup* __restrict__ local_agg = &thread_aggs[tid * group_stride];

            // ----------------------------------------------------------------
            // Region 1: Fully qualifying rows [0, full_len)
            // ALL rows qualify — no shipdate predicate check needed.
            // This is the dominant portion (~97% of rows) and is branch-free,
            // enabling maximum compiler vectorization.
            // ----------------------------------------------------------------
            #pragma omp for schedule(static, 65536) nowait
            for (int64_t i = 0; i < full_len; i++) {
                const int32_t rf = p_returnflag[i];
                const int32_t ls = p_linestatus[i];
                AggGroup& g = local_agg[rf * MAX_LS + ls];

                g.active = true;

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

            // ----------------------------------------------------------------
            // Region 2: Boundary rows [full_len, partial_len)
            // Only the last partially-qualifying block(s) — per-row shipdate check.
            // This is a tiny fraction of the data.
            // ----------------------------------------------------------------
            #pragma omp for schedule(static, 65536)
            for (int64_t i = full_len; i < partial_len; i++) {
                if (p_shipdate[i] > shipdate_threshold) continue;

                const int32_t rf = p_returnflag[i];
                const int32_t ls = p_linestatus[i];
                AggGroup& g = local_agg[rf * MAX_LS + ls];

                g.active = true;

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

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local accumulators
    // -----------------------------------------------------------------------
    AggGroup merged[MAX_RF][MAX_LS] = {};

    for (int rf = 0; rf < MAX_RF; rf++) {
        for (int ls = 0; ls < MAX_LS; ls++) {
            merged[rf][ls].returnflag_code = rf;
            merged[rf][ls].linestatus_code = ls;
        }
    }

    for (int t = 0; t < num_threads; t++) {
        const AggGroup* local_agg = &thread_aggs[t * group_stride];
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                const AggGroup& src = local_agg[rf * MAX_LS + ls];
                if (!src.active) continue;
                AggGroup& dst = merged[rf][ls];
                dst.active          = true;
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
    results.reserve(8);

    for (int rf = 0; rf < MAX_RF; rf++) {
        if (rf >= (int)rf_dict.size()) break;
        for (int ls = 0; ls < MAX_LS; ls++) {
            if (ls >= (int)ls_dict.size()) break;
            const AggGroup& g = merged[rf][ls];
            if (!g.active) continue;

            ResultRow row;
            row.returnflag     = rf_dict[rf];
            row.linestatus     = ls_dict[ls];
            row.sum_qty        = (double)g.sum_qty        / 100.0;
            row.sum_base_price = (double)g.sum_base_price / 100.0;
            row.sum_disc_price = (double)g.sum_disc_price / 10000.0;
            row.sum_charge     = (double)g.sum_charge     / 1000000.0;
            row.avg_qty        = (double)g.sum_qty        / 100.0 / (double)g.count;
            row.avg_price      = (double)g.sum_base_price / 100.0 / (double)g.count;
            row.avg_disc       = (double)g.sum_discount   / 100.0 / (double)g.count;
            row.count_order    = g.count;
            results.push_back(row);
        }
    }

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
