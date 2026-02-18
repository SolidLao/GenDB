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
 * Scan Strategy: Zone map pruning on l_shipdate (sorted column) - split into two regions:
 *   1. [0, safe_end): blocks where ALL rows have shipdate <= threshold (max_val <= threshold)
 *      => NO per-row shipdate check, NO shipdate column access — pure aggregation kernel
 *   2. [safe_end, scan_end): boundary block(s) with min_val <= threshold < max_val
 *      => per-row shipdate check required (small region, typically 1 block = 65536 rows)
 *   - This eliminates the shipdate predicate branch and memory read for ~99% of rows
 *   - madvise(MADV_DONTNEED) on the tail to reduce OS page cache pressure
 * Aggregation: Flat array [returnflag_code * MAX_LINESTATUS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with local accumulators
 *   - Hot struct: only int64_t accumulators, cache-line aligned
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *   - 64 cores available; schedule(static) for contiguous memory access
 *   - Columns opened and prefetched in dim_filter phase to overlap HDD I/O with setup
 *
 * === KEY IMPLEMENTATION DETAILS ===
 * - l_extendedprice * (1 - l_discount): both scaled by 100, so product is scale 10000
 *   => disc_price = extprice * (100 - discount)  [scale: 10000]
 * - l_extendedprice * (1 - l_discount) * (1 + l_tax): add tax
 *   => charge = extprice * (100 - discount) * (100 + tax)  [scale: 1000000]
 * - Output: divide by scale factor before printing (2 decimal places)
 *
 * === OVERFLOW ANALYSIS (justifying int64_t for all accumulators) ===
 * - extprice max: ~$10000 => 1,000,000 at scale 100
 * - disc_price per row max: 1,000,000 * 100 = 1e8
 * - sum_disc_price for 60M rows: 60M * 1e8 = 6e15 < INT64_MAX (9.2e18) ✓
 * - charge per row max: 1e8 * 110 = 1.1e10
 * - sum_charge for 60M rows: 60M * 1.1e10 = 6.6e17 < INT64_MAX ✓
 * Using int64_t enables full AVX2 auto-vectorization of the hot loop.
 *
 * === ITERATION 10 OPTIMIZATIONS ===
 * - Split scan into safe region (no predicate) + boundary region (with predicate)
 *   => Eliminates shipdate column access for ~99% of rows
 *   => Reduces memory bandwidth: 6 columns instead of 7 for the bulk scan
 *   => Eliminates branch misprediction in the hot loop
 * - Template parameter NeedPredicate=false for bulk loop: compiler generates
 *   a specialized tight inner loop with no shipdate read/compare
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

// Hot aggregation struct: only int64_t accumulators, cache-line aligned.
// 6 fields * 8 bytes = 48 bytes, padded to 64 bytes (one cache line).
// Keeps false-sharing away with alignas(64).
struct alignas(64) AggGroup {
    int64_t sum_qty        = 0;  // scale 100
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000: extprice*(100-discount)
    int64_t sum_charge     = 0;  // scale 1000000: extprice*(100-discount)*(100+tax)
    int64_t sum_discount   = 0;  // scale 100
    int64_t count          = 0;
    // 48 bytes used; 16 bytes padding to reach 64-byte cache line
    int64_t _pad0          = 0;
    int64_t _pad1          = 0;
};

static_assert(sizeof(AggGroup) == 64, "AggGroup must be exactly 64 bytes (1 cache line)");

static constexpr int MAX_RF = 8;   // returnflag distinct values (3 actual)
static constexpr int MAX_LS = 8;   // linestatus distinct values (2 actual)
static constexpr int N_GROUPS = MAX_RF * MAX_LS;  // = 64

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

    // Open all columns early — fires MADV_SEQUENTIAL which triggers OS readahead.
    // Do this in dim_filter phase so HDD I/O overlaps with dictionary + zone map work.
    gendb::MmapColumn<int32_t> col_shipdate, col_returnflag, col_linestatus;
    gendb::MmapColumn<int64_t> col_quantity, col_extprice, col_discount, col_tax;

    // -----------------------------------------------------------------------
    // Phase 1: Load dictionary files + open all columns + fire prefetch
    // -----------------------------------------------------------------------
    std::vector<std::string> rf_dict, ls_dict;
    {
        GENDB_PHASE("dim_filter");

        // Open all columns now — triggers OS sequential readahead (MADV_SEQUENTIAL in open())
        col_shipdate.open(lineitem_dir   + "l_shipdate.bin");
        col_returnflag.open(lineitem_dir + "l_returnflag.bin");
        col_linestatus.open(lineitem_dir + "l_linestatus.bin");
        col_quantity.open(lineitem_dir   + "l_quantity.bin");
        col_extprice.open(lineitem_dir   + "l_extendedprice.bin");
        col_discount.open(lineitem_dir   + "l_discount.bin");
        col_tax.open(lineitem_dir        + "l_tax.bin");

        // Fire MADV_WILLNEED on all columns — tells OS to bring pages into page cache now.
        // This overlaps HDD I/O with the dictionary load + zone map processing below.
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

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
    // safe_end: rows [0, safe_end) are ALL guaranteed to pass the shipdate predicate
    // (blocks where max_val <= threshold). No per-row check needed in this range.
    // Rows [safe_end, scan_end) are in boundary blocks that need per-row checking.
    uint64_t safe_end   = 0;

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

            // Lineitem is sorted by l_shipdate ascending.
            // We distinguish two regions:
            //   - SAFE blocks: max_val <= threshold => ALL rows qualify (no per-row check)
            //   - BOUNDARY blocks: min_val <= threshold < max_val => per-row check needed
            //   - SKIP blocks: min_val > threshold => stop scanning

            uint64_t range_end = 0;

            for (uint32_t i = 0; i < num_blocks; i++) {
                const auto& e = entries[i];
                if (e.min_val > shipdate_threshold) {
                    // All rows in this block exceed threshold — stop
                    break;
                }
                uint64_t bend = e.row_start + e.row_count;
                if (bend > range_end) range_end = bend;

                if (e.max_val <= shipdate_threshold) {
                    // All rows in this block qualify — safe to scan without per-row check
                    if (bend > safe_end) safe_end = bend;
                }
                // else: boundary block — rows [safe_end, bend) need per-row check
            }

            if (range_end > 0) {
                scan_start = 0;
                scan_end   = std::min(range_end, (uint64_t)59986052ULL);
            }
            safe_end = std::min(safe_end, scan_end);

            // Precise MADV_DONTNEED on column tail pages beyond scan_end
            // Only release if we actually have a meaningful tail (saves cache pollution)
            const size_t page_size = 4096;
            auto dontneed_tail = [&](const void* col_data, size_t elem_size, size_t col_count) {
                if (!col_data || scan_end >= col_count) return;
                size_t tail_byte_start = scan_end * elem_size;
                size_t aligned_start = (tail_byte_start + page_size - 1) & ~(page_size - 1);
                size_t total_bytes = col_count * elem_size;
                if (aligned_start < total_bytes) {
                    madvise(const_cast<void*>(reinterpret_cast<const void*>(
                                static_cast<const char*>(col_data) + aligned_start)),
                            total_bytes - aligned_start,
                            MADV_DONTNEED);
                }
            };

            dontneed_tail(col_shipdate.data,   sizeof(int32_t), col_shipdate.count);
            dontneed_tail(col_returnflag.data,  sizeof(int32_t), col_returnflag.count);
            dontneed_tail(col_linestatus.data,  sizeof(int32_t), col_linestatus.count);
            dontneed_tail(col_quantity.data,    sizeof(int64_t), col_quantity.count);
            dontneed_tail(col_extprice.data,    sizeof(int64_t), col_extprice.count);
            dontneed_tail(col_discount.data,    sizeof(int64_t), col_discount.count);
            dontneed_tail(col_tax.data,         sizeof(int64_t), col_tax.count);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan - parallel aggregation
    // Split into two regions for maximum throughput:
    //   Region A [scan_start, safe_end): ALL rows qualify — no shipdate check needed
    //   Region B [safe_end, scan_end):  boundary rows — per-row shipdate check
    // -----------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();

    // Thread-local aggregation: flat [thread * N_GROUPS + rf * MAX_LS + ls]
    // Each AggGroup is 64 bytes (one cache line) — no false sharing between threads.
    std::vector<AggGroup> thread_aggs((size_t)num_threads * N_GROUPS);

    {
        GENDB_PHASE("main_scan");

        // Raw pointers for tight loop — avoids operator[] overhead, enables __restrict__
        // Note: p_shipdate only needed in region B (boundary check) — not loaded in region A
        const int32_t* __restrict__ p_shipdate   = col_shipdate.data   + scan_start;
        const int32_t* __restrict__ p_returnflag = col_returnflag.data + scan_start;
        const int32_t* __restrict__ p_linestatus = col_linestatus.data + scan_start;
        const int64_t* __restrict__ p_quantity   = col_quantity.data   + scan_start;
        const int64_t* __restrict__ p_extprice   = col_extprice.data   + scan_start;
        const int64_t* __restrict__ p_discount   = col_discount.data   + scan_start;
        const int64_t* __restrict__ p_tax        = col_tax.data        + scan_start;

        // Sizes of each region (relative to scan_start offset)
        const int64_t safe_size     = (int64_t)(safe_end - scan_start);
        const int64_t boundary_size = (int64_t)(scan_end - safe_end);
        // Pointer to boundary region
        const int32_t* __restrict__ pb_shipdate   = p_shipdate   + safe_size;
        const int32_t* __restrict__ pb_returnflag = p_returnflag + safe_size;
        const int32_t* __restrict__ pb_linestatus = p_linestatus + safe_size;
        const int64_t* __restrict__ pb_quantity   = p_quantity   + safe_size;
        const int64_t* __restrict__ pb_extprice   = p_extprice   + safe_size;
        const int64_t* __restrict__ pb_discount   = p_discount   + safe_size;
        const int64_t* __restrict__ pb_tax        = p_tax        + safe_size;

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            AggGroup* __restrict__ local_agg = &thread_aggs[(size_t)tid * N_GROUPS];

            // === Region A: Safe zone — NO per-row shipdate check, NO shipdate memory access ===
            // Compiler can fully vectorize/unroll this loop with no branches.
            #pragma omp for schedule(static) nowait
            for (int64_t i = 0; i < safe_size; i++) {
                const int32_t rf = p_returnflag[i];
                const int32_t ls = p_linestatus[i];
                AggGroup& g = local_agg[rf * MAX_LS + ls];

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

            // === Region B: Boundary zone — per-row shipdate predicate check ===
            // This region is typically 1 block = 65536 rows, tiny fraction of total.
            #pragma omp for schedule(static)
            for (int64_t i = 0; i < boundary_size; i++) {
                if (__builtin_expect(pb_shipdate[i] > shipdate_threshold, 0)) continue;

                const int32_t rf = pb_returnflag[i];
                const int32_t ls = pb_linestatus[i];
                AggGroup& g = local_agg[rf * MAX_LS + ls];

                const int64_t qty      = pb_quantity[i];
                const int64_t extprice = pb_extprice[i];
                const int64_t discount = pb_discount[i];
                const int64_t tax      = pb_tax[i];

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
    // Track which (rf, ls) groups are active using a separate bool array
    // (keeps the hot AggGroup struct clean)
    bool group_active[MAX_RF][MAX_LS] = {};

    AggGroup merged[MAX_RF * MAX_LS] = {};

    for (int t = 0; t < num_threads; t++) {
        const AggGroup* local_agg = &thread_aggs[(size_t)t * N_GROUPS];
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                const AggGroup& src = local_agg[rf * MAX_LS + ls];
                if (src.count == 0) continue;
                AggGroup& dst = merged[rf * MAX_LS + ls];
                group_active[rf][ls]     = true;
                dst.sum_qty             += src.sum_qty;
                dst.sum_base_price      += src.sum_base_price;
                dst.sum_disc_price      += src.sum_disc_price;
                dst.sum_charge          += src.sum_charge;
                dst.sum_discount        += src.sum_discount;
                dst.count               += src.count;
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
            if (!group_active[rf][ls]) continue;

            const AggGroup& g = merged[rf * MAX_LS + ls];

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
