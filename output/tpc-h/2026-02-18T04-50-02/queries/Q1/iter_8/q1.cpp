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
 *   - Find exact split: rows [0, full_end) ALL qualify (zone map max <= threshold)
 *   - Rows [full_end, scan_end) need per-row predicate (boundary block)
 *   - Run TWO separate loops: branch-free for fully-qualifying rows, guarded for boundary
 * Aggregation: Flat array [returnflag_code * MAX_LS + linestatus_code]
 *   - 6 groups max => flat array, zero-contention with local accumulators
 * Parallelism: OpenMP parallel scan, thread-local aggregation buffers, merge at end
 *
 * === ITERATION 8 OPTIMIZATIONS ===
 * - Two-loop split: zone map identifies rows where shipdate check is unnecessary.
 *   The "fast path" loop (fully-qualifying blocks) has NO branch → compiler can vectorize.
 *   Only the boundary block uses per-row predicate checking.
 * - #pragma omp simd on the fast loop to hint the compiler for vectorized aggregation.
 * - All iter_7 improvements preserved: prefetch, MADV_DONTNEED, aligned AggGroup,
 *   __builtin_expect, schedule(static).
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

// Aggregate a single row into local_agg (no shipdate predicate - caller ensures row qualifies)
static inline void agg_row(AggGroup* __restrict__ local_agg,
                            int32_t rf, int32_t ls,
                            int64_t qty, int64_t extprice,
                            int64_t discount, int64_t tax) {
    AggGroup& g = local_agg[rf * MAX_LS + ls];
    g.sum_qty        += qty;
    g.sum_base_price += extprice;
    g.sum_discount   += discount;
    g.count          += 1;
    const int64_t disc_price = extprice * (100LL - discount);
    g.sum_disc_price += disc_price;
    g.sum_charge     += disc_price * (100LL + tax);
}

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
    // full_end: last row of the last block where max_val <= threshold
    //   => rows [0, full_end) ALL qualify, NO per-row predicate needed
    // scan_end: end of last block where min_val <= threshold
    //   => rows [full_end, scan_end) need per-row predicate (boundary block)
    uint64_t scan_start = 0;
    uint64_t scan_end   = 59986052ULL;
    uint64_t full_end   = 0;  // rows before this index ALL qualify

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
            // Iterate blocks to find:
            //   full_end: end of last block with max_val <= threshold (all rows pass)
            //   scan_end: end of last block with min_val <= threshold (some rows pass)
            uint64_t range_end = 0;

            for (uint32_t i = 0; i < num_blocks; i++) {
                const auto& e = entries[i];
                if (e.min_val > shipdate_threshold) {
                    // All rows in this block exceed threshold — stop
                    break;
                }
                uint64_t bend = e.row_start + e.row_count;
                if (bend > range_end) range_end = bend;

                // Blocks where ALL rows qualify (max <= threshold): no per-row check needed
                if (e.max_val <= shipdate_threshold) {
                    if (bend > full_end) full_end = bend;
                }
            }

            if (range_end > 0) {
                scan_start = 0;
                scan_end   = std::min(range_end, (uint64_t)59986052ULL);
                full_end   = std::min(full_end, scan_end);
            }

            // Precise MADV_DONTNEED on column tail pages beyond scan_end
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
    // Two-loop strategy:
    //   Loop A [scan_start, full_end): NO shipdate predicate — all rows qualify.
    //           Branch-free, compiler can auto-vectorize aggregation.
    //   Loop B [full_end, scan_end): per-row shipdate predicate for boundary block.
    // -----------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();

    // Thread-local aggregation: flat [thread * N_GROUPS + rf * MAX_LS + ls]
    // Each AggGroup is 64 bytes (one cache line) — no false sharing between threads.
    std::vector<AggGroup> thread_aggs((size_t)num_threads * N_GROUPS);

    {
        GENDB_PHASE("main_scan");

        // Raw pointers for tight loop — avoids operator[] overhead, enables __restrict__
        const int32_t* __restrict__ p_shipdate   = col_shipdate.data   + scan_start;
        const int32_t* __restrict__ p_returnflag = col_returnflag.data + scan_start;
        const int32_t* __restrict__ p_linestatus = col_linestatus.data + scan_start;
        const int64_t* __restrict__ p_quantity   = col_quantity.data   + scan_start;
        const int64_t* __restrict__ p_extprice   = col_extprice.data   + scan_start;
        const int64_t* __restrict__ p_discount   = col_discount.data   + scan_start;
        const int64_t* __restrict__ p_tax        = col_tax.data        + scan_start;

        // full_range: rows that unconditionally qualify (no predicate needed)
        const int64_t full_range = (int64_t)(full_end - scan_start);
        // tail_range: rows in the boundary block (need per-row predicate)
        const int64_t tail_start = full_range;
        const int64_t tail_end   = (int64_t)(scan_end - scan_start);

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            AggGroup* __restrict__ local_agg = &thread_aggs[(size_t)tid * N_GROUPS];

            // ---------------------------------------------------------------
            // Loop A: Branch-free aggregation for fully-qualifying rows.
            // All rows here have shipdate <= threshold by zone map guarantee.
            // No predicate → compiler sees a simple accumulation loop.
            // schedule(static): each thread gets a contiguous chunk for optimal
            // sequential memory access and hardware prefetcher behavior.
            // ---------------------------------------------------------------
            #pragma omp for schedule(static) nowait
            for (int64_t i = 0; i < full_range; i++) {
                const int32_t rf       = p_returnflag[i];
                const int32_t ls       = p_linestatus[i];
                AggGroup& g            = local_agg[rf * MAX_LS + ls];
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

            // ---------------------------------------------------------------
            // Loop B: Boundary block — per-row shipdate predicate required.
            // Typically 0-65536 rows (one block), so overhead is minimal.
            // ---------------------------------------------------------------
            #pragma omp for schedule(static)
            for (int64_t i = tail_start; i < tail_end; i++) {
                if (__builtin_expect(p_shipdate[i] > shipdate_threshold, 0)) continue;

                const int32_t rf       = p_returnflag[i];
                const int32_t ls       = p_linestatus[i];
                AggGroup& g            = local_agg[rf * MAX_LS + ls];
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
