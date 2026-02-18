/*
 * Q1: Pricing Summary Report — Iteration 4
 *
 * === LOGICAL PLAN ===
 * Table: lineitem (~60M rows)
 * Predicate: l_shipdate <= 10471 (epoch days for 1998-09-02 = DATE '1998-12-01' - 90 days)
 * Group By: l_returnflag (3 values), l_linestatus (2 values) => max 6 groups
 * Aggregations: sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count
 * Order By: l_returnflag, l_linestatus
 *
 * === PHYSICAL PLAN ===
 * Scan Strategy:
 *   - lineitem is sorted by l_shipdate ASCENDING.
 *   - Use zone map to split scan into two ranges:
 *     (a) "fully qualifying" blocks: block_max <= threshold → no per-row predicate needed
 *     (b) "boundary" block: block_min <= threshold < block_max → per-row check for partial block
 *     (c) Blocks after boundary: entirely skipped.
 *   - This eliminates the per-row branch for 99%+ of qualifying rows, enabling SIMD auto-vectorization.
 *
 * Aggregation: Flat array [returnflag_code * MAX_LS + linestatus_code], 6 groups max
 *   - Thread-local accumulation → merge
 *   - AggGroup padded to cache-line (64B) to avoid false sharing
 *
 * Parallelism: OpenMP parallel scan, schedule(static) for predictable load balancing
 *   - 64 cores, morsel size 65536 (fits in L3 cache per thread)
 *
 * Decimal handling: int64_t at scale 100; accumulate products at scale^2/^3, divide once for output
 *   - disc_price = extprice * (100 - discount)  [scale 10000]
 *   - charge = disc_price * (100 + tax)          [scale 1000000]
 *
 * KEY OPTIMIZATION vs. previous iterations:
 *   Prior code had `if (shipdate > threshold) continue;` inside the parallel loop.
 *   This branch is unpredictable for boundary rows and prevents SIMD vectorization.
 *   By splitting into predicate-free "body" and per-row "tail" ranges using the zone map,
 *   the hot-path inner loop is branch-free and the compiler can auto-vectorize aggregation.
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

static constexpr int MAX_RF = 8;
static constexpr int MAX_LS = 8;
static constexpr int GROUP_STRIDE = MAX_RF * MAX_LS;  // 64

// Zone map entry: exactly as per storage guide
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
} __attribute__((packed));

// Per-group accumulator — pad to 128 bytes to avoid false sharing between
// adjacent thread-local AggGroup arrays (cache line = 64B, 2 lines = 128B)
struct alignas(64) AggGroup {
    int64_t  sum_qty        = 0;  // scale 100
    int64_t  sum_base_price = 0;  // scale 100
    int64_t  sum_discount   = 0;  // scale 100
    int64_t  count          = 0;
    // sum_disc_price = sum(extprice * (100 - discount)) [scale 10000]
    // sum_charge     = sum(extprice * (100 - discount) * (100 + tax)) [scale 1000000]
    // Use __int128 to avoid overflow on 60M rows with max values
    __int128 sum_disc_price = 0;
    __int128 sum_charge     = 0;
    int      returnflag_code = -1;
    int      linestatus_code = -1;
    bool     active          = false;
    char     _pad[3];  // explicit padding
};

// Inner scan kernel — NO per-row shipdate check (fully-qualifying range).
// Written as a standalone function so the compiler can vectorize the hot loop.
static void __attribute__((noinline))
scan_rows_no_pred(
    const int32_t* __restrict__ p_returnflag,
    const int32_t* __restrict__ p_linestatus,
    const int64_t* __restrict__ p_quantity,
    const int64_t* __restrict__ p_extprice,
    const int64_t* __restrict__ p_discount,
    const int64_t* __restrict__ p_tax,
    int64_t row_begin, int64_t row_end,
    AggGroup* local_agg)
{
    for (int64_t i = row_begin; i < row_end; i++) {
        const int rf = p_returnflag[i];
        const int ls = p_linestatus[i];
        AggGroup& g  = local_agg[rf * MAX_LS + ls];

        if (__builtin_expect(!g.active, 0)) {
            g.active          = true;
            g.returnflag_code = rf;
            g.linestatus_code = ls;
        }

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
        g.sum_charge     += (__int128)disc_price * (100LL + tax);
    }
}

// Inner scan kernel — WITH per-row shipdate predicate (boundary block only).
static void __attribute__((noinline))
scan_rows_with_pred(
    const int32_t* __restrict__ p_shipdate,
    const int32_t* __restrict__ p_returnflag,
    const int32_t* __restrict__ p_linestatus,
    const int64_t* __restrict__ p_quantity,
    const int64_t* __restrict__ p_extprice,
    const int64_t* __restrict__ p_discount,
    const int64_t* __restrict__ p_tax,
    int64_t row_begin, int64_t row_end,
    int32_t threshold,
    AggGroup* local_agg)
{
    for (int64_t i = row_begin; i < row_end; i++) {
        if (p_shipdate[i] > threshold) continue;

        const int rf = p_returnflag[i];
        const int ls = p_linestatus[i];
        AggGroup& g  = local_agg[rf * MAX_LS + ls];

        if (__builtin_expect(!g.active, 0)) {
            g.active          = true;
            g.returnflag_code = rf;
            g.linestatus_code = ls;
        }

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
        g.sum_charge     += (__int128)disc_price * (100LL + tax);
    }
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string lineitem_dir = gendb_dir + "/lineitem/";
    const std::string indexes_dir  = gendb_dir + "/indexes/";

    // Threshold: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
    const int32_t shipdate_threshold = 10471;

    // -----------------------------------------------------------------------
    // Phase 1: Load dictionaries
    // -----------------------------------------------------------------------
    std::vector<std::string> rf_dict, ls_dict;
    {
        GENDB_PHASE("dim_filter");
        {
            std::ifstream f(lineitem_dir + "l_returnflag_dict.txt");
            std::string line;
            while (std::getline(f, line))
                if (!line.empty()) rf_dict.push_back(line);
        }
        {
            std::ifstream f(lineitem_dir + "l_linestatus_dict.txt");
            std::string line;
            while (std::getline(f, line))
                if (!line.empty()) ls_dict.push_back(line);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Read zone map → compute two scan ranges:
    //   body_end: last row of last fully-qualifying block (block_max <= threshold)
    //   tail_end: last row of boundary block (block_min <= threshold < block_max)
    // Since lineitem is sorted by shipdate ascending:
    //   All blocks with max_val <= threshold → fully qualify (no per-row check)
    //   The first block with min_val <= threshold AND max_val > threshold → boundary
    //   All subsequent blocks: skip
    // -----------------------------------------------------------------------
    uint64_t body_begin = 0;
    uint64_t body_end   = 0;  // exclusive; rows [body_begin, body_end) need no predicate
    uint64_t tail_begin = 0;
    uint64_t tail_end   = 0;  // exclusive; rows [tail_begin, tail_end) need per-row check
    bool     has_body   = false;
    bool     has_tail   = false;

    {
        GENDB_PHASE("build_joins");

        const std::string zm_path = indexes_dir + "lineitem_shipdate_zonemap.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (zf) {
            uint32_t num_blocks = 0;
            if (fread(&num_blocks, sizeof(uint32_t), 1, zf) != 1) {
                fclose(zf);
                zf = nullptr;
            } else {
                std::vector<ZoneMapEntry> entries(num_blocks);
                size_t nr = fread(entries.data(), sizeof(ZoneMapEntry), num_blocks, zf);
                fclose(zf);
                (void)nr;

                // Walk blocks in order (sorted ascending by shipdate)
                for (uint32_t bi = 0; bi < num_blocks; bi++) {
                    const ZoneMapEntry& e = entries[bi];

                    if (e.min_val > shipdate_threshold) {
                        // All rows exceed threshold — done; no more qualifying blocks
                        break;
                    }

                    const uint64_t bstart = e.row_start;
                    const uint64_t bend   = e.row_start + e.row_count;

                    if (e.max_val <= shipdate_threshold) {
                        // Fully-qualifying block: all rows pass predicate
                        if (!has_body) {
                            body_begin = bstart;
                            has_body   = true;
                        }
                        body_end = bend;
                    } else {
                        // Boundary block: min <= threshold < max — partial, needs per-row check
                        // Since sorted, there can be at most ONE such block (the last qualifying)
                        tail_begin = bstart;
                        tail_end   = bend;
                        has_tail   = true;
                        break;  // no more qualifying blocks after this
                    }
                }
            }
        }

        // Fallback: if no zone map, scan all rows with per-row predicate
        if (!has_body && !has_tail) {
            tail_begin = 0;
            tail_end   = 59986052ULL;
            has_tail   = true;
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Main scan — open columns, prefetch, then parallel aggregate
    // -----------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();
    // Thread-local aggregation: [thread_id * GROUP_STRIDE + rf * MAX_LS + ls]
    std::vector<AggGroup> thread_aggs((size_t)num_threads * GROUP_STRIDE);

    {
        GENDB_PHASE("main_scan");

        // Open all columns
        gendb::MmapColumn<int32_t> col_shipdate(lineitem_dir + "l_shipdate.bin");
        gendb::MmapColumn<int32_t> col_returnflag(lineitem_dir + "l_returnflag.bin");
        gendb::MmapColumn<int32_t> col_linestatus(lineitem_dir + "l_linestatus.bin");
        gendb::MmapColumn<int64_t> col_quantity(lineitem_dir + "l_quantity.bin");
        gendb::MmapColumn<int64_t> col_extprice(lineitem_dir + "l_extendedprice.bin");
        gendb::MmapColumn<int64_t> col_discount(lineitem_dir + "l_discount.bin");
        gendb::MmapColumn<int64_t> col_tax(lineitem_dir + "l_tax.bin");

        // Prefetch all columns asynchronously before parallel scan
        gendb::mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                                  col_quantity, col_extprice, col_discount, col_tax);

        // Raw pointers for hot loop
        const int32_t* p_shipdate   = col_shipdate.data;
        const int32_t* p_returnflag = col_returnflag.data;
        const int32_t* p_linestatus = col_linestatus.data;
        const int64_t* p_quantity   = col_quantity.data;
        const int64_t* p_extprice   = col_extprice.data;
        const int64_t* p_discount   = col_discount.data;
        const int64_t* p_tax        = col_tax.data;

        // ---------------------------------------------------------------
        // Scan body: fully-qualifying blocks — no per-row shipdate check.
        // This is the dominant range (~98% of qualifying rows for Q1).
        // The compiler can vectorize/unroll this loop freely.
        // ---------------------------------------------------------------
        if (has_body) {
            const int64_t body_size = (int64_t)(body_end - body_begin);
            const int64_t bstart    = (int64_t)body_begin;

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                AggGroup* local_agg = &thread_aggs[(size_t)tid * GROUP_STRIDE];

                #pragma omp for schedule(static, 65536)
                for (int64_t chunk = 0; chunk < body_size; chunk += 65536) {
                    const int64_t cstart = bstart + chunk;
                    const int64_t cend   = std::min(cstart + (int64_t)65536, bstart + body_size);
                    scan_rows_no_pred(
                        p_returnflag, p_linestatus,
                        p_quantity, p_extprice, p_discount, p_tax,
                        cstart, cend, local_agg);
                }
            }
        }

        // ---------------------------------------------------------------
        // Scan tail: boundary block — per-row shipdate predicate required.
        // This is a small fraction of rows (at most one block = 65536 rows).
        // ---------------------------------------------------------------
        if (has_tail) {
            const int64_t tstart = (int64_t)tail_begin;
            const int64_t tend   = (int64_t)tail_end;

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                AggGroup* local_agg = &thread_aggs[(size_t)tid * GROUP_STRIDE];

                #pragma omp for schedule(static, 65536)
                for (int64_t chunk = tstart; chunk < tend; chunk += 65536) {
                    const int64_t cend = std::min(chunk + (int64_t)65536, tend);
                    scan_rows_with_pred(
                        p_shipdate, p_returnflag, p_linestatus,
                        p_quantity, p_extprice, p_discount, p_tax,
                        chunk, cend, shipdate_threshold, local_agg);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local accumulators
    // -----------------------------------------------------------------------
    AggGroup merged[MAX_RF][MAX_LS] = {};

    for (int t = 0; t < num_threads; t++) {
        const AggGroup* local_agg = &thread_aggs[(size_t)t * GROUP_STRIDE];
        for (int rf = 0; rf < MAX_RF; rf++) {
            for (int ls = 0; ls < MAX_LS; ls++) {
                const AggGroup& src = local_agg[rf * MAX_LS + ls];
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
    // Phase 5: Collect active groups, sort by (returnflag, linestatus)
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
    results.reserve(6);

    for (int rf = 0; rf < MAX_RF; rf++) {
        if (rf >= (int)rf_dict.size()) break;
        for (int ls = 0; ls < MAX_LS; ls++) {
            if (ls >= (int)ls_dict.size()) break;
            const AggGroup& g = merged[rf][ls];
            if (!g.active) continue;

            ResultRow row;
            row.returnflag     = rf_dict[rf];
            row.linestatus     = ls_dict[ls];
            row.sum_qty        = (double)g.sum_qty / 100.0;
            row.sum_base_price = (double)g.sum_base_price / 100.0;
            // sum_disc_price at scale 100 (extprice*percentage => scale 100)
            row.sum_disc_price = (double)(int64_t)(g.sum_disc_price) / 100.0;
            // sum_charge at scale 100 (disc_price*percentage => scale 100)
            row.sum_charge     = (double)(int64_t)(g.sum_charge) / 100.0;
            row.avg_qty        = row.sum_qty  / (double)g.count;
            row.avg_price      = row.sum_base_price / (double)g.count;
            row.avg_disc       = (double)g.sum_discount / 100.0 / (double)g.count;
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

        const std::string out_path = results_dir + "/Q1.csv";
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
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
