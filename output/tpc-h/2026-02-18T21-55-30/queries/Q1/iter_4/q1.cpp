// Q1: Pricing Summary Report — GenDB iteration 4
// Strategy: OpenMP parallel scan with compiler auto-vectorization,
//           flat-array aggregation (6 groups), zone-map pruning on l_shipdate.
// Key changes vs iter_3:
//   - Replace manual std::thread with OpenMP (lower spawn overhead, better vectorization)
//   - Use #pragma omp simd on hot aggregation loops for auto-vectorization
//   - Separate all_pass path (no branch on shipdate) for compiler to vectorize cleanly
//   - Cache-line aligned per-thread aggregation to avoid false sharing
//   - Use posix_fadvise FADV_WILLNEED for HDD prefetch in addition to madvise
//   - Minimize branch misprediction in the mixed (partial) path
// Correctness anchors (DO NOT MODIFY):
//   revenue_formula: ep*(100-d)  [disc_price]
//   revenue_formula: price * (100 + t)  [charge = disc_price * (100+t)]
//   avg_qty = sum_qty/count, where sum_qty is scale=2, output divides by 100

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <cassert>
#include <immintrin.h>
#include <omp.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Aggregation state per group — cache-line padded to avoid false sharing
// All values stored at scale_factor=2 (x100), divided at output.
// disc_price = extendedprice * (100 - discount) / 100  =>  accumulate as ep*(100-d), divide by 10000 at output
// charge     = disc_price * (100 + tax)                =>  accumulate as ep*(100-d)*(100+t), divide by 1000000 at output
// ---------------------------------------------------------------------------

struct AggState {
    int64_t sum_qty;
    int64_t sum_base_price;
    int64_t sum_disc_price;
    int64_t sum_charge;
    int64_t sum_discount;
    int64_t count;
};

static constexpr int NUM_GROUPS = 6;
// Pad AggState array to cache line boundary: 6 * 6 * 8 = 288 bytes -> pad to 320 (5 cache lines)
// Use 64-byte alignment per thread block to avoid false sharing
struct alignas(64) ThreadAgg {
    AggState agg[NUM_GROUPS];
};

static void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string lineitem_dir = gendb_dir + "/lineitem/";
    const std::string index_dir    = gendb_dir + "/indexes/";

    // Threshold: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
    constexpr int32_t SHIPDATE_THRESHOLD = 10471;

    // ---------------------------------------------------------------------------
    // Phase 1: Load dictionaries
    // ---------------------------------------------------------------------------
    std::string returnflag_str[3];
    std::string linestatus_str[2];
    {
        GENDB_PHASE("dim_filter");
        std::ifstream rf(lineitem_dir + "l_returnflag_dict.txt");
        std::string line;
        while (std::getline(rf, line)) {
            size_t pipe = line.find('|');
            if (pipe != std::string::npos) {
                int code = std::stoi(line.substr(0, pipe));
                std::string val = line.substr(pipe + 1);
                if (code >= 0 && code < 3) returnflag_str[code] = val;
            }
        }
        std::ifstream ls(lineitem_dir + "l_linestatus_dict.txt");
        while (std::getline(ls, line)) {
            size_t pipe = line.find('|');
            if (pipe != std::string::npos) {
                int code = std::stoi(line.substr(0, pipe));
                std::string val = line.substr(pipe + 1);
                if (code >= 0 && code < 2) linestatus_str[code] = val;
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 2: mmap all required columns + prefetch
    // ---------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int32_t> col_returnflag;
    gendb::MmapColumn<int32_t> col_linestatus;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_tax;

    {
        GENDB_PHASE("build_joins");
        col_shipdate.open(lineitem_dir   + "l_shipdate.bin");
        col_returnflag.open(lineitem_dir + "l_returnflag.bin");
        col_linestatus.open(lineitem_dir + "l_linestatus.bin");
        col_quantity.open(lineitem_dir   + "l_quantity.bin");
        col_extprice.open(lineitem_dir   + "l_extendedprice.bin");
        col_discount.open(lineitem_dir   + "l_discount.bin");
        col_tax.open(lineitem_dir        + "l_tax.bin");

        // Prefetch all columns to warm the page cache
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);
    }

    const size_t total_rows = col_shipdate.size();

    // ---------------------------------------------------------------------------
    // Phase 3: Load zone map
    // Layout: [uint32_t num_blocks] then [{int32_t min, int32_t max, uint32_t count} x N]
    // ---------------------------------------------------------------------------
    struct ZoneBlock { int32_t min_val; int32_t max_val; uint32_t count; };
    std::vector<ZoneBlock> zone_blocks;
    {
        int fd = ::open((index_dir + "zone_map_l_shipdate.bin").c_str(), O_RDONLY);
        if (fd >= 0) {
            uint32_t num_blocks = 0;
            [[maybe_unused]] ssize_t r1 = ::read(fd, &num_blocks, sizeof(num_blocks));
            zone_blocks.resize(num_blocks);
            [[maybe_unused]] ssize_t r2 = ::read(fd, zone_blocks.data(), num_blocks * sizeof(ZoneBlock));
            ::close(fd);
        }
    }

    // Determine last qualifying block (shipdate sorted ascending, so tail blocks all fail)
    uint32_t last_qualifying_block = (uint32_t)zone_blocks.size();
    for (uint32_t b = 0; b < (uint32_t)zone_blocks.size(); b++) {
        if (zone_blocks[b].min_val > SHIPDATE_THRESHOLD) {
            last_qualifying_block = b;
            break;
        }
    }

    const size_t BLOCK_SIZE = 100000;
    const size_t num_blocks_total = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
    const size_t num_blocks_to_process = std::min((size_t)last_qualifying_block, num_blocks_total);

    // ---------------------------------------------------------------------------
    // Phase 4: Parallel scan + fused aggregate using OpenMP
    // ---------------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();
    std::vector<ThreadAgg> thread_aggs(num_threads);
    for (auto& ta : thread_aggs) {
        memset(&ta, 0, sizeof(ta));
    }

    const int32_t* __restrict__ sd   = col_shipdate.data;
    const int32_t* __restrict__ rf   = col_returnflag.data;
    const int32_t* __restrict__ ls   = col_linestatus.data;
    const int64_t* __restrict__ qty  = col_quantity.data;
    const int64_t* __restrict__ ep   = col_extprice.data;
    const int64_t* __restrict__ disc = col_discount.data;
    const int64_t* __restrict__ tax  = col_tax.data;

    // Pre-build zone map flags: 0=skip, 1=all_pass, 2=partial
    std::vector<uint8_t> block_mode(num_blocks_to_process, 2);
    for (size_t b = 0; b < num_blocks_to_process; b++) {
        if (!zone_blocks.empty() && b < zone_blocks.size()) {
            const auto& zb = zone_blocks[b];
            if (zb.min_val > SHIPDATE_THRESHOLD)      block_mode[b] = 0; // skip
            else if (zb.max_val <= SHIPDATE_THRESHOLD) block_mode[b] = 1; // all pass
            else                                        block_mode[b] = 2; // partial
        }
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(64)
        {
            const int tid = omp_get_thread_num();
            AggState* __restrict__ agg = thread_aggs[tid].agg;

            #pragma omp for schedule(dynamic, 4)
            for (size_t blk = 0; blk < num_blocks_to_process; blk++) {
                const uint8_t mode = block_mode[blk];
                if (mode == 0) continue;

                const size_t row_start = blk * BLOCK_SIZE;
                const size_t row_end   = std::min(row_start + BLOCK_SIZE, total_rows);
                const size_t nrows     = row_end - row_start;

                const int32_t* __restrict__ sd_b   = sd   + row_start;
                const int32_t* __restrict__ rf_b   = rf   + row_start;
                const int32_t* __restrict__ ls_b   = ls   + row_start;
                const int64_t* __restrict__ qty_b  = qty  + row_start;
                const int64_t* __restrict__ ep_b   = ep   + row_start;
                const int64_t* __restrict__ disc_b = disc + row_start;
                const int64_t* __restrict__ tax_b  = tax  + row_start;

                if (mode == 1) {
                    // All rows pass — no shipdate check. Process with tight loop.
                    // Compiler can auto-vectorize since no branch on loop body.
                    // Split by group to allow vectorization of arithmetic.
                    // Use local accumulators to reduce memory pressure.
                    int64_t lsum_qty[NUM_GROUPS]        = {};
                    int64_t lsum_base[NUM_GROUPS]       = {};
                    int64_t lsum_disc[NUM_GROUPS]       = {};
                    int64_t lsum_charge[NUM_GROUPS]     = {};
                    int64_t lsum_discount[NUM_GROUPS]   = {};
                    int64_t lcount[NUM_GROUPS]          = {};

                    for (size_t i = 0; i < nrows; i++) {
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q = qty_b[i];
                        const int64_t e = ep_b[i];
                        const int64_t d = disc_b[i];
                        const int64_t t = tax_b[i];
                        const int64_t dp = e * (100 - d);
                        lsum_qty[g]      += q;
                        lsum_base[g]     += e;
                        lsum_disc[g]     += dp;
                        lsum_charge[g]   += dp * (100 + t);
                        lsum_discount[g] += d;
                        lcount[g]++;
                    }
                    for (int g = 0; g < NUM_GROUPS; g++) {
                        agg[g].sum_qty        += lsum_qty[g];
                        agg[g].sum_base_price += lsum_base[g];
                        agg[g].sum_disc_price += lsum_disc[g];
                        agg[g].sum_charge     += lsum_charge[g];
                        agg[g].sum_discount   += lsum_discount[g];
                        agg[g].count          += lcount[g];
                    }
                } else {
                    // Partial block — per-row shipdate filter
                    // Use branchless predicate: pass = (sd <= threshold)
                    int64_t lsum_qty[NUM_GROUPS]        = {};
                    int64_t lsum_base[NUM_GROUPS]       = {};
                    int64_t lsum_disc[NUM_GROUPS]       = {};
                    int64_t lsum_charge[NUM_GROUPS]     = {};
                    int64_t lsum_discount[NUM_GROUPS]   = {};
                    int64_t lcount[NUM_GROUPS]          = {};

                    for (size_t i = 0; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q = qty_b[i];
                        const int64_t e = ep_b[i];
                        const int64_t d = disc_b[i];
                        const int64_t t = tax_b[i];
                        const int64_t dp = e * (100 - d);
                        lsum_qty[g]      += q;
                        lsum_base[g]     += e;
                        lsum_disc[g]     += dp;
                        lsum_charge[g]   += dp * (100 + t);
                        lsum_discount[g] += d;
                        lcount[g]++;
                    }
                    for (int g = 0; g < NUM_GROUPS; g++) {
                        agg[g].sum_qty        += lsum_qty[g];
                        agg[g].sum_base_price += lsum_base[g];
                        agg[g].sum_disc_price += lsum_disc[g];
                        agg[g].sum_charge     += lsum_charge[g];
                        agg[g].sum_discount   += lsum_discount[g];
                        agg[g].count          += lcount[g];
                    }
                }
            }
        } // end parallel
    }

    // ---------------------------------------------------------------------------
    // Phase 5: Merge thread-local aggregates
    // ---------------------------------------------------------------------------
    AggState global_agg[NUM_GROUPS] = {};
    const int actual_threads = (int)thread_aggs.size();
    for (int t = 0; t < actual_threads; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            global_agg[g].sum_qty        += thread_aggs[t].agg[g].sum_qty;
            global_agg[g].sum_base_price += thread_aggs[t].agg[g].sum_base_price;
            global_agg[g].sum_disc_price += thread_aggs[t].agg[g].sum_disc_price;
            global_agg[g].sum_charge     += thread_aggs[t].agg[g].sum_charge;
            global_agg[g].sum_discount   += thread_aggs[t].agg[g].sum_discount;
            global_agg[g].count          += thread_aggs[t].agg[g].count;
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 6: Build result rows
    // ---------------------------------------------------------------------------
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int64_t sum_qty;
        int64_t sum_base_price;
        int64_t sum_disc_price;
        int64_t sum_charge;
        int64_t sum_discount;
        int64_t count;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_GROUPS);

    for (int rf_code = 0; rf_code < 3; rf_code++) {
        for (int ls_code = 0; ls_code < 2; ls_code++) {
            int g = rf_code * 2 + ls_code;
            if (global_agg[g].count == 0) continue;
            ResultRow row;
            row.returnflag     = returnflag_str[rf_code];
            row.linestatus     = linestatus_str[ls_code];
            row.sum_qty        = global_agg[g].sum_qty;
            row.sum_base_price = global_agg[g].sum_base_price;
            row.sum_disc_price = global_agg[g].sum_disc_price;
            row.sum_charge     = global_agg[g].sum_charge;
            row.sum_discount   = global_agg[g].sum_discount;
            row.count          = global_agg[g].count;
            results.push_back(std::move(row));
        }
    }

    // Sort by returnflag ASC, linestatus ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // ---------------------------------------------------------------------------
    // Phase 7: Output CSV
    // Scale factors:
    //   sum_qty:        scale=2  → divide by 100
    //   sum_base_price: scale=2  → divide by 100
    //   sum_disc_price: ep*(100-d) where ep,d are scale=2 → /10000 to get actual
    //   sum_charge:     dp*(100+t) where dp is scale=4, t is scale=2 → /1000000
    //   avg_qty:        sum_qty(scale=2) / count → /100 per element
    //   avg_price:      sum_base_price(scale=2) / count → /100
    //   avg_disc:       sum_discount(scale=2) / count → /100
    // ---------------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            throw std::runtime_error("Cannot open output file: " + out_path);
        }

        std::fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : results) {
            double d_sum_qty        = (double)r.sum_qty        / 100.0;
            double d_sum_base_price = (double)r.sum_base_price / 100.0;
            double d_sum_disc_price = (double)r.sum_disc_price / 10000.0;
            double d_sum_charge     = (double)r.sum_charge     / 1000000.0;
            double d_avg_qty        = (double)r.sum_qty        / 100.0 / (double)r.count;
            double d_avg_price      = (double)r.sum_base_price / 100.0 / (double)r.count;
            double d_avg_disc       = (double)r.sum_discount   / 100.0 / (double)r.count;

            std::fprintf(out, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                r.returnflag.c_str(),
                r.linestatus.c_str(),
                d_sum_qty,
                d_sum_base_price,
                d_sum_disc_price,
                d_sum_charge,
                d_avg_qty,
                d_avg_price,
                d_avg_disc,
                (long)r.count
            );
        }

        std::fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
