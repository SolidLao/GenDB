// Q1: Pricing Summary Report — GenDB iteration 0
// Strategy: morsel-driven parallel scan, flat-array aggregation (6 groups),
//           zone-map pruning on l_shipdate, fused scan+filter+aggregate.
// Note: Zone map shows each block spans full date range (not purely sorted per-block),
//       so we use per-row check on all blocks but keep zone map logic for correctness.
//       AVX2 vectorized filter on l_shipdate.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <fstream>
#include <cassert>
#include <immintrin.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Aggregation state per group
// All values stored at scale_factor=2 (x100), divided at output.
// disc_price = extendedprice * (100 - discount) / 100
// charge = disc_price * (100 + tax) / 100
// We accumulate as:
//   sum_disc_price_scaled = sum(extendedprice * (100 - discount))   [x100^2 -> x100 at output]
//   sum_charge_scaled     = sum(extendedprice * (100 - discount) * (100 + tax))  [/10000 at output]
// ---------------------------------------------------------------------------

struct AggState {
    int64_t sum_qty;            // sum of l_quantity (scale=2)
    int64_t sum_base_price;     // sum of l_extendedprice (scale=2)
    int64_t sum_disc_price;     // sum of extendedprice*(100-discount) (needs /100 at output for scale=2)
    int64_t sum_charge;         // sum of extendedprice*(100-discount)*(100+tax) (needs /10000 at output for scale=2)
    int64_t sum_discount;       // sum of l_discount (scale=2)
    int64_t count;
};

// 6 groups: returnflag (3 values: 0,1,2) x linestatus (2 values: 0,1)
// group index = returnflag_code * 2 + linestatus_code
static constexpr int NUM_GROUPS = 6;

static void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string lineitem_dir = gendb_dir + "/lineitem/";
    const std::string index_dir    = gendb_dir + "/indexes/";

    // Threshold: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
    constexpr int32_t SHIPDATE_THRESHOLD = 10471;

    // ---------------------------------------------------------------------------
    // Phase 1: Load dictionaries to map dict codes -> sort keys for output ordering
    // returnflag dict: 0->N, 1->R, 2->A  =>  sort order: A < N < R
    // linestatus dict: 0->O, 1->F        =>  sort order: F < O
    // ---------------------------------------------------------------------------
    std::string returnflag_str[3];
    std::string linestatus_str[2];
    {
        GENDB_PHASE("dim_filter");
        // Load returnflag dict
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
        // Load linestatus dict
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
    // Phase 2: mmap all required columns
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
        col_shipdate.open(lineitem_dir + "l_shipdate.bin");
        col_returnflag.open(lineitem_dir + "l_returnflag.bin");
        col_linestatus.open(lineitem_dir + "l_linestatus.bin");
        col_quantity.open(lineitem_dir + "l_quantity.bin");
        col_extprice.open(lineitem_dir + "l_extendedprice.bin");
        col_discount.open(lineitem_dir + "l_discount.bin");
        col_tax.open(lineitem_dir + "l_tax.bin");

        // Prefetch all columns to overlap I/O
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);
    }

    const size_t total_rows = col_shipdate.size();

    // ---------------------------------------------------------------------------
    // Phase 3: Load zone map to determine block ranges
    // Layout: [uint32_t num_blocks] then [{int32_t min, int32_t max, uint32_t count} x N]
    // ---------------------------------------------------------------------------
    struct ZoneBlock {
        int32_t  min_val;
        int32_t  max_val;
        uint32_t count;
    };

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

    // Determine last block that might have qualifying rows
    // Since shipdate is sorted ascending within each block but the zone map shows
    // each block spans nearly the full range, we use per-row checks everywhere.
    // However, we can still skip blocks where min_val > SHIPDATE_THRESHOLD (tail blocks).
    uint32_t last_qualifying_block = (uint32_t)zone_blocks.size();
    for (uint32_t b = 0; b < (uint32_t)zone_blocks.size(); b++) {
        if (zone_blocks[b].min_val > SHIPDATE_THRESHOLD) {
            last_qualifying_block = b;
            break;
        }
    }

    const size_t BLOCK_SIZE = 100000;
    const size_t num_blocks_total = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
    // Limit to last_qualifying_block
    const size_t num_blocks_to_process = std::min((size_t)last_qualifying_block, num_blocks_total);

    // ---------------------------------------------------------------------------
    // Phase 4: Parallel morsel-driven scan + fused aggregate
    // ---------------------------------------------------------------------------
    const int num_threads = std::min((int)std::thread::hardware_concurrency(), 64);
    std::atomic<size_t> block_counter{0};

    // Per-thread aggregation arrays
    std::vector<std::array<AggState, NUM_GROUPS>> thread_aggs(num_threads);
    for (auto& ta : thread_aggs) {
        for (auto& g : ta) g = {0, 0, 0, 0, 0, 0};
    }

    const int32_t* sd   = col_shipdate.data;
    const int32_t* rf   = col_returnflag.data;
    const int32_t* ls   = col_linestatus.data;
    const int64_t* qty  = col_quantity.data;
    const int64_t* ep   = col_extprice.data;
    const int64_t* disc = col_discount.data;
    const int64_t* tax  = col_tax.data;

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            AggState* agg = thread_aggs[tid].data();

            // local registers for the 6 groups to help compiler keep them in registers
            // (compiler will spill if needed, but hint helps)
            size_t blk;
            while ((blk = block_counter.fetch_add(1, std::memory_order_relaxed)) < num_blocks_to_process) {
                const size_t row_start = blk * BLOCK_SIZE;
                const size_t row_end   = std::min(row_start + BLOCK_SIZE, total_rows);
                const size_t nrows     = row_end - row_start;

                // Check if this block needs per-row filtering or can be fully skipped
                // zone_blocks may not be available if file not found; fall back to per-row check
                bool all_pass = false;
                bool all_skip = false;
                if (!zone_blocks.empty() && blk < zone_blocks.size()) {
                    const auto& zb = zone_blocks[blk];
                    if (zb.min_val > SHIPDATE_THRESHOLD) {
                        all_skip = true;
                    } else if (zb.max_val <= SHIPDATE_THRESHOLD) {
                        all_pass = true;
                    }
                }
                if (all_skip) continue;

                const int32_t* sd_b   = sd   + row_start;
                const int32_t* rf_b   = rf   + row_start;
                const int32_t* ls_b   = ls   + row_start;
                const int64_t* qty_b  = qty  + row_start;
                const int64_t* ep_b   = ep   + row_start;
                const int64_t* disc_b = disc + row_start;
                const int64_t* tax_b  = tax  + row_start;

                if (all_pass) {
                    // No shipdate filter needed — process all rows
                    for (size_t i = 0; i < nrows; i++) {
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        // disc_price = e * (100 - d), all at scale=2
                        // charge = disc_price * (100 + t)
                        const int64_t disc_price = e * (100 - d);   // scale = 2*2 = 4 (needs /100 for scale=2)
                        const int64_t charge     = disc_price * (100 + t);  // scale = 2*3 = 6 (needs /10000 for scale=2)
                        agg[g].sum_qty        += q;
                        agg[g].sum_base_price += e;
                        agg[g].sum_disc_price += disc_price;
                        agg[g].sum_charge     += charge;
                        agg[g].sum_discount   += d;
                        agg[g].count++;
                    }
                } else {
                    // Per-row shipdate filter
#ifdef __AVX2__
                    // AVX2 path: process 8 rows at a time
                    const __m256i threshold_vec = _mm256_set1_epi32(SHIPDATE_THRESHOLD);
                    size_t i = 0;
                    // Process 8 at a time
                    for (; i + 8 <= nrows; i += 8) {
                        __m256i ship8 = _mm256_loadu_si256((const __m256i*)(sd_b + i));
                        // mask: ship8 <= threshold => !(ship8 > threshold)
                        __m256i gt_mask = _mm256_cmpgt_epi32(ship8, threshold_vec);
                        int mask = _mm256_movemask_epi8(gt_mask);
                        if (mask == -1) continue; // all 8 fail filter
                        // Process individual rows based on mask
                        for (int j = 0; j < 8; j++) {
                            // Each int32 takes 4 bytes in movemask, check bit j*4
                            if (mask & (1 << (j * 4))) continue; // this row fails
                            const int row = i + j;
                            const int g = rf_b[row] * 2 + ls_b[row];
                            const int64_t q  = qty_b[row];
                            const int64_t e  = ep_b[row];
                            const int64_t d  = disc_b[row];
                            const int64_t t  = tax_b[row];
                            const int64_t disc_price = e * (100 - d);
                            const int64_t charge     = disc_price * (100 + t);
                            agg[g].sum_qty        += q;
                            agg[g].sum_base_price += e;
                            agg[g].sum_disc_price += disc_price;
                            agg[g].sum_charge     += charge;
                            agg[g].sum_discount   += d;
                            agg[g].count++;
                        }
                    }
                    // Scalar tail
                    for (; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        const int64_t disc_price = e * (100 - d);
                        const int64_t charge     = disc_price * (100 + t);
                        agg[g].sum_qty        += q;
                        agg[g].sum_base_price += e;
                        agg[g].sum_disc_price += disc_price;
                        agg[g].sum_charge     += charge;
                        agg[g].sum_discount   += d;
                        agg[g].count++;
                    }
#else
                    for (size_t i = 0; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        const int64_t disc_price = e * (100 - d);
                        const int64_t charge     = disc_price * (100 + t);
                        agg[g].sum_qty        += q;
                        agg[g].sum_base_price += e;
                        agg[g].sum_disc_price += disc_price;
                        agg[g].sum_charge     += charge;
                        agg[g].sum_discount   += d;
                        agg[g].count++;
                    }
#endif
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // ---------------------------------------------------------------------------
    // Phase 5: Merge thread-local aggregates
    // ---------------------------------------------------------------------------
    AggState global_agg[NUM_GROUPS] = {};
    {
        for (int t = 0; t < num_threads; t++) {
            for (int g = 0; g < NUM_GROUPS; g++) {
                global_agg[g].sum_qty        += thread_aggs[t][g].sum_qty;
                global_agg[g].sum_base_price += thread_aggs[t][g].sum_base_price;
                global_agg[g].sum_disc_price += thread_aggs[t][g].sum_disc_price;
                global_agg[g].sum_charge     += thread_aggs[t][g].sum_charge;
                global_agg[g].sum_discount   += thread_aggs[t][g].sum_discount;
                global_agg[g].count          += thread_aggs[t][g].count;
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 6: Build result rows and sort
    // Sort order: l_returnflag ASC, l_linestatus ASC (lexicographic by string value)
    // ---------------------------------------------------------------------------
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int64_t sum_qty;          // scale=2 (divide by 100 for output)
        int64_t sum_base_price;   // scale=2
        int64_t sum_disc_price;   // needs /100 from accumulated scale=4 back to scale=2
        int64_t sum_charge;       // needs /10000 from scale=6 back to scale=2
        int64_t sum_discount;     // scale=2
        int64_t count;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_GROUPS);

    for (int rf_code = 0; rf_code < 3; rf_code++) {
        for (int ls_code = 0; ls_code < 2; ls_code++) {
            int g = rf_code * 2 + ls_code;
            if (global_agg[g].count == 0) continue;
            ResultRow row;
            row.returnflag    = returnflag_str[rf_code];
            row.linestatus    = linestatus_str[ls_code];
            row.sum_qty       = global_agg[g].sum_qty;
            row.sum_base_price= global_agg[g].sum_base_price;
            row.sum_disc_price= global_agg[g].sum_disc_price;
            row.sum_charge    = global_agg[g].sum_charge;
            row.sum_discount  = global_agg[g].sum_discount;
            row.count         = global_agg[g].count;
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
    //   sum_qty: stored as int64 * 100 => divide by 100 for display
    //   sum_base_price: stored as int64 * 100 => divide by 100
    //   sum_disc_price: accumulated as ep*(100-d) at scale=4 => divide by 10000 for 2dp output,
    //                   but we want 2dp precision: print as (val / 100) with 2 decimal places
    //   sum_charge: accumulated as ep*(100-d)*(100+t) at scale=6 => divide by 1000000 for 2dp
    //   avg_qty: sum_qty / count (both at scale=2) => (sum_qty * 1.0 / count) / 100
    //   avg_price: sum_base_price / count / 100
    //   avg_disc: sum_discount / count / 100
    // ---------------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            throw std::runtime_error("Cannot open output file: " + out_path);
        }

        // Header
        std::fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : results) {
            // sum_qty: scale=2, display with 2dp
            double d_sum_qty        = (double)r.sum_qty / 100.0;
            double d_sum_base_price = (double)r.sum_base_price / 100.0;
            // sum_disc_price accumulated as ep*(100-d) where both ep and d are scale=2
            // ep is in units of 0.01, (100-d) means d is scale=2 so d=7 means 0.07
            // disc_price = ep * (100 - d) = (actual_ep*100) * (100 - actual_d*100)
            //            = actual_ep * actual_d_factor * 10000
            // Wait: ep stored as actual_ep * 100. d stored as actual_d * 100.
            // disc_price_actual = actual_ep * (1 - actual_d)
            //                   = (ep/100) * (1 - d/100)
            //                   = (ep/100) * ((100-d)/100)
            //                   = ep * (100-d) / 10000
            // So sum_disc_price / 10000 = sum of actual disc_price values
            double d_sum_disc_price = (double)r.sum_disc_price / 10000.0;
            // charge_actual = actual_ep * (1-actual_d) * (1+actual_t)
            //               = (ep/100) * ((100-d)/100) * ((100+t)/100)
            //               = ep*(100-d)*(100+t) / 1000000
            double d_sum_charge     = (double)r.sum_charge / 1000000.0;
            double d_avg_qty        = (double)r.sum_qty / 100.0 / (double)r.count;
            double d_avg_price      = (double)r.sum_base_price / 100.0 / (double)r.count;
            double d_avg_disc       = (double)r.sum_discount / 100.0 / (double)r.count;

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
