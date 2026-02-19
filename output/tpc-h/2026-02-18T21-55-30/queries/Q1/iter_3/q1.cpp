// Q1: Pricing Summary Report — GenDB iteration 3
// Strategy: OpenMP static-scheduled parallel scan, flat-array aggregation (6 groups),
//           zone-map pruning on l_shipdate, fused scan+filter+aggregate.
//           Cache-line-padded per-thread AggState to prevent false sharing.
//           Register-cached group accumulators for all_pass blocks.
//           Software prefetch for wide int64 columns.

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
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Aggregation state per group, padded to 64 bytes (cache line) to prevent
// false sharing between threads when stored in per-thread arrays.
// All values stored at scale_factor=2 (x100), divided at output.
// disc_price = extendedprice * (100 - discount) / 100
// charge = disc_price * (100 + tax) / 100
// We accumulate as:
//   sum_disc_price_scaled = sum(extendedprice * (100 - discount))   [x10000 -> /10000 at output]
//   sum_charge_scaled     = sum(extendedprice * (100 - discount) * (100 + tax)) [/1000000 at output]
// ---------------------------------------------------------------------------

struct alignas(64) AggState {
    int64_t sum_qty;            // sum of l_quantity (scale=2)
    int64_t sum_base_price;     // sum of l_extendedprice (scale=2)
    int64_t sum_disc_price;     // sum of extendedprice*(100-discount) (needs /10000 at output for scale=2)
    int64_t sum_charge;         // sum of extendedprice*(100-discount)*(100+tax) (needs /1000000 at output for scale=2)
    int64_t sum_discount;       // sum of l_discount (scale=2)
    int64_t count;
    int64_t _pad[2];            // pad to 64 bytes (6*8 = 48, +2*8 = 64)
};
static_assert(sizeof(AggState) == 64, "AggState must be 64 bytes");

// 6 groups: returnflag (3 values: 0,1,2) x linestatus (2 values: 0,1)
// group index = returnflag_code * 2 + linestatus_code
static constexpr int NUM_GROUPS = 6;

// Per-thread agg array: NUM_GROUPS * 64-byte AggState = 384 bytes total (fits in L1)
struct alignas(64) ThreadAgg {
    AggState groups[NUM_GROUPS];
};

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

    // Precompute per-block flags: 0=skip, 1=all_pass, 2=partial (per-row check)
    const size_t BLOCK_SIZE = 100000;
    const size_t num_blocks_total = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // Find last qualifying block
    size_t num_blocks_to_process = num_blocks_total;
    for (size_t b = 0; b < zone_blocks.size(); b++) {
        if (zone_blocks[b].min_val > SHIPDATE_THRESHOLD) {
            num_blocks_to_process = b;
            break;
        }
    }

    // Build per-block flag array for quick lookup
    // flag: 0=skip, 1=all_pass (no per-row filter needed), 2=partial
    std::vector<uint8_t> block_flags(num_blocks_to_process, 2);
    for (size_t b = 0; b < num_blocks_to_process && b < zone_blocks.size(); b++) {
        if (zone_blocks[b].min_val > SHIPDATE_THRESHOLD) {
            block_flags[b] = 0; // skip
        } else if (zone_blocks[b].max_val <= SHIPDATE_THRESHOLD) {
            block_flags[b] = 1; // all_pass
        } else {
            block_flags[b] = 2; // partial
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 4: OpenMP static-scheduled parallel scan + fused aggregate
    // Static scheduling eliminates atomic block counter overhead.
    // 600 blocks / 64 threads = ~9 blocks per thread, excellent load balance.
    // ---------------------------------------------------------------------------
    const int num_threads = omp_get_max_threads();

    // Per-thread aggregation: cache-line padded to prevent false sharing
    std::vector<ThreadAgg> thread_aggs(num_threads);
    for (auto& ta : thread_aggs) {
        for (auto& g : ta.groups) {
            g.sum_qty = g.sum_base_price = g.sum_disc_price =
            g.sum_charge = g.sum_discount = g.count = g._pad[0] = g._pad[1] = 0;
        }
    }

    const int32_t* __restrict__ sd   = col_shipdate.data;
    const int32_t* __restrict__ rf   = col_returnflag.data;
    const int32_t* __restrict__ ls   = col_linestatus.data;
    const int64_t* __restrict__ qty  = col_quantity.data;
    const int64_t* __restrict__ ep   = col_extprice.data;
    const int64_t* __restrict__ disc = col_discount.data;
    const int64_t* __restrict__ tax  = col_tax.data;

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            AggState* agg = thread_aggs[tid].groups;

            // Register-cached accumulators for the 6 groups to minimize memory traffic
            // We'll accumulate into these and flush to agg[] at end
            int64_t r_sum_qty[NUM_GROUPS]        = {};
            int64_t r_sum_base_price[NUM_GROUPS] = {};
            int64_t r_sum_disc_price[NUM_GROUPS] = {};
            int64_t r_sum_charge[NUM_GROUPS]     = {};
            int64_t r_sum_discount[NUM_GROUPS]   = {};
            int64_t r_count[NUM_GROUPS]          = {};

            #pragma omp for schedule(static)
            for (size_t blk = 0; blk < num_blocks_to_process; blk++) {
                const uint8_t flag = (blk < block_flags.size()) ? block_flags[blk] : 2;
                if (flag == 0) continue; // skip block entirely

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

                if (flag == 1) {
                    // All rows pass — no shipdate filter needed
                    // Process in chunks of 8 for better ILP and prefetching
                    // Prefetch distance: 16 rows ahead (128 bytes for int32, 256 bytes for int64)
                    constexpr int PREFETCH_DIST = 16;
                    size_t i = 0;

                    for (; i + 8 <= nrows; i += 8) {
                        // Prefetch next chunk
                        if (i + PREFETCH_DIST + 8 <= nrows) {
                            __builtin_prefetch(ep_b   + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(disc_b + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(tax_b  + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(qty_b  + i + PREFETCH_DIST, 0, 1);
                        }

                        // Unrolled 8-row loop — compiler can use SIMD for accumulation
                        #pragma GCC unroll 8
                        for (int j = 0; j < 8; j++) {
                            const int g        = rf_b[i+j] * 2 + ls_b[i+j];
                            const int64_t q    = qty_b[i+j];
                            const int64_t e    = ep_b[i+j];
                            const int64_t d    = disc_b[i+j];
                            const int64_t t    = tax_b[i+j];
                            const int64_t dp   = e * (100 - d);
                            const int64_t ch   = dp * (100 + t);
                            r_sum_qty[g]        += q;
                            r_sum_base_price[g] += e;
                            r_sum_disc_price[g] += dp;
                            r_sum_charge[g]     += ch;
                            r_sum_discount[g]   += d;
                            r_count[g]++;
                        }
                    }
                    // Scalar tail
                    for (; i < nrows; i++) {
                        const int g        = rf_b[i] * 2 + ls_b[i];
                        const int64_t q    = qty_b[i];
                        const int64_t e    = ep_b[i];
                        const int64_t d    = disc_b[i];
                        const int64_t t    = tax_b[i];
                        const int64_t dp   = e * (100 - d);
                        const int64_t ch   = dp * (100 + t);
                        r_sum_qty[g]        += q;
                        r_sum_base_price[g] += e;
                        r_sum_disc_price[g] += dp;
                        r_sum_charge[g]     += ch;
                        r_sum_discount[g]   += d;
                        r_count[g]++;
                    }
                } else {
                    // Partial block — per-row shipdate filter
                    // Use AVX2 to vectorize the filter, then scalar for arithmetic
#ifdef __AVX2__
                    const __m256i threshold_vec = _mm256_set1_epi32(SHIPDATE_THRESHOLD);
                    size_t i = 0;

                    for (; i + 8 <= nrows; i += 8) {
                        __m256i ship8 = _mm256_loadu_si256((const __m256i*)(sd_b + i));
                        // gt_mask: bit set where ship8[j] > threshold (i.e., fails filter)
                        __m256i gt_mask = _mm256_cmpgt_epi32(ship8, threshold_vec);
                        int mask = _mm256_movemask_epi8(gt_mask);

                        if (mask == -1) continue; // all 8 fail filter — skip

                        // Process individual rows based on mask
                        #pragma GCC unroll 8
                        for (int j = 0; j < 8; j++) {
                            // Each int32 occupies 4 bits in movemask output
                            if (mask & (0xF << (j * 4))) continue; // row j fails filter
                            const size_t row = i + j;
                            const int g        = rf_b[row] * 2 + ls_b[row];
                            const int64_t q    = qty_b[row];
                            const int64_t e    = ep_b[row];
                            const int64_t d    = disc_b[row];
                            const int64_t t    = tax_b[row];
                            const int64_t dp   = e * (100 - d);
                            const int64_t ch   = dp * (100 + t);
                            r_sum_qty[g]        += q;
                            r_sum_base_price[g] += e;
                            r_sum_disc_price[g] += dp;
                            r_sum_charge[g]     += ch;
                            r_sum_discount[g]   += d;
                            r_count[g]++;
                        }
                    }
                    // Scalar tail
                    for (; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g        = rf_b[i] * 2 + ls_b[i];
                        const int64_t q    = qty_b[i];
                        const int64_t e    = ep_b[i];
                        const int64_t d    = disc_b[i];
                        const int64_t t    = tax_b[i];
                        const int64_t dp   = e * (100 - d);
                        const int64_t ch   = dp * (100 + t);
                        r_sum_qty[g]        += q;
                        r_sum_base_price[g] += e;
                        r_sum_disc_price[g] += dp;
                        r_sum_charge[g]     += ch;
                        r_sum_discount[g]   += d;
                        r_count[g]++;
                    }
#else
                    for (size_t i = 0; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g        = rf_b[i] * 2 + ls_b[i];
                        const int64_t q    = qty_b[i];
                        const int64_t e    = ep_b[i];
                        const int64_t d    = disc_b[i];
                        const int64_t t    = tax_b[i];
                        const int64_t dp   = e * (100 - d);
                        const int64_t ch   = dp * (100 + t);
                        r_sum_qty[g]        += q;
                        r_sum_base_price[g] += e;
                        r_sum_disc_price[g] += dp;
                        r_sum_charge[g]     += ch;
                        r_sum_discount[g]   += d;
                        r_count[g]++;
                    }
#endif
                }
            } // end omp for

            // Flush register-cached accumulators to thread-local agg[]
            for (int g = 0; g < NUM_GROUPS; g++) {
                agg[g].sum_qty        += r_sum_qty[g];
                agg[g].sum_base_price += r_sum_base_price[g];
                agg[g].sum_disc_price += r_sum_disc_price[g];
                agg[g].sum_charge     += r_sum_charge[g];
                agg[g].sum_discount   += r_sum_discount[g];
                agg[g].count          += r_count[g];
            }
        } // end omp parallel
    }

    // ---------------------------------------------------------------------------
    // Phase 5: Merge thread-local aggregates (serial, trivial with 6 groups)
    // ---------------------------------------------------------------------------
    AggState global_agg[NUM_GROUPS] = {};
    {
        for (int t = 0; t < num_threads; t++) {
            for (int g = 0; g < NUM_GROUPS; g++) {
                global_agg[g].sum_qty        += thread_aggs[t].groups[g].sum_qty;
                global_agg[g].sum_base_price += thread_aggs[t].groups[g].sum_base_price;
                global_agg[g].sum_disc_price += thread_aggs[t].groups[g].sum_disc_price;
                global_agg[g].sum_charge     += thread_aggs[t].groups[g].sum_charge;
                global_agg[g].sum_discount   += thread_aggs[t].groups[g].sum_discount;
                global_agg[g].count          += thread_aggs[t].groups[g].count;
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
    //   sum_disc_price: accumulated as ep*(100-d) at scale=4 => divide by 10000 for 2dp output
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
            double d_sum_qty        = (double)r.sum_qty / 100.0;
            double d_sum_base_price = (double)r.sum_base_price / 100.0;
            // ep*(100-d): ep in units of 0.01, (100-d) means d is scale=2 so d=7 means 0.07
            // disc_price_actual = (ep/100) * ((100-d)/100) = ep*(100-d)/10000
            double d_sum_disc_price = (double)r.sum_disc_price / 10000.0;
            // charge_actual = ep*(100-d)*(100+t)/1000000
            double d_sum_charge     = (double)r.sum_charge / 1000000.0;
            double d_avg_qty        = (double)r.sum_qty / (double)r.count;
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
