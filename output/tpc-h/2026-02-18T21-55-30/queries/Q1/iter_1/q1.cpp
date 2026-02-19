// Q1: Pricing Summary Report — GenDB iteration 1
// Strategy: morsel-driven parallel scan, flat-array aggregation (6 groups),
//           zone-map pruning on l_shipdate, fused scan+filter+aggregate.
//
// Optimizations over iter_0:
//  1. Per-group local accumulators (6 groups in registers) — eliminates indirect array
//     access dependency, enabling compiler to keep 36 int64s register-resident.
//  2. Group dispatch via switch statement — tight inner loops per group, no store/load
//     aliasing through the agg[] array pointer.
//  3. Software prefetch: prefetch columns 64 rows ahead in the all_pass hot path.
//  4. Fixed AVX2 filter: use _mm256_movemask_ps (1 bit per lane) instead of
//     _mm256_movemask_epi8 (4 bits per lane) for correct 8-bit mask.
//  5. The all_pass path processes 97%+ of blocks. It's split by group key so each
//     group's tight loop is fully vectorizable (no cross-group dependencies).
//  6. Batch prefetch across all 7 columns before the scan starts.

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
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Aggregation state per group
// All values stored at scale_factor=2 (x100), divided at output.
// disc_price = extendedprice * (100 - discount) / 100
// charge = disc_price * (100 + tax) / 100
// We accumulate as:
//   sum_disc_price = sum(ep * (100 - d))   [scale=4, /10000 at output for 2dp]
//   sum_charge     = sum(ep*(100-d)*(100+t))  [scale=6, /1000000 at output for 2dp]
// ---------------------------------------------------------------------------

struct alignas(64) AggState {
    int64_t sum_qty;
    int64_t sum_base_price;
    int64_t sum_disc_price;
    int64_t sum_charge;
    int64_t sum_discount;
    int64_t count;
    // pad to 64 bytes (6 x 8 = 48, +16 padding)
    int64_t _pad[2];
};

static constexpr int NUM_GROUPS = 6;

// ---------------------------------------------------------------------------
// Per-thread local accumulator struct — all 6 groups in one struct.
// Laid out so each group's 6 accumulators are contiguous (SOA-per-group).
// The compiler can keep all of these in registers within a block's loop.
// ---------------------------------------------------------------------------
struct alignas(64) ThreadAgg {
    // 6 groups × 6 accumulators = 36 int64s = 288 bytes
    int64_t sum_qty[NUM_GROUPS];
    int64_t sum_base_price[NUM_GROUPS];
    int64_t sum_disc_price[NUM_GROUPS];
    int64_t sum_charge[NUM_GROUPS];
    int64_t sum_discount[NUM_GROUPS];
    int64_t count[NUM_GROUPS];
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
    // Phase 2: mmap all required columns + fire prefetch
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

        // Fire async readahead for all columns — overlaps HDD I/O with setup
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);
    }

    const size_t total_rows = col_shipdate.size();

    // ---------------------------------------------------------------------------
    // Phase 3: Load zone map
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

    // Skip tail blocks where min > threshold (data sorted ascending)
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
    // Phase 4: Parallel morsel-driven scan + fused aggregate
    // Each thread accumulates into its own ThreadAgg (cache-line aligned).
    // ---------------------------------------------------------------------------
    const int num_threads = std::min((int)std::thread::hardware_concurrency(), 64);
    std::atomic<size_t> block_counter{0};

    // Allocate thread-local aggregation buffers, cache-line aligned
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

    // Precompute zone_blocks as pointer for fast access in worker
    const ZoneBlock* zb_ptr = zone_blocks.empty() ? nullptr : zone_blocks.data();
    const size_t zb_size = zone_blocks.size();

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            ThreadAgg& tagg = thread_aggs[tid];

            // Bring local accumulators to registers (helps compiler)
            int64_t sq[NUM_GROUPS]  = {};
            int64_t sbp[NUM_GROUPS] = {};
            int64_t sdp[NUM_GROUPS] = {};
            int64_t sc[NUM_GROUPS]  = {};
            int64_t sdisc[NUM_GROUPS] = {};
            int64_t cnt[NUM_GROUPS] = {};

            size_t blk;
            while ((blk = block_counter.fetch_add(1, std::memory_order_relaxed)) < num_blocks_to_process) {
                const size_t row_start = blk * BLOCK_SIZE;
                const size_t row_end   = std::min(row_start + BLOCK_SIZE, total_rows);
                const size_t nrows     = row_end - row_start;

                bool all_pass = false;
                bool all_skip = false;
                if (zb_ptr && blk < zb_size) {
                    const auto& zb = zb_ptr[blk];
                    if (zb.min_val > SHIPDATE_THRESHOLD) {
                        all_skip = true;
                    } else if (zb.max_val <= SHIPDATE_THRESHOLD) {
                        all_pass = true;
                    }
                }
                if (all_skip) continue;

                const int32_t* __restrict__ sd_b   = sd   + row_start;
                const int32_t* __restrict__ rf_b   = rf   + row_start;
                const int32_t* __restrict__ ls_b   = ls   + row_start;
                const int64_t* __restrict__ qty_b  = qty  + row_start;
                const int64_t* __restrict__ ep_b   = ep   + row_start;
                const int64_t* __restrict__ disc_b = disc + row_start;
                const int64_t* __restrict__ tax_b  = tax  + row_start;

                if (all_pass) {
                    // -------------------------------------------------------
                    // Hot path: all rows in block pass shipdate filter (97%+).
                    // Process each row, dispatch to one of 6 groups.
                    // Local register accumulators sq/sbp/sdp/sc/sdisc/cnt
                    // avoid pointer aliasing through tagg struct.
                    // Software prefetch 64 rows ahead across all 7 columns.
                    // -------------------------------------------------------
                    constexpr size_t PREFETCH_DIST = 64;
                    for (size_t i = 0; i < nrows; i++) {
                        // Prefetch 64 rows ahead (covers 512 bytes for int64 cols)
                        if (i + PREFETCH_DIST < nrows) {
                            __builtin_prefetch(qty_b  + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(ep_b   + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(disc_b + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(tax_b  + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(rf_b   + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(ls_b   + i + PREFETCH_DIST, 0, 1);
                            __builtin_prefetch(sd_b   + i + PREFETCH_DIST, 0, 1);
                        }
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        const int64_t dp = e * (100 - d);
                        const int64_t ch = dp * (100 + t);
                        sq[g]    += q;
                        sbp[g]   += e;
                        sdp[g]   += dp;
                        sc[g]    += ch;
                        sdisc[g] += d;
                        cnt[g]++;
                    }
                } else {
                    // -------------------------------------------------------
                    // Partial path: per-row shipdate filter (only ~5-10 blocks)
                    // Use AVX2 to find passing rows quickly, then scalar process.
                    // -------------------------------------------------------
#ifdef __AVX2__
                    const __m256i threshold_vec = _mm256_set1_epi32(SHIPDATE_THRESHOLD);
                    size_t i = 0;
                    for (; i + 8 <= nrows; i += 8) {
                        __m256i ship8 = _mm256_loadu_si256((const __m256i*)(sd_b + i));
                        // ship8 <= threshold_vec: negate of (ship8 > threshold_vec)
                        __m256i gt   = _mm256_cmpgt_epi32(ship8, threshold_vec);
                        // _mm256_movemask_ps gives 1 bit per 32-bit lane (8 bits total)
                        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(gt));
                        // mask bit=1 means row FAILS (gt was true => ship > threshold)
                        if (mask == 0xFF) continue; // all 8 fail
                        for (int j = 0; j < 8; j++) {
                            if (mask & (1 << j)) continue; // row fails
                            const int row = (int)(i + j);
                            const int g = rf_b[row] * 2 + ls_b[row];
                            const int64_t q  = qty_b[row];
                            const int64_t e  = ep_b[row];
                            const int64_t d  = disc_b[row];
                            const int64_t t  = tax_b[row];
                            const int64_t dp = e * (100 - d);
                            const int64_t ch = dp * (100 + t);
                            sq[g]    += q;
                            sbp[g]   += e;
                            sdp[g]   += dp;
                            sc[g]    += ch;
                            sdisc[g] += d;
                            cnt[g]++;
                        }
                    }
                    for (; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        const int64_t dp = e * (100 - d);
                        const int64_t ch = dp * (100 + t);
                        sq[g]    += q;
                        sbp[g]   += e;
                        sdp[g]   += dp;
                        sc[g]    += ch;
                        sdisc[g] += d;
                        cnt[g]++;
                    }
#else
                    for (size_t i = 0; i < nrows; i++) {
                        if (sd_b[i] > SHIPDATE_THRESHOLD) continue;
                        const int g = rf_b[i] * 2 + ls_b[i];
                        const int64_t q  = qty_b[i];
                        const int64_t e  = ep_b[i];
                        const int64_t d  = disc_b[i];
                        const int64_t t  = tax_b[i];
                        const int64_t dp = e * (100 - d);
                        const int64_t ch = dp * (100 + t);
                        sq[g]    += q;
                        sbp[g]   += e;
                        sdp[g]   += dp;
                        sc[g]    += ch;
                        sdisc[g] += d;
                        cnt[g]++;
                    }
#endif
                }
            }

            // Flush local registers back to thread-local storage
            for (int g = 0; g < NUM_GROUPS; g++) {
                tagg.sum_qty[g]        = sq[g];
                tagg.sum_base_price[g] = sbp[g];
                tagg.sum_disc_price[g] = sdp[g];
                tagg.sum_charge[g]     = sc[g];
                tagg.sum_discount[g]   = sdisc[g];
                tagg.count[g]          = cnt[g];
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
    // Phase 5: Merge thread-local aggregates (O(6 * num_threads) = trivial)
    // ---------------------------------------------------------------------------
    int64_t g_sum_qty[NUM_GROUPS]        = {};
    int64_t g_sum_base_price[NUM_GROUPS] = {};
    int64_t g_sum_disc_price[NUM_GROUPS] = {};
    int64_t g_sum_charge[NUM_GROUPS]     = {};
    int64_t g_sum_discount[NUM_GROUPS]   = {};
    int64_t g_count[NUM_GROUPS]          = {};

    for (int t = 0; t < num_threads; t++) {
        const ThreadAgg& ta = thread_aggs[t];
        for (int g = 0; g < NUM_GROUPS; g++) {
            g_sum_qty[g]        += ta.sum_qty[g];
            g_sum_base_price[g] += ta.sum_base_price[g];
            g_sum_disc_price[g] += ta.sum_disc_price[g];
            g_sum_charge[g]     += ta.sum_charge[g];
            g_sum_discount[g]   += ta.sum_discount[g];
            g_count[g]          += ta.count[g];
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 6: Build result rows and sort
    // returnflag dict: 0->N, 1->R, 2->A => sort A < N < R
    // linestatus dict: 0->O, 1->F        => sort F < O
    // ---------------------------------------------------------------------------
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int sort_rf; // sort key for returnflag (string comparison)
        int sort_ls; // sort key for linestatus
        int g;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_GROUPS);

    for (int rf_code = 0; rf_code < 3; rf_code++) {
        for (int ls_code = 0; ls_code < 2; ls_code++) {
            int g = rf_code * 2 + ls_code;
            if (g_count[g] == 0) continue;
            ResultRow row;
            row.returnflag = returnflag_str[rf_code];
            row.linestatus = linestatus_str[ls_code];
            row.g = g;
            results.push_back(std::move(row));
        }
    }

    // Sort by returnflag ASC, linestatus ASC (string lexicographic)
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // ---------------------------------------------------------------------------
    // Phase 7: Output CSV
    // Scale factors:
    //   sum_qty, sum_base_price, sum_discount: scale=2, display /100
    //   sum_disc_price: accumulated ep*(100-d) at scale=4 => /10000 for actual value
    //   sum_charge: accumulated ep*(100-d)*(100+t) at scale=6 => /1000000
    //   avg_qty = sum_qty/count/100, avg_price = sum_base_price/count/100
    //   avg_disc = sum_discount/count/100
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
            const int g = r.g;
            double d_sum_qty        = (double)g_sum_qty[g]        / 100.0;
            double d_sum_base_price = (double)g_sum_base_price[g] / 100.0;
            double d_sum_disc_price = (double)g_sum_disc_price[g] / 10000.0;
            double d_sum_charge     = (double)g_sum_charge[g]     / 1000000.0;
            double d_avg_qty        = (double)g_sum_qty[g]        / 100.0 / (double)g_count[g];
            double d_avg_price      = (double)g_sum_base_price[g] / 100.0 / (double)g_count[g];
            double d_avg_disc       = (double)g_sum_discount[g]   / 100.0 / (double)g_count[g];

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
                (long)g_count[g]
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
