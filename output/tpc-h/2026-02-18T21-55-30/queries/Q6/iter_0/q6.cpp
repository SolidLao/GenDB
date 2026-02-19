// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24
//
// Implementation notes:
// - Zone maps cover all 600 blocks (data not sorted by shipdate), so no block pruning possible.
//   Deviation from plan: skip zone map loading; proceed directly to parallel full scan.
// - Data encoding: l_discount stored as 0-10 (scale 100), l_quantity scale=100, l_extendedprice scale=100
// - Predicates in stored units: shipdate >= 8766 && shipdate < 9131
//                                discount >= 5 && discount <= 7  (0.05 and 0.07 * 100)
//                                quantity < 2400                  (24.00 * 100)
// - Revenue = SUM(extendedprice * discount) / 10000 for final output
// - 64 threads, morsel-driven over 600 blocks of 100,000 rows each

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <fstream>

#include "mmap_utils.h"
#include "timing_utils.h"

static constexpr int32_t  SHIPDATE_LO  = 8766;   // 1994-01-01
static constexpr int32_t  SHIPDATE_HI  = 9131;   // 1995-01-01
static constexpr int64_t  DISC_LO      = 5;      // 0.05 * 100
static constexpr int64_t  DISC_HI      = 7;      // 0.07 * 100
static constexpr int64_t  QTY_HI       = 2400;   // 24.00 * 100 (quantity scale=100)

static constexpr size_t   BLOCK_SIZE   = 100000;

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Phase 1: Load columns via mmap (zero-copy)
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;

    {
        GENDB_PHASE("load_columns");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        // Prefetch all columns into page cache concurrently
        mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);
    }

    const size_t total_rows = col_shipdate.count;
    // Compute number of blocks (ceiling division)
    const size_t num_blocks = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // -----------------------------------------------------------------------
    // Phase 2: Parallel morsel-driven scan
    // -----------------------------------------------------------------------
    int64_t global_sum = 0;

    {
        GENDB_PHASE("main_scan");

        const unsigned int nthreads = std::min(
            (unsigned int)64,
            std::thread::hardware_concurrency()
        );

        std::vector<int64_t> partial_sums(nthreads, 0LL);
        std::atomic<size_t> block_counter{0};

        const int32_t*  shipdate_ptr = col_shipdate.data;
        const int64_t*  discount_ptr = col_discount.data;
        const int64_t*  quantity_ptr = col_quantity.data;
        const int64_t*  extprice_ptr = col_extprice.data;

        auto worker = [&](unsigned int tid) {
            int64_t local_sum = 0;

            while (true) {
                size_t blk = block_counter.fetch_add(1, std::memory_order_relaxed);
                if (blk >= num_blocks) break;

                size_t row_start = blk * BLOCK_SIZE;
                size_t row_end   = row_start + BLOCK_SIZE;
                if (row_end > total_rows) row_end = total_rows;

                const int32_t* sd  = shipdate_ptr + row_start;
                const int64_t* dis = discount_ptr + row_start;
                const int64_t* qty = quantity_ptr + row_start;
                const int64_t* ep  = extprice_ptr + row_start;

                size_t n = row_end - row_start;

                // Hot loop: fused scan + filter + accumulate
                // Predicate order: discount (cheapest), then quantity, then shipdate
                for (size_t i = 0; i < n; i++) {
                    // Check discount first (most selective for narrowing: only 3 values out of 11)
                    int64_t d = dis[i];
                    if (d < DISC_LO || d > DISC_HI) continue;

                    // Check quantity (eliminates ~46% of remaining)
                    int64_t q = qty[i];
                    if (q >= QTY_HI) continue;

                    // Check shipdate last (still must check, ~14% selectivity)
                    int32_t s = sd[i];
                    if (s < SHIPDATE_LO || s >= SHIPDATE_HI) continue;

                    // Accumulate: extprice * discount (both scaled by 100, so product scaled by 10000)
                    local_sum += ep[i] * d;
                }
            }

            partial_sums[tid] = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (unsigned int t = 0; t < nthreads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();

        // Merge partial sums
        for (unsigned int t = 0; t < nthreads; t++) {
            global_sum += partial_sums[t];
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Output results
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // global_sum = SUM(extprice * discount) where both are scaled by 100
        // So actual revenue = global_sum / (100 * 100) = global_sum / 10000
        // We need 4 decimal places of precision: global_sum / 10000 with remainder
        int64_t integer_part = global_sum / 10000LL;
        int64_t frac_part    = global_sum % 10000LL;

        // Format: "integer_part.frac_part" with 4 decimal places
        // Expected output has 4 decimal places (e.g., 1230113636.0101)
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%lld.%04lld",
                      (long long)integer_part, (long long)frac_part);

        std::string out_path = results_dir + "/Q6.csv";
        std::ofstream ofs(out_path);
        ofs << "revenue\n";
        ofs << buf << "\n";
        ofs.close();

        std::printf("revenue = %s\n", buf);
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
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
