#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <algorithm>
#include <fstream>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Date thresholds — encoded as epoch days
    // 1994-01-01 = 8766, 1995-01-01 = 9131
    const int32_t DATE_LO = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t DATE_HI = gendb::date_str_to_epoch_days("1995-01-01");

    // Discount filter: BETWEEN 0.05 AND 0.07 => scale=100 => [5, 7]
    const int64_t DISC_LO = 5;
    const int64_t DISC_HI = 7;

    // Quantity filter: < 24, scale=1
    const int64_t QTY_MAX = 24;

    // Open columns via mmap (zero-copy)
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Prefetch all columns to page cache concurrently
    gendb::mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    const size_t total_rows = col_shipdate.size();

    // Load zone map index for l_shipdate
    gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/lineitem_l_shipdate.bin");

    // Get qualifying zones (ranges that overlap the date predicate)
    std::vector<std::pair<uint32_t, uint32_t>> zone_ranges;
    {
        GENDB_PHASE("zone_map_prune");
        zone_map.qualifying_ranges(DATE_LO, DATE_HI, zone_ranges);
    }

    // Parallel scan using morsel-driven parallelism
    const int NUM_THREADS = 64;

    // Per-thread int64 accumulators (no false sharing: pad to cache line)
    struct alignas(64) ThreadAcc {
        int64_t sum = 0;
        char pad[64 - sizeof(int64_t)];
    };
    std::vector<ThreadAcc> thread_sums(NUM_THREADS);

    // Distribute zone_ranges across threads using atomic index
    std::atomic<size_t> zone_idx{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            int64_t local_sum = 0;

            const int32_t* shipdate = col_shipdate.data;
            const int64_t* discount = col_discount.data;
            const int64_t* quantity = col_quantity.data;
            const int64_t* extprice = col_extprice.data;

            while (true) {
                size_t zi = zone_idx.fetch_add(1, std::memory_order_relaxed);
                if (zi >= zone_ranges.size()) break;

                uint32_t row_start = zone_ranges[zi].first;
                uint32_t row_end   = zone_ranges[zi].second;
                if (row_end > (uint32_t)total_rows) row_end = (uint32_t)total_rows;

                // Tight fused loop: all 3 filters + accumulate
                for (uint32_t i = row_start; i < row_end; i++) {
                    int32_t sd = shipdate[i];
                    if (sd < DATE_LO | sd >= DATE_HI) continue;
                    int64_t disc = discount[i];
                    if (disc < DISC_LO | disc > DISC_HI) continue;
                    int64_t qty  = quantity[i];
                    if (qty >= QTY_MAX) continue;
                    // accumulate extendedprice * discount (both scale=100, product is scale=10000)
                    local_sum += extprice[i] * disc;
                }
            }

            thread_sums[tid].sum = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();
    }

    // Merge thread-local accumulators
    int64_t total_sum = 0;
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < NUM_THREADS; t++) {
            total_sum += thread_sums[t].sum;
        }
    }

    // Output result
    {
        GENDB_PHASE("output");
        // Scale: extendedprice (scale=100) * discount (scale=100) = scale=10000
        // Revenue = total_sum / 10000.0
        double revenue = static_cast<double>(total_sum) / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        std::FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", revenue);
        std::fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
