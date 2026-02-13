#include "queries.h"
#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>

namespace gendb {

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns
    size_t row_count;
    auto l_shipdate = ColumnReader::mmap_int32(gendb_dir + "/lineitem.l_shipdate.bin", row_count);
    auto l_discount = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_discount.bin", row_count);
    auto l_quantity = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_quantity.bin", row_count);
    auto l_extendedprice = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_extendedprice.bin", row_count);

    // Load or build zone maps for selective filtering
    std::string zm_shipdate_path = gendb_dir + "/lineitem.l_shipdate.zonemap";
    std::string zm_discount_path = gendb_dir + "/lineitem.l_discount.zonemap";
    std::string zm_quantity_path = gendb_dir + "/lineitem.l_quantity.zonemap";

    ZoneMap<int32_t> shipdate_zonemap;
    ZoneMap<int64_t> discount_zonemap;
    ZoneMap<int64_t> quantity_zonemap;

    // Try to load existing zone maps, otherwise build them
    constexpr size_t BLOCK_SIZE = 8192;

    try {
        shipdate_zonemap = ZoneMapReader::read_zonemap<int32_t>(zm_shipdate_path);
        discount_zonemap = ZoneMapReader::read_zonemap<int64_t>(zm_discount_path);
        quantity_zonemap = ZoneMapReader::read_zonemap<int64_t>(zm_quantity_path);
    } catch (const std::exception&) {
        // Build zone maps on-the-fly if they don't exist
        size_t num_blocks_build = (row_count + BLOCK_SIZE - 1) / BLOCK_SIZE;

        shipdate_zonemap.block_size = BLOCK_SIZE;
        shipdate_zonemap.entries.reserve(num_blocks_build);
        for (size_t block_idx = 0; block_idx < num_blocks_build; ++block_idx) {
            size_t block_start = block_idx * BLOCK_SIZE;
            size_t block_end = std::min(block_start + BLOCK_SIZE, row_count);
            int32_t min_val = l_shipdate[block_start];
            int32_t max_val = l_shipdate[block_start];
            for (size_t i = block_start + 1; i < block_end; ++i) {
                if (l_shipdate[i] < min_val) min_val = l_shipdate[i];
                if (l_shipdate[i] > max_val) max_val = l_shipdate[i];
            }
            shipdate_zonemap.entries.push_back({min_val, max_val});
        }

        discount_zonemap.block_size = BLOCK_SIZE;
        discount_zonemap.entries.reserve(num_blocks_build);
        for (size_t block_idx = 0; block_idx < num_blocks_build; ++block_idx) {
            size_t block_start = block_idx * BLOCK_SIZE;
            size_t block_end = std::min(block_start + BLOCK_SIZE, row_count);
            int64_t min_val = l_discount[block_start];
            int64_t max_val = l_discount[block_start];
            for (size_t i = block_start + 1; i < block_end; ++i) {
                if (l_discount[i] < min_val) min_val = l_discount[i];
                if (l_discount[i] > max_val) max_val = l_discount[i];
            }
            discount_zonemap.entries.push_back({min_val, max_val});
        }

        quantity_zonemap.block_size = BLOCK_SIZE;
        quantity_zonemap.entries.reserve(num_blocks_build);
        for (size_t block_idx = 0; block_idx < num_blocks_build; ++block_idx) {
            size_t block_start = block_idx * BLOCK_SIZE;
            size_t block_end = std::min(block_start + BLOCK_SIZE, row_count);
            int64_t min_val = l_quantity[block_start];
            int64_t max_val = l_quantity[block_start];
            for (size_t i = block_start + 1; i < block_end; ++i) {
                if (l_quantity[i] < min_val) min_val = l_quantity[i];
                if (l_quantity[i] > max_val) max_val = l_quantity[i];
            }
            quantity_zonemap.entries.push_back({min_val, max_val});
        }

        // Try to save for next time (may fail if directory is read-only, which is fine)
        try {
            ZoneMapWriter::write_zonemap(zm_shipdate_path, shipdate_zonemap);
            ZoneMapWriter::write_zonemap(zm_discount_path, discount_zonemap);
            ZoneMapWriter::write_zonemap(zm_quantity_path, quantity_zonemap);
        } catch (...) {
            // Ignore write failures
        }
    }

    // Parallel scan and aggregation with zone map block skipping
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<int64_t> local_sums(num_threads, 0);
    std::vector<size_t> blocks_scanned(num_threads, 0);
    std::vector<size_t> blocks_skipped(num_threads, 0);

    size_t num_blocks = shipdate_zonemap.get_num_blocks();
    size_t blocks_per_thread = (num_blocks + num_threads - 1) / num_threads;

    // Filters:
    // l_shipdate >= '1994-01-01' AND < '1995-01-01'
    // l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7)
    // l_quantity < 24 (scaled: 2400)

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            size_t start_block = t * blocks_per_thread;
            size_t end_block = std::min(start_block + blocks_per_thread, num_blocks);

            int64_t local_sum = 0;
            size_t scanned = 0;
            size_t skipped = 0;

            for (size_t block_id = start_block; block_id < end_block; ++block_id) {
                // Zone map filtering: skip blocks that can't match predicates
                // l_shipdate: [DATE_1994_01_01, DATE_1995_01_01)
                bool shipdate_match = shipdate_zonemap.block_overlaps_range(
                    block_id, DATE_1994_01_01, DATE_1995_01_01 - 1);

                // l_discount: [5, 7]
                bool discount_match = discount_zonemap.block_overlaps_range(
                    block_id, 5, 7);

                // l_quantity: < 2400 (need min_value < 2400)
                bool quantity_match = quantity_zonemap.block_contains_lt(
                    block_id, 2400);

                if (!shipdate_match || !discount_match || !quantity_match) {
                    skipped++;
                    continue;  // Skip this entire block
                }

                scanned++;

                // Scan rows in this block
                auto [start_row, end_row] = shipdate_zonemap.get_block_range(block_id, row_count);

                for (size_t i = start_row; i < end_row; ++i) {
                    if (l_shipdate[i] >= DATE_1994_01_01 &&
                        l_shipdate[i] < DATE_1995_01_01 &&
                        l_discount[i] >= 5 && l_discount[i] <= 7 &&
                        l_quantity[i] < 2400) {
                        // revenue = l_extendedprice * l_discount
                        // Both scaled by 100, so result is scaled by 10000
                        local_sum += l_extendedprice[i] * l_discount[i];
                    }
                }
            }

            local_sums[t] = local_sum;
            blocks_scanned[t] = scanned;
            blocks_skipped[t] = skipped;
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Reduce
    int64_t total_revenue = 0;
    for (int64_t sum : local_sums) {
        total_revenue += sum;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Calculate block skipping statistics
    size_t total_scanned = 0, total_skipped = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_scanned += blocks_scanned[i];
        total_skipped += blocks_skipped[i];
    }

    std::cout << "Q6: 1 row in " << duration << " ms";
    std::cout << " [Zone map: " << total_scanned << " blocks scanned, "
              << total_skipped << " blocks skipped ("
              << (100.0 * total_skipped / (total_scanned + total_skipped))
              << "%)]\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << std::fixed << std::setprecision(2);
        // FIX: Add header row before data
        out << "revenue\n";
        // Divide by 10000 to unscale (100 * 100)
        out << (total_revenue / 10000.0) << "\n";
    }

    // Cleanup
    ColumnReader::unmap(l_shipdate, row_count * sizeof(int32_t));
    ColumnReader::unmap(l_discount, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_quantity, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_extendedprice, row_count * sizeof(int64_t));
}

} // namespace gendb
