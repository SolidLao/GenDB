#ifndef SCAN_H
#define SCAN_H

#include <cstdint>
#include <vector>
#include <functional>
#include <thread>
#include <algorithm>
#include "../storage/storage.h"

// Generic table scan with predicate pushdown
// Predicate: function that takes row_id and returns true if row passes filter
// Projection: function that processes a passing row

template<typename PredicateFn, typename ProjectionFn>
void scan_table(uint64_t row_count, PredicateFn predicate, ProjectionFn projection,
                bool enable_parallel = true, uint64_t parallel_threshold = 100000) {
    if (!enable_parallel || row_count < parallel_threshold) {
        // Single-threaded scan
        for (uint64_t row = 0; row < row_count; ++row) {
            if (predicate(row)) {
                projection(row);
            }
        }
    } else {
        // Parallel scan
        uint32_t num_threads = std::min<uint32_t>(std::thread::hardware_concurrency(), 16);
        std::vector<std::thread> threads;
        uint64_t chunk_size = (row_count + num_threads - 1) / num_threads;

        for (uint32_t tid = 0; tid < num_threads; ++tid) {
            uint64_t start = tid * chunk_size;
            uint64_t end = std::min(start + chunk_size, row_count);

            threads.emplace_back([start, end, &predicate, &projection]() {
                for (uint64_t row = start; row < end; ++row) {
                    if (predicate(row)) {
                        projection(row);
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }
    }
}

// Scan with zone map pruning for range predicates
template<typename T, typename PredicateFn, typename ProjectionFn>
void scan_with_zone_maps(uint64_t row_count, uint32_t block_size,
                         const std::vector<ZoneMap>& zone_maps,
                         T min_value, T max_value,
                         PredicateFn predicate, ProjectionFn projection) {
    uint32_t num_blocks = (row_count + block_size - 1) / block_size;

    for (uint32_t block_id = 0; block_id < num_blocks; ++block_id) {
        // Check zone map
        if (!zone_maps.empty() && block_id < zone_maps.size()) {
            const auto& zone = zone_maps[block_id];
            T zone_min = static_cast<T>(zone.min_value);
            T zone_max = static_cast<T>(zone.max_value);

            // Skip block if it doesn't overlap with range
            if (zone_max < min_value || zone_min > max_value) {
                continue;
            }
        }

        // Scan block
        uint64_t start_row = static_cast<uint64_t>(block_id) * block_size;
        uint64_t end_row = std::min(start_row + block_size, row_count);

        for (uint64_t row = start_row; row < end_row; ++row) {
            if (predicate(row)) {
                projection(row);
            }
        }
    }
}

#endif // SCAN_H
