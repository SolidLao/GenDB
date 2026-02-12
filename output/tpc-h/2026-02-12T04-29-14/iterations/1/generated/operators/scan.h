#pragma once

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <algorithm>

namespace gendb {
namespace operators {

/**
 * Parallel table scan with optional predicate pushdown
 *
 * Template parameters:
 *   ProcessFunc: Function to process each row that passes the filter
 *                Signature: void(size_t row_idx)
 *   FilterFunc:  Optional filter predicate
 *                Signature: bool(size_t row_idx)
 */
template<typename ProcessFunc, typename FilterFunc>
void parallel_scan(
    size_t row_count,
    ProcessFunc process_row,
    FilterFunc filter_row,
    size_t morsel_size = 10000
) {
    int num_threads = std::thread::hardware_concurrency();
    std::atomic<size_t> next_row{0};

    auto worker = [&]() {
        while (true) {
            size_t start_row = next_row.fetch_add(morsel_size);
            if (start_row >= row_count) break;
            size_t end_row = std::min(start_row + morsel_size, row_count);

            for (size_t i = start_row; i < end_row; i++) {
                if (filter_row(i)) {
                    process_row(i);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) {
        t.join();
    }
}

/**
 * Parallel scan variant without filter (processes all rows)
 */
template<typename ProcessFunc>
void parallel_scan(
    size_t row_count,
    ProcessFunc process_row,
    size_t morsel_size = 10000
) {
    parallel_scan(row_count, process_row, [](size_t) { return true; }, morsel_size);
}

/**
 * Parallel scan with thread-local state
 *
 * Template parameters:
 *   LocalState:  Type of thread-local state
 *   ProcessFunc: Function to process each row
 *                Signature: void(LocalState& local, size_t row_idx)
 *   FilterFunc:  Optional filter predicate
 *                Signature: bool(size_t row_idx)
 *   MergeFunc:   Function to merge thread-local states
 *                Signature: void(const LocalState& local)
 */
template<typename LocalState, typename ProcessFunc, typename FilterFunc, typename MergeFunc>
void parallel_scan_with_local(
    size_t row_count,
    ProcessFunc process_row,
    FilterFunc filter_row,
    MergeFunc merge_local,
    size_t morsel_size = 10000
) {
    int num_threads = std::thread::hardware_concurrency();
    std::vector<LocalState> local_states(num_threads);
    std::atomic<size_t> next_row{0};

    auto worker = [&](int tid) {
        auto& local = local_states[tid];
        while (true) {
            size_t start_row = next_row.fetch_add(morsel_size);
            if (start_row >= row_count) break;
            size_t end_row = std::min(start_row + morsel_size, row_count);

            for (size_t i = start_row; i < end_row; i++) {
                if (filter_row(i)) {
                    process_row(local, i);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local states
    for (const auto& local : local_states) {
        merge_local(local);
    }
}

/**
 * Parallel scan with thread-local state, no filter
 */
template<typename LocalState, typename ProcessFunc, typename MergeFunc>
void parallel_scan_with_local(
    size_t row_count,
    ProcessFunc process_row,
    MergeFunc merge_local,
    size_t morsel_size = 10000
) {
    parallel_scan_with_local<LocalState>(
        row_count,
        process_row,
        [](size_t) { return true; },
        merge_local,
        morsel_size
    );
}

} // namespace operators
} // namespace gendb
