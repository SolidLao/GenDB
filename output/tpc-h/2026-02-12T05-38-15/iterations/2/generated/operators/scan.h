#pragma once

#include <cstdint>
#include <vector>
#include <thread>
#include <functional>

namespace gendb {
namespace operators {

/**
 * Generic parallel scan operator with optional predicate pushdown.
 *
 * Splits input data into chunks and processes them in parallel using
 * all available hardware threads. Each thread executes a user-provided
 * function on its assigned row range.
 *
 * Template Parameters:
 *   ProcessFunc: Function signature (size_t thread_id, size_t start_row, size_t end_row)
 *
 * Usage:
 *   parallel_scan(num_rows, [&](size_t tid, size_t start, size_t end) {
 *       // Process rows [start, end) on thread tid
 *   });
 */
template<typename ProcessFunc>
void parallel_scan(size_t num_rows, ProcessFunc&& process_func) {
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    size_t rows_per_thread = num_rows / num_threads;

    for (unsigned int t = 0; t < num_threads; ++t) {
        size_t start = t * rows_per_thread;
        size_t end = (t == num_threads - 1) ? num_rows : (t + 1) * rows_per_thread;
        threads.emplace_back(std::forward<ProcessFunc>(process_func), t, start, end);
    }

    for (auto& th : threads) {
        th.join();
    }
}

/**
 * Sequential scan operator (for small tables or debugging).
 *
 * Processes all rows sequentially using a single thread.
 *
 * Template Parameters:
 *   ProcessFunc: Function signature (size_t row_idx)
 */
template<typename ProcessFunc>
void sequential_scan(size_t num_rows, ProcessFunc&& process_func) {
    for (size_t i = 0; i < num_rows; ++i) {
        process_func(i);
    }
}

} // namespace operators
} // namespace gendb
