#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <algorithm>

namespace gendb {
namespace operators {

// Generic parallel scan operator with predicate pushdown
// PredicateFunc: (size_t row_idx) -> bool
// ProcessFunc: (size_t row_idx) -> void
template<typename PredicateFunc, typename ProcessFunc>
void parallel_scan(
    size_t row_count,
    PredicateFunc&& predicate,
    ProcessFunc&& process,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    std::vector<std::thread> threads;
    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (predicate(i)) {
                    process(i);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

// Sequential scan (for small tables)
template<typename PredicateFunc, typename ProcessFunc>
void sequential_scan(
    size_t row_count,
    PredicateFunc&& predicate,
    ProcessFunc&& process)
{
    for (size_t i = 0; i < row_count; i++) {
        if (predicate(i)) {
            process(i);
        }
    }
}

} // namespace operators
} // namespace gendb
