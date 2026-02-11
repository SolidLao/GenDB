#pragma once

#include "../storage/storage.h"
#include <functional>
#include <thread>
#include <vector>

namespace gendb {
namespace operators {

// Generic parallel scan operator with predicate pushdown
// Scans a column-based table in parallel with morsel-driven execution
// Filters tuples using a user-provided predicate function
// Returns a vector of row indices that pass the filter
template<typename... ColumnTypes>
class ParallelScan {
public:
    using PredicateFn = std::function<bool(size_t, const ColumnTypes&...)>;

    // Constructor: takes columns to scan and number of rows
    ParallelScan(size_t num_rows) : num_rows_(num_rows) {}

    // Execute scan with predicate filter
    // Returns indices of rows that pass the predicate
    std::vector<size_t> execute(PredicateFn predicate, const ColumnTypes&... columns) {
        const size_t num_threads = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        std::vector<std::vector<size_t>> local_results(num_threads);

        size_t chunk_size = (num_rows_ + num_threads - 1) / num_threads;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                size_t start_idx = tid * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, num_rows_);

                auto& local_result = local_results[tid];
                local_result.reserve((end_idx - start_idx) / 4); // Estimate 25% selectivity

                for (size_t i = start_idx; i < end_idx; ++i) {
                    if (predicate(i, columns...)) {
                        local_result.push_back(i);
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Merge results
        size_t total_size = 0;
        for (const auto& local : local_results) {
            total_size += local.size();
        }

        std::vector<size_t> result;
        result.reserve(total_size);
        for (const auto& local : local_results) {
            result.insert(result.end(), local.begin(), local.end());
        }

        return result;
    }

    // Execute scan without predicate (full table scan)
    std::vector<size_t> execute() {
        std::vector<size_t> result(num_rows_);
        for (size_t i = 0; i < num_rows_; ++i) {
            result[i] = i;
        }
        return result;
    }

private:
    size_t num_rows_;
};

// Simplified scan operator for common patterns
// Scans with parallel execution and applies a filter predicate
template<typename PredicateFn>
std::vector<size_t> parallel_scan(size_t num_rows, PredicateFn predicate) {
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<std::vector<size_t>> local_results(num_threads);

    size_t chunk_size = (num_rows + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, num_rows);

            auto& local_result = local_results[tid];
            local_result.reserve((end_idx - start_idx) / 4); // Estimate 25% selectivity

            for (size_t i = start_idx; i < end_idx; ++i) {
                if (predicate(i)) {
                    local_result.push_back(i);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge results
    size_t total_size = 0;
    for (const auto& local : local_results) {
        total_size += local.size();
    }

    std::vector<size_t> result;
    result.reserve(total_size);
    for (const auto& local : local_results) {
        result.insert(result.end(), local.begin(), local.end());
    }

    return result;
}

}  // namespace operators
}  // namespace gendb
