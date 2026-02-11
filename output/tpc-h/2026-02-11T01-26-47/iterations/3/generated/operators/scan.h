#pragma once

#include <vector>
#include <functional>
#include <thread>
#include <atomic>

namespace gendb {

// Parallel scan with predicate pushdown
// Processes table in morsels (chunks) across multiple threads
// Returns vector of row indices that pass the predicate
template<typename Table, typename Predicate>
std::vector<size_t> parallel_scan(const Table& table,
                                   Predicate&& pred,
                                   size_t num_threads = 0,
                                   size_t morsel_size = 10000) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    const size_t row_count = table.size();
    const size_t total_morsels = (row_count + morsel_size - 1) / morsel_size;

    std::atomic<size_t> next_morsel{0};
    std::vector<std::vector<size_t>> thread_results(num_threads);

    auto worker = [&](size_t thread_id) {
        auto& local_result = thread_results[thread_id];
        local_result.reserve(row_count / num_threads);

        while (true) {
            size_t morsel_idx = next_morsel.fetch_add(1);
            if (morsel_idx >= total_morsels) break;

            size_t start = morsel_idx * morsel_size;
            size_t end = std::min(start + morsel_size, row_count);

            for (size_t i = start; i < end; ++i) {
                if (pred(table, i)) {
                    local_result.push_back(i);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Merge results
    std::vector<size_t> result;
    size_t total_size = 0;
    for (const auto& r : thread_results) {
        total_size += r.size();
    }
    result.reserve(total_size);

    for (const auto& r : thread_results) {
        result.insert(result.end(), r.begin(), r.end());
    }

    return result;
}

// Parallel scan with aggregation (e.g., for Q6-style queries)
// Instead of returning row indices, applies an aggregation function
// AggFunc signature: void(AggState& state, const Table& table, size_t row_index)
// MergeFunc signature: void(AggState& dest, const AggState& src)
template<typename Table, typename Predicate, typename AggState, typename AggFunc, typename MergeFunc>
AggState parallel_scan_aggregate(const Table& table,
                                  Predicate&& pred,
                                  AggFunc&& agg_fn,
                                  MergeFunc&& merge_fn,
                                  size_t num_threads = 0,
                                  size_t morsel_size = 10000) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    const size_t row_count = table.size();
    const size_t total_morsels = (row_count + morsel_size - 1) / morsel_size;

    std::atomic<size_t> next_morsel{0};
    std::vector<AggState> thread_states(num_threads);

    auto worker = [&](size_t thread_id) {
        auto& local_state = thread_states[thread_id];

        while (true) {
            size_t morsel_idx = next_morsel.fetch_add(1);
            if (morsel_idx >= total_morsels) break;

            size_t start = morsel_idx * morsel_size;
            size_t end = std::min(start + morsel_size, row_count);

            for (size_t i = start; i < end; ++i) {
                if (pred(table, i)) {
                    agg_fn(local_state, table, i);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local states
    AggState result{};
    for (const auto& state : thread_states) {
        merge_fn(result, state);
    }

    return result;
}

// Sequential scan (simpler, for small tables or when parallelism overhead too high)
template<typename Table, typename Predicate>
std::vector<size_t> scan(const Table& table, Predicate&& pred) {
    std::vector<size_t> result;
    result.reserve(table.size() / 10); // Estimate

    for (size_t i = 0; i < table.size(); ++i) {
        if (pred(table, i)) {
            result.push_back(i);
        }
    }

    return result;
}

// Sequential scan with aggregation
template<typename Table, typename Predicate, typename AggState, typename AggFunc>
AggState scan_aggregate(const Table& table,
                       Predicate&& pred,
                       AggFunc&& agg_fn) {
    AggState result{};

    for (size_t i = 0; i < table.size(); ++i) {
        if (pred(table, i)) {
            agg_fn(result, table, i);
        }
    }

    return result;
}

} // namespace gendb
