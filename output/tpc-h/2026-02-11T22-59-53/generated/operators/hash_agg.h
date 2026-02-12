#pragma once

#include <unordered_map>
#include <vector>
#include <thread>
#include <functional>
#include <algorithm>

namespace gendb {
namespace operators {

// Generic parallel hash aggregation operator
// GroupKeyFunc: (size_t row_idx) -> KeyType
// AggregateFunc: (size_t row_idx, AggType& agg) -> void (updates aggregate state)
// MergeFunc: (AggType& dest, const AggType& src) -> void (merges two aggregate states)
template<typename KeyType, typename AggType,
         typename GroupKeyFunc, typename AggregateFunc, typename MergeFunc>
std::unordered_map<KeyType, AggType> parallel_hash_aggregate(
    size_t row_count,
    GroupKeyFunc&& group_key,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    // Local aggregation phase
    std::vector<std::unordered_map<KeyType, AggType>> local_aggs(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, row_count);

            auto& local_agg = local_aggs[t];

            for (size_t i = start_idx; i < end_idx; i++) {
                KeyType key = group_key(i);
                aggregate(i, local_agg[key]);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge phase
    std::unordered_map<KeyType, AggType> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, agg] : local) {
            merge(global_agg[key], agg);
        }
    }

    return global_agg;
}

// Parallel hash aggregation with predicate pushdown
template<typename KeyType, typename AggType,
         typename PredicateFunc, typename GroupKeyFunc,
         typename AggregateFunc, typename MergeFunc>
std::unordered_map<KeyType, AggType> parallel_hash_aggregate_filtered(
    size_t row_count,
    PredicateFunc&& predicate,
    GroupKeyFunc&& group_key,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    // Local aggregation phase with filtering
    std::vector<std::unordered_map<KeyType, AggType>> local_aggs(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, row_count);

            auto& local_agg = local_aggs[t];

            for (size_t i = start_idx; i < end_idx; i++) {
                if (predicate(i)) {
                    KeyType key = group_key(i);
                    aggregate(i, local_agg[key]);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge phase
    std::unordered_map<KeyType, AggType> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, agg] : local) {
            merge(global_agg[key], agg);
        }
    }

    return global_agg;
}

// Scalar aggregation (no grouping) - parallel local aggregation then merge
template<typename AggType, typename AggregateFunc, typename MergeFunc>
AggType parallel_scalar_aggregate(
    size_t row_count,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    const AggType& init_value,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    std::vector<AggType> local_aggs(num_threads, init_value);

    std::vector<std::thread> threads;
    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                aggregate(i, local_aggs[t]);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge all local aggregates
    AggType result = init_value;
    for (const auto& local : local_aggs) {
        merge(result, local);
    }

    return result;
}

// Scalar aggregation with predicate pushdown
template<typename AggType, typename PredicateFunc,
         typename AggregateFunc, typename MergeFunc>
AggType parallel_scalar_aggregate_filtered(
    size_t row_count,
    PredicateFunc&& predicate,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    const AggType& init_value,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    std::vector<AggType> local_aggs(num_threads, init_value);

    std::vector<std::thread> threads;
    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (predicate(i)) {
                    aggregate(i, local_aggs[t]);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge all local aggregates
    AggType result = init_value;
    for (const auto& local : local_aggs) {
        merge(result, local);
    }

    return result;
}

} // namespace operators
} // namespace gendb
