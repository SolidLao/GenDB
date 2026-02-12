#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <algorithm>
#include "../utils/flat_hash.h"

namespace gendb {
namespace operators {

// Generic parallel hash aggregation operator
// GroupKeyFunc: (size_t row_idx) -> KeyType
// AggregateFunc: (size_t row_idx, AggType& agg) -> void (updates aggregate state)
// MergeFunc: (AggType& dest, const AggType& src) -> void (merges two aggregate states)
// Hash: Hash function for KeyType (defaults to std::hash<KeyType>)
// reserve_size: Optional pre-sizing hint to avoid rehashing (0 = no pre-sizing)
template<typename KeyType, typename AggType,
         typename GroupKeyFunc, typename AggregateFunc, typename MergeFunc,
         typename Hash = std::hash<KeyType>>
gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash> parallel_hash_aggregate(
    size_t row_count,
    GroupKeyFunc&& group_key,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    int num_threads = 0,
    size_t reserve_size = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    // Local aggregation phase with pre-sizing
    std::vector<gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash>> local_aggs(num_threads);

    // Pre-size local aggregation maps if hint provided
    if (reserve_size > 0) {
        size_t local_reserve = (reserve_size + num_threads - 1) / num_threads;
        for (auto& local_agg : local_aggs) {
            local_agg.reserve(local_reserve);
        }
    }

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

    // Merge phase with pre-sizing
    gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash> global_agg;
    if (reserve_size > 0) {
        global_agg.reserve(reserve_size);
    }

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
         typename AggregateFunc, typename MergeFunc,
         typename Hash = std::hash<KeyType>>
gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash> parallel_hash_aggregate_filtered(
    size_t row_count,
    PredicateFunc&& predicate,
    GroupKeyFunc&& group_key,
    AggregateFunc&& aggregate,
    MergeFunc&& merge,
    int num_threads = 0,
    size_t reserve_size = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    // Local aggregation phase with filtering and pre-sizing
    std::vector<gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash>> local_aggs(num_threads);

    // Pre-size local aggregation maps if hint provided
    if (reserve_size > 0) {
        size_t local_reserve = (reserve_size + num_threads - 1) / num_threads;
        for (auto& local_agg : local_aggs) {
            local_agg.reserve(local_reserve);
        }
    }

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

    // Merge phase with pre-sizing
    gendb::flat_hash::flat_hash_map<KeyType, AggType, Hash> global_agg;
    if (reserve_size > 0) {
        global_agg.reserve(reserve_size);
    }

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
