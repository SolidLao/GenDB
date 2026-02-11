#pragma once

#include <unordered_map>
#include <vector>
#include <thread>
#include <functional>

namespace gendb {
namespace operators {

// Generic parallel hash aggregation operator
// Uses thread-local hash tables for lock-free parallel aggregation
// Template parameters:
//   KeyType: type of the grouping key (must be hashable)
//   AggStateType: type of the aggregate state (must support += or custom merge)
//   HashFn: hash function for KeyType (default: std::hash)
template<typename KeyType, typename AggStateType, typename HashFn = std::hash<KeyType>>
class ParallelHashAgg {
public:
    using LocalHashTable = std::unordered_map<KeyType, AggStateType, HashFn>;
    using GlobalHashTable = std::unordered_map<KeyType, AggStateType, HashFn>;

    // Function type for extracting key and updating aggregate state
    using AggFn = std::function<void(size_t, KeyType&, AggStateType&)>;

    // Constructor
    ParallelHashAgg() = default;

    // Execute aggregation with parallel processing
    // input_size: number of rows to process
    // agg_fn: function that extracts key and updates aggregate state for a given row index
    //         signature: void(size_t row_idx, KeyType& key, AggStateType& state)
    //         Should populate key and state for the row. State should contain delta values.
    GlobalHashTable execute(size_t input_size, AggFn agg_fn) {
        const size_t num_threads = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        std::vector<LocalHashTable> local_aggs(num_threads);

        size_t chunk_size = (input_size + num_threads - 1) / num_threads;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                size_t start_idx = tid * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, input_size);

                auto& local_agg = local_aggs[tid];

                for (size_t i = start_idx; i < end_idx; ++i) {
                    KeyType key = KeyType();
                    AggStateType state = AggStateType();
                    agg_fn(i, key, state);

                    // Skip if state count is 0 (filtered out)
                    if (state.count == 0) continue;

                    // Update local hash table
                    auto& agg = local_agg[key];
                    agg.sum_qty += state.sum_qty;
                    agg.sum_base_price += state.sum_base_price;
                    agg.sum_disc_price += state.sum_disc_price;
                    agg.sum_charge += state.sum_charge;
                    agg.sum_discount += state.sum_discount;
                    agg.count += state.count;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Merge thread-local results into global hash table
        return merge_local_tables(local_aggs);
    }

    // Execute aggregation on a filtered subset of rows
    GlobalHashTable execute(const std::vector<size_t>& indices, AggFn agg_fn) {
        const size_t num_threads = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        std::vector<LocalHashTable> local_aggs(num_threads);

        size_t input_size = indices.size();
        size_t chunk_size = (input_size + num_threads - 1) / num_threads;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                size_t start_idx = tid * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, input_size);

                auto& local_agg = local_aggs[tid];

                for (size_t idx = start_idx; idx < end_idx; ++idx) {
                    size_t row_id = indices[idx];
                    KeyType key = KeyType();
                    AggStateType state = AggStateType();
                    agg_fn(row_id, key, state);

                    // Skip if state count is 0 (filtered out)
                    if (state.count == 0) continue;

                    // Update local hash table
                    auto& agg = local_agg[key];
                    agg.sum_qty += state.sum_qty;
                    agg.sum_base_price += state.sum_base_price;
                    agg.sum_disc_price += state.sum_disc_price;
                    agg.sum_charge += state.sum_charge;
                    agg.sum_discount += state.sum_discount;
                    agg.count += state.count;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Merge thread-local results into global hash table
        return merge_local_tables(local_aggs);
    }

private:
    // Merge two aggregate states (default: assumes += operator)
    void merge_state(AggStateType& dest, const AggStateType& src) {
        dest += src;
    }

    // Merge all thread-local hash tables into a global one
    GlobalHashTable merge_local_tables(const std::vector<LocalHashTable>& local_aggs) {
        GlobalHashTable global_agg;

        for (const auto& local_agg : local_aggs) {
            for (const auto& [key, state] : local_agg) {
                auto it = global_agg.find(key);
                if (it != global_agg.end()) {
                    merge_state(it->second, state);
                } else {
                    global_agg[key] = state;
                }
            }
        }

        return global_agg;
    }
};

// Simplified parallel hash aggregation for common aggregate types
// Uses thread-local hash tables and merges at the end
template<typename KeyType, typename AggStateType, typename HashFn, typename AggFn>
std::unordered_map<KeyType, AggStateType, HashFn>
parallel_hash_aggregate(size_t input_size, AggFn agg_fn) {
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<std::unordered_map<KeyType, AggStateType, HashFn>> local_aggs(num_threads);

    size_t chunk_size = (input_size + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, input_size);

            auto& local_agg = local_aggs[tid];

            for (size_t i = start_idx; i < end_idx; ++i) {
                agg_fn(i, local_agg);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results
    std::unordered_map<KeyType, AggStateType, HashFn> global_agg;
    for (const auto& local_agg : local_aggs) {
        for (const auto& [key, state] : local_agg) {
            auto& global_state = global_agg[key];
            // Assumes AggStateType has appropriate merge semantics
            // For simple aggregates, this is typically +=
            global_state.sum_qty += state.sum_qty;
            global_state.sum_base_price += state.sum_base_price;
            global_state.sum_disc_price += state.sum_disc_price;
            global_state.sum_charge += state.sum_charge;
            global_state.sum_discount += state.sum_discount;
            global_state.count += state.count;
        }
    }

    return global_agg;
}

}  // namespace operators
}  // namespace gendb
