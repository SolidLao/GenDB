#pragma once

#include <unordered_map>
#include <vector>
#include <thread>
#include <atomic>
#include <algorithm>

namespace gendb {
namespace operators {

/**
 * Generic hash aggregation implementation
 * Supports parallel aggregation with thread-local hash tables
 *
 * Template parameters:
 *   GroupKey:   Type of the GROUP BY key
 *   AggState:   Type of the aggregate state (e.g., sum, count, etc.)
 *
 * Usage:
 *   1. Create HashAggregation instance
 *   2. Call aggregate() with input data and update function
 *   3. Call finalize() to get sorted results
 */
template<typename GroupKey, typename AggState>
class HashAggregation {
public:
    using AggMap = std::unordered_map<GroupKey, AggState>;

    /**
     * Perform parallel hash aggregation
     *
     * @param row_count Number of input rows
     * @param get_key Function to extract group key: GroupKey(size_t row_idx)
     * @param update_agg Function to update aggregate: void(AggState& agg, size_t row_idx)
     * @param filter Optional filter predicate: bool(size_t row_idx)
     * @param estimated_groups Estimated number of distinct groups (for pre-sizing)
     * @param morsel_size Chunk size for parallel processing
     */
    template<typename GetKeyFunc, typename UpdateFunc, typename FilterFunc>
    void aggregate(
        size_t row_count,
        GetKeyFunc get_key,
        UpdateFunc update_agg,
        FilterFunc filter,
        size_t estimated_groups = 0,
        size_t morsel_size = 10000
    ) {
        int num_threads = std::thread::hardware_concurrency();
        std::vector<AggMap> local_aggs(num_threads);

        // Pre-size local hash tables
        if (estimated_groups > 0) {
            size_t local_size = static_cast<size_t>(estimated_groups / 0.75);
            for (auto& local : local_aggs) {
                local.reserve(local_size);
            }
        }

        std::atomic<size_t> next_row{0};

        auto worker = [&](int tid) {
            auto& local = local_aggs[tid];
            while (true) {
                size_t start_row = next_row.fetch_add(morsel_size);
                if (start_row >= row_count) break;
                size_t end_row = std::min(start_row + morsel_size, row_count);

                for (size_t i = start_row; i < end_row; i++) {
                    if (filter(i)) {
                        GroupKey key = get_key(i);
                        auto& agg = local[key];
                        update_agg(agg, i);
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

        // Merge thread-local aggregations into global hash table
        merge_local_aggregations(local_aggs);
    }

    /**
     * Aggregate without filter (processes all rows)
     */
    template<typename GetKeyFunc, typename UpdateFunc>
    void aggregate(
        size_t row_count,
        GetKeyFunc get_key,
        UpdateFunc update_agg,
        size_t estimated_groups = 0,
        size_t morsel_size = 10000
    ) {
        aggregate(
            row_count,
            get_key,
            update_agg,
            [](size_t) { return true; },
            estimated_groups,
            morsel_size
        );
    }

    /**
     * Merge thread-local aggregations
     * Can be overridden for custom merge logic
     */
    template<typename MergeFunc>
    void merge_local_aggregations(
        const std::vector<AggMap>& local_aggs,
        MergeFunc merge_func
    ) {
        for (const auto& local : local_aggs) {
            for (const auto& [key, agg] : local) {
                merge_func(global_agg_[key], agg);
            }
        }
    }

    /**
     * Default merge: assume AggState has operator+= defined
     */
    void merge_local_aggregations(const std::vector<AggMap>& local_aggs) {
        for (const auto& local : local_aggs) {
            for (const auto& [key, agg] : local) {
                merge_default(global_agg_[key], agg);
            }
        }
    }

    /**
     * Get sorted results
     *
     * @param compare Optional comparison function for sorting
     * @return Vector of (key, aggregate) pairs sorted by compare function
     */
    template<typename CompareFunc>
    std::vector<std::pair<GroupKey, AggState>> finalize(CompareFunc compare) const {
        std::vector<std::pair<GroupKey, AggState>> results(global_agg_.begin(), global_agg_.end());
        std::sort(results.begin(), results.end(), compare);
        return results;
    }

    /**
     * Get unsorted results
     */
    std::vector<std::pair<GroupKey, AggState>> finalize() const {
        return std::vector<std::pair<GroupKey, AggState>>(global_agg_.begin(), global_agg_.end());
    }

    /**
     * Get the global aggregation map (const)
     */
    const AggMap& get_aggregates() const {
        return global_agg_;
    }

    /**
     * Get the global aggregation map (mutable)
     */
    AggMap& get_aggregates() {
        return global_agg_;
    }

    /**
     * Clear all aggregates
     */
    void clear() {
        global_agg_.clear();
    }

    /**
     * Get number of distinct groups
     */
    size_t size() const {
        return global_agg_.size();
    }

private:
    AggMap global_agg_;

    /**
     * Default merge implementation for aggregate states
     * Assumes the AggState type supports merging via member access
     * This is a placeholder - actual merge logic is query-specific
     */
    static void merge_default(AggState& target, const AggState& source) {
        // For simple types, just assign
        // For complex types with multiple fields, caller should use custom merge
        target = source;
    }
};

/**
 * Helper struct for common aggregate patterns
 */

// Simple sum aggregate
struct SumAggregate {
    double value = 0.0;
    void add(double x) { value += x; }
};

// Count aggregate
struct CountAggregate {
    size_t count = 0;
    void increment() { count++; }
};

// Sum and count for AVG
struct AvgAggregate {
    double sum = 0.0;
    size_t count = 0;
    void add(double x) { sum += x; count++; }
    double avg() const { return count > 0 ? sum / count : 0.0; }
};

} // namespace operators
} // namespace gendb
