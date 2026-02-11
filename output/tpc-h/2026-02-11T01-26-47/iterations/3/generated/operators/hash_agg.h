#pragma once

#include "robin_hood_map.h"
#include <unordered_map>
#include <vector>
#include <thread>
#include <atomic>
#include <functional>

namespace gendb {

// Parallel hash aggregation with morsel-driven execution
// Designed for queries like Q1 (group by with aggregation) and Q3 (aggregate after join)
template<typename KeyType, typename AggState, typename KeyHash = std::hash<KeyType>>
class ParallelHashAggregation {
public:
    using AggTable = RobinHoodMap<KeyType, AggState, KeyHash>;

    ParallelHashAggregation(size_t estimated_groups = 1000, size_t num_threads = 0)
        : num_threads_(num_threads ? num_threads : std::thread::hardware_concurrency()) {
        local_tables_.resize(num_threads_);
        // Pre-size thread-local tables to reduce rehashing
        for (auto& table : local_tables_) {
            table.reserve(estimated_groups / num_threads_ + 100);
        }
    }

    // Execute parallel aggregation over a table (without merging)
    // ProcessFunc signature: void(AggTable& table, size_t row_index)
    // Call merge() or merge_with() after to get final result
    template<typename ProcessFunc>
    void execute(size_t row_count, ProcessFunc&& process_row, size_t morsel_size = 10000) {
        const size_t total_morsels = (row_count + morsel_size - 1) / morsel_size;
        std::atomic<size_t> next_morsel{0};

        auto worker = [&](size_t thread_id) {
            auto& local_table = local_tables_[thread_id];

            while (true) {
                size_t morsel_idx = next_morsel.fetch_add(1);
                if (morsel_idx >= total_morsels) break;

                size_t start = morsel_idx * morsel_size;
                size_t end = std::min(start + morsel_size, row_count);

                for (size_t i = start; i < end; ++i) {
                    process_row(local_table, i);
                }
            }
        };

        // Launch worker threads
        std::vector<std::thread> threads;
        threads.reserve(num_threads_);
        for (size_t i = 0; i < num_threads_; ++i) {
            threads.emplace_back(worker, i);
        }

        // Wait for completion
        for (auto& t : threads) {
            t.join();
        }
    }

    // Execute and merge in one call (convenience method)
    template<typename ProcessFunc>
    AggTable execute_and_merge(size_t row_count, ProcessFunc&& process_row, size_t morsel_size = 10000) {
        execute(row_count, std::forward<ProcessFunc>(process_row), morsel_size);
        return merge();
    }

    // Merge all thread-local tables into global result
    // Uses custom merge function for AggState
    template<typename MergeFunc>
    AggTable merge_with(MergeFunc&& merge_fn) {
        AggTable result;

        // Estimate total groups for pre-sizing
        size_t total_groups = 0;
        for (const auto& local : local_tables_) {
            total_groups += local.size();
        }
        result.reserve(total_groups);

        for (auto& local : local_tables_) {
            for (auto entry : local) {
                const auto& key = entry.first;
                auto& state = entry.second;
                auto* existing = result.find(key);
                if (existing != nullptr) {
                    merge_fn(*existing, state);
                } else {
                    result[key] = state;
                }
            }
        }

        return result;
    }

    // Default merge (assumes AggState has merging support)
    AggTable merge() {
        return merge_with([](AggState& dest, const AggState& src) {
            merge_default(dest, src);
        });
    }

private:
    size_t num_threads_;
    std::vector<AggTable> local_tables_;

    // Default merge for aggregate states - accumulate values
    template<typename T>
    static void merge_default(T& dest, const T& src);
};

// Specializations for common aggregate state types
// For custom AggState types, either:
// 1. Define operator+= on the AggState type, OR
// 2. Call merge_with() with a custom merge function

// Simple sequential hash aggregation (for small data or low-cardinality groups)
template<typename KeyType, typename AggState, typename KeyHash = std::hash<KeyType>>
class HashAggregation {
public:
    using AggTable = RobinHoodMap<KeyType, AggState, KeyHash>;

    HashAggregation(size_t estimated_groups = 1000) {
        table_.reserve(estimated_groups);
    }

    // Process a single row
    template<typename ProcessFunc>
    void process(size_t row_index, ProcessFunc&& process_row) {
        process_row(table_, row_index);
    }

    // Execute aggregation over all rows
    template<typename ProcessFunc>
    AggTable execute(size_t row_count, ProcessFunc&& process_row) {
        for (size_t i = 0; i < row_count; ++i) {
            process_row(table_, i);
        }
        return std::move(table_);
    }

    const AggTable& get_table() const { return table_; }
    AggTable& get_table() { return table_; }

private:
    AggTable table_;
};

} // namespace gendb
