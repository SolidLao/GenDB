#pragma once

#include "robin_hood_map.h"
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>

namespace gendb {

// Hash join operator: build hash table on smaller side, probe with larger
// Supports both 1:N joins (build side can have duplicates) and 1:1 joins

template<typename KeyType, typename BuildValue, typename ProbeValue>
struct HashJoinResult {
    std::vector<KeyType> keys;
    std::vector<BuildValue> build_values;
    std::vector<ProbeValue> probe_values;

    void reserve(size_t n) {
        keys.reserve(n);
        build_values.reserve(n);
        probe_values.reserve(n);
    }

    size_t size() const { return keys.size(); }
};

// Hash join operator class for cleaner interface
template<typename KeyType, typename BuildValue, typename ProbeValue, typename KeyHash = std::hash<KeyType>>
class HashJoin {
public:
    using BuildTable = RobinHoodMap<KeyType, std::vector<BuildValue>, KeyHash>;
    using Result = HashJoinResult<KeyType, BuildValue, ProbeValue>;

    HashJoin() = default;

    // Build phase: create hash table from build-side keys/values
    // Supports multiple values per key (1:N join)
    void build(const std::vector<KeyType>& keys, const std::vector<BuildValue>& values) {
        build_table_.clear();
        build_table_.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            build_table_[keys[i]].push_back(values[i]);
        }
    }

    // Build phase with predicate filter (filter build side before inserting)
    template<typename FilterFunc>
    void build_filtered(const std::vector<KeyType>& keys,
                       const std::vector<BuildValue>& values,
                       FilterFunc&& filter) {
        build_table_.clear();
        build_table_.reserve(keys.size() / 2); // Estimate after filtering

        for (size_t i = 0; i < keys.size(); ++i) {
            if (filter(i)) {
                build_table_[keys[i]].push_back(values[i]);
            }
        }
    }

    // Probe phase: probe hash table with probe-side keys
    Result probe(const std::vector<KeyType>& probe_keys,
                 const std::vector<ProbeValue>& probe_values) const {
        Result result;
        result.reserve(probe_keys.size()); // Initial estimate

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            auto* values_ptr = build_table_.find(probe_keys[i]);
            if (values_ptr != nullptr) {
                // Found matching key(s) in build table
                for (const auto& build_value : *values_ptr) {
                    result.keys.push_back(probe_keys[i]);
                    result.build_values.push_back(build_value);
                    result.probe_values.push_back(probe_values[i]);
                }
            }
        }

        return result;
    }

    // Probe with predicate filter (filter probe side before probing)
    template<typename FilterFunc>
    Result probe_filtered(const std::vector<KeyType>& probe_keys,
                         const std::vector<ProbeValue>& probe_values,
                         FilterFunc&& filter) const {
        Result result;
        result.reserve(probe_keys.size() / 2); // Estimate after filtering

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            if (!filter(i)) continue;

            auto* values_ptr = build_table_.find(probe_keys[i]);
            if (values_ptr != nullptr) {
                for (const auto& build_value : *values_ptr) {
                    result.keys.push_back(probe_keys[i]);
                    result.build_values.push_back(build_value);
                    result.probe_values.push_back(probe_values[i]);
                }
            }
        }

        return result;
    }

    // Get build table size (useful for statistics)
    size_t build_size() const { return build_table_.size(); }

private:
    BuildTable build_table_;
};

// Specialized hash join for unique build keys (1:1 join, PK-FK)
// More efficient than general case - stores single value per key
template<typename KeyType, typename BuildValue, typename ProbeValue, typename KeyHash = std::hash<KeyType>>
class UniqueHashJoin {
public:
    using BuildTable = RobinHoodMap<KeyType, BuildValue, KeyHash>;
    using Result = HashJoinResult<KeyType, BuildValue, ProbeValue>;

    UniqueHashJoin() = default;

    // Build phase for unique keys
    void build(const std::vector<KeyType>& keys, const std::vector<BuildValue>& values) {
        build_table_.clear();
        build_table_.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            build_table_[keys[i]] = values[i];
        }
    }

    // Build with filter
    template<typename FilterFunc>
    void build_filtered(const std::vector<KeyType>& keys,
                       const std::vector<BuildValue>& values,
                       FilterFunc&& filter) {
        build_table_.clear();
        build_table_.reserve(keys.size() / 2);

        for (size_t i = 0; i < keys.size(); ++i) {
            if (filter(i)) {
                build_table_[keys[i]] = values[i];
            }
        }
    }

    // Probe phase
    Result probe(const std::vector<KeyType>& probe_keys,
                 const std::vector<ProbeValue>& probe_values) const {
        Result result;
        result.reserve(probe_keys.size());

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            auto* value_ptr = build_table_.find(probe_keys[i]);
            if (value_ptr != nullptr) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(*value_ptr);
                result.probe_values.push_back(probe_values[i]);
            }
        }

        return result;
    }

    // Probe with filter
    template<typename FilterFunc>
    Result probe_filtered(const std::vector<KeyType>& probe_keys,
                         const std::vector<ProbeValue>& probe_values,
                         FilterFunc&& filter) const {
        Result result;
        result.reserve(probe_keys.size() / 2);

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            if (!filter(i)) continue;

            auto* value_ptr = build_table_.find(probe_keys[i]);
            if (value_ptr != nullptr) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(*value_ptr);
                result.probe_values.push_back(probe_values[i]);
            }
        }

        return result;
    }

    // Parallel probe with filter
    template<typename FilterFunc>
    Result probe_filtered_parallel(const std::vector<KeyType>& probe_keys,
                                   const std::vector<ProbeValue>& probe_values,
                                   FilterFunc&& filter,
                                   size_t num_threads = std::thread::hardware_concurrency()) const {
        if (probe_keys.empty() || num_threads == 0) {
            return probe_filtered(probe_keys, probe_values, filter);
        }

        // Ensure we don't use more threads than data
        const size_t n = probe_keys.size();
        num_threads = std::min(num_threads, n);
        
        if (num_threads == 1) {
            return probe_filtered(probe_keys, probe_values, filter);
        }

        // Thread-local results
        std::vector<Result> thread_results(num_threads);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        // Calculate chunk size
        const size_t chunk_size = (n + num_threads - 1) / num_threads;

        // Launch threads
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([this, &probe_keys, &probe_values, &filter, &thread_results, t, chunk_size, n]() {
                const size_t start = t * chunk_size;
                const size_t end = std::min(start + chunk_size, n);
                
                auto& local_result = thread_results[t];
                local_result.reserve((end - start) / 2); // Estimate

                for (size_t i = start; i < end; ++i) {
                    if (!filter(i)) continue;

                    auto* value_ptr = build_table_.find(probe_keys[i]);
                    if (value_ptr != nullptr) {
                        local_result.keys.push_back(probe_keys[i]);
                        local_result.build_values.push_back(*value_ptr);
                        local_result.probe_values.push_back(probe_values[i]);
                    }
                }
            });
        }

        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }

        // Merge results
        Result final_result;
        size_t total_size = 0;
        for (const auto& r : thread_results) {
            total_size += r.size();
        }
        final_result.reserve(total_size);

        for (auto& r : thread_results) {
            final_result.keys.insert(final_result.keys.end(), r.keys.begin(), r.keys.end());
            final_result.build_values.insert(final_result.build_values.end(), r.build_values.begin(), r.build_values.end());
            final_result.probe_values.insert(final_result.probe_values.end(), r.probe_values.begin(), r.probe_values.end());
        }

        return final_result;
    }

    size_t build_size() const { return build_table_.size(); }

    // Direct key lookup for optimized parallel probing
    const BuildValue* find(const KeyType& key) const {
        return build_table_.find(key);
    }

private:
    BuildTable build_table_;
};

// Legacy functional interface (for backward compatibility)
template<typename KeyType, typename ValueType>
RobinHoodMap<KeyType, std::vector<ValueType>> hash_join_build(
    const std::vector<KeyType>& keys,
    const std::vector<ValueType>& values) {

    RobinHoodMap<KeyType, std::vector<ValueType>> hash_table(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        hash_table[keys[i]].push_back(values[i]);
    }

    return hash_table;
}

template<typename KeyType, typename BuildValue, typename ProbeValue>
HashJoinResult<KeyType, BuildValue, ProbeValue> hash_join_probe(
    const RobinHoodMap<KeyType, std::vector<BuildValue>>& hash_table,
    const std::vector<KeyType>& probe_keys,
    const std::vector<ProbeValue>& probe_values) {

    HashJoinResult<KeyType, BuildValue, ProbeValue> result;
    result.reserve(probe_keys.size());

    for (size_t i = 0; i < probe_keys.size(); ++i) {
        auto* values_ptr = hash_table.find(probe_keys[i]);
        if (values_ptr != nullptr) {
            for (const auto& build_value : *values_ptr) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(build_value);
                result.probe_values.push_back(probe_values[i]);
            }
        }
    }

    return result;
}

} // namespace gendb
