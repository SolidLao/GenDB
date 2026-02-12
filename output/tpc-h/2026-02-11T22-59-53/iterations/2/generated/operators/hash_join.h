#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include <functional>

namespace gendb {
namespace operators {

// Generic hash join operator
// BuildKeyFunc: (size_t row_idx) -> KeyType
// BuildValueFunc: (size_t row_idx) -> ValueType
// ProbeKeyFunc: (size_t row_idx) -> KeyType
// JoinFunc: (size_t probe_idx, const ValueType& build_value) -> void
template<typename KeyType, typename ValueType,
         typename BuildKeyFunc, typename BuildValueFunc,
         typename ProbeKeyFunc, typename JoinFunc>
void hash_join(
    size_t build_row_count,
    BuildKeyFunc&& build_key,
    BuildValueFunc&& build_value,
    size_t probe_row_count,
    ProbeKeyFunc&& probe_key,
    JoinFunc&& join_func,
    int num_threads = 0)
{
    // Build phase: create hash table on build side
    std::unordered_map<KeyType, ValueType> hash_table;
    hash_table.reserve(build_row_count);

    for (size_t i = 0; i < build_row_count; i++) {
        hash_table[build_key(i)] = build_value(i);
    }

    // Probe phase: parallel probe with larger relation
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    std::vector<std::thread> threads;
    size_t chunk_size = (probe_row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, probe_row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                auto it = hash_table.find(probe_key(i));
                if (it != hash_table.end()) {
                    join_func(i, it->second);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

// Semi-join variant: returns set of matching keys (for filtering)
template<typename KeyType, typename BuildKeyFunc, typename ProbeKeyFunc>
std::unordered_set<KeyType> hash_semi_join(
    size_t build_row_count,
    BuildKeyFunc&& build_key,
    size_t probe_row_count,
    ProbeKeyFunc&& probe_key)
{
    // Build phase: create hash set
    std::unordered_set<KeyType> hash_set;
    hash_set.reserve(build_row_count);

    for (size_t i = 0; i < build_row_count; i++) {
        hash_set.insert(build_key(i));
    }

    return hash_set;
}

// Hash build: creates hash table (for multi-stage joins like Q3)
template<typename KeyType, typename ValueType,
         typename BuildKeyFunc, typename BuildValueFunc>
std::unordered_map<KeyType, ValueType> hash_build(
    size_t build_row_count,
    BuildKeyFunc&& build_key,
    BuildValueFunc&& build_value)
{
    std::unordered_map<KeyType, ValueType> hash_table;
    hash_table.reserve(build_row_count);

    for (size_t i = 0; i < build_row_count; i++) {
        hash_table[build_key(i)] = build_value(i);
    }

    return hash_table;
}

// Hash probe with filtering: parallel probe on pre-built hash table
template<typename KeyType, typename ValueType,
         typename PredicateFunc, typename ProbeKeyFunc, typename JoinFunc>
void hash_probe(
    const std::unordered_map<KeyType, ValueType>& hash_table,
    size_t probe_row_count,
    PredicateFunc&& predicate,
    ProbeKeyFunc&& probe_key,
    JoinFunc&& join_func,
    int num_threads = 0)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }

    std::vector<std::thread> threads;
    size_t chunk_size = (probe_row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, probe_row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (predicate(i)) {
                    auto it = hash_table.find(probe_key(i));
                    if (it != hash_table.end()) {
                        join_func(i, it->second);
                    }
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

} // namespace operators
} // namespace gendb
