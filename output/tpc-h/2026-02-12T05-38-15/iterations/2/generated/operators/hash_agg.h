#pragma once

#include <unordered_map>
#include <vector>
#include <thread>

namespace gendb {
namespace operators {

/**
 * Generic thread-local hash aggregation operator.
 *
 * Performs parallel aggregation using thread-local hash tables, then merges
 * results into a global hash table. Suitable for medium-to-high cardinality
 * GROUP BY operations (100-10M groups).
 *
 * Template Parameters:
 *   Key: Group key type (must have operator== and std::hash specialization)
 *   Value: Aggregate state type (e.g., struct with sum/count fields)
 *   Hash: Hash function for Key (default: std::hash<Key>)
 *
 * Usage:
 *   // Define key and aggregate types
 *   struct MyKey { int col1; char col2; bool operator==(const MyKey&) const; };
 *   struct MyAgg { double sum = 0; size_t count = 0; };
 *
 *   // Aggregate with parallel scan
 *   auto result = parallel_hash_aggregate<MyKey, MyAgg>(
 *       num_rows,
 *       [&](size_t tid, size_t start, size_t end, auto& local_map) {
 *           for (size_t i = start; i < end; ++i) {
 *               if (filter_predicate(i)) {
 *                   MyKey key = extract_key(i);
 *                   auto& agg = local_map[key];
 *                   agg.sum += values[i];
 *                   agg.count++;
 *               }
 *           }
 *       }
 *   );
 */
template<typename Key, typename Value, typename Hash = std::hash<Key>>
std::unordered_map<Key, Value, Hash> parallel_hash_aggregate(
    size_t num_rows,
    std::function<void(size_t, size_t, size_t, std::unordered_map<Key, Value, Hash>&)> agg_func
) {
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<Key, Value, Hash>> local_aggs(num_threads);

    // Phase 1: Thread-local aggregation
    std::vector<std::thread> threads;
    size_t rows_per_thread = num_rows / num_threads;

    for (unsigned int t = 0; t < num_threads; ++t) {
        size_t start = t * rows_per_thread;
        size_t end = (t == num_threads - 1) ? num_rows : (t + 1) * rows_per_thread;

        threads.emplace_back([&, t, start, end]() {
            agg_func(t, start, end, local_aggs[t]);
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Phase 2: Merge thread-local results into global map
    std::unordered_map<Key, Value, Hash> global_agg;

    for (const auto& local : local_aggs) {
        for (const auto& [key, value] : local) {
            // Note: This assumes Value supports operator+=
            // For custom merge logic, specialize this operator or pass merge function
            auto it = global_agg.find(key);
            if (it == global_agg.end()) {
                global_agg[key] = value;
            } else {
                merge_aggregate(it->second, value);
            }
        }
    }

    return global_agg;
}

/**
 * Default aggregate merge function (can be specialized for custom types).
 * This is a placeholder - users should provide merge logic specific to their aggregate.
 */
template<typename Value>
void merge_aggregate(Value& dest, const Value& src) {
    // For simple aggregates with numeric fields, this would sum them
    // Users should specialize this for their specific Value types
    // For now, we'll handle common cases in query code
    dest = dest + src;  // Requires Value to support operator+
}

/**
 * Sequential hash aggregation (for small datasets or debugging).
 */
template<typename Key, typename Value, typename Hash = std::hash<Key>>
std::unordered_map<Key, Value, Hash> sequential_hash_aggregate(
    size_t num_rows,
    std::function<void(size_t, std::unordered_map<Key, Value, Hash>&)> agg_func
) {
    std::unordered_map<Key, Value, Hash> result;

    for (size_t i = 0; i < num_rows; ++i) {
        agg_func(i, result);
    }

    return result;
}

} // namespace operators
} // namespace gendb
