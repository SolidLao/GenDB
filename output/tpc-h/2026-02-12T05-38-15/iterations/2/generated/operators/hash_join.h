#pragma once

#include <unordered_map>
#include <vector>
#include <functional>
#include <cstdint>
#include <thread>

namespace gendb {
namespace operators {

/**
 * Generic hash join operator implementing the classic build-probe algorithm.
 *
 * Phase 1 (Build): Scans the smaller relation (build side) and constructs a
 *                  hash table mapping join keys to payloads.
 * Phase 2 (Probe): Scans the larger relation (probe side) and looks up each
 *                  key in the hash table, emitting matching pairs.
 *
 * Template Parameters:
 *   BuildKey: Type of the join key from build side
 *   BuildPayload: Type of additional data to store in hash table
 *   ProbeKey: Type of the join key from probe side (usually same as BuildKey)
 *   Hash: Hash function for BuildKey
 *
 * Performance Notes:
 * - Always choose smaller relation as build side (check cardinality first)
 * - Pre-size hash table to ~75% load factor to avoid rehashing
 * - For large joins, consider partitioned hash join (not implemented here)
 *
 * Usage:
 *   // Build phase: create hash table from filtered customer
 *   auto ht = hash_join_build<int32_t, CustomerData>(
 *       customer_size,
 *       [&](size_t i) -> bool { return c_mktsegment[i] == "BUILDING"; },
 *       [&](size_t i) -> int32_t { return c_custkey[i]; },
 *       [&](size_t i) -> CustomerData { return {c_name[i], c_phone[i]}; }
 *   );
 *
 *   // Probe phase: join orders with customer hash table
 *   auto results = hash_join_probe(
 *       orders_size,
 *       ht,
 *       [&](size_t i) -> int32_t { return o_custkey[i]; },
 *       [&](size_t i, const CustomerData& cust) {
 *           // Emit join result
 *           output.push_back({o_orderkey[i], cust.name});
 *       }
 *   );
 */

/**
 * Hash join build phase: construct hash table from build side.
 *
 * @param num_rows Number of rows in build side relation
 * @param filter_func Predicate function returning true if row passes filter
 * @param key_func Extract join key from row index
 * @param payload_func Extract payload data from row index
 * @return Hash table mapping keys to payloads
 */
template<typename BuildKey, typename BuildPayload, typename FilterFunc, typename KeyFunc, typename PayloadFunc, typename Hash = std::hash<BuildKey>>
std::unordered_map<BuildKey, BuildPayload, Hash> hash_join_build(
    size_t num_rows,
    FilterFunc&& filter_func,
    KeyFunc&& key_func,
    PayloadFunc&& payload_func
) {
    // Pre-size hash table (estimate: assume ~20% selectivity after filter)
    size_t estimated_size = num_rows / 5;
    std::unordered_map<BuildKey, BuildPayload, Hash> hash_table;
    hash_table.reserve(estimated_size);

    // Scan build side and populate hash table
    for (size_t i = 0; i < num_rows; ++i) {
        if (filter_func(i)) {
            BuildKey key = key_func(i);
            BuildPayload payload = payload_func(i);
            hash_table[key] = payload;
        }
    }

    return hash_table;
}

/**
 * Hash join build phase with multiple values per key (one-to-many join).
 *
 * For joins where build side may have duplicate keys, this variant stores
 * a vector of payloads per key.
 */
template<typename BuildKey, typename BuildPayload, typename FilterFunc, typename KeyFunc, typename PayloadFunc, typename Hash = std::hash<BuildKey>>
std::unordered_map<BuildKey, std::vector<BuildPayload>, Hash> hash_join_build_multi(
    size_t num_rows,
    FilterFunc&& filter_func,
    KeyFunc&& key_func,
    PayloadFunc&& payload_func
) {
    std::unordered_map<BuildKey, std::vector<BuildPayload>, Hash> hash_table;

    for (size_t i = 0; i < num_rows; ++i) {
        if (filter_func(i)) {
            BuildKey key = key_func(i);
            BuildPayload payload = payload_func(i);
            hash_table[key].push_back(payload);
        }
    }

    return hash_table;
}

/**
 * Hash join probe phase: scan probe side and look up in hash table.
 *
 * @param num_rows Number of rows in probe side relation
 * @param hash_table Hash table built from build side
 * @param key_func Extract join key from probe row index
 * @param emit_func Callback invoked for each matching pair (row_idx, build_payload)
 */
template<typename ProbeKey, typename BuildPayload, typename KeyFunc, typename EmitFunc, typename Hash = std::hash<ProbeKey>>
void hash_join_probe(
    size_t num_rows,
    const std::unordered_map<ProbeKey, BuildPayload, Hash>& hash_table,
    KeyFunc&& key_func,
    EmitFunc&& emit_func
) {
    for (size_t i = 0; i < num_rows; ++i) {
        ProbeKey key = key_func(i);
        auto it = hash_table.find(key);
        if (it != hash_table.end()) {
            emit_func(i, it->second);
        }
    }
}

/**
 * Hash join probe phase with predicate (filter on probe side).
 *
 * Applies filter predicate before probing hash table.
 */
template<typename ProbeKey, typename BuildPayload, typename FilterFunc, typename KeyFunc, typename EmitFunc, typename Hash = std::hash<ProbeKey>>
void hash_join_probe_filtered(
    size_t num_rows,
    const std::unordered_map<ProbeKey, BuildPayload, Hash>& hash_table,
    FilterFunc&& filter_func,
    KeyFunc&& key_func,
    EmitFunc&& emit_func
) {
    for (size_t i = 0; i < num_rows; ++i) {
        if (filter_func(i)) {
            ProbeKey key = key_func(i);
            auto it = hash_table.find(key);
            if (it != hash_table.end()) {
                emit_func(i, it->second);
            }
        }
    }
}

/**
 * Parallel hash join probe phase with predicate (filter on probe side).
 *
 * Parallelizes the probe phase across multiple threads. Each thread processes
 * a chunk of the probe relation and calls emit_func for matches.
 *
 * NOTE: emit_func must be thread-safe or aggregate into thread-local storage.
 */
template<typename ProbeKey, typename BuildPayload, typename FilterFunc, typename KeyFunc, typename EmitFunc, typename Hash = std::hash<ProbeKey>>
void hash_join_probe_filtered_parallel(
    size_t num_rows,
    const std::unordered_map<ProbeKey, BuildPayload, Hash>& hash_table,
    FilterFunc&& filter_func,
    KeyFunc&& key_func,
    EmitFunc&& emit_func
) {
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    size_t rows_per_thread = num_rows / num_threads;

    for (unsigned int t = 0; t < num_threads; ++t) {
        size_t start = t * rows_per_thread;
        size_t end = (t == num_threads - 1) ? num_rows : (t + 1) * rows_per_thread;

        threads.emplace_back([&, t, start, end]() {
            for (size_t i = start; i < end; ++i) {
                if (filter_func(i)) {
                    ProbeKey key = key_func(i);
                    auto it = hash_table.find(key);
                    if (it != hash_table.end()) {
                        emit_func(t, i, it->second);
                    }
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

/**
 * Hash join probe phase for one-to-many joins.
 *
 * Handles case where build side hash table maps keys to vectors of payloads.
 */
template<typename ProbeKey, typename BuildPayload, typename KeyFunc, typename EmitFunc, typename Hash = std::hash<ProbeKey>>
void hash_join_probe_multi(
    size_t num_rows,
    const std::unordered_map<ProbeKey, std::vector<BuildPayload>, Hash>& hash_table,
    KeyFunc&& key_func,
    EmitFunc&& emit_func
) {
    for (size_t i = 0; i < num_rows; ++i) {
        ProbeKey key = key_func(i);
        auto it = hash_table.find(key);
        if (it != hash_table.end()) {
            // Emit result for each matching build row
            for (const auto& payload : it->second) {
                emit_func(i, payload);
            }
        }
    }
}

} // namespace operators
} // namespace gendb
