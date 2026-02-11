#pragma once

#include <unordered_map>
#include <vector>
#include <thread>
#include <functional>

namespace gendb {
namespace operators {

// Generic hash join operator
// Implements build-probe hash join with parallel probe phase
// Template parameters:
//   KeyType: type of the join key
//   BuildPayloadType: additional data stored from build side
template<typename KeyType, typename BuildPayloadType>
class HashJoin {
public:
    // Hash table structure: join key -> vector of (row_index, payload)
    struct BuildEntry {
        size_t row_idx;
        BuildPayloadType payload;
    };

    using HashTable = std::unordered_map<KeyType, std::vector<BuildEntry>>;

    // Constructor
    HashJoin() = default;

    // Build phase: construct hash table from build side
    // build_size: number of rows in build table
    // extract_key_fn: function that extracts join key from build row
    // extract_payload_fn: function that extracts payload from build row
    template<typename ExtractKeyFn, typename ExtractPayloadFn>
    void build(size_t build_size, ExtractKeyFn extract_key_fn, ExtractPayloadFn extract_payload_fn) {
        hash_table_.clear();
        hash_table_.reserve(build_size / 0.75); // Pre-size for 75% load factor

        for (size_t i = 0; i < build_size; ++i) {
            KeyType key = extract_key_fn(i);
            BuildPayloadType payload = extract_payload_fn(i);
            hash_table_[key].push_back({i, payload});
        }
    }

    // Build phase with predicate filter
    template<typename ExtractKeyFn, typename ExtractPayloadFn, typename PredicateFn>
    void build_filtered(size_t build_size, ExtractKeyFn extract_key_fn,
                       ExtractPayloadFn extract_payload_fn, PredicateFn predicate) {
        hash_table_.clear();
        hash_table_.reserve(build_size / 4); // Estimate 25% selectivity

        for (size_t i = 0; i < build_size; ++i) {
            if (!predicate(i)) continue;

            KeyType key = extract_key_fn(i);
            BuildPayloadType payload = extract_payload_fn(i);
            hash_table_[key].push_back({i, payload});
        }
    }

    // Probe phase: join probe side with hash table
    // Returns vector of (probe_row_idx, build_row_idx, build_payload) tuples
    template<typename ExtractKeyFn>
    std::vector<std::tuple<size_t, size_t, BuildPayloadType>>
    probe(size_t probe_size, ExtractKeyFn extract_key_fn) {
        std::vector<std::tuple<size_t, size_t, BuildPayloadType>> results;
        results.reserve(probe_size); // Estimate 1:1 join cardinality

        for (size_t i = 0; i < probe_size; ++i) {
            KeyType key = extract_key_fn(i);
            auto it = hash_table_.find(key);
            if (it != hash_table_.end()) {
                for (const auto& entry : it->second) {
                    results.emplace_back(i, entry.row_idx, entry.payload);
                }
            }
        }

        return results;
    }

    // Probe phase with predicate filter on probe side
    template<typename ExtractKeyFn, typename PredicateFn>
    std::vector<std::tuple<size_t, size_t, BuildPayloadType>>
    probe_filtered(size_t probe_size, ExtractKeyFn extract_key_fn, PredicateFn predicate) {
        std::vector<std::tuple<size_t, size_t, BuildPayloadType>> results;
        results.reserve(probe_size / 4); // Estimate 25% selectivity

        for (size_t i = 0; i < probe_size; ++i) {
            if (!predicate(i)) continue;

            KeyType key = extract_key_fn(i);
            auto it = hash_table_.find(key);
            if (it != hash_table_.end()) {
                for (const auto& entry : it->second) {
                    results.emplace_back(i, entry.row_idx, entry.payload);
                }
            }
        }

        return results;
    }

    // Parallel probe phase: join probe side with hash table using multiple threads
    template<typename ExtractKeyFn>
    std::vector<std::tuple<size_t, size_t, BuildPayloadType>>
    probe_parallel(size_t probe_size, ExtractKeyFn extract_key_fn) {
        const size_t num_threads = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        std::vector<std::vector<std::tuple<size_t, size_t, BuildPayloadType>>> local_results(num_threads);

        size_t chunk_size = (probe_size + num_threads - 1) / num_threads;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                size_t start_idx = tid * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, probe_size);

                auto& local_result = local_results[tid];
                local_result.reserve((end_idx - start_idx)); // Estimate 1:1 join

                for (size_t i = start_idx; i < end_idx; ++i) {
                    KeyType key = extract_key_fn(i);
                    auto it = hash_table_.find(key);
                    if (it != hash_table_.end()) {
                        for (const auto& entry : it->second) {
                            local_result.emplace_back(i, entry.row_idx, entry.payload);
                        }
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Merge results
        size_t total_size = 0;
        for (const auto& local : local_results) {
            total_size += local.size();
        }

        std::vector<std::tuple<size_t, size_t, BuildPayloadType>> results;
        results.reserve(total_size);
        for (const auto& local : local_results) {
            results.insert(results.end(), local.begin(), local.end());
        }

        return results;
    }

    // Parallel probe with predicate filter
    template<typename ExtractKeyFn, typename PredicateFn>
    std::vector<std::tuple<size_t, size_t, BuildPayloadType>>
    probe_parallel_filtered(size_t probe_size, ExtractKeyFn extract_key_fn, PredicateFn predicate) {
        const size_t num_threads = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        std::vector<std::vector<std::tuple<size_t, size_t, BuildPayloadType>>> local_results(num_threads);

        size_t chunk_size = (probe_size + num_threads - 1) / num_threads;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                size_t start_idx = tid * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, probe_size);

                auto& local_result = local_results[tid];
                local_result.reserve((end_idx - start_idx) / 4); // Estimate 25% selectivity

                for (size_t i = start_idx; i < end_idx; ++i) {
                    if (!predicate(i)) continue;

                    KeyType key = extract_key_fn(i);
                    auto it = hash_table_.find(key);
                    if (it != hash_table_.end()) {
                        for (const auto& entry : it->second) {
                            local_result.emplace_back(i, entry.row_idx, entry.payload);
                        }
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Merge results
        size_t total_size = 0;
        for (const auto& local : local_results) {
            total_size += local.size();
        }

        std::vector<std::tuple<size_t, size_t, BuildPayloadType>> results;
        results.reserve(total_size);
        for (const auto& local : local_results) {
            results.insert(results.end(), local.begin(), local.end());
        }

        return results;
    }

    // Get the hash table (for debugging or advanced usage)
    const HashTable& get_hash_table() const { return hash_table_; }

private:
    HashTable hash_table_;
};

}  // namespace operators
}  // namespace gendb
