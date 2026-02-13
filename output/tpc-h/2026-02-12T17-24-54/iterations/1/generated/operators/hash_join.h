#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <thread>
#include <algorithm>

namespace gendb {

// Hash join: build hash table on left, probe with right
// Returns pairs of (left_row_id, right_row_id) for matches
template<typename KeyType>
class HashJoin {
public:
    // Build phase: create hash table from left side
    void build(const KeyType* left_keys, size_t left_count) {
        hash_table_.reserve(left_count);
        for (size_t i = 0; i < left_count; ++i) {
            hash_table_[left_keys[i]].push_back(i);
        }
    }

    // Build phase with pre-filtered row IDs
    void build(const KeyType* left_keys, const std::vector<size_t>& left_row_ids) {
        hash_table_.reserve(left_row_ids.size());
        for (size_t row_id : left_row_ids) {
            hash_table_[left_keys[row_id]].push_back(row_id);
        }
    }

    // Probe phase: match right side keys
    std::vector<std::pair<size_t, size_t>> probe(const KeyType* right_keys, size_t right_count) {
        std::vector<std::pair<size_t, size_t>> result;
        result.reserve(right_count);

        for (size_t r = 0; r < right_count; ++r) {
            auto it = hash_table_.find(right_keys[r]);
            if (it != hash_table_.end()) {
                for (size_t left_row : it->second) {
                    result.emplace_back(left_row, r);
                }
            }
        }

        return result;
    }

    // Probe with pre-filtered right row IDs
    std::vector<std::pair<size_t, size_t>> probe(const KeyType* right_keys,
                                                   const std::vector<size_t>& right_row_ids) {
        std::vector<std::pair<size_t, size_t>> result;
        result.reserve(right_row_ids.size());

        for (size_t r : right_row_ids) {
            auto it = hash_table_.find(right_keys[r]);
            if (it != hash_table_.end()) {
                for (size_t left_row : it->second) {
                    result.emplace_back(left_row, r);
                }
            }
        }

        return result;
    }

    // Parallel probe with pre-filtered right row IDs
    // Returns vector of match pairs, processing chunks in parallel
    std::vector<std::pair<size_t, size_t>> probe_parallel(const KeyType* right_keys,
                                                            const std::vector<size_t>& right_row_ids) {
        const size_t num_threads = std::thread::hardware_concurrency();
        const size_t chunk_size = (right_row_ids.size() + num_threads - 1) / num_threads;

        std::vector<std::thread> threads;
        std::vector<std::vector<std::pair<size_t, size_t>>> local_results(num_threads);

        // Parallel probe phase
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                size_t start_idx = t * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, right_row_ids.size());

                auto& local_result = local_results[t];
                local_result.reserve((end_idx - start_idx) * 2); // estimate

                for (size_t i = start_idx; i < end_idx; ++i) {
                    size_t r = right_row_ids[i];
                    auto it = hash_table_.find(right_keys[r]);
                    if (it != hash_table_.end()) {
                        for (size_t left_row : it->second) {
                            local_result.emplace_back(left_row, r);
                        }
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Merge results
        size_t total_size = 0;
        for (const auto& local : local_results) {
            total_size += local.size();
        }

        std::vector<std::pair<size_t, size_t>> result;
        result.reserve(total_size);

        for (auto& local : local_results) {
            result.insert(result.end(), local.begin(), local.end());
        }

        return result;
    }

    // Parallel probe with aggregation callback
    // Useful for fused probe+aggregate operations to avoid materializing match pairs
    template<typename Callback>
    void probe_parallel_with_callback(const KeyType* right_keys,
                                       const std::vector<size_t>& right_row_ids,
                                       Callback callback) {
        const size_t num_threads = std::thread::hardware_concurrency();
        const size_t chunk_size = (right_row_ids.size() + num_threads - 1) / num_threads;

        std::vector<std::thread> threads;

        // Parallel probe phase with callback
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                size_t start_idx = t * chunk_size;
                size_t end_idx = std::min(start_idx + chunk_size, right_row_ids.size());

                for (size_t i = start_idx; i < end_idx; ++i) {
                    size_t r = right_row_ids[i];
                    auto it = hash_table_.find(right_keys[r]);
                    if (it != hash_table_.end()) {
                        for (size_t left_row : it->second) {
                            callback(t, left_row, r);
                        }
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }
    }

private:
    std::unordered_map<KeyType, std::vector<size_t>> hash_table_;
};

} // namespace gendb
