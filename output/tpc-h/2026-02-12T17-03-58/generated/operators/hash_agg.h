#ifndef HASH_AGG_H
#define HASH_AGG_H

#include <unordered_map>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

// Generic hash aggregation operator
// KeyType: type of group key (or tuple of keys)
// AggType: type of aggregate state (e.g., struct with sum, count, etc.)
// KeyFn: function to extract key from row
// UpdateFn: function to update aggregate state with row data
// MergeFn: function to merge two aggregate states (for parallel aggregation)

template<typename KeyType, typename AggType>
class HashAgg {
private:
    std::unordered_map<KeyType, AggType> agg_map;

public:
    void insert_or_update(const KeyType& key, const AggType& value,
                         std::function<void(AggType&, const AggType&)> merge_fn) {
        auto it = agg_map.find(key);
        if (it != agg_map.end()) {
            merge_fn(it->second, value);
        } else {
            agg_map[key] = value;
        }
    }

    void update(const KeyType& key, std::function<void(AggType&)> update_fn) {
        auto it = agg_map.find(key);
        if (it != agg_map.end()) {
            update_fn(it->second);
        } else {
            AggType new_agg{};
            update_fn(new_agg);
            agg_map[key] = new_agg;
        }
    }

    // Get results as vector
    std::vector<std::pair<KeyType, AggType>> get_results() const {
        std::vector<std::pair<KeyType, AggType>> results;
        results.reserve(agg_map.size());
        for (const auto& [key, value] : agg_map) {
            results.emplace_back(key, value);
        }
        return results;
    }

    // Sort results by key or value
    template<typename CompareFn>
    std::vector<std::pair<KeyType, AggType>> get_sorted_results(CompareFn compare_fn) const {
        auto results = get_results();
        std::sort(results.begin(), results.end(), compare_fn);
        return results;
    }

    // Get top-K results (useful for LIMIT queries)
    template<typename CompareFn>
    std::vector<std::pair<KeyType, AggType>> get_top_k(size_t k, CompareFn compare_fn) const {
        auto results = get_results();

        if (results.size() <= k) {
            std::sort(results.begin(), results.end(), compare_fn);
            return results;
        }

        // Partial sort: only sort the top k elements
        std::partial_sort(results.begin(), results.begin() + k, results.end(), compare_fn);
        results.resize(k);
        return results;
    }

    size_t size() const {
        return agg_map.size();
    }

    void clear() {
        agg_map.clear();
    }

    // Merge another hash table into this one
    void merge(const HashAgg<KeyType, AggType>& other,
               std::function<void(AggType&, const AggType&)> merge_fn) {
        for (const auto& [key, value] : other.agg_map) {
            insert_or_update(key, value, merge_fn);
        }
    }
};

// Parallel aggregation helper
// Splits input rows across threads, each builds partial hash table, then merges
template<typename KeyType, typename AggType, typename KeyFn, typename UpdateFn, typename MergeFn>
std::vector<std::pair<KeyType, AggType>> parallel_hash_agg(
    uint64_t row_count, KeyFn key_fn, UpdateFn update_fn, MergeFn merge_fn,
    uint32_t num_threads = 0) {

    if (num_threads == 0) {
        num_threads = std::min<uint32_t>(std::thread::hardware_concurrency(), 16);
    }

    if (row_count < 100000 || num_threads == 1) {
        // Single-threaded aggregation
        HashAgg<KeyType, AggType> agg;
        for (uint64_t row = 0; row < row_count; ++row) {
            KeyType key = key_fn(row);
            agg.update(key, [&](AggType& state) { update_fn(state, row); });
        }
        return agg.get_results();
    }

    // Parallel aggregation
    std::vector<HashAgg<KeyType, AggType>> partial_aggs(num_threads);
    std::vector<std::thread> threads;
    uint64_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (uint32_t tid = 0; tid < num_threads; ++tid) {
        uint64_t start = tid * chunk_size;
        uint64_t end = std::min(start + chunk_size, row_count);

        threads.emplace_back([&, tid, start, end]() {
            auto& agg = partial_aggs[tid];
            for (uint64_t row = start; row < end; ++row) {
                KeyType key = key_fn(row);
                agg.update(key, [&](AggType& state) { update_fn(state, row); });
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge partial aggregates
    HashAgg<KeyType, AggType> final_agg;
    for (const auto& partial : partial_aggs) {
        final_agg.merge(partial, merge_fn);
    }

    return final_agg.get_results();
}

#endif // HASH_AGG_H
