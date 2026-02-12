#pragma once

#include <unordered_map>
#include <cstdint>
#include <utility>

namespace gendb {
namespace index {

// Simple hash index: key -> row_id
template<typename K>
using HashIndex = std::unordered_map<K, size_t>;

// Multi-value hash index: key -> vector of row_ids
template<typename K>
using MultiHashIndex = std::unordered_map<K, std::vector<size_t>>;

// Composite key for two-column indexes
template<typename K1, typename K2>
struct CompositeKey {
    K1 key1;
    K2 key2;

    bool operator==(const CompositeKey& other) const {
        return key1 == other.key1 && key2 == other.key2;
    }
};

// Hash functor for composite keys
template<typename K1, typename K2>
struct CompositeKeyHash {
    size_t operator()(const CompositeKey<K1, K2>& k) const {
        return std::hash<K1>()(k.key1) ^ (std::hash<K2>()(k.key2) << 1);
    }
};

// Zone map: min/max values per block for range pruning
template<typename T>
struct ZoneMap {
    std::vector<T> min_vals;
    std::vector<T> max_vals;
    size_t block_size;

    ZoneMap() : block_size(0) {}

    void build(const std::vector<T>& column, size_t blk_size) {
        block_size = blk_size;
        size_t num_blocks = (column.size() + block_size - 1) / block_size;
        min_vals.resize(num_blocks);
        max_vals.resize(num_blocks);

        for (size_t b = 0; b < num_blocks; ++b) {
            size_t start = b * block_size;
            size_t end = std::min(start + block_size, column.size());
            T min_val = column[start];
            T max_val = column[start];
            for (size_t i = start + 1; i < end; ++i) {
                if (column[i] < min_val) min_val = column[i];
                if (column[i] > max_val) max_val = column[i];
            }
            min_vals[b] = min_val;
            max_vals[b] = max_val;
        }
    }

    // Check if block might contain values in range [low, high]
    bool block_may_contain(size_t block_id, T low, T high) const {
        return !(max_vals[block_id] < low || min_vals[block_id] > high);
    }
};

} // namespace index
} // namespace gendb
