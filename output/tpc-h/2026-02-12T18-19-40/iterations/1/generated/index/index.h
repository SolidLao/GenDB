#ifndef INDEX_H
#define INDEX_H

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <algorithm>

// Index structures for primary key lookups
// Hash-based indexes for O(1) lookup

namespace gendb_index {

// Simple hash index: key -> row_id
template<typename KeyType>
class HashIndex {
public:
    void insert(KeyType key, size_t row_id) {
        index_[key] = row_id;
    }

    bool lookup(KeyType key, size_t& row_id) const {
        auto it = index_.find(key);
        if (it != index_.end()) {
            row_id = it->second;
            return true;
        }
        return false;
    }

    size_t size() const { return index_.size(); }

private:
    std::unordered_map<KeyType, size_t> index_;
};

// Composite key hash index (for lineitem, partsupp)
template<typename K1, typename K2>
struct CompositeKey {
    K1 key1;
    K2 key2;

    bool operator==(const CompositeKey& other) const {
        return key1 == other.key1 && key2 == other.key2;
    }
};

template<typename K1, typename K2>
struct CompositeKeyHash {
    size_t operator()(const CompositeKey<K1, K2>& k) const {
        return std::hash<K1>()(k.key1) ^ (std::hash<K2>()(k.key2) << 1);
    }
};

template<typename K1, typename K2>
class CompositeHashIndex {
public:
    void insert(K1 key1, K2 key2, size_t row_id) {
        index_[{key1, key2}] = row_id;
    }

    bool lookup(K1 key1, K2 key2, size_t& row_id) const {
        auto it = index_.find({key1, key2});
        if (it != index_.end()) {
            row_id = it->second;
            return true;
        }
        return false;
    }

    size_t size() const { return index_.size(); }

private:
    std::unordered_map<CompositeKey<K1, K2>, size_t, CompositeKeyHash<K1, K2>> index_;
};

// Zone map for sorted columns (min/max per block for pruning)
template<typename T>
struct ZoneMap {
    static constexpr size_t BLOCK_SIZE = 4096;

    struct Block {
        T min_val;
        T max_val;
        size_t start_idx;
        size_t end_idx;
    };

    std::vector<Block> blocks;

    void build(const std::vector<T>& sorted_column) {
        blocks.clear();
        size_t n = sorted_column.size();

        for (size_t i = 0; i < n; i += BLOCK_SIZE) {
            size_t end = std::min(i + BLOCK_SIZE, n);
            Block b;
            b.min_val = sorted_column[i];
            b.max_val = sorted_column[end - 1];
            b.start_idx = i;
            b.end_idx = end;
            blocks.push_back(b);
        }
    }

    // Find blocks that may contain values in [min_query, max_query]
    std::vector<size_t> query_blocks(T min_query, T max_query) const {
        std::vector<size_t> result;
        for (size_t i = 0; i < blocks.size(); i++) {
            const auto& b = blocks[i];
            // Block overlaps query range if block_max >= min_query AND block_min <= max_query
            if (b.max_val >= min_query && b.min_val <= max_query) {
                result.push_back(i);
            }
        }
        return result;
    }
};

} // namespace gendb_index

#endif // INDEX_H
