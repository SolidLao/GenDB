#ifndef HASH_JOIN_H
#define HASH_JOIN_H

#include <vector>
#include <unordered_map>
#include <cstddef>

// Hash join operators

namespace operators {

// Simple hash join: build hash table on left, probe with right
template<typename KeyType>
class HashJoin {
public:
    // Build phase: create hash table mapping key -> list of row indices
    void build(const KeyType* keys, size_t count) {
        hash_table_.clear();
        hash_table_.reserve(count);

        for (size_t i = 0; i < count; i++) {
            hash_table_[keys[i]].push_back(i);
        }
    }

    // Probe phase: find matching rows
    // Returns pairs of (left_row_id, right_row_id)
    std::vector<std::pair<size_t, size_t>> probe(const KeyType* keys, size_t count) const {
        std::vector<std::pair<size_t, size_t>> result;
        result.reserve(count); // Assume ~1:1 join ratio

        for (size_t i = 0; i < count; i++) {
            auto it = hash_table_.find(keys[i]);
            if (it != hash_table_.end()) {
                // Match found - emit all matching pairs
                for (size_t left_idx : it->second) {
                    result.emplace_back(left_idx, i);
                }
            }
        }

        return result;
    }

    size_t build_size() const { return hash_table_.size(); }

private:
    std::unordered_map<KeyType, std::vector<size_t>> hash_table_;
};

// Multi-key hash join (for composite keys)
template<typename K1, typename K2>
struct JoinKey {
    K1 key1;
    K2 key2;

    bool operator==(const JoinKey& other) const {
        return key1 == other.key1 && key2 == other.key2;
    }
};

template<typename K1, typename K2>
struct JoinKeyHash {
    size_t operator()(const JoinKey<K1, K2>& k) const {
        return std::hash<K1>()(k.key1) ^ (std::hash<K2>()(k.key2) << 1);
    }
};

template<typename K1, typename K2>
class MultiKeyHashJoin {
public:
    void build(const K1* keys1, const K2* keys2, size_t count) {
        hash_table_.clear();
        hash_table_.reserve(count);

        for (size_t i = 0; i < count; i++) {
            hash_table_[{keys1[i], keys2[i]}].push_back(i);
        }
    }

    std::vector<std::pair<size_t, size_t>> probe(const K1* keys1, const K2* keys2, size_t count) const {
        std::vector<std::pair<size_t, size_t>> result;
        result.reserve(count);

        for (size_t i = 0; i < count; i++) {
            auto it = hash_table_.find({keys1[i], keys2[i]});
            if (it != hash_table_.end()) {
                for (size_t left_idx : it->second) {
                    result.emplace_back(left_idx, i);
                }
            }
        }

        return result;
    }

private:
    std::unordered_map<JoinKey<K1, K2>, std::vector<size_t>, JoinKeyHash<K1, K2>> hash_table_;
};

} // namespace operators

#endif // HASH_JOIN_H
