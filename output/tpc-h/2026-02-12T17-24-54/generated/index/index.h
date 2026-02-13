#pragma once
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace gendb {

// Simple hash-based index for primary key lookups
template<typename KeyType>
class HashIndex {
public:
    void insert(KeyType key, size_t row_id) {
        index_[key] = row_id;
    }

    bool find(KeyType key, size_t& row_id) const {
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

// Composite key index for (key1, key2) pairs
template<typename K1, typename K2>
class CompositeHashIndex {
public:
    void insert(K1 key1, K2 key2, size_t row_id) {
        uint64_t composite = (static_cast<uint64_t>(key1) << 32) | static_cast<uint64_t>(key2);
        index_[composite] = row_id;
    }

    bool find(K1 key1, K2 key2, size_t& row_id) const {
        uint64_t composite = (static_cast<uint64_t>(key1) << 32) | static_cast<uint64_t>(key2);
        auto it = index_.find(composite);
        if (it != index_.end()) {
            row_id = it->second;
            return true;
        }
        return false;
    }

    size_t size() const { return index_.size(); }

private:
    std::unordered_map<uint64_t, size_t> index_;
};

} // namespace gendb
