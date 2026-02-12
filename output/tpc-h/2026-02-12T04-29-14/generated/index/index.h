#pragma once

#include <unordered_map>
#include <cstdint>
#include <vector>
#include <utility>

namespace gendb {

// Simple hash index: key -> vector of row IDs
template<typename K>
using HashIndex = std::unordered_map<K, std::vector<size_t>>;

// Composite key for multi-column indexes
struct CompositeKey2 {
    int32_t k1;
    int32_t k2;

    bool operator==(const CompositeKey2& o) const {
        return k1 == o.k1 && k2 == o.k2;
    }
};

struct CompositeKey3 {
    int32_t k1;
    int32_t k2;
    int32_t k3;

    bool operator==(const CompositeKey3& o) const {
        return k1 == o.k1 && k2 == o.k2 && k3 == o.k3;
    }
};

} // namespace gendb

// Hash functors for composite keys
namespace std {
    template<>
    struct hash<gendb::CompositeKey2> {
        size_t operator()(const gendb::CompositeKey2& k) const {
            return std::hash<int32_t>()(k.k1) ^ (std::hash<int32_t>()(k.k2) << 1);
        }
    };

    template<>
    struct hash<gendb::CompositeKey3> {
        size_t operator()(const gendb::CompositeKey3& k) const {
            return std::hash<int32_t>()(k.k1) ^ (std::hash<int32_t>()(k.k2) << 1) ^ (std::hash<int32_t>()(k.k3) << 2);
        }
    };
}
