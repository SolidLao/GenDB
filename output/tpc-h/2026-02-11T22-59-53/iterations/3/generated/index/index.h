#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

// Hash index typedefs for join operations
using HashIndex_int32 = std::unordered_map<int32_t, std::vector<size_t>>;

// Composite key for multi-column indexes
struct CompositeKey2 {
    int32_t k1;
    int32_t k2;

    bool operator==(const CompositeKey2& other) const {
        return k1 == other.k1 && k2 == other.k2;
    }
};

struct CompositeKey3 {
    int32_t k1;
    int32_t k2;
    int32_t k3;

    bool operator==(const CompositeKey3& other) const {
        return k1 == other.k1 && k2 == other.k2 && k3 == other.k3;
    }
};

// Hash functors
namespace std {
    template<>
    struct hash<CompositeKey2> {
        size_t operator()(const CompositeKey2& k) const {
            return hash<int32_t>()(k.k1) ^ (hash<int32_t>()(k.k2) << 1);
        }
    };

    template<>
    struct hash<CompositeKey3> {
        size_t operator()(const CompositeKey3& k) const {
            return hash<int32_t>()(k.k1) ^ (hash<int32_t>()(k.k2) << 1) ^ (hash<int32_t>()(k.k3) << 2);
        }
    };
}
