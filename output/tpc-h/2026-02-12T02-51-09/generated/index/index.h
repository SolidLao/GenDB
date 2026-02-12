#pragma once

#include <unordered_map>
#include <cstdint>

namespace gendb {

// Hash index typedefs for joins
using OrderKeyIndex = std::unordered_map<int32_t, size_t>;
using CustKeyIndex = std::unordered_map<int32_t, size_t>;

// Composite key for multi-column grouping
struct CompositeKey2 {
    char key1;
    char key2;

    bool operator==(const CompositeKey2& other) const {
        return key1 == other.key1 && key2 == other.key2;
    }
};

struct CompositeKey3 {
    int32_t key1;
    int32_t key2;
    int32_t key3;

    bool operator==(const CompositeKey3& other) const {
        return key1 == other.key1 && key2 == other.key2 && key3 == other.key3;
    }
};

} // namespace gendb

// Hash functors
namespace std {
    template<>
    struct hash<gendb::CompositeKey2> {
        size_t operator()(const gendb::CompositeKey2& k) const {
            return hash<int>()((int)k.key1 * 256 + (int)k.key2);
        }
    };

    template<>
    struct hash<gendb::CompositeKey3> {
        size_t operator()(const gendb::CompositeKey3& k) const {
            size_t h1 = hash<int32_t>()(k.key1);
            size_t h2 = hash<int32_t>()(k.key2);
            size_t h3 = hash<int32_t>()(k.key3);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}
