#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

namespace gendb {

// Hash index: maps key to vector of row indices
using HashIndex = std::unordered_map<int32_t, std::vector<size_t>>;

// Composite key for Q3 aggregation: (l_orderkey, o_orderdate, o_shippriority)
struct Q3GroupKey {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;

    bool operator==(const Q3GroupKey& other) const {
        return orderkey == other.orderkey &&
               orderdate == other.orderdate &&
               shippriority == other.shippriority;
    }
};

// Hash functor for Q3GroupKey
struct Q3GroupKeyHash {
    size_t operator()(const Q3GroupKey& k) const {
        // Simple hash combination
        size_t h1 = std::hash<int32_t>{}(k.orderkey);
        size_t h2 = std::hash<int32_t>{}(k.orderdate);
        size_t h3 = std::hash<int32_t>{}(k.shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Composite key for Q1 aggregation: (returnflag_code, linestatus_code)
// Using dictionary-encoded uint8_t instead of strings
struct Q1GroupKey {
    uint8_t returnflag_code;
    uint8_t linestatus_code;

    bool operator==(const Q1GroupKey& other) const {
        return returnflag_code == other.returnflag_code &&
               linestatus_code == other.linestatus_code;
    }

    // For sorting
    bool operator<(const Q1GroupKey& other) const {
        if (returnflag_code != other.returnflag_code)
            return returnflag_code < other.returnflag_code;
        return linestatus_code < other.linestatus_code;
    }
};

// Hash functor for Q1GroupKey (not used with direct indexing, but kept for compatibility)
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& k) const {
        return (static_cast<size_t>(k.returnflag_code) << 8) | k.linestatus_code;
    }
};

} // namespace gendb
