#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

// Hash index structures for efficient joins
// Uses std::unordered_map for simplicity in baseline implementation

// Hash index: orderkey -> list of row indices
using OrderkeyIndex = std::unordered_map<int32_t, std::vector<size_t>>;

// Hash index: custkey -> list of row indices
using CustkeyIndex = std::unordered_map<int32_t, std::vector<size_t>>;

// Composite key for Q3 grouping: (l_orderkey, o_orderdate, o_shippriority)
struct Q3GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

// Hash functor for Q3GroupKey
struct Q3GroupKeyHash {
    size_t operator()(const Q3GroupKey& key) const {
        // Simple hash combination
        size_t h1 = std::hash<int32_t>{}(key.l_orderkey);
        size_t h2 = std::hash<int32_t>{}(key.o_orderdate);
        size_t h3 = std::hash<int32_t>{}(key.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Composite key for Q1 grouping: (returnflag, linestatus)
struct Q1GroupKey {
    char returnflag;
    char linestatus;

    bool operator==(const Q1GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& key) const {
        return (static_cast<size_t>(key.returnflag) << 8) | static_cast<size_t>(key.linestatus);
    }
};
