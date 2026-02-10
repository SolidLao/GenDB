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

// Composite key for Q1 aggregation: (returnflag, linestatus)
struct Q1GroupKey {
    std::string returnflag;
    std::string linestatus;

    bool operator==(const Q1GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& k) const {
        size_t h1 = std::hash<std::string>{}(k.returnflag);
        size_t h2 = std::hash<std::string>{}(k.linestatus);
        return h1 ^ (h2 << 1);
    }
};

} // namespace gendb
