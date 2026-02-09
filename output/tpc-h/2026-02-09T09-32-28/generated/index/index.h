#pragma once

#include <unordered_map>
#include <cstdint>
#include <string>
#include <functional>

// Hash index type alias for single-key lookups
using HashIndex = std::unordered_map<int32_t, size_t>;

// Composite key for Q1 grouping: (l_returnflag, l_linestatus)
struct Q1GroupKey {
    std::string returnflag;
    std::string linestatus;

    bool operator==(const Q1GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }

    bool operator<(const Q1GroupKey& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& key) const {
        return std::hash<std::string>()(key.returnflag) ^
               (std::hash<std::string>()(key.linestatus) << 1);
    }
};

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
        return std::hash<int32_t>()(key.l_orderkey) ^
               (std::hash<int32_t>()(key.o_orderdate) << 1) ^
               (std::hash<int32_t>()(key.o_shippriority) << 2);
    }
};
