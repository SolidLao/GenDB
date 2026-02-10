#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

// Hash index type definitions for join keys

// Simple hash index: key -> vector of row indices
using HashIndex = std::unordered_map<int32_t, std::vector<size_t>>;

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
    std::size_t operator()(const Q3GroupKey& k) const {
        // Simple hash combination
        std::size_t h1 = std::hash<int32_t>{}(k.l_orderkey);
        std::size_t h2 = std::hash<int32_t>{}(k.o_orderdate);
        std::size_t h3 = std::hash<int32_t>{}(k.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Composite key for Q1 grouping: (l_returnflag, l_linestatus)
struct Q1GroupKey {
    char l_returnflag;
    char l_linestatus;

    bool operator==(const Q1GroupKey& other) const {
        return l_returnflag == other.l_returnflag &&
               l_linestatus == other.l_linestatus;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    std::size_t operator()(const Q1GroupKey& k) const {
        return (static_cast<size_t>(k.l_returnflag) << 8) | static_cast<size_t>(k.l_linestatus);
    }
};

// Aggregate values for Q1
struct Q1Aggregate {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;
};

// Aggregate values for Q3
struct Q3Aggregate {
    double revenue = 0.0;
};
