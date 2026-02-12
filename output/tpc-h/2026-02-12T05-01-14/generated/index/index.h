#pragma once

#include <unordered_map>
#include <cstdint>
#include <string>
#include <vector>

namespace gendb {

// Hash index type aliases
using Int32HashIndex = std::unordered_map<int32_t, std::vector<size_t>>;
using Int32UniqueHashIndex = std::unordered_map<int32_t, size_t>;

// Composite key for Q3 aggregation (l_orderkey, o_orderdate, o_shippriority)
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
        size_t h1 = std::hash<int32_t>{}(key.l_orderkey);
        size_t h2 = std::hash<int32_t>{}(key.o_orderdate);
        size_t h3 = std::hash<int32_t>{}(key.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Aggregation result for Q3
struct Q3AggValue {
    double revenue = 0.0;
};

using Q3AggMap = std::unordered_map<Q3GroupKey, Q3AggValue, Q3GroupKeyHash>;

// Composite key for Q1 aggregation (l_returnflag, l_linestatus)
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
    size_t operator()(const Q1GroupKey& key) const {
        return (size_t)key.l_returnflag * 256 + (size_t)key.l_linestatus;
    }
};

// Aggregation result for Q1
struct Q1AggValue {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    size_t count = 0;
};

using Q1AggMap = std::unordered_map<Q1GroupKey, Q1AggValue, Q1GroupKeyHash>;

} // namespace gendb
