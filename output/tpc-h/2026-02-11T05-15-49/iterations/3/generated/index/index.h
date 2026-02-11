#pragma once

#include <unordered_map>
#include <cstdint>
#include <vector>

namespace gendb {

// Hash index typedefs for join operations
// These are in-memory hash indexes built during query execution

// Simple hash index: key -> list of row indices
using HashIndex = std::unordered_map<int32_t, std::vector<size_t>>;

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
        // Simple hash combination
        size_t h1 = std::hash<int32_t>{}(key.l_orderkey);
        size_t h2 = std::hash<int32_t>{}(key.o_orderdate);
        size_t h3 = std::hash<int32_t>{}(key.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Composite key for Q1 aggregation (l_returnflag, l_linestatus)
struct Q1GroupKey {
    uint8_t l_returnflag;
    uint8_t l_linestatus;

    bool operator==(const Q1GroupKey& other) const {
        return l_returnflag == other.l_returnflag &&
               l_linestatus == other.l_linestatus;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& key) const {
        return (static_cast<size_t>(key.l_returnflag) << 8) | key.l_linestatus;
    }
};

// Aggregation structures
struct Q1AggResult {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;  // For AVG(l_discount)
    size_t count = 0;

    // Merge operator for parallel aggregation
    Q1AggResult& operator+=(const Q1AggResult& other) {
        sum_qty += other.sum_qty;
        sum_base_price += other.sum_base_price;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        sum_discount += other.sum_discount;
        count += other.count;
        return *this;
    }
};

struct Q3AggResult {
    double revenue = 0.0;

    // Merge operator for parallel aggregation
    Q3AggResult& operator+=(const Q3AggResult& other) {
        revenue += other.revenue;
        return *this;
    }
};

}  // namespace gendb
