#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

namespace gendb {

// Hash index: key -> list of row indices
using HashIndex = std::unordered_map<int32_t, std::vector<size_t>>;

// Build hash index on a column
inline HashIndex build_hash_index(const std::vector<int32_t>& column) {
    HashIndex index;
    for (size_t i = 0; i < column.size(); i++) {
        index[column[i]].push_back(i);
    }
    return index;
}

// Composite key for Q3 aggregation (orderkey, orderdate, shippriority)
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
    size_t operator()(const Q3GroupKey& key) const {
        // Simple hash combination
        size_t h1 = std::hash<int32_t>{}(key.orderkey);
        size_t h2 = std::hash<int32_t>{}(key.orderdate);
        size_t h3 = std::hash<int32_t>{}(key.shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Aggregation state for Q3
struct Q3AggState {
    double revenue = 0.0;
    int32_t orderdate = 0;
    int32_t shippriority = 0;
};

} // namespace gendb
