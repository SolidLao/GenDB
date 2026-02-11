#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>

namespace gendb {

// Hash index for join keys (orderkey -> custkey)
using OrderCustomerIndex = std::unordered_map<int32_t, int32_t>;

// Hash index for aggregation groups (composite key)
struct CompositeKey {
    char returnflag;
    char linestatus;

    bool operator==(const CompositeKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash functor for composite key
struct CompositeKeyHash {
    size_t operator()(const CompositeKey& key) const {
        return std::hash<int>()(static_cast<int>(key.returnflag) << 8 |
                                static_cast<int>(key.linestatus));
    }
};

// Aggregation state for Q1
struct Q1AggState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_disc = 0.0;
    int64_t count = 0;

    double avg_qty() const { return count > 0 ? sum_qty / count : 0.0; }
    double avg_price() const { return count > 0 ? sum_base_price / count : 0.0; }
    double avg_disc() const { return count > 0 ? sum_disc / count : 0.0; }
};

using Q1AggTable = std::unordered_map<CompositeKey, Q1AggState, CompositeKeyHash>;

// Aggregation state for Q3 (orderkey -> revenue)
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

struct Q3GroupKeyHash {
    size_t operator()(const Q3GroupKey& key) const {
        return std::hash<int32_t>()(key.l_orderkey) ^
               (std::hash<int32_t>()(key.o_orderdate) << 1) ^
               (std::hash<int32_t>()(key.o_shippriority) << 2);
    }
};

using Q3AggTable = std::unordered_map<Q3GroupKey, double, Q3GroupKeyHash>;

} // namespace gendb
