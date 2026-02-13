#pragma once
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <tuple>

namespace gendb {

// Hash aggregation for single-key grouping
template<typename KeyType, typename AggType>
class HashAgg {
public:
    void insert(KeyType key, AggType value) {
        auto& agg = agg_table_[key];
        agg.sum += value;
        agg.count++;
    }

    struct AggResult {
        AggType sum;
        size_t count;
        double avg() const { return static_cast<double>(sum) / count; }
    };

    const std::unordered_map<KeyType, AggResult>& get_results() const {
        return agg_table_;
    }

private:
    std::unordered_map<KeyType, AggResult> agg_table_;
};

// Multi-value aggregation (for multiple SUM/AVG in same group)
template<typename KeyType>
class MultiAgg {
public:
    struct AggValues {
        int64_t sum_qty = 0;
        int64_t sum_base_price = 0;
        int64_t sum_disc_price = 0;
        int64_t sum_charge = 0;
        int64_t sum_discount = 0;
        size_t count = 0;
    };

    void insert(KeyType key, int64_t qty, int64_t base_price,
                int64_t disc_price, int64_t charge, int64_t discount) {
        auto& agg = agg_table_[key];
        agg.sum_qty += qty;
        agg.sum_base_price += base_price;
        agg.sum_disc_price += disc_price;
        agg.sum_charge += charge;
        agg.sum_discount += discount;
        agg.count++;
    }

    const std::unordered_map<KeyType, AggValues>& get_results() const {
        return agg_table_;
    }

    std::unordered_map<KeyType, AggValues>& get_results() {
        return agg_table_;
    }

private:
    std::unordered_map<KeyType, AggValues> agg_table_;
};

// Composite key (2 values) aggregation
struct CompositeKey2 {
    uint8_t k1;
    uint8_t k2;

    bool operator==(const CompositeKey2& other) const {
        return k1 == other.k1 && k2 == other.k2;
    }
};

struct CompositeKey3 {
    int32_t k1;
    int32_t k2;
    int32_t k3;

    bool operator==(const CompositeKey3& other) const {
        return k1 == other.k1 && k2 == other.k2 && k3 == other.k3;
    }
};

} // namespace gendb

// Hash functions for composite keys
namespace std {
    template<>
    struct hash<gendb::CompositeKey2> {
        size_t operator()(const gendb::CompositeKey2& k) const {
            return (static_cast<size_t>(k.k1) << 8) | k.k2;
        }
    };

    template<>
    struct hash<gendb::CompositeKey3> {
        size_t operator()(const gendb::CompositeKey3& k) const {
            size_t h1 = std::hash<int32_t>{}(k.k1);
            size_t h2 = std::hash<int32_t>{}(k.k2);
            size_t h3 = std::hash<int32_t>{}(k.k3);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}
