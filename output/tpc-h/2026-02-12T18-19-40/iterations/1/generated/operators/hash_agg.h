#ifndef HASH_AGG_H
#define HASH_AGG_H

#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstddef>

// Hash aggregation operators

namespace operators {

// Single-key aggregation with multiple aggregate functions
template<typename KeyType>
class HashAggregator {
public:
    struct AggState {
        int64_t sum = 0;
        int64_t count = 0;
        int64_t min_val = INT64_MAX;
        int64_t max_val = INT64_MIN;

        void update(int64_t val) {
            sum += val;
            count++;
            if (val < min_val) min_val = val;
            if (val > max_val) max_val = val;
        }

        double avg() const {
            return count > 0 ? static_cast<double>(sum) / count : 0.0;
        }
    };

    void insert(KeyType key, int64_t value) {
        agg_table_[key].update(value);
    }

    const std::unordered_map<KeyType, AggState>& get_results() const {
        return agg_table_;
    }

    size_t size() const { return agg_table_.size(); }

private:
    std::unordered_map<KeyType, AggState> agg_table_;
};

// Multi-key aggregation (for GROUP BY with multiple columns)
template<typename K1, typename K2>
struct AggKey {
    K1 key1;
    K2 key2;

    bool operator==(const AggKey& other) const {
        return key1 == other.key1 && key2 == other.key2;
    }
};

template<typename K1, typename K2>
struct AggKeyHash {
    size_t operator()(const AggKey<K1, K2>& k) const {
        return std::hash<K1>()(k.key1) ^ (std::hash<K2>()(k.key2) << 1);
    }
};

// Three-key aggregation (for Q3: l_orderkey, o_orderdate, o_shippriority)
template<typename K1, typename K2, typename K3>
struct AggKey3 {
    K1 key1;
    K2 key2;
    K3 key3;

    bool operator==(const AggKey3& other) const {
        return key1 == other.key1 && key2 == other.key2 && key3 == other.key3;
    }
};

template<typename K1, typename K2, typename K3>
struct AggKey3Hash {
    size_t operator()(const AggKey3<K1, K2, K3>& k) const {
        size_t h1 = std::hash<K1>()(k.key1);
        size_t h2 = std::hash<K2>()(k.key2);
        size_t h3 = std::hash<K3>()(k.key3);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

template<typename K1, typename K2, typename K3>
class MultiKeyAggregator {
public:
    struct AggState {
        int64_t sum = 0;
        int64_t count = 0;

        void update(int64_t val) {
            sum += val;
            count++;
        }

        double avg() const {
            return count > 0 ? static_cast<double>(sum) / count : 0.0;
        }
    };

    void insert(K1 key1, K2 key2, K3 key3, int64_t value) {
        agg_table_[{key1, key2, key3}].update(value);
    }

    const std::unordered_map<AggKey3<K1, K2, K3>, AggState, AggKey3Hash<K1, K2, K3>>& get_results() const {
        return agg_table_;
    }

    size_t size() const { return agg_table_.size(); }

private:
    std::unordered_map<AggKey3<K1, K2, K3>, AggState, AggKey3Hash<K1, K2, K3>> agg_table_;
};

// Top-K heap for ORDER BY ... LIMIT queries
template<typename KeyType, typename ValueType>
struct TopKHeap {
    struct Entry {
        KeyType key;
        ValueType value;

        // Min-heap comparison (to keep top K largest values)
        bool operator>(const Entry& other) const {
            return value > other.value;
        }
    };

    std::vector<Entry> heap;
    size_t k;

    TopKHeap(size_t k_) : k(k_) {
        heap.reserve(k + 1);
    }

    void insert(KeyType key, ValueType value) {
        if (heap.size() < k) {
            // Heap not full yet
            heap.push_back({key, value});
            std::push_heap(heap.begin(), heap.end(), std::greater<Entry>());
        } else if (value > heap.front().value) {
            // New value is larger than smallest in heap
            std::pop_heap(heap.begin(), heap.end(), std::greater<Entry>());
            heap.back() = {key, value};
            std::push_heap(heap.begin(), heap.end(), std::greater<Entry>());
        }
    }

    std::vector<Entry> get_sorted() {
        std::sort_heap(heap.begin(), heap.end(), std::greater<Entry>());
        std::reverse(heap.begin(), heap.end()); // Want descending order
        return heap;
    }
};

} // namespace operators

#endif // HASH_AGG_H
