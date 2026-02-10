#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_set>
#include <algorithm>
#include <vector>
#include <queue>
#include <chrono>
#include <cstring>
#include <immintrin.h>  // AVX2 intrinsics for SIMD filtering

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < DATE '1995-03-15'
//   AND l_shipdate > DATE '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10;

// Custom linear probing hash table for order_info lookup
// Power-of-2 sizing for fast modulo with bitwise AND
template<typename V>
class LinearProbingHashTable {
private:
    struct Entry {
        int32_t key;
        V value;
        uint32_t hash;
        bool occupied;
    };

    std::vector<Entry> entries;
    size_t capacity_;
    size_t size_;
    size_t mask_;  // capacity - 1, for fast modulo

    // Power-of-2 ceiling
    static size_t next_power_of_2(size_t n) {
        size_t power = 16;
        while (power < n) power *= 2;
        return power;
    }

    // Simple hash function for int32_t keys
    static uint32_t hash_key(int32_t key) {
        uint32_t h = static_cast<uint32_t>(key);
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

public:
    LinearProbingHashTable() : capacity_(0), size_(0), mask_(0) {}

    void reserve(size_t min_capacity) {
        // Size to ~75% load factor: capacity = min_capacity / 0.75
        size_t target_capacity = next_power_of_2((min_capacity * 4) / 3);
        capacity_ = target_capacity;
        mask_ = capacity_ - 1;
        entries.resize(capacity_);
        size_ = 0;

        // Initialize all entries as unoccupied
        for (auto& entry : entries) {
            entry.occupied = false;
        }
    }

    void insert(int32_t key, const V& value) {
        uint32_t hash = hash_key(key);
        size_t idx = hash & mask_;

        // Linear probing
        while (entries[idx].occupied) {
            if (entries[idx].key == key) {
                // Key already exists, update value
                entries[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask_;
        }

        // Insert new entry
        entries[idx].key = key;
        entries[idx].value = value;
        entries[idx].hash = hash;
        entries[idx].occupied = true;
        size_++;
    }

    bool find(int32_t key, V& out_value) const {
        uint32_t hash = hash_key(key);
        size_t idx = hash & mask_;

        // Linear probing
        while (entries[idx].occupied) {
            if (entries[idx].hash == hash && entries[idx].key == key) {
                out_value = entries[idx].value;
                return true;
            }
            idx = (idx + 1) & mask_;
        }

        return false;
    }

    // Prefetch for future lookup (used for pipelining)
    void prefetch(int32_t key) const {
        uint32_t hash = hash_key(key);
        size_t idx = hash & mask_;
        __builtin_prefetch(&entries[idx], 0, 3);  // Read, high temporal locality
    }

    size_t size() const { return size_; }
};

// Custom linear probing hash table for aggregation
class AggregationHashTable {
private:
    struct Entry {
        Q3GroupKey key;
        double value;
        uint32_t hash;
        bool occupied;
    };

    std::vector<Entry> entries;
    size_t capacity_;
    size_t size_;
    size_t mask_;

    static size_t next_power_of_2(size_t n) {
        size_t power = 16;
        while (power < n) power *= 2;
        return power;
    }

    // Hash function for Q3GroupKey
    static uint32_t hash_key(const Q3GroupKey& key) {
        uint32_t h1 = static_cast<uint32_t>(key.l_orderkey);
        uint32_t h2 = static_cast<uint32_t>(key.o_orderdate);
        uint32_t h3 = static_cast<uint32_t>(key.o_shippriority);

        uint32_t h = h1;
        h ^= h2 + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= h3 + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }

public:
    AggregationHashTable() : capacity_(0), size_(0), mask_(0) {}

    void reserve(size_t min_capacity) {
        size_t target_capacity = next_power_of_2((min_capacity * 4) / 3);
        capacity_ = target_capacity;
        mask_ = capacity_ - 1;
        entries.resize(capacity_);
        size_ = 0;

        for (auto& entry : entries) {
            entry.occupied = false;
            entry.value = 0.0;
        }
    }

    void add(const Q3GroupKey& key, double value) {
        uint32_t hash = hash_key(key);
        size_t idx = hash & mask_;

        // Linear probing
        while (entries[idx].occupied) {
            if (entries[idx].hash == hash && entries[idx].key == key) {
                // Key exists, accumulate value
                entries[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask_;
        }

        // Insert new entry
        entries[idx].key = key;
        entries[idx].value = value;
        entries[idx].hash = hash;
        entries[idx].occupied = true;
        size_++;
    }

    // Prefetch for future add operation
    void prefetch(const Q3GroupKey& key) const {
        uint32_t hash = hash_key(key);
        size_t idx = hash & mask_;
        __builtin_prefetch(&entries[idx], 1, 3);  // Write, high temporal locality
    }

    // Iterator support for extracting results
    class iterator {
        const Entry* ptr;
        const Entry* end;
    public:
        iterator(const Entry* p, const Entry* e) : ptr(p), end(e) {
            // Skip to first occupied entry
            while (ptr != end && !ptr->occupied) ++ptr;
        }

        iterator& operator++() {
            do { ++ptr; } while (ptr != end && !ptr->occupied);
            return *this;
        }

        bool operator!=(const iterator& other) const { return ptr != other.ptr; }

        const Entry& operator*() const { return *ptr; }
        const Entry* operator->() const { return ptr; }
    };

    iterator begin() const { return iterator(entries.data(), entries.data() + capacity_); }
    iterator end() const { return iterator(entries.data() + capacity_, entries.data() + capacity_); }

    size_t size() const { return size_; }
};

struct Q3Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        // Min-heap comparator: smaller revenue has higher priority (for top-K)
        if (revenue != other.revenue) {
            return revenue > other.revenue;  // Reverse for max-heap behavior
        }
        return o_orderdate > other.o_orderdate;
    }
};

void execute_q3(const CustomerTable& customer, const OrdersTable& orders, const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date constants
    int32_t orders_cutoff = date_utils::parse_date("1995-03-15");
    int32_t lineitem_cutoff = date_utils::parse_date("1995-03-15");

    // Step 1: Filter customers by c_mktsegment = 'BUILDING'
    // Use dictionary-encoded integer comparison (BUILDING = code 1)
    constexpr uint8_t BUILDING_CODE = 1;
    std::unordered_set<int32_t> building_custkeys;
    building_custkeys.reserve(customer.size() / 5);  // ~20% selectivity
    for (size_t i = 0; i < customer.size(); i++) {
        if (customer.c_mktsegment_code[i] == BUILDING_CODE) {
            building_custkeys.insert(customer.c_custkey[i]);
        }
    }

    // Step 2: Build custom hash table for orders and filter by date + custkey
    LinearProbingHashTable<std::pair<int32_t, int32_t>> order_info;
    order_info.reserve(orders.size() / 2);  // Pre-size for ~750K entries

    for (size_t i = 0; i < orders.size(); i++) {
        if (orders.o_orderdate[i] < orders_cutoff &&
            building_custkeys.count(orders.o_custkey[i]) > 0) {
            order_info.insert(orders.o_orderkey[i],
                            {orders.o_orderdate[i], orders.o_shippriority[i]});
        }
    }

    // Step 3: Probe lineitem with vectorized filtering and enhanced prefetching
    AggregationHashTable agg_table;
    agg_table.reserve(order_info.size());  // Pre-size to avoid rehashing

    constexpr size_t PREFETCH_DISTANCE = 16;  // Increased from 8 to hide more latency
    constexpr size_t BATCH_SIZE = 256;  // Process in batches for better cache behavior
    size_t n = lineitem.size();

#ifdef __AVX2__
    // SIMD-accelerated processing with batched prefetching
    __m256i lineitem_cutoff_vec = _mm256_set1_epi32(lineitem_cutoff);

    for (size_t batch_start = 0; batch_start < n; batch_start += BATCH_SIZE) {
        size_t batch_end = std::min(batch_start + BATCH_SIZE, n);

        // First pass: SIMD filter dates and build selection vector
        // This avoids loading orderkey/extendedprice/discount for non-qualifying rows
        uint16_t selection_vector[BATCH_SIZE];
        size_t selected_count = 0;

        size_t i = batch_start;
        for (; i + 8 <= batch_end; i += 8) {
            // Load 8 shipdate values
            __m256i shipdate = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&lineitem.l_shipdate[i]));

            // Compare: shipdate > lineitem_cutoff
            __m256i date_pass = _mm256_cmpgt_epi32(shipdate, lineitem_cutoff_vec);

            // Extract mask
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(date_pass));

            // Add passing indices to selection vector
            for (int j = 0; j < 8; j++) {
                if (mask & (1 << j)) {
                    selection_vector[selected_count++] = i + j;
                }
            }
        }

        // Scalar tail for remaining rows in batch
        for (; i < batch_end; i++) {
            if (lineitem.l_shipdate[i] > lineitem_cutoff) {
                selection_vector[selected_count++] = i;
            }
        }

        // Second pass: Process selected rows with aggressive prefetching
        for (size_t sel_idx = 0; sel_idx < selected_count; sel_idx++) {
            size_t idx = selection_vector[sel_idx];

            // Prefetch order_info for future rows in selection vector
            if (sel_idx + PREFETCH_DISTANCE < selected_count) {
                size_t future_idx = selection_vector[sel_idx + PREFETCH_DISTANCE];
                int32_t future_orderkey = lineitem.l_orderkey[future_idx];
                order_info.prefetch(future_orderkey);
            }

            // Join with filtered orders
            std::pair<int32_t, int32_t> order_data;
            if (!order_info.find(lineitem.l_orderkey[idx], order_data)) {
                continue;
            }

            // Compute revenue
            double revenue = lineitem.l_extendedprice[idx] * (1.0 - lineitem.l_discount[idx]);

            // Aggregate by (orderkey, orderdate, shippriority)
            Q3GroupKey key{lineitem.l_orderkey[idx], order_data.first, order_data.second};

            // Prefetch aggregation table entry for future rows
            if (sel_idx + PREFETCH_DISTANCE / 2 < selected_count) {
                size_t future_idx = selection_vector[sel_idx + PREFETCH_DISTANCE / 2];
                std::pair<int32_t, int32_t> future_order_data;
                if (order_info.find(lineitem.l_orderkey[future_idx], future_order_data)) {
                    Q3GroupKey future_key{lineitem.l_orderkey[future_idx], future_order_data.first, future_order_data.second};
                    agg_table.prefetch(future_key);
                }
            }

            agg_table.add(key, revenue);
        }
    }
#else
    // Fallback: scalar implementation with prefetching
    for (size_t i = 0; i < n; i++) {
        // Prefetch hash table entries for future iterations
        if (i + PREFETCH_DISTANCE < n) {
            int32_t future_orderkey = lineitem.l_orderkey[i + PREFETCH_DISTANCE];
            order_info.prefetch(future_orderkey);
        }

        // Date filter
        if (lineitem.l_shipdate[i] <= lineitem_cutoff) {
            continue;
        }

        // Join with filtered orders
        std::pair<int32_t, int32_t> order_data;
        if (!order_info.find(lineitem.l_orderkey[i], order_data)) {
            continue;
        }

        // Compute revenue
        double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);

        // Aggregate by (orderkey, orderdate, shippriority)
        Q3GroupKey key{lineitem.l_orderkey[i], order_data.first, order_data.second};

        agg_table.add(key, revenue);
    }
#endif

    // Step 4: Top-K extraction using min-heap (k=10)
    std::priority_queue<Q3Result> top_k;  // Min-heap

    for (auto it = agg_table.begin(); it != agg_table.end(); ++it) {
        Q3Result result{it->key.l_orderkey, it->value, it->key.o_orderdate, it->key.o_shippriority};

        if (top_k.size() < 10) {
            top_k.push(result);
        } else if (result < top_k.top()) {  // Better than smallest in heap
            top_k.pop();
            top_k.push(result);
        }
    }

    // Extract results and sort
    std::vector<Q3Result> results;
    while (!top_k.empty()) {
        results.push_back(top_k.top());
        top_k.pop();
    }
    std::reverse(results.begin(), results.end());  // Reverse to get descending order

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q3: Shipping Priority ===" << std::endl;
    std::cout << std::left
              << std::setw(12) << "orderkey"
              << std::setw(18) << "revenue"
              << std::setw(15) << "orderdate"
              << std::setw(15) << "shippriority"
              << std::endl;

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& result : results) {
        std::cout << std::left
                  << std::setw(12) << result.l_orderkey
                  << std::setw(18) << result.revenue
                  << std::setw(15) << date_utils::days_to_date_str(result.o_orderdate)
                  << std::setw(15) << result.o_shippriority
                  << std::endl;
    }

    std::cout << "\nExecution time: " << duration.count() << " ms" << std::endl;
}
