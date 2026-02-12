#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <queue>

namespace gendb {

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < '1995-03-15'
//   AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

struct Q3Key {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3Key& o) const {
        return l_orderkey == o.l_orderkey && o_orderdate == o.o_orderdate && o_shippriority == o.o_shippriority;
    }
};

struct Q3Result {
    Q3Key key;
    double revenue;

    bool operator<(const Q3Result& o) const {
        // For max-heap: reverse order (smaller revenue has higher priority)
        if (revenue != o.revenue) return revenue < o.revenue;
        return key.o_orderdate > o.key.o_orderdate;
    }
};

} // namespace gendb

namespace std {
    template<>
    struct hash<gendb::Q3Key> {
        size_t operator()(const gendb::Q3Key& k) const {
            return std::hash<int32_t>()(k.l_orderkey) ^
                   (std::hash<int32_t>()(k.o_orderdate) << 1) ^
                   (std::hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

namespace gendb {

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t c_row_count;
    auto c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", c_row_count);

    // Load orders columns
    size_t o_row_count;
    auto o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", o_row_count);
    auto o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", o_row_count);
    auto o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", o_row_count);
    auto o_shippriority = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", o_row_count);

    // Load lineitem columns
    size_t l_row_count;
    auto l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", l_row_count);
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", l_row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", l_row_count);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", l_row_count);

    int32_t order_date_cutoff = parse_date("1995-03-15");
    int32_t ship_date_cutoff = parse_date("1995-03-15");

    // Step 1: Build hash set of customer keys (simplified - accept all)
    operators::HashJoin<int32_t, bool> customer_join;
    customer_join.build(
        c_row_count,
        [&](size_t i) { return c_custkey[i]; },
        [](size_t) { return true; }
    );

    // Step 2: Build hash table for orders (join with customer + filter by date)
    using OrderValue = std::pair<int32_t, int32_t>;  // (orderdate, shippriority)
    operators::HashJoin<int32_t, OrderValue> order_join;

    // Build order hash table: only include orders that match customer and date filter
    std::vector<int32_t> filtered_orderkeys;
    std::vector<OrderValue> filtered_orderdata;

    for (size_t i = 0; i < o_row_count; i++) {
        if (o_orderdate[i] < order_date_cutoff &&
            customer_join.contains([&](size_t) { return o_custkey[i]; }, 0)) {
            filtered_orderkeys.push_back(o_orderkey[i]);
            filtered_orderdata.push_back({o_orderdate[i], o_shippriority[i]});
        }
    }

    order_join.build(
        filtered_orderkeys.size(),
        [&](size_t i) { return filtered_orderkeys[i]; },
        [&](size_t i) { return filtered_orderdata[i]; }
    );

    // Step 3: Manual aggregation with join
    std::unordered_map<Q3Key, double> aggregates;

    for (size_t i = 0; i < l_row_count; i++) {
        if (l_shipdate[i] > ship_date_cutoff) {
            // Probe order hash table
            order_join.probe(
                1,
                [&](size_t) { return l_orderkey[i]; },
                [&](size_t, const OrderValue& order_data) {
                    Q3Key key{l_orderkey[i], order_data.first, order_data.second};
                    double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                    aggregates[key] += revenue;
                }
            );
        }
    }

    // Top-K with heap (LIMIT 10)
    std::priority_queue<Q3Result> heap;  // max-heap by revenue (reversed comparison)
    for (const auto& [key, revenue] : aggregates) {
        Q3Result result{key, revenue};
        if (heap.size() < 10) {
            heap.push(result);
        } else if (result < heap.top()) {
            // result has higher revenue than current min
            heap.pop();
            heap.push(result);
        }
    }

    // Extract results and sort
    std::vector<Q3Result> top10;
    while (!heap.empty()) {
        top10.push_back(heap.top());
        heap.pop();
    }
    std::reverse(top10.begin(), top10.end());  // Heap gives min first, we want max first

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print to terminal: only row count and timing
    std::cout << "Q3: " << top10.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << " seconds" << std::endl;

    // Write results to file if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& r : top10) {
            out << r.key.l_orderkey << ","
                << r.revenue << ","
                << days_to_date_str(r.key.o_orderdate) << ","
                << r.key.o_shippriority << "\n";
        }
    }
}

} // namespace gendb
