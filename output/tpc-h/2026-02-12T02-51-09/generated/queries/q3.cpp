#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <queue>

namespace gendb {

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

struct Q3Result {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;

    bool operator<(const Q3Result& other) const {
        // Min-heap comparator (reverse for top-K)
        if (revenue != other.revenue) return revenue > other.revenue; // Want largest
        return o_orderdate < other.o_orderdate;
    }
};

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Read metadata
    size_t customer_count = read_row_count(gendb_dir, "customer");
    size_t orders_count = read_row_count(gendb_dir, "orders");
    size_t lineitem_count = read_row_count(gendb_dir, "lineitem");

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    auto c_custkey_col = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", customer_count);

    // Read customer_c_mktsegment (string column - need to read manually for filtering)
    // For simplicity, we'll load mktsegment into memory for filtering
    std::string mktseg_path = gendb_dir + "/customer_c_mktsegment.bin";
    FILE* mktseg_file = fopen(mktseg_path.c_str(), "rb");
    std::vector<std::string> c_mktsegment;
    c_mktsegment.reserve(customer_count);
    for (size_t i = 0; i < customer_count; ++i) {
        uint32_t len;
        fread(&len, sizeof(len), 1, mktseg_file);
        std::string s(len, '\0');
        fread(&s[0], 1, len, mktseg_file);
        c_mktsegment.push_back(s);
    }
    fclose(mktseg_file);

    std::unordered_set<int32_t> building_custkeys;
    for (size_t i = 0; i < customer_count; ++i) {
        if (c_mktsegment[i] == "BUILDING") {
            building_custkeys.insert(c_custkey_col.data[i]);
        }
    }

    // Step 2: Filter orders by o_orderdate < '1995-03-15' AND o_custkey IN building_custkeys
    auto o_orderkey_col = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", orders_count);
    auto o_custkey_col = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", orders_count);
    auto o_orderdate_col = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", orders_count);
    auto o_shippriority_col = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", orders_count);

    int32_t order_date_cutoff = parse_date("1995-03-15");

    // Build hash table: o_orderkey -> (o_orderdate, o_shippriority)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> qualified_orders;
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderdate_col.data[i] < order_date_cutoff &&
            building_custkeys.count(o_custkey_col.data[i]) > 0) {
            qualified_orders[o_orderkey_col.data[i]] = {o_orderdate_col.data[i], o_shippriority_col.data[i]};
        }
    }

    // Step 3: Scan lineitem, filter by l_shipdate > '1995-03-15', probe qualified_orders, aggregate
    auto l_orderkey_col = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", lineitem_count);
    auto l_shipdate_col = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", lineitem_count);
    auto l_extendedprice_col = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", lineitem_count);
    auto l_discount_col = mmap_column<double>(gendb_dir, "lineitem", "l_discount", lineitem_count);

    int32_t line_date_cutoff = parse_date("1995-03-15");

    // Aggregate by (l_orderkey, o_orderdate, o_shippriority)
    std::unordered_map<CompositeKey3, double> aggregates;

    for (size_t i = 0; i < lineitem_count; ++i) {
        if (l_shipdate_col.data[i] > line_date_cutoff) {
            int32_t orderkey = l_orderkey_col.data[i];
            auto it = qualified_orders.find(orderkey);
            if (it != qualified_orders.end()) {
                CompositeKey3 key{orderkey, it->second.first, it->second.second};
                double revenue = l_extendedprice_col.data[i] * (1.0 - l_discount_col.data[i]);
                aggregates[key] += revenue;
            }
        }
    }

    // Step 4: Top-K using min-heap (LIMIT 10)
    std::priority_queue<Q3Result> min_heap;

    for (const auto& [key, revenue] : aggregates) {
        Q3Result result{key.key1, key.key2, key.key3, revenue};

        if (min_heap.size() < 10) {
            min_heap.push(result);
        } else if (result < min_heap.top()) {
            // New result is better (larger revenue or same revenue but earlier date)
            min_heap.pop();
            min_heap.push(result);
        }
    }

    // Extract and sort top-K
    std::vector<Q3Result> top_results;
    while (!min_heap.empty()) {
        top_results.push_back(min_heap.top());
        min_heap.pop();
    }

    // Sort in descending order by revenue, then ascending by orderdate
    std::sort(top_results.begin(), top_results.end(),
              [](const Q3Result& a, const Q3Result& b) {
                  if (a.revenue != b.revenue) return a.revenue > b.revenue;
                  return a.o_orderdate < b.o_orderdate;
              });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Output results
    std::cout << "Q3 Results: " << top_results.size() << " rows in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& result : top_results) {
            out << result.l_orderkey << "," << result.revenue << ","
                << days_to_date_str(result.o_orderdate) << "," << result.o_shippriority << "\n";
        }
        out.close();
    }
}

} // namespace gendb
