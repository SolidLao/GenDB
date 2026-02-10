#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <vector>
#include <queue>
#include <chrono>

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

    // Step 2: Build hash index on orders.o_orderkey and filter by date + custkey
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_info;  // orderkey -> (orderdate, shippriority)
    order_info.reserve(orders.size() / 2);
    for (size_t i = 0; i < orders.size(); i++) {
        if (orders.o_orderdate[i] < orders_cutoff &&
            building_custkeys.count(orders.o_custkey[i]) > 0) {
            order_info[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
        }
    }

    // Step 3: Probe lineitem with date filter and join with filtered orders
    std::unordered_map<Q3GroupKey, double, Q3GroupKeyHash> agg_table;
    agg_table.reserve(order_info.size());  // Pre-size to avoid rehashing
    for (size_t i = 0; i < lineitem.size(); i++) {
        if (lineitem.l_shipdate[i] <= lineitem_cutoff) {
            continue;
        }

        auto it = order_info.find(lineitem.l_orderkey[i]);
        if (it == order_info.end()) {
            continue;
        }

        // Compute revenue
        double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);

        // Aggregate by (orderkey, orderdate, shippriority)
        Q3GroupKey key{lineitem.l_orderkey[i], it->second.first, it->second.second};
        agg_table[key] += revenue;
    }

    // Step 4: Top-K extraction using min-heap (k=10)
    std::priority_queue<Q3Result> top_k;  // Min-heap

    for (const auto& [key, revenue] : agg_table) {
        Q3Result result{key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority};

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
