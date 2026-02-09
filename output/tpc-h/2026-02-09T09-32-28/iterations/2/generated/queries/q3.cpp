#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < date '1995-03-15'
//   AND l_shipdate > date '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

struct Q3Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        if (revenue != other.revenue) return revenue > other.revenue; // DESC
        return o_orderdate < other.o_orderdate; // ASC
    }
};

void execute_q3(const CustomerTable& customer,
                const OrdersTable& orders,
                const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Parse filter dates
    int32_t orderdate_cutoff = parse_date("1995-03-15");
    int32_t shipdate_cutoff = parse_date("1995-03-15");

    // Filter customers with c_mktsegment = 'BUILDING'
    std::unordered_map<int32_t, size_t> customer_idx;
    for (size_t i = 0; i < customer.size(); ++i) {
        if (customer.c_mktsegment[i] == "BUILDING") {
            customer_idx[customer.c_custkey[i]] = i;
        }
    }

    // Build filtered orders index for orders matching customer and date filter
    // Using pre-built o_orderkey_index as base, filter by custkey and date
    std::unordered_map<int32_t, size_t> orders_idx;
    for (size_t i = 0; i < orders.size(); ++i) {
        if (orders.o_orderdate[i] < orderdate_cutoff &&
            customer_idx.count(orders.o_custkey[i]) > 0) {
            orders_idx[orders.o_orderkey[i]] = i;
        }
    }

    // Scan lineitem and join with orders, then aggregate
    std::unordered_map<Q3GroupKey, double, Q3GroupKeyHash> groups;

    for (size_t i = 0; i < lineitem.size(); ++i) {
        if (lineitem.l_shipdate[i] > shipdate_cutoff) {
            auto it = orders_idx.find(lineitem.l_orderkey[i]);
            if (it != orders_idx.end()) {
                size_t o_idx = it->second;
                Q3GroupKey key{
                    lineitem.l_orderkey[i],
                    orders.o_orderdate[o_idx],
                    orders.o_shippriority[o_idx]
                };
                double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);
                groups[key] += revenue;
            }
        }
    }

    // Convert to vector and use partial_sort for top 10
    std::vector<Q3Result> results;
    results.reserve(groups.size());
    for (const auto& [key, revenue] : groups) {
        results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    }

    // Use partial_sort to find top 10 results efficiently
    // Only partially sorts the first 10 elements, O(n log 10) instead of O(n log n)
    size_t top_k = std::min(results.size(), static_cast<size_t>(10));
    if (top_k > 0) {
        std::partial_sort(results.begin(), results.begin() + top_k, results.end());
        results.resize(top_k);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Print results
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::left
              << std::setw(15) << "l_orderkey"
              << std::setw(18) << "revenue"
              << std::setw(15) << "o_orderdate"
              << std::setw(18) << "o_shippriority"
              << "\n";

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& result : results) {
        std::cout << std::left
                  << std::setw(15) << result.l_orderkey
                  << std::setw(18) << result.revenue
                  << std::setw(15) << days_to_date_str(result.o_orderdate)
                  << std::setw(18) << result.o_shippriority
                  << "\n";
    }

    std::cout << "Execution time: " << duration << " ms\n";
}
