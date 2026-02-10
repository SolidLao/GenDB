#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <chrono>

void execute_q3(const Customer& customer, const Orders& orders, const LineItem& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date constants
    int32_t order_date_cutoff = date_utils::parse_date("1995-03-15");
    int32_t ship_date_cutoff = date_utils::parse_date("1995-03-15");

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    // Build hash set of qualifying customer keys (small build side)
    std::unordered_set<int32_t> qualifying_customers;
    for (size_t i = 0; i < customer.size(); ++i) {
        if (customer.c_mktsegment[i] == "BUILDING") {
            qualifying_customers.insert(customer.c_custkey[i]);
        }
    }

    // Step 2: Filter orders by o_orderdate < '1995-03-15' AND join with qualifying customers
    // Build hash index: o_orderkey -> (o_orderdate, o_shippriority)
    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };
    std::unordered_map<int32_t, OrderInfo> qualifying_orders;

    for (size_t i = 0; i < orders.size(); ++i) {
        if (orders.o_orderdate[i] < order_date_cutoff &&
            qualifying_customers.count(orders.o_custkey[i]) > 0) {
            qualifying_orders[orders.o_orderkey[i]] = {
                orders.o_orderdate[i],
                orders.o_shippriority[i]
            };
        }
    }

    // Step 3: Scan lineitem with l_shipdate > '1995-03-15' filter
    // Join with qualifying_orders and aggregate
    std::unordered_map<Q3GroupKey, Q3Aggregate, Q3GroupKeyHash> aggregates;

    for (size_t i = 0; i < lineitem.size(); ++i) {
        if (lineitem.l_shipdate[i] > ship_date_cutoff) {
            int32_t orderkey = lineitem.l_orderkey[i];

            // Probe hash table
            auto it = qualifying_orders.find(orderkey);
            if (it != qualifying_orders.end()) {
                Q3GroupKey key{
                    orderkey,
                    it->second.o_orderdate,
                    it->second.o_shippriority
                };

                double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);
                aggregates[key].revenue += revenue;
            }
        }
    }

    // Convert to vector for sorting
    struct ResultRow {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<ResultRow> results;
    results.reserve(aggregates.size());

    for (const auto& [key, agg] : aggregates) {
        results.push_back({key.l_orderkey, agg.revenue, key.o_orderdate, key.o_shippriority});
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;  // DESC
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q3: Shipping Priority ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "l_orderkey | revenue       | o_orderdate | o_shippriority" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    for (const auto& row : results) {
        std::cout << std::setw(10) << row.l_orderkey << " | "
                  << std::setw(13) << row.revenue << " | "
                  << date_utils::days_to_date_str(row.o_orderdate) << " | "
                  << std::setw(14) << row.o_shippriority << std::endl;
    }

    std::cout << "\nQ3 execution time: " << duration.count() << " ms" << std::endl;
}
