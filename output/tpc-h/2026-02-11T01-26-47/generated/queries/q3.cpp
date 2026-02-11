#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include "../operators/hash_join.h"
#include "../operators/sort.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <unordered_map>

namespace gendb {

void execute_q3(const LineitemTable& lineitem, const OrdersTable& orders, const CustomerTable& customer) {
    auto start = std::chrono::high_resolution_clock::now();

    // Q3: Shipping Priority
    // Filters:
    // - c_mktsegment = 'BUILDING'
    // - o_orderdate < '1995-03-15'
    // - l_shipdate > '1995-03-15'

    int32_t order_cutoff = date_to_days(1995, 3, 15);
    int32_t ship_cutoff = date_to_days(1995, 3, 15);

    // Step 1: Filter customer by c_mktsegment = 'BUILDING' and build hash table
    std::vector<int32_t> filtered_custkeys;
    std::vector<bool> filtered_flags; // Dummy value for customer (we just need the key)

    filtered_custkeys.reserve(customer.size() / 5); // Estimate 20% selectivity
    filtered_flags.reserve(customer.size() / 5);

    for (size_t i = 0; i < customer.size(); ++i) {
        if (customer.c_mktsegment[i] == "BUILDING") {
            filtered_custkeys.push_back(customer.c_custkey[i]);
            filtered_flags.push_back(true);
        }
    }

    // Join 1: customer ⋈ orders (on custkey)
    // Build on filtered customers (smaller side)
    UniqueHashJoin<int32_t, bool, size_t> customer_join;
    customer_join.build(filtered_custkeys, filtered_flags);

    // Probe with orders, filtering by orderdate
    std::vector<int32_t> orders_custkeys;
    std::vector<size_t> orders_indices;

    orders_custkeys.reserve(orders.size());
    orders_indices.reserve(orders.size());

    for (size_t i = 0; i < orders.size(); ++i) {
        orders_custkeys.push_back(orders.o_custkey[i]);
        orders_indices.push_back(i);
    }

    auto join1_result = customer_join.probe_filtered(
        orders_custkeys,
        orders_indices,
        [&orders, order_cutoff](size_t i) {
            return orders.o_orderdate[i] < order_cutoff;
        }
    );

    // Extract joined order information
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<int32_t> joined_orderkeys;
    std::vector<OrderInfo> joined_order_infos;

    joined_orderkeys.reserve(join1_result.size());
    joined_order_infos.reserve(join1_result.size());

    for (size_t i = 0; i < join1_result.size(); ++i) {
        size_t order_idx = join1_result.probe_values[i];
        joined_orderkeys.push_back(orders.o_orderkey[order_idx]);
        joined_order_infos.push_back({
            orders.o_orderdate[order_idx],
            orders.o_shippriority[order_idx]
        });
    }

    // Join 2: (customer ⋈ orders) ⋈ lineitem (on orderkey)
    // Build on orders (smaller side after filtering)
    UniqueHashJoin<int32_t, OrderInfo, size_t> lineitem_join;
    lineitem_join.build(joined_orderkeys, joined_order_infos);

    // Probe with lineitem, filtering by shipdate
    std::vector<int32_t> lineitem_orderkeys;
    std::vector<size_t> lineitem_indices;

    lineitem_orderkeys.reserve(lineitem.size());
    lineitem_indices.reserve(lineitem.size());

    for (size_t i = 0; i < lineitem.size(); ++i) {
        lineitem_orderkeys.push_back(lineitem.l_orderkey[i]);
        lineitem_indices.push_back(i);
    }

    auto join2_result = lineitem_join.probe_filtered(
        lineitem_orderkeys,
        lineitem_indices,
        [&lineitem, ship_cutoff](size_t i) {
            return lineitem.l_shipdate[i] > ship_cutoff;
        }
    );

    // Step 3: Aggregate by (l_orderkey, o_orderdate, o_shippriority)
    Q3AggTable agg_table;
    agg_table.reserve(join2_result.size() / 4); // Estimate ~4 lineitems per order

    for (size_t i = 0; i < join2_result.size(); ++i) {
        int32_t orderkey = join2_result.keys[i];
        const OrderInfo& order_info = join2_result.build_values[i];
        size_t lineitem_idx = join2_result.probe_values[i];

        Q3GroupKey key{orderkey, order_info.orderdate, order_info.shippriority};
        double revenue = lineitem.l_extendedprice[lineitem_idx] *
                        (1.0 - lineitem.l_discount[lineitem_idx]);
        agg_table[key] += revenue;
    }

    // Step 4: Sort by revenue DESC, o_orderdate ASC, and take top 10
    struct ResultRow {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<ResultRow> results;
    results.reserve(agg_table.size());

    for (const auto& [key, revenue] : agg_table) {
        results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    }

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue)
            return a.revenue > b.revenue; // DESC
        return a.o_orderdate < b.o_orderdate; // ASC
    });

    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "[BASELINE DEBUG] join2_result size: " << join2_result.size() << std::endl;
    double baseline_total = 0.0;
    for (const auto& [key, revenue] : agg_table) {
        baseline_total += revenue;
    }
    std::cout << "[BASELINE DEBUG] total revenue: " << baseline_total << std::endl;
    std::cout << "[BASELINE DEBUG] agg_table size: " << agg_table.size() << std::endl;
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::left << std::setw(15) << "l_orderkey"
              << std::right << std::setw(18) << "revenue"
              << std::setw(15) << "o_orderdate"
              << std::setw(18) << "o_shippriority" << "\n";
    std::cout << std::string(66, '-') << "\n";

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& row : results) {
        std::cout << std::left << std::setw(15) << row.l_orderkey
                  << std::right << std::setw(18) << row.revenue
                  << std::setw(15) << days_to_date_str(row.o_orderdate)
                  << std::setw(18) << row.o_shippriority << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
    std::cout << "Rows returned: " << results.size() << "\n";
}

} // namespace gendb
