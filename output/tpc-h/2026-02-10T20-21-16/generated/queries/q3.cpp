#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <queue>

namespace gendb {

// Q3: Shipping Priority
void execute_q3(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load tables with only needed columns
    CustomerTable customer;
    load_customer(gendb_dir, customer, {"c_custkey", "c_mktsegment"});

    OrdersTable orders;
    load_orders(gendb_dir, orders, {"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"});

    LineitemTable lineitem;
    load_lineitem(gendb_dir, lineitem, {"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"});

    // Filter dates
    int32_t order_date_cutoff = date_to_days(1995, 3, 15);
    int32_t ship_date_cutoff = date_to_days(1995, 3, 15);

    // Step 1: Filter customer (c_mktsegment = 'BUILDING')
    uint8_t building_code = customer.mktsegment_lookup["BUILDING"];
    std::unordered_map<int32_t, bool> valid_customers;
    for (size_t i = 0; i < customer.c_custkey.size(); i++) {
        if (customer.c_mktsegment[i] == building_code) {
            valid_customers[customer.c_custkey[i]] = true;
        }
    }

    // Step 2: Filter orders and join with customer
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> valid_orders; // orderkey -> (orderdate, shippriority)
    for (size_t i = 0; i < orders.o_orderkey.size(); i++) {
        if (orders.o_orderdate[i] < order_date_cutoff &&
            valid_customers.find(orders.o_custkey[i]) != valid_customers.end()) {
            valid_orders[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
        }
    }

    // Step 3: Filter lineitem and join with orders, aggregate
    std::unordered_map<Q3GroupKey, Q3AggState, Q3GroupKeyHash> groups;

    for (size_t i = 0; i < lineitem.l_orderkey.size(); i++) {
        if (lineitem.l_shipdate[i] > ship_date_cutoff) {
            auto it = valid_orders.find(lineitem.l_orderkey[i]);
            if (it != valid_orders.end()) {
                Q3GroupKey key{lineitem.l_orderkey[i], it->second.first, it->second.second};

                double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);

                auto& agg = groups[key];
                agg.revenue += revenue;
                agg.orderdate = it->second.first;
                agg.shippriority = it->second.second;
            }
        }
    }

    // Step 4: Sort by revenue DESC, orderdate and take top 10
    std::vector<std::pair<Q3GroupKey, Q3AggState>> results(groups.begin(), groups.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.second.revenue != b.second.revenue) {
            return a.second.revenue > b.second.revenue;  // DESC
        }
        return a.second.orderdate < b.second.orderdate;  // ASC
    });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print top 10 results
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "l_orderkey | revenue | o_orderdate | o_shippriority\n";
    std::cout << "-----------|---------|-------------|---------------\n";

    size_t limit = std::min(size_t(10), results.size());
    for (size_t i = 0; i < limit; i++) {
        const auto& [key, agg] = results[i];
        std::cout << std::setw(10) << key.orderkey << " | "
                  << std::setw(7) << agg.revenue << " | "
                  << std::setw(11) << days_to_date_str(agg.orderdate) << " | "
                  << std::setw(14) << agg.shippriority << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
