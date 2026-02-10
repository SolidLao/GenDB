#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../utils/compact_hash.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <chrono>
#include <queue>

namespace gendb {

// Top-K min-heap comparator (min-heap on revenue for efficient top-K maintenance)
struct Q3Result {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;

    bool operator>(const Q3Result& other) const {
        // Min-heap: smaller revenue at top
        if (revenue != other.revenue) return revenue > other.revenue;
        return orderdate > other.orderdate;
    }
};

// Q3: Shipping Priority with zone maps, pre-sized hash tables, and top-K min-heap
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
    RobinHoodSet valid_customers;
    valid_customers.reserve(350000);  // Pre-size for ~300K expected customers at SF10 (20% of 1.5M)

    for (size_t i = 0; i < customer.row_count; i++) {
        if (customer.c_mktsegment[i] == building_code) {
            valid_customers.insert(customer.c_custkey[i]);
        }
    }

    // Step 2: Filter orders and join with customer
    RobinHoodMap<std::pair<int32_t, int32_t>> valid_orders; // orderkey -> (orderdate, shippriority)
    valid_orders.reserve(300000);  // Pre-size for expected filtered orders

    // Use zone maps to skip orders blocks that don't match date filter
    if (!orders.orderdate_zones.empty()) {
        for (const auto& zone : orders.orderdate_zones) {
            // Skip block if min_orderdate >= order_date_cutoff (all dates too late)
            if (zone.min_value >= order_date_cutoff) {
                continue;  // Skip entire block
            }

            // Scan this block
            size_t block_start = zone.row_offset;
            size_t block_end = block_start + zone.row_count;

            for (size_t i = block_start; i < block_end; i++) {
                if (orders.o_orderdate[i] < order_date_cutoff &&
                    valid_customers.contains(orders.o_custkey[i])) {
                    valid_orders[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
                }
            }
        }
    } else {
        // Fallback: no zone maps
        for (size_t i = 0; i < orders.row_count; i++) {
            if (orders.o_orderdate[i] < order_date_cutoff &&
                valid_customers.contains(orders.o_custkey[i])) {
                valid_orders[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
            }
        }
    }

    // Step 3: Filter lineitem and join with orders, aggregate with top-K min-heap
    RobinHoodMapGeneric<Q3GroupKey, Q3AggState, Q3GroupKeyHash> groups;
    groups.reserve(100000);  // Pre-size for expected group count

    // Use zone maps to skip lineitem blocks that don't match shipdate filter
    if (!lineitem.shipdate_zones.empty()) {
        for (const auto& zone : lineitem.shipdate_zones) {
            // Skip block if max_shipdate <= ship_date_cutoff (all dates too early)
            if (zone.max_value <= ship_date_cutoff) {
                continue;  // Skip entire block
            }

            // Scan this block
            size_t block_start = zone.row_offset;
            size_t block_end = block_start + zone.row_count;

            for (size_t i = block_start; i < block_end; i++) {
                if (lineitem.l_shipdate[i] > ship_date_cutoff) {
                    const auto* order_info = valid_orders.find(lineitem.l_orderkey[i]);
                    if (order_info != nullptr) {
                        Q3GroupKey key{lineitem.l_orderkey[i], order_info->first, order_info->second};

                        double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);

                        auto& agg = groups[key];
                        agg.revenue += revenue;
                        agg.orderdate = order_info->first;
                        agg.shippriority = order_info->second;
                    }
                }
            }
        }
    } else {
        // Fallback: no zone maps
        for (size_t i = 0; i < lineitem.row_count; i++) {
            if (lineitem.l_shipdate[i] > ship_date_cutoff) {
                const auto* order_info = valid_orders.find(lineitem.l_orderkey[i]);
                if (order_info != nullptr) {
                    Q3GroupKey key{lineitem.l_orderkey[i], order_info->first, order_info->second};

                    double revenue = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);

                    auto& agg = groups[key];
                    agg.revenue += revenue;
                    agg.orderdate = order_info->first;
                    agg.shippriority = order_info->second;
                }
            }
        }
    }

    // Step 4: Use top-K min-heap instead of full sort
    std::priority_queue<Q3Result, std::vector<Q3Result>, std::greater<Q3Result>> top_k;
    const size_t K = 10;

    for (const auto& [key, agg] : groups) {
        Q3Result result{key.orderkey, agg.revenue, agg.orderdate, agg.shippriority};

        if (top_k.size() < K) {
            top_k.push(result);
        } else if (result.revenue > top_k.top().revenue) {
            top_k.pop();
            top_k.push(result);
        }
    }

    // Extract top-K and sort for output
    std::vector<Q3Result> results;
    while (!top_k.empty()) {
        results.push_back(top_k.top());
        top_k.pop();
    }

    // Sort descending by revenue, ascending by orderdate
    std::sort(results.begin(), results.end(), [](const Q3Result& a, const Q3Result& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;  // DESC
        }
        return a.orderdate < b.orderdate;  // ASC
    });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print top 10 results
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "l_orderkey | revenue | o_orderdate | o_shippriority\n";
    std::cout << "-----------|---------|-------------|---------------\n";

    for (const auto& result : results) {
        std::cout << std::setw(10) << result.orderkey << " | "
                  << std::setw(7) << result.revenue << " | "
                  << std::setw(11) << days_to_date_str(result.orderdate) << " | "
                  << std::setw(14) << result.shippriority << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
