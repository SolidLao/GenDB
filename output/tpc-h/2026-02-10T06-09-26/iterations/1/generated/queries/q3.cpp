#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <vector>

namespace gendb {

struct Q3Result {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

void execute_q3(const CustomerTable& customer, const OrdersTable& orders, const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date filters
    int32_t orders_max_date = parse_date("1995-03-15");
    int32_t lineitem_min_date = parse_date("1995-03-15");

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    // Find the code for 'BUILDING' in the dictionary
    uint8_t building_code = 255; // Invalid default
    auto it = customer.c_mktsegment_lookup.find("BUILDING");
    if (it != customer.c_mktsegment_lookup.end()) {
        building_code = it->second;
    }

    std::unordered_set<int32_t> filtered_custkeys;
    for (size_t i = 0; i < customer.size(); i++) {
        if (customer.c_mktsegment_code[i] == building_code) {
            filtered_custkeys.insert(customer.c_custkey[i]);
        }
    }

    // Step 2: Filter orders and build hash index on o_orderkey
    // Also filter by o_orderdate < '1995-03-15' and join with filtered customers
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_info; // orderkey -> (orderdate, shippriority)
    order_info.reserve(1000000); // Pre-size for better performance

    // Use zone maps to skip blocks
    const auto& orders_zonemap = orders.orderdate_zonemap;
    size_t orders_block_size = orders_zonemap.block_size;
    size_t orders_num_blocks = orders_zonemap.block_min.size();

    for (size_t block = 0; block < orders_num_blocks; block++) {
        // Skip blocks where all dates are >= orders_max_date
        if (orders_zonemap.block_min[block] >= orders_max_date) {
            continue;
        }

        size_t start = block * orders_block_size;
        size_t end = std::min(start + orders_block_size, orders.size());

        for (size_t i = start; i < end; i++) {
            if (orders.o_orderdate[i] < orders_max_date &&
                filtered_custkeys.count(orders.o_custkey[i]) > 0) {
                order_info[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
            }
        }
    }

    // Step 3: Scan lineitem, filter by l_shipdate > '1995-03-15', join with orders, aggregate
    std::unordered_map<Q3GroupKey, double, Q3GroupKeyHash> revenue_map;
    revenue_map.reserve(100000); // Pre-size for better performance

    // Cache pointers for better performance
    const int32_t* l_shipdate = lineitem.l_shipdate.data();
    const int32_t* l_orderkey = lineitem.l_orderkey.data();
    const double* l_extendedprice = lineitem.l_extendedprice.data();
    const double* l_discount = lineitem.l_discount.data();

    // Use zone maps to skip blocks
    const auto& lineitem_zonemap = lineitem.shipdate_zonemap;
    size_t lineitem_block_size = lineitem_zonemap.block_size;
    size_t lineitem_num_blocks = lineitem_zonemap.block_min.size();

    for (size_t block = 0; block < lineitem_num_blocks; block++) {
        // Skip blocks where all dates are <= lineitem_min_date
        if (lineitem_zonemap.block_max[block] <= lineitem_min_date) {
            continue;
        }

        size_t start = block * lineitem_block_size;
        size_t end = std::min(start + lineitem_block_size, lineitem.size());

        for (size_t i = start; i < end; i++) {
            if (l_shipdate[i] > lineitem_min_date) {
                auto it = order_info.find(l_orderkey[i]);
                if (it != order_info.end()) {
                    Q3GroupKey key{l_orderkey[i], it->second.first, it->second.second};
                    double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                    revenue_map[key] += revenue;
                }
            }
        }
    }

    // Step 4: Convert to vector and sort by revenue DESC, orderdate ASC
    std::vector<Q3Result> results;
    results.reserve(revenue_map.size());
    for (const auto& [key, rev] : revenue_map) {
        results.push_back({key.orderkey, rev, key.orderdate, key.shippriority});
    }

    // Use partial_sort for Top-10 optimization (only sort what we need)
    size_t k = std::min(static_cast<size_t>(10), results.size());
    std::partial_sort(results.begin(), results.begin() + k, results.end(),
        [](const Q3Result& a, const Q3Result& b) {
            if (a.revenue != b.revenue)
                return a.revenue > b.revenue; // DESC
            return a.orderdate < b.orderdate; // ASC
        });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::right << std::setw(12) << "ORDERKEY"
              << std::setw(18) << "REVENUE"
              << std::setw(15) << "ORDERDATE"
              << std::setw(15) << "SHIPPRIORITY" << "\n";
    std::cout << std::string(60, '-') << "\n";

    for (const auto& r : results) {
        std::cout << std::right << std::setw(12) << r.orderkey
                  << std::fixed << std::setprecision(2) << std::setw(18) << r.revenue
                  << std::setw(15) << days_to_date_str(r.orderdate)
                  << std::setw(15) << r.shippriority << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
