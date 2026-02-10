#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include "../utils/bloom_filter.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <queue>
#include <chrono>

void execute_q3(const Customer& customer, const Orders& orders, const LineItem& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date constants
    int32_t order_date_cutoff = date_utils::parse_date("1995-03-15");
    int32_t ship_date_cutoff = date_utils::parse_date("1995-03-15");

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    // Build hash set of qualifying customer keys (small build side)
    // Use dictionary-encoded comparison (integer equality, not string comparison)
    std::unordered_set<int32_t> qualifying_customers;
    qualifying_customers.reserve(350000);  // Right-sized based on actual cardinality (~300K)

    // Find the code for "BUILDING" by searching the dictionary
    // This avoids modifying the const dictionary
    uint8_t building_code = 255;  // Invalid code as default
    for (size_t dict_i = 0; dict_i < customer.c_mktsegment_dict.size(); ++dict_i) {
        if (customer.c_mktsegment_dict.decode(dict_i) == "BUILDING") {
            building_code = static_cast<uint8_t>(dict_i);
            break;
        }
    }

    for (size_t i = 0; i < customer.size(); ++i) {
        if (customer.c_mktsegment_code[i] == building_code) {
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
    qualifying_orders.reserve(350000);  // Right-sized: ~200-300K actual (was 1M, 3x reduction)

    for (size_t i = 0; i < orders.size(); ++i) {
        if (orders.o_orderdate[i] < order_date_cutoff &&
            qualifying_customers.count(orders.o_custkey[i]) > 0) {
            qualifying_orders[orders.o_orderkey[i]] = {
                orders.o_orderdate[i],
                orders.o_shippriority[i]
            };
        }
    }

    // Build bloom filter on qualifying_orders to skip most lineitem rows
    BloomFilter order_bloom(qualifying_orders.size(), 0.01);  // 1% false positive rate
    for (const auto& [orderkey, _] : qualifying_orders) {
        order_bloom.insert(orderkey);
    }

    // Step 3: Scan lineitem with l_shipdate > '1995-03-15' filter
    // Join with qualifying_orders and aggregate
    // Use hash aggregation first, then extract top-K
    std::unordered_map<Q3GroupKey, Q3Aggregate, Q3GroupKeyHash> aggregates;
    aggregates.reserve(250000);  // Right-sized: ~150-200K actual unique orderkeys (was 500K, 2x reduction)

    for (size_t i = 0; i < lineitem.size(); ++i) {
        if (lineitem.l_shipdate[i] > ship_date_cutoff) {
            int32_t orderkey = lineitem.l_orderkey[i];

            // Early rejection via bloom filter (eliminates ~88% of rows)
            if (!order_bloom.contains(orderkey)) {
                continue;
            }

            // Probe hash table only if bloom filter passed
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

    // Use top-K heap to avoid sorting all aggregates (300K groups)
    // We only need top 10, so use min-heap of size 10
    struct ResultRow {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Comparator for min-heap (smallest revenue at top)
    auto comp = [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;  // Min-heap on revenue
        return a.o_orderdate < b.o_orderdate;  // Tie-break by date
    };

    std::priority_queue<ResultRow, std::vector<ResultRow>, decltype(comp)> top_k(comp);
    const size_t K = 10;

    // Maintain heap of top K results
    for (const auto& [key, agg] : aggregates) {
        ResultRow row{key.l_orderkey, agg.revenue, key.o_orderdate, key.o_shippriority};

        if (top_k.size() < K) {
            top_k.push(row);
        } else if (row.revenue > top_k.top().revenue ||
                   (row.revenue == top_k.top().revenue && row.o_orderdate < top_k.top().o_orderdate)) {
            top_k.pop();
            top_k.push(row);
        }
    }

    // Extract results from heap
    std::vector<ResultRow> results;
    results.reserve(K);
    while (!top_k.empty()) {
        results.push_back(top_k.top());
        top_k.pop();
    }

    // Reverse to get descending order
    std::reverse(results.begin(), results.end());

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
