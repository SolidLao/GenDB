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
#include <thread>
#include <mutex>

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

    const size_t num_threads = std::thread::hardware_concurrency();

    // Step 1: Filter customer by c_mktsegment = 'BUILDING' in parallel
    std::vector<std::vector<int32_t>> thread_custkeys(num_threads);
    std::vector<std::vector<bool>> thread_flags(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    const size_t customer_chunk_size = (customer.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * customer_chunk_size;
            const size_t end_idx = std::min(start_idx + customer_chunk_size, customer.size());
            
            auto& local_custkeys = thread_custkeys[t];
            auto& local_flags = thread_flags[t];
            local_custkeys.reserve((end_idx - start_idx) / 5); // Estimate 20% selectivity
            local_flags.reserve((end_idx - start_idx) / 5);

            for (size_t i = start_idx; i < end_idx; ++i) {
                if (customer.c_mktsegment[i] == "BUILDING") {
                    local_custkeys.push_back(customer.c_custkey[i]);
                    local_flags.push_back(true);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge customer results
    std::vector<int32_t> filtered_custkeys;
    std::vector<bool> filtered_flags;
    size_t total_customers = 0;
    for (const auto& vec : thread_custkeys) {
        total_customers += vec.size();
    }
    filtered_custkeys.reserve(total_customers);
    filtered_flags.reserve(total_customers);

    for (size_t t = 0; t < num_threads; ++t) {
        filtered_custkeys.insert(filtered_custkeys.end(), 
                                thread_custkeys[t].begin(), 
                                thread_custkeys[t].end());
        filtered_flags.insert(filtered_flags.end(), 
                            thread_flags[t].begin(), 
                            thread_flags[t].end());
    }

    // Join 1: customer ⋈ orders (on custkey)
    // Build on filtered customers (smaller side)
    UniqueHashJoin<int32_t, bool, size_t> customer_join;
    customer_join.build(filtered_custkeys, filtered_flags);

    // Probe with orders in parallel, filtering by orderdate
    std::vector<std::vector<int32_t>> thread_orders_custkeys(num_threads);
    std::vector<std::vector<size_t>> thread_orders_indices(num_threads);

    const size_t orders_chunk_size = (orders.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * orders_chunk_size;
            const size_t end_idx = std::min(start_idx + orders_chunk_size, orders.size());

            auto& local_custkeys = thread_orders_custkeys[t];
            auto& local_indices = thread_orders_indices[t];
            local_custkeys.reserve(end_idx - start_idx);
            local_indices.reserve(end_idx - start_idx);

            for (size_t i = start_idx; i < end_idx; ++i) {
                local_custkeys.push_back(orders.o_custkey[i]);
                local_indices.push_back(i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge orders results
    std::vector<int32_t> orders_custkeys;
    std::vector<size_t> orders_indices;
    size_t total_orders = 0;
    for (const auto& vec : thread_orders_custkeys) {
        total_orders += vec.size();
    }
    orders_custkeys.reserve(total_orders);
    orders_indices.reserve(total_orders);

    for (size_t t = 0; t < num_threads; ++t) {
        orders_custkeys.insert(orders_custkeys.end(),
                              thread_orders_custkeys[t].begin(),
                              thread_orders_custkeys[t].end());
        orders_indices.insert(orders_indices.end(),
                            thread_orders_indices[t].begin(),
                            thread_orders_indices[t].end());
    }

    auto join1_result = customer_join.probe_filtered_parallel(
        orders_custkeys,
        orders_indices,
        [&orders, order_cutoff](size_t i) {
            return orders.o_orderdate[i] < order_cutoff;
        },
        num_threads
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

    // Probe with lineitem in parallel, filtering by shipdate
    std::vector<std::vector<int32_t>> thread_lineitem_orderkeys(num_threads);
    std::vector<std::vector<size_t>> thread_lineitem_indices(num_threads);

    const size_t lineitem_chunk_size = (lineitem.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * lineitem_chunk_size;
            const size_t end_idx = std::min(start_idx + lineitem_chunk_size, lineitem.size());

            auto& local_orderkeys = thread_lineitem_orderkeys[t];
            auto& local_indices = thread_lineitem_indices[t];
            local_orderkeys.reserve(end_idx - start_idx);
            local_indices.reserve(end_idx - start_idx);

            for (size_t i = start_idx; i < end_idx; ++i) {
                local_orderkeys.push_back(lineitem.l_orderkey[i]);
                local_indices.push_back(i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge lineitem results
    std::vector<int32_t> lineitem_orderkeys;
    std::vector<size_t> lineitem_indices;
    size_t total_lineitem = 0;
    for (const auto& vec : thread_lineitem_orderkeys) {
        total_lineitem += vec.size();
    }
    lineitem_orderkeys.reserve(total_lineitem);
    lineitem_indices.reserve(total_lineitem);

    for (size_t t = 0; t < num_threads; ++t) {
        lineitem_orderkeys.insert(lineitem_orderkeys.end(),
                                 thread_lineitem_orderkeys[t].begin(),
                                 thread_lineitem_orderkeys[t].end());
        lineitem_indices.insert(lineitem_indices.end(),
                              thread_lineitem_indices[t].begin(),
                              thread_lineitem_indices[t].end());
    }

    auto join2_result = lineitem_join.probe_filtered_parallel(
        lineitem_orderkeys,
        lineitem_indices,
        [&lineitem, ship_cutoff](size_t i) {
            return lineitem.l_shipdate[i] > ship_cutoff;
        },
        num_threads
    );

    // Step 3: Aggregate by (l_orderkey, o_orderdate, o_shippriority) in parallel
    std::vector<Q3AggTable> thread_agg_tables(num_threads);
    const size_t agg_chunk_size = (join2_result.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * agg_chunk_size;
            const size_t end_idx = std::min(start_idx + agg_chunk_size, join2_result.size());

            auto& local_agg = thread_agg_tables[t];
            local_agg.reserve((end_idx - start_idx) / 4);

            for (size_t i = start_idx; i < end_idx; ++i) {
                int32_t orderkey = join2_result.keys[i];
                const OrderInfo& order_info = join2_result.build_values[i];
                size_t lineitem_idx = join2_result.probe_values[i];

                Q3GroupKey key{orderkey, order_info.orderdate, order_info.shippriority};
                double revenue = lineitem.l_extendedprice[lineitem_idx] *
                                (1.0 - lineitem.l_discount[lineitem_idx]);
                local_agg[key] += revenue;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge aggregation results
    Q3AggTable agg_table;
    size_t total_groups = 0;
    for (const auto& local_agg : thread_agg_tables) {
        total_groups += local_agg.size();
    }
    agg_table.reserve(total_groups);

    for (const auto& local_agg : thread_agg_tables) {
        for (const auto& [key, revenue] : local_agg) {
            agg_table[key] += revenue;
        }
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
