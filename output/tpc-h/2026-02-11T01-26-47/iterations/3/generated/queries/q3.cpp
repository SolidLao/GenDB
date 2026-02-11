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
    // Direct hash probe without intermediate vectors - eliminates 2GB+ of data copies
    // Use zone maps to skip blocks where max(o_orderdate) < order_cutoff
    struct OrderMatch {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<std::vector<OrderMatch>> thread_order_matches(num_threads);
    const size_t orders_chunk_size = (orders.size() + num_threads - 1) / num_threads;

    // Pre-compute which blocks to skip based on zone maps
    std::vector<bool> skip_orders_block(orders.o_orderdate_zone_maps.size(), false);
    size_t skipped_orders_blocks = 0;
    for (size_t b = 0; b < orders.o_orderdate_zone_maps.size(); ++b) {
        const auto& zm = orders.o_orderdate_zone_maps[b];
        // Skip block if min(o_orderdate) >= order_cutoff (we want o_orderdate < cutoff)
        if (zm.min_value >= order_cutoff) {
            skip_orders_block[b] = true;
            skipped_orders_blocks++;
        }
    }

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * orders_chunk_size;
            const size_t end_idx = std::min(start_idx + orders_chunk_size, orders.size());

            auto& local_matches = thread_order_matches[t];
            local_matches.reserve((end_idx - start_idx) / 10);  // Estimate 10% selectivity

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Check zone map - skip entire block if possible
                if (!orders.o_orderdate_zone_maps.empty()) {
                    size_t block_id = i / 32768;  // Match block size used in storage
                    if (block_id < skip_orders_block.size() && skip_orders_block[block_id]) {
                        // Skip to next block
                        i = (block_id + 1) * 32768 - 1;
                        continue;
                    }
                }

                // Apply date filter first
                if (orders.o_orderdate[i] < order_cutoff) {
                    // Probe hash join directly
                    const bool* match = customer_join.find(orders.o_custkey[i]);
                    if (match) {
                        local_matches.push_back({
                            orders.o_orderkey[i],
                            orders.o_orderdate[i],
                            orders.o_shippriority[i]
                        });
                    }
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge only the matched orders (much smaller than full table)
    std::vector<OrderMatch> order_matches;
    size_t total_matches = 0;
    for (const auto& vec : thread_order_matches) {
        total_matches += vec.size();
    }
    order_matches.reserve(total_matches);

    for (size_t t = 0; t < num_threads; ++t) {
        order_matches.insert(order_matches.end(),
                           thread_order_matches[t].begin(),
                           thread_order_matches[t].end());
    }

    // Build hash join for lineitem on matched orderkeys
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<int32_t> joined_orderkeys;
    std::vector<OrderInfo> joined_order_infos;

    joined_orderkeys.reserve(order_matches.size());
    joined_order_infos.reserve(order_matches.size());

    for (const auto& match : order_matches) {
        joined_orderkeys.push_back(match.orderkey);
        joined_order_infos.push_back({match.orderdate, match.shippriority});
    }

    // Join 2: (customer ⋈ orders) ⋈ lineitem (on orderkey)
    // Build on orders (smaller side after filtering)
    UniqueHashJoin<int32_t, OrderInfo, size_t> lineitem_join;
    lineitem_join.build(joined_orderkeys, joined_order_infos);

    // Probe with lineitem in parallel, filtering by shipdate
    // Direct hash probe without intermediate vectors
    // Use zone maps to skip blocks where max(l_shipdate) <= ship_cutoff
    struct LineitemMatch {
        int32_t orderkey;
        OrderInfo order_info;
        size_t lineitem_idx;
    };

    std::vector<std::vector<LineitemMatch>> thread_lineitem_matches(num_threads);
    const size_t lineitem_chunk_size = (lineitem.size() + num_threads - 1) / num_threads;

    // Pre-compute which blocks to skip based on zone maps
    std::vector<bool> skip_lineitem_block(lineitem.l_shipdate_zone_maps.size(), false);
    size_t skipped_lineitem_blocks = 0;
    for (size_t b = 0; b < lineitem.l_shipdate_zone_maps.size(); ++b) {
        const auto& zm = lineitem.l_shipdate_zone_maps[b];
        // Skip block if max(l_shipdate) <= ship_cutoff (we want l_shipdate > cutoff)
        if (zm.max_value <= ship_cutoff) {
            skip_lineitem_block[b] = true;
            skipped_lineitem_blocks++;
        }
    }

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * lineitem_chunk_size;
            const size_t end_idx = std::min(start_idx + lineitem_chunk_size, lineitem.size());

            auto& local_matches = thread_lineitem_matches[t];
            local_matches.reserve((end_idx - start_idx) / 50);  // Estimate 2% selectivity

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Check zone map - skip entire block if possible
                if (!lineitem.l_shipdate_zone_maps.empty()) {
                    size_t block_id = i / 65536;  // Match block size used in storage
                    if (block_id < skip_lineitem_block.size() && skip_lineitem_block[block_id]) {
                        // Skip to next block
                        i = (block_id + 1) * 65536 - 1;
                        continue;
                    }
                }

                // Apply shipdate filter first
                if (lineitem.l_shipdate[i] > ship_cutoff) {
                    // Probe hash join directly
                    const OrderInfo* match = lineitem_join.find(lineitem.l_orderkey[i]);
                    if (match) {
                        local_matches.push_back({
                            lineitem.l_orderkey[i],
                            *match,
                            i
                        });
                    }
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    // Merge only the matched lineitems (much smaller than full table)
    std::vector<LineitemMatch> lineitem_matches;
    size_t total_lineitem_matches = 0;
    for (const auto& vec : thread_lineitem_matches) {
        total_lineitem_matches += vec.size();
    }
    lineitem_matches.reserve(total_lineitem_matches);

    for (size_t t = 0; t < num_threads; ++t) {
        lineitem_matches.insert(lineitem_matches.end(),
                               thread_lineitem_matches[t].begin(),
                               thread_lineitem_matches[t].end());
    }

    // Step 3: Aggregate by (l_orderkey, o_orderdate, o_shippriority) in parallel
    std::vector<Q3AggTable> thread_agg_tables(num_threads);
    const size_t agg_chunk_size = (lineitem_matches.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            const size_t start_idx = t * agg_chunk_size;
            const size_t end_idx = std::min(start_idx + agg_chunk_size, lineitem_matches.size());

            auto& local_agg = thread_agg_tables[t];
            local_agg.reserve((end_idx - start_idx) / 4);

            for (size_t i = start_idx; i < end_idx; ++i) {
                const auto& match = lineitem_matches[i];
                Q3GroupKey key{match.orderkey, match.order_info.orderdate, match.order_info.shippriority};
                double revenue = lineitem.l_extendedprice[match.lineitem_idx] *
                                (1.0 - lineitem.l_discount[match.lineitem_idx]);
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

    // Use partial_sort for top-10 selection (O(n log k) instead of O(n log n))
    if (results.size() > 10) {
        std::partial_sort(results.begin(), results.begin() + 10, results.end(),
                         [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue)
                return a.revenue > b.revenue; // DESC
            return a.o_orderdate < b.o_orderdate; // ASC
        });
        results.resize(10);
    } else {
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue)
                return a.revenue > b.revenue; // DESC
            return a.o_orderdate < b.o_orderdate; // ASC
        });
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
