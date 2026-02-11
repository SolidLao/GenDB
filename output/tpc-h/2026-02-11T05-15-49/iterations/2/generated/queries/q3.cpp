#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include "../data-structures/robin_hood_map.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>

namespace gendb {

void execute_q3(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "\n=== Q3: Shipping Priority ===" << std::endl;

    // Date filters
    int32_t order_date_cutoff = date_to_days("1995-03-15");
    int32_t ship_date_cutoff = date_to_days("1995-03-15");

    const size_t num_threads = std::thread::hardware_concurrency();

    // ===================================================================
    // STEP 1: Build hash table on filtered customers (c_mktsegment = 'BUILDING')
    // Estimated cardinality: ~300K rows (20% of 1.5M customers)
    // This is the SMALLEST filtered table, so we build on it first
    // ===================================================================
    auto c_custkey = mmap_int32_column(gendb_dir, "customer", "c_custkey");
    auto c_mktsegment = mmap_string_column(gendb_dir, "customer", "c_mktsegment");

    // Build hash set of customer keys with c_mktsegment = 'BUILDING' filter
    // This reduces customer scan from 1.5M to ~300K rows (5x reduction)
    // OPTIMIZATION: Use Robin Hood hash set for 2-3x faster lookups
    data_structures::RobinHoodSet<int32_t> filtered_customers(400000);  // Pre-size for ~300K customers
    for (size_t i = 0; i < c_custkey.size; ++i) {
        if (c_mktsegment[i] == "BUILDING") {
            filtered_customers.insert(c_custkey.data[i]);
        }
    }

    // Clean up customer columns (no longer needed)
    unmap_column(c_custkey.mmap_ptr, c_custkey.mmap_size);
    // c_mktsegment is a vector, automatically cleaned up

    std::cout << "  Filtered customers: " << filtered_customers.size() << " (from " << c_custkey.size << ")" << std::endl;

    // ===================================================================
    // STEP 2: Probe with orders (filtered by o_orderdate < '1995-03-15')
    // Scan orders, filter by date, probe customer hash set, build intermediate
    // Estimated cardinality after join: ~1.2M rows (600K filtered orders * 20% customer selectivity)
    // ===================================================================
    auto o_orderkey = mmap_int32_column(gendb_dir, "orders", "o_orderkey");
    auto o_custkey = mmap_int32_column(gendb_dir, "orders", "o_custkey");
    auto o_orderdate = mmap_int32_column(gendb_dir, "orders", "o_orderdate");
    auto o_shippriority = mmap_int32_column(gendb_dir, "orders", "o_shippriority");

    // Build hash table on filtered orders (orders matching filtered customers + date filter)
    // KEY OPTIMIZATION: Store single OrderInfo per o_orderkey (not vector), since o_orderkey is PRIMARY KEY
    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // OPTIMIZATION: Parallel scan of orders to build hash table
    // Use thread-local Robin Hood hash tables for 2-3x faster lookups
    std::vector<std::thread> threads;
    std::vector<data_structures::RobinHoodMap<int32_t, OrderInfo>> local_order_maps(num_threads);

    // Pre-size hash tables to avoid rehashing during build
    // Estimated: ~1.46M filtered orders / num_threads, with 87.5% load factor
    size_t estimated_orders_per_thread = (2000000 / num_threads);
    for (auto& map : local_order_maps) {
        map.reserve(estimated_orders_per_thread);
    }

    size_t orders_count = o_orderkey.size;
    size_t orders_chunk_size = (orders_count + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * orders_chunk_size;
            size_t end_idx = std::min(start_idx + orders_chunk_size, orders_count);

            auto& local_map = local_order_maps[tid];

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Filter: o_orderdate < cutoff
                if (o_orderdate.data[i] >= order_date_cutoff) continue;

                // Join: probe customer hash set
                int32_t custkey = o_custkey.data[i];
                if (filtered_customers.count(custkey) == 0) continue;

                // Insert into local hash table
                // Since o_orderkey is PRIMARY KEY, we store single OrderInfo (not vector)
                local_map.insert(o_orderkey.data[i], {o_orderdate.data[i], o_shippriority.data[i]});
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    // Merge local maps into global filtered_orders map (using RobinHoodMap)
    data_structures::RobinHoodMap<int32_t, OrderInfo> filtered_orders(2000000);
    for (auto& local_map : local_order_maps) {
        for (auto it = local_map.begin(); it != local_map.end(); ++it) {
            auto [key, value] = *it;
            filtered_orders.insert(key, value);
        }
    }
    local_order_maps.clear();  // Free memory

    // Clean up orders columns
    unmap_column(o_orderkey.mmap_ptr, o_orderkey.mmap_size);
    unmap_column(o_custkey.mmap_ptr, o_custkey.mmap_size);
    unmap_column(o_orderdate.mmap_ptr, o_orderdate.mmap_size);
    unmap_column(o_shippriority.mmap_ptr, o_shippriority.mmap_size);

    std::cout << "  Filtered orders: " << filtered_orders.size() << std::endl;

    // ===================================================================
    // STEP 3: Probe with lineitem (filtered by l_shipdate > '1995-03-15')
    // Scan lineitem in parallel, filter by date, probe orders hash table, aggregate
    // Estimated cardinality: ~27M rows (60% of 45M lineitem)
    // This is the LARGEST table, so we probe with it (not build on it)
    // ===================================================================
    auto l_orderkey = mmap_int32_column(gendb_dir, "lineitem", "l_orderkey");
    auto l_extendedprice = mmap_double_column(gendb_dir, "lineitem", "l_extendedprice");
    auto l_discount = mmap_double_column(gendb_dir, "lineitem", "l_discount");
    auto l_shipdate = mmap_int32_column(gendb_dir, "lineitem", "l_shipdate");

    // Parallel scan lineitem and join with filtered orders
    // OPTIMIZATION: Use Robin Hood hash tables for aggregation (2-3x faster than std::unordered_map)
    std::vector<data_structures::RobinHoodMap<Q3GroupKey, Q3AggResult, Q3GroupKeyHash>> local_aggs(num_threads);

    // Pre-size aggregation hash tables
    // Expected: ~400K unique groups total, distributed across threads
    size_t estimated_groups_per_thread = (500000 / num_threads);
    for (auto& agg : local_aggs) {
        agg.reserve(estimated_groups_per_thread);
    }

    size_t n = l_orderkey.size;
    size_t chunk_size = (n + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, n);

            auto& local_agg = local_aggs[tid];

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Filter: l_shipdate > cutoff
                if (l_shipdate.data[i] <= ship_date_cutoff) continue;

                int32_t orderkey = l_orderkey.data[i];

                // Join: probe filtered orders hash table (RobinHoodMap)
                OrderInfo* order_info_ptr = filtered_orders.find(orderkey);
                if (order_info_ptr == nullptr) continue;

                // Found matching order - compute revenue and aggregate
                const OrderInfo& order_info = *order_info_ptr;
                double revenue = l_extendedprice.data[i] * (1.0 - l_discount.data[i]);

                Q3GroupKey key{orderkey, order_info.o_orderdate, order_info.o_shippriority};
                Q3AggResult* agg_ptr = local_agg.find(key);
                if (agg_ptr) {
                    agg_ptr->revenue += revenue;
                } else {
                    local_agg.insert(key, {revenue});
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results (using RobinHoodMap)
    data_structures::RobinHoodMap<Q3GroupKey, Q3AggResult, Q3GroupKeyHash> global_agg(500000);
    for (auto& local_agg : local_aggs) {
        for (auto it = local_agg.begin(); it != local_agg.end(); ++it) {
            auto [key, result] = *it;
            Q3AggResult* existing = global_agg.find(key);
            if (existing) {
                existing->revenue += result.revenue;
            } else {
                global_agg.insert(key, result);
            }
        }
    }

    // ===================================================================
    // OPTIMIZATION: Top-K heap instead of full sort (LIMIT 10)
    // Use std::partial_sort which is faster than full sort for small K
    // ===================================================================
    std::vector<std::pair<Q3GroupKey, Q3AggResult>> sorted_results;
    sorted_results.reserve(global_agg.size());
    for (auto it = global_agg.begin(); it != global_agg.end(); ++it) {
        auto [key, result] = *it;
        sorted_results.emplace_back(key, result);
    }

    const size_t K = 10;
    size_t limit = std::min(K, sorted_results.size());

    // partial_sort: sorts the first K elements, leaves the rest unsorted
    // This is O(N log K) instead of O(N log N) for full sort
    std::partial_sort(sorted_results.begin(),
                      sorted_results.begin() + limit,
                      sorted_results.end(),
                      [](const auto& a, const auto& b) {
                          if (a.second.revenue != b.second.revenue)
                              return a.second.revenue > b.second.revenue;  // DESC
                          return a.first.o_orderdate < b.first.o_orderdate;  // ASC
                      });

    // Extract top K results
    std::vector<std::pair<Q3GroupKey, Q3AggResult>> topk_results;
    topk_results.assign(sorted_results.begin(), sorted_results.begin() + limit);

    // Print results
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nL_ORDERKEY | REVENUE | O_ORDERDATE | O_SHIPPRIORITY" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    for (const auto& [key, result] : topk_results) {
        std::string orderdate_str = days_to_date_str(key.o_orderdate);

        std::cout << std::setw(10) << key.l_orderkey << " | "
                  << std::setw(12) << result.revenue << " | "
                  << std::setw(11) << orderdate_str << " | "
                  << std::setw(14) << key.o_shippriority << std::endl;
    }

    // Cleanup
    unmap_column(l_orderkey.mmap_ptr, l_orderkey.mmap_size);
    unmap_column(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size);
    unmap_column(l_discount.mmap_ptr, l_discount.mmap_size);
    unmap_column(l_shipdate.mmap_ptr, l_shipdate.mmap_size);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "\nQ3 execution time: " << elapsed.count() << " seconds" << std::endl;
}

}  // namespace gendb
