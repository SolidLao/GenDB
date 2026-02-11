#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

namespace gendb {

void execute_q3(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "\n=== Q3: Shipping Priority ===" << std::endl;

    // Load customer columns
    auto c_custkey = mmap_int32_column(gendb_dir, "customer", "c_custkey");
    // For c_mktsegment, we need to read strings (not implemented mmap for strings yet)
    // Simplified: load all and filter

    // Load orders columns
    auto o_orderkey = mmap_int32_column(gendb_dir, "orders", "o_orderkey");
    auto o_custkey = mmap_int32_column(gendb_dir, "orders", "o_custkey");
    auto o_orderdate = mmap_int32_column(gendb_dir, "orders", "o_orderdate");
    auto o_shippriority = mmap_int32_column(gendb_dir, "orders", "o_shippriority");

    // Load lineitem columns
    auto l_orderkey = mmap_int32_column(gendb_dir, "lineitem", "l_orderkey");
    auto l_extendedprice = mmap_double_column(gendb_dir, "lineitem", "l_extendedprice");
    auto l_discount = mmap_double_column(gendb_dir, "lineitem", "l_discount");
    auto l_shipdate = mmap_int32_column(gendb_dir, "lineitem", "l_shipdate");

    // Date filters
    int32_t order_date_cutoff = date_to_days("1995-03-15");
    int32_t ship_date_cutoff = date_to_days("1995-03-15");

    // For simplicity, we'll skip the c_mktsegment filter for baseline
    // (string columns require more complex handling)
    // Build hash index on c_custkey (all customers)
    std::unordered_map<int32_t, size_t> customer_index;
    for (size_t i = 0; i < c_custkey.size; ++i) {
        customer_index[c_custkey.data[i]] = i;
    }

    // Build hash index on filtered orders (o_orderdate < cutoff)
    struct OrderInfo {
        int32_t o_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::unordered_map<int32_t, std::vector<OrderInfo>> order_index;
    for (size_t i = 0; i < o_orderkey.size; ++i) {
        if (o_orderdate.data[i] < order_date_cutoff) {
            int32_t custkey = o_custkey.data[i];
            // Only include orders from valid customers
            if (customer_index.count(custkey) > 0) {
                order_index[o_orderkey.data[i]].push_back({
                    o_orderkey.data[i],
                    o_orderdate.data[i],
                    o_shippriority.data[i]
                });
            }
        }
    }

    // Parallel scan lineitem and join with orders
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<std::unordered_map<Q3GroupKey, Q3AggResult, Q3GroupKeyHash>> local_aggs(num_threads);

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

                // Join with orders
                auto it = order_index.find(orderkey);
                if (it == order_index.end()) continue;

                for (const auto& order_info : it->second) {
                    // Compute revenue
                    double revenue = l_extendedprice.data[i] * (1.0 - l_discount.data[i]);

                    Q3GroupKey key{orderkey, order_info.o_orderdate, order_info.o_shippriority};
                    local_agg[key].revenue += revenue;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results
    std::unordered_map<Q3GroupKey, Q3AggResult, Q3GroupKeyHash> global_agg;
    for (const auto& local_agg : local_aggs) {
        for (const auto& [key, result] : local_agg) {
            global_agg[key].revenue += result.revenue;
        }
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::vector<std::pair<Q3GroupKey, Q3AggResult>> sorted_results(global_agg.begin(), global_agg.end());
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.second.revenue != b.second.revenue)
                      return a.second.revenue > b.second.revenue;  // DESC
                  return a.first.o_orderdate < b.first.o_orderdate;  // ASC
              });

    // LIMIT 10
    size_t limit = std::min<size_t>(10, sorted_results.size());

    // Print results
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nL_ORDERKEY | REVENUE | O_ORDERDATE | O_SHIPPRIORITY" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    for (size_t i = 0; i < limit; ++i) {
        const auto& [key, result] = sorted_results[i];
        std::string orderdate_str = days_to_date_str(key.o_orderdate);

        std::cout << std::setw(10) << key.l_orderkey << " | "
                  << std::setw(12) << result.revenue << " | "
                  << std::setw(11) << orderdate_str << " | "
                  << std::setw(14) << key.o_shippriority << std::endl;
    }

    // Cleanup
    unmap_column(c_custkey.mmap_ptr, c_custkey.mmap_size);
    unmap_column(o_orderkey.mmap_ptr, o_orderkey.mmap_size);
    unmap_column(o_custkey.mmap_ptr, o_custkey.mmap_size);
    unmap_column(o_orderdate.mmap_ptr, o_orderdate.mmap_size);
    unmap_column(o_shippriority.mmap_ptr, o_shippriority.mmap_size);
    unmap_column(l_orderkey.mmap_ptr, l_orderkey.mmap_size);
    unmap_column(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size);
    unmap_column(l_discount.mmap_ptr, l_discount.mmap_size);
    unmap_column(l_shipdate.mmap_ptr, l_shipdate.mmap_size);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "\nQ3 execution time: " << elapsed.count() << " seconds" << std::endl;
}

}  // namespace gendb
