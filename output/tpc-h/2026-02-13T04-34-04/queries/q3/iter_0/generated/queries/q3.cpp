#include "parquet_reader.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <mutex>
#include <iomanip>
#include <cmath>

// Result structure for group-by aggregation
struct OrderGroup {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;

    OrderGroup() : l_orderkey(0), o_orderdate(0), o_shippriority(0), revenue(0.0) {}
};

// Kahan summation for numerical stability
inline void kahan_add(double& sum, double& compensation, double value) {
    double y = value - compensation;
    double t = sum + y;
    compensation = (t - sum) - y;
    sum = t;
}

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Filter customer table for c_mktsegment = 'BUILDING'
    // Read only necessary columns: c_custkey, c_mktsegment
    auto customer_table = read_parquet(
        parquet_dir + "/customer.parquet",
        {"c_custkey", "c_mktsegment"}
    );

    const int32_t* c_custkey = customer_table.column<int32_t>("c_custkey");
    const auto& c_mktsegment = customer_table.string_column("c_mktsegment");
    int64_t customer_rows = customer_table.num_rows;

    // Build hash set of qualifying customer keys (c_mktsegment = 'BUILDING')
    std::unordered_set<int32_t> building_customers;
    building_customers.reserve(customer_rows / 5);  // ~20% selectivity

    for (int64_t i = 0; i < customer_rows; i++) {
        // Trim and compare (CHAR(10) may have trailing spaces)
        std::string segment = c_mktsegment[i];
        size_t end = segment.find_last_not_of(" \t\n\r");
        if (end != std::string::npos) {
            segment = segment.substr(0, end + 1);
        }

        if (segment == "BUILDING") {
            building_customers.insert(c_custkey[i]);
        }
    }

    // Step 2: Filter orders table and join with customer
    // o_orderdate < '1995-03-15' AND o_custkey IN (building_customers)
    const int32_t date_cutoff = date_to_days(1995, 3, 15);

    auto orders_table = read_parquet(
        parquet_dir + "/orders.parquet",
        {"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"}
    );

    const int32_t* o_orderkey = orders_table.column<int32_t>("o_orderkey");
    const int32_t* o_custkey = orders_table.column<int32_t>("o_custkey");
    const int32_t* o_orderdate = orders_table.column<int32_t>("o_orderdate");
    const int32_t* o_shippriority = orders_table.column<int32_t>("o_shippriority");
    int64_t orders_rows = orders_table.num_rows;

    // Build hash map: o_orderkey -> (o_orderdate, o_shippriority)
    // Only for orders that pass filters
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, OrderInfo> qualifying_orders;
    qualifying_orders.reserve(orders_rows / 4);  // ~25% selectivity for date filter

    for (int64_t i = 0; i < orders_rows; i++) {
        // Combined filter: date AND customer membership
        if (o_orderdate[i] < date_cutoff &&
            building_customers.find(o_custkey[i]) != building_customers.end()) {
            qualifying_orders[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    }

    // Step 3: Scan lineitem and aggregate
    // l_shipdate > '1995-03-15' AND l_orderkey IN (qualifying_orders)
    auto lineitem_table = read_parquet(
        parquet_dir + "/lineitem.parquet",
        {"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"}
    );

    const int32_t* l_orderkey = lineitem_table.column<int32_t>("l_orderkey");
    const double* l_extendedprice = lineitem_table.column<double>("l_extendedprice");
    const double* l_discount = lineitem_table.column<double>("l_discount");
    const int32_t* l_shipdate = lineitem_table.column<int32_t>("l_shipdate");
    int64_t lineitem_rows = lineitem_table.num_rows;

    // Thread-parallel aggregation
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<int32_t, OrderGroup>> thread_maps(num_threads);

    // Reserve space in each thread's hash map
    for (auto& tmap : thread_maps) {
        tmap.reserve(100000);  // Estimate based on typical result size
    }

    auto process_chunk = [&](int thread_id, int64_t start, int64_t end) {
        auto& local_map = thread_maps[thread_id];

        for (int64_t i = start; i < end; i++) {
            // Filter: l_shipdate > '1995-03-15'
            if (l_shipdate[i] <= date_cutoff) continue;

            int32_t orderkey = l_orderkey[i];
            auto it = qualifying_orders.find(orderkey);
            if (it == qualifying_orders.end()) continue;

            // Compute revenue contribution
            double revenue_contrib = l_extendedprice[i] * (1.0 - l_discount[i]);

            // Aggregate by orderkey
            auto& group = local_map[orderkey];
            if (group.revenue == 0.0 && group.l_orderkey == 0) {
                // First time seeing this orderkey in this thread
                group.l_orderkey = orderkey;
                group.o_orderdate = it->second.orderdate;
                group.o_shippriority = it->second.shippriority;
                group.revenue = revenue_contrib;
            } else {
                group.revenue += revenue_contrib;
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    int64_t chunk_size = (lineitem_rows + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        int64_t start = t * chunk_size;
        int64_t end = std::min(start + chunk_size, lineitem_rows);
        if (start < end) {
            threads.emplace_back(process_chunk, t, start, end);
        }
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Merge thread-local results
    std::unordered_map<int32_t, OrderGroup> final_results;
    final_results.reserve(100000);

    for (const auto& tmap : thread_maps) {
        for (const auto& [orderkey, group] : tmap) {
            auto& final_group = final_results[orderkey];
            if (final_group.l_orderkey == 0) {
                final_group = group;
            } else {
                final_group.revenue += group.revenue;
            }
        }
    }

    // Convert to vector for sorting
    std::vector<OrderGroup> results;
    results.reserve(final_results.size());

    for (const auto& [orderkey, group] : final_results) {
        results.push_back(group);
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::partial_sort(
        results.begin(),
        results.begin() + std::min(static_cast<size_t>(10), results.size()),
        results.end(),
        [](const OrderGroup& a, const OrderGroup& b) {
            if (std::fabs(a.revenue - b.revenue) > 0.005) {
                return a.revenue > b.revenue;  // DESC
            }
            return a.o_orderdate < b.o_orderdate;  // ASC
        }
    );

    // Write output (top 10)
    std::ofstream out(results_dir + "/Q3.csv");
    out << std::fixed << std::setprecision(2);

    int output_count = std::min(static_cast<int>(results.size()), 10);
    for (int i = 0; i < output_count; i++) {
        const auto& r = results[i];
        out << r.l_orderkey << ","
            << r.revenue << ","
            << days_to_date_str(r.o_orderdate) << ","
            << r.o_shippriority << "\n";
    }
    out.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q3: " << output_count << " rows in " << duration.count() << " ms" << std::endl;
}
