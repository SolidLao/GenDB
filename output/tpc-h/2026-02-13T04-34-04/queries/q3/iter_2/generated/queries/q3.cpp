#include "parquet_reader.h"
#include "open_hash_table.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <mutex>
#include <iomanip>
#include <cmath>
#include <unordered_map>

// Result structure for group-by aggregation
struct OrderGroup {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;

    OrderGroup() : l_orderkey(0), o_orderdate(0), o_shippriority(0), revenue(0.0) {}
    OrderGroup(int32_t key, int32_t date, int32_t priority, double rev)
        : l_orderkey(key), o_orderdate(date), o_shippriority(priority), revenue(rev) {}
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
    OpenHashSet building_customers(customer_rows / 5);  // ~20% selectivity

    for (int64_t i = 0; i < customer_rows; i++) {
        // Fast-path: check if first 8 bytes match "BUILDING" prefix
        const std::string& segment = c_mktsegment[i];
        if (segment.size() >= 8 && memcmp(segment.data(), "BUILDING", 8) == 0) {
            // Only insert if exact match (may have trailing spaces but first 8 bytes match)
            building_customers.insert(c_custkey[i]);
        }
    }

    // Step 2: Filter orders table and join with customer
    // o_orderdate < '1995-03-15' AND o_custkey IN (building_customers)
    const int32_t date_cutoff = date_to_days(1995, 3, 15);

    // Row group pruning for orders: skip row groups where min(o_orderdate) >= date_cutoff
    // Filter is: o_orderdate < date_cutoff, so skip if all dates are >= date_cutoff
    auto orders_stats = get_row_group_stats(parquet_dir + "/orders.parquet", "o_orderdate");
    std::vector<int> orders_row_groups;
    for (const auto& s : orders_stats) {
        if (s.has_min_max && s.min_int >= date_cutoff) {
            // Skip: all dates in this row group are >= date_cutoff (don't match < filter)
            continue;
        }
        orders_row_groups.push_back(s.row_group_index);
    }

    auto orders_table = read_parquet_row_groups(
        parquet_dir + "/orders.parquet",
        {"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"},
        orders_row_groups
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
    OpenHashMap<OrderInfo> qualifying_orders(orders_rows / 4);  // ~25% selectivity for date filter

    for (int64_t i = 0; i < orders_rows; i++) {
        // Combined filter: date AND customer membership
        if (o_orderdate[i] < date_cutoff &&
            building_customers.find(o_custkey[i])) {
            qualifying_orders[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    }

    // Step 3: Scan lineitem and aggregate
    // l_shipdate > '1995-03-15' AND l_orderkey IN (qualifying_orders)

    // Row group pruning for lineitem: skip row groups where max(l_shipdate) <= date_cutoff
    // Filter is: l_shipdate > date_cutoff, so skip if all dates are <= date_cutoff
    auto lineitem_stats = get_row_group_stats(parquet_dir + "/lineitem.parquet", "l_shipdate");
    std::vector<int> lineitem_row_groups;
    for (const auto& s : lineitem_stats) {
        if (s.has_min_max && s.max_int <= date_cutoff) {
            // Skip: all dates in this row group are <= date_cutoff (don't match > filter)
            continue;
        }
        lineitem_row_groups.push_back(s.row_group_index);
    }

    auto lineitem_table = read_parquet_row_groups(
        parquet_dir + "/lineitem.parquet",
        {"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"},
        lineitem_row_groups
    );

    const int32_t* l_orderkey = lineitem_table.column<int32_t>("l_orderkey");
    const double* l_extendedprice = lineitem_table.column<double>("l_extendedprice");
    const double* l_discount = lineitem_table.column<double>("l_discount");
    const int32_t* l_shipdate = lineitem_table.column<int32_t>("l_shipdate");
    int64_t lineitem_rows = lineitem_table.num_rows;

    // Thread-parallel aggregation with vector-based approach for top-k optimization
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::vector<OrderGroup>> thread_vectors;
    thread_vectors.reserve(num_threads);

    // Reserve space in each thread's vector (~2000 capacity per guidance)
    for (int i = 0; i < num_threads; i++) {
        thread_vectors.emplace_back();
        thread_vectors.back().reserve(2000);
    }

    auto process_chunk = [&](int thread_id, int64_t start, int64_t end) {
        auto& local_vec = thread_vectors[thread_id];

        for (int64_t i = start; i < end; i++) {
            // Filter: l_shipdate > '1995-03-15'
            if (l_shipdate[i] <= date_cutoff) continue;

            int32_t orderkey = l_orderkey[i];
            const OrderInfo* order_info = qualifying_orders.find(orderkey);
            if (order_info == nullptr) continue;

            // Compute revenue contribution
            double revenue_contrib = l_extendedprice[i] * (1.0 - l_discount[i]);

            // Linear search in vector for existing orderkey (efficient for small result sets)
            OrderGroup* found = nullptr;
            for (auto& group : local_vec) {
                if (group.l_orderkey == orderkey) {
                    found = &group;
                    break;
                }
            }

            if (found) {
                found->revenue += revenue_contrib;
            } else {
                // New orderkey for this thread
                local_vec.push_back({
                    orderkey,
                    order_info->orderdate,
                    order_info->shippriority,
                    revenue_contrib
                });
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

    // Merge thread-local results using hash map (handles duplicate orderkeys across threads)
    std::unordered_map<int32_t, OrderGroup> merged_map;
    for (auto& vec : thread_vectors) {
        for (auto& group : vec) {
            auto it = merged_map.find(group.l_orderkey);
            if (it != merged_map.end()) {
                it->second.revenue += group.revenue;
            } else {
                merged_map[group.l_orderkey] = group;
            }
        }
    }

    // Convert to vector for sorting
    std::vector<OrderGroup> results;
    results.reserve(merged_map.size());
    for (auto& [key, group] : merged_map) {
        results.push_back(group);
    }

    // Sort by revenue DESC, o_orderdate ASC (partial sort for top 10)
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

    // CSV header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

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
