#include "queries.h"
#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <queue>

namespace gendb {

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t c_count;
    auto c_custkey = ColumnReader::mmap_int32(gendb_dir + "/customer.c_custkey.bin", c_count);
    auto c_mktsegment = ColumnReader::mmap_uint8(gendb_dir + "/customer.c_mktsegment.bin", c_count);

    // Load customer dictionary
    auto mktsegment_dict = ColumnReader::read_dictionary(gendb_dir + "/customer.c_mktsegment.dict");

    // Filter customer: c_mktsegment = 'BUILDING'
    uint8_t building_code = 255;
    for (size_t i = 0; i < mktsegment_dict.size(); ++i) {
        if (mktsegment_dict.decode(i) == "BUILDING") {
            building_code = i;
            break;
        }
    }

    auto filtered_customers = scan_filter(c_mktsegment, c_count,
        [building_code](uint8_t seg) { return seg == building_code; });

    // Load orders columns
    size_t o_count;
    auto o_orderkey = ColumnReader::mmap_int32(gendb_dir + "/orders.o_orderkey.bin", o_count);
    auto o_custkey = ColumnReader::mmap_int32(gendb_dir + "/orders.o_custkey.bin", o_count);
    auto o_orderdate = ColumnReader::mmap_int32(gendb_dir + "/orders.o_orderdate.bin", o_count);
    auto o_shippriority = ColumnReader::mmap_int32(gendb_dir + "/orders.o_shippriority.bin", o_count);

    // Filter orders: o_orderdate < '1995-03-15'
    auto filtered_orders = scan_filter(o_orderdate, o_count,
        [](int32_t date) { return date < DATE_1995_03_15; });

    // Join customer -> orders
    HashJoin<int32_t> customer_join;
    customer_join.build(c_custkey, filtered_customers);
    auto cust_order_matches = customer_join.probe(o_custkey, filtered_orders);

    // Extract matching order keys
    std::vector<int32_t> matched_orderkeys;
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_info; // orderkey -> (orderdate, shippriority)

    matched_orderkeys.reserve(cust_order_matches.size());
    for (const auto& [c_row, o_row] : cust_order_matches) {
        int32_t orderkey = o_orderkey[o_row];
        matched_orderkeys.push_back(orderkey);
        order_info[orderkey] = std::make_pair(o_orderdate[o_row], o_shippriority[o_row]);
    }

    // Load lineitem columns
    size_t l_count;
    auto l_orderkey = ColumnReader::mmap_int32(gendb_dir + "/lineitem.l_orderkey.bin", l_count);
    auto l_shipdate = ColumnReader::mmap_int32(gendb_dir + "/lineitem.l_shipdate.bin", l_count);
    auto l_extendedprice = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_extendedprice.bin", l_count);
    auto l_discount = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_discount.bin", l_count);

    // Filter lineitem: l_shipdate > '1995-03-15'
    auto filtered_lineitems = scan_filter(l_shipdate, l_count,
        [](int32_t date) { return date > DATE_1995_03_15; });

    // Join orders -> lineitem
    HashJoin<int32_t> order_join;
    std::unordered_map<int32_t, size_t> orderkey_index;
    for (size_t i = 0; i < matched_orderkeys.size(); ++i) {
        orderkey_index[matched_orderkeys[i]] = i;
    }

    // Build with matched orderkeys
    std::vector<int32_t> build_keys(matched_orderkeys.begin(), matched_orderkeys.end());
    std::vector<size_t> build_rows(matched_orderkeys.size());
    std::iota(build_rows.begin(), build_rows.end(), 0);

    order_join.build(build_keys.data(), build_rows);

    // Parallel probe with per-thread local aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<int32_t, int64_t>> local_revenue(num_threads);

    // Use callback-based parallel probe to fuse probe + aggregate
    order_join.probe_parallel_with_callback(l_orderkey, filtered_lineitems,
        [&](size_t thread_id, size_t build_idx, size_t l_row) {
            int32_t orderkey = matched_orderkeys[build_idx];
            // FIX: Correct arithmetic scaling to match q1.cpp fix
            // Original: extendedprice * (100 - discount) / 100
            // Corrected: (extendedprice / 100) * (100 - discount / 100)
            // This preserves semantic equivalence with proper decimal scaling
            int64_t disc_price = (l_extendedprice[l_row] / 100) * (100 - l_discount[l_row] / 100);
            local_revenue[thread_id][orderkey] += disc_price;
        });

    // Merge local revenue aggregations
    std::unordered_map<int32_t, int64_t> revenue_by_order;
    for (const auto& local_rev : local_revenue) {
        for (const auto& [orderkey, revenue] : local_rev) {
            revenue_by_order[orderkey] += revenue;
        }
    }

    // Sort by revenue DESC, o_orderdate, and take top 10
    struct Result {
        int32_t orderkey;
        int64_t revenue;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<Result> results;
    results.reserve(revenue_by_order.size());

    for (const auto& [orderkey, revenue] : revenue_by_order) {
        const auto& info = order_info[orderkey];
        results.push_back({orderkey, revenue, info.first, info.second});
    }

    std::partial_sort(results.begin(),
                      results.begin() + std::min(size_t(10), results.size()),
                      results.end(),
                      [](const Result& a, const Result& b) {
                          if (a.revenue != b.revenue) return a.revenue > b.revenue;
                          return a.orderdate < b.orderdate;
                      });

    size_t top_n = std::min(size_t(10), results.size());

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "Q3: " << top_n << " rows in " << duration << " ms\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << std::fixed << std::setprecision(2);
        // FIX: Add header row before data
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (size_t i = 0; i < top_n; ++i) {
            const auto& r = results[i];
            // FIX: Change delimiter from pipe to comma for CSV format
            // FIX: Correct divisor for revenue from 10000.0 to 100.0
            // This matches the corrected disc_price calculation (divided by 100, not 10000)
            out << r.orderkey << ","
                << (r.revenue / 100.0) << ","
                << days_to_date(r.orderdate) << ","
                << r.shippriority << "\n";
        }
    }

    // Cleanup
    ColumnReader::unmap(c_custkey, c_count * sizeof(int32_t));
    ColumnReader::unmap(c_mktsegment, c_count * sizeof(uint8_t));
    ColumnReader::unmap(o_orderkey, o_count * sizeof(int32_t));
    ColumnReader::unmap(o_custkey, o_count * sizeof(int32_t));
    ColumnReader::unmap(o_orderdate, o_count * sizeof(int32_t));
    ColumnReader::unmap(o_shippriority, o_count * sizeof(int32_t));
    ColumnReader::unmap(l_orderkey, l_count * sizeof(int32_t));
    ColumnReader::unmap(l_shipdate, l_count * sizeof(int32_t));
    ColumnReader::unmap(l_extendedprice, l_count * sizeof(int64_t));
    ColumnReader::unmap(l_discount, l_count * sizeof(int64_t));
}

} // namespace gendb
