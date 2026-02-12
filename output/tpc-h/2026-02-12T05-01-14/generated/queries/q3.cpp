#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <chrono>
#include <cstdio>

namespace gendb {

// Helper to load string column (simplified)
static std::vector<std::string> load_string_column(const std::string& gendb_dir,
                                                     const std::string& table_name,
                                                     const std::string& column_name,
                                                     size_t expected_count) {
    std::string path = gendb_dir + "/" + table_name + "_" + column_name + ".bin";
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) return {};

    std::vector<std::string> result;
    result.reserve(expected_count);

    while (!feof(f)) {
        uint32_t len;
        if (fread(&len, sizeof(uint32_t), 1, f) != 1) break;
        std::string str(len, '\0');
        if (fread(&str[0], 1, len, f) != len) break;
        result.push_back(std::move(str));
    }

    fclose(f);
    return result;
}

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < DATE '1995-03-15'
//   AND l_shipdate > DATE '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load customer columns
    const int32_t* c_custkey;
    const std::string* c_mktsegment_ptr;
    size_t customer_count;
    load_customer_columns_q3(gendb_dir, &c_custkey, &c_mktsegment_ptr, customer_count);

    // Load c_mktsegment as string vector
    auto c_mktsegment = load_string_column(gendb_dir, "customer", "c_mktsegment", customer_count);

    // Filter customer: c_mktsegment = 'BUILDING'
    std::unordered_set<int32_t> building_customers;
    for (size_t i = 0; i < customer_count; ++i) {
        if (c_mktsegment[i] == "BUILDING") {
            building_customers.insert(c_custkey[i]);
        }
    }

    // Load orders columns
    const int32_t* o_orderkey;
    const int32_t* o_custkey;
    const int32_t* o_orderdate;
    const int32_t* o_shippriority;
    size_t orders_count;
    load_orders_columns_q3(gendb_dir, &o_orderkey, &o_custkey, &o_orderdate, &o_shippriority, orders_count);

    // Filter orders: o_custkey IN building_customers AND o_orderdate < '1995-03-15'
    int32_t order_date_cutoff = parse_date("1995-03-15");

    // Build hash map: orderkey -> (orderdate, shippriority)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_map;
    for (size_t i = 0; i < orders_count; ++i) {
        if (building_customers.count(o_custkey[i]) && o_orderdate[i] < order_date_cutoff) {
            order_map[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    }

    // Load lineitem columns
    const int32_t* l_orderkey;
    const int32_t* l_shipdate;
    const double* l_extendedprice;
    const double* l_discount;
    size_t lineitem_count;
    load_lineitem_columns_q3(gendb_dir, &l_orderkey, &l_shipdate, &l_extendedprice, &l_discount, lineitem_count);

    // Filter lineitem: l_shipdate > '1995-03-15' AND l_orderkey IN order_map
    int32_t line_date_cutoff = parse_date("1995-03-15");

    // Aggregation
    Q3AggMap agg_map;
    for (size_t i = 0; i < lineitem_count; ++i) {
        if (l_shipdate[i] > line_date_cutoff) {
            auto it = order_map.find(l_orderkey[i]);
            if (it != order_map.end()) {
                Q3GroupKey key{l_orderkey[i], it->second.first, it->second.second};
                agg_map[key].revenue += l_extendedprice[i] * (1.0 - l_discount[i]);
            }
        }
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::vector<std::pair<Q3GroupKey, Q3AggValue>> results(agg_map.begin(), agg_map.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.second.revenue != b.second.revenue)
            return a.second.revenue > b.second.revenue;  // DESC
        return a.first.o_orderdate < b.first.o_orderdate;  // ASC
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // Write results if requested
    if (!results_dir.empty()) {
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (f) {
            fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            for (const auto& [key, val] : results) {
                fprintf(f, "%d,%.2f,%s,%d\n",
                        key.l_orderkey, val.revenue,
                        days_to_date_str(key.o_orderdate).c_str(),
                        key.o_shippriority);
            }
            fclose(f);
        }
    }

    // Print summary
    std::cout << "Q3: " << results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << "s" << std::endl;
}

} // namespace gendb
