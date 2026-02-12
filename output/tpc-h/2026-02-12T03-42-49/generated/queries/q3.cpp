#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <sys/mman.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <algorithm>
#include <queue>

namespace gendb {

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t c_row_count = 0;
    auto c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", c_row_count);
    size_t tmp;
    auto c_mktsegment = mmap_column<uint8_t>(gendb_dir, "customer", "c_mktsegment", tmp);
    auto mktsegment_dict = load_dictionary(gendb_dir + "/customer/c_mktsegment.dict");

    // Find target mktsegment encoding
    uint8_t building_code = 255;
    for (const auto& [code, seg] : mktsegment_dict) {
        if (seg == "BUILDING") {
            building_code = code;
            break;
        }
    }

    // Filter customers by mktsegment = 'BUILDING'
    std::unordered_map<int32_t, size_t> filtered_customers;
    for (size_t i = 0; i < c_row_count; i++) {
        if (c_mktsegment[i] == building_code) {
            filtered_customers[c_custkey[i]] = i;
        }
    }

    // Load orders columns
    size_t o_row_count = 0;
    auto o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", o_row_count);
    auto o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", tmp);
    auto o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", tmp);
    auto o_shippriority = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", tmp);

    // Date filter: o_orderdate < '1995-03-15'
    int32_t order_date_threshold = parse_date("1995-03-15");

    // Join customer and orders, filter by date
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> filtered_orders; // orderkey -> (orderdate, shippriority)
    for (size_t i = 0; i < o_row_count; i++) {
        if (o_orderdate[i] < order_date_threshold) {
            if (filtered_customers.count(o_custkey[i]) > 0) {
                filtered_orders[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
            }
        }
    }

    // Load lineitem columns
    size_t l_row_count = 0;
    auto l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", l_row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", tmp);

    // Date filter: l_shipdate > '1995-03-15'
    int32_t ship_date_threshold = parse_date("1995-03-15");

    // Join with lineitem and aggregate
    struct AggKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
        bool operator==(const AggKey& o) const {
            return l_orderkey == o.l_orderkey && o_orderdate == o.o_orderdate && o_shippriority == o.o_shippriority;
        }
    };
    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            return std::hash<int32_t>()(k.l_orderkey) ^ (std::hash<int32_t>()(k.o_orderdate) << 1) ^ (std::hash<int32_t>()(k.o_shippriority) << 2);
        }
    };

    std::unordered_map<AggKey, double, AggKeyHash> revenue_map;

    for (size_t i = 0; i < l_row_count; i++) {
        if (l_shipdate[i] > ship_date_threshold) {
            auto it = filtered_orders.find(l_orderkey[i]);
            if (it != filtered_orders.end()) {
                AggKey key{l_orderkey[i], it->second.first, it->second.second};
                revenue_map[key] += l_extendedprice[i] * (1.0 - l_discount[i]);
            }
        }
    }

    // Sort by revenue DESC, o_orderdate ASC and take top 10
    std::vector<std::pair<AggKey, double>> results(revenue_map.begin(), revenue_map.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second; // revenue DESC
        return a.first.o_orderdate < b.first.o_orderdate; // o_orderdate ASC
    });

    if (results.size() > 10) results.resize(10);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q3: " << results.size() << " rows in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    // Write to CSV if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& [key, revenue] : results) {
            out << key.l_orderkey << ","
                << revenue << ","
                << days_to_date_str(key.o_orderdate) << ","
                << key.o_shippriority << "\n";
        }
    }

    // Cleanup
    munmap(c_custkey, c_row_count * sizeof(int32_t));
    munmap(c_mktsegment, c_row_count * sizeof(uint8_t));
    munmap(o_orderkey, o_row_count * sizeof(int32_t));
    munmap(o_custkey, o_row_count * sizeof(int32_t));
    munmap(o_orderdate, o_row_count * sizeof(int32_t));
    munmap(o_shippriority, o_row_count * sizeof(int32_t));
    munmap(l_orderkey, l_row_count * sizeof(int32_t));
    munmap(l_extendedprice, l_row_count * sizeof(double));
    munmap(l_discount, l_row_count * sizeof(double));
    munmap(l_shipdate, l_row_count * sizeof(int32_t));
}

} // namespace gendb
