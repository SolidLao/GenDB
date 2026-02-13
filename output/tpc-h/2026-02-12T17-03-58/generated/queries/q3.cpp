#include "queries.h"
#include "../storage/storage.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <vector>

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

struct Q3Key {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3Key& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

namespace std {
    template<>
    struct hash<Q3Key> {
        size_t operator()(const Q3Key& k) const {
            return std::hash<int32_t>()(k.l_orderkey) ^
                   (std::hash<int32_t>()(k.o_orderdate) << 1) ^
                   (std::hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

struct Q3Agg {
    int64_t revenue = 0;
};

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load customer metadata and columns
    auto customer_meta = Storage::readMetadata(gendb_dir, "customer");
    auto c_custkey = Storage::mmapColumn(gendb_dir, "customer", "c_custkey", "int32");
    auto c_mktsegment = Storage::mmapColumn(gendb_dir, "customer", "c_mktsegment", "uint8");

    const int32_t* c_custkey_data = c_custkey->as<int32_t>();
    const uint8_t* c_mktsegment_data = c_mktsegment->as<uint8_t>();

    // Find BUILDING segment ID
    uint8_t building_id = customer_meta.dictionaries.at("c_mktsegment").value_to_id.at("BUILDING");

    // Filter customers: c_mktsegment = 'BUILDING'
    std::vector<int32_t> filtered_custkeys;
    for (uint64_t row = 0; row < customer_meta.row_count; ++row) {
        if (c_mktsegment_data[row] == building_id) {
            filtered_custkeys.push_back(c_custkey_data[row]);
        }
    }

    // Build hash table for filtered customers
    HashIndex<int32_t> customer_hash;
    for (int32_t custkey : filtered_custkeys) {
        customer_hash.insert(custkey, 0);
    }

    // Load orders metadata and columns
    auto orders_meta = Storage::readMetadata(gendb_dir, "orders");
    auto o_orderkey = Storage::mmapColumn(gendb_dir, "orders", "o_orderkey", "int32");
    auto o_custkey = Storage::mmapColumn(gendb_dir, "orders", "o_custkey", "int32");
    auto o_orderdate = Storage::mmapColumn(gendb_dir, "orders", "o_orderdate", "int32");
    auto o_shippriority = Storage::mmapColumn(gendb_dir, "orders", "o_shippriority", "int32");

    const int32_t* o_orderkey_data = o_orderkey->as<int32_t>();
    const int32_t* o_custkey_data = o_custkey->as<int32_t>();
    const int32_t* o_orderdate_data = o_orderdate->as<int32_t>();
    const int32_t* o_shippriority_data = o_shippriority->as<int32_t>();

    int32_t max_orderdate = DateUtils::stringToDate("1995-03-15");

    // Filter orders: join with customer AND o_orderdate < '1995-03-15'
    struct OrderInfo {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };
    std::vector<OrderInfo> filtered_orders;

    for (uint64_t row = 0; row < orders_meta.row_count; ++row) {
        if (o_orderdate_data[row] < max_orderdate &&
            customer_hash.contains(o_custkey_data[row])) {
            filtered_orders.push_back({
                o_orderkey_data[row],
                o_orderdate_data[row],
                o_shippriority_data[row]
            });
        }
    }

    // Build hash table for filtered orders
    HashIndex<int32_t> order_hash;
    std::unordered_map<int32_t, OrderInfo> order_map;
    for (const auto& order : filtered_orders) {
        order_hash.insert(order.orderkey, 0);
        order_map[order.orderkey] = order;
    }

    // Load lineitem metadata and columns
    auto lineitem_meta = Storage::readMetadata(gendb_dir, "lineitem");
    auto l_orderkey = Storage::mmapColumn(gendb_dir, "lineitem", "l_orderkey", "int32");
    auto l_shipdate = Storage::mmapColumn(gendb_dir, "lineitem", "l_shipdate", "int32");
    auto l_extendedprice = Storage::mmapColumn(gendb_dir, "lineitem", "l_extendedprice", "int64");
    auto l_discount = Storage::mmapColumn(gendb_dir, "lineitem", "l_discount", "int64");

    const int32_t* l_orderkey_data = l_orderkey->as<int32_t>();
    const int32_t* l_shipdate_data = l_shipdate->as<int32_t>();
    const int64_t* l_extendedprice_data = l_extendedprice->as<int64_t>();
    const int64_t* l_discount_data = l_discount->as<int64_t>();

    int32_t min_shipdate = DateUtils::stringToDate("1995-03-15");

    // Join lineitem with orders and aggregate
    HashAgg<Q3Key, Q3Agg> agg;

    for (uint64_t row = 0; row < lineitem_meta.row_count; ++row) {
        if (l_shipdate_data[row] > min_shipdate) {
            int32_t orderkey = l_orderkey_data[row];
            if (order_hash.contains(orderkey)) {
                const auto& order = order_map[orderkey];
                Q3Key key{orderkey, order.orderdate, order.shippriority};

                agg.update(key, [&](Q3Agg& state) {
                    int64_t price = l_extendedprice_data[row];
                    int64_t disc = l_discount_data[row];
                    int64_t revenue = (price * (10000 - disc)) / 10000;
                    state.revenue += revenue;
                });
            }
        }
    }

    // Get top 10 results sorted by revenue DESC, o_orderdate ASC
    auto results = agg.get_top_k(10, [](const auto& a, const auto& b) {
        if (a.second.revenue != b.second.revenue)
            return a.second.revenue > b.second.revenue; // DESC
        return a.first.o_orderdate < b.first.o_orderdate; // ASC
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output results
    std::cout << "Q3: " << results.size() << " rows, " << duration.count() << " ms" << std::endl;

    // Optionally write to CSV
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/q3_results.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

        for (const auto& [key, val] : results) {
            out << key.l_orderkey << ","
                << std::fixed << std::setprecision(2) << (val.revenue / 100.0) << ","
                << DateUtils::dateToString(key.o_orderdate) << ","
                << key.o_shippriority << "\n";
        }
        out.close();
    }
}
