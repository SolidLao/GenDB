#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <unordered_map>
#include <thread>
#include <vector>
#include <algorithm>
#include <queue>

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < '1995-03-15'
//   AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

struct Q3GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3GroupKey& o) const {
        return l_orderkey == o.l_orderkey &&
               o_orderdate == o.o_orderdate &&
               o_shippriority == o.o_shippriority;
    }
};

namespace std {
    template<>
    struct hash<Q3GroupKey> {
        size_t operator()(const Q3GroupKey& k) const {
            return hash<int32_t>()(k.l_orderkey) ^
                   (hash<int32_t>()(k.o_orderdate) << 1) ^
                   (hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

struct Q3Result {
    Q3GroupKey key;
    double revenue;

    bool operator<(const Q3Result& o) const {
        // Min-heap: reverse comparison for top-K
        if (revenue != o.revenue)
            return revenue > o.revenue;
        return key.o_orderdate > o.key.o_orderdate;
    }
};

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t c_size = get_row_count(gendb_dir, "customer");
    size_t sz;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", sz);
    uint8_t* c_mktsegment = mmap_column<uint8_t>(gendb_dir, "customer", "c_mktsegment", sz);

    // Load customer dictionary to find "BUILDING" code
    auto c_mkt_dict = load_string_column(gendb_dir, "customer", "c_mktsegment.dict");
    uint8_t building_code = 255;
    for (size_t i = 0; i < c_mkt_dict.size(); i++) {
        if (c_mkt_dict[i] == "BUILDING") {
            building_code = static_cast<uint8_t>(i);
            break;
        }
    }

    // Filter customers and build hash set using scan operator
    std::unordered_set<int32_t> filtered_customers;
    gendb::operators::sequential_scan(
        c_size,
        // Predicate: filter by market segment
        [&](size_t i) { return c_mktsegment[i] == building_code; },
        // Process: insert customer key
        [&](size_t i) { filtered_customers.insert(c_custkey[i]); }
    );

    // Load orders columns
    size_t o_size = get_row_count(gendb_dir, "orders");
    int32_t* o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", sz);
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", sz);
    int32_t* o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", sz);
    int32_t* o_shippriority = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", sz);

    int32_t orderdate_threshold = date_utils::date_to_days(1995, 3, 15);

    // Build hash index on filtered orders using scan operator
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, OrderInfo> filtered_orders;

    gendb::operators::sequential_scan(
        o_size,
        // Predicate: filter by orderdate and join with customer
        [&](size_t i) {
            return o_orderdate[i] < orderdate_threshold &&
                   filtered_customers.count(o_custkey[i]);
        },
        // Process: build hash table
        [&](size_t i) {
            filtered_orders[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    );

    // Load lineitem columns
    size_t l_size = get_row_count(gendb_dir, "lineitem");
    int32_t* l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", sz);
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", sz);
    double* l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", sz);
    double* l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", sz);

    int32_t shipdate_threshold = date_utils::date_to_days(1995, 3, 15);

    // Parallel aggregation with hash probe using parallel_hash_aggregate_filtered
    // We need to handle the join within the aggregation
    auto global_agg = gendb::operators::parallel_hash_aggregate_filtered<Q3GroupKey, double>(
        l_size,
        // Predicate: filter by shipdate and check if order exists
        [&](size_t i) {
            return l_shipdate[i] > shipdate_threshold &&
                   filtered_orders.count(l_orderkey[i]);
        },
        // Group key function: construct key from lineitem and joined order
        [&](size_t i) {
            const auto& order_info = filtered_orders.at(l_orderkey[i]);
            return Q3GroupKey{l_orderkey[i], order_info.orderdate, order_info.shippriority};
        },
        // Aggregate function: compute revenue
        [&](size_t i, double& revenue) {
            revenue += l_extendedprice[i] * (1.0 - l_discount[i]);
        },
        // Merge function: sum revenues
        [](double& dest, const double& src) {
            dest += src;
        }
    );

    // Top-K heap
    std::priority_queue<Q3Result> heap;
    for (const auto& [key, revenue] : global_agg) {
        heap.push({key, revenue});
        if (heap.size() > 10) {
            heap.pop();
        }
    }

    // Extract results
    std::vector<Q3Result> results;
    while (!heap.empty()) {
        results.push_back(heap.top());
        heap.pop();
    }

    // Sort in descending order
    std::sort(results.begin(), results.end(), [](const Q3Result& a, const Q3Result& b) {
        if (a.revenue != b.revenue)
            return a.revenue > b.revenue;
        return a.key.o_orderdate < b.key.o_orderdate;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q3: " << results.size() << " rows in " << elapsed << "s" << std::endl;

    // Write results if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& r : results) {
            out << r.key.l_orderkey << ","
                << r.revenue << ","
                << date_utils::days_to_date_str(r.key.o_orderdate) << ","
                << r.key.o_shippriority << "\n";
        }
    }
}
