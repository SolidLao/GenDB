#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <sys/mman.h>

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Convert date constants
    const int32_t orderdate_threshold = date_utils::date_to_days("1995-03-15");
    const int32_t shipdate_threshold = date_utils::date_to_days("1995-03-15");

    // Step 1: Load customer columns (c_custkey, c_mktsegment)
    storage::MappedColumn customer_custkey, customer_mktsegment, customer_mktsegment_dict_file;
    customer_custkey.open(gendb_dir + "/customer.c_custkey");
    customer_mktsegment.open(gendb_dir + "/customer.c_mktsegment");
    customer_mktsegment_dict_file.open(gendb_dir + "/customer.c_mktsegment.dict");

    const int32_t* c_custkey = customer_custkey.as<int32_t>() + 1; // +1 to skip count
    const uint8_t* c_mktsegment = customer_mktsegment.as<uint8_t>() + 1;
    size_t customer_count = customer_custkey.count<int32_t>() - 1;

    // Load dictionary for c_mktsegment
    storage::Dictionary mktsegment_dict = storage::read_dictionary(gendb_dir + "/customer.c_mktsegment.dict");
    uint8_t building_code = mktsegment_dict.value_to_code["BUILDING"];

    // Filter customer on c_mktsegment = 'BUILDING'
    auto filtered_customer_indices = operators::scan_filter<uint8_t>(
        c_mktsegment, customer_count,
        [building_code](uint8_t code) { return code == building_code; }
    );

    std::cout << "Filtered customers: " << filtered_customer_indices.size() << std::endl;

    // Materialize filtered c_custkey
    std::vector<int32_t> filtered_custkeys = operators::gather(c_custkey, filtered_customer_indices);

    // Step 2: Load orders columns (o_orderkey, o_custkey, o_orderdate, o_shippriority)
    storage::MappedColumn orders_orderkey, orders_custkey, orders_orderdate, orders_shippriority;
    orders_orderkey.open(gendb_dir + "/orders.o_orderkey");
    orders_custkey.open(gendb_dir + "/orders.o_custkey");
    orders_orderdate.open(gendb_dir + "/orders.o_orderdate");
    orders_shippriority.open(gendb_dir + "/orders.o_shippriority");

    const int32_t* o_orderkey = orders_orderkey.as<int32_t>() + 1;
    const int32_t* o_custkey = orders_custkey.as<int32_t>() + 1;
    const int32_t* o_orderdate = orders_orderdate.as<int32_t>() + 1;
    const int32_t* o_shippriority = orders_shippriority.as<int32_t>() + 1;
    size_t orders_count = orders_orderkey.count<int32_t>() - 1;

    // Hash join: customer ⋈ orders (build on filtered_customer, probe with orders)
    operators::HashJoin<int32_t> customer_orders_join;
    customer_orders_join.build(filtered_custkeys.data(), filtered_custkeys.size());

    auto customer_orders_matches = customer_orders_join.probe(o_custkey, orders_count);

    std::cout << "Customer-Orders join matches: " << customer_orders_matches.size() << std::endl;

    // Apply o_orderdate < '1995-03-15' filter to joined results
    std::vector<size_t> filtered_orders_indices;
    filtered_orders_indices.reserve(customer_orders_matches.size());

    for (const auto& match : customer_orders_matches) {
        size_t orders_idx = match.second;
        if (o_orderdate[orders_idx] < orderdate_threshold) {
            filtered_orders_indices.push_back(orders_idx);
        }
    }

    std::cout << "Orders after date filter: " << filtered_orders_indices.size() << std::endl;

    // Materialize filtered orders data
    std::vector<int32_t> filtered_o_orderkey = operators::gather(o_orderkey, filtered_orders_indices);
    std::vector<int32_t> filtered_o_orderdate = operators::gather(o_orderdate, filtered_orders_indices);
    std::vector<int32_t> filtered_o_shippriority = operators::gather(o_shippriority, filtered_orders_indices);

    // Step 3: Load lineitem columns (l_orderkey, l_shipdate, l_extendedprice, l_discount)
    storage::MappedColumn lineitem_orderkey, lineitem_shipdate, lineitem_extendedprice, lineitem_discount;
    lineitem_orderkey.open(gendb_dir + "/lineitem.l_orderkey");
    lineitem_shipdate.open(gendb_dir + "/lineitem.l_shipdate");
    lineitem_extendedprice.open(gendb_dir + "/lineitem.l_extendedprice");
    lineitem_discount.open(gendb_dir + "/lineitem.l_discount");

    const int32_t* l_orderkey = lineitem_orderkey.as<int32_t>() + 1;
    const int32_t* l_shipdate = lineitem_shipdate.as<int32_t>() + 1;
    const int64_t* l_extendedprice = lineitem_extendedprice.as<int64_t>() + 1;
    const int64_t* l_discount = lineitem_discount.as<int64_t>() + 1;
    size_t lineitem_count = lineitem_orderkey.count<int32_t>() - 1;

    // Use scan_range for l_shipdate > '1995-03-15' (assuming sorted)
    // Note: lineitem is sorted by l_shipdate based on the guidance
    auto filtered_lineitem_indices = operators::scan_range<int32_t>(
        l_shipdate, lineitem_count,
        shipdate_threshold + 1, INT32_MAX
    );

    std::cout << "Lineitem after shipdate filter: " << filtered_lineitem_indices.size() << std::endl;

    // Materialize filtered lineitem data
    std::vector<int32_t> filtered_l_orderkey = operators::gather(l_orderkey, filtered_lineitem_indices);
    std::vector<int64_t> filtered_l_extendedprice = operators::gather(l_extendedprice, filtered_lineitem_indices);
    std::vector<int64_t> filtered_l_discount = operators::gather(l_discount, filtered_lineitem_indices);

    // Step 4: Hash join orders ⋈ lineitem (build on filtered_orders, probe with filtered_lineitem)
    operators::HashJoin<int32_t> orders_lineitem_join;
    orders_lineitem_join.build(filtered_o_orderkey.data(), filtered_o_orderkey.size());

    auto final_matches = orders_lineitem_join.probe(filtered_l_orderkey.data(), filtered_l_orderkey.size());

    std::cout << "Final join matches: " << final_matches.size() << std::endl;

    // Step 5: Hash aggregation with TopKHeap
    // Group by (l_orderkey, o_orderdate, o_shippriority), compute SUM(l_extendedprice * (1 - l_discount))
    struct AggKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator==(const AggKey& other) const {
            return l_orderkey == other.l_orderkey &&
                   o_orderdate == other.o_orderdate &&
                   o_shippriority == other.o_shippriority;
        }
    };

    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            size_t h1 = std::hash<int32_t>()(k.l_orderkey);
            size_t h2 = std::hash<int32_t>()(k.o_orderdate);
            size_t h3 = std::hash<int32_t>()(k.o_shippriority);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };

    std::unordered_map<AggKey, int64_t, AggKeyHash> agg_table;

    for (const auto& match : final_matches) {
        size_t orders_idx = match.first;
        size_t lineitem_idx = match.second;

        int32_t orderkey = filtered_o_orderkey[orders_idx];
        int32_t orderdate = filtered_o_orderdate[orders_idx];
        int32_t shippriority = filtered_o_shippriority[orders_idx];

        int64_t extendedprice = filtered_l_extendedprice[lineitem_idx];
        int64_t discount = filtered_l_discount[lineitem_idx];

        // revenue = l_extendedprice * (1 - l_discount)
        // Since values are scaled by 100, (1 - l_discount) = (100 - l_discount) / 100
        // So revenue = extendedprice * (100 - discount) / 100
        int64_t revenue = extendedprice * (100 - discount) / 100;

        AggKey key = {orderkey, orderdate, shippriority};
        agg_table[key] += revenue;
    }

    std::cout << "Aggregated groups: " << agg_table.size() << std::endl;

    // Step 6: Use TopKHeap to maintain top 10 by revenue
    struct ResultEntry {
        int32_t l_orderkey;
        int64_t revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator<(const ResultEntry& other) const {
            if (revenue != other.revenue) {
                return revenue > other.revenue; // DESC by revenue
            }
            return o_orderdate < other.o_orderdate; // ASC by o_orderdate
        }
    };

    std::vector<ResultEntry> results;
    results.reserve(agg_table.size());

    for (const auto& entry : agg_table) {
        results.push_back({
            entry.first.l_orderkey,
            entry.second,
            entry.first.o_orderdate,
            entry.first.o_shippriority
        });
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end());

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Query Q3 returned " << results.size() << " rows in "
              << duration.count() << " ms" << std::endl;

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q3.csv";
        std::ofstream out(output_path);

        if (!out.is_open()) {
            std::cerr << "Error: Cannot open output file: " << output_path << std::endl;
            return;
        }

        out << "l_orderkey|revenue|o_orderdate|o_shippriority\n";

        for (const auto& row : results) {
            out << row.l_orderkey << "|"
                << std::fixed << std::setprecision(2) << (row.revenue / 100.0) << "|"
                << date_utils::days_to_date(row.o_orderdate) << "|"
                << row.o_shippriority << "\n";
        }

        out.close();
        std::cout << "Results written to: " << output_path << std::endl;
    }
}
