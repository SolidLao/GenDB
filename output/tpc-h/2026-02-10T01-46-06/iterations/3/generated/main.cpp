#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>
#include <unordered_set>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <data_directory>" << std::endl;
        std::cerr << "Example: " << argv[0] << " /home/jl4492/GenDB/benchmarks/tpc-h/data/sf10" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    if (data_dir.back() != '/') {
        data_dir += '/';
    }

    std::cout << "=== GenDB TPC-H Query Engine ===" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << std::endl;

    // Load tables with selective column loading
    std::cout << "Loading tables..." << std::endl;
    auto load_start = std::chrono::high_resolution_clock::now();

    LineItem lineitem;
    Orders orders;
    Customer customer;

    // String pool for interning low-cardinality strings
    StringPool string_pool;

    // Define columns needed by each query
    // Q1 needs: l_orderkey, l_quantity, l_extendedprice, l_discount, l_tax,
    //           l_returnflag, l_linestatus, l_shipdate
    // Q3 needs: l_orderkey, l_extendedprice, l_discount, l_shipdate
    // Q6 needs: l_shipdate, l_discount, l_quantity, l_extendedprice
    // Union of all: l_orderkey, l_quantity, l_extendedprice, l_discount, l_tax,
    //               l_returnflag, l_linestatus, l_shipdate
    std::unordered_set<std::string> lineitem_columns = {
        "l_orderkey", "l_quantity", "l_extendedprice", "l_discount",
        "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"
    };

    // Q3 needs: o_orderkey, o_custkey, o_orderdate, o_shippriority
    std::unordered_set<std::string> orders_columns = {
        "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"
    };

    // Q3 needs: c_custkey, c_mktsegment
    std::unordered_set<std::string> customer_columns = {
        "c_custkey", "c_mktsegment"
    };

    load_lineitem(data_dir + "lineitem.tbl", lineitem, lineitem_columns);
    load_orders(data_dir + "orders.tbl", orders, orders_columns);
    load_customer(data_dir + "customer.tbl", customer, customer_columns, &string_pool);

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);

    std::cout << "\nTable row counts:" << std::endl;
    std::cout << "  lineitem: " << lineitem.size() << " rows" << std::endl;
    std::cout << "  orders:   " << orders.size() << " rows" << std::endl;
    std::cout << "  customer: " << customer.size() << " rows" << std::endl;
    std::cout << "\nTotal load time: " << load_duration.count() << " ms" << std::endl;

    // Execute queries
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "Executing queries..." << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    execute_q1(lineitem);
    execute_q3(customer, orders, lineitem);
    execute_q6(lineitem);

    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "All queries completed successfully." << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    return 0;
}
