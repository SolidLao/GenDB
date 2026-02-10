#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>

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

    // Load tables with selective column loading to reduce memory usage
    std::cout << "Loading tables..." << std::endl;
    auto load_start = std::chrono::high_resolution_clock::now();

    LineItem lineitem;
    Orders orders;
    Customer customer;

    // Load only columns needed by queries (8 of 16 lineitem columns)
    // Q1 needs: l_shipdate, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus
    // Q3 needs: l_orderkey, l_shipdate, l_extendedprice, l_discount
    // Q6 needs: l_shipdate, l_quantity, l_discount, l_extendedprice
    std::unordered_set<std::string> lineitem_cols = {
        "l_orderkey", "l_quantity", "l_extendedprice", "l_discount",
        "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"
    };
    load_lineitem(data_dir + "lineitem.tbl", lineitem, lineitem_cols);

    // Load only columns needed by Q3 (4 of 9 orders columns)
    std::unordered_set<std::string> orders_cols = {
        "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"
    };
    load_orders(data_dir + "orders.tbl", orders, orders_cols);

    // Load only columns needed by Q3 (2 of 8 customer columns)
    std::unordered_set<std::string> customer_cols = {
        "c_custkey", "c_mktsegment"
    };
    load_customer(data_dir + "customer.tbl", customer, customer_cols);

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
