#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <data_directory>" << std::endl;
        std::cerr << "Example: " << argv[0] << " /path/to/tpch/data/sf1" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    if (data_dir.back() != '/') {
        data_dir += '/';
    }

    std::cout << "=== GenDB: TPC-H Query Execution ===" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;

    // Load tables
    std::cout << "\n--- Loading Tables ---" << std::endl;

    auto load_start = std::chrono::high_resolution_clock::now();

    LineitemTable lineitem;
    std::cout << "Loading lineitem..." << std::flush;
    load_lineitem(data_dir + "lineitem.tbl", lineitem);
    std::cout << " " << lineitem.size() << " rows" << std::endl;

    OrdersTable orders;
    std::cout << "Loading orders..." << std::flush;
    load_orders(data_dir + "orders.tbl", orders);
    std::cout << " " << orders.size() << " rows" << std::endl;

    CustomerTable customer;
    std::cout << "Loading customer..." << std::flush;
    load_customer(data_dir + "customer.tbl", customer);
    std::cout << " " << customer.size() << " rows" << std::endl;

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);
    std::cout << "Total load time: " << load_duration.count() << " ms" << std::endl;

    // Execute queries
    std::cout << "\n--- Executing Queries ---" << std::endl;

    execute_q1(lineitem);
    execute_q3(customer, orders, lineitem);
    execute_q6(lineitem);

    std::cout << "\n=== Execution Complete ===" << std::endl;

    return 0;
}
