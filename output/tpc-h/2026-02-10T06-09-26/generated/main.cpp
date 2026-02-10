#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <data_directory>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    if (data_dir.back() != '/') {
        data_dir += '/';
    }

    std::cout << "Loading TPC-H data from: " << data_dir << "\n\n";

    // Load tables
    gendb::LineitemTable lineitem;
    gendb::OrdersTable orders;
    gendb::CustomerTable customer;

    auto load_start = std::chrono::high_resolution_clock::now();

    std::cout << "Loading lineitem...\n";
    gendb::load_lineitem(data_dir + "lineitem.tbl", lineitem);

    std::cout << "Loading orders...\n";
    gendb::load_orders(data_dir + "orders.tbl", orders);

    std::cout << "Loading customer...\n";
    gendb::load_customer(data_dir + "customer.tbl", customer);

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::seconds>(load_end - load_start);

    std::cout << "\n=== Data Loading Complete ===\n";
    std::cout << "Lineitem rows: " << lineitem.size() << "\n";
    std::cout << "Orders rows: " << orders.size() << "\n";
    std::cout << "Customer rows: " << customer.size() << "\n";
    std::cout << "Total load time: " << load_duration.count() << " seconds\n";

    // Execute queries
    std::cout << "\n" << std::string(80, '=') << "\n";
    gendb::execute_q1(lineitem);

    std::cout << "\n" << std::string(80, '=') << "\n";
    gendb::execute_q3(customer, orders, lineitem);

    std::cout << "\n" << std::string(80, '=') << "\n";
    gendb::execute_q6(lineitem);

    std::cout << "\n" << std::string(80, '=') << "\n";
    std::cout << "All queries completed successfully!\n";

    return 0;
}
