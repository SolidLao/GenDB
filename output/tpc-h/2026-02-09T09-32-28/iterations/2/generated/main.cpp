#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>

int main(int argc, char* argv[]) {
    // Parse data directory from command line (default: current directory)
    std::string data_dir = (argc > 1) ? argv[1] : ".";

    std::cout << "=== GenDB: TPC-H Query Execution ===\n";
    std::cout << "Data directory: " << data_dir << "\n\n";

    // Load tables
    std::cout << "Loading tables...\n";
    auto load_start = std::chrono::high_resolution_clock::now();

    LineitemTable lineitem;
    CustomerTable customer;
    OrdersTable orders;

    load_lineitem(data_dir + "/lineitem.tbl", lineitem);
    std::cout << "  lineitem: " << lineitem.size() << " rows\n";

    load_customer(data_dir + "/customer.tbl", customer);
    std::cout << "  customer: " << customer.size() << " rows\n";

    load_orders(data_dir + "/orders.tbl", orders);
    std::cout << "  orders: " << orders.size() << " rows\n";

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start).count();
    std::cout << "\nData loading completed in " << load_duration << " ms\n";

    // Execute queries
    std::cout << "\n========================================\n";
    std::cout << "Executing queries...\n";
    std::cout << "========================================\n";

    execute_q1(lineitem);
    execute_q3(customer, orders, lineitem);
    execute_q6(lineitem);

    std::cout << "\n========================================\n";
    std::cout << "All queries completed successfully.\n";
    std::cout << "========================================\n";

    return 0;
}
