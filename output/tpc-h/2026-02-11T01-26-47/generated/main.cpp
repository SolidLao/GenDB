#include "storage/storage.h"
#include "queries/queries.h"
#include <iostream>
#include <chrono>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        std::cerr << "  gendb_dir: Directory containing binary columnar data\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "=== GenDB Query Execution Program ===\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n\n";

    // Load tables from binary storage
    auto load_start = std::chrono::high_resolution_clock::now();

    std::cout << "Loading tables from binary storage...\n";

    auto lineitem = read_lineitem(gendb_dir);
    std::cout << "  - Lineitem: " << lineitem.size() << " rows\n";

    auto orders = read_orders(gendb_dir);
    std::cout << "  - Orders: " << orders.size() << " rows\n";

    auto customer = read_customer(gendb_dir);
    std::cout << "  - Customer: " << customer.size() << " rows\n";

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);

    std::cout << "Loading complete in " << load_duration.count() << " ms\n";

    // Execute queries
    std::cout << "\n=== Executing TPC-H Queries ===\n";

    execute_q1(lineitem);
    execute_q6(lineitem);
    execute_q3(lineitem, orders, customer);

    std::cout << "\n=== All Queries Complete ===\n";

    return 0;
}
