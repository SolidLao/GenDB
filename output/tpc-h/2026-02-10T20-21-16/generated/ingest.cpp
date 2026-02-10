#include "storage/storage.h"
#include <iostream>
#include <chrono>
#include <string>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        std::cerr << "  <data_dir>: Directory containing .tbl files\n";
        std::cerr << "  <gendb_dir>: Output directory for binary columnar storage\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    std::cout << "=== GenDB Ingestion ===\n";
    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n\n";

    auto start = std::chrono::high_resolution_clock::now();

    // Ingest each table
    gendb::ingest_customer(data_dir + "/customer.tbl", gendb_dir);
    gendb::ingest_orders(data_dir + "/orders.tbl", gendb_dir);
    gendb::ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

    std::cout << "\n=== Ingestion Complete ===\n";
    std::cout << "Total time: " << duration.count() << " seconds\n";

    return 0;
}
