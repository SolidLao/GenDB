#include "storage/storage.h"
#include <iostream>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_gendb_dir>" << std::endl;
        std::cerr << "  data_dir: directory containing .tbl files" << std::endl;
        std::cerr << "  output_gendb_dir: directory to write binary column files" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directory if it doesn't exist
    mkdir(output_dir.c_str(), 0755);

    std::cout << "GenDB Ingestion Tool" << std::endl;
    std::cout << "====================" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "Output directory: " << output_dir << std::endl;
    std::cout << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    try {
        // Ingest lineitem (largest table)
        ingest_lineitem(data_dir + "/lineitem.tbl", output_dir);

        // Ingest orders
        ingest_orders(data_dir + "/orders.tbl", output_dir);

        // Ingest customer
        ingest_customer(data_dir + "/customer.tbl", output_dir);

        // Skip unused tables for now
        ingest_part(data_dir + "/part.tbl", output_dir);
        ingest_partsupp(data_dir + "/partsupp.tbl", output_dir);
        ingest_supplier(data_dir + "/supplier.tbl", output_dir);
        ingest_nation(data_dir + "/nation.tbl", output_dir);
        ingest_region(data_dir + "/region.tbl", output_dir);

    } catch (const std::exception& e) {
        std::cerr << "Error during ingestion: " << e.what() << std::endl;
        return 1;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "\n====================" << std::endl;
    std::cout << "Ingestion complete!" << std::endl;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
