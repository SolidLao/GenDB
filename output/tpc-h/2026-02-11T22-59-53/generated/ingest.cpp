#include "storage/storage.h"
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <sys/stat.h>

void create_directory(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create output directory
    create_directory(gendb_dir);

    auto overall_start = std::chrono::high_resolution_clock::now();

    std::cout << "Starting TPC-H data ingestion..." << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "Output directory: " << gendb_dir << std::endl;

    // Phase 1: Ingest small dimension tables in parallel
    std::vector<std::thread> phase1;
    phase1.emplace_back([&]() { ingest_nation(data_dir + "/nation.tbl", gendb_dir); });
    phase1.emplace_back([&]() { ingest_region(data_dir + "/region.tbl", gendb_dir); });
    phase1.emplace_back([&]() { ingest_supplier(data_dir + "/supplier.tbl", gendb_dir); });
    phase1.emplace_back([&]() { ingest_part(data_dir + "/part.tbl", gendb_dir); });

    for (auto& th : phase1) {
        th.join();
    }

    // Phase 2: Ingest medium tables
    std::vector<std::thread> phase2;
    phase2.emplace_back([&]() { ingest_customer(data_dir + "/customer.tbl", gendb_dir); });
    phase2.emplace_back([&]() { ingest_partsupp(data_dir + "/partsupp.tbl", gendb_dir); });

    for (auto& th : phase2) {
        th.join();
    }

    // Phase 3: Ingest orders
    ingest_orders(data_dir + "/orders.tbl", gendb_dir);

    // Phase 4: Ingest lineitem (largest table)
    ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir);

    auto overall_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(overall_end - overall_start).count();

    std::cout << "\nIngestion complete!" << std::endl;
    std::cout << "Total time: " << total_elapsed << "s" << std::endl;

    return 0;
}
