#include "storage/storage.h"
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    mkdir(gendb_dir.c_str(), 0755);

    std::cout << "Starting ingestion from " << data_dir << " to " << gendb_dir << std::endl;
    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest tables in parallel (small tables first, then large)
    std::vector<std::thread> threads;

    threads.emplace_back([&]() {
        ingest_nation(data_dir + "/nation.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_region(data_dir + "/region.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_supplier(data_dir + "/supplier.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_part(data_dir + "/part.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_partsupp(data_dir + "/partsupp.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_customer(data_dir + "/customer.tbl", gendb_dir);
    });

    // Wait for small tables
    for (auto& th : threads) {
        th.join();
    }
    threads.clear();

    // Now ingest large tables
    threads.emplace_back([&]() {
        ingest_orders(data_dir + "/orders.tbl", gendb_dir);
    });

    threads.emplace_back([&]() {
        ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir);
    });

    for (auto& th : threads) {
        th.join();
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "Total ingestion time: " << total_elapsed << "s" << std::endl;

    return 0;
}
