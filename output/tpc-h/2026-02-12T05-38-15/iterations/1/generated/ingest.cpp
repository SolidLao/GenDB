#include "storage/storage.h"

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <sys/stat.h>

using namespace gendb::storage;

// Helper to create directory
void create_directory(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directory
    create_directory(output_dir);

    std::cout << "Starting ingestion from " << data_dir << " to " << output_dir << std::endl;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Parallel table ingestion for small tables
    std::vector<std::thread> threads;

    threads.emplace_back([&]() {
        ingest_nation(data_dir + "/nation.tbl", output_dir);
    });

    threads.emplace_back([&]() {
        ingest_region(data_dir + "/region.tbl", output_dir);
    });

    threads.emplace_back([&]() {
        ingest_supplier(data_dir + "/supplier.tbl", output_dir);
    });

    threads.emplace_back([&]() {
        ingest_part(data_dir + "/part.tbl", output_dir);
    });

    threads.emplace_back([&]() {
        ingest_partsupp(data_dir + "/partsupp.tbl", output_dir);
    });

    threads.emplace_back([&]() {
        ingest_customer(data_dir + "/customer.tbl", output_dir);
    });

    // Join small table threads
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    // Ingest large tables sequentially (they're already parallelized internally)
    ingest_orders(data_dir + "/orders.tbl", output_dir);
    ingest_lineitem(data_dir + "/lineitem.tbl", output_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "\nTotal ingestion time: " << total_elapsed << "s" << std::endl;
    std::cout << "Binary data written to: " << output_dir << std::endl;

    return 0;
}
