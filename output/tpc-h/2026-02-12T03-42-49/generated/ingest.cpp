#include "storage/storage.h"
#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <vector>
#include <string>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    std::string mkdir_cmd = "mkdir -p " + gendb_dir;
    system(mkdir_cmd.c_str());

    std::cout << "Ingesting TPC-H data from " << data_dir << " to " << gendb_dir << "\n";

    auto start = std::chrono::high_resolution_clock::now();

    // Parallel ingestion of small tables
    std::vector<std::thread> threads;

    threads.emplace_back([&]() { ingest_nation(data_dir + "/nation.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_region(data_dir + "/region.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_supplier(data_dir + "/supplier.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_part(data_dir + "/part.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_partsupp(data_dir + "/partsupp.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_customer(data_dir + "/customer.tbl", gendb_dir); });

    for (auto& t : threads) t.join();
    threads.clear();

    // Large tables sequentially (they are already parallelized internally)
    ingest_orders(data_dir + "/orders.tbl", gendb_dir);
    ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "\nIngestion complete in " << std::fixed << std::setprecision(1) << elapsed << "s\n";

    return 0;
}
