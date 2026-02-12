#include "storage/storage.h"
#include <cstdio>
#include <cstdlib>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <chrono>

using namespace gendb;

void ensure_directory(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]);
        fprintf(stderr, "  data_dir: directory containing .tbl files\n");
        fprintf(stderr, "  gendb_dir: output directory for binary columnar storage\n");
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    ensure_directory(gendb_dir);

    printf("Starting ingestion from %s to %s\n", data_dir.c_str(), gendb_dir.c_str());
    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest small tables in parallel
    std::vector<std::thread> threads;

    threads.emplace_back([&]() { ingest_nation(data_dir + "/nation.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_region(data_dir + "/region.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_supplier(data_dir + "/supplier.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_part(data_dir + "/part.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_customer(data_dir + "/customer.tbl", gendb_dir); });

    for (auto& t : threads) t.join();
    threads.clear();

    // Ingest partsupp
    ingest_partsupp(data_dir + "/partsupp.tbl", gendb_dir);

    // Ingest large tables (orders and lineitem) in parallel
    threads.emplace_back([&]() { ingest_orders(data_dir + "/orders.tbl", gendb_dir); });
    threads.emplace_back([&]() { ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir); });

    for (auto& t : threads) t.join();

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    printf("\nIngestion complete in %.2fs\n", total_elapsed);
    printf("Binary columnar data written to %s\n", gendb_dir.c_str());

    return 0;
}
