#include "storage/storage.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <string>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        std::cerr << "  data_dir: Directory containing .tbl files\n";
        std::cerr << "  gendb_dir: Output directory for binary columnar storage\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    std::cout << "=== GenDB Ingestion Program ===\n";
    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n\n";

    // Create output directory
    int ret = system(("mkdir -p " + gendb_dir).c_str());
    (void)ret; // Suppress unused warning

    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest tables in parallel
    std::cout << "Starting parallel ingestion of 3 tables...\n\n";

    LineitemTable lineitem;
    OrdersTable orders;
    CustomerTable customer;

    auto ingest_lineitem = [&]() {
        auto start = std::chrono::high_resolution_clock::now();
        std::cout << "[Lineitem] Reading " << data_dir << "/lineitem.tbl...\n";
        lineitem = ingest_lineitem_tbl(data_dir + "/lineitem.tbl");
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_sec = std::chrono::duration_cast<std::chrono::seconds>(end - start);
        std::cout << "[Lineitem] Parsed " << lineitem.size() << " rows in "
                  << duration_sec.count() << "s\n";

        start = std::chrono::high_resolution_clock::now();
        std::cout << "[Lineitem] Writing binary columns...\n";
        write_lineitem(gendb_dir, lineitem);
        end = std::chrono::high_resolution_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "[Lineitem] Written in " << duration_ms.count() << "ms\n\n";
    };

    auto ingest_orders = [&]() {
        auto start = std::chrono::high_resolution_clock::now();
        std::cout << "[Orders] Reading " << data_dir << "/orders.tbl...\n";
        orders = ingest_orders_tbl(data_dir + "/orders.tbl");
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_sec = std::chrono::duration_cast<std::chrono::seconds>(end - start);
        std::cout << "[Orders] Parsed " << orders.size() << " rows in "
                  << duration_sec.count() << "s\n";

        start = std::chrono::high_resolution_clock::now();
        std::cout << "[Orders] Writing binary columns...\n";
        write_orders(gendb_dir, orders);
        end = std::chrono::high_resolution_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "[Orders] Written in " << duration_ms.count() << "ms\n\n";
    };

    auto ingest_customer = [&]() {
        auto start = std::chrono::high_resolution_clock::now();
        std::cout << "[Customer] Reading " << data_dir << "/customer.tbl...\n";
        customer = ingest_customer_tbl(data_dir + "/customer.tbl");
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "[Customer] Parsed " << customer.size() << " rows in "
                  << duration.count() << "ms\n";

        start = std::chrono::high_resolution_clock::now();
        std::cout << "[Customer] Writing binary columns...\n";
        write_customer(gendb_dir, customer);
        end = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "[Customer] Written in " << duration.count() << "ms\n\n";
    };

    // Parallel ingestion
    std::thread t1(ingest_lineitem);
    std::thread t2(ingest_orders);
    std::thread t3(ingest_customer);

    t1.join();
    t2.join();
    t3.join();

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(total_end - total_start);

    std::cout << "=== Ingestion Complete ===\n";
    std::cout << "Total time: " << total_duration.count() << "s\n";
    std::cout << "Total rows ingested: "
              << (lineitem.size() + orders.size() + customer.size()) << "\n";
    std::cout << "  - Lineitem: " << lineitem.size() << " rows\n";
    std::cout << "  - Orders: " << orders.size() << " rows\n";
    std::cout << "  - Customer: " << customer.size() << " rows\n";

    return 0;
}
