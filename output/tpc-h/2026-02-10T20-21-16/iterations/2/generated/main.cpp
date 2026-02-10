#include "queries/queries.h"
#include <iostream>
#include <chrono>
#include <string>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        std::cerr << "  <gendb_dir>: Directory containing binary columnar storage\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "=== GenDB Query Execution ===\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n";

    auto total_start = std::chrono::high_resolution_clock::now();

    // Execute all queries
    gendb::execute_q1(gendb_dir);
    gendb::execute_q3(gendb_dir);
    gendb::execute_q6(gendb_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);

    std::cout << "\n=== All Queries Complete ===\n";
    std::cout << "Total execution time: " << total_duration.count() << " ms\n";

    return 0;
}
