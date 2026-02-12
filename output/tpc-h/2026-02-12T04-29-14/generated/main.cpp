#include "queries/queries.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    // Create results directory if specified
    if (!results_dir.empty()) {
        system(("mkdir -p " + results_dir).c_str());
    }

    std::cout << "Executing TPC-H queries on " << gendb_dir << std::endl;
    std::cout << "========================================" << std::endl;

    // Execute queries
    gendb::execute_q1(gendb_dir, results_dir);
    gendb::execute_q3(gendb_dir, results_dir);
    gendb::execute_q6(gendb_dir, results_dir);

    std::cout << "========================================" << std::endl;
    std::cout << "All queries completed successfully." << std::endl;

    return 0;
}
