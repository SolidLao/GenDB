#include "queries/queries.h"
#include <iostream>
#include <string>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    std::cout << "Executing TPC-H queries from " << gendb_dir << std::endl;

    // Execute queries
    execute_q1(gendb_dir, results_dir);
    execute_q3(gendb_dir, results_dir);
    execute_q6(gendb_dir, results_dir);

    return 0;
}
