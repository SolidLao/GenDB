#include "queries/queries.h"

#include <iostream>
#include <iomanip>
#include <string>
#include <chrono>

using namespace gendb::queries;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    std::cout << "Running TPC-H queries on: " << gendb_dir << std::endl;
    if (!results_dir.empty()) {
        std::cout << "Writing results to: " << results_dir << std::endl;
    }
    std::cout << std::endl;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Run each query
    run_q1(gendb_dir, results_dir);
    run_q3(gendb_dir, results_dir);
    run_q6(gendb_dir, results_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "\nTotal query time: " << std::fixed << std::setprecision(3)
              << total_elapsed << "s" << std::endl;

    return 0;
}
