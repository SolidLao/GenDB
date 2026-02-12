#include "queries/queries.h"
#include <iostream>
#include <iomanip>
#include <string>
#include <chrono>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    if (!results_dir.empty()) {
        std::string mkdir_cmd = "mkdir -p " + results_dir;
        system(mkdir_cmd.c_str());
    }

    std::cout << "Executing TPC-H queries on " << gendb_dir << "\n";
    if (!results_dir.empty()) {
        std::cout << "Writing results to " << results_dir << "\n";
    }
    std::cout << "\n";

    auto total_start = std::chrono::high_resolution_clock::now();

    // Execute queries
    execute_q1(gendb_dir, results_dir);
    execute_q3(gendb_dir, results_dir);
    execute_q6(gendb_dir, results_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "\nTotal execution time: " << std::fixed << std::setprecision(3) << total_elapsed << "s\n";

    return 0;
}
