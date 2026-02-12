#include "queries/queries.h"
#include <iostream>
#include <string>
#include <chrono>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    std::cout << "GenDB Query Execution" << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    if (!results_dir.empty()) {
        std::cout << "Results directory: " << results_dir << std::endl;
    }
    std::cout << std::endl;

    auto overall_start = std::chrono::high_resolution_clock::now();

    // Execute Q1
    execute_q1(gendb_dir, results_dir);

    // Execute Q3
    execute_q3(gendb_dir, results_dir);

    // Execute Q6
    execute_q6(gendb_dir, results_dir);

    auto overall_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(overall_end - overall_start).count();

    std::cout << "\nTotal query execution time: " << total_elapsed << "s" << std::endl;

    return 0;
}
