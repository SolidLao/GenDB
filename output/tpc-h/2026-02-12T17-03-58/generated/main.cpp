#include "queries/queries.h"
#include <iostream>
#include <chrono>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = "";
    if (argc == 3) {
        results_dir = argv[2];
    }

    std::cout << "=== GenDB Query Executor ===" << std::endl;
    std::cout << "GenDB Directory: " << gendb_dir << std::endl;
    if (!results_dir.empty()) {
        std::cout << "Results Directory: " << results_dir << std::endl;
    }
    std::cout << std::endl;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Execute Q1
    std::cout << "Executing Q1..." << std::endl;
    execute_q1(gendb_dir, results_dir);
    std::cout << std::endl;

    // Execute Q3
    std::cout << "Executing Q3..." << std::endl;
    execute_q3(gendb_dir, results_dir);
    std::cout << std::endl;

    // Execute Q6
    std::cout << "Executing Q6..." << std::endl;
    execute_q6(gendb_dir, results_dir);
    std::cout << std::endl;

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);

    std::cout << "=== Total execution time: " << total_duration.count() << " ms ===" << std::endl;

    return 0;
}
