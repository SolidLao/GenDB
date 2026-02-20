#include <iostream>
#include <iomanip>
#include <string>
#include <chrono>
#include "queries/queries.h"

int main(int argc, char* argv[]) {
    if (argc < 2) { std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl; return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";

    auto total_start = std::chrono::high_resolution_clock::now();

    std::cout << "Running Q1..." << std::endl;
    run_q1(gendb_dir, results_dir);

    std::cout << "Running Q6..." << std::endl;
    run_q6(gendb_dir, results_dir);

    std::cout << "Running Q9..." << std::endl;
    run_q9(gendb_dir, results_dir);

    std::cout << "Running Q18..." << std::endl;
    run_q18(gendb_dir, results_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "\nTotal execution time: " << std::fixed << std::setprecision(2) << total_ms << " ms" << std::endl;
    return 0;
}
