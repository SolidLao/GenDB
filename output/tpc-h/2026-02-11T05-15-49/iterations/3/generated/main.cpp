#include "queries/queries.h"
#include <iostream>
#include <chrono>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        std::cerr << "  gendb_dir: directory containing binary column files" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Query Execution" << std::endl;
    std::cout << "=====================" << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    try {
        // Execute Q1: Pricing Summary Report
        execute_q1(gendb_dir);

        // Execute Q3: Shipping Priority
        execute_q3(gendb_dir);

        // Execute Q6: Forecasting Revenue Change
        execute_q6(gendb_dir);

    } catch (const std::exception& e) {
        std::cerr << "Error during query execution: " << e.what() << std::endl;
        return 1;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "\n=====================" << std::endl;
    std::cout << "All queries complete!" << std::endl;
    std::cout << "Total execution time: " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
