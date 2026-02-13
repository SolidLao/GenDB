#include <iostream>
#include <string>
#include <chrono>
#include "queries/queries.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string parquet_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    auto total_start = std::chrono::high_resolution_clock::now();

    run_q1(parquet_dir, results_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "Total execution time: " << total_ms << " ms" << std::endl;

    return 0;
}
