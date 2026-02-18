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

    std::cout << "Running Q3..." << std::endl;
    run_q3(gendb_dir, results_dir);

    std::cout << "Running Q5..." << std::endl;
    run_q5(gendb_dir, results_dir);

    std::cout << "Running Q6..." << std::endl;
    run_q6(gendb_dir, results_dir);

    std::cout << "Running Q7..." << std::endl;
    run_q7(gendb_dir, results_dir);

    std::cout << "Running Q9..." << std::endl;
    run_q9(gendb_dir, results_dir);

    std::cout << "Running Q10..." << std::endl;
    run_q10(gendb_dir, results_dir);

    std::cout << "Running Q12..." << std::endl;
    run_q12(gendb_dir, results_dir);

    std::cout << "Running Q13..." << std::endl;
    run_q13(gendb_dir, results_dir);

    std::cout << "Running Q16..." << std::endl;
    run_q16(gendb_dir, results_dir);

    std::cout << "Running Q18..." << std::endl;
    run_q18(gendb_dir, results_dir);

    std::cout << "Running Q20..." << std::endl;
    run_q20(gendb_dir, results_dir);

    std::cout << "Running Q21..." << std::endl;
    run_q21(gendb_dir, results_dir);

    std::cout << "Running Q22..." << std::endl;
    run_q22(gendb_dir, results_dir);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "\nTotal execution time: " << std::fixed << std::setprecision(2) << total_ms << " ms" << std::endl;
    return 0;
}
