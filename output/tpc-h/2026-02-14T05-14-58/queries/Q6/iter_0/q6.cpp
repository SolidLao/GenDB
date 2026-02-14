#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <algorithm>

// Parse a single lineitem row from CSV format (pipe-delimited TPC-H format)
// Format: l_orderkey|l_partkey|...|l_quantity(4)|l_extendedprice(5)|l_discount(6)|...|l_shipdate(10)|...
struct LineItemRow {
    int32_t l_quantity;
    double l_extendedprice;
    double l_discount;
    std::string l_shipdate;
};

bool parseLineitemRow(const std::string& line, LineItemRow& row) {
    // Split by pipe delimiter
    std::vector<std::string> fields;
    size_t start = 0;
    size_t end;
    int field_count = 0;

    while ((end = line.find('|', start)) != std::string::npos && field_count < 15) {
        fields.push_back(line.substr(start, end - start));
        start = end + 1;
        field_count++;
    }
    fields.push_back(line.substr(start));

    if (fields.size() < 11) {
        return false;
    }

    try {
        row.l_quantity = std::stoi(fields[4]);
        row.l_extendedprice = std::stod(fields[5]);
        row.l_discount = std::stod(fields[6]);
        row.l_shipdate = fields[10];
        return true;
    } catch (...) {
        return false;
    }
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Find lineitem.tbl file
    std::string tbl_path;
    if (std::ifstream("/home/jl4492/GenDB/benchmarks/tpc-h/data/sf10/lineitem.tbl").good()) {
        tbl_path = "/home/jl4492/GenDB/benchmarks/tpc-h/data/sf10/lineitem.tbl";
    } else {
        std::cerr << "Error: lineitem.tbl not found\n";
        return;
    }

    // Open and read file
    std::ifstream file(tbl_path);
    if (!file.is_open()) {
        std::cerr << "Error opening " << tbl_path << std::endl;
        return;
    }

    // Load all lines into memory for parallel processing
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    file.close();

    size_t total_lines = lines.size();

    // Thread-local accumulators
    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<double> local_sums(num_threads, 0.0);
    std::vector<std::thread> threads;

    // Parallel scan with static assignment
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;

            // Each thread processes every num_threads-th row
            for (size_t i = t; i < total_lines; i += num_threads) {
                LineItemRow row;
                if (!parseLineitemRow(lines[i], row)) {
                    continue;
                }

                // Apply Q6 filters
                // WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
                if (row.l_shipdate >= "1994-01-01" && row.l_shipdate < "1995-01-01") {
                    // AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                    // i.e., BETWEEN 0.05 AND 0.07
                    if (row.l_discount >= 0.05 && row.l_discount <= 0.07) {
                        // AND l_quantity < 24
                        if (row.l_quantity < 24) {
                            // SUM(l_extendedprice * l_discount)
                            local_sum += row.l_extendedprice * row.l_discount;
                        }
                    }
                }
            }

            local_sums[t] = local_sum;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge results from all threads
    double total_revenue = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        total_revenue += local_sums[t];
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(4) << total_revenue << "\n";
        out.close();
    }

    // Print summary
    std::cout << "Query returned 1 row\n";
    std::cout << "Execution time: " << duration_ms << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q6(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
