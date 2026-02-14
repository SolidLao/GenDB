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
#include <memory>

// Fast integer parsing without exceptions
// Assumes string starts with digit or sign
inline int32_t fast_atoi32(const char* str, const char* end) {
    int32_t result = 0;
    int sign = 1;

    if (*str == '-') {
        sign = -1;
        ++str;
    }

    while (str < end) {
        result = result * 10 + (*str - '0');
        ++str;
    }

    return result * sign;
}

// Fast double parsing
inline double fast_atod(const char* str, const char* end) {
    double result = 0.0;
    double fraction = 0.1;
    int sign = 1;
    bool in_fraction = false;

    if (*str == '-') {
        sign = -1;
        ++str;
    }

    while (str < end) {
        char c = *str;
        if (c == '.') {
            in_fraction = true;
        } else if (in_fraction) {
            result += (c - '0') * fraction;
            fraction *= 0.1;
        } else {
            result = result * 10.0 + (c - '0');
        }
        ++str;
    }

    return result * sign;
}

// Parse a single lineitem row from CSV format
// Only extracts the fields we need: quantity (field 4), extendedprice (5), discount (6), shipdate (10)
// Format: l_orderkey|l_partkey|...|l_quantity(4)|l_extendedprice(5)|l_discount(6)|...|l_shipdate(10)|...
struct LineItemRow {
    int32_t l_quantity;
    double l_extendedprice;
    double l_discount;
    std::string l_shipdate;
};

void parseLineitemRowFast(const char* line, const char* line_end, LineItemRow& row) {
    const char* field_start = line;
    const char* field_end;
    int field_num = 0;

    // Parse fields, extract only what we need
    while (field_start < line_end && field_num < 11) {
        field_end = field_start;
        while (field_end < line_end && *field_end != '|') {
            ++field_end;
        }

        // Extract needed fields
        if (field_num == 4) {  // l_quantity
            row.l_quantity = fast_atoi32(field_start, field_end);
        } else if (field_num == 5) {  // l_extendedprice
            row.l_extendedprice = fast_atod(field_start, field_end);
        } else if (field_num == 6) {  // l_discount
            row.l_discount = fast_atod(field_start, field_end);
        } else if (field_num == 10) {  // l_shipdate
            row.l_shipdate = std::string(field_start, field_end - field_start);
        }

        field_start = field_end + 1;
        ++field_num;
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

    // Read entire file into memory for efficient parallel processing
    std::ifstream file(tbl_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Error opening " << tbl_path << std::endl;
        return;
    }

    size_t file_size = (size_t)file.tellg();
    file.seekg(0, std::ios::beg);

    auto file_data = std::make_unique<char[]>(file_size);
    if (!file.read(file_data.get(), file_size)) {
        std::cerr << "Error reading file\n";
        return;
    }
    file.close();

    // Pre-parse line boundaries for thread-safe parallel access
    // Find all newline positions
    std::vector<size_t> line_starts;
    line_starts.push_back(0);

    for (size_t i = 0; i < file_size - 1; ++i) {
        if (file_data[i] == '\n') {
            line_starts.push_back(i + 1);
        }
    }

    size_t num_lines = line_starts.size() - 1;  // Don't count the position after last newline
    if (line_starts.back() < file_size) {
        // Handle case where file doesn't end with newline
        num_lines = line_starts.size();
    }

    // Thread-local accumulators with cache-line padding to avoid false sharing
    struct alignas(64) LocalSum {
        double sum;
        char padding[64 - sizeof(double)];
    };

    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<LocalSum> local_sums(num_threads);
    for (auto& ls : local_sums) ls.sum = 0.0;

    std::vector<std::thread> threads;

    // Parallel scan with morsel-driven assignment
    size_t morsel_size = std::max(size_t(50000), num_lines / (num_threads * 8));

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;
            size_t start_line = t * morsel_size;
            size_t end_line = std::min((t + 1) * morsel_size, num_lines);

            if (start_line >= num_lines) return;

            // Q6 filters reordered for maximum selectivity and speed:
            // 1. l_discount BETWEEN 0.05 AND 0.07 (11% selectivity, cheapest check)
            // 2. l_quantity < 24 (48% selectivity, cheap integer comparison)
            // 3. l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01 (15.7% selectivity)

            for (size_t line_idx = start_line; line_idx < end_line; ++line_idx) {
                size_t line_start = line_starts[line_idx];
                size_t line_end = (line_idx + 1 < line_starts.size()) ? line_starts[line_idx + 1] - 1 : file_size;

                if (line_start >= file_size) break;

                const char* line = file_data.get() + line_start;
                const char* line_end_ptr = file_data.get() + line_end;

                LineItemRow row;
                parseLineitemRowFast(line, line_end_ptr, row);

                // Filter 1: Discount (most selective first)
                // 0.05 <= discount <= 0.07
                if (row.l_discount < 0.05 || row.l_discount > 0.07) continue;

                // Filter 2: Quantity (second most selective)
                // l_quantity < 24
                if (row.l_quantity >= 24) continue;

                // Filter 3: Ship date range (15.7% selectivity)
                // l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
                if (row.l_shipdate < "1994-01-01" || row.l_shipdate >= "1995-01-01") continue;

                // All filters passed: accumulate l_extendedprice * l_discount
                local_sum += row.l_extendedprice * row.l_discount;
            }

            local_sums[t].sum = local_sum;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge results from all threads
    double total_revenue = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        total_revenue += local_sums[t].sum;
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
