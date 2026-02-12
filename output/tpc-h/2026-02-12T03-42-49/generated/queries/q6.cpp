#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <sys/mman.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

namespace gendb {

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load needed columns
    size_t row_count = 0;
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    size_t tmp;
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", tmp);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);

    // Date range: l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    int32_t date_start = parse_date("1994-01-01");
    int32_t date_end = parse_date("1995-01-01");

    // Discount range: [0.05, 0.07]
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold: < 24
    double quantity_max = 24.0;

    // Parallel scan with thread-local accumulation
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 10000;

    std::vector<double> local_revenues(num_threads, 0.0);
    std::atomic<size_t> next_morsel(0);

    auto worker = [&](size_t thread_id) {
        double revenue = 0.0;

        while (true) {
            size_t start_idx = next_morsel.fetch_add(morsel_size);
            if (start_idx >= row_count) break;
            size_t end_idx = std::min(start_idx + morsel_size, row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
                    l_discount[i] >= discount_min && l_discount[i] <= discount_max &&
                    l_quantity[i] < quantity_max) {
                    revenue += l_extendedprice[i] * l_discount[i];
                }
            }
        }

        local_revenues[thread_id] = revenue;
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) t.join();

    // Sum thread-local results
    double total_revenue = 0.0;
    for (double rev : local_revenues) {
        total_revenue += rev;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q6: 1 row in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    // Write to CSV if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << total_revenue << "\n";
    }

    // Cleanup
    munmap(l_shipdate, row_count * sizeof(int32_t));
    munmap(l_discount, row_count * sizeof(double));
    munmap(l_quantity, row_count * sizeof(double));
    munmap(l_extendedprice, row_count * sizeof(double));
}

} // namespace gendb
