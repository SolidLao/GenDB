#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>

namespace gendb {

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Read metadata
    size_t row_count = read_row_count(gendb_dir, "lineitem");

    // Mmap only needed columns
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", row_count);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", row_count);

    // Date filters
    int32_t min_date = parse_date("1994-01-01");
    int32_t max_date = parse_date("1995-01-01");

    // Parallel scan and aggregation
    const int num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::atomic<size_t> next_morsel{0};
    std::vector<double> local_revenue(num_threads, 0.0);

    std::vector<std::thread> threads;
    for (int tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            double local_sum = 0.0;

            while (true) {
                size_t start_idx = next_morsel.fetch_add(morsel_size);
                if (start_idx >= row_count) break;

                size_t end_idx = std::min(start_idx + morsel_size, row_count);

                for (size_t i = start_idx; i < end_idx; ++i) {
                    int32_t shipdate = l_shipdate.data[i];
                    double discount = l_discount.data[i];
                    double quantity = l_quantity.data[i];

                    // Apply filters
                    if (shipdate >= min_date && shipdate < max_date &&
                        discount >= 0.05 && discount <= 0.07 &&
                        quantity < 24.0) {
                        local_sum += l_extendedprice.data[i] * discount;
                    }
                }
            }

            local_revenue[tid] = local_sum;
        });
    }

    for (auto& t : threads) t.join();

    // Merge results
    double revenue = 0.0;
    for (double r : local_revenue) {
        revenue += r;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Output results
    std::cout << "Q6 Results: 1 row in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
        out.close();
    }
}

} // namespace gendb
