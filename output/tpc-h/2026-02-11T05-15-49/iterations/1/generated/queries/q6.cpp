#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>

namespace gendb {

void execute_q6(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "\n=== Q6: Forecasting Revenue Change ===" << std::endl;

    // Load required columns via mmap
    auto l_shipdate = mmap_int32_column(gendb_dir, "lineitem", "l_shipdate");
    auto l_discount = mmap_double_column(gendb_dir, "lineitem", "l_discount");
    auto l_quantity = mmap_double_column(gendb_dir, "lineitem", "l_quantity");
    auto l_extendedprice = mmap_double_column(gendb_dir, "lineitem", "l_extendedprice");

    size_t n = l_shipdate.size;

    // Date range: '1994-01-01' to '1994-12-31'
    int32_t date_start = date_to_days("1994-01-01");
    int32_t date_end = date_to_days("1995-01-01");  // Exclusive upper bound

    // Discount range: 0.05 to 0.07
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold
    double quantity_max = 24.0;

    // Parallel aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<double> local_revenues(num_threads, 0.0);

    size_t chunk_size = (n + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, n);

            double local_revenue = 0.0;

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Filter: l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
                if (l_shipdate.data[i] < date_start || l_shipdate.data[i] >= date_end) continue;

                // Filter: l_discount BETWEEN 0.05 AND 0.07
                double disc = l_discount.data[i];
                if (disc < discount_min || disc > discount_max) continue;

                // Filter: l_quantity < 24
                if (l_quantity.data[i] >= quantity_max) continue;

                // Compute revenue
                local_revenue += l_extendedprice.data[i] * disc;
            }

            local_revenues[tid] = local_revenue;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Sum up thread-local revenues
    double total_revenue = 0.0;
    for (double rev : local_revenues) {
        total_revenue += rev;
    }

    // Print result
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nREVENUE" << std::endl;
    std::cout << std::string(15, '-') << std::endl;
    std::cout << total_revenue << std::endl;

    // Cleanup
    unmap_column(l_shipdate.mmap_ptr, l_shipdate.mmap_size);
    unmap_column(l_discount.mmap_ptr, l_discount.mmap_size);
    unmap_column(l_quantity.mmap_ptr, l_quantity.mmap_size);
    unmap_column(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "\nQ6 execution time: " << elapsed.count() << " seconds" << std::endl;
}

}  // namespace gendb
