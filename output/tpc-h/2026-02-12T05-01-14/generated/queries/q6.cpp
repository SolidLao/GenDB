#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>

namespace gendb {

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns
    const int32_t* l_shipdate;
    const double* l_quantity;
    const double* l_extendedprice;
    const double* l_discount;
    size_t count;

    load_lineitem_columns_q6(gendb_dir, &l_shipdate, &l_quantity, &l_extendedprice, &l_discount, count);

    // Date filters: >= 1994-01-01 AND < 1995-01-01
    int32_t date_start = parse_date("1994-01-01");
    int32_t date_end = parse_date("1995-01-01");

    // Parallel reduction
    int num_threads = std::min(16, (int)std::thread::hardware_concurrency());
    std::vector<double> thread_local_sums(num_threads, 0.0);

    auto worker = [&](int thread_id) {
        size_t chunk_size = (count + num_threads - 1) / num_threads;
        size_t start = thread_id * chunk_size;
        size_t end = std::min(start + chunk_size, count);

        double local_sum = 0.0;
        for (size_t i = start; i < end; ++i) {
            if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
                l_discount[i] >= 0.05 && l_discount[i] <= 0.07 &&
                l_quantity[i] < 24.0) {
                local_sum += l_extendedprice[i] * l_discount[i];
            }
        }
        thread_local_sums[thread_id] = local_sum;
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) {
        th.join();
    }

    // Merge results
    double revenue = 0.0;
    for (double s : thread_local_sums) {
        revenue += s;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // Write results if requested
    if (!results_dir.empty()) {
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (f) {
            fprintf(f, "revenue\n");
            fprintf(f, "%.2f\n", revenue);
            fclose(f);
        }
    }

    // Print summary
    std::cout << "Q6: 1 row in " << std::fixed << std::setprecision(3) << elapsed << "s" << std::endl;
}

} // namespace gendb
