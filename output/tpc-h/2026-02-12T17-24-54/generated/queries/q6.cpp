#include "queries.h"
#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>

namespace gendb {

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns
    size_t row_count;
    auto l_shipdate = ColumnReader::mmap_int32(gendb_dir + "/lineitem.l_shipdate.bin", row_count);
    auto l_discount = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_discount.bin", row_count);
    auto l_quantity = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_quantity.bin", row_count);
    auto l_extendedprice = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_extendedprice.bin", row_count);

    // Parallel scan and aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<int64_t> local_sums(num_threads, 0);

    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    // Filters:
    // l_shipdate >= '1994-01-01' AND < '1995-01-01'
    // l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7)
    // l_quantity < 24 (scaled: 2400)

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            size_t start_row = t * chunk_size;
            size_t end_row = std::min(start_row + chunk_size, row_count);

            int64_t local_sum = 0;
            for (size_t i = start_row; i < end_row; ++i) {
                if (l_shipdate[i] >= DATE_1994_01_01 &&
                    l_shipdate[i] < DATE_1995_01_01 &&
                    l_discount[i] >= 5 && l_discount[i] <= 7 &&
                    l_quantity[i] < 2400) {
                    // revenue = l_extendedprice * l_discount
                    // Both scaled by 100, so result is scaled by 10000
                    local_sum += l_extendedprice[i] * l_discount[i];
                }
            }
            local_sums[t] = local_sum;
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Reduce
    int64_t total_revenue = 0;
    for (int64_t sum : local_sums) {
        total_revenue += sum;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "Q6: 1 row in " << duration << " ms\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/q6.csv");
        out << std::fixed << std::setprecision(2);
        // Divide by 10000 to unscale (100 * 100)
        out << (total_revenue / 10000.0) << "\n";
    }

    // Cleanup
    ColumnReader::unmap(l_shipdate, row_count * sizeof(int32_t));
    ColumnReader::unmap(l_discount, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_quantity, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_extendedprice, row_count * sizeof(int64_t));
}

} // namespace gendb
