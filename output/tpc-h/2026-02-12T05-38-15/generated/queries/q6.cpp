#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"

#include <sys/mman.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

namespace gendb {
namespace queries {

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    size_t num_rows;
    int32_t* l_shipdate = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", num_rows);
    double* l_discount = storage::mmap_column<double>(gendb_dir + "/lineitem_l_discount.bin", num_rows);
    double* l_quantity = storage::mmap_column<double>(gendb_dir + "/lineitem_l_quantity.bin", num_rows);
    double* l_extendedprice = storage::mmap_column<double>(gendb_dir + "/lineitem_l_extendedprice.bin", num_rows);

    // Filter predicates
    int32_t date_start = date_utils::date_to_days(1994, 1, 1);
    int32_t date_end = date_utils::date_to_days(1995, 1, 1);
    double disc_min = 0.05;
    double disc_max = 0.07;
    double qty_max = 24.0;

    // Parallel aggregation using operator library
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<double> local_sums(num_threads, 0.0);

    // Use parallel scan operator
    operators::parallel_scan(num_rows, [&](size_t thread_id, size_t start_row, size_t end_row) {
        double local_sum = 0.0;
        for (size_t i = start_row; i < end_row; ++i) {
            if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
                l_discount[i] >= disc_min && l_discount[i] <= disc_max &&
                l_quantity[i] < qty_max) {
                local_sum += l_extendedprice[i] * l_discount[i];
            }
        }
        local_sums[thread_id] = local_sum;
    });

    // Sum up local results
    double revenue = 0.0;
    for (double s : local_sums) {
        revenue += s;
    }

    // Cleanup mmap
    munmap(l_shipdate, num_rows * sizeof(int32_t));
    munmap(l_discount, num_rows * sizeof(double));
    munmap(l_quantity, num_rows * sizeof(double));
    munmap(l_extendedprice, num_rows * sizeof(double));

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q6: 1 row in " << std::fixed << std::setprecision(3) << elapsed << "s" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
    }
}

} // namespace queries
} // namespace gendb
