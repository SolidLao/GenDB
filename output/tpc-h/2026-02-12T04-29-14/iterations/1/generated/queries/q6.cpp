#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <vector>

namespace gendb {

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01'
//   AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns via mmap
    size_t row_count;
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", row_count);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", row_count);

    // Filter predicates
    int32_t date_start = parse_date("1994-01-01");
    int32_t date_end = parse_date("1995-01-01");
    double disc_min = 0.05;
    double disc_max = 0.07;
    double qty_max = 24.0;

    // Use parallel scan with local state
    auto filter = [&](size_t i) -> bool {
        return l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
               l_discount[i] >= disc_min && l_discount[i] <= disc_max &&
               l_quantity[i] < qty_max;
    };

    auto process = [&](double& local_sum, size_t i) {
        local_sum += l_extendedprice[i] * l_discount[i];
    };

    // Perform parallel scan with aggregation
    double revenue = 0.0;
    operators::parallel_scan_with_local<double>(
        row_count,
        process,
        filter,
        [&revenue](const double& local_sum) {
            revenue += local_sum;
        }
    );

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print to terminal: only row count (1 result row) and timing
    std::cout << "Q6: 1 row in " << std::fixed << std::setprecision(3)
              << elapsed << " seconds" << std::endl;

    // Write results to file if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
    }
}

} // namespace gendb
