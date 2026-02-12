#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01'
//   AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load columns
    size_t row_count = get_row_count(gendb_dir, "lineitem");

    size_t size;
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", size);
    double* l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", size);
    double* l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", size);
    double* l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", size);

    // Date thresholds
    int32_t date_start = date_utils::date_to_days(1994, 1, 1);
    int32_t date_end = date_utils::date_to_days(1995, 1, 1);

    // Use parallel scalar aggregation operator with predicate pushdown
    double total_revenue = gendb::operators::parallel_scalar_aggregate_filtered<double>(
        row_count,
        // Predicate: filter rows
        [&](size_t i) {
            return l_shipdate[i] >= date_start &&
                   l_shipdate[i] < date_end &&
                   l_discount[i] >= 0.05 &&
                   l_discount[i] <= 0.07 &&
                   l_quantity[i] < 24.0;
        },
        // Aggregate: compute revenue for each row
        [&](size_t i, double& revenue) {
            revenue += l_extendedprice[i] * l_discount[i];
        },
        // Merge: sum local revenues
        [](double& dest, const double& src) {
            dest += src;
        },
        0.0  // Initial value
    );

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q6: 1 row in " << elapsed << "s" << std::endl;

    // Write results if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2);
        out << total_revenue << "\n";
    }
}
