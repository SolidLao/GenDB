#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: [1994-01-01, 1995-01-01)
    int32_t start_date = date_utils::parse_date("1994-01-01");
    int32_t end_date = date_utils::parse_date("1995-01-01");

    // Discount range: [0.05, 0.07]
    double min_discount = 0.05;
    double max_discount = 0.07;

    // Quantity threshold
    double max_quantity = 24.0;

    // Scan and aggregate
    double revenue = 0.0;
    size_t qualifying_rows = 0;

    size_t n = lineitem.size();
    for (size_t i = 0; i < n; i++) {
        // Apply filters (ordered by selectivity: date first, then discount, then quantity)
        if (lineitem.l_shipdate[i] < start_date || lineitem.l_shipdate[i] >= end_date) {
            continue;
        }

        if (lineitem.l_discount[i] < min_discount || lineitem.l_discount[i] > max_discount) {
            continue;
        }

        if (lineitem.l_quantity[i] >= max_quantity) {
            continue;
        }

        // Compute revenue
        revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
        qualifying_rows++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Revenue: " << revenue << std::endl;
    std::cout << "Qualifying rows: " << qualifying_rows << " / " << n
              << " (" << (100.0 * qualifying_rows / n) << "%)" << std::endl;
    std::cout << "\nExecution time: " << duration.count() << " ms" << std::endl;
}
