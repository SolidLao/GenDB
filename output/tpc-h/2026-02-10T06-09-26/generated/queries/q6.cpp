#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

namespace gendb {

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: '1994-01-01' to '1994-12-31' (inclusive start, exclusive end in SQL)
    int32_t min_date = parse_date("1994-01-01");
    int32_t max_date = parse_date("1995-01-01");

    // Discount range: [0.05, 0.07]
    double min_discount = 0.05;
    double max_discount = 0.07;

    // Quantity threshold: < 24
    double max_quantity = 24.0;

    double revenue = 0.0;

    size_t n = lineitem.size();
    for (size_t i = 0; i < n; i++) {
        if (lineitem.l_shipdate[i] >= min_date &&
            lineitem.l_shipdate[i] < max_date &&
            lineitem.l_discount[i] >= min_discount &&
            lineitem.l_discount[i] <= max_discount &&
            lineitem.l_quantity[i] < max_quantity) {
            revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << "REVENUE: " << std::fixed << std::setprecision(2) << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
