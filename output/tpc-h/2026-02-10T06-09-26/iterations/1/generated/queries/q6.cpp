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

    // Cache pointers for better performance
    const int32_t* shipdate = lineitem.l_shipdate.data();
    const double* price = lineitem.l_extendedprice.data();
    const double* discount = lineitem.l_discount.data();
    const double* quantity = lineitem.l_quantity.data();

    size_t n = lineitem.size();

    // Use zone maps to skip blocks
    const auto& zonemap = lineitem.shipdate_zonemap;
    size_t block_size = zonemap.block_size;
    size_t num_blocks = zonemap.block_min.size();

    for (size_t block = 0; block < num_blocks; block++) {
        // Skip blocks that don't overlap with [min_date, max_date)
        if (zonemap.block_max[block] < min_date || zonemap.block_min[block] >= max_date) {
            continue;
        }

        size_t start = block * block_size;
        size_t end = std::min(start + block_size, n);

        for (size_t i = start; i < end; i++) {
            int32_t date = shipdate[i];
            double disc = discount[i];
            double qty = quantity[i];

            // Combined condition for better branch prediction
            if (date >= min_date && date < max_date &&
                disc >= min_discount && disc <= max_discount &&
                qty < max_quantity) {
                revenue += price[i] * disc;
            }
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
