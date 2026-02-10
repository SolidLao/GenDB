#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

void execute_q6(const LineItem& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: [1994-01-01, 1995-01-01)
    int32_t start_date = date_utils::parse_date("1994-01-01");
    int32_t end_date = date_utils::parse_date("1995-01-01");

    // Discount range: [0.05, 0.07] (0.06 +/- 0.01)
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold
    double quantity_max = 24.0;

    // Single aggregate
    double revenue = 0.0;

    // Scan lineitem with filters
    size_t n = lineitem.size();
    for (size_t i = 0; i < n; ++i) {
        int32_t shipdate = lineitem.l_shipdate[i];
        double discount = lineitem.l_discount[i];
        double quantity = lineitem.l_quantity[i];

        // Apply filters in order of selectivity (most selective first)
        if (shipdate >= start_date && shipdate < end_date) {
            if (discount >= discount_min && discount <= discount_max) {
                if (quantity < quantity_max) {
                    revenue += lineitem.l_extendedprice[i] * discount;
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "revenue: " << revenue << std::endl;
    std::cout << "\nQ6 execution time: " << duration.count() << " ms" << std::endl;
}
