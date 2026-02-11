#include "queries.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include <iostream>
#include <iomanip>
#include <chrono>

namespace gendb {

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Q6: Forecasting Revenue Change
    // Filters:
    // - l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    // - l_discount BETWEEN 0.05 AND 0.07
    // - l_quantity < 24

    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    constexpr double discount_min = 0.05;
    constexpr double discount_max = 0.07;
    constexpr double quantity_max = 24.0;

    // Define predicate for filtering
    auto predicate = [date_start, date_end, discount_min, discount_max, quantity_max]
                    (const LineitemTable& table, size_t i) {
        return table.l_shipdate[i] >= date_start &&
               table.l_shipdate[i] < date_end &&
               table.l_discount[i] >= discount_min &&
               table.l_discount[i] <= discount_max &&
               table.l_quantity[i] < quantity_max;
    };

    // Define aggregation function (accumulate revenue)
    auto agg_func = [](double& sum, const LineitemTable& table, size_t i) {
        sum += table.l_extendedprice[i] * table.l_discount[i];
    };

    // Define merge function (sum thread-local results)
    auto merge_func = [](double& dest, const double& src) {
        dest += src;
    };

    // Execute parallel scan with aggregation
    double revenue = parallel_scan_aggregate<LineitemTable, decltype(predicate), double, decltype(agg_func), decltype(merge_func)>(
        lineitem,
        std::move(predicate),
        std::move(agg_func),
        std::move(merge_func)
    );

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print result
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Revenue: " << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
