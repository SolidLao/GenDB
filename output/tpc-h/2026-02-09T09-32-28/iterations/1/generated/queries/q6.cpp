#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= date '1994-01-01'
//   AND l_shipdate < date '1994-01-01' + interval '1' year
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Parse filter dates and bounds
    int32_t shipdate_start = parse_date("1994-01-01");
    int32_t shipdate_end = parse_date("1995-01-01");
    double discount_min = 0.05;
    double discount_max = 0.07;
    double quantity_max = 24.0;

    // Scan and aggregate with reordered predicates by selectivity
    // Order: (1) l_discount BETWEEN (most selective, ~10% rows)
    //        (2) l_quantity < 24 (~40%)
    //        (3) l_shipdate range (~17% of full dataset)
    // Use short-circuit evaluation in a single if statement for better branch prediction
    double revenue = 0.0;

    for (size_t i = 0; i < lineitem.size(); ++i) {
        if (lineitem.l_discount[i] >= discount_min &&
            lineitem.l_discount[i] <= discount_max &&
            lineitem.l_quantity[i] < quantity_max &&
            lineitem.l_shipdate[i] >= shipdate_start &&
            lineitem.l_shipdate[i] < shipdate_end) {
            revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Revenue: " << revenue << "\n";
    std::cout << "Execution time: " << duration << " ms\n";
}
