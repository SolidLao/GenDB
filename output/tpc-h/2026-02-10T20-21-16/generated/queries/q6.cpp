#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

namespace gendb {

// Q6: Forecasting Revenue Change
void execute_q6(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    LineitemTable lineitem;
    std::vector<std::string> columns = {
        "l_shipdate", "l_discount", "l_quantity", "l_extendedprice"
    };
    load_lineitem(gendb_dir, lineitem, columns);

    // Filter predicates
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    double discount_min = 0.05;
    double discount_max = 0.07;
    double quantity_max = 24.0;

    // Scan and aggregate
    double revenue = 0.0;
    size_t n = lineitem.l_shipdate.size();

    for (size_t i = 0; i < n; i++) {
        if (lineitem.l_shipdate[i] >= date_start &&
            lineitem.l_shipdate[i] < date_end &&
            lineitem.l_discount[i] >= discount_min &&
            lineitem.l_discount[i] <= discount_max &&
            lineitem.l_quantity[i] < quantity_max) {
            revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print result
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "revenue: " << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
