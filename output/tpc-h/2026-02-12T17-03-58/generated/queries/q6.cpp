#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'  -- 1 year range
//   AND l_discount BETWEEN 0.05 AND 0.07  -- 0.06 +/- 0.01
//   AND l_quantity < 24

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Read metadata
    auto metadata = Storage::readMetadata(gendb_dir, "lineitem");
    uint64_t row_count = metadata.row_count;

    // Load needed columns
    auto l_shipdate = Storage::mmapColumn(gendb_dir, "lineitem", "l_shipdate", "int32");
    auto l_quantity = Storage::mmapColumn(gendb_dir, "lineitem", "l_quantity", "int64");
    auto l_extendedprice = Storage::mmapColumn(gendb_dir, "lineitem", "l_extendedprice", "int64");
    auto l_discount = Storage::mmapColumn(gendb_dir, "lineitem", "l_discount", "int64");

    const int32_t* shipdate_data = l_shipdate->as<int32_t>();
    const int64_t* quantity_data = l_quantity->as<int64_t>();
    const int64_t* extendedprice_data = l_extendedprice->as<int64_t>();
    const int64_t* discount_data = l_discount->as<int64_t>();

    // Filter ranges
    int32_t min_shipdate = DateUtils::stringToDate("1994-01-01");
    int32_t max_shipdate = DateUtils::stringToDate("1995-01-01");
    int64_t min_discount = 500;  // 0.05 * 10000 (scaled by 100)
    int64_t max_discount = 700;  // 0.07 * 10000
    int64_t max_quantity = 2400; // 24 * 100

    // Compute revenue
    int64_t revenue = 0;
    uint64_t matching_rows = 0;

    for (uint64_t row = 0; row < row_count; ++row) {
        if (shipdate_data[row] >= min_shipdate &&
            shipdate_data[row] < max_shipdate &&
            discount_data[row] >= min_discount &&
            discount_data[row] <= max_discount &&
            quantity_data[row] < max_quantity) {

            // revenue = extendedprice * discount
            revenue += (extendedprice_data[row] * discount_data[row]) / 10000;
            matching_rows++;
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output results
    std::cout << "Q6: 1 row (matched " << matching_rows << " input rows), "
              << duration.count() << " ms" << std::endl;

    // Optionally write to CSV
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/q6_results.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << (revenue / 100.0) << "\n";
        out.close();
    }
}
