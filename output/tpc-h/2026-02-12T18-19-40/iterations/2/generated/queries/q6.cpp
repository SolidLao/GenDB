#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <sys/mman.h>

// Q6: Forecasting Revenue Change Query
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only the 4 columns needed for this query
    storage::MappedColumn l_shipdate_col, l_discount_col, l_quantity_col, l_extendedprice_col;

    l_shipdate_col.open(gendb_dir + "/lineitem.l_shipdate");
    l_discount_col.open(gendb_dir + "/lineitem.l_discount");
    l_quantity_col.open(gendb_dir + "/lineitem.l_quantity");
    l_extendedprice_col.open(gendb_dir + "/lineitem.l_extendedprice");

    // Get pointers to the data (skip 8-byte size header)
    const int32_t* l_shipdate = reinterpret_cast<const int32_t*>(
        static_cast<const char*>(l_shipdate_col.data) + sizeof(size_t));
    const int64_t* l_discount = reinterpret_cast<const int64_t*>(
        static_cast<const char*>(l_discount_col.data) + sizeof(size_t));
    const int64_t* l_quantity = reinterpret_cast<const int64_t*>(
        static_cast<const char*>(l_quantity_col.data) + sizeof(size_t));
    const int64_t* l_extendedprice = reinterpret_cast<const int64_t*>(
        static_cast<const char*>(l_extendedprice_col.data) + sizeof(size_t));

    size_t row_count = l_shipdate_col.count<int32_t>() - 1; // -1 for size header

    // Convert date strings to days since epoch
    int32_t min_shipdate = date_utils::date_to_days("1994-01-01");
    int32_t max_shipdate = date_utils::date_to_days("1995-01-01");

    // Discount range: 0.05 to 0.07 (scaled by 100 → 5 to 7)
    int64_t min_discount = 5;
    int64_t max_discount = 7;

    // Quantity threshold: < 24 (scaled by 100 → < 2400)
    int64_t max_quantity = 2400;

    // Step 1: Use scan_range on sorted l_shipdate to find candidate rows
    // Since l_shipdate is sorted, we use binary search (scan_range excludes max_val)
    std::vector<size_t> date_filtered = operators::scan_range(
        l_shipdate, row_count, min_shipdate, max_shipdate - 1);

    // Step 2 & 3: Apply remaining filters and fuse aggregation into scan loop
    // This is the tightest loop - optimized for branch prediction and cache locality
    int64_t revenue_sum = 0;
    size_t qualified_rows = 0;

    for (size_t idx : date_filtered) {
        int64_t discount = l_discount[idx];
        int64_t quantity = l_quantity[idx];

        // Fused filter predicates
        if (discount >= min_discount && discount <= max_discount && quantity < max_quantity) {
            // Inline aggregation: revenue = l_extendedprice * l_discount / 10000
            // (both are scaled by 100, so divide by 10000 to get actual product)
            revenue_sum += l_extendedprice[idx] * discount;
            qualified_rows++;
        }
    }

    // Convert revenue from scaled (×10000) to actual value (÷10000)
    double revenue = static_cast<double>(revenue_sum) / 10000.0;

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Write results to CSV if output directory is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
        out.close();
    }

    // Print row count and timing to stdout
    std::cout << "Q6: " << qualified_rows << " rows, ";
    std::cout << "revenue = " << std::fixed << std::setprecision(2) << revenue;
    std::cout << " (" << elapsed.count() << " ms)\n";
}
