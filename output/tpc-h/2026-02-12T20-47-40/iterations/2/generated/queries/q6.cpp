#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <chrono>
#include <cmath>

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: [1994-01-01, 1995-01-01) = [date32 8766, 8766+365)
    int32_t min_date = gendb::DateToArrowDate32(1994, 1, 1);   // 8766
    int32_t max_date = gendb::DateToArrowDate32(1994, 12, 31); // 9130 (inclusive upper bound)

    // Step 1: Scan lineitem with row group pruning on l_shipdate
    // Project only needed columns: l_shipdate, l_quantity, l_discount, l_extendedprice
    gendb::operators::ScanWithDateFilter scan(parquet_dir + "/lineitem.parquet");
    auto table = scan
        .Project({"l_shipdate", "l_quantity", "l_discount", "l_extendedprice"})
        .FilterDate("l_shipdate", min_date, max_date)
        .Execute();

    if (table->num_rows() == 0) {
        std::cout << "Row count: 0" << std::endl;
        std::cout << "revenue: 0.00" << std::endl;
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << "revenue" << std::endl;
            out << "0.00" << std::endl;
        }
        return;
    }

    // Step 2: Apply in-memory filters
    // l_discount BETWEEN 0.05 AND 0.07
    auto discount_col = table->GetColumnByName("l_discount");
    auto discount_combined = arrow::Concatenate(discount_col->chunks()).ValueOrDie();

    auto discount_min = arrow::MakeScalar(arrow::float64(), 0.05);
    auto discount_max = arrow::MakeScalar(arrow::float64(), 0.07);

    auto discount_ge = arrow::compute::CallFunction("greater_equal",
        {arrow::Datum(discount_combined), arrow::Datum(discount_min.ValueOrDie())});
    auto discount_le = arrow::compute::CallFunction("less_equal",
        {arrow::Datum(discount_combined), arrow::Datum(discount_max.ValueOrDie())});
    auto discount_mask = arrow::compute::CallFunction("and_kleene",
        {discount_ge.ValueOrDie(), discount_le.ValueOrDie()});

    // l_quantity < 24
    auto quantity_col = table->GetColumnByName("l_quantity");
    auto quantity_combined = arrow::Concatenate(quantity_col->chunks()).ValueOrDie();

    auto quantity_threshold = arrow::MakeScalar(arrow::float64(), 24.0);
    auto quantity_mask = arrow::compute::CallFunction("less",
        {arrow::Datum(quantity_combined), arrow::Datum(quantity_threshold.ValueOrDie())});

    // Combine all filters with AND
    auto combined_mask = arrow::compute::CallFunction("and_kleene",
        {discount_mask.ValueOrDie(), quantity_mask.ValueOrDie()});

    // Apply filter to table
    auto filtered = arrow::compute::CallFunction("filter",
        {arrow::Datum(table), arrow::Datum(combined_mask.ValueOrDie().make_array())});
    auto filtered_table = filtered.ValueOrDie().table();

    if (filtered_table->num_rows() == 0) {
        std::cout << "Row count: 0" << std::endl;
        std::cout << "revenue: 0.00" << std::endl;
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << "revenue" << std::endl;
            out << "0.00" << std::endl;
        }
        return;
    }

    // Step 3: Compute revenue = l_extendedprice * l_discount
    auto price_col = filtered_table->GetColumnByName("l_extendedprice");
    auto price_combined = arrow::Concatenate(price_col->chunks()).ValueOrDie();

    auto discount_filtered_col = filtered_table->GetColumnByName("l_discount");
    auto discount_filtered_combined = arrow::Concatenate(discount_filtered_col->chunks()).ValueOrDie();

    auto revenue_array = arrow::compute::CallFunction("multiply",
        {arrow::Datum(price_combined), arrow::Datum(discount_filtered_combined)});
    auto revenue_arr = revenue_array.ValueOrDie().make_array();

    // Step 4: SUM(revenue)
    auto sum_result = arrow::compute::CallFunction("sum",
        {arrow::Datum(revenue_arr)});
    auto sum_scalar = sum_result.ValueOrDie().scalar();

    // Handle both DoubleScalar and Decimal128Scalar
    double total_revenue = 0.0;
    if (sum_scalar->type->id() == arrow::Type::DOUBLE) {
        total_revenue = std::static_pointer_cast<arrow::DoubleScalar>(sum_scalar)->value;
    } else if (sum_scalar->type->id() == arrow::Type::DECIMAL128) {
        auto decimal_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(sum_scalar);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(decimal_scalar->type);
        int32_t scale = decimal_type->scale();
        arrow::Decimal128 decimal_val = decimal_scalar->value;
        double divisor = std::pow(10.0, scale);
        total_revenue = static_cast<double>(decimal_val.low_bits()) / divisor;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Row count: 1" << std::endl;
    std::cout << "Query time: " << duration.count() << " ms" << std::endl;

    // Write results
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue" << std::endl;
        out << std::fixed << std::setprecision(2) << total_revenue << std::endl;
    }

    // Print result to stdout
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "revenue: " << total_revenue << std::endl;
}
