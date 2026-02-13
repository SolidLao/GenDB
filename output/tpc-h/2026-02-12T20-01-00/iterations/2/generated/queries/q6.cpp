#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>

using namespace gendb;
using namespace gendb::operators;

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Scan lineitem with date filter and column projection
    // Read only 4 columns: l_shipdate, l_quantity, l_extendedprice, l_discount
    // Use row group pruning for l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'

    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    // Convert dates to Arrow date32 format (days since 1970-01-01)
    int32_t min_date = DateToArrowDate32(1994, 1, 1);   // 1994-01-01
    int32_t max_date = DateToArrowDate32(1994, 12, 31); // 1994-12-31

    ScanWithDateFilter scan(lineitem_path);
    auto table = scan
        .Project({"l_shipdate", "l_quantity", "l_extendedprice", "l_discount"})
        .FilterDate("l_shipdate", min_date, max_date)
        .Execute();

    std::cout << "After date filter: " << table->num_rows() << " rows" << std::endl;

    // Step 2: Apply additional filters using Arrow compute
    // l_discount BETWEEN 0.05 AND 0.07 (0.06 - 0.01 to 0.06 + 0.01)
    // l_quantity < 24

    // Get column arrays
    auto l_quantity_chunked = table->GetColumnByName("l_quantity");
    auto l_discount_chunked = table->GetColumnByName("l_discount");
    auto l_extendedprice_chunked = table->GetColumnByName("l_extendedprice");

    // Concatenate chunks
    auto l_quantity_result = arrow::Concatenate(l_quantity_chunked->chunks());
    auto l_discount_result = arrow::Concatenate(l_discount_chunked->chunks());
    auto l_extendedprice_result = arrow::Concatenate(l_extendedprice_chunked->chunks());

    if (!l_quantity_result.ok() || !l_discount_result.ok() || !l_extendedprice_result.ok()) {
        throw std::runtime_error("Failed to concatenate chunks");
    }

    auto l_quantity = l_quantity_result.ValueOrDie();
    auto l_discount = l_discount_result.ValueOrDie();
    auto l_extendedprice = l_extendedprice_result.ValueOrDie();

    // Build filter mask: l_quantity < 24
    arrow::compute::ExecContext ctx;

    auto quantity_scalar = arrow::MakeScalar(l_quantity->type(), int64_t(24));
    if (!quantity_scalar.ok()) {
        throw std::runtime_error("Failed to create quantity scalar");
    }

    auto quantity_filter_result = arrow::compute::CallFunction(
        "less",
        {l_quantity, quantity_scalar.ValueOrDie()},
        &ctx
    );

    if (!quantity_filter_result.ok()) {
        throw std::runtime_error("Failed to apply quantity filter: " + quantity_filter_result.status().ToString());
    }

    // Build filter mask: l_discount >= 0.05
    auto discount_min_scalar = arrow::MakeScalar(l_discount->type(), 0.05);
    if (!discount_min_scalar.ok()) {
        throw std::runtime_error("Failed to create discount min scalar");
    }

    auto discount_ge_result = arrow::compute::CallFunction(
        "greater_equal",
        {l_discount, discount_min_scalar.ValueOrDie()},
        &ctx
    );

    if (!discount_ge_result.ok()) {
        throw std::runtime_error("Failed to apply discount >= filter: " + discount_ge_result.status().ToString());
    }

    // Build filter mask: l_discount <= 0.07
    auto discount_max_scalar = arrow::MakeScalar(l_discount->type(), 0.07);
    if (!discount_max_scalar.ok()) {
        throw std::runtime_error("Failed to create discount max scalar");
    }

    auto discount_le_result = arrow::compute::CallFunction(
        "less_equal",
        {l_discount, discount_max_scalar.ValueOrDie()},
        &ctx
    );

    if (!discount_le_result.ok()) {
        throw std::runtime_error("Failed to apply discount <= filter: " + discount_le_result.status().ToString());
    }

    // Combine all three predicates using and_kleene
    auto discount_between_result = arrow::compute::CallFunction(
        "and_kleene",
        {discount_ge_result.ValueOrDie().make_array(), discount_le_result.ValueOrDie().make_array()},
        &ctx
    );

    if (!discount_between_result.ok()) {
        throw std::runtime_error("Failed to combine discount predicates: " + discount_between_result.status().ToString());
    }

    auto combined_filter_result = arrow::compute::CallFunction(
        "and_kleene",
        {quantity_filter_result.ValueOrDie().make_array(), discount_between_result.ValueOrDie().make_array()},
        &ctx
    );

    if (!combined_filter_result.ok()) {
        throw std::runtime_error("Failed to combine all predicates: " + combined_filter_result.status().ToString());
    }

    auto filter_mask = combined_filter_result.ValueOrDie().make_array();

    // Apply filter to l_extendedprice and l_discount
    auto filtered_price_result = arrow::compute::CallFunction(
        "filter",
        {l_extendedprice, filter_mask},
        &ctx
    );

    auto filtered_discount_result = arrow::compute::CallFunction(
        "filter",
        {l_discount, filter_mask},
        &ctx
    );

    if (!filtered_price_result.ok() || !filtered_discount_result.ok()) {
        throw std::runtime_error("Failed to filter arrays");
    }

    auto filtered_price = filtered_price_result.ValueOrDie().make_array();
    auto filtered_discount = filtered_discount_result.ValueOrDie().make_array();

    std::cout << "After all filters: " << filtered_price->length() << " rows" << std::endl;

    // Step 3: Compute revenue = SUM(l_extendedprice * l_discount)
    // Fuse multiplication with sum to avoid materializing intermediate array

    auto multiply_result = arrow::compute::CallFunction(
        "multiply",
        {filtered_price, filtered_discount},
        &ctx
    );

    if (!multiply_result.ok()) {
        throw std::runtime_error("Failed to multiply arrays: " + multiply_result.status().ToString());
    }

    auto revenue_array = multiply_result.ValueOrDie().make_array();

    // Sum the revenue
    auto sum_result = arrow::compute::CallFunction(
        "sum",
        {revenue_array},
        &ctx
    );

    if (!sum_result.ok()) {
        throw std::runtime_error("Failed to sum revenue: " + sum_result.status().ToString());
    }

    auto revenue_scalar = sum_result.ValueOrDie().scalar();
    double revenue = 0.0;

    // Extract the scalar value
    if (revenue_scalar->is_valid) {
        if (revenue_scalar->type->id() == arrow::Type::DOUBLE) {
            revenue = std::static_pointer_cast<arrow::DoubleScalar>(revenue_scalar)->value;
        } else if (revenue_scalar->type->id() == arrow::Type::DECIMAL128) {
            // Handle decimal type
            auto decimal_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(revenue_scalar);
            auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(revenue_scalar->type);
            std::string str = decimal_scalar->value.ToString(decimal_type->scale());
            revenue = std::stod(str);
        } else {
            throw std::runtime_error("Unexpected sum result type: " + revenue_scalar->type->ToString());
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Output results
    std::cout << "Q6 Results:" << std::endl;
    std::cout << "Revenue: " << std::fixed << std::setprecision(2) << revenue << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q6.csv";
        std::ofstream outfile(output_path);

        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open output file: " + output_path);
        }

        // Write header
        outfile << "revenue" << std::endl;

        // Write result
        outfile << std::fixed << std::setprecision(2) << revenue << std::endl;

        outfile.close();
        std::cout << "Results written to " << output_path << std::endl;
    }
}
