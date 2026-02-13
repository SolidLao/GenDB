#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>

// Date conversion helper: days since 1970-01-01 for a given date
inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month-1] + day - 1;
    if (month > 2 && (year%4==0 && (year%100!=0 || year%400==0))) d++;
    return d;
}

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: [1994-01-01, 1995-01-01)
    int32_t date_start = date_to_days(1994, 1, 1);   // 8766 days
    int32_t date_end = date_to_days(1995, 1, 1);     // 9131 days

    // Discount range: [0.05, 0.07]
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold
    double quantity_max = 24.0;

    // Find all lineitem parquet files
    std::vector<std::string> parquet_files;
    std::string lineitem_path = parquet_dir + "/lineitem";

    if (std::filesystem::exists(lineitem_path) && std::filesystem::is_directory(lineitem_path)) {
        for (const auto& entry : std::filesystem::directory_iterator(lineitem_path)) {
            if (entry.path().extension() == ".parquet") {
                parquet_files.push_back(entry.path().string());
            }
        }
    } else {
        std::cerr << "Error: lineitem directory not found at " << lineitem_path << std::endl;
        return;
    }

    if (parquet_files.empty()) {
        std::cerr << "Error: no parquet files found in " << lineitem_path << std::endl;
        return;
    }

    double total_revenue = 0.0;
    int64_t total_rows = 0;

    // Process each parquet file
    for (const auto& filepath : parquet_files) {
        // Open file
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filepath));

        // Create reader
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

        // Get schema to find column indices
        std::shared_ptr<arrow::Schema> schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));

        // Read only the 4 columns we need (column projection for 75% I/O reduction)
        std::vector<int> col_indices = {
            schema->GetFieldIndex("l_shipdate"),
            schema->GetFieldIndex("l_discount"),
            schema->GetFieldIndex("l_quantity"),
            schema->GetFieldIndex("l_extendedprice")
        };

        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(col_indices, &table));

        if (table->num_rows() == 0) continue;

        // Concatenate chunks for each column
        auto shipdate_arr = arrow::Concatenate(table->GetColumnByName("l_shipdate")->chunks()).ValueOrDie();
        auto discount_arr = arrow::Concatenate(table->GetColumnByName("l_discount")->chunks()).ValueOrDie();
        auto quantity_arr = arrow::Concatenate(table->GetColumnByName("l_quantity")->chunks()).ValueOrDie();
        auto price_arr = arrow::Concatenate(table->GetColumnByName("l_extendedprice")->chunks()).ValueOrDie();

        // Convert decimals to double for computation
        arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());

        auto discount_dbl = arrow::compute::CallFunction("cast",
            std::vector<arrow::Datum>{arrow::Datum(discount_arr)},
            &cast_opts).ValueOrDie().make_array();

        auto quantity_dbl = arrow::compute::CallFunction("cast",
            std::vector<arrow::Datum>{arrow::Datum(quantity_arr)},
            &cast_opts).ValueOrDie().make_array();

        auto price_dbl = arrow::compute::CallFunction("cast",
            std::vector<arrow::Datum>{arrow::Datum(price_arr)},
            &cast_opts).ValueOrDie().make_array();

        // Filter 1: l_shipdate >= '1994-01-01'
        auto date_start_scalar = arrow::MakeScalar(arrow::date32(), date_start).ValueOrDie();
        auto mask1 = arrow::compute::CallFunction("greater_equal",
            std::vector<arrow::Datum>{arrow::Datum(shipdate_arr), arrow::Datum(date_start_scalar)}).ValueOrDie().make_array();

        // Filter 2: l_shipdate < '1995-01-01'
        auto date_end_scalar = arrow::MakeScalar(arrow::date32(), date_end).ValueOrDie();
        auto mask2 = arrow::compute::CallFunction("less",
            std::vector<arrow::Datum>{arrow::Datum(shipdate_arr), arrow::Datum(date_end_scalar)}).ValueOrDie().make_array();

        // Combine date filters with AND
        auto date_mask = arrow::compute::CallFunction("and_kleene",
            std::vector<arrow::Datum>{arrow::Datum(mask1), arrow::Datum(mask2)}).ValueOrDie().make_array();

        // Filter 3: l_discount >= 0.05
        auto discount_min_scalar = arrow::MakeScalar(arrow::float64(), discount_min).ValueOrDie();
        auto mask3 = arrow::compute::CallFunction("greater_equal",
            std::vector<arrow::Datum>{arrow::Datum(discount_dbl), arrow::Datum(discount_min_scalar)}).ValueOrDie().make_array();

        // Filter 4: l_discount <= 0.07
        auto discount_max_scalar = arrow::MakeScalar(arrow::float64(), discount_max).ValueOrDie();
        auto mask4 = arrow::compute::CallFunction("less_equal",
            std::vector<arrow::Datum>{arrow::Datum(discount_dbl), arrow::Datum(discount_max_scalar)}).ValueOrDie().make_array();

        // Combine discount filters with AND
        auto discount_mask = arrow::compute::CallFunction("and_kleene",
            std::vector<arrow::Datum>{arrow::Datum(mask3), arrow::Datum(mask4)}).ValueOrDie().make_array();

        // Filter 5: l_quantity < 24
        auto quantity_max_scalar = arrow::MakeScalar(arrow::float64(), quantity_max).ValueOrDie();
        auto quantity_mask = arrow::compute::CallFunction("less",
            std::vector<arrow::Datum>{arrow::Datum(quantity_dbl), arrow::Datum(quantity_max_scalar)}).ValueOrDie().make_array();

        // Combine all filters with AND
        auto combined_mask1 = arrow::compute::CallFunction("and_kleene",
            std::vector<arrow::Datum>{arrow::Datum(date_mask), arrow::Datum(discount_mask)}).ValueOrDie().make_array();
        auto final_mask = arrow::compute::CallFunction("and_kleene",
            std::vector<arrow::Datum>{arrow::Datum(combined_mask1), arrow::Datum(quantity_mask)}).ValueOrDie().make_array();

        // Apply filter to price and discount arrays
        auto filtered_price = arrow::compute::CallFunction("filter",
            std::vector<arrow::Datum>{arrow::Datum(price_dbl), arrow::Datum(final_mask)}).ValueOrDie().make_array();

        auto filtered_discount = arrow::compute::CallFunction("filter",
            std::vector<arrow::Datum>{arrow::Datum(discount_dbl), arrow::Datum(final_mask)}).ValueOrDie().make_array();

        // Compute revenue = l_extendedprice * l_discount
        auto revenue = arrow::compute::CallFunction("multiply",
            std::vector<arrow::Datum>{arrow::Datum(filtered_price), arrow::Datum(filtered_discount)}).ValueOrDie().make_array();

        // Sum the revenue
        auto sum_result = arrow::compute::CallFunction("sum",
            std::vector<arrow::Datum>{arrow::Datum(revenue)}).ValueOrDie().scalar();

        auto sum_scalar = std::static_pointer_cast<arrow::DoubleScalar>(sum_result);
        if (sum_scalar->is_valid) {
            total_revenue += sum_scalar->value;
        }

        total_rows += filtered_price->length();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Write results to CSV
    std::string output_file = results_dir + "/Q6.csv";
    std::ofstream out(output_file);
    if (!out) {
        std::cerr << "Error: cannot open output file " << output_file << std::endl;
        return;
    }

    // Header
    out << "revenue" << std::endl;

    // Data
    out << std::fixed << std::setprecision(2) << total_revenue << std::endl;

    out.close();

    // Print timing and row count
    std::cout << "Q6: " << total_rows << " rows, " << duration.count() << " ms" << std::endl;
}
