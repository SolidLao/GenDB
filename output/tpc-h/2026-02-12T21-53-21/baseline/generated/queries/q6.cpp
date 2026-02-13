#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <ctime>

// Date conversion helper
inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month-1] + day - 1;
    if (month > 2 && (year%4==0 && (year%100!=0 || year%400==0))) d++;
    return d;
}

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Open lineitem.parquet
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(lineitem_path));

    // Create Parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    // Get schema to find column indices
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));

    // Read only required columns: l_shipdate, l_discount, l_extendedprice, l_quantity
    std::vector<int> col_indices = {
        schema->GetFieldIndex("l_shipdate"),
        schema->GetFieldIndex("l_discount"),
        schema->GetFieldIndex("l_extendedprice"),
        schema->GetFieldIndex("l_quantity")
    };

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(col_indices, &table));

    // Filter 1: l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    auto shipdate_chunked = table->GetColumnByName("l_shipdate");
    auto shipdate_array = arrow::Concatenate(shipdate_chunked->chunks()).ValueOrDie();

    int32_t date_start = date_to_days(1994, 1, 1);  // 1994-01-01
    int32_t date_end = date_to_days(1995, 1, 1);    // 1995-01-01

    auto start_scalar = arrow::MakeScalar(arrow::date32(), date_start).ValueOrDie();
    auto end_scalar = arrow::MakeScalar(arrow::date32(), date_end).ValueOrDie();

    auto mask_ge = arrow::compute::CallFunction("greater_equal",
        {arrow::Datum(shipdate_array), arrow::Datum(start_scalar)}).ValueOrDie().make_array();
    auto mask_lt = arrow::compute::CallFunction("less",
        {arrow::Datum(shipdate_array), arrow::Datum(end_scalar)}).ValueOrDie().make_array();
    auto mask_date = arrow::compute::CallFunction("and_kleene",
        {arrow::Datum(mask_ge), arrow::Datum(mask_lt)}).ValueOrDie().make_array();

    // Apply date filter
    auto filtered = arrow::compute::CallFunction("filter",
        {arrow::Datum(table), arrow::Datum(mask_date)}).ValueOrDie().table();

    // Filter 2: l_discount BETWEEN 0.05 AND 0.07
    auto discount_chunked = filtered->GetColumnByName("l_discount");
    auto discount_array = arrow::Concatenate(discount_chunked->chunks()).ValueOrDie();

    // Cast decimal to double for comparison
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());
    auto discount_dbl = arrow::compute::CallFunction("cast",
        {arrow::Datum(discount_array)}, &cast_opts).ValueOrDie().make_array();

    auto disc_low = arrow::MakeScalar(arrow::float64(), 0.05).ValueOrDie();
    auto disc_high = arrow::MakeScalar(arrow::float64(), 0.07).ValueOrDie();

    auto mask_ge_disc = arrow::compute::CallFunction("greater_equal",
        {arrow::Datum(discount_dbl), arrow::Datum(disc_low)}).ValueOrDie().make_array();
    auto mask_le_disc = arrow::compute::CallFunction("less_equal",
        {arrow::Datum(discount_dbl), arrow::Datum(disc_high)}).ValueOrDie().make_array();
    auto mask_discount = arrow::compute::CallFunction("and_kleene",
        {arrow::Datum(mask_ge_disc), arrow::Datum(mask_le_disc)}).ValueOrDie().make_array();

    filtered = arrow::compute::CallFunction("filter",
        {arrow::Datum(filtered), arrow::Datum(mask_discount)}).ValueOrDie().table();

    // Filter 3: l_quantity < 24
    auto quantity_chunked = filtered->GetColumnByName("l_quantity");
    auto quantity_array = arrow::Concatenate(quantity_chunked->chunks()).ValueOrDie();

    // Cast decimal to double for comparison
    arrow::compute::CastOptions cast_opts_qty = arrow::compute::CastOptions::Safe(arrow::float64());
    auto quantity_dbl = arrow::compute::CallFunction("cast",
        {arrow::Datum(quantity_array)}, &cast_opts_qty).ValueOrDie().make_array();

    auto qty_threshold = arrow::MakeScalar(arrow::float64(), 24.0).ValueOrDie();
    auto mask_qty = arrow::compute::CallFunction("less",
        {arrow::Datum(quantity_dbl), arrow::Datum(qty_threshold)}).ValueOrDie().make_array();

    filtered = arrow::compute::CallFunction("filter",
        {arrow::Datum(filtered), arrow::Datum(mask_qty)}).ValueOrDie().table();

    // Compute revenue = l_extendedprice * l_discount
    auto extendedprice_chunked = filtered->GetColumnByName("l_extendedprice");
    auto extendedprice_array = arrow::Concatenate(extendedprice_chunked->chunks()).ValueOrDie();

    // Cast to double for arithmetic
    arrow::compute::CastOptions cast_opts_price = arrow::compute::CastOptions::Safe(arrow::float64());
    auto price_dbl = arrow::compute::CallFunction("cast",
        {arrow::Datum(extendedprice_array)}, &cast_opts_price).ValueOrDie().make_array();

    // Get discount in double (already computed earlier, need to recompute for filtered table)
    auto discount_final_chunked = filtered->GetColumnByName("l_discount");
    auto discount_final_array = arrow::Concatenate(discount_final_chunked->chunks()).ValueOrDie();
    arrow::compute::CastOptions cast_opts_disc = arrow::compute::CastOptions::Safe(arrow::float64());
    auto discount_final_dbl = arrow::compute::CallFunction("cast",
        {arrow::Datum(discount_final_array)}, &cast_opts_disc).ValueOrDie().make_array();

    auto revenue_array = arrow::compute::CallFunction("multiply",
        {arrow::Datum(price_dbl), arrow::Datum(discount_final_dbl)}).ValueOrDie().make_array();

    // Sum revenue
    std::vector<arrow::Datum> sum_args = {arrow::Datum(revenue_array)};
    auto sum_result = arrow::compute::CallFunction("sum", sum_args).ValueOrDie().scalar();

    double total_revenue = 0.0;
    if (sum_result->is_valid) {
        auto sum_scalar = std::static_pointer_cast<arrow::DoubleScalar>(sum_result);
        total_revenue = sum_scalar->value;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    // Write output
    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    out << "revenue\n";
    out << std::fixed << std::setprecision(2) << total_revenue << "\n";
    out.close();

    std::cout << "Q6: " << filtered->num_rows() << " rows, "
              << elapsed.count() << " seconds" << std::endl;
}
