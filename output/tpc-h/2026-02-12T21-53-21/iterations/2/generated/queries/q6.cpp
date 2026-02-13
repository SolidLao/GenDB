#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
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

    // Build filter expression: l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    // This will be pushed down to Parquet reader using row group statistics
    int32_t date_start = date_to_days(1994, 1, 1);  // 1994-01-01
    int32_t date_end = date_to_days(1995, 1, 1);    // 1995-01-01

    auto start_scalar = arrow::MakeScalar(arrow::date32(), date_start).ValueOrDie();
    auto end_scalar = arrow::MakeScalar(arrow::date32(), date_end).ValueOrDie();

    auto filter_expr = arrow::compute::and_(
        arrow::compute::greater_equal(arrow::compute::field_ref("l_shipdate"), arrow::compute::literal(start_scalar)),
        arrow::compute::less(arrow::compute::field_ref("l_shipdate"), arrow::compute::literal(end_scalar))
    );

    // Setup filesystem and Parquet dataset for predicate pushdown
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    arrow::dataset::FileSource source(lineitem_path, fs);

    auto fragment_result = format->MakeFragment(source);
    if (!fragment_result.ok()) {
        throw std::runtime_error("Failed to create fragment: " + fragment_result.status().ToString());
    }
    auto fragment = fragment_result.ValueOrDie();

    // Get physical schema from fragment
    auto schema_result = fragment->ReadPhysicalSchema();
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to read schema: " + schema_result.status().ToString());
    }
    auto schema = schema_result.ValueOrDie();

    // Create dataset from single fragment
    arrow::fs::FileInfoVector file_infos;
    auto file_info = fs->GetFileInfo(lineitem_path).ValueOrDie();
    file_infos.push_back(file_info);

    auto dataset_factory_result = arrow::dataset::FileSystemDatasetFactory::Make(
        fs, file_infos, format, {}
    );
    if (!dataset_factory_result.ok()) {
        throw std::runtime_error("Failed to create dataset factory: " + dataset_factory_result.status().ToString());
    }
    auto dataset_factory = dataset_factory_result.ValueOrDie();

    auto dataset_result = dataset_factory->Finish();
    if (!dataset_result.ok()) {
        throw std::runtime_error("Failed to create dataset: " + dataset_result.status().ToString());
    }
    auto dataset = dataset_result.ValueOrDie();

    // Build scanner with filter and column projection
    auto scanner_builder_result = dataset->NewScan();
    if (!scanner_builder_result.ok()) {
        throw std::runtime_error("Failed to create scanner builder: " + scanner_builder_result.status().ToString());
    }
    auto scanner_builder = scanner_builder_result.ValueOrDie();

    // Set filter for predicate pushdown to Parquet reader
    auto filter_status = scanner_builder->Filter(filter_expr);
    if (!filter_status.ok()) {
        throw std::runtime_error("Failed to set filter: " + filter_status.ToString());
    }

    // Project only required columns: l_extendedprice, l_discount, l_quantity
    auto project_status = scanner_builder->Project({"l_extendedprice", "l_discount", "l_quantity"});
    if (!project_status.ok()) {
        throw std::runtime_error("Failed to set projection: " + project_status.ToString());
    }

    auto scanner_result = scanner_builder->Finish();
    if (!scanner_result.ok()) {
        throw std::runtime_error("Failed to build scanner: " + scanner_result.status().ToString());
    }
    auto scanner = scanner_result.ValueOrDie();

    // Execute scan with predicate pushdown
    auto table_result = scanner->ToTable();
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to scan table: " + table_result.status().ToString());
    }
    auto filtered = table_result.ValueOrDie();

    // Filter 2: l_discount BETWEEN 0.05 AND 0.07
    auto discount_chunked = filtered->GetColumnByName("l_discount");
    auto discount_array = arrow::Concatenate(discount_chunked->chunks()).ValueOrDie();

    // Cast decimal to double for comparison
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());
    std::vector<arrow::Datum> cast_args = {arrow::Datum(discount_array)};
    auto discount_dbl = arrow::compute::CallFunction("cast", cast_args, &cast_opts).ValueOrDie().make_array();

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
    std::vector<arrow::Datum> cast_qty_args = {arrow::Datum(quantity_array)};
    auto quantity_dbl = arrow::compute::CallFunction("cast", cast_qty_args, &cast_opts_qty).ValueOrDie().make_array();

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
    std::vector<arrow::Datum> cast_price_args = {arrow::Datum(extendedprice_array)};
    auto price_dbl = arrow::compute::CallFunction("cast", cast_price_args, &cast_opts_price).ValueOrDie().make_array();

    // Get discount in double (already computed earlier, need to recompute for filtered table)
    auto discount_final_chunked = filtered->GetColumnByName("l_discount");
    auto discount_final_array = arrow::Concatenate(discount_final_chunked->chunks()).ValueOrDie();
    arrow::compute::CastOptions cast_opts_disc = arrow::compute::CastOptions::Safe(arrow::float64());
    std::vector<arrow::Datum> cast_disc_args = {arrow::Datum(discount_final_array)};
    auto discount_final_dbl = arrow::compute::CallFunction("cast", cast_disc_args, &cast_opts_disc).ValueOrDie().make_array();

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
