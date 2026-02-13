#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>

// Helper function to convert date to days since epoch
// SEMANTIC: static keyword prevents multiple definition linker errors across translation units
static inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month - 1] + day - 1;
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) d++;
    return d;
}

// Aggregation state for each group
struct AggState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;

    std::string returnflag;
    std::string linestatus;
};

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Calculate the date threshold: 1998-12-01 - 90 days = 1998-09-02
    int32_t cutoff_date = date_to_days(1998, 9, 2);

    // Open lineitem.parquet
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(lineitem_path));

    // Create Parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto reader_result = parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
    PARQUET_THROW_NOT_OK(reader_result.status());
    reader = std::move(reader_result).ValueOrDie();

    // Get schema to find column indices
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));

    // Project only the columns we need (8 out of 16)
    std::vector<int> col_indices = {
        schema->GetFieldIndex("l_returnflag"),
        schema->GetFieldIndex("l_linestatus"),
        schema->GetFieldIndex("l_quantity"),
        schema->GetFieldIndex("l_extendedprice"),
        schema->GetFieldIndex("l_discount"),
        schema->GetFieldIndex("l_tax"),
        schema->GetFieldIndex("l_shipdate")
    };

    // Read the table with column projection
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(col_indices, &table));

    // Filter by l_shipdate <= cutoff_date
    auto shipdate_chunked = table->GetColumnByName("l_shipdate");
    auto shipdate_array = arrow::Concatenate(shipdate_chunked->chunks()).ValueOrDie();
    auto cutoff_scalar = arrow::MakeScalar(arrow::date32(), cutoff_date).ValueOrDie();

    auto mask = arrow::compute::CallFunction("less_equal",
        {arrow::Datum(shipdate_array), arrow::Datum(cutoff_scalar)}).ValueOrDie().make_array();

    auto filtered_table = arrow::compute::CallFunction("filter",
        {arrow::Datum(table), arrow::Datum(mask)}).ValueOrDie().table();

    // Extract columns and concatenate chunks
    auto returnflag_chunked = filtered_table->GetColumnByName("l_returnflag");
    auto linestatus_chunked = filtered_table->GetColumnByName("l_linestatus");
    auto quantity_chunked = filtered_table->GetColumnByName("l_quantity");
    auto extendedprice_chunked = filtered_table->GetColumnByName("l_extendedprice");
    auto discount_chunked = filtered_table->GetColumnByName("l_discount");
    auto tax_chunked = filtered_table->GetColumnByName("l_tax");

    auto returnflag_array = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(returnflag_chunked->chunks()).ValueOrDie());
    auto linestatus_array = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(linestatus_chunked->chunks()).ValueOrDie());

    // Cast decimal columns to double for computation
    std::shared_ptr<arrow::Array> quantity_array, extendedprice_array, discount_array, tax_array;
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());

    auto qty_concat = arrow::Concatenate(quantity_chunked->chunks()).ValueOrDie();
    if (qty_concat->type()->id() == arrow::Type::DECIMAL128 || qty_concat->type()->id() == arrow::Type::DECIMAL256) {
        quantity_array = arrow::compute::CallFunction("cast",
            {arrow::Datum(qty_concat)}, &cast_opts).ValueOrDie().make_array();
    } else {
        quantity_array = qty_concat;
    }

    auto price_concat = arrow::Concatenate(extendedprice_chunked->chunks()).ValueOrDie();
    if (price_concat->type()->id() == arrow::Type::DECIMAL128 || price_concat->type()->id() == arrow::Type::DECIMAL256) {
        extendedprice_array = arrow::compute::CallFunction("cast",
            {arrow::Datum(price_concat)}, &cast_opts).ValueOrDie().make_array();
    } else {
        extendedprice_array = price_concat;
    }

    auto disc_concat = arrow::Concatenate(discount_chunked->chunks()).ValueOrDie();
    if (disc_concat->type()->id() == arrow::Type::DECIMAL128 || disc_concat->type()->id() == arrow::Type::DECIMAL256) {
        discount_array = arrow::compute::CallFunction("cast",
            {arrow::Datum(disc_concat)}, &cast_opts).ValueOrDie().make_array();
    } else {
        discount_array = disc_concat;
    }

    auto tax_concat = arrow::Concatenate(tax_chunked->chunks()).ValueOrDie();
    if (tax_concat->type()->id() == arrow::Type::DECIMAL128 || tax_concat->type()->id() == arrow::Type::DECIMAL256) {
        tax_array = arrow::compute::CallFunction("cast",
            {arrow::Datum(tax_concat)}, &cast_opts).ValueOrDie().make_array();
    } else {
        tax_array = tax_concat;
    }

    auto quantity_dbl = std::static_pointer_cast<arrow::DoubleArray>(quantity_array);
    auto extendedprice_dbl = std::static_pointer_cast<arrow::DoubleArray>(extendedprice_array);
    auto discount_dbl = std::static_pointer_cast<arrow::DoubleArray>(discount_array);
    auto tax_dbl = std::static_pointer_cast<arrow::DoubleArray>(tax_array);

    // Hash aggregate on (returnflag, linestatus)
    std::unordered_map<std::string, AggState> groups;

    int64_t num_rows = filtered_table->num_rows();
    for (int64_t i = 0; i < num_rows; ++i) {
        std::string rf = returnflag_array->GetString(i);
        std::string ls = linestatus_array->GetString(i);
        std::string key = rf + "|" + ls;

        auto& state = groups[key];
        if (state.count == 0) {
            state.returnflag = rf;
            state.linestatus = ls;
        }

        double qty = quantity_dbl->Value(i);
        double price = extendedprice_dbl->Value(i);
        double disc = discount_dbl->Value(i);
        double tx = tax_dbl->Value(i);

        double disc_price = price * (1.0 - disc);
        double charge = disc_price * (1.0 + tx);

        state.sum_qty += qty;
        state.sum_base_price += price;
        state.sum_disc_price += disc_price;
        state.sum_charge += charge;
        state.sum_discount += disc;
        state.count += 1;
    }

    // Convert to vector for sorting
    std::vector<AggState> results;
    results.reserve(groups.size());
    for (const auto& kv : groups) {
        results.push_back(kv.second);
    }

    // Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const AggState& a, const AggState& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // Write output to CSV
    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    out << std::fixed << std::setprecision(2);

    // Header
    out << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";

    // Data rows
    for (const auto& row : results) {
        double avg_qty = row.sum_qty / row.count;
        double avg_price = row.sum_base_price / row.count;
        double avg_disc = row.sum_discount / row.count;

        out << row.returnflag << "|"
            << row.linestatus << "|"
            << row.sum_qty << "|"
            << row.sum_base_price << "|"
            << row.sum_disc_price << "|"
            << row.sum_charge << "|"
            << avg_qty << "|"
            << avg_price << "|"
            << avg_disc << "|"
            << row.count << "\n";
    }

    out.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "Q1: " << results.size() << " groups, " << num_rows << " rows processed in "
              << duration << " ms" << std::endl;
}
