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
#include <algorithm>
#include <ctime>

// Helper: convert date to days since epoch
inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month-1] + day - 1;
    if (month > 2 && (year%4==0 && (year%100!=0 || year%400==0))) d++;
    return d;
}

// Group key: (l_returnflag, l_linestatus)
struct GroupKey {
    std::string returnflag;
    std::string linestatus;

    bool operator==(const GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    std::size_t operator()(const GroupKey& k) const {
        return std::hash<std::string>()(k.returnflag) ^
               (std::hash<std::string>()(k.linestatus) << 1);
    }
};

// Aggregate values
struct AggValues {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;  // for AVG(l_discount)
    int64_t count = 0;
};

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date filter: 1998-12-01 - 90 days = 1998-09-02
    int32_t max_shipdate = date_to_days(1998, 9, 2);

    // Open lineitem.parquet
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(lineitem_path));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto parquet_reader = parquet::ParquetFileReader::OpenFile(lineitem_path);
    PARQUET_THROW_NOT_OK(parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), std::move(parquet_reader), &reader));

    // Get schema and find column indices
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));

    std::vector<int> col_indices = {
        schema->GetFieldIndex("l_returnflag"),
        schema->GetFieldIndex("l_linestatus"),
        schema->GetFieldIndex("l_quantity"),
        schema->GetFieldIndex("l_extendedprice"),
        schema->GetFieldIndex("l_discount"),
        schema->GetFieldIndex("l_tax"),
        schema->GetFieldIndex("l_shipdate")
    };

    // Read table with column projection
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(col_indices, &table));

    // Concatenate chunked arrays
    auto returnflag_arr = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(table->GetColumnByName("l_returnflag")->chunks()).ValueOrDie());
    auto linestatus_arr = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(table->GetColumnByName("l_linestatus")->chunks()).ValueOrDie());
    auto shipdate_arr = std::static_pointer_cast<arrow::Date32Array>(
        arrow::Concatenate(table->GetColumnByName("l_shipdate")->chunks()).ValueOrDie());

    // Cast decimal columns to double for arithmetic
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());

    auto quantity_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast",
            {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_quantity")->chunks()).ValueOrDie())},
            &cast_opts).ValueOrDie().make_array());
    auto price_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast",
            {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_extendedprice")->chunks()).ValueOrDie())},
            &cast_opts).ValueOrDie().make_array());
    auto discount_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast",
            {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_discount")->chunks()).ValueOrDie())},
            &cast_opts).ValueOrDie().make_array());
    auto tax_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast",
            {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_tax")->chunks()).ValueOrDie())},
            &cast_opts).ValueOrDie().make_array());

    // Manual aggregation with filtering
    std::unordered_map<GroupKey, AggValues, GroupKeyHash> groups;

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        // Filter: l_shipdate <= max_shipdate
        if (shipdate_arr->IsNull(i) || shipdate_arr->Value(i) > max_shipdate) {
            continue;
        }

        // Extract group key
        GroupKey key;
        key.returnflag = returnflag_arr->GetString(i);
        key.linestatus = linestatus_arr->GetString(i);

        // Extract values
        double qty = quantity_arr->Value(i);
        double price = price_arr->Value(i);
        double discount = discount_arr->Value(i);
        double tax = tax_arr->Value(i);

        // Compute derived values
        double disc_price = price * (1.0 - discount);
        double charge = disc_price * (1.0 + tax);

        // Update aggregates
        auto& agg = groups[key];
        agg.sum_qty += qty;
        agg.sum_base_price += price;
        agg.sum_disc_price += disc_price;
        agg.sum_charge += charge;
        agg.sum_discount += discount;
        agg.count++;
    }

    // Sort groups by (returnflag, linestatus)
    std::vector<std::pair<GroupKey, AggValues>> sorted_groups(groups.begin(), groups.end());
    std::sort(sorted_groups.begin(), sorted_groups.end(),
        [](const auto& a, const auto& b) {
            if (a.first.returnflag != b.first.returnflag)
                return a.first.returnflag < b.first.returnflag;
            return a.first.linestatus < b.first.linestatus;
        });

    // Write results
    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    out << std::fixed << std::setprecision(2);

    // Header
    out << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";

    // Data rows
    for (const auto& [key, agg] : sorted_groups) {
        double avg_qty = agg.sum_qty / agg.count;
        double avg_price = agg.sum_base_price / agg.count;
        double avg_disc = agg.sum_discount / agg.count;

        out << key.returnflag << "|"
            << key.linestatus << "|"
            << agg.sum_qty << "|"
            << agg.sum_base_price << "|"
            << agg.sum_disc_price << "|"
            << agg.sum_charge << "|"
            << avg_qty << "|"
            << avg_price << "|"
            << avg_disc << "|"
            << agg.count << "\n";
    }

    out.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Q1: " << sorted_groups.size() << " rows, "
              << duration.count() << " ms" << std::endl;
}
