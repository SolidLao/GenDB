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
// SEMANTIC FIX: Use double for intermediate aggregations to maintain precision
// when summing billions of decimal values (TPC-H spec requires 2 decimal places accuracy)
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

    // Build filter expression: l_shipdate <= '1998-09-02'
    // This will be pushed down to Parquet reader using row group statistics
    auto max_scalar = arrow::MakeScalar(arrow::date32(), max_shipdate).ValueOrDie();
    auto filter_expr = arrow::compute::less_equal(
        arrow::compute::field_ref("l_shipdate"),
        arrow::compute::literal(max_scalar)
    );

    // Setup filesystem and Parquet dataset for predicate pushdown
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

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

    // Project only required columns
    auto project_status = scanner_builder->Project({
        "l_returnflag", "l_linestatus", "l_quantity",
        "l_extendedprice", "l_discount", "l_tax"
    });
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
    auto table = table_result.ValueOrDie();

    // Concatenate chunked arrays (date filter already applied by scanner)
    auto returnflag_arr = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(table->GetColumnByName("l_returnflag")->chunks()).ValueOrDie());
    auto linestatus_arr = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(table->GetColumnByName("l_linestatus")->chunks()).ValueOrDie());

    // Cast decimal columns to double for arithmetic
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());

    std::vector<arrow::Datum> cast_qty_args = {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_quantity")->chunks()).ValueOrDie())};
    auto quantity_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast", cast_qty_args, &cast_opts).ValueOrDie().make_array());

    std::vector<arrow::Datum> cast_price_args = {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_extendedprice")->chunks()).ValueOrDie())};
    auto price_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast", cast_price_args, &cast_opts).ValueOrDie().make_array());

    std::vector<arrow::Datum> cast_disc_args = {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_discount")->chunks()).ValueOrDie())};
    auto discount_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast", cast_disc_args, &cast_opts).ValueOrDie().make_array());

    std::vector<arrow::Datum> cast_tax_args = {arrow::Datum(arrow::Concatenate(table->GetColumnByName("l_tax")->chunks()).ValueOrDie())};
    auto tax_arr = std::static_pointer_cast<arrow::DoubleArray>(
        arrow::compute::CallFunction("cast", cast_tax_args, &cast_opts).ValueOrDie().make_array());

    // Manual aggregation (no filtering needed - already done by scanner)
    std::unordered_map<GroupKey, AggValues, GroupKeyHash> groups;

    for (int64_t i = 0; i < table->num_rows(); ++i) {

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

    // CORRECTNESS FIX: Write CSV with comma separators (not pipe delimiters).
    // Each column must be a separate field in the CSV output.
    // Apply std::fixed and precision only to numeric columns.

    // Header (comma-separated)
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows (comma-separated)
    for (const auto& [key, agg] : sorted_groups) {
        double avg_qty = agg.sum_qty / agg.count;
        double avg_price = agg.sum_base_price / agg.count;
        double avg_disc = agg.sum_discount / agg.count;

        // Write all columns with comma separators
        out << key.returnflag << ","
            << key.linestatus << ",";

        // Write numeric columns with fixed precision (2 decimal places)
        out << std::fixed << std::setprecision(2);
        out << agg.sum_qty << ","
            << agg.sum_base_price << ","
            << agg.sum_disc_price << ","
            << agg.sum_charge << ","
            << avg_qty << ","
            << avg_price << ","
            << avg_disc << ",";

        // Write count as integer (no decimal places)
        out << std::setprecision(0);
        out << agg.count << "\n";
    }

    out.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Q1: " << sorted_groups.size() << " rows, "
              << duration.count() << " ms" << std::endl;
}
