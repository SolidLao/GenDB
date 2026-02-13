#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include "../operators/hash_agg.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <algorithm>

using namespace gendb;
using namespace gendb::operators;

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Scan lineitem table with date filter and column projection
    // WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    // = '1998-09-02'
    int32_t filter_date = DateToArrowDate32(1998, 9, 2);

    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    // Project only the 7 columns we need out of 16 total
    std::vector<std::string> columns = {
        "l_returnflag",
        "l_linestatus",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_shipdate"
    };

    auto scan = ScanWithDateFilter(lineitem_path)
        .Project(columns)
        .FilterDate("l_shipdate", -1, filter_date);  // -1 = no lower bound, filter_date = upper bound

    auto scanned_table = scan.Execute();

    std::cout << "Scanned rows: " << scanned_table->num_rows() << std::endl;

    // Step 2: Compute derived columns using Arrow compute
    // We need: l_extendedprice * (1 - l_discount) and l_extendedprice * (1 - l_discount) * (1 + l_tax)

    auto l_extendedprice = scanned_table->GetColumnByName("l_extendedprice");
    auto l_discount = scanned_table->GetColumnByName("l_discount");
    auto l_tax = scanned_table->GetColumnByName("l_tax");

    if (!l_extendedprice || !l_discount || !l_tax) {
        throw std::runtime_error("Required columns not found in scanned table");
    }

    // Concatenate chunks for computation
    auto ep_result = arrow::Concatenate(l_extendedprice->chunks());
    auto disc_result = arrow::Concatenate(l_discount->chunks());
    auto tax_result = arrow::Concatenate(l_tax->chunks());

    if (!ep_result.ok() || !disc_result.ok() || !tax_result.ok()) {
        throw std::runtime_error("Failed to concatenate chunks");
    }

    auto ep_array = ep_result.ValueOrDie();
    auto disc_array = disc_result.ValueOrDie();
    auto tax_array = tax_result.ValueOrDie();

    arrow::compute::ExecContext ctx;

    // Compute (1 - l_discount)
    auto one_scalar = arrow::MakeScalar(arrow::float64(), 1.0);
    auto one_minus_disc = arrow::compute::CallFunction(
        "subtract",
        {one_scalar.ValueOrDie(), disc_array},
        &ctx
    );
    if (!one_minus_disc.ok()) {
        throw std::runtime_error("Failed to compute (1 - discount): " + one_minus_disc.status().ToString());
    }

    // Compute l_extendedprice * (1 - l_discount) = disc_price
    auto disc_price = arrow::compute::CallFunction(
        "multiply",
        {ep_array, one_minus_disc.ValueOrDie().make_array()},
        &ctx
    );
    if (!disc_price.ok()) {
        throw std::runtime_error("Failed to compute disc_price: " + disc_price.status().ToString());
    }

    // Compute (1 + l_tax)
    auto one_plus_tax = arrow::compute::CallFunction(
        "add",
        {one_scalar.ValueOrDie(), tax_array},
        &ctx
    );
    if (!one_plus_tax.ok()) {
        throw std::runtime_error("Failed to compute (1 + tax): " + one_plus_tax.status().ToString());
    }

    // Compute l_extendedprice * (1 - l_discount) * (1 + l_tax) = charge
    auto charge = arrow::compute::CallFunction(
        "multiply",
        {disc_price.ValueOrDie().make_array(), one_plus_tax.ValueOrDie().make_array()},
        &ctx
    );
    if (!charge.ok()) {
        throw std::runtime_error("Failed to compute charge: " + charge.status().ToString());
    }

    // Add computed columns to table
    auto disc_price_chunked = std::make_shared<arrow::ChunkedArray>(disc_price.ValueOrDie().make_array());
    auto charge_chunked = std::make_shared<arrow::ChunkedArray>(charge.ValueOrDie().make_array());

    auto extended_table = scanned_table->AddColumn(
        scanned_table->num_columns(),
        arrow::field("disc_price", arrow::float64()),
        disc_price_chunked
    );
    if (!extended_table.ok()) {
        throw std::runtime_error("Failed to add disc_price column");
    }

    auto extended_table2 = extended_table.ValueOrDie()->AddColumn(
        extended_table.ValueOrDie()->num_columns(),
        arrow::field("charge", arrow::float64()),
        charge_chunked
    );
    if (!extended_table2.ok()) {
        throw std::runtime_error("Failed to add charge column");
    }

    auto agg_input = extended_table2.ValueOrDie();

    // Step 3: Hash aggregation
    // GROUP BY l_returnflag, l_linestatus
    auto hash_agg = HashAgg(agg_input)
        .GroupBy({"l_returnflag", "l_linestatus"})
        .Aggregate(AggFunc::SUM, "l_quantity", "sum_qty")
        .Aggregate(AggFunc::SUM, "l_extendedprice", "sum_base_price")
        .Aggregate(AggFunc::SUM, "disc_price", "sum_disc_price")
        .Aggregate(AggFunc::SUM, "charge", "sum_charge")
        .Aggregate(AggFunc::AVG, "l_quantity", "avg_qty")
        .Aggregate(AggFunc::AVG, "l_extendedprice", "avg_price")
        .Aggregate(AggFunc::AVG, "l_discount", "avg_disc")
        .Aggregate(AggFunc::COUNT, "l_quantity", "count_order");  // COUNT(*) - any column works

    auto agg_result = hash_agg.Execute();

    std::cout << "Aggregation groups: " << agg_result->num_rows() << std::endl;

    // Step 4: Sort by l_returnflag, l_linestatus
    // Extract data for sorting
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        double count_order;
    };

    std::vector<ResultRow> rows;
    int64_t num_groups = agg_result->num_rows();

    auto returnflag_arr = GetTypedColumn<arrow::StringType>(agg_result, "l_returnflag");
    auto linestatus_arr = GetTypedColumn<arrow::StringType>(agg_result, "l_linestatus");
    auto sum_qty_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "sum_qty");
    auto sum_base_price_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "sum_base_price");
    auto sum_disc_price_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "sum_disc_price");
    auto sum_charge_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "sum_charge");
    auto avg_qty_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "avg_qty");
    auto avg_price_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "avg_price");
    auto avg_disc_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "avg_disc");
    auto count_order_arr = GetTypedColumn<arrow::DoubleType>(agg_result, "count_order");

    for (int64_t i = 0; i < num_groups; ++i) {
        ResultRow row;
        row.returnflag = returnflag_arr->GetString(i);
        row.linestatus = linestatus_arr->GetString(i);
        row.sum_qty = sum_qty_arr->Value(i);
        row.sum_base_price = sum_base_price_arr->Value(i);
        row.sum_disc_price = sum_disc_price_arr->Value(i);
        row.sum_charge = sum_charge_arr->Value(i);
        row.avg_qty = avg_qty_arr->Value(i);
        row.avg_price = avg_price_arr->Value(i);
        row.avg_disc = avg_disc_arr->Value(i);
        row.count_order = count_order_arr->Value(i);
        rows.push_back(row);
    }

    // Sort by returnflag, then linestatus
    std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) {
            return a.returnflag < b.returnflag;
        }
        return a.linestatus < b.linestatus;
    });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Step 5: Output results
    std::cout << "Query Q1 completed in " << duration.count() << " ms" << std::endl;
    std::cout << "Result rows: " << rows.size() << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q1.csv";
        std::ofstream outfile(output_path);

        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open output file: " + output_path);
        }

        // Write header
        outfile << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";

        // Write data rows
        for (const auto& row : rows) {
            outfile << row.returnflag << "|"
                   << row.linestatus << "|"
                   << std::fixed << std::setprecision(2) << row.sum_qty << "|"
                   << std::fixed << std::setprecision(2) << row.sum_base_price << "|"
                   << std::fixed << std::setprecision(2) << row.sum_disc_price << "|"
                   << std::fixed << std::setprecision(2) << row.sum_charge << "|"
                   << std::fixed << std::setprecision(2) << row.avg_qty << "|"
                   << std::fixed << std::setprecision(2) << row.avg_price << "|"
                   << std::fixed << std::setprecision(2) << row.avg_disc << "|"
                   << std::fixed << std::setprecision(0) << row.count_order << "\n";
        }

        outfile.close();
        std::cout << "Results written to: " << output_path << std::endl;
    }
}
