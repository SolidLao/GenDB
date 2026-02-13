#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include "../operators/hash_agg.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date: 1998-12-01 - 90 days = 1998-09-02 (date32 = 10471)
    int32_t max_shipdate = gendb::DateToArrowDate32(1998, 9, 2);

    // Scan lineitem with date filter and column projection
    gendb::operators::ScanWithDateFilter scan(parquet_dir + "/lineitem.parquet");
    auto table = scan
        .Project({"l_shipdate", "l_returnflag", "l_linestatus", "l_quantity",
                  "l_extendedprice", "l_discount", "l_tax"})
        .FilterDate("l_shipdate", -1, max_shipdate)  // <= max_shipdate
        .Execute();

    if (table->num_rows() == 0) {
        std::cout << "Q1: 0 rows" << std::endl;
        return;
    }

    // Extract columns and concatenate chunks
    auto quantity = table->GetColumnByName("l_quantity");
    auto extendedprice = table->GetColumnByName("l_extendedprice");
    auto discount = table->GetColumnByName("l_discount");
    auto tax = table->GetColumnByName("l_tax");

    auto quantity_arr = arrow::Concatenate(quantity->chunks()).ValueOrDie();
    auto price_arr = arrow::Concatenate(extendedprice->chunks()).ValueOrDie();
    auto discount_arr = arrow::Concatenate(discount->chunks()).ValueOrDie();
    auto tax_arr = arrow::Concatenate(tax->chunks()).ValueOrDie();

    // Compute disc_price = extendedprice * (1 - discount)
    auto one = arrow::MakeScalar(arrow::float64(), 1.0);
    auto one_minus_discount = arrow::compute::CallFunction("subtract",
        {arrow::Datum(one.ValueOrDie()), arrow::Datum(discount_arr)});
    auto disc_price = arrow::compute::CallFunction("multiply",
        {arrow::Datum(price_arr), one_minus_discount.ValueOrDie()});
    auto disc_price_arr = disc_price.ValueOrDie().make_array();

    // Compute charge = extendedprice * (1 - discount) * (1 + tax)
    auto one_plus_tax = arrow::compute::CallFunction("add",
        {arrow::Datum(one.ValueOrDie()), arrow::Datum(tax_arr)});
    auto charge = arrow::compute::CallFunction("multiply",
        {arrow::Datum(disc_price_arr), one_plus_tax.ValueOrDie()});
    auto charge_arr = charge.ValueOrDie().make_array();

    // Add computed columns to table
    auto disc_price_chunked = std::make_shared<arrow::ChunkedArray>(disc_price_arr);
    auto charge_chunked = std::make_shared<arrow::ChunkedArray>(charge_arr);

    table = table->AddColumn(
        table->num_columns(),
        arrow::field("disc_price", arrow::float64()),
        disc_price_chunked
    ).ValueOrDie();

    table = table->AddColumn(
        table->num_columns(),
        arrow::field("charge", arrow::float64()),
        charge_chunked
    ).ValueOrDie();

    // Hash aggregation: GROUP BY l_returnflag, l_linestatus
    gendb::operators::HashAgg agg(table);
    auto result = agg
        .GroupBy({"l_returnflag", "l_linestatus"})
        .Aggregate(gendb::operators::AggFunc::SUM, "l_quantity", "sum_qty")
        .Aggregate(gendb::operators::AggFunc::SUM, "l_extendedprice", "sum_base_price")
        .Aggregate(gendb::operators::AggFunc::SUM, "disc_price", "sum_disc_price")
        .Aggregate(gendb::operators::AggFunc::SUM, "charge", "sum_charge")
        .Aggregate(gendb::operators::AggFunc::AVG, "l_quantity", "avg_qty")
        .Aggregate(gendb::operators::AggFunc::AVG, "l_extendedprice", "avg_price")
        .Aggregate(gendb::operators::AggFunc::AVG, "l_discount", "avg_disc")
        .Aggregate(gendb::operators::AggFunc::COUNT, "l_quantity", "count_order")
        .Execute();

    // Sort by l_returnflag, l_linestatus
    arrow::compute::SortOptions sort_opts;
    sort_opts.sort_keys = {
        arrow::compute::SortKey("l_returnflag", arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("l_linestatus", arrow::compute::SortOrder::Ascending)
    };
    auto indices = arrow::compute::CallFunction("sort_indices",
        {arrow::Datum(result)}, &sort_opts);
    auto idx_arr = indices.ValueOrDie().make_array();

    // Apply sort to each column via "take"
    arrow::ArrayVector sorted_cols;
    for (int i = 0; i < result->num_columns(); ++i) {
        auto col = arrow::Concatenate(result->column(i)->chunks()).ValueOrDie();
        auto taken = arrow::compute::CallFunction("take",
            {arrow::Datum(col), arrow::Datum(idx_arr)});
        sorted_cols.push_back(taken.ValueOrDie().make_array());
    }
    auto sorted_result = arrow::Table::Make(result->schema(), sorted_cols);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "Q1: " << sorted_result->num_rows() << " rows in "
              << std::fixed << std::setprecision(3) << elapsed.count() << "s" << std::endl;

    // Write CSV results
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);

        // Header
        out << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";

        auto returnflag = gendb::GetTypedColumn<arrow::StringType>(sorted_result, "l_returnflag");
        auto linestatus = gendb::GetTypedColumn<arrow::StringType>(sorted_result, "l_linestatus");
        auto sum_qty = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "sum_qty");
        auto sum_base_price = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "sum_base_price");
        auto sum_disc_price = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "sum_disc_price");
        auto sum_charge = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "sum_charge");
        auto avg_qty = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "avg_qty");
        auto avg_price = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "avg_price");
        auto avg_disc = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "avg_disc");
        auto count_order = gendb::GetTypedColumn<arrow::DoubleType>(sorted_result, "count_order");

        for (int64_t i = 0; i < sorted_result->num_rows(); ++i) {
            out << returnflag->GetString(i) << "|"
                << linestatus->GetString(i) << "|"
                << sum_qty->Value(i) << "|"
                << sum_base_price->Value(i) << "|"
                << sum_disc_price->Value(i) << "|"
                << sum_charge->Value(i) << "|"
                << avg_qty->Value(i) << "|"
                << avg_price->Value(i) << "|"
                << avg_disc->Value(i) << "|"
                << static_cast<int64_t>(count_order->Value(i)) << "\n";
        }
    }
}
