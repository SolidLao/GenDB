#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date constants: 1995-03-15
    int32_t date_1995_03_15 = gendb::DateToArrowDate32(1995, 3, 15);

    // Step 1: Scan customer with projection and filter by c_mktsegment = 'BUILDING'
    gendb::operators::Scan customer_scan(parquet_dir + "/customer.parquet");
    auto customer_table = customer_scan.Project({"c_custkey", "c_mktsegment"}).Execute();

    // Filter customer by c_mktsegment = 'BUILDING'
    auto c_mktsegment_col = customer_table->GetColumnByName("c_mktsegment");
    auto c_mktsegment_combined = arrow::Concatenate(c_mktsegment_col->chunks()).ValueOrDie();
    auto building_scalar = arrow::MakeScalar(arrow::utf8(), std::string("BUILDING"));
    auto c_eq = arrow::compute::CallFunction("equal",
        {arrow::Datum(c_mktsegment_combined), arrow::Datum(building_scalar.ValueOrDie())});
    auto c_mask = c_eq.ValueOrDie().make_array();
    auto c_filtered = arrow::compute::CallFunction("filter",
        {arrow::Datum(customer_table), arrow::Datum(c_mask)});
    auto customer_filtered = c_filtered.ValueOrDie().table();

    // Step 2: Scan orders with date filter (o_orderdate < 1995-03-15)
    gendb::operators::ScanWithDateFilter orders_scan(parquet_dir + "/orders.parquet");
    auto orders_table = orders_scan
        .Project({"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"})
        .FilterDate("o_orderdate", -1, date_1995_03_15 - 1)  // < date means <= (date - 1)
        .Execute();

    // Step 3: Join customer and orders (build on customer, probe with orders)
    gendb::operators::HashJoinInt32 join1(customer_filtered, orders_table, "c_custkey", "o_custkey");
    auto customer_orders = join1.Execute();

    // Step 4: Scan lineitem with date filter (l_shipdate > 1995-03-15)
    gendb::operators::ScanWithDateFilter lineitem_scan(parquet_dir + "/lineitem.parquet");
    auto lineitem_table = lineitem_scan
        .Project({"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"})
        .FilterDate("l_shipdate", date_1995_03_15 + 1, -1)  // > date means >= (date + 1)
        .Execute();

    // Step 5: Join customer_orders with lineitem (build on customer_orders, probe with lineitem)
    gendb::operators::HashJoinInt32 join2(customer_orders, lineitem_table, "o_orderkey", "l_orderkey");
    auto joined = join2.Execute();

    // Step 6: Calculate revenue = l_extendedprice * (1 - l_discount)
    auto price_col = joined->GetColumnByName("l_extendedprice");
    auto discount_col = joined->GetColumnByName("l_discount");
    auto price_arr = arrow::Concatenate(price_col->chunks()).ValueOrDie();
    auto discount_arr = arrow::Concatenate(discount_col->chunks()).ValueOrDie();

    // 1 - l_discount
    auto one_scalar = arrow::MakeScalar(arrow::float64(), 1.0);
    auto one_minus_discount = arrow::compute::CallFunction("subtract",
        {arrow::Datum(one_scalar.ValueOrDie()), arrow::Datum(discount_arr)});
    auto one_minus_discount_arr = one_minus_discount.ValueOrDie().make_array();

    // l_extendedprice * (1 - l_discount)
    auto revenue_result = arrow::compute::CallFunction("multiply",
        {arrow::Datum(price_arr), arrow::Datum(one_minus_discount_arr)});
    auto revenue_arr = revenue_result.ValueOrDie().make_array();

    // Add revenue column to table
    auto revenue_chunked = std::make_shared<arrow::ChunkedArray>(revenue_arr);
    auto joined_with_revenue = joined->AddColumn(
        joined->num_columns(),
        arrow::field("revenue", arrow::float64()),
        revenue_chunked
    ).ValueOrDie();

    // Step 7: Aggregate by (l_orderkey, o_orderdate, o_shippriority) SUM(revenue)
    gendb::operators::HashAgg agg(joined_with_revenue);
    auto agg_result = agg
        .GroupBy({"l_orderkey", "o_orderdate", "o_shippriority"})
        .Aggregate(gendb::operators::AggFunc::SUM, "revenue", "revenue")
        .Execute();

    // Step 8: Sort by revenue DESC, o_orderdate ASC
    arrow::compute::SortOptions sort_opts;
    sort_opts.sort_keys = {
        arrow::compute::SortKey("revenue", arrow::compute::SortOrder::Descending),
        arrow::compute::SortKey("o_orderdate", arrow::compute::SortOrder::Ascending)
    };
    auto sort_indices = arrow::compute::CallFunction("sort_indices",
        {arrow::Datum(agg_result)}, &sort_opts);
    auto sort_idx_arr = sort_indices.ValueOrDie().make_array();

    // Apply sorting via take on each column
    arrow::ArrayVector sorted_cols;
    for (int i = 0; i < agg_result->num_columns(); ++i) {
        auto col = arrow::Concatenate(agg_result->column(i)->chunks()).ValueOrDie();
        auto taken = arrow::compute::CallFunction("take",
            {arrow::Datum(col), arrow::Datum(sort_idx_arr)});
        sorted_cols.push_back(taken.ValueOrDie().make_array());
    }
    auto sorted_table = arrow::Table::Make(agg_result->schema(), sorted_cols);

    // Step 9: Limit to 10 rows
    int64_t limit = std::min<int64_t>(10, sorted_table->num_rows());
    auto limited_table = sorted_table->Slice(0, limit);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Q3: " << limited_table->num_rows() << " rows in " << duration.count() << " ms" << std::endl;

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << std::fixed << std::setprecision(2);

        // Write rows
        auto l_orderkey_arr = gendb::GetTypedColumn<arrow::Int32Type>(limited_table, "l_orderkey");
        auto revenue_arr_final = gendb::GetTypedColumn<arrow::DoubleType>(limited_table, "revenue");
        auto o_orderdate_arr = gendb::GetTypedColumn<arrow::Int32Type>(limited_table, "o_orderdate");
        auto o_shippriority_arr = gendb::GetTypedColumn<arrow::Int32Type>(limited_table, "o_shippriority");

        for (int64_t i = 0; i < limited_table->num_rows(); ++i) {
            out << l_orderkey_arr->Value(i) << "|"
                << revenue_arr_final->Value(i) << "|"
                << o_orderdate_arr->Value(i) << "|"
                << o_shippriority_arr->Value(i) << "\n";
        }
        out.close();
    }
}
