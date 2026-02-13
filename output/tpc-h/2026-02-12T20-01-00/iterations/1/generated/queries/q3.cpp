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
#include <algorithm>
#include <vector>

using namespace gendb;
using namespace gendb::operators;

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    try {
        // Step 1: Scan customer with filter on c_mktsegment = 'BUILDING'
        // Column projection: c_custkey, c_mktsegment (2/8 columns)
        std::string customer_file = parquet_dir + "/customer.parquet";
        Scan customer_scan(customer_file);
        auto customer_table = customer_scan.Project({"c_custkey", "c_mktsegment"}).Execute();

        // Filter customer by c_mktsegment = 'BUILDING'
        auto c_mktsegment = customer_table->GetColumnByName("c_mktsegment");
        auto c_mktsegment_combined_result = arrow::Concatenate(c_mktsegment->chunks());
        if (!c_mktsegment_combined_result.ok()) {
            throw std::runtime_error("Failed to concatenate c_mktsegment chunks");
        }
        auto c_mktsegment_array = c_mktsegment_combined_result.ValueOrDie();

        // Create filter mask for c_mktsegment = 'BUILDING'
        auto building_scalar = arrow::MakeScalar("BUILDING");
        arrow::compute::ExecContext ctx;
        auto eq_result = arrow::compute::CallFunction(
            "equal",
            std::vector<arrow::Datum>{c_mktsegment_array, building_scalar.ValueOrDie()},
            &ctx
        );
        if (!eq_result.ok()) {
            throw std::runtime_error("Failed to apply c_mktsegment filter");
        }

        auto filter_result = arrow::compute::CallFunction(
            "filter",
            std::vector<arrow::Datum>{arrow::Datum(customer_table), eq_result.ValueOrDie().make_array()},
            &ctx
        );
        if (!filter_result.ok()) {
            throw std::runtime_error("Failed to filter customer table");
        }
        auto filtered_customer = filter_result.ValueOrDie().table();

        // Step 2: Scan orders with date filter o_orderdate < '1995-03-15'
        // Column projection: o_orderkey, o_custkey, o_orderdate, o_shippriority (4/9 columns)
        std::string orders_file = parquet_dir + "/orders.parquet";
        int32_t date_1995_03_15 = DateToArrowDate32(1995, 3, 15);
        ScanWithDateFilter orders_scan(orders_file);
        auto orders_table = orders_scan
            .Project({"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"})
            .FilterDate("o_orderdate", -1, date_1995_03_15 - 1)  // o_orderdate < '1995-03-15'
            .Execute();

        // Step 3: Join customer and orders (c_custkey = o_custkey)
        // Build hash table on filtered customer (~3K rows), probe with filtered orders (~6M rows)
        HashJoinInt32 customer_orders_join(
            filtered_customer,
            orders_table,
            "c_custkey",
            "o_custkey"
        );
        auto customer_orders = customer_orders_join.Execute();

        // Step 4: Scan lineitem with date filter l_shipdate > '1995-03-15'
        // Column projection: l_orderkey, l_extendedprice, l_discount, l_shipdate (4/16 columns)
        std::string lineitem_file = parquet_dir + "/lineitem.parquet";
        ScanWithDateFilter lineitem_scan(lineitem_file);
        auto lineitem_table = lineitem_scan
            .Project({"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"})
            .FilterDate("l_shipdate", date_1995_03_15 + 1, -1)  // l_shipdate > '1995-03-15'
            .Execute();

        // Step 5: Join (customer ⋈ orders) with lineitem (l_orderkey = o_orderkey)
        // Build on join result (~600K rows), probe with filtered lineitem (~36M rows)
        HashJoinInt32 final_join(
            customer_orders,
            lineitem_table,
            "o_orderkey",
            "l_orderkey"
        );
        auto joined_table = final_join.Execute();

        // Step 6: Compute revenue = l_extendedprice * (1 - l_discount) for aggregation
        // Get columns needed for revenue calculation
        auto l_extendedprice_chunked = joined_table->GetColumnByName("l_extendedprice");
        auto l_discount_chunked = joined_table->GetColumnByName("l_discount");

        auto l_extendedprice_result = arrow::Concatenate(l_extendedprice_chunked->chunks());
        auto l_discount_result = arrow::Concatenate(l_discount_chunked->chunks());

        if (!l_extendedprice_result.ok() || !l_discount_result.ok()) {
            throw std::runtime_error("Failed to concatenate lineitem columns");
        }

        auto l_extendedprice_array = l_extendedprice_result.ValueOrDie();
        auto l_discount_array = l_discount_result.ValueOrDie();

        // Compute (1 - l_discount)
        auto one_scalar = arrow::MakeScalar(arrow::float64(), 1.0);
        auto one_minus_discount_result = arrow::compute::CallFunction(
            "subtract",
            std::vector<arrow::Datum>{one_scalar.ValueOrDie(), l_discount_array},
            &ctx
        );
        if (!one_minus_discount_result.ok()) {
            throw std::runtime_error("Failed to compute 1 - l_discount");
        }
        auto one_minus_discount = one_minus_discount_result.ValueOrDie().make_array();

        // Compute l_extendedprice * (1 - l_discount)
        auto revenue_result = arrow::compute::CallFunction(
            "multiply",
            std::vector<arrow::Datum>{l_extendedprice_array, one_minus_discount},
            &ctx
        );
        if (!revenue_result.ok()) {
            throw std::runtime_error("Failed to compute revenue");
        }
        auto revenue_array = revenue_result.ValueOrDie().make_array();

        // Add revenue column to joined table
        auto joined_schema = joined_table->schema();
        auto revenue_field = arrow::field("revenue", arrow::float64());
        auto new_schema_result = joined_schema->AddField(joined_schema->num_fields(), revenue_field);
        if (!new_schema_result.ok()) {
            throw std::runtime_error("Failed to add revenue field to schema");
        }
        auto new_schema = new_schema_result.ValueOrDie();

        auto revenue_chunked = std::make_shared<arrow::ChunkedArray>(revenue_array);
        auto columns = joined_table->columns();
        columns.push_back(revenue_chunked);
        auto table_with_revenue = arrow::Table::Make(new_schema, columns);

        // Step 7: Hash aggregation on (l_orderkey, o_orderdate, o_shippriority)
        // Aggregate: SUM(revenue) as revenue
        HashAgg agg(table_with_revenue);
        auto agg_result = agg
            .GroupBy({"l_orderkey", "o_orderdate", "o_shippriority"})
            .Aggregate(AggFunc::SUM, "revenue", "revenue")
            .Execute();

        // Step 8: Sort by revenue DESC, o_orderdate ASC
        // Get arrays for sorting
        auto revenue_agg_chunked = agg_result->GetColumnByName("revenue");
        auto o_orderdate_agg_chunked = agg_result->GetColumnByName("o_orderdate");

        auto revenue_agg_result = arrow::Concatenate(revenue_agg_chunked->chunks());
        auto o_orderdate_agg_result = arrow::Concatenate(o_orderdate_agg_chunked->chunks());

        if (!revenue_agg_result.ok() || !o_orderdate_agg_result.ok()) {
            throw std::runtime_error("Failed to concatenate aggregate columns");
        }

        auto revenue_agg_array = revenue_agg_result.ValueOrDie();
        auto o_orderdate_agg_array = o_orderdate_agg_result.ValueOrDie();

        // Sort by revenue DESC, o_orderdate ASC
        arrow::compute::SortOptions sort_options;
        sort_options.sort_keys = {
            arrow::compute::SortKey("revenue", arrow::compute::SortOrder::Descending),
            arrow::compute::SortKey("o_orderdate", arrow::compute::SortOrder::Ascending)
        };

        auto sort_indices_result = arrow::compute::CallFunction(
            "sort_indices",
            std::vector<arrow::Datum>{arrow::Datum(agg_result)},
            &sort_options,
            &ctx
        );
        if (!sort_indices_result.ok()) {
            throw std::runtime_error("Failed to compute sort indices");
        }
        auto sort_indices = sort_indices_result.ValueOrDie().make_array();

        // Apply sorting
        arrow::ArrayVector sorted_arrays;
        for (int i = 0; i < agg_result->num_columns(); ++i) {
            auto column = agg_result->column(i);
            auto combined_result = arrow::Concatenate(column->chunks());
            if (!combined_result.ok()) {
                throw std::runtime_error("Failed to concatenate column for sorting");
            }

            auto take_result = arrow::compute::CallFunction(
                "take",
                std::vector<arrow::Datum>{combined_result.ValueOrDie(), sort_indices},
                &ctx
            );
            if (!take_result.ok()) {
                throw std::runtime_error("Failed to apply sorting");
            }
            sorted_arrays.push_back(take_result.ValueOrDie().make_array());
        }
        auto sorted_table = arrow::Table::Make(agg_result->schema(), sorted_arrays);

        // Step 9: Limit to top 10
        auto final_result = sorted_table->Slice(0, std::min(static_cast<int64_t>(10), sorted_table->num_rows()));

        // Print row count and timing
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Q3 returned " << final_result->num_rows() << " rows in "
                  << duration.count() << " ms" << std::endl;

        // Write results to CSV if results_dir is provided
        if (!results_dir.empty()) {
            std::string output_file = results_dir + "/Q3.csv";
            std::ofstream out(output_file);
            if (!out.is_open()) {
                throw std::runtime_error("Failed to open output file: " + output_file);
            }

            // Get result columns
            auto l_orderkey_out = GetTypedColumn<arrow::StringType>(final_result, "l_orderkey");
            auto revenue_out = GetTypedColumn<arrow::DoubleType>(final_result, "revenue");
            auto o_orderdate_out = GetTypedColumn<arrow::StringType>(final_result, "o_orderdate");
            auto o_shippriority_out = GetTypedColumn<arrow::StringType>(final_result, "o_shippriority");

            // Write rows
            for (int64_t i = 0; i < final_result->num_rows(); ++i) {
                out << l_orderkey_out->GetString(i) << "|"
                    << std::fixed << std::setprecision(2) << revenue_out->Value(i) << "|"
                    << o_orderdate_out->GetString(i) << "|"
                    << o_shippriority_out->GetString(i) << "\n";
            }

            out.close();
        }

    } catch (const std::exception& e) {
        std::cerr << "Error in Q3: " << e.what() << std::endl;
        throw;
    }
}
