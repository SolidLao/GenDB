#include "../arrow_helpers.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <vector>

using namespace gendb;
using namespace gendb::operators;

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Scan customer with c_mktsegment='BUILDING' filter
    // Column projection: [c_custkey, c_mktsegment]
    std::string customer_path = parquet_dir + "/customer.parquet";
    ParquetScan customer_scan(customer_path, {"c_custkey", "c_mktsegment"});
    auto customer_table = customer_scan.Execute();

    // Apply string filter for c_mktsegment='BUILDING'
    customer_table = FilterByStringEquality(customer_table, "c_mktsegment", "BUILDING");

    std::cout << "Customer filtered: " << customer_table->num_rows() << " rows" << std::endl;

    // Step 2: Scan orders with o_orderdate < '1995-03-15' filter
    // Column projection: [o_custkey, o_orderkey, o_orderdate, o_shippriority]
    // Use row group pruning on o_orderdate
    std::string orders_path = parquet_dir + "/orders.parquet";
    ParquetScan orders_scan(orders_path, {"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"});
    auto orders_table = orders_scan.ExecuteWithDateComparison("o_orderdate", "<", "1995-03-15");

    std::cout << "Orders filtered: " << orders_table->num_rows() << " rows" << std::endl;

    // Step 3: HashJoin customer and orders on c_custkey=o_custkey
    // Build on customer (smaller: 300K vs 6M)
    HashJoin join1;
    join1.Build(customer_table, "c_custkey");
    auto customer_orders = join1.Probe(orders_table, "o_custkey", customer_table, "c_custkey");

    std::cout << "Customer-Orders join: " << customer_orders->num_rows() << " rows" << std::endl;

    // Step 4: Scan lineitem with l_shipdate > '1995-03-15' filter
    // Column projection: [l_orderkey, l_extendedprice, l_discount, l_shipdate]
    // Use row group pruning on l_shipdate
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    ParquetScan lineitem_scan(lineitem_path, {"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"});
    auto lineitem_table = lineitem_scan.ExecuteWithDateComparison("l_shipdate", ">", "1995-03-15");

    std::cout << "Lineitem filtered: " << lineitem_table->num_rows() << " rows" << std::endl;

    // Step 5: HashJoin customer_orders and lineitem on o_orderkey=l_orderkey
    // Build on customer_orders (smaller: 6M vs 30M)
    HashJoin join2;
    join2.Build(customer_orders, "o_orderkey");
    auto joined_table = join2.Probe(lineitem_table, "l_orderkey", customer_orders, "o_orderkey");

    std::cout << "Final join: " << joined_table->num_rows() << " rows" << std::endl;

    // Step 6: Compute revenue = l_extendedprice * (1 - l_discount)
    auto l_extendedprice = GetTypedColumn<arrow::DoubleType>(joined_table, "l_extendedprice");
    auto l_discount = GetTypedColumn<arrow::DoubleType>(joined_table, "l_discount");

    arrow::DoubleBuilder revenue_builder;
    revenue_builder.Reserve(joined_table->num_rows());

    for (int64_t i = 0; i < joined_table->num_rows(); ++i) {
        if (l_extendedprice->IsNull(i) || l_discount->IsNull(i)) {
            revenue_builder.AppendNull();
        } else {
            double price = l_extendedprice->Value(i);
            double discount = l_discount->Value(i);
            double revenue = price * (1.0 - discount);
            revenue_builder.UnsafeAppend(revenue);
        }
    }

    auto revenue_array = revenue_builder.Finish().ValueOrDie();

    // Add revenue column to joined table
    auto schema_fields = joined_table->schema()->fields();
    schema_fields.push_back(arrow::field("revenue", arrow::float64()));
    auto new_schema = arrow::schema(schema_fields);

    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    for (int i = 0; i < joined_table->num_columns(); ++i) {
        columns.push_back(joined_table->column(i));
    }
    columns.push_back(std::make_shared<arrow::ChunkedArray>(revenue_array));

    auto table_with_revenue = arrow::Table::Make(new_schema, columns);

    // Step 7: MultiKeyHashAggregate on [l_orderkey, o_orderdate, o_shippriority]
    // Computing SUM(revenue)
    MultiKeyHashAggregate agg;
    std::vector<AggregateSpec> agg_specs = {
        {"revenue", AggregateOp::SUM, "revenue"}
    };
    auto agg_table = agg.Execute(table_with_revenue,
                                 {"l_orderkey", "o_orderdate", "o_shippriority"},
                                 agg_specs);

    std::cout << "Aggregated groups: " << agg_table->num_rows() << " rows" << std::endl;

    // Step 8: Top-K sort by revenue DESC, o_orderdate ASC, LIMIT 10
    // Extract arrays for sorting
    auto l_orderkey_arr = GetTypedColumn<arrow::Int32Type>(agg_table, "l_orderkey");
    auto o_orderdate_arr = GetTypedColumn<arrow::Int32Type>(agg_table, "o_orderdate");
    auto o_shippriority_arr = GetTypedColumn<arrow::Int32Type>(agg_table, "o_shippriority");
    auto revenue_arr = GetTypedColumn<arrow::DoubleType>(agg_table, "revenue");

    // Create index array for sorting
    struct SortEntry {
        int64_t index;
        double revenue;
        int32_t orderdate;
    };

    std::vector<SortEntry> sort_entries;
    sort_entries.reserve(agg_table->num_rows());

    for (int64_t i = 0; i < agg_table->num_rows(); ++i) {
        SortEntry entry;
        entry.index = i;
        entry.revenue = revenue_arr->Value(i);
        entry.orderdate = o_orderdate_arr->Value(i);
        sort_entries.push_back(entry);
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::partial_sort(sort_entries.begin(),
                      sort_entries.begin() + std::min(10L, static_cast<int64_t>(sort_entries.size())),
                      sort_entries.end(),
                      [](const SortEntry& a, const SortEntry& b) {
                          if (a.revenue != b.revenue) {
                              return a.revenue > b.revenue; // DESC
                          }
                          return a.orderdate < b.orderdate; // ASC
                      });

    // Take top 10
    int64_t result_rows = std::min(10L, static_cast<int64_t>(sort_entries.size()));

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q3.csv";
        std::ofstream outfile(output_path);

        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open output file: " + output_path);
        }

        // Write header
        outfile << "l_orderkey|revenue|o_orderdate|o_shippriority\n";

        // Write top 10 rows
        for (int64_t i = 0; i < result_rows; ++i) {
            int64_t idx = sort_entries[i].index;
            outfile << l_orderkey_arr->Value(idx) << "|"
                    << std::fixed << std::setprecision(2) << revenue_arr->Value(idx) << "|"
                    << o_orderdate_arr->Value(idx) << "|"
                    << o_shippriority_arr->Value(idx) << "\n";
        }

        outfile.close();
        std::cout << "Results written to " << output_path << std::endl;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q3 completed: " << result_rows << " result rows" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
}
