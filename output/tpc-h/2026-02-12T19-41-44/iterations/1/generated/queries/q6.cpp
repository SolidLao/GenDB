#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>

using namespace gendb;

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Scan lineitem table with date range filter and column projection
    // CRITICAL: Use row group pruning on sorted l_shipdate to read only 100/800 row groups
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    ParquetReader reader(lineitem_path);

    // Apply Parquet predicate pushdown for date range: 1994-01-01 to 1994-12-31 (exclusive)
    // Date range: [1994-01-01, 1995-01-01)
    int32_t min_date = DateToDays("1994-01-01");
    int32_t max_date = DateToDays("1994-12-31");  // Inclusive upper bound for row group pruning

    // Get row groups that overlap with date range (leverages sorted l_shipdate)
    // This skips ~87.5% of row groups (reading only 100/800 row groups)
    auto selected_row_groups = reader.GetRowGroupsForDateRange("l_shipdate", min_date, max_date);

    std::shared_ptr<arrow::Table> table;
    if (selected_row_groups.empty()) {
        // No matching row groups - create empty result
        std::cout << "Query Q6: 0 rows (no matching row groups)" << std::endl;
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << "revenue" << std::endl;
            out << "0.00" << std::endl;
        }
        return;
    }

    // Read only selected row groups with column projection
    std::vector<std::string> columns = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
    table = reader.ReadRowGroups(selected_row_groups, columns);

    if (table->num_rows() == 0) {
        std::cout << "Query Q6: 0 rows (empty table)" << std::endl;
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << "revenue" << std::endl;
            out << "0.00" << std::endl;
        }
        return;
    }

    // Step 2: Apply in-memory filters

    // Extract column arrays for filtering
    auto shipdate_array = GetTypedColumn<arrow::Date32Type>(table, "l_shipdate");
    auto discount_array = GetTypedColumn<arrow::DoubleType>(table, "l_discount");
    auto quantity_array = GetTypedColumn<arrow::DoubleType>(table, "l_quantity");
    auto extendedprice_array = GetTypedColumn<arrow::DoubleType>(table, "l_extendedprice");

    // Build combined filter:
    // l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    // AND l_discount BETWEEN 0.05 AND 0.07
    // AND l_quantity < 24
    int32_t date_lower = DateToDays("1994-01-01");
    int32_t date_upper = DateToDays("1995-01-01");  // Exclusive

    arrow::BooleanBuilder filter_builder;
    auto status = filter_builder.Reserve(table->num_rows());
    if (!status.ok()) {
        throw std::runtime_error("Failed to reserve filter builder");
    }

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        bool pass = true;

        // Check shipdate filter
        if (shipdate_array->IsNull(i)) {
            pass = false;
        } else {
            int32_t date = shipdate_array->Value(i);
            if (date < date_lower || date >= date_upper) {
                pass = false;
            }
        }

        // Check discount filter: BETWEEN 0.05 AND 0.07
        if (pass && discount_array->IsNull(i)) {
            pass = false;
        } else if (pass) {
            double discount = discount_array->Value(i);
            if (discount < 0.05 || discount > 0.07) {
                pass = false;
            }
        }

        // Check quantity filter: < 24
        if (pass && quantity_array->IsNull(i)) {
            pass = false;
        } else if (pass) {
            double quantity = quantity_array->Value(i);
            if (quantity >= 24.0) {
                pass = false;
            }
        }

        filter_builder.UnsafeAppend(pass);
    }

    auto filter_result = filter_builder.Finish();
    if (!filter_result.ok()) {
        throw std::runtime_error("Failed to build filter");
    }
    auto filter = filter_result.ValueOrDie();

    // Apply filter to table
    auto filtered_result = arrow::compute::Filter(table, filter);
    if (!filtered_result.ok()) {
        throw std::runtime_error("Failed to filter table: " + filtered_result.status().ToString());
    }

    // Extract filtered datum as table
    auto filtered_datum = filtered_result.ValueOrDie();
    std::shared_ptr<arrow::Table> filtered_table;
    if (filtered_datum.kind() == arrow::Datum::TABLE) {
        filtered_table = filtered_datum.table();
    } else if (filtered_datum.kind() == arrow::Datum::RECORD_BATCH) {
        auto batch = filtered_datum.record_batch();
        filtered_table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();
    } else {
        throw std::runtime_error("Unexpected datum type from filter");
    }

    if (filtered_table->num_rows() == 0) {
        std::cout << "Query Q6: 0 rows (after filtering)" << std::endl;
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << "revenue" << std::endl;
            out << "0.00" << std::endl;
        }
        return;
    }

    // Step 3: Compute revenue = l_extendedprice * l_discount using Arrow compute
    auto filtered_extendedprice = GetTypedColumn<arrow::DoubleType>(filtered_table, "l_extendedprice");
    auto filtered_discount = GetTypedColumn<arrow::DoubleType>(filtered_table, "l_discount");

    auto multiply_result = arrow::compute::Multiply(filtered_extendedprice, filtered_discount);
    if (!multiply_result.ok()) {
        throw std::runtime_error("Failed to multiply: " + multiply_result.status().ToString());
    }

    // Extract result as DoubleArray
    auto multiply_datum = multiply_result.ValueOrDie();
    std::shared_ptr<arrow::DoubleArray> revenue_array;
    if (multiply_datum.is_array()) {
        revenue_array = std::static_pointer_cast<arrow::DoubleArray>(multiply_datum.make_array());
    } else {
        throw std::runtime_error("Unexpected multiply result type");
    }

    // Step 4: Compute SUM(revenue) - scalar accumulation (no GROUP BY)
    double total_revenue = 0.0;
    for (int64_t i = 0; i < revenue_array->length(); ++i) {
        if (!revenue_array->IsNull(i)) {
            total_revenue += revenue_array->Value(i);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output results
    std::cout << "Query Q6: 1 row" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;

    // Write CSV output if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue" << std::endl;
        out << std::fixed << std::setprecision(2) << total_revenue << std::endl;
    }
}
