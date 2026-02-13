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
#include <map>

using namespace gendb;
using namespace gendb::operators;

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Scan lineitem table with date filter and column projection
    // Filter: l_shipdate <= '1998-09-02' (which is '1998-12-01' - 90 days)
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    std::vector<std::string> columns = {
        "l_shipdate", "l_returnflag", "l_linestatus",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax"
    };

    ParquetScan scan(lineitem_path, columns);
    auto table = scan.ExecuteWithDateComparison("l_shipdate", "<=", "1998-09-02");

    if (table->num_rows() == 0) {
        std::cout << "No rows matched filter" << std::endl;
        return;
    }

    // Step 2: Compute derived columns using Arrow compute kernels
    // disc_price = l_extendedprice * (1 - l_discount)
    // charge = disc_price * (1 + l_tax)

    auto l_extendedprice = GetTypedColumn<arrow::DoubleType>(table, "l_extendedprice");
    auto l_discount = GetTypedColumn<arrow::DoubleType>(table, "l_discount");
    auto l_tax = GetTypedColumn<arrow::DoubleType>(table, "l_tax");

    // Compute (1 - l_discount)
    auto one_scalar = arrow::MakeScalar(1.0);
    auto one_minus_discount_result = arrow::compute::Subtract(one_scalar, l_discount);
    if (!one_minus_discount_result.ok()) {
        throw std::runtime_error("Failed to compute (1 - l_discount): " + one_minus_discount_result.status().ToString());
    }
    auto one_minus_discount = one_minus_discount_result.ValueOrDie().make_array();

    // Compute disc_price = l_extendedprice * (1 - l_discount)
    auto disc_price_result = arrow::compute::Multiply(l_extendedprice, one_minus_discount);
    if (!disc_price_result.ok()) {
        throw std::runtime_error("Failed to compute disc_price: " + disc_price_result.status().ToString());
    }
    auto disc_price_array = std::static_pointer_cast<arrow::DoubleArray>(
        disc_price_result.ValueOrDie().make_array());

    // Compute (1 + l_tax)
    auto one_plus_tax_result = arrow::compute::Add(one_scalar, l_tax);
    if (!one_plus_tax_result.ok()) {
        throw std::runtime_error("Failed to compute (1 + l_tax): " + one_plus_tax_result.status().ToString());
    }
    auto one_plus_tax = one_plus_tax_result.ValueOrDie().make_array();

    // Compute charge = disc_price * (1 + l_tax)
    auto charge_result = arrow::compute::Multiply(disc_price_array, one_plus_tax);
    if (!charge_result.ok()) {
        throw std::runtime_error("Failed to compute charge: " + charge_result.status().ToString());
    }
    auto charge_array = std::static_pointer_cast<arrow::DoubleArray>(
        charge_result.ValueOrDie().make_array());

    // Step 3: Add computed columns to the table
    auto disc_price_field = arrow::field("disc_price", arrow::float64());
    auto charge_field = arrow::field("charge", arrow::float64());

    auto schema_result = table->schema()->AddField(table->num_columns(), disc_price_field);
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to add disc_price field");
    }
    auto new_schema = schema_result.ValueOrDie();

    schema_result = new_schema->AddField(new_schema->num_fields(), charge_field);
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to add charge field");
    }
    new_schema = schema_result.ValueOrDie();

    std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
    for (int i = 0; i < table->num_columns(); ++i) {
        new_columns.push_back(table->column(i));
    }
    new_columns.push_back(std::make_shared<arrow::ChunkedArray>(disc_price_array));
    new_columns.push_back(std::make_shared<arrow::ChunkedArray>(charge_array));

    table = arrow::Table::Make(new_schema, new_columns);

    // Step 4: Group by l_returnflag, l_linestatus using composite string key
    // We need to handle two string columns as group keys
    // Create a composite key by concatenating returnflag and linestatus

    auto l_returnflag = GetTypedColumn<arrow::StringType>(table, "l_returnflag");
    auto l_linestatus = GetTypedColumn<arrow::StringType>(table, "l_linestatus");
    auto l_quantity = GetTypedColumn<arrow::DoubleType>(table, "l_quantity");

    // Manual hash aggregation with composite string key
    struct AggState {
        double sum_qty = 0.0;
        double sum_base_price = 0.0;
        double sum_disc_price = 0.0;
        double sum_charge = 0.0;
        double sum_qty_for_avg = 0.0;
        double sum_price_for_avg = 0.0;
        double sum_disc_for_avg = 0.0;
        int64_t count = 0;
    };

    std::map<std::pair<std::string, std::string>, AggState> groups;

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        if (l_returnflag->IsNull(i) || l_linestatus->IsNull(i)) continue;

        std::string returnflag = l_returnflag->GetString(i);
        std::string linestatus = l_linestatus->GetString(i);

        // Trim whitespace (TPC-H uses CHAR columns)
        returnflag.erase(returnflag.find_last_not_of(' ') + 1);
        linestatus.erase(linestatus.find_last_not_of(' ') + 1);

        auto key = std::make_pair(returnflag, linestatus);
        auto& state = groups[key];

        double qty = l_quantity->IsNull(i) ? 0.0 : l_quantity->Value(i);
        double base_price = l_extendedprice->IsNull(i) ? 0.0 : l_extendedprice->Value(i);
        double disc_price = disc_price_array->IsNull(i) ? 0.0 : disc_price_array->Value(i);
        double charge = charge_array->IsNull(i) ? 0.0 : charge_array->Value(i);
        double discount = l_discount->IsNull(i) ? 0.0 : l_discount->Value(i);

        state.sum_qty += qty;
        state.sum_base_price += base_price;
        state.sum_disc_price += disc_price;
        state.sum_charge += charge;
        state.sum_qty_for_avg += qty;
        state.sum_price_for_avg += base_price;
        state.sum_disc_for_avg += discount;
        state.count++;
    }

    // Step 5: Build output (already sorted by map key which orders by returnflag, linestatus)
    std::vector<std::tuple<std::string, std::string, double, double, double, double, double, double, double, int64_t>> results;

    for (const auto& [key, state] : groups) {
        double avg_qty = state.count > 0 ? state.sum_qty_for_avg / state.count : 0.0;
        double avg_price = state.count > 0 ? state.sum_price_for_avg / state.count : 0.0;
        double avg_disc = state.count > 0 ? state.sum_disc_for_avg / state.count : 0.0;

        results.push_back(std::make_tuple(
            key.first, key.second,
            state.sum_qty, state.sum_base_price, state.sum_disc_price, state.sum_charge,
            avg_qty, avg_price, avg_disc, state.count
        ));
    }

    // Step 6: Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);

        for (const auto& row : results) {
            out << std::get<0>(row) << "|"  // l_returnflag
                << std::get<1>(row) << "|"  // l_linestatus
                << std::get<2>(row) << "|"  // sum_qty
                << std::get<3>(row) << "|"  // sum_base_price
                << std::get<4>(row) << "|"  // sum_disc_price
                << std::get<5>(row) << "|"  // sum_charge
                << std::get<6>(row) << "|"  // avg_qty
                << std::get<7>(row) << "|"  // avg_price
                << std::get<8>(row) << "|"  // avg_disc
                << std::get<9>(row) << "\n";  // count_order
        }

        out.close();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q1 Results: " << results.size() << " rows" << std::endl;
    std::cout << "Q1 Execution Time: " << duration.count() << " ms" << std::endl;
}
