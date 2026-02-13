#ifndef OPERATORS_SCAN_H
#define OPERATORS_SCAN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <vector>
#include <string>

namespace gendb {
namespace operators {

// ============================================================================
// Parquet Table Scan Operator
// ============================================================================

class ParquetScan {
private:
    std::string filepath_;
    std::vector<std::string> columns_;
    ParquetReader reader_;

public:
    ParquetScan(const std::string& filepath, const std::vector<std::string>& columns = {})
        : filepath_(filepath), columns_(columns), reader_(filepath) {
    }

    // Simple scan - read all data with column projection
    std::shared_ptr<arrow::Table> Execute() {
        return reader_.ReadTable(columns_);
    }

    // Scan with date range filter (enables row group pruning)
    std::shared_ptr<arrow::Table> ExecuteWithDateFilter(
        const std::string& date_column,
        const std::string& min_date_str,
        const std::string& max_date_str) {

        int32_t min_date = DateToDays(min_date_str);
        int32_t max_date = DateToDays(max_date_str);

        // Get row groups that overlap with date range
        auto row_groups = reader_.GetRowGroupsForDateRange(date_column, min_date, max_date);

        if (row_groups.empty()) {
            // No matching row groups - return empty table
            return arrow::Table::Make(reader_.GetSchema(), {});
        }

        // Read selected row groups with column projection
        auto table = reader_.ReadRowGroups(row_groups, columns_);

        // Apply in-memory filter to exact date range
        // (row group pruning is coarse, we need exact filtering)
        return FilterTableByDateRange(table, date_column, min_date, max_date);
    }

    // Scan with date range filter (comparison operators)
    std::shared_ptr<arrow::Table> ExecuteWithDateComparison(
        const std::string& date_column,
        const std::string& op, // "<", "<=", ">", ">=", "="
        const std::string& date_str) {

        int32_t date_value = DateToDays(date_str);

        // Determine row group scan range based on operator
        std::vector<int> row_groups;
        if (op == "<" || op == "<=") {
            // Scan from beginning up to date
            row_groups = reader_.GetRowGroupsForDateRange(date_column, INT32_MIN, date_value);
        } else if (op == ">" || op == ">=") {
            // Scan from date to end
            row_groups = reader_.GetRowGroupsForDateRange(date_column, date_value, INT32_MAX);
        } else if (op == "=") {
            // Scan only matching date
            row_groups = reader_.GetRowGroupsForDateRange(date_column, date_value, date_value);
        } else {
            throw std::runtime_error("Unsupported comparison operator: " + op);
        }

        if (row_groups.empty()) {
            return arrow::Table::Make(reader_.GetSchema(), {});
        }

        auto table = reader_.ReadRowGroups(row_groups, columns_);

        // Apply exact filter
        return FilterTableByDateComparison(table, date_column, op, date_value);
    }

private:
    // Filter table by date range (in-memory)
    std::shared_ptr<arrow::Table> FilterTableByDateRange(
        std::shared_ptr<arrow::Table> table,
        const std::string& date_column,
        int32_t min_date,
        int32_t max_date) {

        if (table->num_rows() == 0) {
            return table;
        }

        auto date_array = GetTypedColumn<arrow::Date32Type>(table, date_column);

        // Build boolean filter array
        arrow::BooleanBuilder filter_builder;
        auto status = filter_builder.Reserve(table->num_rows());
        if (!status.ok()) {
            throw std::runtime_error("Failed to reserve filter builder");
        }

        for (int64_t i = 0; i < date_array->length(); ++i) {
            if (date_array->IsNull(i)) {
                filter_builder.UnsafeAppend(false);
            } else {
                int32_t val = date_array->Value(i);
                filter_builder.UnsafeAppend(val >= min_date && val <= max_date);
            }
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

        return arrow::compute::GetRecordBatchAsTable(filtered_result.ValueOrDie());
    }

    // Filter table by date comparison (in-memory)
    std::shared_ptr<arrow::Table> FilterTableByDateComparison(
        std::shared_ptr<arrow::Table> table,
        const std::string& date_column,
        const std::string& op,
        int32_t date_value) {

        if (table->num_rows() == 0) {
            return table;
        }

        auto date_array = GetTypedColumn<arrow::Date32Type>(table, date_column);

        // Build boolean filter array
        arrow::BooleanBuilder filter_builder;
        auto status = filter_builder.Reserve(table->num_rows());
        if (!status.ok()) {
            throw std::runtime_error("Failed to reserve filter builder");
        }

        for (int64_t i = 0; i < date_array->length(); ++i) {
            if (date_array->IsNull(i)) {
                filter_builder.UnsafeAppend(false);
            } else {
                int32_t val = date_array->Value(i);
                bool match = false;
                if (op == "<") match = (val < date_value);
                else if (op == "<=") match = (val <= date_value);
                else if (op == ">") match = (val > date_value);
                else if (op == ">=") match = (val >= date_value);
                else if (op == "=") match = (val == date_value);
                filter_builder.UnsafeAppend(match);
            }
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

        return arrow::compute::GetRecordBatchAsTable(filtered_result.ValueOrDie());
    }
};

// ============================================================================
// Generic Filter Helpers
// ============================================================================

// Filter table by numeric column range
template<typename ArrowType>
std::shared_ptr<arrow::Table> FilterByRange(
    std::shared_ptr<arrow::Table> table,
    const std::string& column_name,
    typename ArrowType::c_type min_value,
    typename ArrowType::c_type max_value) {

    if (table->num_rows() == 0) {
        return table;
    }

    auto array = GetTypedColumn<ArrowType>(table, column_name);

    arrow::BooleanBuilder filter_builder;
    auto status = filter_builder.Reserve(table->num_rows());
    if (!status.ok()) {
        throw std::runtime_error("Failed to reserve filter builder");
    }

    for (int64_t i = 0; i < array->length(); ++i) {
        if (array->IsNull(i)) {
            filter_builder.UnsafeAppend(false);
        } else {
            auto val = array->Value(i);
            filter_builder.UnsafeAppend(val >= min_value && val <= max_value);
        }
    }

    auto filter_result = filter_builder.Finish();
    if (!filter_result.ok()) {
        throw std::runtime_error("Failed to build filter");
    }
    auto filter = filter_result.ValueOrDie();

    auto filtered_result = arrow::compute::Filter(table, filter);
    if (!filtered_result.ok()) {
        throw std::runtime_error("Failed to filter table: " + filtered_result.status().ToString());
    }

    return arrow::compute::GetRecordBatchAsTable(filtered_result.ValueOrDie());
}

// Filter table by string equality
inline std::shared_ptr<arrow::Table> FilterByStringEquality(
    std::shared_ptr<arrow::Table> table,
    const std::string& column_name,
    const std::string& target_value) {

    if (table->num_rows() == 0) {
        return table;
    }

    auto array = GetTypedColumn<arrow::StringType>(table, column_name);

    arrow::BooleanBuilder filter_builder;
    auto status = filter_builder.Reserve(table->num_rows());
    if (!status.ok()) {
        throw std::runtime_error("Failed to reserve filter builder");
    }

    for (int64_t i = 0; i < array->length(); ++i) {
        if (array->IsNull(i)) {
            filter_builder.UnsafeAppend(false);
        } else {
            std::string val = array->GetString(i);
            // Trim whitespace (TPC-H CHAR columns are space-padded)
            val.erase(val.find_last_not_of(' ') + 1);
            filter_builder.UnsafeAppend(val == target_value);
        }
    }

    auto filter_result = filter_builder.Finish();
    if (!filter_result.ok()) {
        throw std::runtime_error("Failed to build filter");
    }
    auto filter = filter_result.ValueOrDie();

    auto filtered_result = arrow::compute::Filter(table, filter);
    if (!filtered_result.ok()) {
        throw std::runtime_error("Failed to filter table: " + filtered_result.status().ToString());
    }

    return arrow::compute::GetRecordBatchAsTable(filtered_result.ValueOrDie());
}

} // namespace operators
} // namespace gendb

#endif // OPERATORS_SCAN_H
