#ifndef OPERATORS_SCAN_H
#define OPERATORS_SCAN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace gendb {
namespace operators {

// Table scan operator with column projection and predicate pushdown
class Scan {
private:
    std::string parquet_file_;
    std::vector<std::string> column_names_;
    std::vector<int> column_indices_;
    std::shared_ptr<arrow::Table> result_;

public:
    explicit Scan(const std::string& parquet_file)
        : parquet_file_(parquet_file) {}

    // Set columns to project (by name)
    Scan& Project(const std::vector<std::string>& columns) {
        column_names_ = columns;
        return *this;
    }

    // Set columns to project (by index)
    Scan& ProjectIndices(const std::vector<int>& indices) {
        column_indices_ = indices;
        return *this;
    }

    // Execute scan and return table
    std::shared_ptr<arrow::Table> Execute() {
        ParquetReader reader(parquet_file_);

        if (!column_names_.empty()) {
            result_ = reader.ReadColumnsByName(column_names_);
        } else if (!column_indices_.empty()) {
            result_ = reader.ReadColumns(column_indices_);
        } else {
            result_ = reader.ReadTable();
        }

        return result_;
    }

    std::shared_ptr<arrow::Table> GetResult() const {
        return result_;
    }
};

// Scan with date range filter (uses row group pruning)
class ScanWithDateFilter {
private:
    std::string parquet_file_;
    std::vector<std::string> column_names_;
    std::string date_column_;
    int32_t min_date_;  // Arrow date32, -1 = no lower bound
    int32_t max_date_;  // Arrow date32, -1 = no upper bound
    std::shared_ptr<arrow::Table> result_;

public:
    explicit ScanWithDateFilter(const std::string& parquet_file)
        : parquet_file_(parquet_file), min_date_(-1), max_date_(-1) {}

    // Set columns to project
    ScanWithDateFilter& Project(const std::vector<std::string>& columns) {
        column_names_ = columns;
        return *this;
    }

    // Set date filter (uses row group pruning if data is sorted by this column)
    ScanWithDateFilter& FilterDate(
        const std::string& column,
        int32_t min_date,
        int32_t max_date
    ) {
        date_column_ = column;
        min_date_ = min_date;
        max_date_ = max_date;
        return *this;
    }

    // Execute scan with row group pruning
    std::shared_ptr<arrow::Table> Execute() {
        ParquetReader reader(parquet_file_);

        std::vector<int> column_indices;
        if (!column_names_.empty()) {
            auto schema = reader.schema();
            for (const auto& name : column_names_) {
                int idx = schema->GetFieldIndex(name);
                if (idx < 0) {
                    throw std::runtime_error("Column not found: " + name);
                }
                column_indices.push_back(idx);
            }
        }

        // Apply row group filtering if date filter is set
        if (!date_column_.empty()) {
            auto selected_row_groups = reader.FilterRowGroupsByDate32(
                date_column_, min_date_, max_date_
            );

            // Read only selected row groups
            if (column_indices.empty()) {
                // Read all columns from selected row groups
                std::vector<std::shared_ptr<arrow::Table>> tables;
                for (int rg : selected_row_groups) {
                    auto table = reader.ReadRowGroups({rg}, {});
                    tables.push_back(table);
                }
                if (!tables.empty()) {
                    result_ = CombineTables(tables);
                } else {
                    // Return empty table with correct schema
                    arrow::ArrayVector empty_arrays;
                    auto schema = reader.schema();
                    for (int i = 0; i < schema->num_fields(); ++i) {
                        auto array_result = arrow::MakeArrayOfNull(schema->field(i)->type(), 0);
                        if (!array_result.ok()) {
                            throw std::runtime_error("Failed to create empty array");
                        }
                        empty_arrays.push_back(array_result.ValueOrDie());
                    }
                    result_ = arrow::Table::Make(schema, empty_arrays);
                }
            } else {
                // Read specific columns from selected row groups
                std::vector<std::shared_ptr<arrow::Table>> tables;
                for (int rg : selected_row_groups) {
                    auto table = reader.ReadRowGroups({rg}, column_indices);
                    tables.push_back(table);
                }
                if (!tables.empty()) {
                    result_ = CombineTables(tables);
                } else {
                    // Return empty table with correct schema
                    arrow::ArrayVector empty_arrays;
                    auto schema = reader.schema();
                    for (int idx : column_indices) {
                        auto array_result = arrow::MakeArrayOfNull(schema->field(idx)->type(), 0);
                        if (!array_result.ok()) {
                            throw std::runtime_error("Failed to create empty array");
                        }
                        empty_arrays.push_back(array_result.ValueOrDie());
                    }
                    arrow::FieldVector fields;
                    for (int idx : column_indices) {
                        fields.push_back(schema->field(idx));
                    }
                    auto filtered_schema = arrow::schema(fields);
                    result_ = arrow::Table::Make(filtered_schema, empty_arrays);
                }
            }

            // Apply in-memory filter to handle rows within selected row groups
            // that don't match the predicate
            if (result_->num_rows() > 0) {
                result_ = ApplyDateFilter(result_, date_column_, min_date_, max_date_);
            }
        } else {
            // No date filter, read normally
            if (!column_names_.empty()) {
                result_ = reader.ReadColumnsByName(column_names_);
            } else {
                result_ = reader.ReadTable();
            }
        }

        return result_;
    }

    std::shared_ptr<arrow::Table> GetResult() const {
        return result_;
    }

private:
    // Apply date filter using Arrow compute
    static std::shared_ptr<arrow::Table> ApplyDateFilter(
        const std::shared_ptr<arrow::Table>& table,
        const std::string& column_name,
        int32_t min_date,
        int32_t max_date
    ) {
        auto date_array = table->GetColumnByName(column_name);
        if (!date_array) {
            throw std::runtime_error("Date column not found: " + column_name);
        }

        // Combine chunks
        auto combined_result = arrow::Concatenate(date_array->chunks());
        if (!combined_result.ok()) {
            throw std::runtime_error("Failed to concatenate date chunks");
        }
        auto combined_date = combined_result.ValueOrDie();

        // Build filter mask
        arrow::compute::ExecContext ctx;
        std::shared_ptr<arrow::Array> mask;

        // Create date32 scalars for comparison
        auto date32_type = arrow::date32();

        if (min_date >= 0 && max_date >= 0) {
            // Both bounds
            auto min_scalar = arrow::MakeScalar(date32_type, min_date);
            auto max_scalar = arrow::MakeScalar(date32_type, max_date);

            auto ge_result = arrow::compute::CallFunction(
                "greater_equal",
                {combined_date, min_scalar.ValueOrDie()},
                &ctx
            );
            auto le_result = arrow::compute::CallFunction(
                "less_equal",
                {combined_date, max_scalar.ValueOrDie()},
                &ctx
            );

            if (!ge_result.ok() || !le_result.ok()) {
                throw std::runtime_error("Failed to apply date comparison: " +
                    (ge_result.ok() ? le_result.status().ToString() : ge_result.status().ToString()));
            }

            auto and_result = arrow::compute::CallFunction(
                "and",
                {ge_result.ValueOrDie().make_array(), le_result.ValueOrDie().make_array()},
                &ctx
            );

            if (!and_result.ok()) {
                throw std::runtime_error("Failed to combine predicates");
            }

            mask = and_result.ValueOrDie().make_array();
        } else if (min_date >= 0) {
            // Only lower bound
            auto min_scalar = arrow::MakeScalar(date32_type, min_date);
            auto ge_result = arrow::compute::CallFunction(
                "greater_equal",
                {combined_date, min_scalar.ValueOrDie()},
                &ctx
            );
            if (!ge_result.ok()) {
                throw std::runtime_error("Failed to apply date comparison: " + ge_result.status().ToString());
            }
            mask = ge_result.ValueOrDie().make_array();
        } else if (max_date >= 0) {
            // Only upper bound
            auto max_scalar = arrow::MakeScalar(date32_type, max_date);
            auto le_result = arrow::compute::CallFunction(
                "less_equal",
                {combined_date, max_scalar.ValueOrDie()},
                &ctx
            );
            if (!le_result.ok()) {
                throw std::runtime_error("Failed to apply date comparison: " + le_result.status().ToString());
            }
            mask = le_result.ValueOrDie().make_array();
        } else {
            // No filter
            return table;
        }

        // Apply filter to table
        auto filtered_result = arrow::compute::CallFunction(
            "filter",
            {arrow::Datum(table), arrow::Datum(mask)},
            &ctx
        );

        if (!filtered_result.ok()) {
            throw std::runtime_error("Failed to filter table: " + filtered_result.status().ToString());
        }

        return filtered_result.ValueOrDie().table();
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_SCAN_H
