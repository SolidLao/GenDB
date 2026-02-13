#ifndef OPERATORS_SCAN_H
#define OPERATORS_SCAN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <vector>

namespace gendb {
namespace operators {

// Table scan with column projection
class Scan {
private:
    std::string parquet_file_;
    std::vector<std::string> column_names_;

public:
    explicit Scan(const std::string& parquet_file) : parquet_file_(parquet_file) {}

    Scan& Project(const std::vector<std::string>& columns) {
        column_names_ = columns;
        return *this;
    }

    std::shared_ptr<arrow::Table> Execute() {
        ParquetReader reader(parquet_file_);
        if (!column_names_.empty()) {
            return reader.ReadColumnsByName(column_names_);
        }
        return reader.ReadTable();
    }
};

// Scan with date range filter and row group pruning
class ScanWithDateFilter {
private:
    std::string parquet_file_;
    std::vector<std::string> column_names_;
    std::string date_column_;
    int32_t min_date_ = -1;  // -1 = no lower bound
    int32_t max_date_ = -1;  // -1 = no upper bound

public:
    explicit ScanWithDateFilter(const std::string& parquet_file) : parquet_file_(parquet_file) {}

    ScanWithDateFilter& Project(const std::vector<std::string>& columns) {
        column_names_ = columns;
        return *this;
    }

    ScanWithDateFilter& FilterDate(const std::string& column, int32_t min_date, int32_t max_date) {
        date_column_ = column;
        min_date_ = min_date;
        max_date_ = max_date;
        return *this;
    }

    std::shared_ptr<arrow::Table> Execute() {
        ParquetReader reader(parquet_file_);

        // Resolve column indices
        std::vector<int> column_indices;
        if (!column_names_.empty()) {
            auto schema = reader.schema();
            for (const auto& name : column_names_) {
                int idx = schema->GetFieldIndex(name);
                if (idx < 0) throw std::runtime_error("Column not found: " + name);
                column_indices.push_back(idx);
            }
        }

        std::shared_ptr<arrow::Table> result;

        if (!date_column_.empty()) {
            // Row group pruning
            auto selected_rgs = reader.FilterRowGroupsByDate32(date_column_, min_date_, max_date_);

            if (selected_rgs.empty()) {
                auto schema = reader.schema();
                arrow::FieldVector fields;
                arrow::ArrayVector empty_arrays;
                if (column_indices.empty()) {
                    for (int i = 0; i < schema->num_fields(); ++i) {
                        fields.push_back(schema->field(i));
                        empty_arrays.push_back(arrow::MakeArrayOfNull(schema->field(i)->type(), 0).ValueOrDie());
                    }
                } else {
                    for (int idx : column_indices) {
                        fields.push_back(schema->field(idx));
                        empty_arrays.push_back(arrow::MakeArrayOfNull(schema->field(idx)->type(), 0).ValueOrDie());
                    }
                }
                return arrow::Table::Make(arrow::schema(fields), empty_arrays);
            }

            // Read selected row groups
            if (column_indices.empty()) {
                // Read all columns — read each row group individually
                std::vector<std::shared_ptr<arrow::Table>> tables;
                for (int rg : selected_rgs) {
                    std::shared_ptr<arrow::Table> t;
                    // ReadRowGroups with empty column list doesn't work, use ReadTable per rg
                    auto all_cols = std::vector<int>();
                    for (int i = 0; i < reader.schema()->num_fields(); ++i) all_cols.push_back(i);
                    tables.push_back(reader.ReadRowGroups({rg}, all_cols));
                }
                result = CombineTables(tables);
            } else {
                std::vector<std::shared_ptr<arrow::Table>> tables;
                for (int rg : selected_rgs) {
                    tables.push_back(reader.ReadRowGroups({rg}, column_indices));
                }
                result = CombineTables(tables);
            }

            // Apply in-memory date filter (row groups may contain out-of-range rows)
            result = ApplyDateFilter(result, date_column_, min_date_, max_date_);
        } else {
            if (!column_names_.empty()) {
                result = reader.ReadColumnsByName(column_names_);
            } else {
                result = reader.ReadTable();
            }
        }

        return result;
    }

private:
    static std::shared_ptr<arrow::Table> ApplyDateFilter(
        const std::shared_ptr<arrow::Table>& table,
        const std::string& column_name,
        int32_t min_date,
        int32_t max_date
    ) {
        if (table->num_rows() == 0) return table;

        auto date_col = table->GetColumnByName(column_name);
        if (!date_col) throw std::runtime_error("Date column not found: " + column_name);

        auto combined = arrow::Concatenate(date_col->chunks()).ValueOrDie();
        auto date32_type = arrow::date32();
        std::shared_ptr<arrow::Array> mask;

        if (min_date >= 0 && max_date >= 0) {
            auto ge = arrow::compute::CallFunction("greater_equal",
                {arrow::Datum(combined), arrow::Datum(arrow::MakeScalar(date32_type, min_date).ValueOrDie())});
            auto le = arrow::compute::CallFunction("less_equal",
                {arrow::Datum(combined), arrow::Datum(arrow::MakeScalar(date32_type, max_date).ValueOrDie())});
            auto both = arrow::compute::CallFunction("and",
                {ge.ValueOrDie(), le.ValueOrDie()});
            mask = both.ValueOrDie().make_array();
        } else if (min_date >= 0) {
            auto ge = arrow::compute::CallFunction("greater_equal",
                {arrow::Datum(combined), arrow::Datum(arrow::MakeScalar(date32_type, min_date).ValueOrDie())});
            mask = ge.ValueOrDie().make_array();
        } else if (max_date >= 0) {
            auto le = arrow::compute::CallFunction("less_equal",
                {arrow::Datum(combined), arrow::Datum(arrow::MakeScalar(date32_type, max_date).ValueOrDie())});
            mask = le.ValueOrDie().make_array();
        } else {
            return table;
        }

        auto filtered = arrow::compute::CallFunction("filter",
            {arrow::Datum(table), arrow::Datum(mask)});
        return filtered.ValueOrDie().table();
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_SCAN_H
