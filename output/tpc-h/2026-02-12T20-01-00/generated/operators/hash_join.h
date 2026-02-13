#ifndef OPERATORS_HASH_JOIN_H
#define OPERATORS_HASH_JOIN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>

namespace gendb {
namespace operators {

// Hash join on int32 keys (most common case for TPC-H)
// Supports inner join: build hash table from right, probe from left
class HashJoinInt32 {
private:
    std::shared_ptr<arrow::Table> left_table_;
    std::shared_ptr<arrow::Table> right_table_;
    std::string left_key_column_;
    std::string right_key_column_;
    std::shared_ptr<arrow::Table> result_;

    // Hash table: key -> vector of row indices in right table
    std::unordered_map<int32_t, std::vector<int64_t>> hash_table_;

public:
    HashJoinInt32(
        std::shared_ptr<arrow::Table> left_table,
        std::shared_ptr<arrow::Table> right_table,
        const std::string& left_key_column,
        const std::string& right_key_column
    ) : left_table_(left_table),
        right_table_(right_table),
        left_key_column_(left_key_column),
        right_key_column_(right_key_column) {}

    // Execute hash join
    std::shared_ptr<arrow::Table> Execute() {
        // Build phase: build hash table from right table
        BuildHashTable();

        // Probe phase: probe with left table
        auto joined_data = ProbeHashTable();

        // Construct result table
        result_ = ConstructResultTable(joined_data);

        return result_;
    }

    std::shared_ptr<arrow::Table> GetResult() const {
        return result_;
    }

private:
    struct JoinedRow {
        int64_t left_idx;
        int64_t right_idx;
    };

    void BuildHashTable() {
        hash_table_.clear();

        // Get right key column
        auto right_key_array = GetTypedColumn<arrow::Int32Type>(right_table_, right_key_column_);
        int64_t num_rows = right_key_array->length();

        // Build hash table
        for (int64_t i = 0; i < num_rows; ++i) {
            if (!right_key_array->IsNull(i)) {
                int32_t key = right_key_array->Value(i);
                hash_table_[key].push_back(i);
            }
        }
    }

    std::vector<JoinedRow> ProbeHashTable() {
        std::vector<JoinedRow> joined_rows;

        // Get left key column
        auto left_key_array = GetTypedColumn<arrow::Int32Type>(left_table_, left_key_column_);
        int64_t num_rows = left_key_array->length();

        // Probe hash table
        for (int64_t i = 0; i < num_rows; ++i) {
            if (!left_key_array->IsNull(i)) {
                int32_t key = left_key_array->Value(i);

                auto it = hash_table_.find(key);
                if (it != hash_table_.end()) {
                    // Found matching rows in right table
                    for (int64_t right_idx : it->second) {
                        joined_rows.push_back({i, right_idx});
                    }
                }
            }
        }

        return joined_rows;
    }

    std::shared_ptr<arrow::Table> ConstructResultTable(const std::vector<JoinedRow>& joined_rows) {
        if (joined_rows.empty()) {
            // Return empty table with combined schema
            auto combined_schema = CombineSchemas(left_table_->schema(), right_table_->schema());
            arrow::ArrayVector empty_arrays;
            for (int i = 0; i < combined_schema->num_fields(); ++i) {
                auto array_result = arrow::MakeArrayOfNull(combined_schema->field(i)->type(), 0);
                if (!array_result.ok()) {
                    throw std::runtime_error("Failed to create empty array");
                }
                empty_arrays.push_back(array_result.ValueOrDie());
            }
            return arrow::Table::Make(combined_schema, empty_arrays);
        }

        // Build index arrays for Take operation
        arrow::Int64Builder left_indices_builder;
        arrow::Int64Builder right_indices_builder;

        for (const auto& row : joined_rows) {
            auto status = left_indices_builder.Append(row.left_idx);
            if (!status.ok()) {
                throw std::runtime_error("Failed to append left index");
            }
            status = right_indices_builder.Append(row.right_idx);
            if (!status.ok()) {
                throw std::runtime_error("Failed to append right index");
            }
        }

        std::shared_ptr<arrow::Array> left_indices;
        auto status = left_indices_builder.Finish(&left_indices);
        if (!status.ok()) {
            throw std::runtime_error("Failed to build left indices");
        }

        std::shared_ptr<arrow::Array> right_indices;
        status = right_indices_builder.Finish(&right_indices);
        if (!status.ok()) {
            throw std::runtime_error("Failed to build right indices");
        }

        // Take rows from left and right tables
        auto left_taken = TakeTable(left_table_, left_indices);
        auto right_taken = TakeTable(right_table_, right_indices);

        // Combine columns from both tables
        arrow::FieldVector combined_fields;
        arrow::ArrayVector combined_arrays;

        // Add left table columns
        for (int i = 0; i < left_taken->num_columns(); ++i) {
            combined_fields.push_back(left_taken->schema()->field(i));
            combined_arrays.push_back(left_taken->column(i)->chunk(0));
        }

        // Add right table columns
        for (int i = 0; i < right_taken->num_columns(); ++i) {
            combined_fields.push_back(right_taken->schema()->field(i));
            combined_arrays.push_back(right_taken->column(i)->chunk(0));
        }

        auto combined_schema = arrow::schema(combined_fields);
        return arrow::Table::Make(combined_schema, combined_arrays);
    }

    std::shared_ptr<arrow::Table> TakeTable(
        const std::shared_ptr<arrow::Table>& table,
        const std::shared_ptr<arrow::Array>& indices
    ) {
        arrow::compute::ExecContext ctx;
        arrow::ArrayVector taken_arrays;

        for (int i = 0; i < table->num_columns(); ++i) {
            auto column = table->column(i);
            auto combined_result = arrow::Concatenate(column->chunks());
            if (!combined_result.ok()) {
                throw std::runtime_error("Failed to concatenate chunks");
            }

            auto take_result = arrow::compute::CallFunction(
                "take",
                {combined_result.ValueOrDie(), indices},
                &ctx
            );

            if (!take_result.ok()) {
                throw std::runtime_error("Failed to take values: " + take_result.status().ToString());
            }

            taken_arrays.push_back(take_result.ValueOrDie().make_array());
        }

        return arrow::Table::Make(table->schema(), taken_arrays);
    }

    std::shared_ptr<arrow::Schema> CombineSchemas(
        const std::shared_ptr<arrow::Schema>& left_schema,
        const std::shared_ptr<arrow::Schema>& right_schema
    ) {
        arrow::FieldVector fields;
        for (int i = 0; i < left_schema->num_fields(); ++i) {
            fields.push_back(left_schema->field(i));
        }
        for (int i = 0; i < right_schema->num_fields(); ++i) {
            fields.push_back(right_schema->field(i));
        }
        return arrow::schema(fields);
    }
};

// Multi-way hash join (for joining 3+ tables)
class MultiHashJoin {
private:
    std::vector<std::shared_ptr<arrow::Table>> tables_;
    std::vector<std::string> key_columns_;
    std::shared_ptr<arrow::Table> result_;

public:
    MultiHashJoin() {}

    // Add table to join with its key column
    MultiHashJoin& AddTable(
        std::shared_ptr<arrow::Table> table,
        const std::string& key_column
    ) {
        tables_.push_back(table);
        key_columns_.push_back(key_column);
        return *this;
    }

    // Execute multi-way join (left-deep join tree)
    std::shared_ptr<arrow::Table> Execute() {
        if (tables_.size() < 2) {
            throw std::runtime_error("Need at least 2 tables to join");
        }

        // Start with first two tables
        HashJoinInt32 first_join(
            tables_[0], tables_[1],
            key_columns_[0], key_columns_[1]
        );
        result_ = first_join.Execute();

        // Join remaining tables
        for (size_t i = 2; i < tables_.size(); ++i) {
            HashJoinInt32 join(
                result_, tables_[i],
                key_columns_[0],  // Use first key column from accumulated result
                key_columns_[i]
            );
            result_ = join.Execute();
        }

        return result_;
    }

    std::shared_ptr<arrow::Table> GetResult() const {
        return result_;
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_JOIN_H
