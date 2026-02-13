#ifndef OPERATORS_HASH_JOIN_H
#define OPERATORS_HASH_JOIN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>

namespace gendb {
namespace operators {

// Hash join on int32 keys (most common for TPC-H foreign keys)
// Build hash table from left (smaller) table, probe with right (larger) table
class HashJoinInt32 {
private:
    std::shared_ptr<arrow::Table> left_table_;
    std::shared_ptr<arrow::Table> right_table_;
    std::string left_key_column_;
    std::string right_key_column_;
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

    std::shared_ptr<arrow::Table> Execute() {
        BuildHashTable();
        auto joined_rows = ProbeHashTable();
        return ConstructResultTable(joined_rows);
    }

private:
    struct JoinedRow {
        int64_t left_idx;
        int64_t right_idx;
    };

    void BuildHashTable() {
        hash_table_.clear();
        auto key_array = GetTypedColumn<arrow::Int32Type>(left_table_, left_key_column_);
        int64_t n = key_array->length();
        for (int64_t i = 0; i < n; ++i) {
            if (!key_array->IsNull(i)) {
                hash_table_[key_array->Value(i)].push_back(i);
            }
        }
    }

    std::vector<JoinedRow> ProbeHashTable() {
        std::vector<JoinedRow> joined_rows;
        auto key_array = GetTypedColumn<arrow::Int32Type>(right_table_, right_key_column_);
        int64_t n = key_array->length();
        for (int64_t i = 0; i < n; ++i) {
            if (!key_array->IsNull(i)) {
                auto it = hash_table_.find(key_array->Value(i));
                if (it != hash_table_.end()) {
                    for (int64_t left_idx : it->second) {
                        joined_rows.push_back({left_idx, i});
                    }
                }
            }
        }
        return joined_rows;
    }

    std::shared_ptr<arrow::Table> ConstructResultTable(const std::vector<JoinedRow>& joined_rows) {
        if (joined_rows.empty()) {
            auto schema = CombineSchemas(left_table_->schema(), right_table_->schema());
            arrow::ArrayVector empty;
            for (int i = 0; i < schema->num_fields(); ++i) {
                empty.push_back(arrow::MakeArrayOfNull(schema->field(i)->type(), 0).ValueOrDie());
            }
            return arrow::Table::Make(schema, empty);
        }

        // Build index arrays
        arrow::Int64Builder left_idx_builder, right_idx_builder;
        for (const auto& row : joined_rows) {
            PARQUET_THROW_NOT_OK(left_idx_builder.Append(row.left_idx));
            PARQUET_THROW_NOT_OK(right_idx_builder.Append(row.right_idx));
        }

        std::shared_ptr<arrow::Array> left_indices, right_indices;
        PARQUET_THROW_NOT_OK(left_idx_builder.Finish(&left_indices));
        PARQUET_THROW_NOT_OK(right_idx_builder.Finish(&right_indices));

        auto left_taken = TakeTable(left_table_, left_indices);
        auto right_taken = TakeTable(right_table_, right_indices);

        // Combine columns
        arrow::FieldVector fields;
        arrow::ArrayVector arrays;
        for (int i = 0; i < left_taken->num_columns(); ++i) {
            fields.push_back(left_taken->schema()->field(i));
            arrays.push_back(left_taken->column(i)->chunk(0));
        }
        for (int i = 0; i < right_taken->num_columns(); ++i) {
            fields.push_back(right_taken->schema()->field(i));
            arrays.push_back(right_taken->column(i)->chunk(0));
        }
        return arrow::Table::Make(arrow::schema(fields), arrays);
    }

    std::shared_ptr<arrow::Table> TakeTable(
        const std::shared_ptr<arrow::Table>& table,
        const std::shared_ptr<arrow::Array>& indices
    ) {
        arrow::ArrayVector taken;
        for (int i = 0; i < table->num_columns(); ++i) {
            auto col = arrow::Concatenate(table->column(i)->chunks()).ValueOrDie();
            auto result = arrow::compute::CallFunction("take", {arrow::Datum(col), arrow::Datum(indices)});
            taken.push_back(result.ValueOrDie().make_array());
        }
        return arrow::Table::Make(table->schema(), taken);
    }

    static std::shared_ptr<arrow::Schema> CombineSchemas(
        const std::shared_ptr<arrow::Schema>& left,
        const std::shared_ptr<arrow::Schema>& right
    ) {
        arrow::FieldVector fields;
        for (int i = 0; i < left->num_fields(); ++i) fields.push_back(left->field(i));
        for (int i = 0; i < right->num_fields(); ++i) fields.push_back(right->field(i));
        return arrow::schema(fields);
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_JOIN_H
