#ifndef OPERATORS_HASH_JOIN_H
#define OPERATORS_HASH_JOIN_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

namespace gendb {
namespace operators {

// ============================================================================
// Hash Join Operator (Inner Join)
// ============================================================================

class HashJoin {
private:
    // Hash table: key -> list of row indices in build table
    std::unordered_map<int32_t, std::vector<int64_t>> hash_table_;

public:
    HashJoin() = default;

    // Build phase: create hash table from build-side table
    // Assumes join key is int32 (most common in TPC-H)
    void Build(std::shared_ptr<arrow::Table> build_table, const std::string& join_key) {
        hash_table_.clear();

        auto key_array = GetTypedColumn<arrow::Int32Type>(build_table, join_key);

        for (int64_t i = 0; i < key_array->length(); ++i) {
            if (!key_array->IsNull(i)) {
                int32_t key = key_array->Value(i);
                hash_table_[key].push_back(i);
            }
        }
    }

    // Probe phase: join probe table with build table
    // Returns concatenated table with columns from both sides
    std::shared_ptr<arrow::Table> Probe(
        std::shared_ptr<arrow::Table> probe_table,
        const std::string& probe_join_key,
        std::shared_ptr<arrow::Table> build_table,
        const std::string& build_join_key) {

        auto probe_key_array = GetTypedColumn<arrow::Int32Type>(probe_table, probe_join_key);

        // Collect matching row pairs
        std::vector<int64_t> probe_indices;
        std::vector<int64_t> build_indices;

        for (int64_t i = 0; i < probe_key_array->length(); ++i) {
            if (!probe_key_array->IsNull(i)) {
                int32_t key = probe_key_array->Value(i);
                auto it = hash_table_.find(key);
                if (it != hash_table_.end()) {
                    // Found matching rows in build table
                    for (int64_t build_idx : it->second) {
                        probe_indices.push_back(i);
                        build_indices.push_back(build_idx);
                    }
                }
            }
        }

        // Build output table by taking rows from both tables
        return CombineTables(probe_table, build_table, probe_indices, build_indices);
    }

    // Full join operation: build + probe in one call
    static std::shared_ptr<arrow::Table> Execute(
        std::shared_ptr<arrow::Table> left_table,
        const std::string& left_key,
        std::shared_ptr<arrow::Table> right_table,
        const std::string& right_key) {

        HashJoin joiner;

        // Build on smaller table (heuristic: use right as build side)
        // In practice, optimizer should choose based on cardinality
        joiner.Build(right_table, right_key);
        return joiner.Probe(left_table, left_key, right_table, right_key);
    }

private:
    // Combine tables by taking specific rows from each
    std::shared_ptr<arrow::Table> CombineTables(
        std::shared_ptr<arrow::Table> left_table,
        std::shared_ptr<arrow::Table> right_table,
        const std::vector<int64_t>& left_indices,
        const std::vector<int64_t>& right_indices) {

        if (left_indices.empty()) {
            // Empty result - create table with combined schema but no rows
            auto combined_schema = MergeSchemasDistinct(left_table->schema(), right_table->schema());
            return arrow::Table::Make(combined_schema, {});
        }

        // Take rows from left table
        auto left_indices_array = CreateInt64Array(left_indices);
        auto left_result = arrow::compute::Take(left_table, left_indices_array);
        if (!left_result.ok()) {
            throw std::runtime_error("Failed to take left rows: " + left_result.status().ToString());
        }
        auto left_taken = arrow::compute::GetRecordBatchAsTable(left_result.ValueOrDie());

        // Take rows from right table
        auto right_indices_array = CreateInt64Array(right_indices);
        auto right_result = arrow::compute::Take(right_table, right_indices_array);
        if (!right_result.ok()) {
            throw std::runtime_error("Failed to take right rows: " + right_result.status().ToString());
        }
        auto right_taken = arrow::compute::GetRecordBatchAsTable(right_result.ValueOrDie());

        // Concatenate tables horizontally (avoid duplicate columns)
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

        // Add all columns from left table
        for (int i = 0; i < left_taken->num_columns(); ++i) {
            fields.push_back(left_taken->schema()->field(i));
            columns.push_back(left_taken->column(i));
        }

        // Add columns from right table that don't exist in left
        for (int i = 0; i < right_taken->num_columns(); ++i) {
            auto field = right_taken->schema()->field(i);
            if (left_taken->schema()->GetFieldIndex(field->name()) == -1) {
                fields.push_back(field);
                columns.push_back(right_taken->column(i));
            }
        }

        auto combined_schema = arrow::schema(fields);
        return arrow::Table::Make(combined_schema, columns);
    }

    // Helper: create Int64Array from vector
    std::shared_ptr<arrow::Int64Array> CreateInt64Array(const std::vector<int64_t>& values) {
        arrow::Int64Builder builder;
        auto status = builder.Reserve(values.size());
        if (!status.ok()) {
            throw std::runtime_error("Failed to reserve builder");
        }
        status = builder.AppendValues(values);
        if (!status.ok()) {
            throw std::runtime_error("Failed to append values");
        }
        auto result = builder.Finish();
        if (!result.ok()) {
            throw std::runtime_error("Failed to finish builder");
        }
        return result.ValueOrDie();
    }

    // Helper: merge schemas without duplicates
    std::shared_ptr<arrow::Schema> MergeSchemasDistinct(
        std::shared_ptr<arrow::Schema> left,
        std::shared_ptr<arrow::Schema> right) {

        std::vector<std::shared_ptr<arrow::Field>> fields;

        // Add all fields from left
        for (int i = 0; i < left->num_fields(); ++i) {
            fields.push_back(left->field(i));
        }

        // Add fields from right that don't exist in left
        for (int i = 0; i < right->num_fields(); ++i) {
            auto field = right->field(i);
            if (left->GetFieldIndex(field->name()) == -1) {
                fields.push_back(field);
            }
        }

        return arrow::schema(fields);
    }
};

// ============================================================================
// Multi-Column Hash Join (for composite keys)
// ============================================================================

class MultiKeyHashJoin {
private:
    // Hash table: composite key -> list of row indices
    struct CompositeKey {
        std::vector<int32_t> values;

        bool operator==(const CompositeKey& other) const {
            return values == other.values;
        }
    };

    struct CompositeKeyHash {
        std::size_t operator()(const CompositeKey& key) const {
            std::size_t hash = 0;
            for (int32_t val : key.values) {
                hash ^= std::hash<int32_t>{}(val) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };

    std::unordered_map<CompositeKey, std::vector<int64_t>, CompositeKeyHash> hash_table_;

public:
    // Build with multiple join keys
    void Build(std::shared_ptr<arrow::Table> build_table, const std::vector<std::string>& join_keys) {
        hash_table_.clear();

        std::vector<std::shared_ptr<arrow::Int32Array>> key_arrays;
        for (const auto& key_name : join_keys) {
            key_arrays.push_back(GetTypedColumn<arrow::Int32Type>(build_table, key_name));
        }

        int64_t num_rows = build_table->num_rows();
        for (int64_t i = 0; i < num_rows; ++i) {
            CompositeKey key;
            bool has_null = false;
            for (auto& key_array : key_arrays) {
                if (key_array->IsNull(i)) {
                    has_null = true;
                    break;
                }
                key.values.push_back(key_array->Value(i));
            }
            if (!has_null) {
                hash_table_[key].push_back(i);
            }
        }
    }

    // Probe with multiple join keys
    std::shared_ptr<arrow::Table> Probe(
        std::shared_ptr<arrow::Table> probe_table,
        const std::vector<std::string>& probe_join_keys,
        std::shared_ptr<arrow::Table> build_table,
        const std::vector<std::string>& build_join_keys) {

        std::vector<std::shared_ptr<arrow::Int32Array>> probe_key_arrays;
        for (const auto& key_name : probe_join_keys) {
            probe_key_arrays.push_back(GetTypedColumn<arrow::Int32Type>(probe_table, key_name));
        }

        std::vector<int64_t> probe_indices;
        std::vector<int64_t> build_indices;

        int64_t num_rows = probe_table->num_rows();
        for (int64_t i = 0; i < num_rows; ++i) {
            CompositeKey key;
            bool has_null = false;
            for (auto& key_array : probe_key_arrays) {
                if (key_array->IsNull(i)) {
                    has_null = true;
                    break;
                }
                key.values.push_back(key_array->Value(i));
            }
            if (!has_null) {
                auto it = hash_table_.find(key);
                if (it != hash_table_.end()) {
                    for (int64_t build_idx : it->second) {
                        probe_indices.push_back(i);
                        build_indices.push_back(build_idx);
                    }
                }
            }
        }

        // Use same CombineTables logic as single-key join
        HashJoin single_key_joiner;
        return single_key_joiner.CombineTables(probe_table, build_table, probe_indices, build_indices);
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_JOIN_H
