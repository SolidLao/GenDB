#ifndef OPERATORS_HASH_AGG_H
#define OPERATORS_HASH_AGG_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include <cmath>

namespace gendb {
namespace operators {

// ============================================================================
// Aggregate Functions
// ============================================================================

enum class AggregateOp {
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX
};

struct AggregateSpec {
    std::string column_name;
    AggregateOp op;
    std::string output_name;
};

// ============================================================================
// Single-Key Hash Aggregation
// ============================================================================

class HashAggregate {
private:
    // Aggregation state for one group
    struct AggState {
        double sum = 0.0;
        int64_t count = 0;
        double min_val = std::numeric_limits<double>::max();
        double max_val = std::numeric_limits<double>::lowest();
    };

    // Hash table: group key -> aggregation state for each aggregate
    std::unordered_map<int32_t, std::vector<AggState>> hash_table_;
    std::vector<AggregateSpec> agg_specs_;

public:
    HashAggregate() = default;

    // Execute aggregation with single int32 group key
    std::shared_ptr<arrow::Table> Execute(
        std::shared_ptr<arrow::Table> input_table,
        const std::string& group_key,
        const std::vector<AggregateSpec>& aggregates) {

        agg_specs_ = aggregates;
        hash_table_.clear();

        auto key_array = GetTypedColumn<arrow::Int32Type>(input_table, group_key);

        // Process each aggregate column
        std::vector<std::shared_ptr<arrow::DoubleArray>> agg_arrays;
        for (const auto& spec : aggregates) {
            if (spec.op == AggregateOp::COUNT) {
                agg_arrays.push_back(nullptr); // COUNT doesn't need data array
            } else {
                agg_arrays.push_back(GetTypedColumn<arrow::DoubleType>(input_table, spec.column_name));
            }
        }

        // Build hash table and accumulate aggregates
        int64_t num_rows = input_table->num_rows();
        for (int64_t i = 0; i < num_rows; ++i) {
            if (key_array->IsNull(i)) continue;

            int32_t key = key_array->Value(i);

            // Initialize group if new
            if (hash_table_.find(key) == hash_table_.end()) {
                hash_table_[key].resize(aggregates.size());
            }

            auto& states = hash_table_[key];

            // Update each aggregate
            for (size_t agg_idx = 0; agg_idx < aggregates.size(); ++agg_idx) {
                auto& state = states[agg_idx];
                const auto& spec = aggregates[agg_idx];

                if (spec.op == AggregateOp::COUNT) {
                    state.count++;
                } else {
                    auto agg_array = agg_arrays[agg_idx];
                    if (!agg_array->IsNull(i)) {
                        double value = agg_array->Value(i);
                        state.sum += value;
                        state.count++;
                        state.min_val = std::min(state.min_val, value);
                        state.max_val = std::max(state.max_val, value);
                    }
                }
            }
        }

        // Build output table
        return BuildOutputTable(group_key);
    }

private:
    std::shared_ptr<arrow::Table> BuildOutputTable(const std::string& group_key_name) {
        // Build output arrays
        arrow::Int32Builder key_builder;
        std::vector<arrow::DoubleBuilder> agg_builders(agg_specs_.size());

        size_t num_groups = hash_table_.size();
        key_builder.Reserve(num_groups);
        for (auto& builder : agg_builders) {
            builder.Reserve(num_groups);
        }

        // Populate arrays
        for (const auto& [key, states] : hash_table_) {
            key_builder.UnsafeAppend(key);

            for (size_t i = 0; i < agg_specs_.size(); ++i) {
                const auto& spec = agg_specs_[i];
                const auto& state = states[i];

                double output_value = 0.0;
                switch (spec.op) {
                    case AggregateOp::SUM:
                        output_value = state.sum;
                        break;
                    case AggregateOp::COUNT:
                        output_value = static_cast<double>(state.count);
                        break;
                    case AggregateOp::AVG:
                        output_value = state.count > 0 ? state.sum / state.count : 0.0;
                        break;
                    case AggregateOp::MIN:
                        output_value = state.min_val;
                        break;
                    case AggregateOp::MAX:
                        output_value = state.max_val;
                        break;
                }
                agg_builders[i].UnsafeAppend(output_value);
            }
        }

        // Finish arrays
        auto key_result = key_builder.Finish();
        if (!key_result.ok()) {
            throw std::runtime_error("Failed to build key array");
        }
        auto key_array = key_result.ValueOrDie();

        std::vector<std::shared_ptr<arrow::Array>> agg_arrays;
        for (auto& builder : agg_builders) {
            auto result = builder.Finish();
            if (!result.ok()) {
                throw std::runtime_error("Failed to build aggregate array");
            }
            agg_arrays.push_back(result.ValueOrDie());
        }

        // Build schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.push_back(arrow::field(group_key_name, arrow::int32()));
        for (const auto& spec : agg_specs_) {
            fields.push_back(arrow::field(spec.output_name, arrow::float64()));
        }

        // Build table
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.push_back(key_array);
        for (auto& agg_array : agg_arrays) {
            columns.push_back(agg_array);
        }

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, columns);
    }
};

// ============================================================================
// Multi-Key Hash Aggregation
// ============================================================================

class MultiKeyHashAggregate {
private:
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

    struct AggState {
        double sum = 0.0;
        int64_t count = 0;
        double min_val = std::numeric_limits<double>::max();
        double max_val = std::numeric_limits<double>::lowest();
    };

    std::unordered_map<CompositeKey, std::vector<AggState>, CompositeKeyHash> hash_table_;
    std::vector<AggregateSpec> agg_specs_;
    std::vector<std::string> group_keys_;

public:
    // Execute aggregation with multiple group keys (all int32)
    std::shared_ptr<arrow::Table> Execute(
        std::shared_ptr<arrow::Table> input_table,
        const std::vector<std::string>& group_keys,
        const std::vector<AggregateSpec>& aggregates) {

        agg_specs_ = aggregates;
        group_keys_ = group_keys;
        hash_table_.clear();

        // Get group key arrays
        std::vector<std::shared_ptr<arrow::Int32Array>> key_arrays;
        for (const auto& key_name : group_keys) {
            key_arrays.push_back(GetTypedColumn<arrow::Int32Type>(input_table, key_name));
        }

        // Get aggregate value arrays
        std::vector<std::shared_ptr<arrow::DoubleArray>> agg_arrays;
        for (const auto& spec : aggregates) {
            if (spec.op == AggregateOp::COUNT) {
                agg_arrays.push_back(nullptr);
            } else {
                agg_arrays.push_back(GetTypedColumn<arrow::DoubleType>(input_table, spec.column_name));
            }
        }

        // Build hash table
        int64_t num_rows = input_table->num_rows();
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

            if (has_null) continue;

            // Initialize group if new
            if (hash_table_.find(key) == hash_table_.end()) {
                hash_table_[key].resize(aggregates.size());
            }

            auto& states = hash_table_[key];

            // Update each aggregate
            for (size_t agg_idx = 0; agg_idx < aggregates.size(); ++agg_idx) {
                auto& state = states[agg_idx];
                const auto& spec = aggregates[agg_idx];

                if (spec.op == AggregateOp::COUNT) {
                    state.count++;
                } else {
                    auto agg_array = agg_arrays[agg_idx];
                    if (!agg_array->IsNull(i)) {
                        double value = agg_array->Value(i);
                        state.sum += value;
                        state.count++;
                        state.min_val = std::min(state.min_val, value);
                        state.max_val = std::max(state.max_val, value);
                    }
                }
            }
        }

        return BuildOutputTable();
    }

private:
    std::shared_ptr<arrow::Table> BuildOutputTable() {
        // Build output arrays
        std::vector<arrow::Int32Builder> key_builders(group_keys_.size());
        std::vector<arrow::DoubleBuilder> agg_builders(agg_specs_.size());

        size_t num_groups = hash_table_.size();
        for (auto& builder : key_builders) {
            builder.Reserve(num_groups);
        }
        for (auto& builder : agg_builders) {
            builder.Reserve(num_groups);
        }

        // Populate arrays
        for (const auto& [key, states] : hash_table_) {
            // Append group keys
            for (size_t i = 0; i < key.values.size(); ++i) {
                key_builders[i].UnsafeAppend(key.values[i]);
            }

            // Append aggregates
            for (size_t i = 0; i < agg_specs_.size(); ++i) {
                const auto& spec = agg_specs_[i];
                const auto& state = states[i];

                double output_value = 0.0;
                switch (spec.op) {
                    case AggregateOp::SUM:
                        output_value = state.sum;
                        break;
                    case AggregateOp::COUNT:
                        output_value = static_cast<double>(state.count);
                        break;
                    case AggregateOp::AVG:
                        output_value = state.count > 0 ? state.sum / state.count : 0.0;
                        break;
                    case AggregateOp::MIN:
                        output_value = state.min_val;
                        break;
                    case AggregateOp::MAX:
                        output_value = state.max_val;
                        break;
                }
                agg_builders[i].UnsafeAppend(output_value);
            }
        }

        // Finish arrays
        std::vector<std::shared_ptr<arrow::Array>> key_arrays;
        for (auto& builder : key_builders) {
            auto result = builder.Finish();
            if (!result.ok()) {
                throw std::runtime_error("Failed to build key array");
            }
            key_arrays.push_back(result.ValueOrDie());
        }

        std::vector<std::shared_ptr<arrow::Array>> agg_arrays;
        for (auto& builder : agg_builders) {
            auto result = builder.Finish();
            if (!result.ok()) {
                throw std::runtime_error("Failed to build aggregate array");
            }
            agg_arrays.push_back(result.ValueOrDie());
        }

        // Build schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < group_keys_.size(); ++i) {
            fields.push_back(arrow::field(group_keys_[i], arrow::int32()));
        }
        for (const auto& spec : agg_specs_) {
            fields.push_back(arrow::field(spec.output_name, arrow::float64()));
        }

        // Build table
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto& key_array : key_arrays) {
            columns.push_back(key_array);
        }
        for (auto& agg_array : agg_arrays) {
            columns.push_back(agg_array);
        }

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, columns);
    }
};

// ============================================================================
// String Group Key Hash Aggregation
// ============================================================================

class StringKeyHashAggregate {
private:
    struct AggState {
        double sum = 0.0;
        int64_t count = 0;
        double min_val = std::numeric_limits<double>::max();
        double max_val = std::numeric_limits<double>::lowest();
    };

    std::unordered_map<std::string, std::vector<AggState>> hash_table_;
    std::vector<AggregateSpec> agg_specs_;

public:
    // Execute aggregation with single string group key
    std::shared_ptr<arrow::Table> Execute(
        std::shared_ptr<arrow::Table> input_table,
        const std::string& group_key,
        const std::vector<AggregateSpec>& aggregates) {

        agg_specs_ = aggregates;
        hash_table_.clear();

        auto key_array = GetTypedColumn<arrow::StringType>(input_table, group_key);

        // Get aggregate value arrays
        std::vector<std::shared_ptr<arrow::DoubleArray>> agg_arrays;
        for (const auto& spec : aggregates) {
            if (spec.op == AggregateOp::COUNT) {
                agg_arrays.push_back(nullptr);
            } else {
                agg_arrays.push_back(GetTypedColumn<arrow::DoubleType>(input_table, spec.column_name));
            }
        }

        // Build hash table
        int64_t num_rows = input_table->num_rows();
        for (int64_t i = 0; i < num_rows; ++i) {
            if (key_array->IsNull(i)) continue;

            std::string key = key_array->GetString(i);
            // Trim whitespace for TPC-H CHAR columns
            key.erase(key.find_last_not_of(' ') + 1);

            // Initialize group if new
            if (hash_table_.find(key) == hash_table_.end()) {
                hash_table_[key].resize(aggregates.size());
            }

            auto& states = hash_table_[key];

            // Update each aggregate
            for (size_t agg_idx = 0; agg_idx < aggregates.size(); ++agg_idx) {
                auto& state = states[agg_idx];
                const auto& spec = aggregates[agg_idx];

                if (spec.op == AggregateOp::COUNT) {
                    state.count++;
                } else {
                    auto agg_array = agg_arrays[agg_idx];
                    if (!agg_array->IsNull(i)) {
                        double value = agg_array->Value(i);
                        state.sum += value;
                        state.count++;
                        state.min_val = std::min(state.min_val, value);
                        state.max_val = std::max(state.max_val, value);
                    }
                }
            }
        }

        return BuildOutputTable(group_key);
    }

private:
    std::shared_ptr<arrow::Table> BuildOutputTable(const std::string& group_key_name) {
        arrow::StringBuilder key_builder;
        std::vector<arrow::DoubleBuilder> agg_builders(agg_specs_.size());

        size_t num_groups = hash_table_.size();
        key_builder.Reserve(num_groups);
        for (auto& builder : agg_builders) {
            builder.Reserve(num_groups);
        }

        // Populate arrays
        for (const auto& [key, states] : hash_table_) {
            key_builder.Append(key);

            for (size_t i = 0; i < agg_specs_.size(); ++i) {
                const auto& spec = agg_specs_[i];
                const auto& state = states[i];

                double output_value = 0.0;
                switch (spec.op) {
                    case AggregateOp::SUM:
                        output_value = state.sum;
                        break;
                    case AggregateOp::COUNT:
                        output_value = static_cast<double>(state.count);
                        break;
                    case AggregateOp::AVG:
                        output_value = state.count > 0 ? state.sum / state.count : 0.0;
                        break;
                    case AggregateOp::MIN:
                        output_value = state.min_val;
                        break;
                    case AggregateOp::MAX:
                        output_value = state.max_val;
                        break;
                }
                agg_builders[i].UnsafeAppend(output_value);
            }
        }

        // Finish arrays
        auto key_result = key_builder.Finish();
        if (!key_result.ok()) {
            throw std::runtime_error("Failed to build key array");
        }
        auto key_array = key_result.ValueOrDie();

        std::vector<std::shared_ptr<arrow::Array>> agg_arrays;
        for (auto& builder : agg_builders) {
            auto result = builder.Finish();
            if (!result.ok()) {
                throw std::runtime_error("Failed to build aggregate array");
            }
            agg_arrays.push_back(result.ValueOrDie());
        }

        // Build schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.push_back(arrow::field(group_key_name, arrow::utf8()));
        for (const auto& spec : agg_specs_) {
            fields.push_back(arrow::field(spec.output_name, arrow::float64()));
        }

        // Build table
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.push_back(key_array);
        for (auto& agg_array : agg_arrays) {
            columns.push_back(agg_array);
        }

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, columns);
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_AGG_H
