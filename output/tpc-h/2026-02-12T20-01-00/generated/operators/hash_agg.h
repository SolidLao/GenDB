#ifndef OPERATORS_HASH_AGG_H
#define OPERATORS_HASH_AGG_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <cstring>
#include <cmath>
#include <limits>

namespace gendb {
namespace operators {

// Aggregate function types
enum class AggFunc {
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX
};

// Hash aggregation with support for multiple group-by columns and multiple aggregates
class HashAgg {
private:
    std::shared_ptr<arrow::Table> input_table_;
    std::vector<std::string> group_by_columns_;
    std::vector<std::string> agg_columns_;
    std::vector<AggFunc> agg_funcs_;
    std::vector<std::string> agg_output_names_;
    std::shared_ptr<arrow::Table> result_;

    // Hash key for group-by (composite key)
    struct GroupKey {
        std::vector<std::string> values;

        bool operator==(const GroupKey& other) const {
            return values == other.values;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& key) const {
            size_t hash = 0;
            for (const auto& val : key.values) {
                hash ^= std::hash<std::string>{}(val) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };

    // Aggregate state
    struct AggState {
        std::vector<double> sum_values;
        std::vector<int64_t> count_values;
        std::vector<double> min_values;
        std::vector<double> max_values;

        AggState(size_t num_aggs) {
            sum_values.resize(num_aggs, 0.0);
            count_values.resize(num_aggs, 0);
            min_values.resize(num_aggs, std::numeric_limits<double>::max());
            max_values.resize(num_aggs, std::numeric_limits<double>::lowest());
        }
    };

    std::unordered_map<GroupKey, AggState, GroupKeyHash> agg_map_;

public:
    HashAgg(std::shared_ptr<arrow::Table> input_table)
        : input_table_(input_table) {}

    // Set group-by columns
    HashAgg& GroupBy(const std::vector<std::string>& columns) {
        group_by_columns_ = columns;
        return *this;
    }

    // Add aggregate: SUM, COUNT, AVG, MIN, MAX
    HashAgg& Aggregate(
        AggFunc func,
        const std::string& column,
        const std::string& output_name
    ) {
        agg_funcs_.push_back(func);
        agg_columns_.push_back(column);
        agg_output_names_.push_back(output_name);
        return *this;
    }

    // Execute aggregation
    std::shared_ptr<arrow::Table> Execute() {
        // Build aggregation map
        BuildAggregationMap();

        // Construct result table
        result_ = ConstructResultTable();

        return result_;
    }

    std::shared_ptr<arrow::Table> GetResult() const {
        return result_;
    }

private:
    void BuildAggregationMap() {
        agg_map_.clear();

        int64_t num_rows = input_table_->num_rows();
        if (num_rows == 0) return;

        // Get group-by column arrays
        std::vector<std::shared_ptr<arrow::Array>> group_by_arrays;
        for (const auto& col : group_by_columns_) {
            auto chunked = input_table_->GetColumnByName(col);
            if (!chunked) {
                throw std::runtime_error("Group-by column not found: " + col);
            }
            auto combined = arrow::Concatenate(chunked->chunks());
            if (!combined.ok()) {
                throw std::runtime_error("Failed to concatenate chunks");
            }
            group_by_arrays.push_back(combined.ValueOrDie());
        }

        // Get aggregate column arrays (as doubles for numeric operations)
        std::vector<std::shared_ptr<arrow::Array>> agg_arrays;
        for (const auto& col : agg_columns_) {
            auto chunked = input_table_->GetColumnByName(col);
            if (!chunked) {
                throw std::runtime_error("Aggregate column not found: " + col);
            }
            auto combined = arrow::Concatenate(chunked->chunks());
            if (!combined.ok()) {
                throw std::runtime_error("Failed to concatenate chunks");
            }
            agg_arrays.push_back(combined.ValueOrDie());
        }

        // Process each row
        for (int64_t row = 0; row < num_rows; ++row) {
            // Build group key
            GroupKey key;
            for (const auto& group_array : group_by_arrays) {
                key.values.push_back(ArrayValueToString(group_array, row));
            }

            // Get or create aggregate state
            auto it = agg_map_.find(key);
            if (it == agg_map_.end()) {
                it = agg_map_.emplace(key, AggState(agg_funcs_.size())).first;
            }

            // Update aggregates
            for (size_t agg_idx = 0; agg_idx < agg_funcs_.size(); ++agg_idx) {
                double value = ArrayValueToDouble(agg_arrays[agg_idx], row);

                if (!std::isnan(value)) {
                    auto& state = it->second;
                    state.sum_values[agg_idx] += value;
                    state.count_values[agg_idx]++;
                    state.min_values[agg_idx] = std::min(state.min_values[agg_idx], value);
                    state.max_values[agg_idx] = std::max(state.max_values[agg_idx], value);
                }
            }
        }
    }

    std::shared_ptr<arrow::Table> ConstructResultTable() {
        // Build result arrays
        std::vector<arrow::StringBuilder> group_by_builders(group_by_columns_.size());
        std::vector<arrow::DoubleBuilder> agg_builders(agg_funcs_.size());

        for (const auto& entry : agg_map_) {
            // Add group-by values
            for (size_t i = 0; i < group_by_columns_.size(); ++i) {
                auto status = group_by_builders[i].Append(entry.first.values[i]);
                if (!status.ok()) {
                    throw std::runtime_error("Failed to append group value");
                }
            }

            // Add aggregate values
            for (size_t i = 0; i < agg_funcs_.size(); ++i) {
                double value;
                switch (agg_funcs_[i]) {
                    case AggFunc::SUM:
                        value = entry.second.sum_values[i];
                        break;
                    case AggFunc::COUNT:
                        value = static_cast<double>(entry.second.count_values[i]);
                        break;
                    case AggFunc::AVG:
                        value = entry.second.count_values[i] > 0
                            ? entry.second.sum_values[i] / entry.second.count_values[i]
                            : 0.0;
                        break;
                    case AggFunc::MIN:
                        value = entry.second.min_values[i];
                        break;
                    case AggFunc::MAX:
                        value = entry.second.max_values[i];
                        break;
                }

                auto status = agg_builders[i].Append(value);
                if (!status.ok()) {
                    throw std::runtime_error("Failed to append aggregate value");
                }
            }
        }

        // Finish arrays
        arrow::ArrayVector arrays;
        arrow::FieldVector fields;

        for (size_t i = 0; i < group_by_columns_.size(); ++i) {
            std::shared_ptr<arrow::Array> array;
            auto status = group_by_builders[i].Finish(&array);
            if (!status.ok()) {
                throw std::runtime_error("Failed to finish group-by array");
            }
            arrays.push_back(array);
            fields.push_back(arrow::field(group_by_columns_[i], arrow::utf8()));
        }

        for (size_t i = 0; i < agg_funcs_.size(); ++i) {
            std::shared_ptr<arrow::Array> array;
            auto status = agg_builders[i].Finish(&array);
            if (!status.ok()) {
                throw std::runtime_error("Failed to finish aggregate array");
            }
            arrays.push_back(array);
            fields.push_back(arrow::field(agg_output_names_[i], arrow::float64()));
        }

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, arrays);
    }

    // Convert Arrow array value to string (for group-by key)
    std::string ArrayValueToString(const std::shared_ptr<arrow::Array>& array, int64_t index) {
        if (array->IsNull(index)) {
            return "NULL";
        }

        switch (array->type()->id()) {
            case arrow::Type::INT32: {
                auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
                return std::to_string(typed->Value(index));
            }
            case arrow::Type::INT64: {
                auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
                return std::to_string(typed->Value(index));
            }
            case arrow::Type::DOUBLE: {
                auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
                return std::to_string(typed->Value(index));
            }
            case arrow::Type::STRING: {
                auto typed = std::static_pointer_cast<arrow::StringArray>(array);
                return typed->GetString(index);
            }
            case arrow::Type::DATE32: {
                auto typed = std::static_pointer_cast<arrow::Date32Array>(array);
                return std::to_string(typed->Value(index));
            }
            default:
                throw std::runtime_error("Unsupported group-by type");
        }
    }

    // Convert Arrow array value to double (for aggregation)
    double ArrayValueToDouble(const std::shared_ptr<arrow::Array>& array, int64_t index) {
        if (array->IsNull(index)) {
            return std::numeric_limits<double>::quiet_NaN();
        }

        switch (array->type()->id()) {
            case arrow::Type::INT32: {
                auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
                return static_cast<double>(typed->Value(index));
            }
            case arrow::Type::INT64: {
                auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
                return static_cast<double>(typed->Value(index));
            }
            case arrow::Type::DOUBLE: {
                auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
                return typed->Value(index);
            }
            case arrow::Type::FLOAT: {
                auto typed = std::static_pointer_cast<arrow::FloatArray>(array);
                return static_cast<double>(typed->Value(index));
            }
            case arrow::Type::DECIMAL128: {
                // Convert decimal128 to double
                auto typed = std::static_pointer_cast<arrow::Decimal128Array>(array);
                auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(array->type());
                int32_t scale = decimal_type->scale();
                auto decimal_value = typed->GetView(index);
                // FromBigEndian creates a Decimal128 from bytes
                auto dec128_result = arrow::Decimal128::FromBigEndian(
                    reinterpret_cast<const uint8_t*>(decimal_value.data()),
                    static_cast<int32_t>(decimal_value.size())
                );
                if (!dec128_result.ok()) {
                    throw std::runtime_error("Failed to convert decimal128");
                }
                arrow::Decimal128 dec128 = dec128_result.ValueOrDie();
                std::string str = dec128.ToString(scale);
                return std::stod(str);
            }
            default:
                throw std::runtime_error("Unsupported aggregate type: " + array->type()->ToString());
        }
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_AGG_H
