#ifndef OPERATORS_HASH_AGG_H
#define OPERATORS_HASH_AGG_H

#include "../arrow_helpers.h"
#include <arrow/api.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>
#include <cmath>
#include <limits>

namespace gendb {
namespace operators {

enum class AggFunc { SUM, COUNT, AVG, MIN, MAX };

// Hash aggregation with multiple group-by columns and multiple aggregates
class HashAgg {
private:
    std::shared_ptr<arrow::Table> input_table_;
    std::vector<std::string> group_by_columns_;
    std::vector<AggFunc> agg_funcs_;
    std::vector<std::string> agg_columns_;
    std::vector<std::string> agg_output_names_;

    struct GroupKey {
        std::vector<std::string> values;
        bool operator==(const GroupKey& other) const { return values == other.values; }
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

    struct AggState {
        std::vector<double> sum_values;
        std::vector<int64_t> count_values;
        std::vector<double> min_values;
        std::vector<double> max_values;

        AggState(size_t n) {
            sum_values.resize(n, 0.0);
            count_values.resize(n, 0);
            min_values.resize(n, std::numeric_limits<double>::max());
            max_values.resize(n, std::numeric_limits<double>::lowest());
        }
    };

public:
    explicit HashAgg(std::shared_ptr<arrow::Table> input) : input_table_(input) {}

    HashAgg& GroupBy(const std::vector<std::string>& columns) {
        group_by_columns_ = columns;
        return *this;
    }

    HashAgg& Aggregate(AggFunc func, const std::string& column, const std::string& output_name) {
        agg_funcs_.push_back(func);
        agg_columns_.push_back(column);
        agg_output_names_.push_back(output_name);
        return *this;
    }

    std::shared_ptr<arrow::Table> Execute() {
        std::unordered_map<GroupKey, AggState, GroupKeyHash> agg_map;
        int64_t num_rows = input_table_->num_rows();
        if (num_rows == 0) return BuildEmptyResult();

        // Get group-by arrays
        std::vector<std::shared_ptr<arrow::Array>> group_arrays;
        for (const auto& col : group_by_columns_) {
            auto chunked = input_table_->GetColumnByName(col);
            if (!chunked) throw std::runtime_error("Group-by column not found: " + col);
            group_arrays.push_back(arrow::Concatenate(chunked->chunks()).ValueOrDie());
        }

        // Get aggregate arrays
        std::vector<std::shared_ptr<arrow::Array>> agg_arrays;
        for (const auto& col : agg_columns_) {
            auto chunked = input_table_->GetColumnByName(col);
            if (!chunked) throw std::runtime_error("Aggregate column not found: " + col);
            agg_arrays.push_back(arrow::Concatenate(chunked->chunks()).ValueOrDie());
        }

        // Process rows
        for (int64_t row = 0; row < num_rows; ++row) {
            GroupKey key;
            for (const auto& arr : group_arrays) {
                key.values.push_back(ArrayValueToString(arr, row));
            }

            auto it = agg_map.find(key);
            if (it == agg_map.end()) {
                it = agg_map.emplace(key, AggState(agg_funcs_.size())).first;
            }

            for (size_t i = 0; i < agg_funcs_.size(); ++i) {
                double val = ArrayValueToDouble(agg_arrays[i], row);
                if (!std::isnan(val)) {
                    it->second.sum_values[i] += val;
                    it->second.count_values[i]++;
                    it->second.min_values[i] = std::min(it->second.min_values[i], val);
                    it->second.max_values[i] = std::max(it->second.max_values[i], val);
                }
            }
        }

        return BuildResultTable(agg_map);
    }

private:
    std::shared_ptr<arrow::Table> BuildEmptyResult() {
        arrow::FieldVector fields;
        arrow::ArrayVector arrays;
        for (const auto& col : group_by_columns_) {
            fields.push_back(arrow::field(col, arrow::utf8()));
            arrays.push_back(arrow::MakeArrayOfNull(arrow::utf8(), 0).ValueOrDie());
        }
        for (const auto& name : agg_output_names_) {
            fields.push_back(arrow::field(name, arrow::float64()));
            arrays.push_back(arrow::MakeArrayOfNull(arrow::float64(), 0).ValueOrDie());
        }
        return arrow::Table::Make(arrow::schema(fields), arrays);
    }

    std::shared_ptr<arrow::Table> BuildResultTable(
        const std::unordered_map<GroupKey, AggState, GroupKeyHash>& agg_map
    ) {
        std::vector<arrow::StringBuilder> group_builders(group_by_columns_.size());
        std::vector<arrow::DoubleBuilder> agg_builders(agg_funcs_.size());

        for (const auto& [key, state] : agg_map) {
            for (size_t i = 0; i < group_by_columns_.size(); ++i) {
                PARQUET_THROW_NOT_OK(group_builders[i].Append(key.values[i]));
            }
            for (size_t i = 0; i < agg_funcs_.size(); ++i) {
                double value = 0.0;
                switch (agg_funcs_[i]) {
                    case AggFunc::SUM:   value = state.sum_values[i]; break;
                    case AggFunc::COUNT: value = static_cast<double>(state.count_values[i]); break;
                    case AggFunc::AVG:   value = state.count_values[i] > 0 ? state.sum_values[i] / state.count_values[i] : 0.0; break;
                    case AggFunc::MIN:   value = state.min_values[i]; break;
                    case AggFunc::MAX:   value = state.max_values[i]; break;
                }
                PARQUET_THROW_NOT_OK(agg_builders[i].Append(value));
            }
        }

        arrow::FieldVector fields;
        arrow::ArrayVector arrays;
        for (size_t i = 0; i < group_by_columns_.size(); ++i) {
            std::shared_ptr<arrow::Array> arr;
            PARQUET_THROW_NOT_OK(group_builders[i].Finish(&arr));
            arrays.push_back(arr);
            fields.push_back(arrow::field(group_by_columns_[i], arrow::utf8()));
        }
        for (size_t i = 0; i < agg_funcs_.size(); ++i) {
            std::shared_ptr<arrow::Array> arr;
            PARQUET_THROW_NOT_OK(agg_builders[i].Finish(&arr));
            arrays.push_back(arr);
            fields.push_back(arrow::field(agg_output_names_[i], arrow::float64()));
        }
        return arrow::Table::Make(arrow::schema(fields), arrays);
    }

    static std::string ArrayValueToString(const std::shared_ptr<arrow::Array>& array, int64_t index) {
        if (array->IsNull(index)) return "NULL";
        switch (array->type()->id()) {
            case arrow::Type::INT32:
                return std::to_string(std::static_pointer_cast<arrow::Int32Array>(array)->Value(index));
            case arrow::Type::INT64:
                return std::to_string(std::static_pointer_cast<arrow::Int64Array>(array)->Value(index));
            case arrow::Type::DOUBLE:
                return std::to_string(std::static_pointer_cast<arrow::DoubleArray>(array)->Value(index));
            case arrow::Type::STRING:
                return std::static_pointer_cast<arrow::StringArray>(array)->GetString(index);
            case arrow::Type::DATE32:
                return std::to_string(std::static_pointer_cast<arrow::Date32Array>(array)->Value(index));
            default:
                throw std::runtime_error("Unsupported group-by type: " + array->type()->ToString());
        }
    }

    static double ArrayValueToDouble(const std::shared_ptr<arrow::Array>& array, int64_t index) {
        if (array->IsNull(index)) return std::numeric_limits<double>::quiet_NaN();
        switch (array->type()->id()) {
            case arrow::Type::INT32:
                return static_cast<double>(std::static_pointer_cast<arrow::Int32Array>(array)->Value(index));
            case arrow::Type::INT64:
                return static_cast<double>(std::static_pointer_cast<arrow::Int64Array>(array)->Value(index));
            case arrow::Type::DOUBLE:
                return std::static_pointer_cast<arrow::DoubleArray>(array)->Value(index);
            case arrow::Type::FLOAT:
                return static_cast<double>(std::static_pointer_cast<arrow::FloatArray>(array)->Value(index));
            case arrow::Type::DECIMAL128: {
                auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
                auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(array->type());
                int32_t scale = decimal_type->scale();
                const uint8_t* decimal_bytes = decimal_array->Value(index);
                arrow::Decimal128 decimal_val(decimal_bytes);
                // Convert decimal128 to double: divide raw value by 10^scale
                double divisor = std::pow(10.0, scale);
                return static_cast<double>(decimal_val.low_bits()) / divisor;
            }
            default:
                throw std::runtime_error("Unsupported aggregate type: " + array->type()->ToString());
        }
    }
};

} // namespace operators
} // namespace gendb

#endif // OPERATORS_HASH_AGG_H
