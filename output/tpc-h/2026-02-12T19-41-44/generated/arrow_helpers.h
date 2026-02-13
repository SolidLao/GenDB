#ifndef ARROW_HELPERS_H
#define ARROW_HELPERS_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/statistics.h>
#include <memory>
#include <vector>
#include <string>
#include <stdexcept>

namespace gendb {

// ============================================================================
// Parquet File Reader Wrapper
// ============================================================================

class ParquetReader {
private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader_;
    std::shared_ptr<arrow::Schema> schema_;
    int num_row_groups_;

public:
    ParquetReader(const std::string& filepath) {
        auto result = arrow::io::ReadableFile::Open(filepath);
        if (!result.ok()) {
            throw std::runtime_error("Failed to open Parquet file: " + filepath);
        }
        file_ = result.ValueOrDie();

        auto reader_result = parquet::arrow::OpenFile(file_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("Failed to create Arrow reader: " + reader_result.status().ToString());
        }
        arrow_reader_ = std::move(reader_result).ValueOrDie();

        auto status = arrow_reader_->GetSchema(&schema_);
        if (!status.ok()) {
            throw std::runtime_error("Failed to get schema: " + status.ToString());
        }

        num_row_groups_ = arrow_reader_->num_row_groups();
    }

    // Read entire table with optional column projection
    std::shared_ptr<arrow::Table> ReadTable(const std::vector<std::string>& columns = {}) {
        std::shared_ptr<arrow::Table> table;
        arrow::Status status;

        if (columns.empty()) {
            // Read all columns
            status = arrow_reader_->ReadTable(&table);
        } else {
            // Read specific columns
            std::vector<int> column_indices;
            for (const auto& col_name : columns) {
                int idx = schema_->GetFieldIndex(col_name);
                if (idx == -1) {
                    throw std::runtime_error("Column not found: " + col_name);
                }
                column_indices.push_back(idx);
            }
            status = arrow_reader_->ReadTable(column_indices, &table);
        }

        if (!status.ok()) {
            throw std::runtime_error("Failed to read table: " + status.ToString());
        }
        return table;
    }

    // Read specific row groups with column projection
    std::shared_ptr<arrow::Table> ReadRowGroups(
        const std::vector<int>& row_groups,
        const std::vector<std::string>& columns = {}) {

        std::shared_ptr<arrow::Table> table;
        arrow::Status status;

        if (columns.empty()) {
            status = arrow_reader_->ReadRowGroups(row_groups, &table);
        } else {
            std::vector<int> column_indices;
            for (const auto& col_name : columns) {
                int idx = schema_->GetFieldIndex(col_name);
                if (idx == -1) {
                    throw std::runtime_error("Column not found: " + col_name);
                }
                column_indices.push_back(idx);
            }
            status = arrow_reader_->ReadRowGroups(row_groups, column_indices, &table);
        }

        if (!status.ok()) {
            throw std::runtime_error("Failed to read row groups: " + status.ToString());
        }
        return table;
    }

    // Get row group metadata for pruning
    std::vector<int> GetRowGroupsForDateRange(
        const std::string& column_name,
        int32_t min_date,
        int32_t max_date) {

        std::vector<int> selected_row_groups;

        auto parquet_reader = arrow_reader_->parquet_reader();
        auto metadata = parquet_reader->metadata();

        int col_idx = schema_->GetFieldIndex(column_name);
        if (col_idx == -1) {
            throw std::runtime_error("Column not found: " + column_name);
        }

        for (int rg = 0; rg < num_row_groups_; ++rg) {
            auto row_group_metadata = metadata->RowGroup(rg);
            auto column_chunk = row_group_metadata->ColumnChunk(col_idx);

            if (!column_chunk->is_stats_set()) {
                // No stats, must include this row group
                selected_row_groups.push_back(rg);
                continue;
            }

            auto stats = column_chunk->statistics();
            if (stats && stats->HasMinMax()) {
                // For date32 columns stored as int32
                // Use encoded stats which work with all types
                auto encoded_min = stats->EncodeMin();
                auto encoded_max = stats->EncodeMax();

                // Cast to int32 (date32 is stored as int32)
                int32_t rg_min = *reinterpret_cast<const int32_t*>(encoded_min.c_str());
                int32_t rg_max = *reinterpret_cast<const int32_t*>(encoded_max.c_str());

                // Check if row group overlaps with date range
                if (rg_max >= min_date && rg_min <= max_date) {
                    selected_row_groups.push_back(rg);
                }
            } else {
                selected_row_groups.push_back(rg);
            }
        }

        return selected_row_groups;
    }

    int GetNumRowGroups() const { return num_row_groups_; }
    std::shared_ptr<arrow::Schema> GetSchema() const { return schema_; }
};

// ============================================================================
// Arrow Type Helpers
// ============================================================================

// Extract typed array from RecordBatch
template<typename ArrowType>
std::shared_ptr<typename arrow::TypeTraits<ArrowType>::ArrayType>
GetTypedArray(const std::shared_ptr<arrow::RecordBatch>& batch, int column_index) {
    auto array = batch->column(column_index);
    return std::static_pointer_cast<typename arrow::TypeTraits<ArrowType>::ArrayType>(array);
}

template<typename ArrowType>
std::shared_ptr<typename arrow::TypeTraits<ArrowType>::ArrayType>
GetTypedArray(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& column_name) {
    int idx = batch->schema()->GetFieldIndex(column_name);
    if (idx == -1) {
        throw std::runtime_error("Column not found: " + column_name);
    }
    return GetTypedArray<ArrowType>(batch, idx);
}

// Extract typed array from Table column
template<typename ArrowType>
std::shared_ptr<typename arrow::TypeTraits<ArrowType>::ArrayType>
GetTypedColumn(const std::shared_ptr<arrow::Table>& table, int column_index) {
    auto chunked_array = table->column(column_index);
    if (chunked_array->num_chunks() == 0) {
        return nullptr;
    }
    // If multiple chunks, concatenate them
    if (chunked_array->num_chunks() == 1) {
        return std::static_pointer_cast<typename arrow::TypeTraits<ArrowType>::ArrayType>(
            chunked_array->chunk(0));
    } else {
        auto concat_result = arrow::Concatenate(chunked_array->chunks());
        if (!concat_result.ok()) {
            throw std::runtime_error("Failed to concatenate chunks");
        }
        return std::static_pointer_cast<typename arrow::TypeTraits<ArrowType>::ArrayType>(
            concat_result.ValueOrDie());
    }
}

template<typename ArrowType>
std::shared_ptr<typename arrow::TypeTraits<ArrowType>::ArrayType>
GetTypedColumn(const std::shared_ptr<arrow::Table>& table, const std::string& column_name) {
    int idx = table->schema()->GetFieldIndex(column_name);
    if (idx == -1) {
        throw std::runtime_error("Column not found: " + column_name);
    }
    return GetTypedColumn<ArrowType>(table, idx);
}

// ============================================================================
// Date Helpers (TPC-H uses DATE stored as date32 in Parquet)
// ============================================================================

// Convert date string "YYYY-MM-DD" to days since epoch (date32)
inline int32_t DateToDays(const std::string& date_str) {
    // Simple parser for ISO date format "YYYY-MM-DD"
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since 1970-01-01 (Unix epoch)
    // Simplified calculation (doesn't handle all edge cases, but works for TPC-H dates)
    int days_per_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Handle leap years
    auto is_leap = [](int y) {
        return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    };

    int days = 0;
    // Years since epoch
    for (int y = 1970; y < year; ++y) {
        days += is_leap(y) ? 366 : 365;
    }
    // Months in current year
    for (int m = 1; m < month; ++m) {
        days += days_per_month[m];
        if (m == 2 && is_leap(year)) days += 1;
    }
    // Days in current month
    days += day - 1; // -1 because we count from day 0

    return static_cast<int32_t>(days);
}

// ============================================================================
// Batch Processing Utilities
// ============================================================================

class TableBatchIterator {
private:
    std::shared_ptr<arrow::Table> table_;
    int64_t batch_size_;
    int64_t current_offset_;
    int64_t total_rows_;

public:
    TableBatchIterator(std::shared_ptr<arrow::Table> table, int64_t batch_size = 65536)
        : table_(table), batch_size_(batch_size), current_offset_(0) {
        total_rows_ = table_->num_rows();
    }

    bool HasNext() const {
        return current_offset_ < total_rows_;
    }

    std::shared_ptr<arrow::RecordBatch> Next() {
        if (!HasNext()) {
            return nullptr;
        }

        int64_t length = std::min(batch_size_, total_rows_ - current_offset_);
        auto batch_result = table_->Slice(current_offset_, length)->CombineChunksToBatch();

        if (!batch_result.ok()) {
            throw std::runtime_error("Failed to create batch: " + batch_result.status().ToString());
        }

        current_offset_ += length;
        return batch_result.ValueOrDie();
    }

    void Reset() {
        current_offset_ = 0;
    }
};

// ============================================================================
// Parallel Processing Helpers
// ============================================================================

inline int GetHardwareConcurrency() {
    return 64; // Detected from system
}

} // namespace gendb

#endif // ARROW_HELPERS_H
