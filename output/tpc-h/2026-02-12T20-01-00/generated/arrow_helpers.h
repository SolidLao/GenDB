#ifndef ARROW_HELPERS_H
#define ARROW_HELPERS_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <stdexcept>

namespace gendb {

// Parquet file reader wrapper with column projection and row group filtering
class ParquetReader {
private:
    std::string file_path_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::Schema> schema_;
    int num_row_groups_;

public:
    explicit ParquetReader(const std::string& file_path) : file_path_(file_path) {
        // Open Parquet file
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_path));

        // Create Parquet reader (using Result API instead of deprecated Status API)
        auto reader_result = parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("Failed to open Parquet file: " + file_path + " - " + reader_result.status().ToString());
        }
        reader_ = std::move(reader_result).ValueOrDie();

        // Get schema
        auto status = reader_->GetSchema(&schema_);
        if (!status.ok()) {
            throw std::runtime_error("Failed to get schema: " + status.ToString());
        }

        // Get number of row groups
        num_row_groups_ = reader_->num_row_groups();
    }

    // Read entire table
    std::shared_ptr<arrow::Table> ReadTable() {
        std::shared_ptr<arrow::Table> table;
        arrow::Status status = reader_->ReadTable(&table);
        if (!status.ok()) {
            throw std::runtime_error("Failed to read table: " + status.ToString());
        }
        return table;
    }

    // Read specific columns
    std::shared_ptr<arrow::Table> ReadColumns(const std::vector<int>& column_indices) {
        std::shared_ptr<arrow::Table> table;
        arrow::Status status = reader_->ReadTable(column_indices, &table);
        if (!status.ok()) {
            throw std::runtime_error("Failed to read columns: " + status.ToString());
        }
        return table;
    }

    // Read specific columns by name
    std::shared_ptr<arrow::Table> ReadColumnsByName(const std::vector<std::string>& column_names) {
        std::vector<int> column_indices;
        for (const auto& name : column_names) {
            int idx = schema_->GetFieldIndex(name);
            if (idx < 0) {
                throw std::runtime_error("Column not found: " + name);
            }
            column_indices.push_back(idx);
        }
        return ReadColumns(column_indices);
    }

    // Read specific row groups with column projection
    std::shared_ptr<arrow::Table> ReadRowGroups(
        const std::vector<int>& row_group_indices,
        const std::vector<int>& column_indices
    ) {
        std::shared_ptr<arrow::Table> table;
        arrow::Status status = reader_->ReadRowGroups(row_group_indices, column_indices, &table);
        if (!status.ok()) {
            throw std::runtime_error("Failed to read row groups: " + status.ToString());
        }
        return table;
    }

    // Get row group metadata for filtering
    std::shared_ptr<parquet::FileMetaData> GetMetadata() {
        return reader_->parquet_reader()->metadata();
    }

    int num_row_groups() const { return num_row_groups_; }
    std::shared_ptr<arrow::Schema> schema() const { return schema_; }

    // Filter row groups based on date column statistics (for date32 columns)
    // Returns indices of row groups that may contain rows in the date range
    std::vector<int> FilterRowGroupsByDate32(
        const std::string& column_name,
        int32_t min_date,  // Arrow date32 (days since epoch), -1 = no lower bound
        int32_t max_date   // Arrow date32 (days since epoch), -1 = no upper bound
    ) {
        std::vector<int> selected_row_groups;
        auto metadata = GetMetadata();
        int col_idx = schema_->GetFieldIndex(column_name);

        if (col_idx < 0) {
            throw std::runtime_error("Column not found: " + column_name);
        }

        for (int rg = 0; rg < num_row_groups_; ++rg) {
            auto rg_metadata = metadata->RowGroup(rg);
            auto col_metadata = rg_metadata->ColumnChunk(col_idx);

            if (!col_metadata->is_stats_set()) {
                // No statistics available, must include this row group
                selected_row_groups.push_back(rg);
                continue;
            }

            auto stats = col_metadata->statistics();
            if (!stats) {
                selected_row_groups.push_back(rg);
                continue;
            }

            // Get min/max as int32 (date32)
            auto typed_stats = std::static_pointer_cast<parquet::Int32Statistics>(stats);
            int32_t rg_min = typed_stats->min();
            int32_t rg_max = typed_stats->max();

            // Check if row group overlaps with date range
            bool include = true;
            if (min_date >= 0 && rg_max < min_date) {
                include = false;  // Row group entirely before range
            }
            if (max_date >= 0 && rg_min > max_date) {
                include = false;  // Row group entirely after range
            }

            if (include) {
                selected_row_groups.push_back(rg);
            }
        }

        return selected_row_groups;
    }
};

// Arrow type helpers
template<typename ArrowType>
using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;

template<typename ArrowType>
std::shared_ptr<ArrayType<ArrowType>> GetTypedArray(
    const std::shared_ptr<arrow::Array>& array
) {
    return std::static_pointer_cast<ArrayType<ArrowType>>(array);
}

// Extract typed column from table
template<typename ArrowType>
std::shared_ptr<ArrayType<ArrowType>> GetTypedColumn(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& column_name
) {
    auto chunked_array = table->GetColumnByName(column_name);
    if (!chunked_array) {
        throw std::runtime_error("Column not found: " + column_name);
    }

    // Combine chunks into single array
    auto result = arrow::Concatenate(chunked_array->chunks());
    if (!result.ok()) {
        throw std::runtime_error("Failed to concatenate chunks: " + result.status().ToString());
    }

    return GetTypedArray<ArrowType>(result.ValueOrDie());
}

// Extract typed column by index from table
template<typename ArrowType>
std::shared_ptr<ArrayType<ArrowType>> GetTypedColumnByIndex(
    const std::shared_ptr<arrow::Table>& table,
    int column_index
) {
    auto chunked_array = table->column(column_index);

    // Combine chunks into single array
    auto result = arrow::Concatenate(chunked_array->chunks());
    if (!result.ok()) {
        throw std::runtime_error("Failed to concatenate chunks: " + result.status().ToString());
    }

    return GetTypedArray<ArrowType>(result.ValueOrDie());
}

// Date conversion helpers (Arrow date32 is days since 1970-01-01)
inline int32_t DateToArrowDate32(int year, int month, int day) {
    // Simple epoch calculation (not accounting for leap years precisely)
    // For exact conversion, use Arrow's date parsing utilities
    // This is a simplified version for common cases

    // Days since 1970-01-01
    // Approximation: each year has 365.25 days on average
    int days_per_month[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    int years_since_epoch = year - 1970;
    int days = years_since_epoch * 365 + years_since_epoch / 4;  // Approximate leap years
    days += days_per_month[month - 1];
    days += day - 1;

    // Adjust for leap year in current year (simplified)
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        days += 1;
    }

    return days;
}

// Batch iteration helper
class TableBatchIterator {
private:
    std::shared_ptr<arrow::Table> table_;
    int64_t batch_size_;
    int64_t current_offset_;
    int64_t total_rows_;

public:
    TableBatchIterator(std::shared_ptr<arrow::Table> table, int64_t batch_size)
        : table_(table), batch_size_(batch_size), current_offset_(0) {
        total_rows_ = table->num_rows();
    }

    bool HasNext() const {
        return current_offset_ < total_rows_;
    }

    std::shared_ptr<arrow::Table> Next() {
        if (!HasNext()) {
            return nullptr;
        }

        int64_t length = std::min(batch_size_, total_rows_ - current_offset_);
        auto result = table_->Slice(current_offset_, length);
        current_offset_ += length;
        return result;
    }

    void Reset() {
        current_offset_ = 0;
    }
};

// Combine multiple tables (vertical concatenation)
inline std::shared_ptr<arrow::Table> CombineTables(
    const std::vector<std::shared_ptr<arrow::Table>>& tables
) {
    if (tables.empty()) {
        throw std::runtime_error("Cannot concatenate empty table list");
    }

    auto result = arrow::ConcatenateTables(tables);
    if (!result.ok()) {
        throw std::runtime_error("Failed to concatenate tables: " + result.status().ToString());
    }
    return result.ValueOrDie();
}

// Create RecordBatch from Table (for single-batch processing)
inline std::vector<std::shared_ptr<arrow::RecordBatch>> TableToRecordBatches(
    const std::shared_ptr<arrow::Table>& table,
    int64_t batch_size = 65536
) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(batch_size);

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader.ReadNext(&batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to read batch: " + status.ToString());
        }
        if (!batch) break;
        batches.push_back(batch);
    }

    return batches;
}

} // namespace gendb

#endif // ARROW_HELPERS_H
