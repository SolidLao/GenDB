/**
 * parquet_reader.h — Parquet reader + index utilities for GenDB v7
 *
 * Uses Arrow/Parquet ONLY for I/O with parallel reading enabled.
 * Returns raw C++ arrays for processing.
 * Generated query code should never include Arrow headers directly — only this file.
 *
 * Features:
 *   - Parallel multi-threaded Parquet I/O
 *   - Column projection (read only needed columns)
 *   - Row group pruning via statistics
 *   - Sorted index files for selective lookups
 *
 * Usage:
 *   auto table = read_parquet(path, {"col1", "col2"});
 *   const double* col1 = table.column<double>("col1");
 *   int64_t N = table.num_rows;
 *   for (int64_t i = 0; i < N; i++) { ... pure C++ ... }
 */

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <cstdint>
#include <memory>
#include <thread>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Arrow/Parquet headers — used ONLY in this file
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <parquet/metadata.h>

// ─────────────────────────────────────────────────────────────────────────────
// ColumnData: type-erased column storage
// ─────────────────────────────────────────────────────────────────────────────

struct ColumnData {
    std::string name;
    enum Type { INT32, INT64, DOUBLE, STRING, UNKNOWN } type = UNKNOWN;

    // Raw contiguous storage (exactly one is populated)
    std::vector<int32_t> int32_data;
    std::vector<int64_t> int64_data;
    std::vector<double>  double_data;
    std::vector<std::string> string_data;
};

// ─────────────────────────────────────────────────────────────────────────────
// ParquetTable: read result — columns as raw C++ arrays
// ─────────────────────────────────────────────────────────────────────────────

struct ParquetTable {
    int64_t num_rows = 0;
    std::unordered_map<std::string, ColumnData> columns_;

    template<typename T> const T* column(const std::string& name) const;
    template<typename T> T* column_mut(const std::string& name);

    const std::vector<std::string>& string_column(const std::string& name) const {
        auto it = columns_.find(name);
        if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
        return it->second.string_data;
    }

    bool has_column(const std::string& name) const {
        return columns_.find(name) != columns_.end();
    }
};

// Template specializations for column()
template<> inline const int32_t* ParquetTable::column<int32_t>(const std::string& name) const {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.int32_data.data();
}
template<> inline const int64_t* ParquetTable::column<int64_t>(const std::string& name) const {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.int64_data.data();
}
template<> inline const double* ParquetTable::column<double>(const std::string& name) const {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.double_data.data();
}

template<> inline int32_t* ParquetTable::column_mut<int32_t>(const std::string& name) {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.int32_data.data();
}
template<> inline int64_t* ParquetTable::column_mut<int64_t>(const std::string& name) {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.int64_data.data();
}
template<> inline double* ParquetTable::column_mut<double>(const std::string& name) {
    auto it = columns_.find(name);
    if (it == columns_.end()) throw std::runtime_error("Column not found: " + name);
    return it->second.double_data.data();
}

// ─────────────────────────────────────────────────────────────────────────────
// Row group statistics (for predicate pushdown / row group pruning)
// ─────────────────────────────────────────────────────────────────────────────

struct RowGroupStats {
    int row_group_index;
    int64_t num_rows;
    bool has_min_max = false;
    int64_t min_int = 0, max_int = 0;
    double min_double = 0, max_double = 0;
};

inline std::vector<RowGroupStats> get_row_group_stats(
    const std::string& parquet_path,
    const std::string& column_name)
{
    auto infile = arrow::io::ReadableFile::Open(parquet_path).ValueOrDie();
    auto file_reader = parquet::ParquetFileReader::Open(infile);
    auto file_metadata = file_reader->metadata();

    int col_idx = -1;
    auto schema = file_metadata->schema();
    for (int i = 0; i < schema->num_columns(); i++) {
        if (schema->Column(i)->name() == column_name) {
            col_idx = i;
            break;
        }
    }
    if (col_idx < 0) throw std::runtime_error("Column not found in Parquet: " + column_name);

    std::vector<RowGroupStats> stats;
    for (int rg = 0; rg < file_metadata->num_row_groups(); rg++) {
        auto rg_metadata = file_metadata->RowGroup(rg);
        auto col_chunk = rg_metadata->ColumnChunk(col_idx);

        RowGroupStats s;
        s.row_group_index = rg;
        s.num_rows = rg_metadata->num_rows();

        if (col_chunk->is_stats_set()) {
            auto stat = col_chunk->statistics();
            if (stat && stat->HasMinMax()) {
                s.has_min_max = true;
                auto physical_type = schema->Column(col_idx)->physical_type();

                if (physical_type == parquet::Type::INT32) {
                    auto typed = std::static_pointer_cast<parquet::Int32Statistics>(stat);
                    s.min_int = typed->min();
                    s.max_int = typed->max();
                    s.min_double = static_cast<double>(typed->min());
                    s.max_double = static_cast<double>(typed->max());
                } else if (physical_type == parquet::Type::INT64) {
                    auto typed = std::static_pointer_cast<parquet::Int64Statistics>(stat);
                    s.min_int = typed->min();
                    s.max_int = typed->max();
                    s.min_double = static_cast<double>(typed->min());
                    s.max_double = static_cast<double>(typed->max());
                } else if (physical_type == parquet::Type::DOUBLE) {
                    auto typed = std::static_pointer_cast<parquet::DoubleStatistics>(stat);
                    s.min_double = typed->min();
                    s.max_double = typed->max();
                    s.min_int = static_cast<int64_t>(typed->min());
                    s.max_int = static_cast<int64_t>(typed->max());
                } else if (physical_type == parquet::Type::FLOAT) {
                    auto typed = std::static_pointer_cast<parquet::FloatStatistics>(stat);
                    s.min_double = typed->min();
                    s.max_double = typed->max();
                }
            }
        }
        stats.push_back(s);
    }
    return stats;
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal: extract Arrow array → C++ vector
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

inline std::shared_ptr<arrow::Array> concat_chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked) {
    if (chunked->num_chunks() == 1) return chunked->chunk(0);
    return arrow::Concatenate(chunked->chunks()).ValueOrDie();
}

inline void extract_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                           ColumnData& col) {
    auto arr = concat_chunks(chunked);
    auto type = arr->type();
    int64_t len = arr->length();

    if (type->id() == arrow::Type::INT32 || type->id() == arrow::Type::DATE32) {
        col.type = ColumnData::INT32;
        col.int32_data.resize(len);
        auto typed = std::static_pointer_cast<arrow::Int32Array>(arr);
        std::memcpy(col.int32_data.data(), typed->raw_values(), len * sizeof(int32_t));
    }
    else if (type->id() == arrow::Type::INT64) {
        col.type = ColumnData::INT64;
        col.int64_data.resize(len);
        auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
        std::memcpy(col.int64_data.data(), typed->raw_values(), len * sizeof(int64_t));
    }
    else if (type->id() == arrow::Type::DOUBLE) {
        col.type = ColumnData::DOUBLE;
        col.double_data.resize(len);
        auto typed = std::static_pointer_cast<arrow::DoubleArray>(arr);
        std::memcpy(col.double_data.data(), typed->raw_values(), len * sizeof(double));
    }
    else if (type->id() == arrow::Type::FLOAT) {
        col.type = ColumnData::DOUBLE;
        col.double_data.resize(len);
        auto typed = std::static_pointer_cast<arrow::FloatArray>(arr);
        for (int64_t i = 0; i < len; i++) col.double_data[i] = typed->Value(i);
    }
    else if (type->id() == arrow::Type::DECIMAL128) {
        col.type = ColumnData::DOUBLE;
        auto cast_result = arrow::compute::Cast(arr, arrow::float64()).ValueOrDie();
        auto dbl_arr = std::static_pointer_cast<arrow::DoubleArray>(cast_result.make_array());
        col.double_data.resize(len);
        std::memcpy(col.double_data.data(), dbl_arr->raw_values(), len * sizeof(double));
    }
    else if (type->id() == arrow::Type::STRING || type->id() == arrow::Type::LARGE_STRING) {
        col.type = ColumnData::STRING;
        col.string_data.resize(len);
        auto typed = std::static_pointer_cast<arrow::StringArray>(arr);
        for (int64_t i = 0; i < len; i++) col.string_data[i] = typed->GetString(i);
    }
    else if (type->id() == arrow::Type::DICTIONARY) {
        auto dict_arr = std::static_pointer_cast<arrow::DictionaryArray>(arr);
        auto values = dict_arr->dictionary();
        if (values->type()->id() == arrow::Type::STRING) {
            col.type = ColumnData::STRING;
            col.string_data.resize(len);
            auto str_values = std::static_pointer_cast<arrow::StringArray>(values);
            auto indices = dict_arr->indices();
            auto idx32 = std::dynamic_pointer_cast<arrow::Int32Array>(indices);
            auto idx16 = std::dynamic_pointer_cast<arrow::Int16Array>(indices);
            auto idx8  = std::dynamic_pointer_cast<arrow::Int8Array>(indices);
            for (int64_t i = 0; i < len; i++) {
                int32_t idx;
                if (idx32) idx = idx32->Value(i);
                else if (idx16) idx = idx16->Value(i);
                else if (idx8)  idx = idx8->Value(i);
                else idx = 0;
                col.string_data[i] = str_values->GetString(idx);
            }
        }
    }
    else {
        throw std::runtime_error("Unsupported column type for: " + col.name);
    }
}

inline std::unique_ptr<parquet::arrow::FileReader> open_parquet_reader(
    const std::string& path)
{
    auto infile = arrow::io::ReadableFile::Open(path).ValueOrDie();

    parquet::ArrowReaderProperties arrow_props;
    arrow_props.set_use_threads(true);  // parallel column/row-group decoding

    parquet::ReaderProperties reader_props;
    reader_props.enable_buffered_stream();

    auto file_reader = parquet::ParquetFileReader::Open(infile, reader_props);
    auto file_metadata = file_reader->metadata();

    // Re-open with Arrow reader for parallel reading
    auto infile2 = arrow::io::ReadableFile::Open(path).ValueOrDie();
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        parquet::ParquetFileReader::Open(infile2, reader_props),
        arrow_props,
        &reader);
    if (!status.ok()) throw std::runtime_error("Failed to open Parquet: " + path + " — " + status.ToString());

    reader->set_use_threads(true);
    return reader;
}

inline void extract_table_columns(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<std::string>& column_names,
    ParquetTable& result)
{
    result.num_rows = table->num_rows();
    const auto& names = column_names.empty()
        ? [&]() -> std::vector<std::string> {
            std::vector<std::string> all;
            for (int i = 0; i < table->schema()->num_fields(); i++)
                all.push_back(table->schema()->field(i)->name());
            return all;
        }()
        : column_names;

    for (const auto& name : names) {
        auto chunked = table->GetColumnByName(name);
        if (!chunked) continue;
        ColumnData col;
        col.name = name;
        extract_column(chunked, col);
        result.columns_[name] = std::move(col);
    }
}

} // namespace detail

// ─────────────────────────────────────────────────────────────────────────────
// read_parquet: main entry point — with parallel I/O
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Read selected columns from a Parquet file into raw C++ arrays.
 * Uses multi-threaded reading for parallel I/O.
 */
inline ParquetTable read_parquet(
    const std::string& path,
    const std::vector<std::string>& column_names = {})
{
    auto reader = detail::open_parquet_reader(path);

    std::shared_ptr<arrow::Table> table;
    arrow::Status status;

    if (column_names.empty()) {
        status = reader->ReadTable(&table);
    } else {
        std::shared_ptr<arrow::Schema> schema;
        status = reader->GetSchema(&schema);
        if (!status.ok()) throw std::runtime_error("Failed to read schema: " + status.ToString());

        std::vector<int> col_indices;
        for (const auto& name : column_names) {
            int idx = schema->GetFieldIndex(name);
            if (idx < 0) throw std::runtime_error("Column not in Parquet file: " + name + " (file: " + path + ")");
            col_indices.push_back(idx);
        }
        status = reader->ReadTable(col_indices, &table);
    }
    if (!status.ok()) throw std::runtime_error("Failed to read table: " + status.ToString());

    ParquetTable result;
    detail::extract_table_columns(table, column_names, result);
    return result;
}

/**
 * Read specific row groups from a Parquet file (for row group pruning).
 * Uses multi-threaded reading for parallel I/O.
 */
inline ParquetTable read_parquet_row_groups(
    const std::string& path,
    const std::vector<std::string>& column_names,
    const std::vector<int>& row_groups)
{
    auto reader = detail::open_parquet_reader(path);

    std::shared_ptr<arrow::Schema> schema;
    reader->GetSchema(&schema);

    std::vector<int> col_indices;
    for (const auto& name : column_names) {
        int idx = schema->GetFieldIndex(name);
        if (idx < 0) throw std::runtime_error("Column not in Parquet: " + name);
        col_indices.push_back(idx);
    }

    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadRowGroups(row_groups, col_indices, &table);
    if (!status.ok()) throw std::runtime_error("Failed to read row groups: " + status.ToString());

    ParquetTable result;
    detail::extract_table_columns(table, column_names, result);
    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorted Index: binary index file for selective lookups
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Index file format (.idx):
 *   Header:  num_entries (int64_t)
 *   Keys:    sorted array of num_entries keys (int32_t or int64_t)
 *   RowIDs:  array of num_entries row_group IDs (int32_t), parallel to keys
 *
 * Built by build_indexes.py. Memory-mapped for zero-copy access.
 * For each unique key value, stores which row groups contain it.
 * Binary search enables O(log N) lookup of row groups for any key value.
 */
template<typename KeyType>
class SortedIndex {
public:
    SortedIndex() = default;

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        struct stat st;
        fstat(fd, &st);
        size_ = st.st_size;

        data_ = static_cast<char*>(mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0));
        close(fd);
        if (data_ == MAP_FAILED) { data_ = nullptr; return false; }

        madvise(data_, size_, MADV_SEQUENTIAL);

        num_entries_ = *reinterpret_cast<int64_t*>(data_);
        keys_ = reinterpret_cast<const KeyType*>(data_ + sizeof(int64_t));
        row_groups_ = reinterpret_cast<const int32_t*>(
            data_ + sizeof(int64_t) + num_entries_ * sizeof(KeyType));

        loaded_ = true;
        return true;
    }

    ~SortedIndex() {
        if (data_ && data_ != MAP_FAILED) munmap(data_, size_);
    }

    // Non-copyable, movable
    SortedIndex(const SortedIndex&) = delete;
    SortedIndex& operator=(const SortedIndex&) = delete;
    SortedIndex(SortedIndex&& o) noexcept
        : data_(o.data_), size_(o.size_), num_entries_(o.num_entries_),
          keys_(o.keys_), row_groups_(o.row_groups_), loaded_(o.loaded_) {
        o.data_ = nullptr; o.loaded_ = false;
    }

    bool is_loaded() const { return loaded_; }
    int64_t size() const { return num_entries_; }

    /** Find all row group IDs that contain a specific key value. */
    std::vector<int32_t> lookup_row_groups(KeyType key) const {
        if (!loaded_) return {};
        auto [lo, hi] = equal_range(key);
        std::vector<int32_t> result;
        // Deduplicate row group IDs
        std::unordered_map<int32_t, bool> seen;
        for (int64_t i = lo; i < hi; i++) {
            if (!seen[row_groups_[i]]) {
                seen[row_groups_[i]] = true;
                result.push_back(row_groups_[i]);
            }
        }
        return result;
    }

    /** Find row groups for a set of keys (union). */
    std::vector<int32_t> lookup_row_groups_batch(const std::vector<KeyType>& keys) const {
        if (!loaded_) return {};
        std::unordered_map<int32_t, bool> seen;
        for (auto key : keys) {
            auto [lo, hi] = equal_range(key);
            for (int64_t i = lo; i < hi; i++) {
                seen[row_groups_[i]] = true;
            }
        }
        std::vector<int32_t> result;
        result.reserve(seen.size());
        for (auto& [rg, _] : seen) result.push_back(rg);
        std::sort(result.begin(), result.end());
        return result;
    }

    /** Find row groups for a key range [lo_key, hi_key). */
    std::vector<int32_t> lookup_row_groups_range(KeyType lo_key, KeyType hi_key) const {
        if (!loaded_) return {};
        int64_t lo = lower_bound(lo_key);
        int64_t hi = lower_bound(hi_key);
        std::unordered_map<int32_t, bool> seen;
        for (int64_t i = lo; i < hi; i++) {
            seen[row_groups_[i]] = true;
        }
        std::vector<int32_t> result;
        result.reserve(seen.size());
        for (auto& [rg, _] : seen) result.push_back(rg);
        std::sort(result.begin(), result.end());
        return result;
    }

private:
    char* data_ = nullptr;
    size_t size_ = 0;
    int64_t num_entries_ = 0;
    const KeyType* keys_ = nullptr;
    const int32_t* row_groups_ = nullptr;
    bool loaded_ = false;

    int64_t lower_bound(KeyType key) const {
        int64_t lo = 0, hi = num_entries_;
        while (lo < hi) {
            int64_t mid = lo + (hi - lo) / 2;
            if (keys_[mid] < key) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    std::pair<int64_t, int64_t> equal_range(KeyType key) const {
        int64_t lo = lower_bound(key);
        int64_t hi = lo;
        while (hi < num_entries_ && keys_[hi] == key) hi++;
        return {lo, hi};
    }
};

/** Convenience: load a sorted index file. Returns empty index if file doesn't exist. */
template<typename KeyType>
inline SortedIndex<KeyType> load_index(const std::string& path) {
    SortedIndex<KeyType> idx;
    idx.load(path);
    return idx;
}

// ─────────────────────────────────────────────────────────────────────────────
// Date utilities
// ─────────────────────────────────────────────────────────────────────────────

inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int y = year - 1970;
    int d = y * 365 + (y > 0 ? (y + 1) / 4 : 0) + dpm[month - 1] + day - 1;
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) d++;
    return d;
}

inline std::string days_to_date_str(int32_t days) {
    struct tm t = {};
    time_t secs = static_cast<time_t>(days) * 86400;
    gmtime_r(&secs, &t);
    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return buf;
}
