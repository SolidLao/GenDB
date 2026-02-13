#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>
#include <fstream>
#include <stdexcept>
#include <algorithm>

namespace gendb {

// Dictionary for encoding low-cardinality string columns
class Dictionary {
public:
    uint8_t encode(const std::string& value);
    const std::string& decode(uint8_t code) const;
    size_t size() const { return values_.size(); }

private:
    std::vector<std::string> values_;
    std::unordered_map<std::string, uint8_t> code_map_;
};

// Column storage structures (write during ingest, mmap during query)
struct ColumnWriter {
    static void write_int32(const std::string& path, const std::vector<int32_t>& data);
    static void write_int64(const std::string& path, const std::vector<int64_t>& data);
    static void write_uint8(const std::string& path, const std::vector<uint8_t>& data);
    static void write_string(const std::string& path, const std::vector<std::string>& data);
    static void write_dictionary(const std::string& path, const Dictionary& dict);
};

struct ColumnReader {
    static int32_t* mmap_int32(const std::string& path, size_t& count);
    static int64_t* mmap_int64(const std::string& path, size_t& count);
    static uint8_t* mmap_uint8(const std::string& path, size_t& count);
    static std::vector<std::string> read_strings(const std::string& path);
    static Dictionary read_dictionary(const std::string& path);
    static void unmap(void* addr, size_t length);
};

// Zone-map for block skipping (min/max values per block)
template<typename T>
struct ZoneMapEntry {
    T min_value;
    T max_value;
};

template<typename T>
struct ZoneMap {
    size_t block_size;  // rows per block (e.g., 8192)
    std::vector<ZoneMapEntry<T>> entries;

    // Check if a block might contain values in range [min_filter, max_filter)
    bool block_may_match(size_t block_idx, T min_filter, T max_filter) const {
        if (block_idx >= entries.size()) return false;
        const auto& entry = entries[block_idx];
        // Block matches if: block_max >= min_filter AND block_min < max_filter
        return entry.max_value >= min_filter && entry.min_value < max_filter;
    }

    // Check if block overlaps with range [min, max] (inclusive)
    bool block_overlaps_range(size_t block_idx, T range_min, T range_max) const {
        if (block_idx >= entries.size()) return false;
        const auto& entry = entries[block_idx];
        return entry.max_value >= range_min && entry.min_value <= range_max;
    }

    // Check if block contains values >= threshold
    bool block_contains_gte(size_t block_idx, T threshold) const {
        if (block_idx >= entries.size()) return false;
        return entries[block_idx].max_value >= threshold;
    }

    // Check if block contains values < threshold
    bool block_contains_lt(size_t block_idx, T threshold) const {
        if (block_idx >= entries.size()) return false;
        return entries[block_idx].min_value < threshold;
    }

    // Get row range for a block
    std::pair<size_t, size_t> get_block_range(size_t block_idx, size_t total_rows) const {
        size_t start_row = block_idx * block_size;
        size_t end_row = std::min(start_row + block_size, total_rows);
        return {start_row, end_row};
    }

    size_t get_num_blocks() const { return entries.size(); }
};

struct ZoneMapWriter {
    template<typename T>
    static void write_zonemap(const std::string& path, const ZoneMap<T>& zonemap) {
        std::ofstream out(path, std::ios::binary);
        if (!out) throw std::runtime_error("Cannot write zonemap: " + path);

        // Write header: block_size and number of entries
        out.write(reinterpret_cast<const char*>(&zonemap.block_size), sizeof(zonemap.block_size));
        size_t num_entries = zonemap.entries.size();
        out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));

        // Write all entries
        for (const auto& entry : zonemap.entries) {
            out.write(reinterpret_cast<const char*>(&entry.min_value), sizeof(entry.min_value));
            out.write(reinterpret_cast<const char*>(&entry.max_value), sizeof(entry.max_value));
        }
    }
};

struct ZoneMapReader {
    template<typename T>
    static ZoneMap<T> read_zonemap(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) throw std::runtime_error("Cannot read zonemap: " + path);

        ZoneMap<T> zonemap;

        // Read header
        in.read(reinterpret_cast<char*>(&zonemap.block_size), sizeof(zonemap.block_size));
        size_t num_entries;
        in.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));

        // Read all entries
        zonemap.entries.resize(num_entries);
        for (size_t i = 0; i < num_entries; ++i) {
            in.read(reinterpret_cast<char*>(&zonemap.entries[i].min_value), sizeof(T));
            in.read(reinterpret_cast<char*>(&zonemap.entries[i].max_value), sizeof(T));
        }

        return zonemap;
    }
};

// Table metadata
struct TableMetadata {
    std::string name;
    size_t row_count;
    std::vector<std::string> columns;
};

void write_metadata(const std::string& gendb_dir, const std::vector<TableMetadata>& tables);
std::vector<TableMetadata> read_metadata(const std::string& gendb_dir);

} // namespace gendb
