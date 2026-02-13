#ifndef STORAGE_H
#define STORAGE_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

// Forward declarations
struct TableMetadata;
struct ColumnData;

// Dictionary for string encoding
struct Dictionary {
    std::vector<std::string> values;
    std::unordered_map<std::string, uint8_t> value_to_id;

    uint8_t encode(const std::string& value) {
        auto it = value_to_id.find(value);
        if (it != value_to_id.end()) {
            return it->second;
        }
        uint8_t id = static_cast<uint8_t>(values.size());
        values.push_back(value);
        value_to_id[value] = id;
        return id;
    }

    const std::string& decode(uint8_t id) const {
        return values[id];
    }
};

// Zone map for block pruning
struct ZoneMap {
    int64_t min_value;
    int64_t max_value;
};

// Column metadata
struct ColumnInfo {
    std::string name;
    std::string type; // "int32", "int64", "uint8", "string"
    bool is_dict_encoded;
    bool has_zone_map;
};

// Table metadata (stored in metadata.bin)
struct TableMetadata {
    std::string table_name;
    uint64_t row_count;
    uint32_t block_size;
    uint32_t num_blocks;
    std::vector<ColumnInfo> columns;
    std::unordered_map<std::string, Dictionary> dictionaries;
    std::unordered_map<std::string, std::vector<ZoneMap>> zone_maps;

    TableMetadata() : row_count(0), block_size(65536), num_blocks(0) {}
};

// Memory-mapped column data
struct ColumnData {
    void* data;
    size_t size;
    int fd;
    std::string type;

    ColumnData() : data(nullptr), size(0), fd(-1) {}

    ~ColumnData() {
        close();
    }

    void close() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    template<typename T>
    T* as() {
        return static_cast<T*>(data);
    }

    template<typename T>
    const T* as() const {
        return static_cast<const T*>(data);
    }
};

// Storage operations
namespace Storage {

// Write operations (for ingest)
void writeMetadata(const std::string& dir, const TableMetadata& metadata);
void writeColumn(const std::string& dir, const std::string& table_name,
                 const std::string& column_name, const void* data, size_t size);

template<typename T>
void writeColumnTyped(const std::string& dir, const std::string& table_name,
                      const std::string& column_name, const std::vector<T>& data) {
    writeColumn(dir, table_name, column_name, data.data(), data.size() * sizeof(T));
}

// Read operations (for main)
TableMetadata readMetadata(const std::string& dir, const std::string& table_name);
std::unique_ptr<ColumnData> mmapColumn(const std::string& dir, const std::string& table_name,
                                        const std::string& column_name, const std::string& type);

// Utility functions
std::string getColumnPath(const std::string& dir, const std::string& table_name,
                         const std::string& column_name);
std::string getMetadataPath(const std::string& dir, const std::string& table_name);

} // namespace Storage

#endif // STORAGE_H
