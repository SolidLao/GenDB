#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>

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

// Table metadata
struct TableMetadata {
    std::string name;
    size_t row_count;
    std::vector<std::string> columns;
};

void write_metadata(const std::string& gendb_dir, const std::vector<TableMetadata>& tables);
std::vector<TableMetadata> read_metadata(const std::string& gendb_dir);

} // namespace gendb
