#include "storage.h"
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <cstring>

namespace Storage {

std::string getColumnPath(const std::string& dir, const std::string& table_name,
                         const std::string& column_name) {
    return dir + "/" + table_name + "/" + column_name + ".bin";
}

std::string getMetadataPath(const std::string& dir, const std::string& table_name) {
    return dir + "/" + table_name + "/metadata.bin";
}

void writeMetadata(const std::string& dir, const TableMetadata& metadata) {
    std::string path = getMetadataPath(dir, metadata.table_name);
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open metadata file: " << path << std::endl;
        return;
    }

    // Write header
    out.write(reinterpret_cast<const char*>(&metadata.row_count), sizeof(uint64_t));
    out.write(reinterpret_cast<const char*>(&metadata.block_size), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&metadata.num_blocks), sizeof(uint32_t));

    // Write column info
    uint32_t num_columns = metadata.columns.size();
    out.write(reinterpret_cast<const char*>(&num_columns), sizeof(uint32_t));
    for (const auto& col : metadata.columns) {
        uint16_t name_len = col.name.size();
        out.write(reinterpret_cast<const char*>(&name_len), sizeof(uint16_t));
        out.write(col.name.data(), name_len);

        uint16_t type_len = col.type.size();
        out.write(reinterpret_cast<const char*>(&type_len), sizeof(uint16_t));
        out.write(col.type.data(), type_len);

        out.write(reinterpret_cast<const char*>(&col.is_dict_encoded), sizeof(bool));
        out.write(reinterpret_cast<const char*>(&col.has_zone_map), sizeof(bool));
    }

    // Write dictionaries
    uint32_t num_dicts = metadata.dictionaries.size();
    out.write(reinterpret_cast<const char*>(&num_dicts), sizeof(uint32_t));
    for (const auto& [col_name, dict] : metadata.dictionaries) {
        uint16_t name_len = col_name.size();
        out.write(reinterpret_cast<const char*>(&name_len), sizeof(uint16_t));
        out.write(col_name.data(), name_len);

        uint32_t num_entries = dict.values.size();
        out.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));
        for (const auto& value : dict.values) {
            uint16_t value_len = value.size();
            out.write(reinterpret_cast<const char*>(&value_len), sizeof(uint16_t));
            out.write(value.data(), value_len);
        }
    }

    // Write zone maps
    uint32_t num_zone_maps = metadata.zone_maps.size();
    out.write(reinterpret_cast<const char*>(&num_zone_maps), sizeof(uint32_t));
    for (const auto& [col_name, zones] : metadata.zone_maps) {
        uint16_t name_len = col_name.size();
        out.write(reinterpret_cast<const char*>(&name_len), sizeof(uint16_t));
        out.write(col_name.data(), name_len);

        uint32_t num_zones = zones.size();
        out.write(reinterpret_cast<const char*>(&num_zones), sizeof(uint32_t));
        for (const auto& zone : zones) {
            out.write(reinterpret_cast<const char*>(&zone.min_value), sizeof(int64_t));
            out.write(reinterpret_cast<const char*>(&zone.max_value), sizeof(int64_t));
        }
    }

    out.close();
}

TableMetadata readMetadata(const std::string& dir, const std::string& table_name) {
    TableMetadata metadata;
    metadata.table_name = table_name;

    std::string path = getMetadataPath(dir, table_name);
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open metadata file: " << path << std::endl;
        return metadata;
    }

    // Read header
    in.read(reinterpret_cast<char*>(&metadata.row_count), sizeof(uint64_t));
    in.read(reinterpret_cast<char*>(&metadata.block_size), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&metadata.num_blocks), sizeof(uint32_t));

    // Read column info
    uint32_t num_columns;
    in.read(reinterpret_cast<char*>(&num_columns), sizeof(uint32_t));
    metadata.columns.resize(num_columns);
    for (auto& col : metadata.columns) {
        uint16_t name_len;
        in.read(reinterpret_cast<char*>(&name_len), sizeof(uint16_t));
        col.name.resize(name_len);
        in.read(&col.name[0], name_len);

        uint16_t type_len;
        in.read(reinterpret_cast<char*>(&type_len), sizeof(uint16_t));
        col.type.resize(type_len);
        in.read(&col.type[0], type_len);

        in.read(reinterpret_cast<char*>(&col.is_dict_encoded), sizeof(bool));
        in.read(reinterpret_cast<char*>(&col.has_zone_map), sizeof(bool));
    }

    // Read dictionaries
    uint32_t num_dicts;
    in.read(reinterpret_cast<char*>(&num_dicts), sizeof(uint32_t));
    for (uint32_t i = 0; i < num_dicts; ++i) {
        uint16_t name_len;
        in.read(reinterpret_cast<char*>(&name_len), sizeof(uint16_t));
        std::string col_name(name_len, '\0');
        in.read(&col_name[0], name_len);

        Dictionary dict;
        uint32_t num_entries;
        in.read(reinterpret_cast<char*>(&num_entries), sizeof(uint32_t));
        dict.values.resize(num_entries);
        for (auto& value : dict.values) {
            uint16_t value_len;
            in.read(reinterpret_cast<char*>(&value_len), sizeof(uint16_t));
            value.resize(value_len);
            in.read(&value[0], value_len);
        }

        // Rebuild value_to_id map
        for (size_t j = 0; j < dict.values.size(); ++j) {
            dict.value_to_id[dict.values[j]] = static_cast<uint8_t>(j);
        }

        metadata.dictionaries[col_name] = std::move(dict);
    }

    // Read zone maps
    uint32_t num_zone_maps;
    in.read(reinterpret_cast<char*>(&num_zone_maps), sizeof(uint32_t));
    for (uint32_t i = 0; i < num_zone_maps; ++i) {
        uint16_t name_len;
        in.read(reinterpret_cast<char*>(&name_len), sizeof(uint16_t));
        std::string col_name(name_len, '\0');
        in.read(&col_name[0], name_len);

        uint32_t num_zones;
        in.read(reinterpret_cast<char*>(&num_zones), sizeof(uint32_t));
        std::vector<ZoneMap> zones(num_zones);
        for (auto& zone : zones) {
            in.read(reinterpret_cast<char*>(&zone.min_value), sizeof(int64_t));
            in.read(reinterpret_cast<char*>(&zone.max_value), sizeof(int64_t));
        }

        metadata.zone_maps[col_name] = std::move(zones);
    }

    in.close();
    return metadata;
}

void writeColumn(const std::string& dir, const std::string& table_name,
                 const std::string& column_name, const void* data, size_t size) {
    // Create table directory if it doesn't exist
    std::string table_dir = dir + "/" + table_name;
    mkdir(table_dir.c_str(), 0755);

    std::string path = getColumnPath(dir, table_name, column_name);
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open column file: " << path << std::endl;
        return;
    }

    out.write(static_cast<const char*>(data), size);
    out.close();
}

std::unique_ptr<ColumnData> mmapColumn(const std::string& dir, const std::string& table_name,
                                        const std::string& column_name, const std::string& type) {
    auto col_data = std::make_unique<ColumnData>();
    col_data->type = type;

    std::string path = getColumnPath(dir, table_name, column_name);
    col_data->fd = open(path.c_str(), O_RDONLY);
    if (col_data->fd < 0) {
        std::cerr << "Failed to open column file: " << path << std::endl;
        return col_data;
    }

    // Get file size
    struct stat st;
    if (fstat(col_data->fd, &st) < 0) {
        std::cerr << "Failed to stat column file: " << path << std::endl;
        return col_data;
    }
    col_data->size = st.st_size;

    // Memory map the file
    col_data->data = mmap(nullptr, col_data->size, PROT_READ, MAP_PRIVATE, col_data->fd, 0);
    if (col_data->data == MAP_FAILED) {
        std::cerr << "Failed to mmap column file: " << path << std::endl;
        col_data->data = nullptr;
        return col_data;
    }

    // Hint sequential access
    madvise(col_data->data, col_data->size, MADV_SEQUENTIAL);

    return col_data;
}

} // namespace Storage
