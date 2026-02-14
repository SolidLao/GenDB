#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <unordered_map>
#include <map>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;
using namespace std::chrono;

// Read binary column file into memory
template <typename T>
std::vector<T> read_column(const std::string& filepath, size_t count) {
    std::vector<T> data(count);
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }
    file.read(reinterpret_cast<char*>(data.data()), count * sizeof(T));
    return data;
}

// Read string column file
std::vector<std::string> read_string_column(const std::string& filepath, size_t count) {
    std::vector<std::string> data(count);
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    for (size_t i = 0; i < count; ++i) {
        uint32_t len;
        file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        char buffer[256];
        file.read(buffer, len);
        data[i] = std::string(buffer, len);
    }
    return data;
}

// Write sorted index
template <typename T>
void write_sorted_index(const std::string& index_path, const std::vector<T>& column) {
    std::vector<std::pair<T, uint32_t>> pairs;
    for (size_t i = 0; i < column.size(); ++i) {
        pairs.push_back({column[i], (uint32_t)i});
    }

    std::sort(pairs.begin(), pairs.end());

    std::ofstream file(index_path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create index: " + index_path);
    }

    uint32_t size = pairs.size();
    file.write(reinterpret_cast<const char*>(&size), sizeof(uint32_t));

    for (const auto& p : pairs) {
        file.write(reinterpret_cast<const char*>(&p.first), sizeof(T));
        file.write(reinterpret_cast<const char*>(&p.second), sizeof(uint32_t));
    }
}

// Write hash index
template <typename T>
void write_hash_index(const std::string& index_path, const std::vector<T>& column) {
    std::unordered_map<std::string, std::vector<uint32_t>> hash_map;

    for (size_t i = 0; i < column.size(); ++i) {
        std::string key = std::to_string(column[i]);
        hash_map[key].push_back((uint32_t)i);
    }

    std::ofstream file(index_path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create index: " + index_path);
    }

    uint32_t bucket_count = hash_map.size();
    file.write(reinterpret_cast<const char*>(&bucket_count), sizeof(uint32_t));

    for (const auto& [key, values] : hash_map) {
        uint32_t key_len = key.length();
        file.write(reinterpret_cast<const char*>(&key_len), sizeof(uint32_t));
        file.write(key.c_str(), key_len);

        uint32_t value_count = values.size();
        file.write(reinterpret_cast<const char*>(&value_count), sizeof(uint32_t));
        for (uint32_t v : values) {
            file.write(reinterpret_cast<const char*>(&v), sizeof(uint32_t));
        }
    }
}

// Specialized hash index for strings
void write_string_hash_index(const std::string& index_path, const std::vector<uint8_t>& column) {
    std::unordered_map<uint8_t, std::vector<uint32_t>> hash_map;

    for (size_t i = 0; i < column.size(); ++i) {
        hash_map[column[i]].push_back((uint32_t)i);
    }

    std::ofstream file(index_path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create index: " + index_path);
    }

    uint32_t bucket_count = hash_map.size();
    file.write(reinterpret_cast<const char*>(&bucket_count), sizeof(uint32_t));

    for (const auto& [key, values] : hash_map) {
        file.write(reinterpret_cast<const char*>(&key), sizeof(uint8_t));

        uint32_t value_count = values.size();
        file.write(reinterpret_cast<const char*>(&value_count), sizeof(uint32_t));
        for (uint32_t v : values) {
            file.write(reinterpret_cast<const char*>(&v), sizeof(uint32_t));
        }
    }
}

// Write zone map (min/max per block)
template <typename T>
void write_zone_map(const std::string& zonemap_path, const std::vector<T>& column, size_t block_size) {
    std::vector<std::pair<T, T>> zone_map;

    for (size_t i = 0; i < column.size(); i += block_size) {
        size_t block_end = std::min(i + block_size, column.size());
        T min_val = column[i];
        T max_val = column[i];

        for (size_t j = i; j < block_end; ++j) {
            min_val = std::min(min_val, column[j]);
            max_val = std::max(max_val, column[j]);
        }

        zone_map.push_back({min_val, max_val});
    }

    std::ofstream file(zonemap_path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create zone map: " + zonemap_path);
    }

    uint32_t block_count = zone_map.size();
    file.write(reinterpret_cast<const char*>(&block_count), sizeof(uint32_t));
    file.write(reinterpret_cast<const char*>(&block_size), sizeof(uint32_t));

    for (const auto& [min_val, max_val] : zone_map) {
        file.write(reinterpret_cast<const char*>(&min_val), sizeof(T));
        file.write(reinterpret_cast<const char*>(&max_val), sizeof(T));
    }
}

// Build lineitem indexes
void build_lineitem_indexes(const std::string& gendb_dir) {
    std::string table_dir = gendb_dir + "/lineitem";
    std::cout << "Building lineitem indexes...\n";

    // Read l_shipdate and l_orderkey columns
    auto l_shipdate = read_column<int32_t>(table_dir + "/l_shipdate.bin", 59986052);
    auto l_orderkey = read_column<int32_t>(table_dir + "/l_orderkey.bin", 59986052);
    auto l_returnflag = read_column<uint8_t>(table_dir + "/l_returnflag.bin", 59986052);
    auto l_linestatus = read_column<uint8_t>(table_dir + "/l_linestatus.bin", 59986052);

    // Zone map for l_shipdate
    write_zone_map<int32_t>(table_dir + "/idx_l_shipdate.zonemap", l_shipdate, 131072);
    std::cout << "  Built zone map for l_shipdate\n";

    // Sorted index on (l_returnflag, l_linestatus)
    std::vector<std::pair<std::pair<uint8_t, uint8_t>, uint32_t>> pairs;
    for (size_t i = 0; i < l_returnflag.size(); ++i) {
        pairs.push_back({{l_returnflag[i], l_linestatus[i]}, (uint32_t)i});
    }
    std::sort(pairs.begin(), pairs.end());

    std::ofstream idx_file(table_dir + "/idx_l_returnflag_linestatus.idx", std::ios::binary);
    if (!idx_file) throw std::runtime_error("Cannot create index");

    uint32_t size = pairs.size();
    idx_file.write(reinterpret_cast<const char*>(&size), sizeof(uint32_t));
    for (const auto& [key, row_id] : pairs) {
        idx_file.write(reinterpret_cast<const char*>(&key.first), sizeof(uint8_t));
        idx_file.write(reinterpret_cast<const char*>(&key.second), sizeof(uint8_t));
        idx_file.write(reinterpret_cast<const char*>(&row_id), sizeof(uint32_t));
    }
    std::cout << "  Built sorted index for (l_returnflag, l_linestatus)\n";

    // Sorted index on l_orderkey for join
    write_sorted_index<int32_t>(table_dir + "/idx_l_orderkey.idx", l_orderkey);
    std::cout << "  Built sorted index for l_orderkey\n";
}

// Build orders indexes
void build_orders_indexes(const std::string& gendb_dir) {
    std::string table_dir = gendb_dir + "/orders";
    std::cout << "Building orders indexes...\n";

    // Read o_custkey and o_orderdate columns
    auto o_custkey = read_column<int32_t>(table_dir + "/o_custkey.bin", 15000000);
    auto o_orderdate = read_column<int32_t>(table_dir + "/o_orderdate.bin", 15000000);

    // Sorted index on o_custkey for join
    write_sorted_index<int32_t>(table_dir + "/idx_o_custkey.idx", o_custkey);
    std::cout << "  Built sorted index for o_custkey\n";

    // Zone map for o_orderdate
    write_zone_map<int32_t>(table_dir + "/idx_o_orderdate.zonemap", o_orderdate, 131072);
    std::cout << "  Built zone map for o_orderdate\n";
}

// Build customer indexes
void build_customer_indexes(const std::string& gendb_dir) {
    std::string table_dir = gendb_dir + "/customer";
    std::cout << "Building customer indexes...\n";

    // Read c_mktsegment column
    auto c_mktsegment = read_column<uint8_t>(table_dir + "/c_mktsegment.bin", 1500000);

    // Hash index on c_mktsegment
    write_string_hash_index(table_dir + "/idx_c_mktsegment.hash", c_mktsegment);
    std::cout << "  Built hash index for c_mktsegment\n";
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    auto start = high_resolution_clock::now();

    std::cout << "Building indexes...\n";

    try {
        build_lineitem_indexes(gendb_dir);
        build_orders_indexes(gendb_dir);
        build_customer_indexes(gendb_dir);

        auto end = high_resolution_clock::now();
        auto duration = duration_cast<seconds>(end - start);

        std::cout << "Index building complete in " << duration.count() << " seconds\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
