#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <filesystem>
#include <cmath>
#include <sstream>

namespace fs = std::filesystem;

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    size_t row_count;
    size_t null_count;
};

// Hash table entry for hash indexes
struct HashTableEntry {
    int32_t key;
    std::vector<size_t> row_ids;
};

// Read a binary column of int32_t
std::vector<int32_t> read_int_column(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Error reading " << path << "\n";
        return {};
    }

    std::vector<int32_t> data;
    int32_t val;
    while (f.read(reinterpret_cast<char*>(&val), sizeof(int32_t))) {
        data.push_back(val);
    }
    return data;
}

// Read a binary column of int64_t
std::vector<int64_t> read_decimal_column(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Error reading " << path << "\n";
        return {};
    }

    std::vector<int64_t> data;
    int64_t val;
    while (f.read(reinterpret_cast<char*>(&val), sizeof(int64_t))) {
        data.push_back(val);
    }
    return data;
}

// Build zone maps for a column (dividing into blocks)
std::vector<ZoneMap> build_zone_maps(const std::vector<int32_t>& data, size_t block_size) {
    std::vector<ZoneMap> zone_maps;

    for (size_t i = 0; i < data.size(); i += block_size) {
        size_t block_end = std::min(i + block_size, data.size());
        ZoneMap zm;
        zm.min_val = data[i];
        zm.max_val = data[i];
        zm.null_count = 0;

        for (size_t j = i; j < block_end; ++j) {
            zm.min_val = std::min(zm.min_val, data[j]);
            zm.max_val = std::max(zm.max_val, data[j]);
        }

        zm.row_count = block_end - i;
        zone_maps.push_back(zm);
    }

    return zone_maps;
}

// Build zone maps for decimal column
std::vector<ZoneMap> build_zone_maps_decimal(const std::vector<int64_t>& data, size_t block_size) {
    std::vector<ZoneMap> zone_maps;

    for (size_t i = 0; i < data.size(); i += block_size) {
        size_t block_end = std::min(i + block_size, data.size());
        ZoneMap zm;
        zm.min_val = static_cast<int32_t>(data[i]);
        zm.max_val = static_cast<int32_t>(data[i]);
        zm.null_count = 0;

        for (size_t j = i; j < block_end; ++j) {
            zm.min_val = std::min(zm.min_val, static_cast<int32_t>(data[j]));
            zm.max_val = std::max(zm.max_val, static_cast<int32_t>(data[j]));
        }

        zm.row_count = block_end - i;
        zone_maps.push_back(zm);
    }

    return zone_maps;
}

// Build hash index for a column
std::map<int32_t, std::vector<size_t>> build_hash_index(const std::vector<int32_t>& data) {
    std::map<int32_t, std::vector<size_t>> hash_index;

    for (size_t i = 0; i < data.size(); ++i) {
        hash_index[data[i]].push_back(i);
    }

    return hash_index;
}

// Save zone maps to JSON
void save_zone_maps(const std::string& path, const std::vector<ZoneMap>& zone_maps) {
    std::ofstream f(path);
    f << "[\n";
    for (size_t i = 0; i < zone_maps.size(); ++i) {
        const auto& zm = zone_maps[i];
        f << "  {\"min\": " << zm.min_val << ", \"max\": " << zm.max_val
          << ", \"row_count\": " << zm.row_count << ", \"null_count\": " << zm.null_count << "}";
        if (i < zone_maps.size() - 1) f << ",";
        f << "\n";
    }
    f << "]\n";
}

// Save hash index to JSON
void save_hash_index(const std::string& path, const std::map<int32_t, std::vector<size_t>>& index) {
    std::ofstream f(path);
    f << "{\n";
    size_t count = 0;
    for (const auto& [key, row_ids] : index) {
        f << "  \"" << key << "\": [";
        for (size_t i = 0; i < row_ids.size(); ++i) {
            f << row_ids[i];
            if (i < row_ids.size() - 1) f << ", ";
        }
        f << "]";
        if (++count < index.size()) f << ",";
        f << "\n";
    }
    f << "}\n";
}

// Build permutation (sort indices without sorting full data)
std::vector<size_t> build_permutation(const std::vector<int32_t>& data) {
    std::vector<size_t> perm(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        perm[i] = i;
    }

    // Sort permutation by data values
    std::sort(perm.begin(), perm.end(), [&data](size_t i, size_t j) {
        return data[i] < data[j];
    });

    return perm;
}

// Save permutation to binary file
void save_permutation(const std::string& path, const std::vector<size_t>& perm) {
    std::ofstream f(path, std::ios::binary);
    for (size_t idx : perm) {
        uint32_t idx32 = static_cast<uint32_t>(idx);
        f.write(reinterpret_cast<const char*>(&idx32), sizeof(uint32_t));
    }
}

// Main function: build all indexes
void build_indexes_for_table(const std::string& gendb_dir, const std::string& table_name,
                             const std::vector<std::string>& zone_map_cols,
                             const std::vector<std::string>& hash_cols) {
    std::string table_dir = gendb_dir + "/" + table_name;

    std::cout << "Building indexes for " << table_name << "...\n";

    // Build zone maps
    for (const auto& col_name : zone_map_cols) {
        std::string col_path = table_dir + "/" + col_name + ".bin";
        auto col_data = read_int_column(col_path);

        if (!col_data.empty()) {
            auto zone_maps = build_zone_maps(col_data, 100000);
            std::string zm_path = table_dir + "/" + col_name + "_zone_map.json";
            save_zone_maps(zm_path, zone_maps);
            std::cout << "  Built zone maps for " << col_name << " (" << zone_maps.size() << " blocks)\n";
        }
    }

    // Build hash indexes
    for (const auto& col_name : hash_cols) {
        std::string col_path = table_dir + "/" + col_name + ".bin";
        auto col_data = read_int_column(col_path);

        if (!col_data.empty()) {
            auto hash_index = build_hash_index(col_data);
            std::string hi_path = table_dir + "/" + col_name + "_hash_index.json";
            save_hash_index(hi_path, hash_index);
            std::cout << "  Built hash index for " << col_name << " (" << hash_index.size() << " keys)\n";
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "Building indexes for TPC-H SF10 in " << gendb_dir << "\n";

    // lineitem: zone maps on l_shipdate and l_discount (dates are stored as int32_t)
    build_indexes_for_table(gendb_dir, "lineitem", {"l_shipdate"}, {});

    // orders: hash indexes on o_orderkey and o_custkey
    build_indexes_for_table(gendb_dir, "orders", {}, {"o_orderkey", "o_custkey"});

    // customer: hash index on c_custkey
    build_indexes_for_table(gendb_dir, "customer", {}, {"c_custkey"});

    // supplier: hash index on s_suppkey
    build_indexes_for_table(gendb_dir, "supplier", {}, {"s_suppkey"});

    // part: hash index on p_partkey
    build_indexes_for_table(gendb_dir, "part", {}, {"p_partkey"});

    std::cout << "\nIndex building complete!\n";
    return 0;
}
