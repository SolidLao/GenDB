#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <filesystem>
#include <omp.h>
#include <cassert>

namespace fs = std::filesystem;

constexpr size_t BLOCK_SIZE = 100000;

// Zone map structure: min and max per block
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
};

struct ZoneMapDecimal {
    int64_t min_val;
    int64_t max_val;
};

// Read binary column file via mmap
template <typename T>
std::pair<void*, size_t> mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return {nullptr, 0};
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    void* data = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        close(fd);
        return {nullptr, 0};
    }

    madvise(data, file_size, MADV_SEQUENTIAL);
    close(fd);
    return {data, file_size};
}

void munmap_file(void* data, size_t size) {
    if (data) munmap(data, size);
}

// Write binary data
void write_binary(const std::string& path, const void* data, size_t size) {
    std::ofstream f(path, std::ios::binary);
    f.write(static_cast<const char*>(data), size);
}

// Zone maps for l_shipdate (int32_t)
void build_zone_map_int32(const std::string& col_path, const std::string& out_path, size_t num_rows) {
    std::cout << "Building zone map for " << col_path << std::endl;

    auto [data, file_size] = mmap_file<int32_t>(col_path);
    if (!data) return;

    const int32_t* col_data = static_cast<const int32_t*>(data);
    size_t num_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<ZoneMap> zone_maps(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start = b * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, num_rows);

        int32_t min_v = col_data[start];
        int32_t max_v = col_data[start];

        for (size_t i = start; i < end; ++i) {
            min_v = std::min(min_v, col_data[i]);
            max_v = std::max(max_v, col_data[i]);
        }

        zone_maps[b].min_val = min_v;
        zone_maps[b].max_val = max_v;
    }

    write_binary(out_path, zone_maps.data(), zone_maps.size() * sizeof(ZoneMap));
    munmap_file(data, file_size);

    std::cout << "  Zone map written with " << num_blocks << " blocks" << std::endl;
}

// Zone maps for l_discount, l_quantity (int64_t decimals)
void build_zone_map_int64(const std::string& col_path, const std::string& out_path, size_t num_rows) {
    std::cout << "Building zone map for " << col_path << std::endl;

    auto [data, file_size] = mmap_file<int64_t>(col_path);
    if (!data) return;

    const int64_t* col_data = static_cast<const int64_t*>(data);
    size_t num_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<ZoneMapDecimal> zone_maps(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start = b * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, num_rows);

        int64_t min_v = col_data[start];
        int64_t max_v = col_data[start];

        for (size_t i = start; i < end; ++i) {
            min_v = std::min(min_v, col_data[i]);
            max_v = std::max(max_v, col_data[i]);
        }

        zone_maps[b].min_val = min_v;
        zone_maps[b].max_val = max_v;
    }

    write_binary(out_path, zone_maps.data(), zone_maps.size() * sizeof(ZoneMapDecimal));
    munmap_file(data, file_size);

    std::cout << "  Zone map written with " << num_blocks << " blocks" << std::endl;
}

// Hash index for l_orderkey (multi-value: one key maps to multiple rows)
void build_hash_index_l_orderkey(const std::string& col_path, const std::string& out_path, size_t num_rows) {
    std::cout << "Building hash index for l_orderkey" << std::endl;

    auto [data, file_size] = mmap_file<int32_t>(col_path);
    if (!data) return;

    const int32_t* col_data = static_cast<const int32_t*>(data);

    // Count unique keys
    std::unordered_map<int32_t, std::vector<uint32_t>> key_positions;
    for (size_t i = 0; i < num_rows; ++i) {
        key_positions[col_data[i]].push_back(static_cast<uint32_t>(i));
    }

    size_t num_unique = key_positions.size();
    std::cout << "  Unique keys: " << num_unique << std::endl;

    // Build hash table: power-of-2 size with 0.6 load factor
    size_t hash_table_size = 1;
    while (hash_table_size < num_unique / 0.6) hash_table_size *= 2;

    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> hash_table(hash_table_size, {0, 0, 0});

    // Flatten positions array
    std::vector<uint32_t> positions_array;
    for (const auto& [key, positions] : key_positions) {
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
    }

    // Insert into hash table (using multiply-shift hash)
    uint32_t offset = 0;
    for (const auto& [key, positions] : key_positions) {
        uint64_t hash = static_cast<uint64_t>(key) * 0x9E3779B97F4A7C15ULL;
        uint32_t slot = (hash >> 32) & (hash_table_size - 1);

        // Linear probing
        while (hash_table[slot].count != 0) {
            slot = (slot + 1) & (hash_table_size - 1);
        }

        hash_table[slot].key = key;
        hash_table[slot].offset = offset;
        hash_table[slot].count = static_cast<uint32_t>(positions.size());
        offset += hash_table[slot].count;
    }

    // Write index: [num_unique][hash_table_size][hash_table][positions_array]
    std::ofstream out(out_path, std::ios::binary);
    uint32_t nu = static_cast<uint32_t>(num_unique);
    uint32_t hts = static_cast<uint32_t>(hash_table_size);
    out.write(reinterpret_cast<const char*>(&nu), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&hts), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(hash_table.data()), hash_table.size() * sizeof(HashEntry));
    out.write(reinterpret_cast<const char*>(positions_array.data()), positions_array.size() * sizeof(uint32_t));
    out.close();

    munmap_file(data, file_size);

    std::cout << "  Hash index written with " << num_unique << " unique keys, table size " << hash_table_size << std::endl;
}

// Hash index for o_custkey
void build_hash_index_o_custkey(const std::string& col_path, const std::string& out_path, size_t num_rows) {
    std::cout << "Building hash index for o_custkey" << std::endl;

    auto [data, file_size] = mmap_file<int32_t>(col_path);
    if (!data) return;

    const int32_t* col_data = static_cast<const int32_t*>(data);

    // Count unique keys
    std::unordered_map<int32_t, std::vector<uint32_t>> key_positions;
    for (size_t i = 0; i < num_rows; ++i) {
        key_positions[col_data[i]].push_back(static_cast<uint32_t>(i));
    }

    size_t num_unique = key_positions.size();
    std::cout << "  Unique keys: " << num_unique << std::endl;

    // Build hash table: power-of-2 size with 0.6 load factor
    size_t hash_table_size = 1;
    while (hash_table_size < num_unique / 0.6) hash_table_size *= 2;

    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> hash_table(hash_table_size, {0, 0, 0});

    // Flatten positions array
    std::vector<uint32_t> positions_array;
    for (const auto& [key, positions] : key_positions) {
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
    }

    // Insert into hash table
    uint32_t offset = 0;
    for (const auto& [key, positions] : key_positions) {
        uint64_t hash = static_cast<uint64_t>(key) * 0x9E3779B97F4A7C15ULL;
        uint32_t slot = (hash >> 32) & (hash_table_size - 1);

        // Linear probing
        while (hash_table[slot].count != 0) {
            slot = (slot + 1) & (hash_table_size - 1);
        }

        hash_table[slot].key = key;
        hash_table[slot].offset = offset;
        hash_table[slot].count = static_cast<uint32_t>(positions.size());
        offset += hash_table[slot].count;
    }

    // Write index
    std::ofstream out(out_path, std::ios::binary);
    uint32_t nu = static_cast<uint32_t>(num_unique);
    uint32_t hts = static_cast<uint32_t>(hash_table_size);
    out.write(reinterpret_cast<const char*>(&nu), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&hts), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(hash_table.data()), hash_table.size() * sizeof(HashEntry));
    out.write(reinterpret_cast<const char*>(positions_array.data()), positions_array.size() * sizeof(uint32_t));
    out.close();

    munmap_file(data, file_size);

    std::cout << "  Hash index written with " << num_unique << " unique keys, table size " << hash_table_size << std::endl;
}

// Hash index for c_custkey
void build_hash_index_c_custkey(const std::string& col_path, const std::string& out_path, size_t num_rows) {
    std::cout << "Building hash index for c_custkey" << std::endl;

    auto [data, file_size] = mmap_file<int32_t>(col_path);
    if (!data) return;

    const int32_t* col_data = static_cast<const int32_t*>(data);

    // Count unique keys (should be exactly num_rows for customer primary key)
    std::unordered_map<int32_t, std::vector<uint32_t>> key_positions;
    for (size_t i = 0; i < num_rows; ++i) {
        key_positions[col_data[i]].push_back(static_cast<uint32_t>(i));
    }

    size_t num_unique = key_positions.size();
    std::cout << "  Unique keys: " << num_unique << std::endl;

    // Build hash table: power-of-2 size with 0.6 load factor
    size_t hash_table_size = 1;
    while (hash_table_size < num_unique / 0.6) hash_table_size *= 2;

    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> hash_table(hash_table_size, {0, 0, 0});

    // Flatten positions array
    std::vector<uint32_t> positions_array;
    for (const auto& [key, positions] : key_positions) {
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
    }

    // Insert into hash table
    uint32_t offset = 0;
    for (const auto& [key, positions] : key_positions) {
        uint64_t hash = static_cast<uint64_t>(key) * 0x9E3779B97F4A7C15ULL;
        uint32_t slot = (hash >> 32) & (hash_table_size - 1);

        // Linear probing
        while (hash_table[slot].count != 0) {
            slot = (slot + 1) & (hash_table_size - 1);
        }

        hash_table[slot].key = key;
        hash_table[slot].offset = offset;
        hash_table[slot].count = static_cast<uint32_t>(positions.size());
        offset += hash_table[slot].count;
    }

    // Write index
    std::ofstream out(out_path, std::ios::binary);
    uint32_t nu = static_cast<uint32_t>(num_unique);
    uint32_t hts = static_cast<uint32_t>(hash_table_size);
    out.write(reinterpret_cast<const char*>(&nu), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&hts), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(hash_table.data()), hash_table.size() * sizeof(HashEntry));
    out.write(reinterpret_cast<const char*>(positions_array.data()), positions_array.size() * sizeof(uint32_t));
    out.close();

    munmap_file(data, file_size);

    std::cout << "  Hash index written with " << num_unique << " unique keys, table size " << hash_table_size << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    fs::create_directories(gendb_dir + "/indexes");

    std::cout << "Building indexes for TPC-H SF10..." << std::endl;

    // Lineitem indexes
    std::cout << "\n=== Lineitem Indexes ===" << std::endl;
    build_zone_map_int32(gendb_dir + "/lineitem/l_shipdate.bin", gendb_dir + "/indexes/zone_map_l_shipdate.bin", 59986052);
    build_zone_map_int64(gendb_dir + "/lineitem/l_discount.bin", gendb_dir + "/indexes/zone_map_l_discount.bin", 59986052);
    build_zone_map_int64(gendb_dir + "/lineitem/l_quantity.bin", gendb_dir + "/indexes/zone_map_l_quantity.bin", 59986052);
    build_hash_index_l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin", gendb_dir + "/indexes/hash_l_orderkey.bin", 59986052);

    // Orders indexes
    std::cout << "\n=== Orders Indexes ===" << std::endl;
    build_zone_map_int32(gendb_dir + "/orders/o_orderdate.bin", gendb_dir + "/indexes/zone_map_o_orderdate.bin", 15000000);
    build_hash_index_o_custkey(gendb_dir + "/orders/o_custkey.bin", gendb_dir + "/indexes/hash_o_custkey.bin", 15000000);

    // Customer indexes
    std::cout << "\n=== Customer Indexes ===" << std::endl;
    build_hash_index_c_custkey(gendb_dir + "/customer/c_custkey.bin", gendb_dir + "/indexes/hash_c_custkey.bin", 1500000);

    std::cout << "\nIndex building complete!" << std::endl;
    return 0;
}
