#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cmath>

namespace fs = std::filesystem;

constexpr size_t BLOCK_SIZE = 256000;
constexpr float HASH_LOAD_FACTOR = 0.6f;

// ============================================================================
// Utility Functions
// ============================================================================

// Simple multiply-shift hash function for 32-bit integers
uint32_t hash_int32(int32_t key, uint32_t table_size) {
    uint64_t k = static_cast<uint64_t>(key);
    k = (k ^ (k >> 33)) * 0xFF51AFD7ED558CCDULL;
    return (k >> 32) % table_size;
}

// File I/O helpers
std::vector<char> read_entire_file(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Cannot open " << filename << std::endl;
        return {};
    }
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<char> buffer(size);
    if (!file.read(buffer.data(), size)) {
        std::cerr << "Cannot read " << filename << std::endl;
        return {};
    }
    return buffer;
}

// ============================================================================
// Zone Map Index
// ============================================================================

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
    uint32_t null_count;
};

void build_zone_map(const std::string& column_file, const std::string& output_file,
                    size_t num_rows) {
    std::cout << "Building zone map: " << output_file << std::endl;

    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "Cannot mmap " << column_file << std::endl;
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const int32_t* data = static_cast<const int32_t*>(mapped);
    size_t num_values = num_rows;
    size_t num_blocks = (num_values + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<ZoneMapEntry> zone_maps(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t block = 0; block < num_blocks; ++block) {
        size_t start = block * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, num_values);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start; i < end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_maps[block] = {min_val, max_val, static_cast<uint32_t>(end - start), 0};
    }

    // Write zone map file
    std::ofstream outf(output_file, std::ios::binary);
    uint32_t num_blocks_write = zone_maps.size();
    outf.write(reinterpret_cast<const char*>(&num_blocks_write), sizeof(num_blocks_write));

    for (const auto& zm : zone_maps) {
        outf.write(reinterpret_cast<const char*>(&zm.min_val), sizeof(zm.min_val));
        outf.write(reinterpret_cast<const char*>(&zm.max_val), sizeof(zm.max_val));
        outf.write(reinterpret_cast<const char*>(&zm.count), sizeof(zm.count));
        outf.write(reinterpret_cast<const char*>(&zm.null_count), sizeof(zm.null_count));
    }
    outf.close();

    munmap(mapped, file_size);
    close(fd);

    std::cout << "  Zone map written: " << num_blocks_write << " blocks" << std::endl;
}

// ============================================================================
// Hash Index (Multi-Value for Join Columns)
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;  // Offset into positions array
    uint32_t count;   // Number of positions
};

void build_hash_index_multival(const std::string& column_file, const std::string& output_file,
                               size_t num_rows, bool is_single_value = false) {
    std::cout << "Building hash index (multi-value): " << output_file << std::endl;

    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "Cannot mmap " << column_file << std::endl;
        close(fd);
        return;
    }

    const int32_t* data = static_cast<const int32_t*>(mapped);

    // Group positions by key
    std::map<int32_t, std::vector<uint32_t>> key_positions;

    for (uint32_t i = 0; i < num_rows; ++i) {
        key_positions[data[i]].push_back(i);
    }

    // Build hash table
    uint32_t num_unique = key_positions.size();
    uint32_t table_size = static_cast<uint32_t>(num_unique / HASH_LOAD_FACTOR);
    if (table_size < 16) table_size = 16;

    // Round up to power of 2
    uint32_t pow2_size = 1;
    while (pow2_size < table_size) pow2_size *= 2;
    table_size = pow2_size;

    std::vector<HashIndexEntry> hash_table(table_size);
    std::fill(hash_table.begin(), hash_table.end(), HashIndexEntry{-1, 0, 0});

    std::vector<uint32_t> positions_array;
    uint32_t pos_offset = 0;

    // Insert into hash table
    for (auto& [key, positions] : key_positions) {
        uint32_t hash = hash_int32(key, table_size);

        // Linear probing
        while (hash_table[hash].key != -1) {
            hash = (hash + 1) % table_size;
        }

        hash_table[hash].key = key;
        hash_table[hash].offset = pos_offset;
        hash_table[hash].count = positions.size();

        // Add positions to array
        for (uint32_t pos : positions) {
            positions_array.push_back(pos);
        }
        pos_offset += positions.size();
    }

    // Write hash index file
    std::ofstream outf(output_file, std::ios::binary);

    // Header
    outf.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));
    outf.write(reinterpret_cast<const char*>(&table_size), sizeof(table_size));

    // Hash table entries
    for (const auto& entry : hash_table) {
        outf.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        outf.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        outf.write(reinterpret_cast<const char*>(&entry.count), sizeof(entry.count));
    }

    // Positions array
    uint32_t pos_count = positions_array.size();
    outf.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    for (uint32_t pos : positions_array) {
        outf.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
    }

    outf.close();

    munmap(mapped, file_size);
    close(fd);

    std::cout << "  Hash index written: " << num_unique << " unique keys, "
              << "table_size=" << table_size << ", pos_array=" << pos_count << " entries" << std::endl;
}

// ============================================================================
// Build Indexes for Each Table
// ============================================================================

void build_lineitem_indexes(const std::string& gendb_dir, size_t num_rows) {
    std::cout << "\n=== Building LINEITEM indexes ===" << std::endl;

    std::string table_dir = gendb_dir + "/tables/lineitem";
    std::string index_dir = gendb_dir + "/indexes";
    fs::create_directories(index_dir);

    // Zone map on l_shipdate (high cardinality filter column)
    build_zone_map(table_dir + "/l_shipdate.bin",
                   index_dir + "/zone_map_l_shipdate.bin", num_rows);

    // Hash index on l_orderkey (join column, multi-value)
    build_hash_index_multival(table_dir + "/l_orderkey.bin",
                              index_dir + "/hash_l_orderkey.bin", num_rows, false);
}

void build_orders_indexes(const std::string& gendb_dir, size_t num_rows) {
    std::cout << "\n=== Building ORDERS indexes ===" << std::endl;

    std::string table_dir = gendb_dir + "/tables/orders";
    std::string index_dir = gendb_dir + "/indexes";
    fs::create_directories(index_dir);

    // Zone map on o_orderdate (filter column)
    build_zone_map(table_dir + "/o_orderdate.bin",
                   index_dir + "/zone_map_o_orderdate.bin", num_rows);

    // Hash index on o_custkey (join column, multi-value)
    build_hash_index_multival(table_dir + "/o_custkey.bin",
                              index_dir + "/hash_o_custkey.bin", num_rows, false);

    // Hash index on o_orderkey (single-value lookup)
    build_hash_index_multival(table_dir + "/o_orderkey.bin",
                              index_dir + "/hash_o_orderkey.bin", num_rows, true);
}

void build_customer_indexes(const std::string& gendb_dir, size_t num_rows) {
    std::cout << "\n=== Building CUSTOMER indexes ===" << std::endl;

    std::string table_dir = gendb_dir + "/tables/customer";
    std::string index_dir = gendb_dir + "/indexes";
    fs::create_directories(index_dir);

    // Hash index on c_custkey (single-value lookup)
    build_hash_index_multival(table_dir + "/c_custkey.bin",
                              index_dir + "/hash_c_custkey.bin", num_rows, true);

    // Zone map on c_mktsegment would be useful but it's dictionary-encoded
    // For now, skip dictionary column zone maps
}

void build_part_indexes(const std::string& gendb_dir, size_t num_rows) {
    std::cout << "\n=== Building PART indexes ===" << std::endl;

    std::string table_dir = gendb_dir + "/tables/part";
    std::string index_dir = gendb_dir + "/indexes";
    fs::create_directories(index_dir);

    // Hash index on p_partkey (single-value lookup)
    build_hash_index_multival(table_dir + "/p_partkey.bin",
                              index_dir + "/hash_p_partkey.bin", num_rows, true);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "Building indexes for GenDB directory: " << gendb_dir << std::endl;

    // Build indexes for each table
    build_lineitem_indexes(gendb_dir, 59986052);
    build_orders_indexes(gendb_dir, 15000000);
    build_customer_indexes(gendb_dir, 1500000);
    build_part_indexes(gendb_dir, 2000000);

    std::cout << "\nIndex building completed!" << std::endl;

    return 0;
}
