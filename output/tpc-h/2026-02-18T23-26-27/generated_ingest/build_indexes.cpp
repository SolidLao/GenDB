#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <omp.h>
#include <cmath>

namespace fs = std::filesystem;

// Multiply-shift hash function for int32_t
uint32_t hash_int32(int32_t key, uint32_t table_size_bits) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    return (h >> (64 - table_size_bits));
}

// Load int32_t column from binary file using mmap
std::vector<int32_t> load_int32_column(const std::string& filename, size_t num_rows) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t expected_size = num_rows * sizeof(int32_t);

    if (file_size != expected_size) {
        std::cerr << "File size mismatch for " << filename << ": " << file_size << " vs " << expected_size << std::endl;
    }

    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return {};
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    std::vector<int32_t> result(data, data + num_rows);

    munmap((void*)data, file_size);
    close(fd);

    return result;
}

// Load int64_t column from binary file using mmap
std::vector<int64_t> load_int64_column(const std::string& filename, size_t num_rows) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t expected_size = num_rows * sizeof(int64_t);

    if (file_size != expected_size) {
        std::cerr << "File size mismatch for " << filename << ": " << file_size << " vs " << expected_size << std::endl;
    }

    const int64_t* data = (const int64_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return {};
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    std::vector<int64_t> result(data, data + num_rows);

    munmap((void*)data, file_size);
    close(fd);

    return result;
}

// Build zone map (min/max per block) for int32_t column
void build_int32_zonemap(const std::vector<int32_t>& column, const std::string& output_file, size_t block_size) {
    std::vector<int32_t> min_vals;
    std::vector<int32_t> max_vals;
    std::vector<uint32_t> block_sizes;

    for (size_t i = 0; i < column.size(); i += block_size) {
        size_t end = std::min(i + block_size, column.size());
        int32_t min_val = column[i];
        int32_t max_val = column[i];

        for (size_t j = i + 1; j < end; ++j) {
            min_val = std::min(min_val, column[j]);
            max_val = std::max(max_val, column[j]);
        }

        min_vals.push_back(min_val);
        max_vals.push_back(max_val);
        block_sizes.push_back(end - i);
    }

    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_blocks = min_vals.size();
    out.write((const char*)&num_blocks, sizeof(uint32_t));

    for (uint32_t i = 0; i < num_blocks; ++i) {
        out.write((const char*)&min_vals[i], sizeof(int32_t));
        out.write((const char*)&max_vals[i], sizeof(int32_t));
        out.write((const char*)&block_sizes[i], sizeof(uint32_t));
    }

    out.close();
    std::cout << "Built zone map: " << output_file << " (" << num_blocks << " blocks)" << std::endl;
}

// Build zone map for int64_t column
void build_int64_zonemap(const std::vector<int64_t>& column, const std::string& output_file, size_t block_size) {
    std::vector<int64_t> min_vals;
    std::vector<int64_t> max_vals;
    std::vector<uint32_t> block_sizes;

    for (size_t i = 0; i < column.size(); i += block_size) {
        size_t end = std::min(i + block_size, column.size());
        int64_t min_val = column[i];
        int64_t max_val = column[i];

        for (size_t j = i + 1; j < end; ++j) {
            min_val = std::min(min_val, column[j]);
            max_val = std::max(max_val, column[j]);
        }

        min_vals.push_back(min_val);
        max_vals.push_back(max_val);
        block_sizes.push_back(end - i);
    }

    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_blocks = min_vals.size();
    out.write((const char*)&num_blocks, sizeof(uint32_t));

    for (uint32_t i = 0; i < num_blocks; ++i) {
        out.write((const char*)&min_vals[i], sizeof(int64_t));
        out.write((const char*)&max_vals[i], sizeof(int64_t));
        out.write((const char*)&block_sizes[i], sizeof(uint32_t));
    }

    out.close();
    std::cout << "Built zone map: " << output_file << " (" << num_blocks << " blocks)" << std::endl;
}

// Build hash multi-value index for int32_t column using sort-based grouping
void build_int32_hash_index(const std::vector<int32_t>& column, const std::string& output_file) {
    if (column.empty()) return;

    // Create position array
    std::vector<uint32_t> positions(column.size());
    for (size_t i = 0; i < column.size(); ++i) {
        positions[i] = i;
    }

    // Sort positions by key value
    std::sort(positions.begin(), positions.end(),
        [&column](uint32_t a, uint32_t b) { return column[a] < column[b]; });

    // Find unique keys and their boundaries
    std::vector<int32_t> unique_keys;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> counts;

    uint32_t current_offset = 0;
    int32_t current_key = column[positions[0]];

    for (size_t i = 0; i < column.size(); ++i) {
        int32_t key = column[positions[i]];
        if (key != current_key) {
            counts.push_back(i - current_offset);
            current_key = key;
            unique_keys.push_back(key);
            offsets.push_back(i);
            current_offset = i;
        }
    }
    counts.push_back(column.size() - current_offset);
    unique_keys.push_back(current_key);

    // Determine hash table size (power of 2, with 0.5 load factor)
    uint32_t table_size_bits = 1;
    uint32_t table_size = 2;
    while (table_size < unique_keys.size() * 2) {
        table_size_bits++;
        table_size *= 2;
    }

    // Build hash table using open addressing
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
        bool occupied = false;
    };

    std::vector<HashEntry> hash_table(table_size);

    for (size_t i = 0; i < unique_keys.size(); ++i) {
        int32_t key = unique_keys[i];
        uint32_t hash_pos = hash_int32(key, table_size_bits);

        // Linear probing
        while (hash_table[hash_pos].occupied) {
            hash_pos = (hash_pos + 1) % table_size;
        }

        hash_table[hash_pos].key = key;
        hash_table[hash_pos].offset = offsets[i];
        hash_table[hash_pos].count = counts[i];
        hash_table[hash_pos].occupied = true;
    }

    // Write index file
    std::ofstream out(output_file, std::ios::binary);

    uint32_t num_unique = unique_keys.size();
    out.write((const char*)&num_unique, sizeof(uint32_t));
    out.write((const char*)&table_size, sizeof(uint32_t));

    // Write hash table
    for (uint32_t i = 0; i < table_size; ++i) {
        uint8_t occupied = hash_table[i].occupied ? 1 : 0;
        out.write((const char*)&occupied, sizeof(uint8_t));
        if (hash_table[i].occupied) {
            out.write((const char*)&hash_table[i].key, sizeof(int32_t));
            out.write((const char*)&hash_table[i].offset, sizeof(uint32_t));
            out.write((const char*)&hash_table[i].count, sizeof(uint32_t));
        }
    }

    // Write positions array
    uint32_t pos_count = positions.size();
    out.write((const char*)&pos_count, sizeof(uint32_t));
    for (uint32_t pos : positions) {
        out.write((const char*)&pos, sizeof(uint32_t));
    }

    out.close();

    std::cout << "Built hash index: " << output_file << " (" << num_unique << " unique keys, "
              << table_size << " table size)" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: build_indexes <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    // Define indexes to build
    struct IndexSpec {
        std::string table;
        std::string column;
        std::string index_name;
        std::string index_type;
        std::string data_type;  // "int32" or "int64"
        size_t num_rows;
    };

    std::vector<IndexSpec> indexes = {
        {"lineitem", "l_orderkey", "lineitem_l_orderkey", "hash_multi_value", "int32", 59986052},
        {"lineitem", "l_shipdate", "lineitem_l_shipdate_zonemap", "zone_map", "int32", 59986052},
        {"orders", "o_custkey", "orders_o_custkey", "hash_multi_value", "int32", 15000000},
        {"orders", "o_orderdate", "orders_o_orderdate_zonemap", "zone_map", "int32", 15000000},
        {"customer", "c_custkey", "customer_c_custkey", "hash_multi_value", "int32", 1500000},
        {"customer", "c_mktsegment", "customer_c_mktsegment_zonemap", "zone_map", "int32", 1500000},
        {"part", "p_partkey", "part_p_partkey", "hash_multi_value", "int32", 2000000},
        {"partsupp", "ps_partkey", "partsupp_ps_partkey", "hash_multi_value", "int32", 8000000},
        {"partsupp", "ps_suppkey", "partsupp_ps_suppkey", "hash_multi_value", "int32", 8000000},
    };

    fs::create_directories(gendb_dir + "/indexes");

    for (const auto& spec : indexes) {
        std::string col_file = gendb_dir + "/" + spec.table + "/" + spec.column + ".bin";
        std::string index_file = gendb_dir + "/indexes/" + spec.index_name + ".bin";

        if (!fs::exists(col_file)) {
            std::cerr << "Column file not found: " << col_file << std::endl;
            continue;
        }

        if (spec.index_type == "hash_multi_value") {
            std::cout << "Building hash index: " << spec.index_name << std::endl;
            if (spec.data_type == "int32") {
                auto column = load_int32_column(col_file, spec.num_rows);
                if (!column.empty()) {
                    build_int32_hash_index(column, index_file);
                }
            }
        } else if (spec.index_type == "zone_map") {
            std::cout << "Building zone map: " << spec.index_name << std::endl;
            if (spec.data_type == "int32") {
                auto column = load_int32_column(col_file, spec.num_rows);
                if (!column.empty()) {
                    build_int32_zonemap(column, index_file, 100000);
                }
            }
        }
    }

    std::cout << "Index building complete" << std::endl;
    return 0;
}
