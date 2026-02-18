#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <omp.h>

namespace fs = std::filesystem;

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
};

struct ZoneMapEntry64 {
    int64_t min_val;
    int64_t max_val;
    uint32_t count;
};

struct HashIndexHeader {
    uint32_t num_unique;
    uint32_t table_size;
};

// Hash function: multiply-shift for int32_t
uint32_t hash_int32(int32_t key, uint32_t mask) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

// Build zone map for int32_t column
void build_zone_map_int32(const std::string& col_file, uint32_t total_rows, uint32_t block_size, const std::string& out_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    int32_t* data = (int32_t*)mmap(nullptr, total_rows * sizeof(int32_t), PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << std::endl;
        close(fd);
        return;
    }

    madvise(data, total_rows * sizeof(int32_t), MADV_SEQUENTIAL);

    std::vector<ZoneMapEntry> zones;
    for (uint32_t block_start = 0; block_start < total_rows; block_start += block_size) {
        uint32_t block_end = std::min(block_start + block_size, total_rows);
        int32_t min_val = data[block_start];
        int32_t max_val = data[block_start];

        for (uint32_t i = block_start; i < block_end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zones.push_back({min_val, max_val, block_end - block_start});
    }

    munmap(data, total_rows * sizeof(int32_t));
    close(fd);

    // Write zone map
    std::ofstream f(out_file, std::ios::binary);
    uint32_t num_zones = zones.size();
    f.write((const char*)&num_zones, sizeof(uint32_t));
    for (const auto& zone : zones) {
        f.write((const char*)&zone.min_val, sizeof(int32_t));
        f.write((const char*)&zone.max_val, sizeof(int32_t));
        f.write((const char*)&zone.count, sizeof(uint32_t));
    }
    f.close();
    std::cout << "Zone map written: " << out_file << " (" << num_zones << " zones)" << std::endl;
}

// Build zone map for int64_t column
void build_zone_map_int64(const std::string& col_file, uint32_t total_rows, uint32_t block_size, const std::string& out_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    int64_t* data = (int64_t*)mmap(nullptr, total_rows * sizeof(int64_t), PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << std::endl;
        close(fd);
        return;
    }

    madvise(data, total_rows * sizeof(int64_t), MADV_SEQUENTIAL);

    std::vector<ZoneMapEntry64> zones;
    for (uint32_t block_start = 0; block_start < total_rows; block_start += block_size) {
        uint32_t block_end = std::min(block_start + block_size, total_rows);
        int64_t min_val = data[block_start];
        int64_t max_val = data[block_start];

        for (uint32_t i = block_start; i < block_end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zones.push_back({min_val, max_val, block_end - block_start});
    }

    munmap(data, total_rows * sizeof(int64_t));
    close(fd);

    // Write zone map
    std::ofstream f(out_file, std::ios::binary);
    uint32_t num_zones = zones.size();
    f.write((const char*)&num_zones, sizeof(uint32_t));
    for (const auto& zone : zones) {
        f.write((const char*)&zone.min_val, sizeof(int64_t));
        f.write((const char*)&zone.max_val, sizeof(int64_t));
        f.write((const char*)&zone.count, sizeof(uint32_t));
    }
    f.close();
    std::cout << "Zone map written: " << out_file << " (" << num_zones << " zones)" << std::endl;
}

// Build multi-value hash index for int32_t column
void build_hash_index_multi_value(const std::string& col_file, uint32_t total_rows, const std::string& out_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    int32_t* data = (int32_t*)mmap(nullptr, total_rows * sizeof(int32_t), PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << std::endl;
        close(fd);
        return;
    }

    madvise(data, total_rows * sizeof(int32_t), MADV_SEQUENTIAL);

    // Count unique values and build histogram
    std::unordered_map<int32_t, std::vector<uint32_t>> value_to_positions;
    #pragma omp parallel for
    for (uint32_t i = 0; i < total_rows; ++i) {
        #pragma omp critical
        {
            value_to_positions[data[i]].push_back(i);
        }
    }

    uint32_t num_unique = value_to_positions.size();
    uint32_t shift_size = 1 << (32 - __builtin_clz(num_unique * 2));
    uint32_t table_size = std::max((uint32_t)128, (uint32_t)shift_size);
    uint32_t mask = table_size - 1;

    // Build hash table: [key] -> {offset, count}
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> table(table_size);
    std::vector<uint32_t> positions_array;

    for (const auto& [key, positions] : value_to_positions) {
        for (uint32_t pos : positions) {
            positions_array.push_back(pos);
        }
    }

    uint32_t offset = 0;
    for (auto& [key, positions] : value_to_positions) {
        uint32_t hash = hash_int32(key, mask);
        uint32_t probe = 0;
        while (table[hash].key != 0 && probe < table_size) {
            hash = (hash + 1) & mask;
            probe++;
        }
        table[hash] = {key, offset, (uint32_t)positions.size()};
        offset += positions.size();
    }

    munmap(data, total_rows * sizeof(int32_t));
    close(fd);

    // Write hash index
    std::ofstream f(out_file, std::ios::binary);
    f.write((const char*)&num_unique, sizeof(uint32_t));
    f.write((const char*)&table_size, sizeof(uint32_t));

    for (uint32_t i = 0; i < table_size; ++i) {
        f.write((const char*)&table[i].key, sizeof(int32_t));
        f.write((const char*)&table[i].offset, sizeof(uint32_t));
        f.write((const char*)&table[i].count, sizeof(uint32_t));
    }

    for (uint32_t pos : positions_array) {
        f.write((const char*)&pos, sizeof(uint32_t));
    }

    f.close();
    std::cout << "Hash index (multi-value) written: " << out_file << " (unique=" << num_unique << ", table_size=" << table_size << ")" << std::endl;
}

// Build single-value hash index for int32_t column (pk/sk)
void build_hash_index_single(const std::string& col_file, uint32_t total_rows, const std::string& out_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    int32_t* data = (int32_t*)mmap(nullptr, total_rows * sizeof(int32_t), PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << std::endl;
        close(fd);
        return;
    }

    madvise(data, total_rows * sizeof(int32_t), MADV_SEQUENTIAL);

    // Build hash table: [key] -> position
    uint32_t shift_size_single = 1 << (32 - __builtin_clz(total_rows * 2));
    uint32_t table_size = shift_size_single;
    uint32_t mask = table_size - 1;

    struct HashEntry {
        int32_t key;
        uint32_t pos;
    };

    std::vector<HashEntry> table(table_size, {0, UINT32_MAX});

    for (uint32_t i = 0; i < total_rows; ++i) {
        int32_t key = data[i];
        uint32_t hash = hash_int32(key, mask);
        uint32_t probe = 0;

        while (table[hash].key != 0 && probe < table_size) {
            hash = (hash + 1) & mask;
            probe++;
        }
        table[hash] = {key, i};
    }

    munmap(data, total_rows * sizeof(int32_t));
    close(fd);

    // Write hash index
    std::ofstream f(out_file, std::ios::binary);
    f.write((const char*)&total_rows, sizeof(uint32_t));
    f.write((const char*)&table_size, sizeof(uint32_t));

    for (uint32_t i = 0; i < table_size; ++i) {
        f.write((const char*)&table[i].key, sizeof(int32_t));
        f.write((const char*)&table[i].pos, sizeof(uint32_t));
    }

    f.close();
    std::cout << "Hash index (single) written: " << out_file << std::endl;
}

struct IndexDefinition {
    std::string table_name;
    std::string index_name;
    std::string index_type;
    std::string column_name;
    std::string cpp_type;
    uint32_t total_rows;
    uint32_t block_size;
};

std::vector<IndexDefinition> get_index_definitions() {
    return {
        // lineitem indexes
        {"lineitem", "idx_lineitem_orderkey", "hash_multi_value", "l_orderkey", "int32_t", 59986052, 100000},
        {"lineitem", "idx_lineitem_shipdate_zone", "zone_map", "l_shipdate", "int32_t", 59986052, 100000},
        {"lineitem", "idx_lineitem_discount_zone", "zone_map", "l_discount", "int64_t", 59986052, 100000},
        {"lineitem", "idx_lineitem_quantity_zone", "zone_map", "l_quantity", "int64_t", 59986052, 100000},

        // orders indexes
        {"orders", "idx_orders_custkey", "hash_multi_value", "o_custkey", "int32_t", 15000000, 100000},
        {"orders", "idx_orders_orderdate_zone", "zone_map", "o_orderdate", "int32_t", 15000000, 100000},

        // customer indexes
        {"customer", "idx_customer_custkey", "hash_single", "c_custkey", "int32_t", 1500000, 50000},
        {"customer", "idx_customer_mktsegment_zone", "zone_map", "c_mktsegment", "int32_t", 1500000, 50000},

        // partsupp indexes
        {"partsupp", "idx_partsupp_partkey", "hash_multi_value", "ps_partkey", "int32_t", 8000000, 100000},
        {"partsupp", "idx_partsupp_suppkey", "hash_multi_value", "ps_suppkey", "int32_t", 8000000, 100000},

        // part indexes
        {"part", "idx_part_partkey", "hash_single", "p_partkey", "int32_t", 2000000, 50000},

        // supplier indexes
        {"supplier", "idx_supplier_suppkey", "hash_single", "s_suppkey", "int32_t", 100000, 25000},
        {"supplier", "idx_supplier_nationkey", "hash_multi_value", "s_nationkey", "int32_t", 100000, 25000}
    };
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    fs::create_directories(gendb_dir + "/indexes");

    auto indexes = get_index_definitions();

    for (const auto& idx : indexes) {
        std::string col_file = gendb_dir + "/" + idx.table_name + "/" + idx.column_name + ".bin";
        std::string out_file = gendb_dir + "/indexes/" + idx.index_name + ".bin";

        if (!fs::exists(col_file)) {
            std::cerr << "Column file not found: " << col_file << std::endl;
            continue;
        }

        if (idx.index_type == "zone_map") {
            if (idx.cpp_type == "int32_t") {
                build_zone_map_int32(col_file, idx.total_rows, idx.block_size, out_file);
            } else if (idx.cpp_type == "int64_t") {
                build_zone_map_int64(col_file, idx.total_rows, idx.block_size, out_file);
            }
        } else if (idx.index_type == "hash_multi_value") {
            build_hash_index_multi_value(col_file, idx.total_rows, out_file);
        } else if (idx.index_type == "hash_single") {
            build_hash_index_single(col_file, idx.total_rows, out_file);
        }
    }

    std::cout << "Index building complete." << std::endl;
    return 0;
}
