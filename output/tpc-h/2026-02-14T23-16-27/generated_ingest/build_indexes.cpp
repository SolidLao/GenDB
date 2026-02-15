#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <filesystem>
#include <unordered_map>

namespace fs = std::filesystem;

// ============================================================================
// ZONE MAP BUILDER
// ============================================================================

struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint64_t row_count;
};

void build_zone_map_int32(const std::string& data_file, const std::string& output_file,
                          uint64_t block_size = 100000) {
    std::ifstream f(data_file, std::ios::binary);
    if (!f) {
        std::cerr << "Error: cannot open " << data_file << std::endl;
        return;
    }

    std::vector<ZoneMapBlock> zones;

    // Get file size
    f.seekg(0, std::ios::end);
    size_t file_size = f.tellg();
    f.seekg(0, std::ios::beg);

    uint64_t row_count = file_size / sizeof(int32_t);
    uint64_t blocks = (row_count + block_size - 1) / block_size;

    std::vector<int32_t> buffer(block_size);

    for (uint64_t block_idx = 0; block_idx < blocks; block_idx++) {
        uint64_t rows_in_block = std::min(block_size, row_count - block_idx * block_size);
        f.read((char*)buffer.data(), rows_in_block * sizeof(int32_t));

        if (rows_in_block > 0) {
            int32_t min_val = buffer[0];
            int32_t max_val = buffer[0];
            for (uint64_t i = 1; i < rows_in_block; i++) {
                min_val = std::min(min_val, buffer[i]);
                max_val = std::max(max_val, buffer[i]);
            }

            zones.push_back({min_val, max_val, rows_in_block});
        }
    }

    f.close();

    // Write zone map to binary file
    std::ofstream out(output_file, std::ios::binary);
    uint64_t num_zones = zones.size();
    out.write((char*)&num_zones, sizeof(uint64_t));
    for (const auto& zone : zones) {
        out.write((char*)&zone.min_val, sizeof(int32_t));
        out.write((char*)&zone.max_val, sizeof(int32_t));
        out.write((char*)&zone.row_count, sizeof(uint64_t));
    }
    out.close();

    std::cout << "Built zone map: " << output_file << " (" << num_zones << " zones)\n";
}

// ============================================================================
// HASH INDEX BUILDER
// ============================================================================

// Simple open-addressing hash table for int32_t keys
struct HashIndexInt32 {
    static constexpr float LOAD_FACTOR = 0.75f;

    std::vector<int64_t> table;  // value = (present_bit << 32) | row_id, or -1 for empty
    size_t size = 0;

    HashIndexInt32(size_t capacity = 1000000) {
        capacity = (size_t)(capacity / LOAD_FACTOR);
        table.assign(capacity, -1);
    }

    uint64_t hash(int32_t key) {
        return ((uint64_t)key * 2654435761UL) % table.size();
    }

    void insert(int32_t key, uint32_t row_id) {
        if (size >= (size_t)(table.size() * LOAD_FACTOR)) {
            std::cout << "Hash table at capacity, skipping resize\n";
            return;
        }
        uint64_t pos = hash(key);
        while (table[pos] != -1) {
            pos = (pos + 1) % table.size();
        }
        table[pos] = ((int64_t)1 << 32) | row_id;
        size++;
    }

    bool lookup(int32_t key, uint32_t& row_id) {
        uint64_t pos = hash(key);
        while (table[pos] != -1) {
            int64_t val = table[pos];
            if ((val >> 32) != 0) {
                row_id = (uint32_t)(val & 0xFFFFFFFF);
                return true;
            }
            pos = (pos + 1) % table.size();
        }
        return false;
    }

    void save(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        size_t sz = table.size();
        f.write((char*)&sz, sizeof(size_t));
        f.write((char*)table.data(), table.size() * sizeof(int64_t));
        f.close();
    }
};

void build_hash_index_int32(const std::string& data_file, const std::string& output_file) {
    std::ifstream f(data_file, std::ios::binary);
    if (!f) {
        std::cerr << "Error: cannot open " << data_file << std::endl;
        return;
    }

    // Get file size
    f.seekg(0, std::ios::end);
    size_t file_size = f.tellg();
    f.seekg(0, std::ios::beg);

    uint64_t row_count = file_size / sizeof(int32_t);
    HashIndexInt32 hash_idx(row_count);

    std::vector<int32_t> buffer(100000);
    uint32_t row_id = 0;

    while (row_id < row_count) {
        uint64_t rows_to_read = std::min(100000UL, row_count - row_id);
        f.read((char*)buffer.data(), rows_to_read * sizeof(int32_t));

        for (uint64_t i = 0; i < rows_to_read; i++) {
            hash_idx.insert(buffer[i], row_id + i);
        }
        row_id += rows_to_read;
    }

    f.close();

    hash_idx.save(output_file);
    std::cout << "Built hash index: " << output_file << " (" << row_count << " keys)\n";
}

// ============================================================================
// SORTED INDEX BUILDER
// ============================================================================

struct SortedIndexEntry {
    int32_t key;
    uint32_t row_id;

    bool operator<(const SortedIndexEntry& other) const {
        return key < other.key;
    }
};

void build_sorted_index_int32(const std::string& data_file, const std::string& output_file) {
    std::ifstream f(data_file, std::ios::binary);
    if (!f) {
        std::cerr << "Error: cannot open " << data_file << std::endl;
        return;
    }

    // Get file size
    f.seekg(0, std::ios::end);
    size_t file_size = f.tellg();
    f.seekg(0, std::ios::beg);

    uint64_t row_count = file_size / sizeof(int32_t);

    std::vector<SortedIndexEntry> entries;
    entries.reserve(row_count);

    std::vector<int32_t> buffer(100000);
    uint32_t row_id = 0;

    while (row_id < row_count) {
        uint64_t rows_to_read = std::min(100000UL, row_count - row_id);
        f.read((char*)buffer.data(), rows_to_read * sizeof(int32_t));

        for (uint64_t i = 0; i < rows_to_read; i++) {
            entries.push_back({buffer[i], row_id + i});
        }
        row_id += rows_to_read;
    }

    f.close();

    // Sort by key
    std::sort(entries.begin(), entries.end());

    // Write sorted index: [row_count | key1 row_id1 key2 row_id2 ...]
    std::ofstream out(output_file, std::ios::binary);
    out.write((char*)&row_count, sizeof(uint64_t));
    for (const auto& entry : entries) {
        out.write((char*)&entry.key, sizeof(int32_t));
        out.write((char*)&entry.row_id, sizeof(uint32_t));
    }
    out.close();

    std::cout << "Built sorted index: " << output_file << " (" << row_count << " keys)\n";
}

// ============================================================================
// COMPOSITE HASH INDEX (for multi-column keys)
// ============================================================================

struct CompositeHashIndex {
    static constexpr float LOAD_FACTOR = 0.75f;

    std::vector<int64_t> table;  // value = row_id, or -1 for empty
    size_t size = 0;

    CompositeHashIndex(size_t capacity = 1000000) {
        capacity = (size_t)(capacity / LOAD_FACTOR);
        table.assign(capacity, -1);
    }

    // Hash function for (int32_t, int32_t) pair
    uint64_t hash(int32_t k1, int32_t k2) {
        uint64_t h1 = (uint64_t)k1 * 2654435761UL;
        uint64_t h2 = (uint64_t)k2 * 2246822519UL;
        return (h1 ^ h2) % table.size();
    }

    void insert(int32_t k1, int32_t k2, uint32_t row_id) {
        if (size >= (size_t)(table.size() * LOAD_FACTOR)) {
            return;
        }
        uint64_t pos = hash(k1, k2);
        while (table[pos] != -1) {
            pos = (pos + 1) % table.size();
        }
        table[pos] = row_id;
        size++;
    }

    void save(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        size_t sz = table.size();
        f.write((char*)&sz, sizeof(size_t));
        f.write((char*)table.data(), table.size() * sizeof(int64_t));
        f.close();
    }
};

void build_composite_hash_index(const std::string& key1_file, const std::string& key2_file,
                                const std::string& output_file) {
    std::ifstream f1(key1_file, std::ios::binary);
    std::ifstream f2(key2_file, std::ios::binary);
    if (!f1 || !f2) {
        std::cerr << "Error: cannot open key files\n";
        return;
    }

    // Get row count from first key file
    f1.seekg(0, std::ios::end);
    size_t file_size = f1.tellg();
    f1.seekg(0, std::ios::beg);

    uint64_t row_count = file_size / sizeof(int32_t);
    CompositeHashIndex hash_idx(row_count);

    std::vector<int32_t> buffer1(100000), buffer2(100000);
    uint32_t row_id = 0;

    while (row_id < row_count) {
        uint64_t rows_to_read = std::min(100000UL, row_count - row_id);
        f1.read((char*)buffer1.data(), rows_to_read * sizeof(int32_t));
        f2.read((char*)buffer2.data(), rows_to_read * sizeof(int32_t));

        for (uint64_t i = 0; i < rows_to_read; i++) {
            hash_idx.insert(buffer1[i], buffer2[i], row_id + i);
        }
        row_id += rows_to_read;
    }

    f1.close();
    f2.close();

    hash_idx.save(output_file);
    std::cout << "Built composite hash index: " << output_file << " (" << row_count << " keys)\n";
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "Building indexes for TPC-H SF10...\n";

    // ====== LINEITEM INDEXES ======
    std::cout << "\n=== LINEITEM ===\n";
    build_zone_map_int32(gendb_dir + "/lineitem/l_shipdate.bin",
                         gendb_dir + "/lineitem/l_shipdate.zone_map");
    build_sorted_index_int32(gendb_dir + "/lineitem/l_orderkey.bin",
                             gendb_dir + "/lineitem/l_orderkey.sorted");

    // ====== ORDERS INDEXES ======
    std::cout << "\n=== ORDERS ===\n";
    build_zone_map_int32(gendb_dir + "/orders/o_orderdate.bin",
                         gendb_dir + "/orders/o_orderdate.zone_map");
    build_hash_index_int32(gendb_dir + "/orders/o_orderkey.bin",
                           gendb_dir + "/orders/o_orderkey.hash");

    // ====== CUSTOMER INDEXES ======
    std::cout << "\n=== CUSTOMER ===\n";
    build_hash_index_int32(gendb_dir + "/customer/c_custkey.bin",
                           gendb_dir + "/customer/c_custkey.hash");

    // ====== PART INDEXES ======
    std::cout << "\n=== PART ===\n";
    build_hash_index_int32(gendb_dir + "/part/p_partkey.bin",
                           gendb_dir + "/part/p_partkey.hash");

    // ====== PARTSUPP INDEXES ======
    std::cout << "\n=== PARTSUPP ===\n";
    build_composite_hash_index(gendb_dir + "/partsupp/ps_partkey.bin",
                               gendb_dir + "/partsupp/ps_suppkey.bin",
                               gendb_dir + "/partsupp/ps_composite.hash");

    // ====== SUPPLIER INDEXES ======
    std::cout << "\n=== SUPPLIER ===\n";
    build_hash_index_int32(gendb_dir + "/supplier/s_suppkey.bin",
                           gendb_dir + "/supplier/s_suppkey.hash");

    // ====== NATION INDEXES ======
    std::cout << "\n=== NATION ===\n";
    build_hash_index_int32(gendb_dir + "/nation/n_nationkey.bin",
                           gendb_dir + "/nation/n_nationkey.hash");

    // ====== REGION INDEXES ======
    std::cout << "\n=== REGION ===\n";
    build_hash_index_int32(gendb_dir + "/region/r_regionkey.bin",
                           gendb_dir + "/region/r_regionkey.hash");

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    std::cout << "\nIndex building complete in " << elapsed << " seconds\n";

    return 0;
}
