#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <filesystem>
#include <cstring>
#include <chrono>
#include <cassert>

namespace fs = std::filesystem;

// ============================================================================
// Sorted Index: stores (value, row_index) pairs in sorted order
// Enables binary search lookups and range scans
// ============================================================================

template<typename T>
struct SortedIndex {
    std::vector<std::pair<T, int32_t>> entries;  // (key, row_index)

    void build(const std::string& column_file, const std::string& index_file) {
        // Read column data
        std::ifstream f(column_file, std::ios::binary);
        if (!f.is_open()) {
            std::cerr << "ERROR: Cannot open " << column_file << std::endl;
            return;
        }

        f.seekg(0, std::ios::end);
        size_t file_size = f.tellg();
        f.seekg(0, std::ios::beg);

        size_t count = file_size / sizeof(T);
        entries.reserve(count);

        std::vector<T> data(count);
        f.read(reinterpret_cast<char*>(data.data()), file_size);
        f.close();

        // Build index: (value, row_index) pairs
        for (int32_t i = 0; i < (int32_t)count; ++i) {
            entries.push_back({data[i], i});
        }

        // Sort by key value
        std::sort(entries.begin(), entries.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        // Write to disk
        std::ofstream out(index_file, std::ios::binary);
        for (const auto& e : entries) {
            out.write(reinterpret_cast<const char*>(&e.first), sizeof(T));
            out.write(reinterpret_cast<const char*>(&e.second), sizeof(int32_t));
        }
        out.close();

        std::cout << "    Built sorted index with " << entries.size() << " entries" << std::endl;
    }
};

// ============================================================================
// Hash Index: hash table for O(1) lookups
// For small cardinality or equality predicates
// ============================================================================

template<typename T>
struct HashIndex {
    std::map<T, std::vector<int32_t>> hash_map;  // value -> list of row indices

    void build(const std::string& column_file, const std::string& index_file) {
        std::ifstream f(column_file, std::ios::binary);
        if (!f.is_open()) {
            std::cerr << "ERROR: Cannot open " << column_file << std::endl;
            return;
        }

        f.seekg(0, std::ios::end);
        size_t file_size = f.tellg();
        f.seekg(0, std::ios::beg);

        size_t count = file_size / sizeof(T);
        std::vector<T> data(count);
        f.read(reinterpret_cast<char*>(data.data()), file_size);
        f.close();

        // Build hash table
        for (int32_t i = 0; i < (int32_t)count; ++i) {
            hash_map[data[i]].push_back(i);
        }

        // Write to disk: for each unique value, write count then row indices
        std::ofstream out(index_file, std::ios::binary);
        uint32_t unique_count = hash_map.size();
        out.write(reinterpret_cast<const char*>(&unique_count), sizeof(uint32_t));

        for (const auto& [value, indices] : hash_map) {
            out.write(reinterpret_cast<const char*>(&value), sizeof(T));
            uint32_t idx_count = indices.size();
            out.write(reinterpret_cast<const char*>(&idx_count), sizeof(uint32_t));
            for (int32_t idx : indices) {
                out.write(reinterpret_cast<const char*>(&idx), sizeof(int32_t));
            }
        }
        out.close();

        std::cout << "    Built hash index with " << unique_count << " unique values" << std::endl;
    }
};

// ============================================================================
// Zone Maps: min/max per block for block skipping
// ============================================================================

template<typename T>
struct ZoneMap {
    struct Block {
        T min_val, max_val;
        int32_t row_count;
    };
    std::vector<Block> blocks;

    void build(const std::string& column_file, const std::string& index_file, int32_t block_size = 100000) {
        std::ifstream f(column_file, std::ios::binary);
        if (!f.is_open()) {
            std::cerr << "ERROR: Cannot open " << column_file << std::endl;
            return;
        }

        f.seekg(0, std::ios::end);
        size_t file_size = f.tellg();
        f.seekg(0, std::ios::beg);

        size_t count = file_size / sizeof(T);
        std::vector<T> data(count);
        f.read(reinterpret_cast<char*>(data.data()), file_size);
        f.close();

        // Process blocks
        for (int32_t block_start = 0; block_start < (int32_t)count; block_start += block_size) {
            int32_t block_end = std::min(block_start + block_size, (int32_t)count);
            T min_val = data[block_start];
            T max_val = data[block_start];

            for (int32_t i = block_start; i < block_end; ++i) {
                if (data[i] < min_val) min_val = data[i];
                if (data[i] > max_val) max_val = data[i];
            }

            blocks.push_back({min_val, max_val, block_end - block_start});
        }

        // Write to disk
        std::ofstream out(index_file, std::ios::binary);
        uint32_t block_count = blocks.size();
        out.write(reinterpret_cast<const char*>(&block_count), sizeof(uint32_t));
        out.write(reinterpret_cast<const char*>(&block_size), sizeof(int32_t));

        for (const auto& block : blocks) {
            out.write(reinterpret_cast<const char*>(&block.min_val), sizeof(T));
            out.write(reinterpret_cast<const char*>(&block.max_val), sizeof(T));
            out.write(reinterpret_cast<const char*>(&block.row_count), sizeof(int32_t));
        }
        out.close();

        std::cout << "    Built zone map with " << blocks.size() << " blocks" << std::endl;
    }
};

// ============================================================================
// Build indexes for lineitem
// ============================================================================
void build_lineitem_indexes(const std::string& table_dir) {
    std::cout << "Building lineitem indexes..." << std::endl;

    // l_orderkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/l_orderkey.bin", table_dir + "/l_orderkey.idx");
    }

    // l_shipdate: sorted index + zone map
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/l_shipdate.bin", table_dir + "/l_shipdate.idx");

        ZoneMap<int32_t> zmap;
        zmap.build(table_dir + "/l_shipdate.bin", table_dir + "/l_shipdate.zonemap", 100000);
    }
}

// ============================================================================
// Build indexes for orders
// ============================================================================
void build_orders_indexes(const std::string& table_dir) {
    std::cout << "Building orders indexes..." << std::endl;

    // o_orderkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/o_orderkey.bin", table_dir + "/o_orderkey.idx");
    }

    // o_custkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/o_custkey.bin", table_dir + "/o_custkey.idx");
    }

    // o_orderdate: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/o_orderdate.bin", table_dir + "/o_orderdate.idx");
    }
}

// ============================================================================
// Build indexes for customer
// ============================================================================
void build_customer_indexes(const std::string& table_dir) {
    std::cout << "Building customer indexes..." << std::endl;

    // c_custkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/c_custkey.bin", table_dir + "/c_custkey.idx");
    }

    // c_mktsegment: hash index (low cardinality, 5 values)
    {
        HashIndex<uint8_t> idx;
        idx.build(table_dir + "/c_mktsegment.bin", table_dir + "/c_mktsegment.hidx");
    }
}

// ============================================================================
// Build indexes for part
// ============================================================================
void build_part_indexes(const std::string& table_dir) {
    std::cout << "Building part indexes..." << std::endl;

    // p_partkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/p_partkey.bin", table_dir + "/p_partkey.idx");
    }
}

// ============================================================================
// Build indexes for supplier
// ============================================================================
void build_supplier_indexes(const std::string& table_dir) {
    std::cout << "Building supplier indexes..." << std::endl;

    // s_suppkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/s_suppkey.bin", table_dir + "/s_suppkey.idx");
    }
}

// ============================================================================
// Build indexes for partsupp
// ============================================================================
void build_partsupp_indexes(const std::string& table_dir) {
    std::cout << "Building partsupp indexes..." << std::endl;

    // ps_partkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/ps_partkey.bin", table_dir + "/ps_partkey.idx");
    }

    // ps_suppkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/ps_suppkey.bin", table_dir + "/ps_suppkey.idx");
    }
}

// ============================================================================
// Build indexes for nation
// ============================================================================
void build_nation_indexes(const std::string& table_dir) {
    std::cout << "Building nation indexes..." << std::endl;

    // n_nationkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/n_nationkey.bin", table_dir + "/n_nationkey.idx");
    }
}

// ============================================================================
// Build indexes for region
// ============================================================================
void build_region_indexes(const std::string& table_dir) {
    std::cout << "Building region indexes..." << std::endl;

    // r_regionkey: sorted index
    {
        SortedIndex<int32_t> idx;
        idx.build(table_dir + "/r_regionkey.bin", table_dir + "/r_regionkey.idx");
    }
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: ./build_indexes <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Index Building Pipeline" << std::endl;
    std::cout << "=============================" << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    auto overall_start = std::chrono::high_resolution_clock::now();

    build_lineitem_indexes(gendb_dir + "/lineitem");
    build_orders_indexes(gendb_dir + "/orders");
    build_customer_indexes(gendb_dir + "/customer");
    build_part_indexes(gendb_dir + "/part");
    build_supplier_indexes(gendb_dir + "/supplier");
    build_partsupp_indexes(gendb_dir + "/partsupp");
    build_nation_indexes(gendb_dir + "/nation");
    build_region_indexes(gendb_dir + "/region");

    auto overall_end = std::chrono::high_resolution_clock::now();
    auto overall_duration = std::chrono::duration_cast<std::chrono::seconds>(overall_end - overall_start).count();

    std::cout << std::endl;
    std::cout << "Index building completed in " << overall_duration << "s" << std::endl;

    return 0;
}
