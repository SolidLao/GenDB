#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>

namespace fs = std::filesystem;

struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
    uint32_t block_offset;
};

// Build zone map for a DATE column
void build_zone_map_date(const std::string& col_path, const std::string& output_path,
                         uint32_t block_size) {
    std::cout << "Building zone map for " << col_path << std::endl;

    std::ifstream infile(col_path, std::ios::binary);
    if (!infile) {
        std::cerr << "Failed to open " << col_path << std::endl;
        return;
    }

    // Read file size
    infile.seekg(0, std::ios::end);
    size_t file_size = infile.tellg();
    infile.seekg(0, std::ios::beg);

    size_t num_rows = file_size / sizeof(int32_t);
    std::cout << "  Total rows: " << num_rows << std::endl;

    std::vector<ZoneMapBlock> zone_maps;
    std::vector<int32_t> buffer(block_size);

    size_t rows_read = 0;
    uint32_t block_num = 0;

    while (rows_read < num_rows) {
        size_t to_read = std::min(static_cast<size_t>(block_size), num_rows - rows_read);
        infile.read(reinterpret_cast<char*>(buffer.data()), to_read * sizeof(int32_t));

        int32_t min_val = buffer[0];
        int32_t max_val = buffer[0];

        for (size_t i = 0; i < to_read; ++i) {
            min_val = std::min(min_val, buffer[i]);
            max_val = std::max(max_val, buffer[i]);
        }

        zone_maps.push_back({min_val, max_val, static_cast<uint32_t>(to_read),
                            static_cast<uint32_t>(rows_read)});
        rows_read += to_read;
        block_num++;
    }
    infile.close();

    // Write zone maps to file
    std::ofstream outfile(output_path, std::ios::binary);
    uint32_t num_blocks = zone_maps.size();
    outfile.write(reinterpret_cast<const char*>(&num_blocks), sizeof(uint32_t));

    for (const auto& zm : zone_maps) {
        outfile.write(reinterpret_cast<const char*>(&zm.min_val), sizeof(int32_t));
        outfile.write(reinterpret_cast<const char*>(&zm.max_val), sizeof(int32_t));
        outfile.write(reinterpret_cast<const char*>(&zm.row_count), sizeof(uint32_t));
        outfile.write(reinterpret_cast<const char*>(&zm.block_offset), sizeof(uint32_t));
    }
    outfile.close();

    std::cout << "  Built " << num_blocks << " zone map blocks" << std::endl;
}

// Build hash index for an INTEGER column
void build_hash_index(const std::string& col_path, const std::string& output_path) {
    std::cout << "Building hash index for " << col_path << std::endl;

    std::ifstream infile(col_path, std::ios::binary);
    if (!infile) {
        std::cerr << "Failed to open " << col_path << std::endl;
        return;
    }

    infile.seekg(0, std::ios::end);
    size_t file_size = infile.tellg();
    infile.seekg(0, std::ios::beg);

    size_t num_rows = file_size / sizeof(int32_t);
    std::cout << "  Total rows: " << num_rows << std::endl;

    // Simple hash index: map value -> list of row positions
    std::unordered_map<int32_t, std::vector<uint32_t>> hash_index;

    std::vector<int32_t> row_buffer(1024);
    size_t rows_read = 0;

    while (rows_read < num_rows) {
        size_t to_read = std::min(static_cast<size_t>(1024), num_rows - rows_read);
        infile.read(reinterpret_cast<char*>(row_buffer.data()), to_read * sizeof(int32_t));

        for (size_t i = 0; i < to_read; ++i) {
            hash_index[row_buffer[i]].push_back(rows_read + i);
        }

        rows_read += to_read;
    }
    infile.close();

    // Write hash index: num_entries, then for each entry: key, value_count, values...
    std::ofstream outfile(output_path, std::ios::binary);
    uint32_t num_entries = hash_index.size();
    outfile.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));

    for (const auto& [key, positions] : hash_index) {
        outfile.write(reinterpret_cast<const char*>(&key), sizeof(int32_t));
        uint32_t count = positions.size();
        outfile.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
        outfile.write(reinterpret_cast<const char*>(positions.data()), count * sizeof(uint32_t));
    }
    outfile.close();

    std::cout << "  Built hash index with " << num_entries << " unique keys" << std::endl;
}

// Build sorted index (permutation-based) for an INTEGER column
void build_sorted_index(const std::string& col_path, const std::string& output_path) {
    std::cout << "Building sorted index for " << col_path << std::endl;

    std::ifstream infile(col_path, std::ios::binary);
    if (!infile) {
        std::cerr << "Failed to open " << col_path << std::endl;
        return;
    }

    infile.seekg(0, std::ios::end);
    size_t file_size = infile.tellg();
    infile.seekg(0, std::ios::beg);

    size_t num_rows = file_size / sizeof(int32_t);
    std::cout << "  Total rows: " << num_rows << std::endl;

    // Read all values
    std::vector<int32_t> values(num_rows);
    infile.read(reinterpret_cast<char*>(values.data()), num_rows * sizeof(int32_t));
    infile.close();

    // Create permutation (indices sorted by value)
    std::vector<uint32_t> permutation(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        permutation[i] = i;
    }

    std::sort(permutation.begin(), permutation.end(), [&](uint32_t a, uint32_t b) {
        return values[a] < values[b];
    });

    // Write permutation
    std::ofstream outfile(output_path, std::ios::binary);
    uint32_t num_entries = num_rows;
    outfile.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));
    outfile.write(reinterpret_cast<const char*>(permutation.data()), num_rows * sizeof(uint32_t));
    outfile.close();

    std::cout << "  Built sorted index with " << num_rows << " entries" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::cout << "Building indexes in " << gendb_dir << std::endl;

    uint32_t block_size = 100000;

    // Create indexes directory
    std::string indexes_dir = gendb_dir + "/indexes";
    fs::create_directories(indexes_dir);

    // LINEITEM indexes
    std::cout << "\n=== LINEITEM Indexes ===" << std::endl;
    build_zone_map_date(gendb_dir + "/lineitem/l_shipdate.bin",
                        indexes_dir + "/l_shipdate_zone_map.bin", block_size);
    build_sorted_index(gendb_dir + "/lineitem/l_orderkey.bin",
                       indexes_dir + "/l_orderkey_sorted.bin");

    // ORDERS indexes
    std::cout << "\n=== ORDERS Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/orders/o_orderkey.bin",
                     indexes_dir + "/o_orderkey_hash.bin");
    build_hash_index(gendb_dir + "/orders/o_custkey.bin",
                     indexes_dir + "/o_custkey_hash.bin");
    build_zone_map_date(gendb_dir + "/orders/o_orderdate.bin",
                        indexes_dir + "/o_orderdate_zone_map.bin", block_size);

    // CUSTOMER indexes
    std::cout << "\n=== CUSTOMER Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/customer/c_custkey.bin",
                     indexes_dir + "/c_custkey_hash.bin");
    // c_mktsegment is dictionary-encoded, use zone map on codes
    build_zone_map_date(gendb_dir + "/customer/c_mktsegment.bin",
                        indexes_dir + "/c_mktsegment_zone_map.bin", block_size);

    // SUPPLIER indexes
    std::cout << "\n=== SUPPLIER Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/supplier/s_suppkey.bin",
                     indexes_dir + "/s_suppkey_hash.bin");

    // PART indexes
    std::cout << "\n=== PART Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/part/p_partkey.bin",
                     indexes_dir + "/p_partkey_hash.bin");

    // PARTSUPP indexes
    std::cout << "\n=== PARTSUPP Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/partsupp/ps_partkey.bin",
                     indexes_dir + "/ps_partkey_hash.bin");
    build_hash_index(gendb_dir + "/partsupp/ps_suppkey.bin",
                     indexes_dir + "/ps_suppkey_hash.bin");

    // NATION indexes
    std::cout << "\n=== NATION Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/nation/n_nationkey.bin",
                     indexes_dir + "/n_nationkey_hash.bin");

    // REGION indexes
    std::cout << "\n=== REGION Indexes ===" << std::endl;
    build_hash_index(gendb_dir + "/region/r_regionkey.bin",
                     indexes_dir + "/r_regionkey_hash.bin");

    std::cout << "\n=== Index building complete! ===" << std::endl;
    return 0;
}
