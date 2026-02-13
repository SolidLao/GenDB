#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>
#include <sstream>

const int BLOCK_SIZE = 150000;

// Read binary column file into memory
template<typename T>
std::vector<T> read_column(const std::string& filename, size_t num_rows) {
    std::ifstream in(filename, std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "Failed to open " << filename << std::endl;
        return {};
    }

    std::vector<T> data(num_rows);
    in.read((char*)data.data(), num_rows * sizeof(T));
    in.close();

    return data;
}

// Write index file
void write_index_file(const std::string& filename, const void* data, size_t num_elements, size_t element_size) {
    std::ofstream out(filename, std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Failed to create " << filename << std::endl;
        return;
    }

    out.write((const char*)data, num_elements * element_size);
    out.close();
}

// Build sorted index for a column
template<typename T>
void build_sorted_index(const std::string& column_name, const std::vector<T>& column_data,
                        const std::string& gendb_dir) {
    std::cout << "Building sorted index for " << column_name << std::endl;

    size_t num_rows = column_data.size();

    // Create permutation array: pairs of (value, original_index)
    std::vector<std::pair<T, uint32_t>> permutation;
    permutation.reserve(num_rows);

    for (uint32_t i = 0; i < num_rows; i++) {
        permutation.push_back({column_data[i], i});
    }

    // Sort by value
    std::sort(permutation.begin(), permutation.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Write sorted permutation to index file
    std::vector<uint32_t> sorted_indices;
    sorted_indices.reserve(num_rows);
    for (const auto& p : permutation) {
        sorted_indices.push_back(p.second);
    }

    write_index_file(gendb_dir + "/" + column_name + ".sorted.idx",
                     sorted_indices.data(), sorted_indices.size(), sizeof(uint32_t));

    std::cout << "  Sorted index written: " << column_name << ".sorted.idx ("
              << (sorted_indices.size() * sizeof(uint32_t) / 1024 / 1024) << " MB)" << std::endl;
}

// Build hash index for a column
template<typename T>
void build_hash_index(const std::string& column_name, const std::vector<T>& column_data,
                      const std::string& gendb_dir) {
    std::cout << "Building hash index for " << column_name << std::endl;

    size_t num_rows = column_data.size();

    // Hash index: key -> vector of row indices
    std::unordered_map<uint64_t, std::vector<uint32_t>> hash_index;

    for (uint32_t i = 0; i < num_rows; i++) {
        // Cast value to uint64_t for hashing
        uint64_t key = *(uint64_t*)&column_data[i];
        hash_index[key].push_back(i);
    }

    // Serialize hash index to file
    std::ofstream out(gendb_dir + "/" + column_name + ".hash.idx", std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Failed to create hash index file" << std::endl;
        return;
    }

    // Write number of entries
    uint32_t num_entries = hash_index.size();
    out.write((char*)&num_entries, sizeof(uint32_t));

    size_t total_bytes = sizeof(uint32_t);

    // Write each entry: (key, count, list of indices)
    for (const auto& [key, indices] : hash_index) {
        out.write((char*)&key, sizeof(uint64_t));
        uint32_t count = indices.size();
        out.write((char*)&count, sizeof(uint32_t));
        out.write((char*)indices.data(), count * sizeof(uint32_t));
        total_bytes += sizeof(uint64_t) + sizeof(uint32_t) + count * sizeof(uint32_t);
    }

    out.close();
    std::cout << "  Hash index written: " << column_name << ".hash.idx with " << num_entries
              << " entries (" << (total_bytes / 1024 / 1024) << " MB)" << std::endl;
}

// Build zone maps (min/max per block) for a column
template<typename T>
void build_zone_maps(const std::string& column_name, const std::vector<T>& column_data,
                     const std::string& gendb_dir) {
    std::cout << "Building zone maps for " << column_name << std::endl;

    size_t num_rows = column_data.size();
    size_t num_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<T> block_mins, block_maxs;
    std::vector<uint32_t> block_row_counts;

    for (size_t block_id = 0; block_id < num_blocks; block_id++) {
        size_t block_start = block_id * BLOCK_SIZE;
        size_t block_end = std::min(block_start + BLOCK_SIZE, num_rows);
        size_t block_size = block_end - block_start;

        if (block_size == 0) continue;

        T min_val = column_data[block_start];
        T max_val = column_data[block_start];

        for (size_t i = block_start; i < block_end; i++) {
            min_val = std::min(min_val, column_data[i]);
            max_val = std::max(max_val, column_data[i]);
        }

        block_mins.push_back(min_val);
        block_maxs.push_back(max_val);
        block_row_counts.push_back(block_size);
    }

    // Write zone maps to file
    std::ofstream out(gendb_dir + "/" + column_name + ".zone_map.idx", std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Failed to create zone map file" << std::endl;
        return;
    }

    uint32_t num_zone_maps = block_mins.size();
    out.write((char*)&num_zone_maps, sizeof(uint32_t));
    out.write((char*)block_mins.data(), num_zone_maps * sizeof(T));
    out.write((char*)block_maxs.data(), num_zone_maps * sizeof(T));
    out.write((char*)block_row_counts.data(), num_zone_maps * sizeof(uint32_t));

    out.close();
    size_t total_bytes = sizeof(uint32_t) + 2 * num_zone_maps * sizeof(T) + num_zone_maps * sizeof(uint32_t);
    std::cout << "  Zone maps written: " << column_name << ".zone_map.idx with " << num_zone_maps
              << " blocks (" << (total_bytes / 1024) << " KB)" << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    // Read metadata to determine row count
    std::ifstream metadata_file(gendb_dir + "/metadata.txt");
    if (!metadata_file.is_open()) {
        std::cerr << "Failed to read metadata.txt" << std::endl;
        return 1;
    }

    std::string table_name;
    uint32_t num_rows;
    std::string columns_str;

    metadata_file >> table_name >> num_rows;
    std::getline(metadata_file, columns_str);  // Read rest of line
    std::getline(metadata_file, columns_str);  // Read columns line
    metadata_file.close();

    if (table_name != "lineitem") {
        std::cerr << "Expected 'lineitem' table" << std::endl;
        return 1;
    }

    std::cout << "Building indexes for lineitem table with " << num_rows << " rows..." << std::endl;

    // Read lineitem columns from binary files
    auto l_shipdate = read_column<int32_t>(gendb_dir + "/lineitem.l_shipdate", num_rows);
    auto l_orderkey = read_column<int32_t>(gendb_dir + "/lineitem.l_orderkey", num_rows);

    if (l_shipdate.empty() || l_orderkey.empty()) {
        std::cerr << "Failed to read lineitem columns" << std::endl;
        return 1;
    }

    // Build sorted index for l_shipdate
    build_sorted_index("lineitem.l_shipdate", l_shipdate, gendb_dir);

    // Build hash index for l_orderkey
    build_hash_index("lineitem.l_orderkey", l_orderkey, gendb_dir);

    // Build zone maps for l_shipdate
    build_zone_maps("lineitem.l_shipdate", l_shipdate, gendb_dir);

    std::cout << "\nIndex building complete!" << std::endl;

    return 0;
}
