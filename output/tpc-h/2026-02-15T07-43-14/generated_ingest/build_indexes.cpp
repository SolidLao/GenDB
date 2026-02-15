#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;

// ============================================================================
// Zone Map (Min/Max Index)
// Per-block: min, max, count, null_count
// ============================================================================
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
    uint32_t null_count;
};

void build_zone_map_int32(const std::string& data_file, const std::string& index_file,
                          uint32_t block_size = 100000) {
    std::ifstream in(data_file, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open " << data_file << "\n";
        return;
    }

    in.seekg(0, std::ios::end);
    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    size_t num_elements = file_size / sizeof(int32_t);
    std::vector<ZoneMapEntry> zones;

    for (size_t i = 0; i < num_elements; i += block_size) {
        size_t block_count = std::min((size_t)block_size, num_elements - i);
        std::vector<int32_t> block(block_count);
        in.read(reinterpret_cast<char*>(block.data()), block_count * sizeof(int32_t));

        int32_t min_val = INT32_MAX, max_val = INT32_MIN;
        uint32_t null_count = 0;

        for (const auto& val : block) {
            min_val = std::min(min_val, val);
            max_val = std::max(max_val, val);
        }

        ZoneMapEntry entry{min_val, max_val, static_cast<uint32_t>(block_count), null_count};
        zones.push_back(entry);
    }

    in.close();

    // Write zone map
    std::ofstream out(index_file, std::ios::binary);
    uint32_t zone_count = zones.size();
    out.write(reinterpret_cast<char*>(&zone_count), sizeof(zone_count));
    for (const auto& zone : zones) {
        out.write(reinterpret_cast<const char*>(&zone.min_val), sizeof(zone.min_val));
        out.write(reinterpret_cast<const char*>(&zone.max_val), sizeof(zone.max_val));
        out.write(reinterpret_cast<const char*>(&zone.count), sizeof(zone.count));
        out.write(reinterpret_cast<const char*>(&zone.null_count), sizeof(zone.null_count));
    }
    out.close();

    std::cout << "Built zone map for " << data_file << ": " << zone_count << " zones\n";
}

// ============================================================================
// Hash Index
// Maps value -> position in column. Stored as simple linear probing hash table
// ============================================================================
struct HashIndexEntry {
    int32_t key;
    uint32_t position;  // position in column array
};

void build_hash_index_int32(const std::string& data_file, const std::string& index_file) {
    std::ifstream in(data_file, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open " << data_file << "\n";
        return;
    }

    in.seekg(0, std::ios::end);
    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    size_t num_elements = file_size / sizeof(int32_t);

    // Load all values
    std::vector<int32_t> values(num_elements);
    in.read(reinterpret_cast<char*>(values.data()), file_size);
    in.close();

    // Build hash table (linear probing, load factor ~0.75)
    size_t table_size = static_cast<size_t>(num_elements / 0.75 + 1);
    std::vector<HashIndexEntry> hash_table(table_size, {-1, UINT32_MAX});

    for (uint32_t pos = 0; pos < num_elements; ++pos) {
        int32_t key = values[pos];
        size_t hash = std::hash<int32_t>()(key) % table_size;

        // Linear probing
        while (hash_table[hash].position != UINT32_MAX) {
            hash = (hash + 1) % table_size;
        }

        hash_table[hash] = {key, pos};
    }

    // Write hash index: table_size, then all entries
    std::ofstream out(index_file, std::ios::binary);
    uint32_t ts = table_size;
    out.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
    for (const auto& entry : hash_table) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        out.write(reinterpret_cast<const char*>(&entry.position), sizeof(entry.position));
    }
    out.close();

    std::cout << "Built hash index for " << data_file << ": " << table_size << " slots\n";
}

// ============================================================================
// Sorted Index (simple sorted list of (key, position) pairs)
// For range queries and binary search
// ============================================================================
struct SortedIndexEntry {
    int32_t key;
    uint32_t position;
};

void build_sorted_index_int32(const std::string& data_file, const std::string& index_file) {
    std::ifstream in(data_file, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open " << data_file << "\n";
        return;
    }

    in.seekg(0, std::ios::end);
    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    size_t num_elements = file_size / sizeof(int32_t);

    // Load all values
    std::vector<int32_t> values(num_elements);
    in.read(reinterpret_cast<char*>(values.data()), file_size);
    in.close();

    // Build sorted index entries
    std::vector<SortedIndexEntry> index_entries;
    for (uint32_t pos = 0; pos < num_elements; ++pos) {
        index_entries.push_back({values[pos], pos});
    }

    // Sort by key
    std::sort(index_entries.begin(), index_entries.end(),
              [](const SortedIndexEntry& a, const SortedIndexEntry& b) {
                  return a.key < b.key;
              });

    // Write sorted index: count, then all entries
    std::ofstream out(index_file, std::ios::binary);
    uint32_t count = num_elements;
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& entry : index_entries) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        out.write(reinterpret_cast<const char*>(&entry.position), sizeof(entry.position));
    }
    out.close();

    std::cout << "Built sorted index for " << data_file << ": " << count << " entries\n";
}

// ============================================================================
// Hash Index for uint8_t (dictionary-encoded columns)
// ============================================================================
struct HashIndexEntry8 {
    uint8_t key;
    uint32_t position;
};

void build_hash_index_uint8(const std::string& data_file, const std::string& index_file) {
    std::ifstream in(data_file, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open " << data_file << "\n";
        return;
    }

    in.seekg(0, std::ios::end);
    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    size_t num_elements = file_size / sizeof(uint8_t);

    // Load all values
    std::vector<uint8_t> values(num_elements);
    in.read(reinterpret_cast<char*>(values.data()), file_size);
    in.close();

    // For low-cardinality, direct array indexed by value
    std::vector<uint32_t> first_occurrence(256, UINT32_MAX);
    for (uint32_t pos = 0; pos < num_elements; ++pos) {
        uint8_t key = values[pos];
        if (first_occurrence[key] == UINT32_MAX) {
            first_occurrence[key] = pos;
        }
    }

    // Write: 256 entries of first occurrence positions
    std::ofstream out(index_file, std::ios::binary);
    for (int i = 0; i < 256; ++i) {
        out.write(reinterpret_cast<const char*>(&first_occurrence[i]), sizeof(uint32_t));
    }
    out.close();

    std::cout << "Built hash index for uint8 " << data_file << "\n";
}

// ============================================================================
// Main indexing function
// ============================================================================
void build_indexes(const std::string& gendb_dir) {
    fs::create_directories(gendb_dir + "/indexes");

    std::cout << "Building indexes for lineitem...\n";
    build_zone_map_int32(gendb_dir + "/lineitem/l_shipdate.bin",
                         gendb_dir + "/indexes/l_shipdate_zone_map.bin");
    build_hash_index_int32(gendb_dir + "/lineitem/l_orderkey.bin",
                           gendb_dir + "/indexes/l_orderkey_hash.bin");

    std::cout << "Building indexes for orders...\n";
    build_sorted_index_int32(gendb_dir + "/orders/o_orderkey.bin",
                             gendb_dir + "/indexes/o_orderkey_sorted.bin");
    build_zone_map_int32(gendb_dir + "/orders/o_orderdate.bin",
                         gendb_dir + "/indexes/o_orderdate_zone_map.bin");

    std::cout << "Building indexes for customer...\n";
    build_sorted_index_int32(gendb_dir + "/customer/c_custkey.bin",
                             gendb_dir + "/indexes/c_custkey_sorted.bin");
    build_hash_index_uint8(gendb_dir + "/customer/c_mktsegment.bin",
                           gendb_dir + "/indexes/c_mktsegment_hash.bin");

    std::cout << "Index building complete\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: ./build_indexes <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    auto start = std::chrono::high_resolution_clock::now();

    build_indexes(gendb_dir);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "Index building complete in " << duration.count() << " seconds\n";

    return 0;
}
