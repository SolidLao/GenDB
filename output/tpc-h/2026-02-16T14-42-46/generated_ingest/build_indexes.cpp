#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <map>
#include <cmath>
#include <omp.h>

namespace fs = std::filesystem;
using namespace std;

struct ZoneMapEntry {
    int32_t min_val, max_val;
    uint32_t count;
};

struct HashMultiEntry {
    int32_t key;
    uint32_t offset;  // Offset into positions array
    uint32_t count;   // How many positions for this key
};

void build_zone_map_int32(const string& column_path, const string& output_path,
                          uint32_t block_size, uint64_t total_rows) {
    // mmap the column file
    int fd = open(column_path.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "ERROR: Cannot open " << column_path << endl;
        exit(1);
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        cerr << "ERROR: mmap failed for " << column_path << endl;
        exit(1);
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Compute zone maps
    uint32_t num_blocks = (total_rows + block_size - 1) / block_size;
    vector<ZoneMapEntry> zone_maps(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        uint64_t start_row = (uint64_t)block_idx * block_size;
        uint64_t end_row = min((uint64_t)(block_idx + 1) * block_size, (uint64_t)total_rows);
        uint64_t block_rows = end_row - start_row;

        int32_t min_val = data[start_row];
        int32_t max_val = data[start_row];

        for (uint64_t i = start_row; i < end_row; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_maps[block_idx] = {min_val, max_val, (uint32_t)block_rows};
    }

    // Write zone map file
    ofstream outfile(output_path, ios::binary);
    uint32_t num_entries = zone_maps.size();
    outfile.write((char*)&num_entries, sizeof(uint32_t));
    for (const auto& entry : zone_maps) {
        outfile.write((char*)&entry.min_val, sizeof(int32_t));
        outfile.write((char*)&entry.max_val, sizeof(int32_t));
        outfile.write((char*)&entry.count, sizeof(uint32_t));
    }
    outfile.close();

    cout << "  Zone map: " << num_entries << " blocks, " << output_path << endl;

    munmap((void*)data, file_size);
    close(fd);
}

void build_hash_index_multi_value(const string& column_path, const string& output_path,
                                   uint64_t total_rows) {
    cout << "  Building hash index for " << column_path << "..." << flush;

    // mmap the column file
    int fd = open(column_path.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "ERROR: Cannot open " << column_path << endl;
        exit(1);
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        cerr << "ERROR: mmap failed for " << column_path << endl;
        exit(1);
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Use map for simplicity - it's faster than critical sections for multi-threading
    map<int32_t, vector<uint32_t>> key_positions;

    // Single-threaded pass to build groupings (streaming read is fastest)
    for (uint64_t i = 0; i < total_rows; ++i) {
        key_positions[data[i]].push_back((uint32_t)i);
    }

    // Build hash table and positions array
    uint32_t num_unique = key_positions.size();
    uint32_t hash_table_size = 1;
    while (hash_table_size < (uint32_t)((num_unique + 0.5) / 0.6)) {
        hash_table_size *= 2;
    }

    vector<HashMultiEntry> hash_table(hash_table_size);
    fill(hash_table.begin(), hash_table.end(), HashMultiEntry{-1, 0, 0});

    vector<uint32_t> positions_array;

    // Build positions array and hash table
    for (auto& [key, positions] : key_positions) {
        uint32_t offset = positions_array.size();
        uint32_t count = positions.size();

        for (auto pos : positions) {
            positions_array.push_back(pos);
        }

        // Linear probing insert
        uint32_t hash = ((uint32_t)key * 2654435761U) % hash_table_size;
        uint32_t idx = hash;

        while (hash_table[idx].key != -1) {
            idx = (idx + 1) % hash_table_size;
        }

        hash_table[idx] = {key, offset, count};
    }

    // Write to file
    ofstream outfile(output_path, ios::binary);
    outfile.write((char*)&num_unique, sizeof(uint32_t));
    outfile.write((char*)&hash_table_size, sizeof(uint32_t));

    // Write hash table (12 bytes per entry: key, offset, count)
    for (const auto& entry : hash_table) {
        outfile.write((char*)&entry.key, sizeof(int32_t));
        outfile.write((char*)&entry.offset, sizeof(uint32_t));
        outfile.write((char*)&entry.count, sizeof(uint32_t));
    }

    // Write positions array
    uint32_t pos_count = positions_array.size();
    outfile.write((char*)&pos_count, sizeof(uint32_t));
    for (auto pos : positions_array) {
        outfile.write((char*)&pos, sizeof(uint32_t));
    }

    outfile.close();

    cout << " " << num_unique << " unique, size " << hash_table_size << endl;

    munmap((void*)data, file_size);
    close(fd);
}

void build_hash_index_single(const string& column_path, const string& output_path,
                             uint64_t total_rows) {
    cout << "  Building hash index for " << column_path << "..." << flush;

    // mmap the column file
    int fd = open(column_path.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "ERROR: Cannot open " << column_path << endl;
        exit(1);
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        cerr << "ERROR: mmap failed for " << column_path << endl;
        exit(1);
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // For primary keys, build simple key->position map
    map<int32_t, uint32_t> key_to_pos;

    for (uint64_t i = 0; i < total_rows; ++i) {
        key_to_pos[data[i]] = (uint32_t)i;
    }

    // Build hash table with open addressing
    uint32_t num_entries = key_to_pos.size();
    uint32_t hash_table_size = 1;
    while (hash_table_size < (uint32_t)((num_entries + 0.5) / 0.6)) {
        hash_table_size *= 2;
    }

    struct Entry { int32_t key; uint32_t pos; };
    vector<Entry> hash_table(hash_table_size, {-1, 0});

    // Insert into hash table with linear probing
    for (auto& [key, pos] : key_to_pos) {
        uint32_t hash = ((uint32_t)key * 2654435761U) % hash_table_size;
        uint32_t idx = hash;

        while (hash_table[idx].key != -1) {
            idx = (idx + 1) % hash_table_size;
        }

        hash_table[idx] = {key, pos};
    }

    // Write to file
    ofstream outfile(output_path, ios::binary);
    outfile.write((char*)&num_entries, sizeof(uint32_t));
    outfile.write((char*)&hash_table_size, sizeof(uint32_t));

    for (const auto& entry : hash_table) {
        outfile.write((char*)&entry.key, sizeof(int32_t));
        outfile.write((char*)&entry.pos, sizeof(uint32_t));
    }

    outfile.close();

    cout << " " << num_entries << " entries, size " << hash_table_size << endl;

    munmap((void*)data, file_size);
    close(fd);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <gendb_dir>" << endl;
        return 1;
    }

    string gendb_dir = argv[1];
    string indexes_dir = gendb_dir + "/indexes";

    cout << "=== Building Indexes ===" << endl;

    fs::create_directories(indexes_dir);

    // Zone maps
    cout << "\nBuilding zone maps..." << endl;
    build_zone_map_int32(gendb_dir + "/lineitem/l_shipdate.bin",
                         indexes_dir + "/l_shipdate_zone.bin", 262144, 59986052);
    build_zone_map_int32(gendb_dir + "/orders/o_orderdate.bin",
                         indexes_dir + "/o_orderdate_zone.bin", 131072, 15000000);

    // Hash indexes for joins
    cout << "\nBuilding hash indexes..." << endl;

    // Lineitem foreign keys (multi-value)
    build_hash_index_multi_value(gendb_dir + "/lineitem/l_orderkey.bin",
                                 indexes_dir + "/l_orderkey_hash.bin", 59986052);
    build_hash_index_multi_value(gendb_dir + "/lineitem/l_suppkey.bin",
                                 indexes_dir + "/l_suppkey_hash.bin", 59986052);
    build_hash_index_multi_value(gendb_dir + "/lineitem/l_partkey.bin",
                                 indexes_dir + "/l_partkey_hash.bin", 59986052);

    // Orders keys
    build_hash_index_single(gendb_dir + "/orders/o_orderkey.bin",
                            indexes_dir + "/o_orderkey_hash.bin", 15000000);
    build_hash_index_multi_value(gendb_dir + "/orders/o_custkey.bin",
                                 indexes_dir + "/o_custkey_hash.bin", 15000000);

    // Customer key
    build_hash_index_single(gendb_dir + "/customer/c_custkey.bin",
                            indexes_dir + "/c_custkey_hash.bin", 1500000);

    // Part key
    build_hash_index_single(gendb_dir + "/part/p_partkey.bin",
                            indexes_dir + "/p_partkey_hash.bin", 2000000);

    // PartSupp keys (multi-value)
    build_hash_index_multi_value(gendb_dir + "/partsupp/ps_partkey.bin",
                                 indexes_dir + "/ps_partkey_hash.bin", 8000000);
    build_hash_index_multi_value(gendb_dir + "/partsupp/ps_suppkey.bin",
                                 indexes_dir + "/ps_suppkey_hash.bin", 8000000);

    // Supplier key
    build_hash_index_single(gendb_dir + "/supplier/s_suppkey.bin",
                            indexes_dir + "/s_suppkey_hash.bin", 100000);

    // Nation key
    build_hash_index_single(gendb_dir + "/nation/n_nationkey.bin",
                            indexes_dir + "/n_nationkey_hash.bin", 25);

    // Region key
    build_hash_index_single(gendb_dir + "/region/r_regionkey.bin",
                            indexes_dir + "/r_regionkey_hash.bin", 5);

    cout << "\n=== Index Building Complete ===" << endl;
    cout << "Indexes written to: " << indexes_dir << endl;

    return 0;
}
