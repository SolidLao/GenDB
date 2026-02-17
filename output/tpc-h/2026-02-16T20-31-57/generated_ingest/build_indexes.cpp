#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <omp.h>

// Compile with: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp

// Hash function: multiply-shift to avoid clustering
uint64_t hash_int32(int32_t key) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
}

// Read binary column file via mmap
template <typename T>
class MmapColumn {
public:
    int fd;
    void* data;
    size_t size;
    size_t count;

    MmapColumn(const std::string& path) : fd(-1), data(nullptr), size(0), count(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << "\n";
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << "\n";
            close(fd);
            return;
        }

        size = sb.st_size;
        count = size / sizeof(T);

        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << "\n";
            close(fd);
            data = nullptr;
            return;
        }

        // Hint for sequential access
        madvise(data, size, MADV_SEQUENTIAL);
    }

    ~MmapColumn() {
        if (data) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    T* get() { return (T*)data; }
    size_t get_count() { return count; }
};

// Zone map: min/max per block
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

void build_zone_map(const std::string& column_file, const std::string& output_file, uint32_t block_size) {
    MmapColumn<int32_t> col(column_file);
    if (!col.get()) return;

    int32_t* data = col.get();
    size_t total_rows = col.get_count();

    std::vector<ZoneMapBlock> blocks;

#pragma omp parallel
    {
        std::vector<ZoneMapBlock> local_blocks;

#pragma omp for schedule(static)
        for (size_t block_start = 0; block_start < total_rows; block_start += block_size) {
            size_t block_end = std::min(block_start + (size_t)block_size, total_rows);
            int32_t min_val = data[block_start];
            int32_t max_val = data[block_start];

            for (size_t i = block_start; i < block_end; i++) {
                min_val = std::min(min_val, data[i]);
                max_val = std::max(max_val, data[i]);
            }

            local_blocks.push_back({min_val, max_val, (uint32_t)(block_end - block_start)});
        }

#pragma omp critical
        {
            blocks.insert(blocks.end(), local_blocks.begin(), local_blocks.end());
        }
    }

    // Sort blocks by their block index (restore order)
    std::sort(blocks.begin(), blocks.end(), [](const ZoneMapBlock& a, const ZoneMapBlock& b) {
        return a.row_count < b.row_count; // Keep insertion order
    });

    // Write zone map file: [uint32_t num_blocks][ZoneMapBlock...]
    std::ofstream f(output_file, std::ios::binary);
    uint32_t num_blocks = blocks.size();
    f.write((const char*)&num_blocks, sizeof(num_blocks));
    for (const auto& block : blocks) {
        f.write((const char*)&block.min_val, sizeof(block.min_val));
        f.write((const char*)&block.max_val, sizeof(block.max_val));
        f.write((const char*)&block.row_count, sizeof(block.row_count));
    }
    f.close();

    std::cout << "Zone map " << output_file << ": " << num_blocks << " blocks\n";
}

// Hash index: single-value (for primary keys)
struct HashEntry {
    int32_t key;
    uint32_t position;
};

void build_hash_single(const std::string& column_file, const std::string& output_file) {
    MmapColumn<int32_t> col(column_file);
    if (!col.get()) return;

    int32_t* data = col.get();
    size_t total_rows = col.get_count();

    // Build hash table: key -> position
    std::unordered_map<int32_t, uint32_t> hash_table;

    for (size_t i = 0; i < total_rows; i++) {
        hash_table[data[i]] = (uint32_t)i;
    }

    // Write hash index file: [uint32_t num_entries][key:int32_t, pos:uint32_t]...
    std::ofstream f(output_file, std::ios::binary);
    uint32_t num_entries = hash_table.size();
    f.write((const char*)&num_entries, sizeof(num_entries));

    for (const auto& [key, pos] : hash_table) {
        f.write((const char*)&key, sizeof(key));
        f.write((const char*)&pos, sizeof(pos));
    }
    f.close();

    std::cout << "Hash index (single) " << output_file << ": " << num_entries << " entries\n";
}

// Hash index: multi-value (for foreign keys with duplicates)
// Layout: [uint32_t num_unique][uint32_t table_size]
//         [hash_table: key:int32_t, offset:uint32_t, count:uint32_t per slot (12B)]
//         [positions_array: uint32_t positions...]
struct HashEntryMulti {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

void build_hash_multi(const std::string& column_file, const std::string& output_file) {
    MmapColumn<int32_t> col(column_file);
    if (!col.get()) return;

    int32_t* data = col.get();
    size_t total_rows = col.get_count();

    // Group positions by key
    std::unordered_map<int32_t, std::vector<uint32_t>> groups;

#pragma omp parallel
    {
        std::unordered_map<int32_t, std::vector<uint32_t>> local_groups;

#pragma omp for schedule(static)
        for (size_t i = 0; i < total_rows; i++) {
            local_groups[data[i]].push_back((uint32_t)i);
        }

#pragma omp critical
        {
            for (auto& [key, positions] : local_groups) {
                for (auto pos : positions) {
                    groups[key].push_back(pos);
                }
            }
        }
    }

    // Build positions array and hash table
    std::vector<uint32_t> positions_array;
    std::vector<HashEntryMulti> hash_entries;

    for (auto& [key, positions] : groups) {
        uint32_t offset = positions_array.size();
        uint32_t count = positions.size();
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
        hash_entries.push_back({key, offset, count});
    }

    // Determine hash table size (power of 2, load factor ~0.6)
    uint32_t num_unique = hash_entries.size();
    uint32_t table_size = 1;
    while (table_size < num_unique * 2) table_size *= 2;

    // Build hash table with open addressing
    std::vector<HashEntryMulti> hash_table(table_size, {-1, 0, 0});

    for (const auto& entry : hash_entries) {
        uint64_t h = hash_int32(entry.key) % table_size;
        while (hash_table[h].key != -1) {
            h = (h + 1) % table_size;
        }
        hash_table[h] = entry;
    }

    // Write hash index file
    std::ofstream f(output_file, std::ios::binary);
    f.write((const char*)&num_unique, sizeof(num_unique));
    f.write((const char*)&table_size, sizeof(table_size));

    for (const auto& entry : hash_table) {
        f.write((const char*)&entry.key, sizeof(entry.key));
        f.write((const char*)&entry.offset, sizeof(entry.offset));
        f.write((const char*)&entry.count, sizeof(entry.count));
    }

    uint32_t positions_count = positions_array.size();
    f.write((const char*)&positions_count, sizeof(positions_count));
    f.write((const char*)positions_array.data(), positions_count * sizeof(uint32_t));

    f.close();

    std::cout << "Hash index (multi) " << output_file << ": " << num_unique << " unique keys, " << positions_count << " positions\n";
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "Building indexes for TPC-H...\n";

    // Lineitem indexes
    std::cout << "\nLineitem indexes:\n";
    build_zone_map(gendb_dir + "/lineitem/l_shipdate.bin", gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", 100000);
    build_hash_multi(gendb_dir + "/lineitem/l_orderkey.bin", gendb_dir + "/indexes/lineitem_orderkey_hash.bin");
    build_hash_multi(gendb_dir + "/lineitem/l_suppkey.bin", gendb_dir + "/indexes/lineitem_suppkey_hash.bin");

    // Orders indexes
    std::cout << "\nOrders indexes:\n";
    build_zone_map(gendb_dir + "/orders/o_orderdate.bin", gendb_dir + "/indexes/orders_orderdate_zonemap.bin", 100000);
    build_hash_single(gendb_dir + "/orders/o_orderkey.bin", gendb_dir + "/indexes/orders_orderkey_hash.bin");
    build_hash_multi(gendb_dir + "/orders/o_custkey.bin", gendb_dir + "/indexes/orders_custkey_hash.bin");

    // Customer indexes
    std::cout << "\nCustomer indexes:\n";
    build_hash_single(gendb_dir + "/customer/c_custkey.bin", gendb_dir + "/indexes/customer_custkey_hash.bin");

    // Part indexes
    std::cout << "\nPart indexes:\n";
    build_hash_single(gendb_dir + "/part/p_partkey.bin", gendb_dir + "/indexes/part_partkey_hash.bin");

    // Supplier indexes
    std::cout << "\nSupplier indexes:\n";
    build_hash_single(gendb_dir + "/supplier/s_suppkey.bin", gendb_dir + "/indexes/supplier_suppkey_hash.bin");

    // Partsupp indexes
    std::cout << "\nPartsupp indexes:\n";
    build_hash_multi(gendb_dir + "/partsupp/ps_partkey.bin", gendb_dir + "/indexes/partsupp_partkey_hash.bin");
    build_hash_multi(gendb_dir + "/partsupp/ps_suppkey.bin", gendb_dir + "/indexes/partsupp_suppkey_hash.bin");

    // Nation indexes
    std::cout << "\nNation indexes:\n";
    build_hash_single(gendb_dir + "/nation/n_nationkey.bin", gendb_dir + "/indexes/nation_nationkey_hash.bin");

    // Region indexes
    std::cout << "\nRegion indexes:\n";
    build_hash_single(gendb_dir + "/region/r_regionkey.bin", gendb_dir + "/indexes/region_regionkey_hash.bin");

    std::cout << "\nIndex building complete!\n";
    return 0;
}
