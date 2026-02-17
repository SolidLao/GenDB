#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace fs = std::filesystem;

// Hash function for multi-value hash indexes
inline uint32_t hash_key(int32_t key) {
    uint64_t h = static_cast<uint64_t>(key) * 0x9E3779B97F4A7C15ULL;
    return static_cast<uint32_t>(h >> 32);
}

// Load binary column file via mmap
template <typename T>
const T* load_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size == 0) {
        close(fd);
        count = 0;
        return nullptr;
    }

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed: " + path);
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);
    close(fd);

    count = file_size / sizeof(T);
    return reinterpret_cast<const T*>(mapped);
}

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

struct ZoneMapDecimal {
    int64_t min_val;
    int64_t max_val;
    uint32_t row_count;
};

// Multi-value hash index structure
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Build zone map for a column (int32_t)
void build_zone_map_int32(const std::string& column_path, const std::string& output_path,
                          size_t block_size = 100000) {
    std::cout << "Building zone map: " << fs::path(column_path).filename() << std::endl;

    size_t count;
    const int32_t* data = load_column<int32_t>(column_path, count);
    if (!data) {
        std::cout << "  Column is empty, skipping zone map" << std::endl;
        return;
    }

    size_t num_blocks = (count + block_size - 1) / block_size;
    std::vector<ZoneMap> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start_idx = b * block_size;
        size_t end_idx = std::min(start_idx + block_size, count);

        int32_t block_min = data[start_idx];
        int32_t block_max = data[start_idx];

        for (size_t i = start_idx + 1; i < end_idx; ++i) {
            block_min = std::min(block_min, data[i]);
            block_max = std::max(block_max, data[i]);
        }

        zones[b] = {block_min, block_max, static_cast<uint32_t>(end_idx - start_idx)};
    }

    // Write zone map
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_zones = zones.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(num_zones));
    for (const auto& zone : zones) {
        out.write(reinterpret_cast<const char*>(&zone.min_val), sizeof(zone.min_val));
        out.write(reinterpret_cast<const char*>(&zone.max_val), sizeof(zone.max_val));
        out.write(reinterpret_cast<const char*>(&zone.row_count), sizeof(zone.row_count));
    }
    out.close();

    std::cout << "  " << num_zones << " blocks, zone map written" << std::endl;
}

// Build zone map for DECIMAL (int64_t)
void build_zone_map_decimal(const std::string& column_path, const std::string& output_path,
                            size_t block_size = 100000) {
    std::cout << "Building zone map (decimal): " << fs::path(column_path).filename() << std::endl;

    size_t count;
    const int64_t* data = load_column<int64_t>(column_path, count);
    if (!data) {
        std::cout << "  Column is empty, skipping zone map" << std::endl;
        return;
    }

    size_t num_blocks = (count + block_size - 1) / block_size;
    std::vector<ZoneMapDecimal> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start_idx = b * block_size;
        size_t end_idx = std::min(start_idx + block_size, count);

        int64_t block_min = data[start_idx];
        int64_t block_max = data[start_idx];

        for (size_t i = start_idx + 1; i < end_idx; ++i) {
            block_min = std::min(block_min, data[i]);
            block_max = std::max(block_max, data[i]);
        }

        zones[b] = {block_min, block_max, static_cast<uint32_t>(end_idx - start_idx)};
    }

    // Write zone map
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_zones = zones.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(num_zones));
    for (const auto& zone : zones) {
        out.write(reinterpret_cast<const char*>(&zone.min_val), sizeof(zone.min_val));
        out.write(reinterpret_cast<const char*>(&zone.max_val), sizeof(zone.max_val));
        out.write(reinterpret_cast<const char*>(&zone.row_count), sizeof(zone.row_count));
    }
    out.close();

    std::cout << "  " << num_zones << " blocks, zone map written" << std::endl;
}

// Build multi-value hash index
void build_hash_index(const std::string& column_path, const std::string& output_path) {
    std::cout << "Building hash index: " << fs::path(column_path).filename() << std::endl;

    size_t count;
    const int32_t* data = load_column<int32_t>(column_path, count);
    if (!data) {
        std::cout << "  Column is empty, skipping hash index" << std::endl;
        return;
    }

    // Phase 1: Group positions by key (single-threaded for simplicity)
    std::unordered_map<int32_t, std::vector<uint32_t>> key_positions;
    for (size_t i = 0; i < count; ++i) {
        int32_t key = data[i];
        key_positions[key].push_back(static_cast<uint32_t>(i));
    }

    // Phase 2: Build positions array and hash table
    std::vector<uint32_t> positions_array;
    std::vector<std::pair<int32_t, HashIndexEntry>> hash_entries;

    uint32_t current_offset = 0;
    for (auto& [key, positions] : key_positions) {
        uint32_t pos_count = positions.size();
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
        hash_entries.push_back({key, {key, current_offset, pos_count}});
        current_offset += pos_count;
    }

    // Phase 3: Build hash table with open addressing
    size_t num_unique = hash_entries.size();
    size_t table_size = std::max(static_cast<size_t>(num_unique * 2), size_t(16));
    table_size = 1 << (32 - __builtin_clz(static_cast<uint32_t>(table_size - 1)));

    std::vector<HashIndexEntry> hash_table(table_size, {-1, 0, 0});

    for (const auto& [key, entry] : hash_entries) {
        uint32_t h = hash_key(key) & (table_size - 1);
        while (hash_table[h].key != -1) {
            h = (h + 1) & (table_size - 1);
        }
        hash_table[h] = entry;
    }

    // Write hash index
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_unique_u32 = num_unique;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_unique_u32), sizeof(num_unique_u32));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));

    for (size_t i = 0; i < table_size; ++i) {
        out.write(reinterpret_cast<const char*>(&hash_table[i].key), sizeof(hash_table[i].key));
        out.write(reinterpret_cast<const char*>(&hash_table[i].offset), sizeof(hash_table[i].offset));
        out.write(reinterpret_cast<const char*>(&hash_table[i].count), sizeof(hash_table[i].count));
    }

    uint32_t pos_array_size = positions_array.size();
    out.write(reinterpret_cast<const char*>(&pos_array_size), sizeof(pos_array_size));
    for (uint32_t pos : positions_array) {
        out.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
    }
    out.close();

    std::cout << "  " << num_unique << " unique keys, hash table size: " << table_size
              << ", positions array: " << positions_array.size() << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    fs::create_directories(gendb_dir + "/indexes");

    std::cout << "Building indexes for TPC-H SF10..." << std::endl << std::endl;

    // Lineitem indexes
    try {
        build_zone_map_int32(gendb_dir + "/lineitem/l_shipdate.bin",
                             gendb_dir + "/indexes/idx_lineitem_shipdate_zmap.bin");
        build_zone_map_decimal(gendb_dir + "/lineitem/l_discount.bin",
                               gendb_dir + "/indexes/idx_lineitem_discount_zmap.bin");
        build_zone_map_decimal(gendb_dir + "/lineitem/l_quantity.bin",
                               gendb_dir + "/indexes/idx_lineitem_quantity_zmap.bin");
        build_hash_index(gendb_dir + "/lineitem/l_orderkey.bin",
                         gendb_dir + "/indexes/idx_lineitem_orderkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building lineitem indexes: " << e.what() << std::endl;
    }

    // Orders indexes
    try {
        build_zone_map_int32(gendb_dir + "/orders/o_orderdate.bin",
                             gendb_dir + "/indexes/idx_orders_orderdate_zmap.bin");
        build_hash_index(gendb_dir + "/orders/o_orderkey.bin",
                         gendb_dir + "/indexes/idx_orders_orderkey_hash.bin");
        build_hash_index(gendb_dir + "/orders/o_custkey.bin",
                         gendb_dir + "/indexes/idx_orders_custkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building orders indexes: " << e.what() << std::endl;
    }

    // Customer indexes
    try {
        build_hash_index(gendb_dir + "/customer/c_custkey.bin",
                         gendb_dir + "/indexes/idx_customer_custkey_hash.bin");
        build_hash_index(gendb_dir + "/customer/c_nationkey.bin",
                         gendb_dir + "/indexes/idx_customer_nationkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building customer indexes: " << e.what() << std::endl;
    }

    // Part indexes
    try {
        build_hash_index(gendb_dir + "/part/p_partkey.bin",
                         gendb_dir + "/indexes/idx_part_partkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building part indexes: " << e.what() << std::endl;
    }

    // Partsupp indexes
    try {
        build_hash_index(gendb_dir + "/partsupp/ps_partkey.bin",
                         gendb_dir + "/indexes/idx_partsupp_partkey_hash.bin");
        build_hash_index(gendb_dir + "/partsupp/ps_suppkey.bin",
                         gendb_dir + "/indexes/idx_partsupp_suppkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building partsupp indexes: " << e.what() << std::endl;
    }

    // Supplier indexes
    try {
        build_hash_index(gendb_dir + "/supplier/s_suppkey.bin",
                         gendb_dir + "/indexes/idx_supplier_suppkey_hash.bin");
        build_hash_index(gendb_dir + "/supplier/s_nationkey.bin",
                         gendb_dir + "/indexes/idx_supplier_nationkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building supplier indexes: " << e.what() << std::endl;
    }

    // Nation indexes
    try {
        build_hash_index(gendb_dir + "/nation/n_nationkey.bin",
                         gendb_dir + "/indexes/idx_nation_nationkey_hash.bin");
        build_hash_index(gendb_dir + "/nation/n_regionkey.bin",
                         gendb_dir + "/indexes/idx_nation_regionkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building nation indexes: " << e.what() << std::endl;
    }

    // Region indexes
    try {
        build_hash_index(gendb_dir + "/region/r_regionkey.bin",
                         gendb_dir + "/indexes/idx_region_regionkey_hash.bin");
    } catch (const std::exception& e) {
        std::cerr << "Error building region indexes: " << e.what() << std::endl;
    }

    std::cout << "\n✓ Index building complete." << std::endl;
    return 0;
}
