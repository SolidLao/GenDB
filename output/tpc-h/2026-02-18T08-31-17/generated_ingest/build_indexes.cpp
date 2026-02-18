#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <omp.h>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================================
// Zone Map: Min/Max per block
// ============================================================================
template <typename T>
struct ZoneMapEntry {
    T min_val;
    T max_val;
};

template <typename T>
void build_zone_map(const std::string& column_file, const std::string& output_file,
                    uint32_t block_size) {
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    size_t num_rows = file_size / sizeof(T);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "mmap failed for " << column_file << std::endl;
        close(fd);
        return;
    }

    const T* data = static_cast<const T*>(mapped);

    uint32_t num_blocks = (num_rows + block_size - 1) / block_size;
    std::vector<ZoneMapEntry<T>> zones(num_blocks);

#pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; ++b) {
        uint32_t start = b * block_size;
        uint32_t end = std::min(start + block_size, (uint32_t)num_rows);

        T min_val = data[start];
        T max_val = data[start];

        for (uint32_t i = start + 1; i < end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zones[b].min_val = min_val;
        zones[b].max_val = max_val;
    }

    std::ofstream out(output_file, std::ios::binary);
    uint32_t zone_count = zones.size();
    out.write(reinterpret_cast<char*>(&zone_count), sizeof(zone_count));
    out.write(reinterpret_cast<char*>(zones.data()), zones.size() * sizeof(ZoneMapEntry<T>));
    out.close();

    munmap(mapped, file_size);
    close(fd);

    std::cout << "Zone map: " << column_file << " -> " << output_file << " (" << zone_count
              << " zones)\n";
}

// ============================================================================
// Hash Index: Multi-value design for join columns
// Hash key -> [offset in positions array, count]
// ============================================================================
template <typename K>
void build_hash_index(const std::string& column_file, const std::string& output_file,
                      uint32_t num_rows) {
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    if (num_rows == 0) {
        num_rows = file_size / sizeof(K);
    }

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "mmap failed for " << column_file << std::endl;
        close(fd);
        return;
    }

    const K* data = static_cast<const K*>(mapped);

    try {
        // Phase 1: Histogram – count unique keys and per-key positions
        std::unordered_map<K, std::vector<uint32_t>> key_positions;
        key_positions.reserve(std::min(num_rows / 100, 10000000u));  // Reserve space to avoid too many rehashes

        for (uint32_t i = 0; i < num_rows; ++i) {
            key_positions[data[i]].push_back(i);
        }

        // Phase 2: Build hash table
        // Hash table entry: [key, offset, count] (each 4+4+4 = 12 bytes)
        uint32_t num_unique = key_positions.size();
        uint32_t table_size = (num_unique * 200) / 100;  // ~2x load factor (50% fill)
        if (table_size == 0) table_size = 1;

        // Align to power of 2 for fast modulo
        uint32_t p2_size = 1;
        while (p2_size < table_size) p2_size <<= 1;
        table_size = p2_size;

        std::vector<uint32_t> hash_table(table_size * 3, 0);  // [key, offset, count] per slot
        std::vector<uint32_t> positions_array;
        positions_array.reserve(num_rows);

        uint32_t current_offset = 0;
        for (const auto& [key, pos_vec] : key_positions) {
            // Linear probing
            uint32_t hash = std::hash<K>{}(key);
            uint32_t slot = hash & (table_size - 1);

            while (hash_table[slot * 3] != 0 && hash_table[slot * 3] != key) {
                slot = (slot + 1) & (table_size - 1);
            }

            hash_table[slot * 3] = key;
            hash_table[slot * 3 + 1] = current_offset;
            hash_table[slot * 3 + 2] = pos_vec.size();

            for (uint32_t pos : pos_vec) {
                positions_array.push_back(pos);
            }
            current_offset += pos_vec.size();
        }

        // Write to file
        std::ofstream out(output_file, std::ios::binary);
        out.write(reinterpret_cast<char*>(&num_unique), sizeof(num_unique));
        out.write(reinterpret_cast<char*>(&table_size), sizeof(table_size));
        out.write(reinterpret_cast<char*>(hash_table.data()), hash_table.size() * sizeof(uint32_t));
        out.write(reinterpret_cast<char*>(positions_array.data()),
                  positions_array.size() * sizeof(uint32_t));
        out.close();

        std::cout << "Hash index: " << column_file << " -> " << output_file << " (" << num_unique
                  << " unique keys, table_size=" << table_size << ")\n";
    } catch (const std::exception& e) {
        std::cerr << "Error building hash index: " << e.what() << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);
}

// ============================================================================
// Composite Hash Index: (Key1, Key2) tuples
// ============================================================================
void build_composite_hash_index(const std::string& col1_file, const std::string& col2_file,
                                const std::string& output_file, uint32_t num_rows) {
    int fd1 = open(col1_file.c_str(), O_RDONLY);
    int fd2 = open(col2_file.c_str(), O_RDONLY);
    if (fd1 < 0 || fd2 < 0) {
        std::cerr << "Cannot open input files\n";
        return;
    }

    size_t file_size = lseek(fd1, 0, SEEK_END);
    if (num_rows == 0) {
        num_rows = file_size / sizeof(int32_t);
    }

    void* map1 = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd1, 0);
    void* map2 = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd2, 0);
    if (map1 == MAP_FAILED || map2 == MAP_FAILED) {
        std::cerr << "mmap failed\n";
        return;
    }

    const int32_t* data1 = static_cast<const int32_t*>(map1);
    const int32_t* data2 = static_cast<const int32_t*>(map2);

    try {
        // Build composite key histogram
        std::unordered_map<uint64_t, std::vector<uint32_t>> key_positions;
        key_positions.reserve(std::min(num_rows / 100, 10000000u));

        for (uint32_t i = 0; i < num_rows; ++i) {
            uint64_t composite_key = ((uint64_t)data1[i] << 32) | (uint32_t)data2[i];
            key_positions[composite_key].push_back(i);
        }

        uint32_t num_unique = key_positions.size();
        uint32_t table_size = 1;
        while (table_size < num_unique * 2) table_size <<= 1;

        // Write composite hash index: [num_unique][table_size] then per-slot [key(8), offset, count]
        std::ofstream out(output_file, std::ios::binary);
        out.write(reinterpret_cast<char*>(&num_unique), sizeof(num_unique));
        out.write(reinterpret_cast<char*>(&table_size), sizeof(table_size));

        std::vector<uint64_t> composite_table(table_size, 0);
        std::vector<uint32_t> offsets(table_size, 0);
        std::vector<uint32_t> counts(table_size, 0);
        std::vector<uint32_t> positions_array;
        positions_array.reserve(num_rows);

        uint32_t current_offset = 0;
        for (const auto& [key, pos_vec] : key_positions) {
            uint32_t hash = std::hash<uint64_t>{}(key);
            uint32_t slot = hash & (table_size - 1);

            while (composite_table[slot] != 0 && composite_table[slot] != key) {
                slot = (slot + 1) & (table_size - 1);
            }

            composite_table[slot] = key;
            offsets[slot] = current_offset;
            counts[slot] = pos_vec.size();

            for (uint32_t pos : pos_vec) {
                positions_array.push_back(pos);
            }
            current_offset += pos_vec.size();
        }

        out.write(reinterpret_cast<char*>(composite_table.data()),
                  composite_table.size() * sizeof(uint64_t));
        out.write(reinterpret_cast<char*>(offsets.data()), offsets.size() * sizeof(uint32_t));
        out.write(reinterpret_cast<char*>(counts.data()), counts.size() * sizeof(uint32_t));
        out.write(reinterpret_cast<char*>(positions_array.data()),
                  positions_array.size() * sizeof(uint32_t));
        out.close();

        std::cout << "Composite hash index: (" << col1_file << ", " << col2_file
                  << ") -> " << output_file << " (" << num_unique << " unique keys)\n";
    } catch (const std::exception& e) {
        std::cerr << "Error building composite hash index: " << e.what() << std::endl;
    }

    munmap(map1, file_size);
    munmap(map2, file_size);
    close(fd1);
    close(fd2);
}

// ============================================================================
// Main: Build all indexes
// ============================================================================
int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    fs::create_directories(gendb_dir + "/indexes");

    // ========== LINEITEM INDEXES ==========
    std::cout << "\n=== Building lineitem indexes ===\n";

    // Zone map for l_shipdate
    build_zone_map<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",
                            gendb_dir + "/indexes/lineitem_shipdate_zone.bin", 100000);

    // Hash indexes
    build_hash_index<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",
                              gendb_dir + "/indexes/lineitem_orderkey_hash.bin", 59986052);
    build_hash_index<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin",
                              gendb_dir + "/indexes/lineitem_suppkey_hash.bin", 59986052);

    // ========== ORDERS INDEXES ==========
    std::cout << "\n=== Building orders indexes ===\n";

    build_zone_map<int32_t>(gendb_dir + "/orders/o_orderdate.bin",
                            gendb_dir + "/indexes/orders_orderdate_zone.bin", 100000);
    build_hash_index<int32_t>(gendb_dir + "/orders/o_custkey.bin",
                              gendb_dir + "/indexes/orders_custkey_hash.bin", 15000000);
    build_hash_index<int32_t>(gendb_dir + "/orders/o_orderkey.bin",
                              gendb_dir + "/indexes/orders_orderkey_hash.bin", 15000000);

    // ========== CUSTOMER INDEXES ==========
    std::cout << "\n=== Building customer indexes ===\n";

    build_hash_index<int32_t>(gendb_dir + "/customer/c_custkey.bin",
                              gendb_dir + "/indexes/customer_custkey_hash.bin", 1500000);
    build_hash_index<int32_t>(gendb_dir + "/customer/c_mktsegment.bin",
                              gendb_dir + "/indexes/customer_mktsegment_hash.bin", 1500000);

    // ========== PARTSUPP INDEXES ==========
    std::cout << "\n=== Building partsupp indexes ===\n";

    // Composite (ps_partkey, ps_suppkey)
    build_composite_hash_index(gendb_dir + "/partsupp/ps_partkey.bin",
                               gendb_dir + "/partsupp/ps_suppkey.bin",
                               gendb_dir + "/indexes/partsupp_composite_hash.bin", 8000000);

    build_hash_index<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin",
                              gendb_dir + "/indexes/partsupp_suppkey_hash.bin", 8000000);

    // ========== PART INDEXES ==========
    std::cout << "\n=== Building part indexes ===\n";

    build_hash_index<int32_t>(gendb_dir + "/part/p_partkey.bin",
                              gendb_dir + "/indexes/part_partkey_hash.bin", 2000000);
    build_hash_index<int32_t>(gendb_dir + "/part/p_name.bin",
                              gendb_dir + "/indexes/part_name_hash.bin", 2000000);

    // ========== SUPPLIER INDEXES ==========
    std::cout << "\n=== Building supplier indexes ===\n";

    build_hash_index<int32_t>(gendb_dir + "/supplier/s_suppkey.bin",
                              gendb_dir + "/indexes/supplier_suppkey_hash.bin", 100000);
    build_hash_index<int32_t>(gendb_dir + "/supplier/s_nationkey.bin",
                              gendb_dir + "/indexes/supplier_nationkey_hash.bin", 100000);

    // ========== NATION INDEXES ==========
    std::cout << "\n=== Building nation indexes ===\n";

    build_hash_index<int32_t>(gendb_dir + "/nation/n_nationkey.bin",
                              gendb_dir + "/indexes/nation_nationkey_hash.bin", 25);

    std::cout << "\n=== Index building complete! ===\n";
    return 0;
}
