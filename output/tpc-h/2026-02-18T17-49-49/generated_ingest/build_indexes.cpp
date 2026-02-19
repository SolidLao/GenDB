#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Multiply-shift hash function
uint64_t hash_multiply_shift(int32_t key) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
}

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
    uint32_t row_offset;
};

// Build zone map for a column
void build_zone_map(const std::string& col_file, const std::string& output_file,
                    uint32_t block_size, uint32_t total_rows) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << col_file << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << "\n";
        close(fd);
        return;
    }

    // Read dates as int32_t
    const int32_t* data = reinterpret_cast<const int32_t*>(addr);
    uint32_t total_entries = sb.st_size / sizeof(int32_t);

    std::vector<ZoneMapEntry> zones;
    uint32_t block_idx = 0;
    uint32_t rows_processed = 0;

    while (rows_processed < total_entries) {
        uint32_t current_block_size = std::min(block_size, total_entries - rows_processed);

        int32_t min_val = INT32_MAX;
        int32_t max_val = INT32_MIN;

        #pragma omp parallel for reduction(min:min_val) reduction(max:max_val)
        for (uint32_t i = 0; i < current_block_size; ++i) {
            int32_t val = data[rows_processed + i];
            if (val < min_val) min_val = val;
            if (val > max_val) max_val = val;
        }

        ZoneMapEntry entry{min_val, max_val, current_block_size, rows_processed};
        zones.push_back(entry);

        rows_processed += current_block_size;
        block_idx++;
    }

    // Write zone map
    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_zones = zones.size();
    out.write(reinterpret_cast<char*>(&num_zones), sizeof(uint32_t));
    for (const auto& zone : zones) {
        out.write(reinterpret_cast<char*>(const_cast<ZoneMapEntry*>(&zone)), sizeof(ZoneMapEntry));
    }
    out.close();

    munmap(addr, sb.st_size);
    close(fd);
}

// Build hash multi-value index for join columns
void build_hash_multi_value(const std::string& col_file, const std::string& output_file,
                           uint32_t total_rows) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << col_file << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << "\n";
        close(fd);
        return;
    }

    const int32_t* data = reinterpret_cast<const int32_t*>(addr);
    uint32_t num_rows = sb.st_size / sizeof(int32_t);

    // Create position array and sort by key
    std::vector<uint32_t> positions(num_rows);
    #pragma omp parallel for
    for (uint32_t i = 0; i < num_rows; ++i) {
        positions[i] = i;
    }

    // Sort positions by key value
    std::sort(positions.begin(), positions.end(),
             [data](uint32_t a, uint32_t b) { return data[a] < data[b]; });

    // Group by key and build hash table
    std::map<int32_t, std::pair<uint32_t, uint32_t>> groups; // key -> (offset, count)
    std::vector<uint32_t> pos_array;

    uint32_t current_offset = 0;
    int32_t last_key = INT32_MIN - 1;

    for (uint32_t i = 0; i < num_rows; ++i) {
        int32_t key = data[positions[i]];
        if (key != last_key) {
            if (last_key != INT32_MIN - 1) {
                groups[last_key] = {current_offset, (uint32_t)pos_array.size() - current_offset};
            }
            current_offset = pos_array.size();
            last_key = key;
        }
        pos_array.push_back(positions[i]);
    }
    if (last_key != INT32_MIN - 1) {
        groups[last_key] = {current_offset, (uint32_t)pos_array.size() - current_offset};
    }

    // Build hash table
    uint32_t table_size = groups.size() * 2;
    if (table_size < 256) table_size = 256;
    while ((table_size & (table_size - 1)) != 0) table_size++;

    std::vector<std::tuple<int32_t, uint32_t, uint32_t>> hash_table(table_size, {-1, 0, 0});

    for (const auto& [key, group_info] : groups) {
        uint64_t hash_val = hash_multiply_shift(key);
        uint32_t idx = hash_val & (table_size - 1);

        while (std::get<0>(hash_table[idx]) != -1) {
            idx = (idx + 1) & (table_size - 1);
        }

        hash_table[idx] = {key, group_info.first, group_info.second};
    }

    // Write output
    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_unique = groups.size();
    out.write(reinterpret_cast<char*>(&num_unique), sizeof(uint32_t));
    out.write(reinterpret_cast<char*>(&table_size), sizeof(uint32_t));

    for (const auto& entry : hash_table) {
        int32_t key = std::get<0>(entry);
        uint32_t offset = std::get<1>(entry);
        uint32_t count = std::get<2>(entry);
        out.write(reinterpret_cast<char*>(&key), sizeof(int32_t));
        out.write(reinterpret_cast<char*>(&offset), sizeof(uint32_t));
        out.write(reinterpret_cast<char*>(&count), sizeof(uint32_t));
    }

    // Write positions array
    uint32_t pos_count = pos_array.size();
    out.write(reinterpret_cast<char*>(&pos_count), sizeof(uint32_t));
    for (uint32_t pos : pos_array) {
        out.write(reinterpret_cast<char*>(&pos), sizeof(uint32_t));
    }

    out.close();

    munmap(addr, sb.st_size);
    close(fd);
}

// Build hash single-value index for primary keys
void build_hash_single(const std::string& col_file, const std::string& output_file,
                      uint32_t total_rows) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << col_file << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << "\n";
        close(fd);
        return;
    }

    const int32_t* data = reinterpret_cast<const int32_t*>(addr);
    uint32_t num_rows = sb.st_size / sizeof(int32_t);

    // Build hash table with single row per key
    uint32_t table_size = num_rows * 2;
    if (table_size < 256) table_size = 256;
    while ((table_size & (table_size - 1)) != 0) table_size++;

    std::vector<std::pair<int32_t, uint32_t>> hash_table(table_size, {-1, UINT32_MAX});

    for (uint32_t i = 0; i < num_rows; ++i) {
        int32_t key = data[i];
        uint64_t hash_val = hash_multiply_shift(key);
        uint32_t idx = hash_val & (table_size - 1);

        while (hash_table[idx].first != -1) {
            idx = (idx + 1) & (table_size - 1);
        }

        hash_table[idx] = {key, i};
    }

    // Write output
    std::ofstream out(output_file, std::ios::binary);
    out.write(reinterpret_cast<char*>(&table_size), sizeof(uint32_t));
    for (const auto& entry : hash_table) {
        out.write(reinterpret_cast<char*>(const_cast<int32_t*>(&entry.first)), sizeof(int32_t));
        out.write(reinterpret_cast<char*>(const_cast<uint32_t*>(&entry.second)), sizeof(uint32_t));
    }
    out.close();

    munmap(addr, sb.st_size);
    close(fd);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string indexes_dir = gendb_dir + "/indexes";
    mkdir(indexes_dir.c_str(), 0755);

    std::cout << "Building indexes...\n";

    // Zone maps
    std::cout << "Building zone map: lineitem.l_shipdate\n";
    build_zone_map(gendb_dir + "/lineitem/l_shipdate.bin",
                   indexes_dir + "/lineitem_l_shipdate.bin",
                   100000, 59986052);

    std::cout << "Building zone map: orders.o_orderdate\n";
    build_zone_map(gendb_dir + "/orders/o_orderdate.bin",
                   indexes_dir + "/orders_o_orderdate.bin",
                   100000, 15000000);

    // Hash multi-value indexes
    std::cout << "Building hash multi-value index: lineitem.l_orderkey\n";
    build_hash_multi_value(gendb_dir + "/lineitem/l_orderkey.bin",
                          indexes_dir + "/lineitem_l_orderkey.bin",
                          59986052);

    std::cout << "Building hash multi-value index: orders.o_custkey\n";
    build_hash_multi_value(gendb_dir + "/orders/o_custkey.bin",
                          indexes_dir + "/orders_o_custkey.bin",
                          15000000);

    std::cout << "Building hash multi-value index: partsupp composite\n";
    build_hash_multi_value(gendb_dir + "/partsupp/ps_partkey.bin",
                          indexes_dir + "/partsupp_ps_partkey.bin",
                          8000000);

    // Hash single-value indexes
    std::cout << "Building hash single-value index: customer.c_custkey\n";
    build_hash_single(gendb_dir + "/customer/c_custkey.bin",
                     indexes_dir + "/customer_c_custkey.bin",
                     1500000);

    std::cout << "Building hash single-value index: part.p_partkey\n";
    build_hash_single(gendb_dir + "/part/p_partkey.bin",
                     indexes_dir + "/part_p_partkey.bin",
                     2000000);

    std::cout << "Building hash single-value index: supplier.s_suppkey\n";
    build_hash_single(gendb_dir + "/supplier/s_suppkey.bin",
                     indexes_dir + "/supplier_s_suppkey.bin",
                     100000);

    std::cout << "Index building complete!\n";
    return 0;
}
