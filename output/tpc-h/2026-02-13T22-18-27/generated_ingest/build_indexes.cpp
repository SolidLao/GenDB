#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    size_t block_start;
    size_t block_end;
};

// Build zone maps for an int32 column
void build_zone_map_int32(const std::string& column_file, const std::string& index_file, size_t row_count, size_t block_size) {
    std::cout << "Building zone map for " << column_file << std::endl;

    // Open and mmap the column file
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = row_count * sizeof(int32_t);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << column_file << std::endl;
        close(fd);
        return;
    }

    // Build zone maps
    std::vector<ZoneMap> zones;
    for (size_t block_start = 0; block_start < row_count; block_start += block_size) {
        size_t block_end = std::min(block_start + block_size, row_count);

        int32_t min_val = data[block_start];
        int32_t max_val = data[block_start];

        for (size_t i = block_start; i < block_end; i++) {
            min_val = std::min(min_val, data[i]);
            max_val = std::max(max_val, data[i]);
        }

        zones.push_back({min_val, max_val, block_start, block_end});
    }

    // Write zone map to file
    std::ofstream out(index_file, std::ios::binary);
    size_t num_zones = zones.size();
    out.write((char*)&num_zones, sizeof(num_zones));
    out.write((char*)zones.data(), zones.size() * sizeof(ZoneMap));

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "  Created " << zones.size() << " zones" << std::endl;
}

// Build sorted index (array of row indices sorted by column value)
void build_sorted_index_int32(const std::string& column_file, const std::string& index_file, size_t row_count) {
    std::cout << "Building sorted index for " << column_file << std::endl;

    // Open and mmap the column file
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = row_count * sizeof(int32_t);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << column_file << std::endl;
        close(fd);
        return;
    }

    // Create pairs of (value, row_index)
    std::vector<std::pair<int32_t, uint32_t>> pairs;
    pairs.reserve(row_count);
    for (size_t i = 0; i < row_count; i++) {
        pairs.push_back({data[i], (uint32_t)i});
    }

    // Sort by value
    std::sort(pairs.begin(), pairs.end());

    // Write sorted index: array of row indices
    std::ofstream out(index_file, std::ios::binary);
    for (const auto& p : pairs) {
        out.write((char*)&p.second, sizeof(p.second));
    }

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "  Sorted " << row_count << " entries" << std::endl;
}

// Build hash index (simple perfect hash for primary keys)
void build_hash_index_int32(const std::string& column_file, const std::string& index_file, size_t row_count) {
    std::cout << "Building hash index for " << column_file << std::endl;

    // Open and mmap the column file
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << column_file << std::endl;
        return;
    }

    size_t file_size = row_count * sizeof(int32_t);
    const int32_t* data = (const int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << column_file << std::endl;
        close(fd);
        return;
    }

    // Build hash map: key -> row_index
    std::unordered_map<int32_t, uint32_t> hash_map;
    for (size_t i = 0; i < row_count; i++) {
        hash_map[data[i]] = i;
    }

    // Write hash index: simple format (key, row_index) pairs
    std::ofstream out(index_file, std::ios::binary);
    size_t num_entries = hash_map.size();
    out.write((char*)&num_entries, sizeof(num_entries));
    for (const auto& kv : hash_map) {
        out.write((char*)&kv.first, sizeof(kv.first));
        out.write((char*)&kv.second, sizeof(kv.second));
    }

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "  Created hash index with " << num_entries << " entries" << std::endl;
}

// Read row count from metadata.json
size_t read_row_count(const std::string& gendb_dir, const std::string& table_name) {
    std::ifstream f(gendb_dir + "/metadata.json");
    if (!f) {
        std::cerr << "Cannot open metadata.json" << std::endl;
        return 0;
    }

    std::string line;
    bool in_table = false;
    while (std::getline(f, line)) {
        if (line.find("\"" + table_name + "\"") != std::string::npos) {
            in_table = true;
        }
        if (in_table && line.find("\"row_count\"") != std::string::npos) {
            size_t pos = line.find(":");
            if (pos != std::string::npos) {
                std::string count_str = line.substr(pos + 1);
                // Extract only digits
                std::string digits;
                for (char c : count_str) {
                    if (c >= '0' && c <= '9') {
                        digits += c;
                    }
                }
                if (!digits.empty()) {
                    try {
                        return std::stoull(digits);
                    } catch (...) {
                        std::cerr << "Failed to parse row_count: '" << digits << "'" << std::endl;
                        return 0;
                    }
                }
            }
        }
    }
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Index Builder" << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    const size_t BLOCK_SIZE = 100000;

    // Lineitem indexes
    {
        std::cout << "=== Building lineitem indexes ===" << std::endl;
        size_t lineitem_rows = read_row_count(gendb_dir, "lineitem");
        if (lineitem_rows == 0) {
            std::cerr << "Failed to read lineitem row count" << std::endl;
            return 1;
        }
        std::cout << "Row count: " << lineitem_rows << std::endl;

        // Zone maps
        build_zone_map_int32(gendb_dir + "/lineitem.l_shipdate.bin",
                            gendb_dir + "/lineitem.l_shipdate.zonemap.idx",
                            lineitem_rows, BLOCK_SIZE);

        build_zone_map_int32(gendb_dir + "/lineitem.l_quantity.bin",
                            gendb_dir + "/lineitem.l_quantity.zonemap.idx",
                            lineitem_rows, BLOCK_SIZE);

        build_zone_map_int32(gendb_dir + "/lineitem.l_discount.bin",
                            gendb_dir + "/lineitem.l_discount.zonemap.idx",
                            lineitem_rows, BLOCK_SIZE);

        // Sorted index on orderkey
        build_sorted_index_int32(gendb_dir + "/lineitem.l_orderkey.bin",
                                gendb_dir + "/lineitem.l_orderkey.sorted.idx",
                                lineitem_rows);

        std::cout << std::endl;
    }

    // Orders indexes
    {
        std::cout << "=== Building orders indexes ===" << std::endl;
        size_t orders_rows = read_row_count(gendb_dir, "orders");
        if (orders_rows == 0) {
            std::cerr << "Failed to read orders row count" << std::endl;
            return 1;
        }
        std::cout << "Row count: " << orders_rows << std::endl;

        // Hash index on orderkey (primary key)
        build_hash_index_int32(gendb_dir + "/orders.o_orderkey.bin",
                              gendb_dir + "/orders.o_orderkey.hash.idx",
                              orders_rows);

        // Sorted index on custkey (for joins)
        build_sorted_index_int32(gendb_dir + "/orders.o_custkey.bin",
                                gendb_dir + "/orders.o_custkey.sorted.idx",
                                orders_rows);

        // Zone map on orderdate
        build_zone_map_int32(gendb_dir + "/orders.o_orderdate.bin",
                            gendb_dir + "/orders.o_orderdate.zonemap.idx",
                            orders_rows, BLOCK_SIZE);

        std::cout << std::endl;
    }

    // Customer indexes
    {
        std::cout << "=== Building customer indexes ===" << std::endl;
        size_t customer_rows = read_row_count(gendb_dir, "customer");
        if (customer_rows == 0) {
            std::cerr << "Failed to read customer row count" << std::endl;
            return 1;
        }
        std::cout << "Row count: " << customer_rows << std::endl;

        // Hash index on custkey (primary key)
        build_hash_index_int32(gendb_dir + "/customer.c_custkey.bin",
                              gendb_dir + "/customer.c_custkey.hash.idx",
                              customer_rows);

        std::cout << std::endl;
    }

    std::cout << "Index building completed successfully!" << std::endl;

    return 0;
}
