#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    size_t start_row;
    size_t end_row;
};

struct ZoneMapDouble {
    double min_val;
    double max_val;
    size_t start_row;
    size_t end_row;
};

// Hash index structure
struct HashIndex {
    std::unordered_map<int32_t, std::vector<size_t>> index;
};

// Build zone map for int32_t column
std::vector<ZoneMap> build_zonemap_int32(const std::string& column_file, size_t row_count, size_t block_size) {
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << column_file << std::endl;
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    int32_t* data = (int32_t*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    std::vector<ZoneMap> zones;
    for (size_t i = 0; i < row_count; i += block_size) {
        size_t end = std::min(i + block_size, row_count);
        int32_t min_val = data[i];
        int32_t max_val = data[i];

        for (size_t j = i + 1; j < end; j++) {
            if (data[j] < min_val) min_val = data[j];
            if (data[j] > max_val) max_val = data[j];
        }

        zones.push_back({min_val, max_val, i, end});
    }

    munmap(data, sb.st_size);
    close(fd);
    return zones;
}

// Build zone map for double column
std::vector<ZoneMapDouble> build_zonemap_double(const std::string& column_file, size_t row_count, size_t block_size) {
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << column_file << std::endl;
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    double* data = (double*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    std::vector<ZoneMapDouble> zones;
    for (size_t i = 0; i < row_count; i += block_size) {
        size_t end = std::min(i + block_size, row_count);
        double min_val = data[i];
        double max_val = data[i];

        for (size_t j = i + 1; j < end; j++) {
            if (data[j] < min_val) min_val = data[j];
            if (data[j] > max_val) max_val = data[j];
        }

        zones.push_back({min_val, max_val, i, end});
    }

    munmap(data, sb.st_size);
    close(fd);
    return zones;
}

// Build hash index for int32_t column
HashIndex build_hash_index_int32(const std::string& column_file, size_t row_count) {
    int fd = open(column_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << column_file << std::endl;
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    int32_t* data = (int32_t*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    HashIndex index;
    for (size_t i = 0; i < row_count; i++) {
        index.index[data[i]].push_back(i);
    }

    munmap(data, sb.st_size);
    close(fd);
    return index;
}

// Write zone map to file
void write_zonemap_int32(const std::string& output_file, const std::vector<ZoneMap>& zones) {
    std::ofstream out(output_file, std::ios::binary);
    size_t count = zones.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));
    out.write(reinterpret_cast<const char*>(zones.data()), zones.size() * sizeof(ZoneMap));
}

void write_zonemap_double(const std::string& output_file, const std::vector<ZoneMapDouble>& zones) {
    std::ofstream out(output_file, std::ios::binary);
    size_t count = zones.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));
    out.write(reinterpret_cast<const char*>(zones.data()), zones.size() * sizeof(ZoneMapDouble));
}

// Write hash index to file
void write_hash_index(const std::string& output_file, const HashIndex& index) {
    std::ofstream out(output_file, std::ios::binary);
    size_t map_size = index.index.size();
    out.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

    for (const auto& [key, values] : index.index) {
        out.write(reinterpret_cast<const char*>(&key), sizeof(key));
        size_t value_count = values.size();
        out.write(reinterpret_cast<const char*>(&value_count), sizeof(value_count));
        out.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(size_t));
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    size_t block_size = 100000;

    // Build indexes for lineitem
    std::cout << "Building indexes for lineitem..." << std::endl;
    {
        size_t row_count = 59986052; // From workload analysis

        // Zone map for l_shipdate
        auto shipdate_zones = build_zonemap_int32(gendb_dir + "/lineitem/l_shipdate.bin", row_count, block_size);
        write_zonemap_int32(gendb_dir + "/lineitem/l_shipdate.zonemap.idx", shipdate_zones);
        std::cout << "  Built shipdate zone map: " << shipdate_zones.size() << " zones" << std::endl;

        // Zone map for l_discount
        auto discount_zones = build_zonemap_double(gendb_dir + "/lineitem/l_discount.bin", row_count, block_size);
        write_zonemap_double(gendb_dir + "/lineitem/l_discount.zonemap.idx", discount_zones);
        std::cout << "  Built discount zone map: " << discount_zones.size() << " zones" << std::endl;

        // Zone map for l_quantity
        auto quantity_zones = build_zonemap_double(gendb_dir + "/lineitem/l_quantity.bin", row_count, block_size);
        write_zonemap_double(gendb_dir + "/lineitem/l_quantity.zonemap.idx", quantity_zones);
        std::cout << "  Built quantity zone map: " << quantity_zones.size() << " zones" << std::endl;

        // Hash index for l_orderkey
        auto orderkey_hash = build_hash_index_int32(gendb_dir + "/lineitem/l_orderkey.bin", row_count);
        write_hash_index(gendb_dir + "/lineitem/l_orderkey.hash.idx", orderkey_hash);
        std::cout << "  Built orderkey hash index: " << orderkey_hash.index.size() << " keys" << std::endl;
    }

    // Build indexes for orders
    std::cout << "Building indexes for orders..." << std::endl;
    {
        size_t row_count = 15000000; // From workload analysis

        // Zone map for o_orderdate
        auto orderdate_zones = build_zonemap_int32(gendb_dir + "/orders/o_orderdate.bin", row_count, block_size);
        write_zonemap_int32(gendb_dir + "/orders/o_orderdate.zonemap.idx", orderdate_zones);
        std::cout << "  Built orderdate zone map: " << orderdate_zones.size() << " zones" << std::endl;

        // Hash index for o_orderkey
        auto orderkey_hash = build_hash_index_int32(gendb_dir + "/orders/o_orderkey.bin", row_count);
        write_hash_index(gendb_dir + "/orders/o_orderkey.hash.idx", orderkey_hash);
        std::cout << "  Built orderkey hash index: " << orderkey_hash.index.size() << " keys" << std::endl;
    }

    // Build indexes for customer
    std::cout << "Building indexes for customer..." << std::endl;
    {
        size_t row_count = 1500000; // From workload analysis

        // Hash index for c_custkey
        auto custkey_hash = build_hash_index_int32(gendb_dir + "/customer/c_custkey.bin", row_count);
        write_hash_index(gendb_dir + "/customer/c_custkey.hash.idx", custkey_hash);
        std::cout << "  Built custkey hash index: " << custkey_hash.index.size() << " keys" << std::endl;
    }

    std::cout << "Index building complete!" << std::endl;
    return 0;
}
