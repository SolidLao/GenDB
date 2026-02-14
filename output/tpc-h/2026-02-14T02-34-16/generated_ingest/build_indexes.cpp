#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <map>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================================
// Zone Map Structure
// ============================================================================

struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// ============================================================================
// Index Building Functions
// ============================================================================

// Build zone map index for a column
void build_zone_map_index(const std::string& col_path, const std::string& output_path,
                          size_t block_size = 100000) {
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: Cannot open " << col_path << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_rows = file_size / sizeof(int32_t);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        std::cerr << "Warning: mmap failed for " << col_path << std::endl;
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const int32_t* data = (const int32_t*)mapped;

    // Build zone maps
    std::vector<ZoneMap> zones;
    for (size_t i = 0; i < num_rows; i += block_size) {
        size_t block_end = std::min(i + block_size, num_rows);
        int32_t min_val = data[i];
        int32_t max_val = data[i];

        for (size_t j = i + 1; j < block_end; j++) {
            if (data[j] < min_val) min_val = data[j];
            if (data[j] > max_val) max_val = data[j];
        }

        zones.push_back({min_val, max_val, (uint32_t)(block_end - i)});
    }

    // Write zone map file
    std::ofstream out(output_path, std::ios::binary);
    if (out) {
        uint32_t num_zones = zones.size();
        out.write(reinterpret_cast<const char*>(&num_zones), sizeof(num_zones));
        out.write(reinterpret_cast<const char*>(zones.data()), zones.size() * sizeof(ZoneMap));
        out.close();
        std::cout << "  Built zone map: " << output_path << " (" << num_zones << " zones)" << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);
}

// Build sorted index for an integer column (stores sorted (value, row_id) pairs)
void build_sorted_index(const std::string& col_path, const std::string& output_path) {
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: Cannot open " << col_path << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_rows = file_size / sizeof(int32_t);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        std::cerr << "Warning: mmap failed for " << col_path << std::endl;
        return;
    }

    madvise(mapped, file_size, MADV_RANDOM);

    const int32_t* data = (const int32_t*)mapped;

    // Create (value, row_id) pairs
    std::vector<std::pair<int32_t, uint32_t>> pairs;
    pairs.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        pairs.push_back({data[i], (uint32_t)i});
    }

    // Sort by value
    std::sort(pairs.begin(), pairs.end());

    // Write sorted index
    std::ofstream out(output_path, std::ios::binary);
    if (out) {
        uint32_t count = pairs.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(count));
        for (const auto& p : pairs) {
            out.write(reinterpret_cast<const char*>(&p.first), sizeof(p.first));
            out.write(reinterpret_cast<const char*>(&p.second), sizeof(p.second));
        }
        out.close();
        std::cout << "  Built sorted index: " << output_path << " (" << count << " entries)" << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);
}

// Build hash index for a column (stores map of value -> row_ids)
void build_hash_index(const std::string& col_path, const std::string& output_path) {
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: Cannot open " << col_path << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_rows = file_size / sizeof(uint8_t);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        std::cerr << "Warning: mmap failed for " << col_path << std::endl;
        return;
    }

    madvise(mapped, file_size, MADV_RANDOM);

    const uint8_t* data = (const uint8_t*)mapped;

    // Build map of value -> row_ids
    std::map<uint8_t, std::vector<uint32_t>> hash_map;
    for (size_t i = 0; i < num_rows; i++) {
        hash_map[data[i]].push_back((uint32_t)i);
    }

    // Write hash index
    std::ofstream out(output_path, std::ios::binary);
    if (out) {
        uint32_t num_keys = hash_map.size();
        out.write(reinterpret_cast<const char*>(&num_keys), sizeof(num_keys));

        for (const auto& [key, row_ids] : hash_map) {
            out.write(reinterpret_cast<const char*>(&key), sizeof(key));
            uint32_t count = row_ids.size();
            out.write(reinterpret_cast<const char*>(&count), sizeof(count));
            out.write(reinterpret_cast<const char*>(row_ids.data()), count * sizeof(uint32_t));
        }

        out.close();
        std::cout << "  Built hash index: " << output_path << " (" << num_keys << " keys)" << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);
}

// ============================================================================
// Main Index Builder
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "=== TPC-H Index Builder ===" << std::endl;
    std::cout << "GenDB dir: " << gendb_dir << std::endl;

    try {
        // Build indexes for lineitem
        std::cout << "\n[1] Building lineitem indexes..." << std::endl;
        std::string lineitem_dir = gendb_dir + "/lineitem";

        build_zone_map_index(lineitem_dir + "/l_shipdate.col", lineitem_dir + "/l_shipdate.zone_map.idx", 100000);
        build_sorted_index(lineitem_dir + "/l_orderkey.col", lineitem_dir + "/l_orderkey.sorted.idx");

        // Build indexes for orders
        std::cout << "\n[2] Building orders indexes..." << std::endl;
        std::string orders_dir = gendb_dir + "/orders";

        build_zone_map_index(orders_dir + "/o_orderdate.col", orders_dir + "/o_orderdate.zone_map.idx", 100000);
        build_sorted_index(orders_dir + "/o_custkey.col", orders_dir + "/o_custkey.sorted.idx");

        // Build indexes for customer
        std::cout << "\n[3] Building customer indexes..." << std::endl;
        std::string customer_dir = gendb_dir + "/customer";

        build_hash_index(customer_dir + "/c_mktsegment.col", customer_dir + "/c_mktsegment.hash.idx");
        build_sorted_index(customer_dir + "/c_custkey.col", customer_dir + "/c_custkey.sorted.idx");

        std::cout << "\n=== Index Building Complete ===" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
