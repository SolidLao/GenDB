#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>

namespace fs = std::filesystem;

// Zone map entry (min/max per block)
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    size_t start_row;
    size_t end_row;
};

// Hash index entry (key -> row_id)
struct HashIndexEntry {
    int32_t key;
    size_t row_id;
};

// Build hash index for lineitem.l_orderkey
void build_lineitem_orderkey_hash(const std::string& gendb_dir) {
    std::cout << "Building hash index on lineitem.l_orderkey..." << std::endl;

    std::string col_file = gendb_dir + "/lineitem/l_orderkey.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    // Build hash index
    std::vector<HashIndexEntry> index;
    index.reserve(row_count);

    for (size_t i = 0; i < row_count; i++) {
        index.push_back({data[i], i});
    }

    // Sort by key for binary search capability
    std::sort(index.begin(), index.end(), [](const auto& a, const auto& b) {
        return a.key < b.key;
    });

    // Write index to disk
    std::string idx_file = gendb_dir + "/lineitem/l_orderkey_hash.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    ofs.write((char*)&row_count, sizeof(row_count));
    ofs.write((char*)index.data(), sizeof(HashIndexEntry) * index.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built hash index on lineitem.l_orderkey (" << row_count << " entries)" << std::endl;
}

// Build zone map for lineitem.l_shipdate
void build_lineitem_shipdate_zonemap(const std::string& gendb_dir) {
    std::cout << "Building zone map on lineitem.l_shipdate..." << std::endl;

    std::string col_file = gendb_dir + "/lineitem/l_shipdate.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    // Build zone map with 100K row blocks
    const size_t BLOCK_SIZE = 100000;
    std::vector<ZoneMapEntry> zone_map;

    for (size_t start = 0; start < row_count; start += BLOCK_SIZE) {
        size_t end = std::min(start + BLOCK_SIZE, row_count);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_map.push_back({min_val, max_val, start, end});
    }

    // Write zone map to disk
    std::string idx_file = gendb_dir + "/lineitem/l_shipdate_zonemap.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    size_t num_zones = zone_map.size();
    ofs.write((char*)&num_zones, sizeof(num_zones));
    ofs.write((char*)zone_map.data(), sizeof(ZoneMapEntry) * zone_map.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built zone map on lineitem.l_shipdate (" << zone_map.size() << " zones)" << std::endl;
}

// Build hash index for orders.o_orderkey
void build_orders_orderkey_hash(const std::string& gendb_dir) {
    std::cout << "Building hash index on orders.o_orderkey..." << std::endl;

    std::string col_file = gendb_dir + "/orders/o_orderkey.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    std::vector<HashIndexEntry> index;
    index.reserve(row_count);

    for (size_t i = 0; i < row_count; i++) {
        index.push_back({data[i], i});
    }

    std::sort(index.begin(), index.end(), [](const auto& a, const auto& b) {
        return a.key < b.key;
    });

    std::string idx_file = gendb_dir + "/orders/o_orderkey_hash.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    ofs.write((char*)&row_count, sizeof(row_count));
    ofs.write((char*)index.data(), sizeof(HashIndexEntry) * index.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built hash index on orders.o_orderkey (" << row_count << " entries)" << std::endl;
}

// Build hash index for orders.o_custkey
void build_orders_custkey_hash(const std::string& gendb_dir) {
    std::cout << "Building hash index on orders.o_custkey..." << std::endl;

    std::string col_file = gendb_dir + "/orders/o_custkey.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    std::vector<HashIndexEntry> index;
    index.reserve(row_count);

    for (size_t i = 0; i < row_count; i++) {
        index.push_back({data[i], i});
    }

    std::sort(index.begin(), index.end(), [](const auto& a, const auto& b) {
        return a.key < b.key;
    });

    std::string idx_file = gendb_dir + "/orders/o_custkey_hash.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    ofs.write((char*)&row_count, sizeof(row_count));
    ofs.write((char*)index.data(), sizeof(HashIndexEntry) * index.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built hash index on orders.o_custkey (" << row_count << " entries)" << std::endl;
}

// Build zone map for orders.o_orderdate
void build_orders_orderdate_zonemap(const std::string& gendb_dir) {
    std::cout << "Building zone map on orders.o_orderdate..." << std::endl;

    std::string col_file = gendb_dir + "/orders/o_orderdate.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    const size_t BLOCK_SIZE = 100000;
    std::vector<ZoneMapEntry> zone_map;

    for (size_t start = 0; start < row_count; start += BLOCK_SIZE) {
        size_t end = std::min(start + BLOCK_SIZE, row_count);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_map.push_back({min_val, max_val, start, end});
    }

    std::string idx_file = gendb_dir + "/orders/o_orderdate_zonemap.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    size_t num_zones = zone_map.size();
    ofs.write((char*)&num_zones, sizeof(num_zones));
    ofs.write((char*)zone_map.data(), sizeof(ZoneMapEntry) * zone_map.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built zone map on orders.o_orderdate (" << zone_map.size() << " zones)" << std::endl;
}

// Build hash index for customer.c_custkey
void build_customer_custkey_hash(const std::string& gendb_dir) {
    std::cout << "Building hash index on customer.c_custkey..." << std::endl;

    std::string col_file = gendb_dir + "/customer/c_custkey.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t row_count = file_size / sizeof(int32_t);

    int32_t* data = (int32_t*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    std::vector<HashIndexEntry> index;
    index.reserve(row_count);

    for (size_t i = 0; i < row_count; i++) {
        index.push_back({data[i], i});
    }

    std::sort(index.begin(), index.end(), [](const auto& a, const auto& b) {
        return a.key < b.key;
    });

    std::string idx_file = gendb_dir + "/customer/c_custkey_hash.idx";
    std::ofstream ofs(idx_file, std::ios::binary);
    ofs.write((char*)&row_count, sizeof(row_count));
    ofs.write((char*)index.data(), sizeof(HashIndexEntry) * index.size());
    ofs.close();

    munmap(data, file_size);
    close(fd);

    std::cout << "Built hash index on customer.c_custkey (" << row_count << " entries)" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "Building indexes from binary data in: " << gendb_dir << std::endl;

    // Build all indexes
    build_lineitem_orderkey_hash(gendb_dir);
    build_lineitem_shipdate_zonemap(gendb_dir);
    build_orders_orderkey_hash(gendb_dir);
    build_orders_custkey_hash(gendb_dir);
    build_orders_orderdate_zonemap(gendb_dir);
    build_customer_custkey_hash(gendb_dir);

    std::cout << "\nIndex building complete!" << std::endl;

    return 0;
}
