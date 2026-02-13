#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <thread>

// Zone map structure
struct ZoneMap {
    size_t block_id;
    size_t start_row;
    size_t end_row;
    int32_t min_val;
    int32_t max_val;
};

struct ZoneMapDouble {
    size_t block_id;
    size_t start_row;
    size_t end_row;
    double min_val;
    double max_val;
};

// Build sorted index from binary column file
void build_sorted_index_int32(const std::string& col_path, const std::string& idx_path, size_t row_count) {
    std::cout << "  Building sorted index: " << idx_path << std::endl;

    // Load column data
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Failed to open " << col_path << std::endl;
        return;
    }

    const int32_t* data = static_cast<const int32_t*>(
        mmap(nullptr, row_count * sizeof(int32_t), PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "  Failed to mmap " << col_path << std::endl;
        return;
    }

    // Create (value, row_id) pairs
    std::vector<std::pair<int32_t, size_t>> pairs;
    pairs.reserve(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        pairs.emplace_back(data[i], i);
    }

    munmap((void*)data, row_count * sizeof(int32_t));
    close(fd);

    // Sort by value
    std::sort(pairs.begin(), pairs.end());

    // Write sorted index
    std::ofstream out(idx_path, std::ios::binary);
    for (const auto& p : pairs) {
        out.write(reinterpret_cast<const char*>(&p.first), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&p.second), sizeof(size_t));
    }
    out.close();

    std::cout << "    Wrote " << pairs.size() << " entries" << std::endl;
}

// Build zone map for int32 column
void build_zone_map_int32(const std::string& col_path, const std::string& zonemap_path,
                          size_t row_count, size_t block_size) {
    std::cout << "  Building zone map: " << zonemap_path << std::endl;

    // Load column data
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Failed to open " << col_path << std::endl;
        return;
    }

    const int32_t* data = static_cast<const int32_t*>(
        mmap(nullptr, row_count * sizeof(int32_t), PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "  Failed to mmap " << col_path << std::endl;
        return;
    }

    std::vector<ZoneMap> zones;
    size_t num_blocks = (row_count + block_size - 1) / block_size;

    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        size_t start = block_id * block_size;
        size_t end = std::min(start + block_size, row_count);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; ++i) {
            min_val = std::min(min_val, data[i]);
            max_val = std::max(max_val, data[i]);
        }

        ZoneMap zm;
        zm.block_id = block_id;
        zm.start_row = start;
        zm.end_row = end;
        zm.min_val = min_val;
        zm.max_val = max_val;
        zones.push_back(zm);
    }

    munmap((void*)data, row_count * sizeof(int32_t));
    close(fd);

    // Write zone map
    std::ofstream out(zonemap_path, std::ios::binary);
    size_t num_zones = zones.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(size_t));
    out.write(reinterpret_cast<const char*>(zones.data()), num_zones * sizeof(ZoneMap));
    out.close();

    std::cout << "    Wrote " << zones.size() << " zones" << std::endl;
}

// Build zone map for double column
void build_zone_map_double(const std::string& col_path, const std::string& zonemap_path,
                           size_t row_count, size_t block_size) {
    std::cout << "  Building zone map: " << zonemap_path << std::endl;

    // Load column data
    int fd = open(col_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Failed to open " << col_path << std::endl;
        return;
    }

    const double* data = static_cast<const double*>(
        mmap(nullptr, row_count * sizeof(double), PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "  Failed to mmap " << col_path << std::endl;
        return;
    }

    std::vector<ZoneMapDouble> zones;
    size_t num_blocks = (row_count + block_size - 1) / block_size;

    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        size_t start = block_id * block_size;
        size_t end = std::min(start + block_size, row_count);

        double min_val = data[start];
        double max_val = data[start];

        for (size_t i = start + 1; i < end; ++i) {
            min_val = std::min(min_val, data[i]);
            max_val = std::max(max_val, data[i]);
        }

        ZoneMapDouble zm;
        zm.block_id = block_id;
        zm.start_row = start;
        zm.end_row = end;
        zm.min_val = min_val;
        zm.max_val = max_val;
        zones.push_back(zm);
    }

    munmap((void*)data, row_count * sizeof(double));
    close(fd);

    // Write zone map
    std::ofstream out(zonemap_path, std::ios::binary);
    size_t num_zones = zones.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(size_t));
    out.write(reinterpret_cast<const char*>(zones.data()), num_zones * sizeof(ZoneMapDouble));
    out.close();

    std::cout << "    Wrote " << zones.size() << " zones" << std::endl;
}

// Read row count from metadata.json
size_t read_row_count(const std::string& metadata_path) {
    std::ifstream meta(metadata_path);
    if (!meta.is_open()) {
        std::cerr << "Failed to open " << metadata_path << std::endl;
        return 0;
    }

    std::string line;
    while (std::getline(meta, line)) {
        size_t pos = line.find("\"row_count\":");
        if (pos != std::string::npos) {
            size_t start = line.find(':', pos) + 1;
            size_t end = line.find_first_of(",}", start);
            std::string num_str = line.substr(start, end - start);
            // Trim whitespace
            num_str.erase(0, num_str.find_first_not_of(" \t\n\r"));
            num_str.erase(num_str.find_last_not_of(" \t\n\r") + 1);
            return std::stoull(num_str);
        }
    }
    return 0;
}

void build_lineitem_indexes(const std::string& gendb_dir) {
    std::cout << "Building lineitem indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/lineitem";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    std::vector<std::thread> index_threads;

    // Sorted index on l_orderkey
    index_threads.emplace_back([&]() {
        build_sorted_index_int32(
            table_dir + "/l_orderkey.bin",
            table_dir + "/l_orderkey.idx",
            row_count);
    });

    // Zone maps
    const size_t block_size = 100000;

    index_threads.emplace_back([&]() {
        build_zone_map_int32(
            table_dir + "/l_shipdate.bin",
            table_dir + "/l_shipdate.zonemap",
            row_count, block_size);
    });

    index_threads.emplace_back([&]() {
        build_zone_map_double(
            table_dir + "/l_discount.bin",
            table_dir + "/l_discount.zonemap",
            row_count, block_size);
    });

    index_threads.emplace_back([&]() {
        build_zone_map_double(
            table_dir + "/l_quantity.bin",
            table_dir + "/l_quantity.zonemap",
            row_count, block_size);
    });

    for (auto& t : index_threads) {
        t.join();
    }
}

void build_orders_indexes(const std::string& gendb_dir) {
    std::cout << "Building orders indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/orders";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    std::vector<std::thread> index_threads;

    // Sorted indexes
    index_threads.emplace_back([&]() {
        build_sorted_index_int32(
            table_dir + "/o_orderkey.bin",
            table_dir + "/o_orderkey.idx",
            row_count);
    });

    index_threads.emplace_back([&]() {
        build_sorted_index_int32(
            table_dir + "/o_custkey.bin",
            table_dir + "/o_custkey.idx",
            row_count);
    });

    // Zone map
    const size_t block_size = 100000;
    index_threads.emplace_back([&]() {
        build_zone_map_int32(
            table_dir + "/o_orderdate.bin",
            table_dir + "/o_orderdate.zonemap",
            row_count, block_size);
    });

    for (auto& t : index_threads) {
        t.join();
    }
}

void build_customer_indexes(const std::string& gendb_dir) {
    std::cout << "Building customer indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/customer";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    // Sorted index on c_custkey
    build_sorted_index_int32(
        table_dir + "/c_custkey.bin",
        table_dir + "/c_custkey.idx",
        row_count);
}

void build_supplier_indexes(const std::string& gendb_dir) {
    std::cout << "Building supplier indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/supplier";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    // Sorted index on s_suppkey
    build_sorted_index_int32(
        table_dir + "/s_suppkey.bin",
        table_dir + "/s_suppkey.idx",
        row_count);
}

void build_part_indexes(const std::string& gendb_dir) {
    std::cout << "Building part indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/part";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    // Sorted index on p_partkey
    build_sorted_index_int32(
        table_dir + "/p_partkey.bin",
        table_dir + "/p_partkey.idx",
        row_count);
}

void build_partsupp_indexes(const std::string& gendb_dir) {
    std::cout << "Building partsupp indexes..." << std::endl;

    std::string table_dir = gendb_dir + "/partsupp";
    size_t row_count = read_row_count(table_dir + "/metadata.json");

    if (row_count == 0) {
        std::cerr << "  Failed to read row count" << std::endl;
        return;
    }

    std::cout << "  Row count: " << row_count << std::endl;

    std::vector<std::thread> index_threads;

    // Sorted indexes on composite key
    index_threads.emplace_back([&]() {
        build_sorted_index_int32(
            table_dir + "/ps_partkey.bin",
            table_dir + "/ps_partkey.idx",
            row_count);
    });

    index_threads.emplace_back([&]() {
        build_sorted_index_int32(
            table_dir + "/ps_suppkey.bin",
            table_dir + "/ps_suppkey.idx",
            row_count);
    });

    for (auto& t : index_threads) {
        t.join();
    }
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Index Builder" << std::endl;
    std::cout << "===================" << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    auto start_time = std::chrono::high_resolution_clock::now();

    // Build indexes in parallel across tables
    std::vector<std::thread> table_threads;

    table_threads.emplace_back([&]() { build_lineitem_indexes(gendb_dir); });
    table_threads.emplace_back([&]() { build_orders_indexes(gendb_dir); });
    table_threads.emplace_back([&]() { build_customer_indexes(gendb_dir); });
    table_threads.emplace_back([&]() { build_supplier_indexes(gendb_dir); });
    table_threads.emplace_back([&]() { build_part_indexes(gendb_dir); });
    table_threads.emplace_back([&]() { build_partsupp_indexes(gendb_dir); });

    for (auto& t : table_threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double total_seconds = std::chrono::duration<double>(end_time - start_time).count();

    std::cout << std::endl;
    std::cout << "===================" << std::endl;
    std::cout << "Total index building time: " << total_seconds << " seconds" << std::endl;
    std::cout << "Index building complete!" << std::endl;

    return 0;
}
