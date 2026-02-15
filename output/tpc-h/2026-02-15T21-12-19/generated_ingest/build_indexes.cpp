#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <omp.h>

// Zone Map structure: [uint32_t min, uint32_t max, uint32_t count]
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
};

// Hash entry for multi-value hash index: [key, offset, count]
struct HashEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Utility to mmap a file
void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << "\n";
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    out_size = sb.st_size;
    void* data = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        return nullptr;
    }

    madvise(data, out_size, MADV_SEQUENTIAL);
    close(fd);
    return data;
}

// Build zone map for lineitem.l_shipdate
void build_lineitem_shipdate_zonemap(const std::string& data_dir, const std::string& output_dir, uint32_t block_size = 100000) {
    std::cout << "Building lineitem.l_shipdate zone map...\n";

    size_t file_size;
    int32_t* data = static_cast<int32_t*>(mmap_file(data_dir + "/lineitem/l_shipdate.bin", file_size));
    if (!data) {
        std::cerr << "Failed to mmap l_shipdate.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);
    uint32_t num_blocks = (num_rows + block_size - 1) / block_size;

    std::vector<ZoneMapEntry> zone_maps;
    zone_maps.reserve(num_blocks);

    #pragma omp parallel for schedule(static) collapse(1)
    for (uint32_t b = 0; b < num_blocks; ++b) {
        uint32_t start = b * block_size;
        uint32_t end = std::min(start + block_size, num_rows);
        uint32_t block_count = end - start;

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (uint32_t i = start; i < end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_maps[b] = {min_val, max_val, block_count};
    }

    // Write zone map to binary file
    std::ofstream out(output_dir + "/indexes/lineitem_shipdate_zonemap.bin", std::ios::binary);
    uint32_t num_zones = zone_maps.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(num_zones));
    for (const auto& zm : zone_maps) {
        out.write(reinterpret_cast<const char*>(&zm.min_val), sizeof(zm.min_val));
        out.write(reinterpret_cast<const char*>(&zm.max_val), sizeof(zm.max_val));
        out.write(reinterpret_cast<const char*>(&zm.count), sizeof(zm.count));
    }
    out.close();

    munmap(data, file_size);
    std::cout << "  ✓ Zone map written: " << num_zones << " blocks\n";
}

// Build multi-value hash index for lineitem.l_orderkey
void build_lineitem_orderkey_hash(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Building lineitem.l_orderkey hash index (multi-value)...\n";

    size_t file_size;
    int32_t* orderkeys = static_cast<int32_t*>(mmap_file(data_dir + "/lineitem/l_orderkey.bin", file_size));
    if (!orderkeys) {
        std::cerr << "Failed to mmap l_orderkey.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);

    // Step 1: Group positions by key
    std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;
    for (uint32_t i = 0; i < num_rows; ++i) {
        key_to_positions[orderkeys[i]].push_back(i);
    }

    std::cout << "  ✓ Found " << key_to_positions.size() << " unique keys\n";

    // Step 2: Build positions array (contiguous per key)
    std::vector<uint32_t> positions_array;
    positions_array.reserve(num_rows);

    // Step 3: Build hash table entries
    std::vector<HashEntry> hash_entries;
    hash_entries.reserve(key_to_positions.size());

    for (const auto& [key, positions] : key_to_positions) {
        uint32_t offset = positions_array.size();
        uint32_t count = positions.size();

        hash_entries.push_back({key, offset, count});

        for (uint32_t pos : positions) {
            positions_array.push_back(pos);
        }
    }

    // Step 4: Write to binary file
    // Format: [uint32_t num_unique] [HashEntry entries...] [uint32_t positions_array...]
    std::ofstream out(output_dir + "/indexes/lineitem_orderkey_hash.bin", std::ios::binary);
    uint32_t num_unique = hash_entries.size();
    out.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));

    for (const auto& entry : hash_entries) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        out.write(reinterpret_cast<const char*>(&entry.count), sizeof(entry.count));
    }

    uint32_t pos_count = positions_array.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions_array.data()), pos_count * sizeof(uint32_t));
    out.close();

    munmap(orderkeys, file_size);
    std::cout << "  ✓ Hash index written: " << num_unique << " unique keys, " << pos_count << " positions\n";
}

// Build sorted index for orders.o_orderkey (already sorted by ingestion)
void build_orders_orderkey_sorted(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Building orders.o_orderkey sorted index...\n";

    size_t file_size;
    int32_t* orderkeys = static_cast<int32_t*>(mmap_file(data_dir + "/orders/o_orderkey.bin", file_size));
    if (!orderkeys) {
        std::cerr << "Failed to mmap o_orderkey.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);

    // Data is already sorted by ingestion. Write (key, position) pairs to index.
    // For simplicity, just write [count, [key, pos] pairs]
    std::ofstream out(output_dir + "/indexes/orders_orderkey_sorted.bin", std::ios::binary);
    out.write(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows));

    for (uint32_t i = 0; i < num_rows; ++i) {
        int32_t key = orderkeys[i];
        out.write(reinterpret_cast<const char*>(&key), sizeof(key));
        out.write(reinterpret_cast<const char*>(&i), sizeof(i));
    }
    out.close();

    munmap(orderkeys, file_size);
    std::cout << "  ✓ Sorted index written: " << num_rows << " entries\n";
}

// Build hash index for orders.o_custkey
void build_orders_custkey_hash(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Building orders.o_custkey hash index (multi-value)...\n";

    size_t file_size;
    int32_t* custkeys = static_cast<int32_t*>(mmap_file(data_dir + "/orders/o_custkey.bin", file_size));
    if (!custkeys) {
        std::cerr << "Failed to mmap o_custkey.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);

    // Group positions by key
    std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;
    for (uint32_t i = 0; i < num_rows; ++i) {
        key_to_positions[custkeys[i]].push_back(i);
    }

    // Build positions array and hash table
    std::vector<uint32_t> positions_array;
    std::vector<HashEntry> hash_entries;

    for (const auto& [key, positions] : key_to_positions) {
        uint32_t offset = positions_array.size();
        uint32_t count = positions.size();
        hash_entries.push_back({key, offset, count});

        for (uint32_t pos : positions) {
            positions_array.push_back(pos);
        }
    }

    // Write to binary
    std::ofstream out(output_dir + "/indexes/orders_custkey_hash.bin", std::ios::binary);
    uint32_t num_unique = hash_entries.size();
    out.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));

    for (const auto& entry : hash_entries) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        out.write(reinterpret_cast<const char*>(&entry.count), sizeof(entry.count));
    }

    uint32_t pos_count = positions_array.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions_array.data()), pos_count * sizeof(uint32_t));
    out.close();

    munmap(custkeys, file_size);
    std::cout << "  ✓ Hash index written: " << num_unique << " unique keys\n";
}

// Build sorted index for customer.c_custkey
void build_customer_custkey_sorted(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Building customer.c_custkey sorted index...\n";

    size_t file_size;
    int32_t* custkeys = static_cast<int32_t*>(mmap_file(data_dir + "/customer/c_custkey.bin", file_size));
    if (!custkeys) {
        std::cerr << "Failed to mmap c_custkey.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);

    // Write sorted index (already sorted by ingestion)
    std::ofstream out(output_dir + "/indexes/customer_custkey_sorted.bin", std::ios::binary);
    out.write(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows));

    for (uint32_t i = 0; i < num_rows; ++i) {
        int32_t key = custkeys[i];
        out.write(reinterpret_cast<const char*>(&key), sizeof(key));
        out.write(reinterpret_cast<const char*>(&i), sizeof(i));
    }
    out.close();

    munmap(custkeys, file_size);
    std::cout << "  ✓ Sorted index written: " << num_rows << " entries\n";
}

// Build hash index for customer.c_mktsegment
void build_customer_mktsegment_hash(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Building customer.c_mktsegment hash index (multi-value)...\n";

    size_t file_size;
    int32_t* segments = static_cast<int32_t*>(mmap_file(data_dir + "/customer/c_mktsegment.bin", file_size));
    if (!segments) {
        std::cerr << "Failed to mmap c_mktsegment.bin\n";
        return;
    }

    uint32_t num_rows = file_size / sizeof(int32_t);

    // Group positions by segment code
    std::unordered_map<int32_t, std::vector<uint32_t>> seg_to_positions;
    for (uint32_t i = 0; i < num_rows; ++i) {
        seg_to_positions[segments[i]].push_back(i);
    }

    // Build positions array and hash table
    std::vector<uint32_t> positions_array;
    std::vector<HashEntry> hash_entries;

    for (const auto& [seg, positions] : seg_to_positions) {
        uint32_t offset = positions_array.size();
        uint32_t count = positions.size();
        hash_entries.push_back({seg, offset, count});

        for (uint32_t pos : positions) {
            positions_array.push_back(pos);
        }
    }

    // Write to binary
    std::ofstream out(output_dir + "/indexes/customer_mktsegment_hash.bin", std::ios::binary);
    uint32_t num_unique = hash_entries.size();
    out.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));

    for (const auto& entry : hash_entries) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(entry.key));
        out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        out.write(reinterpret_cast<const char*>(&entry.count), sizeof(entry.count));
    }

    uint32_t pos_count = positions_array.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions_array.data()), pos_count * sizeof(uint32_t));
    out.close();

    munmap(segments, file_size);
    std::cout << "  ✓ Hash index written: " << num_unique << " unique segments\n";
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: build_indexes <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string data_dir = gendb_dir;
    std::string output_dir = gendb_dir;

    // Create indexes directory
    std::system(("mkdir -p " + output_dir + "/indexes").c_str());

    std::cout << "Building indexes for TPC-H SF10...\n";

    build_lineitem_shipdate_zonemap(data_dir, output_dir);
    build_lineitem_orderkey_hash(data_dir, output_dir);
    build_orders_orderkey_sorted(data_dir, output_dir);
    build_orders_custkey_hash(data_dir, output_dir);
    build_customer_custkey_sorted(data_dir, output_dir);
    build_customer_mktsegment_hash(data_dir, output_dir);

    std::cout << "✓ Index building complete\n";
    return 0;
}
