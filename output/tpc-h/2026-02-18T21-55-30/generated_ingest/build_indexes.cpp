#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <omp.h>

struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
};

struct ZoneMapBlockMulti {
    int32_t min_val1, max_val1;  // First column
    int32_t min_val2, max_val2;  // Second column
    uint32_t count;
};

// Helper: read int32_t from binary file
int32_t read_int32(const char* data, size_t offset) {
    int32_t val;
    std::memcpy(&val, data + offset, sizeof(int32_t));
    return val;
}

// Helper: write int32_t to vector
void write_int32_vec(std::vector<char>& vec, int32_t val) {
    const char* ptr = (const char*)&val;
    vec.insert(vec.end(), ptr, ptr + sizeof(int32_t));
}

// Helper: write uint32_t to vector
void write_uint32_vec(std::vector<char>& vec, uint32_t val) {
    const char* ptr = (const char*)&val;
    vec.insert(vec.end(), ptr, ptr + sizeof(uint32_t));
}

// Zone map builder for single column
void build_zone_map_single(const std::string& col_file, const std::string& output_file, uint32_t block_size) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_values = file_size / sizeof(int32_t);

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << col_file << std::endl;
        close(fd);
        return;
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    std::vector<ZoneMapBlock> zone_maps;
    for (size_t block_start = 0; block_start < num_values; block_start += block_size) {
        size_t block_end = std::min(block_start + block_size, num_values);
        int32_t min_val = read_int32(data, block_start * sizeof(int32_t));
        int32_t max_val = min_val;

        for (size_t i = block_start; i < block_end; i++) {
            int32_t val = read_int32(data, i * sizeof(int32_t));
            if (val < min_val) min_val = val;
            if (val > max_val) max_val = val;
        }

        zone_maps.push_back({min_val, max_val, (uint32_t)(block_end - block_start)});
    }

    // Write zone map binary
    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_blocks = zone_maps.size();
    out.write((const char*)&num_blocks, sizeof(uint32_t));
    for (const auto& zm : zone_maps) {
        out.write((const char*)&zm.min_val, sizeof(int32_t));
        out.write((const char*)&zm.max_val, sizeof(int32_t));
        out.write((const char*)&zm.count, sizeof(uint32_t));
    }
    out.close();

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "Zone map for " << col_file << ": " << num_blocks << " blocks" << std::endl;
}

// Zone map builder for two columns (discount, quantity)
void build_zone_map_dual(const std::string& col1_file, const std::string& col2_file,
                         const std::string& output_file, uint32_t block_size) {
    int fd1 = open(col1_file.c_str(), O_RDONLY);
    int fd2 = open(col2_file.c_str(), O_RDONLY);
    if (fd1 < 0 || fd2 < 0) {
        std::cerr << "Cannot open column files" << std::endl;
        return;
    }

    struct stat sb1, sb2;
    fstat(fd1, &sb1);
    fstat(fd2, &sb2);
    size_t file_size1 = sb1.st_size;
    size_t file_size2 = sb2.st_size;
    size_t num_values = file_size1 / sizeof(int64_t);

    const char* data1 = (const char*)mmap(nullptr, file_size1, PROT_READ, MAP_PRIVATE, fd1, 0);
    const char* data2 = (const char*)mmap(nullptr, file_size2, PROT_READ, MAP_PRIVATE, fd2, 0);
    if (data1 == MAP_FAILED || data2 == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd1);
        close(fd2);
        return;
    }

    madvise((void*)data1, file_size1, MADV_SEQUENTIAL);
    madvise((void*)data2, file_size2, MADV_SEQUENTIAL);

    std::vector<ZoneMapBlockMulti> zone_maps;
    for (size_t block_start = 0; block_start < num_values; block_start += block_size) {
        size_t block_end = std::min(block_start + block_size, num_values);

        // Read first value from both columns
        int64_t val1_start, val2_start;
        std::memcpy(&val1_start, data1 + block_start * sizeof(int64_t), sizeof(int64_t));
        std::memcpy(&val2_start, data2 + block_start * sizeof(int64_t), sizeof(int64_t));

        int32_t min_val1 = (int32_t)val1_start, max_val1 = (int32_t)val1_start;
        int32_t min_val2 = (int32_t)val2_start, max_val2 = (int32_t)val2_start;

        for (size_t i = block_start; i < block_end; i++) {
            int64_t v1, v2;
            std::memcpy(&v1, data1 + i * sizeof(int64_t), sizeof(int64_t));
            std::memcpy(&v2, data2 + i * sizeof(int64_t), sizeof(int64_t));

            int32_t iv1 = (int32_t)v1, iv2 = (int32_t)v2;
            if (iv1 < min_val1) min_val1 = iv1;
            if (iv1 > max_val1) max_val1 = iv1;
            if (iv2 < min_val2) min_val2 = iv2;
            if (iv2 > max_val2) max_val2 = iv2;
        }

        zone_maps.push_back({min_val1, max_val1, min_val2, max_val2, (uint32_t)(block_end - block_start)});
    }

    // Write zone map
    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_blocks = zone_maps.size();
    out.write((const char*)&num_blocks, sizeof(uint32_t));
    for (const auto& zm : zone_maps) {
        out.write((const char*)&zm.min_val1, sizeof(int32_t));
        out.write((const char*)&zm.max_val1, sizeof(int32_t));
        out.write((const char*)&zm.min_val2, sizeof(int32_t));
        out.write((const char*)&zm.max_val2, sizeof(int32_t));
        out.write((const char*)&zm.count, sizeof(uint32_t));
    }
    out.close();

    munmap((void*)data1, file_size1);
    munmap((void*)data2, file_size2);
    close(fd1);
    close(fd2);

    std::cout << "Zone map for dual columns: " << num_blocks << " blocks" << std::endl;
}

// Multi-value hash index builder
void build_hash_index_multi(const std::string& col_file, const std::string& output_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_values = file_size / sizeof(int32_t);

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Step 1: Create position array [0, 1, 2, ..., N-1]
    std::vector<uint32_t> positions(num_values);
    #pragma omp parallel for
    for (size_t i = 0; i < num_values; i++) {
        positions[i] = i;
    }

    // Step 2: Sort positions by key value
    std::cout << "Sorting " << num_values << " positions by key..." << std::endl;
    std::sort(positions.begin(), positions.end(),
        [&data](uint32_t a, uint32_t b) {
            int32_t va, vb;
            std::memcpy(&va, data + a * sizeof(int32_t), sizeof(int32_t));
            std::memcpy(&vb, data + b * sizeof(int32_t), sizeof(int32_t));
            return va < vb;
        });

    // Step 3: Build groups (key -> (offset, count))
    std::map<int32_t, std::pair<uint32_t, uint32_t>> groups;
    {
        uint32_t offset = 0;
        for (size_t i = 0; i < num_values; ) {
            int32_t key;
            std::memcpy(&key, data + positions[i] * sizeof(int32_t), sizeof(int32_t));
            uint32_t count = 1;
            while (i + count < num_values) {
                int32_t next_key;
                std::memcpy(&next_key, data + positions[i + count] * sizeof(int32_t), sizeof(int32_t));
                if (next_key != key) break;
                count++;
            }
            groups[key] = {offset, count};
            offset += count;
            i += count;
        }
    }

    std::cout << "Found " << groups.size() << " unique keys" << std::endl;

    // Step 4: Build hash table with multiply-shift hash
    const uint64_t HASH_MULT = 0x9E3779B97F4A7C15ULL;
    uint32_t table_size = std::max(100u, (uint32_t)(groups.size() * 1.5));
    if ((table_size & (table_size - 1)) != 0) {
        // Round up to next power of 2
        uint32_t p = 1;
        while (p < table_size) p <<= 1;
        table_size = p;
    }
    uint32_t mask = table_size - 1;
    uint32_t shift = 64 - __builtin_clzll(table_size);

    std::cout << "Building hash table with size " << table_size << std::endl;

    std::vector<int32_t> hash_keys(table_size, INT32_MIN);
    std::vector<uint32_t> hash_offsets(table_size, 0);
    std::vector<uint32_t> hash_counts(table_size, 0);

    for (const auto& [key, offcount] : groups) {
        uint64_t hash = (uint64_t)key * HASH_MULT >> shift;
        uint32_t slot = hash & mask;

        while (hash_keys[slot] != INT32_MIN) {
            slot = (slot + 1) & mask;
        }

        hash_keys[slot] = key;
        hash_offsets[slot] = offcount.first;
        hash_counts[slot] = offcount.second;
    }

    // Step 5: Write index file
    std::ofstream out(output_file, std::ios::binary);
    out.write((const char*)&num_values, sizeof(uint32_t));
    out.write((const char*)&table_size, sizeof(uint32_t));

    // Write hash table entries
    for (uint32_t i = 0; i < table_size; i++) {
        out.write((const char*)&hash_keys[i], sizeof(int32_t));
        out.write((const char*)&hash_offsets[i], sizeof(uint32_t));
        out.write((const char*)&hash_counts[i], sizeof(uint32_t));
    }

    // Write positions array
    uint32_t pos_count = positions.size();
    out.write((const char*)&pos_count, sizeof(uint32_t));
    for (uint32_t pos : positions) {
        out.write((const char*)&pos, sizeof(uint32_t));
    }

    out.close();

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "Hash index built: " << output_file << std::endl;
}

// Single-value hash index (for primary keys)
void build_hash_index_single(const std::string& col_file, const std::string& output_file) {
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_values = file_size / sizeof(int32_t);

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Build hash table with multiply-shift hash
    const uint64_t HASH_MULT = 0x9E3779B97F4A7C15ULL;
    uint32_t table_size = std::max(100u, (uint32_t)(num_values * 0.7));
    if ((table_size & (table_size - 1)) != 0) {
        uint32_t p = 1;
        while (p < table_size) p <<= 1;
        table_size = p;
    }
    uint32_t mask = table_size - 1;
    uint32_t shift = 64 - __builtin_clzll(table_size);

    std::cout << "Building single hash table size " << table_size << std::endl;

    std::vector<int32_t> hash_keys(table_size, INT32_MIN);
    std::vector<uint32_t> hash_positions(table_size, UINT32_MAX);

    for (uint32_t i = 0; i < num_values; i++) {
        int32_t key;
        std::memcpy(&key, data + i * sizeof(int32_t), sizeof(int32_t));

        uint64_t hash = (uint64_t)key * HASH_MULT >> shift;
        uint32_t slot = hash & mask;

        while (hash_keys[slot] != INT32_MIN) {
            slot = (slot + 1) & mask;
        }

        hash_keys[slot] = key;
        hash_positions[slot] = i;
    }

    // Write index file
    std::ofstream out(output_file, std::ios::binary);
    out.write((const char*)&table_size, sizeof(uint32_t));
    for (uint32_t i = 0; i < table_size; i++) {
        out.write((const char*)&hash_keys[i], sizeof(int32_t));
        out.write((const char*)&hash_positions[i], sizeof(uint32_t));
    }
    out.close();

    munmap((void*)data, file_size);
    close(fd);

    std::cout << "Single hash index built: " << output_file << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: ./build_indexes <gendb_dir>" << std::endl;
        return 1;
    }

    std::string base_dir = argv[1];
    std::string indexes_dir = base_dir + "/indexes";

    // Create indexes directory
    std::system(("mkdir -p " + indexes_dir).c_str());

    std::cout << "Building indexes for TPC-H..." << std::endl;

    // Lineitem indexes
    std::cout << "Lineitem indexes..." << std::endl;
    build_zone_map_single(base_dir + "/lineitem/l_shipdate.bin",
                          indexes_dir + "/zone_map_l_shipdate.bin", 100000);
    build_zone_map_dual(base_dir + "/lineitem/l_discount.bin",
                        base_dir + "/lineitem/l_quantity.bin",
                        indexes_dir + "/zone_map_l_discount_qty.bin", 100000);
    build_hash_index_multi(base_dir + "/lineitem/l_orderkey.bin",
                           indexes_dir + "/hash_l_orderkey.bin");

    // Orders indexes
    std::cout << "Orders indexes..." << std::endl;
    build_zone_map_single(base_dir + "/orders/o_orderdate.bin",
                          indexes_dir + "/zone_map_o_orderdate.bin", 100000);
    build_hash_index_single(base_dir + "/orders/o_orderkey.bin",
                            indexes_dir + "/hash_o_orderkey.bin");
    build_hash_index_multi(base_dir + "/orders/o_custkey.bin",
                           indexes_dir + "/hash_o_custkey.bin");

    // Customer indexes
    std::cout << "Customer indexes..." << std::endl;
    build_zone_map_single(base_dir + "/customer/c_mktsegment.bin",
                          indexes_dir + "/zone_map_c_mktsegment.bin", 100000);
    build_hash_index_single(base_dir + "/customer/c_custkey.bin",
                            indexes_dir + "/hash_c_custkey.bin");

    // Part indexes
    std::cout << "Part indexes..." << std::endl;
    build_hash_index_single(base_dir + "/part/p_partkey.bin",
                            indexes_dir + "/hash_p_partkey.bin");

    // Partsupp indexes
    std::cout << "Partsupp indexes..." << std::endl;
    build_hash_index_multi(base_dir + "/partsupp/ps_partkey.bin",
                           indexes_dir + "/hash_ps_partkey.bin");
    build_hash_index_multi(base_dir + "/partsupp/ps_suppkey.bin",
                           indexes_dir + "/hash_ps_suppkey.bin");

    std::cout << "All indexes built successfully!" << std::endl;
    return 0;
}
