#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <omp.h>
#include <filesystem>

namespace fs = std::filesystem;

// Hash function (multiply-shift)
inline uint64_t hash_fn(uint64_t key) {
    return (key * 0x9E3779B97F4A7C15ULL) ^ ((key * 0x9E3779B97F4A7C15ULL) >> 32);
}

// Mmap utility
class MmappedFile {
public:
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            ::close(fd);
            return false;
        }
        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            data = nullptr;
            ::close(fd);
            return false;
        }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }

    ~MmappedFile() {
        if (data) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }

    template<typename T>
    const T* as() const { return reinterpret_cast<const T*>(data); }

    template<typename T>
    size_t count() const { return size / sizeof(T); }
};

// Zone map builder
void build_zone_maps(const std::string& gendb_dir) {
    std::cout << "Building zone maps..." << std::endl;

    // Create indexes directory
    fs::create_directories(gendb_dir + "/indexes");

    // Lineitem zone maps
    {
        // l_shipdate zone map (block size 131072 rows, sizeof(int32_t) = 4)
        MmappedFile l_shipdate_file;
        if (!l_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin")) {
            std::cerr << "Failed to open l_shipdate.bin" << std::endl;
            return;
        }

        const int32_t* l_shipdate = l_shipdate_file.as<int32_t>();
        size_t num_rows = l_shipdate_file.count<int32_t>();
        size_t block_size = 131072;
        size_t num_blocks = (num_rows + block_size - 1) / block_size;

        std::ofstream zone_map_file(gendb_dir + "/indexes/l_shipdate_zone_map.bin", std::ios::binary);
        uint32_t block_count = num_blocks;
        zone_map_file.write(reinterpret_cast<const char*>(&block_count), sizeof(uint32_t));

        #pragma omp parallel for schedule(static)
        for (size_t b = 0; b < num_blocks; ++b) {
            size_t start = b * block_size;
            size_t end = std::min(start + block_size, num_rows);
            int32_t block_min = INT32_MAX;
            int32_t block_max = INT32_MIN;

            for (size_t i = start; i < end; ++i) {
                block_min = std::min(block_min, l_shipdate[i]);
                block_max = std::max(block_max, l_shipdate[i]);
            }

            // Write block metadata: min, max
            #pragma omp critical
            {
                zone_map_file.seekp(sizeof(uint32_t) + b * (sizeof(int32_t) * 2), std::ios::beg);
                zone_map_file.write(reinterpret_cast<const char*>(&block_min), sizeof(int32_t));
                zone_map_file.write(reinterpret_cast<const char*>(&block_max), sizeof(int32_t));
            }
        }
        zone_map_file.close();
        std::cout << "Built l_shipdate_zone_map: " << num_blocks << " blocks" << std::endl;

        // l_discount zone map
        MmappedFile l_discount_file;
        if (l_discount_file.open(gendb_dir + "/lineitem/l_discount.bin")) {
            const int64_t* l_discount = l_discount_file.as<int64_t>();
            size_t num_rows_discount = l_discount_file.count<int64_t>();
            size_t num_blocks_d = (num_rows_discount + block_size - 1) / block_size;

            std::ofstream zone_map_file_d(gendb_dir + "/indexes/l_discount_zone_map.bin", std::ios::binary);
            uint32_t block_count_d = num_blocks_d;
            zone_map_file_d.write(reinterpret_cast<const char*>(&block_count_d), sizeof(uint32_t));

            #pragma omp parallel for schedule(static)
            for (size_t b = 0; b < num_blocks_d; ++b) {
                size_t start = b * block_size;
                size_t end = std::min(start + block_size, num_rows_discount);
                int64_t block_min = INT64_MAX;
                int64_t block_max = INT64_MIN;

                for (size_t i = start; i < end; ++i) {
                    block_min = std::min(block_min, l_discount[i]);
                    block_max = std::max(block_max, l_discount[i]);
                }

                #pragma omp critical
                {
                    zone_map_file_d.seekp(sizeof(uint32_t) + b * (sizeof(int64_t) * 2), std::ios::beg);
                    zone_map_file_d.write(reinterpret_cast<const char*>(&block_min), sizeof(int64_t));
                    zone_map_file_d.write(reinterpret_cast<const char*>(&block_max), sizeof(int64_t));
                }
            }
            zone_map_file_d.close();
            std::cout << "Built l_discount_zone_map: " << num_blocks_d << " blocks" << std::endl;
        }

        // l_quantity zone map
        MmappedFile l_quantity_file;
        if (l_quantity_file.open(gendb_dir + "/lineitem/l_quantity.bin")) {
            const int64_t* l_quantity = l_quantity_file.as<int64_t>();
            size_t num_rows_qty = l_quantity_file.count<int64_t>();
            size_t num_blocks_q = (num_rows_qty + block_size - 1) / block_size;

            std::ofstream zone_map_file_q(gendb_dir + "/indexes/l_quantity_zone_map.bin", std::ios::binary);
            uint32_t block_count_q = num_blocks_q;
            zone_map_file_q.write(reinterpret_cast<const char*>(&block_count_q), sizeof(uint32_t));

            #pragma omp parallel for schedule(static)
            for (size_t b = 0; b < num_blocks_q; ++b) {
                size_t start = b * block_size;
                size_t end = std::min(start + block_size, num_rows_qty);
                int64_t block_min = INT64_MAX;
                int64_t block_max = INT64_MIN;

                for (size_t i = start; i < end; ++i) {
                    block_min = std::min(block_min, l_quantity[i]);
                    block_max = std::max(block_max, l_quantity[i]);
                }

                #pragma omp critical
                {
                    zone_map_file_q.seekp(sizeof(uint32_t) + b * (sizeof(int64_t) * 2), std::ios::beg);
                    zone_map_file_q.write(reinterpret_cast<const char*>(&block_min), sizeof(int64_t));
                    zone_map_file_q.write(reinterpret_cast<const char*>(&block_max), sizeof(int64_t));
                }
            }
            zone_map_file_q.close();
            std::cout << "Built l_quantity_zone_map: " << num_blocks_q << " blocks" << std::endl;
        }
    }

    // Orders zone maps
    {
        MmappedFile o_orderdate_file;
        if (o_orderdate_file.open(gendb_dir + "/orders/o_orderdate.bin")) {
            const int32_t* o_orderdate = o_orderdate_file.as<int32_t>();
            size_t num_rows = o_orderdate_file.count<int32_t>();
            size_t block_size = 131072;
            size_t num_blocks = (num_rows + block_size - 1) / block_size;

            std::ofstream zone_map_file(gendb_dir + "/indexes/o_orderdate_zone_map.bin", std::ios::binary);
            uint32_t block_count = num_blocks;
            zone_map_file.write(reinterpret_cast<const char*>(&block_count), sizeof(uint32_t));

            #pragma omp parallel for schedule(static)
            for (size_t b = 0; b < num_blocks; ++b) {
                size_t start = b * block_size;
                size_t end = std::min(start + block_size, num_rows);
                int32_t block_min = INT32_MAX;
                int32_t block_max = INT32_MIN;

                for (size_t i = start; i < end; ++i) {
                    block_min = std::min(block_min, o_orderdate[i]);
                    block_max = std::max(block_max, o_orderdate[i]);
                }

                #pragma omp critical
                {
                    zone_map_file.seekp(sizeof(uint32_t) + b * (sizeof(int32_t) * 2), std::ios::beg);
                    zone_map_file.write(reinterpret_cast<const char*>(&block_min), sizeof(int32_t));
                    zone_map_file.write(reinterpret_cast<const char*>(&block_max), sizeof(int32_t));
                }
            }
            zone_map_file.close();
            std::cout << "Built o_orderdate_zone_map: " << num_blocks << " blocks" << std::endl;
        }

        // c_mktsegment zone map
        MmappedFile c_mktsegment_file;
        if (c_mktsegment_file.open(gendb_dir + "/customer/c_mktsegment.bin")) {
            const uint8_t* c_mktsegment = c_mktsegment_file.as<uint8_t>();
            size_t num_rows = c_mktsegment_file.count<uint8_t>();
            size_t block_size = 131072;
            size_t num_blocks = (num_rows + block_size - 1) / block_size;

            std::ofstream zone_map_file(gendb_dir + "/indexes/c_mktsegment_zone_map.bin", std::ios::binary);
            uint32_t block_count = num_blocks;
            zone_map_file.write(reinterpret_cast<const char*>(&block_count), sizeof(uint32_t));

            #pragma omp parallel for schedule(static)
            for (size_t b = 0; b < num_blocks; ++b) {
                size_t start = b * block_size;
                size_t end = std::min(start + block_size, num_rows);
                uint8_t block_min = UINT8_MAX;
                uint8_t block_max = 0;

                for (size_t i = start; i < end; ++i) {
                    block_min = std::min(block_min, c_mktsegment[i]);
                    block_max = std::max(block_max, c_mktsegment[i]);
                }

                #pragma omp critical
                {
                    zone_map_file.seekp(sizeof(uint32_t) + b * (sizeof(uint8_t) * 2), std::ios::beg);
                    zone_map_file.write(reinterpret_cast<const char*>(&block_min), sizeof(uint8_t));
                    zone_map_file.write(reinterpret_cast<const char*>(&block_max), sizeof(uint8_t));
                }
            }
            zone_map_file.close();
            std::cout << "Built c_mktsegment_zone_map: " << num_blocks << " blocks" << std::endl;
        }
    }
}

// Multi-value hash index builder
void build_hash_indexes(const std::string& gendb_dir) {
    std::cout << "Building hash indexes..." << std::endl;

    // l_orderkey hash index
    {
        MmappedFile l_orderkey_file;
        if (!l_orderkey_file.open(gendb_dir + "/lineitem/l_orderkey.bin")) {
            std::cerr << "Failed to open l_orderkey.bin" << std::endl;
            return;
        }

        const int32_t* l_orderkey = l_orderkey_file.as<int32_t>();
        size_t num_rows = l_orderkey_file.count<int32_t>();

        // Count unique keys
        std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;

        #pragma omp parallel for schedule(static, 10000)
        for (size_t i = 0; i < num_rows; ++i) {
            int32_t key = l_orderkey[i];
            #pragma omp critical
            {
                key_to_positions[key].push_back(static_cast<uint32_t>(i));
            }
        }

        // Build hash table
        size_t num_unique = key_to_positions.size();
        size_t table_size = 1 << (int)std::ceil(std::log2(num_unique / 0.6)); // Load factor 0.6
        table_size = std::max(table_size, size_t(1024));

        struct HashEntry {
            int32_t key;
            uint32_t offset;
            uint32_t count;
        };

        std::vector<HashEntry> hash_table(table_size, {-1, 0, 0});
        std::vector<uint32_t> positions_array;

        // Fill positions array and hash table
        uint32_t pos_offset = 0;
        for (auto& [key, positions] : key_to_positions) {
            for (uint32_t pos : positions) {
                positions_array.push_back(pos);
            }

            // Insert into hash table
            uint64_t h = hash_fn(key);
            size_t idx = h % table_size;
            while (hash_table[idx].key != -1) {
                idx = (idx + 1) % table_size;
            }
            hash_table[idx] = {key, pos_offset, static_cast<uint32_t>(positions.size())};
            pos_offset += positions.size();
        }

        // Write to file
        std::ofstream hash_file(gendb_dir + "/indexes/l_orderkey_hash.bin", std::ios::binary);
        uint32_t unique_count = num_unique;
        hash_file.write(reinterpret_cast<const char*>(&unique_count), sizeof(uint32_t));
        uint32_t table_sz = table_size;
        hash_file.write(reinterpret_cast<const char*>(&table_sz), sizeof(uint32_t));

        for (const auto& entry : hash_table) {
            hash_file.write(reinterpret_cast<const char*>(&entry.key), sizeof(int32_t));
            hash_file.write(reinterpret_cast<const char*>(&entry.offset), sizeof(uint32_t));
            hash_file.write(reinterpret_cast<const char*>(&entry.count), sizeof(uint32_t));
        }

        uint32_t pos_count = positions_array.size();
        hash_file.write(reinterpret_cast<const char*>(&pos_count), sizeof(uint32_t));
        for (uint32_t pos : positions_array) {
            hash_file.write(reinterpret_cast<const char*>(&pos), sizeof(uint32_t));
        }

        hash_file.close();
        std::cout << "Built l_orderkey_hash: " << num_unique << " unique keys, " << positions_array.size() << " positions" << std::endl;
    }

    // o_orderkey hash index
    {
        MmappedFile o_orderkey_file;
        if (!o_orderkey_file.open(gendb_dir + "/orders/o_orderkey.bin")) {
            std::cerr << "Failed to open o_orderkey.bin" << std::endl;
            return;
        }

        const int32_t* o_orderkey = o_orderkey_file.as<int32_t>();
        size_t num_rows = o_orderkey_file.count<int32_t>();

        // Build simple perfect hash (orderkey is PK, unique)
        std::unordered_map<int32_t, uint32_t> key_to_position;

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < num_rows; ++i) {
            int32_t key = o_orderkey[i];
            #pragma omp critical
            {
                key_to_position[key] = static_cast<uint32_t>(i);
            }
        }

        // Build hash table
        size_t num_unique = key_to_position.size();
        size_t table_size = 1 << (int)std::ceil(std::log2(num_unique / 0.6));
        table_size = std::max(table_size, size_t(1024));

        struct HashEntry {
            int32_t key;
            uint32_t position;
        };

        std::vector<HashEntry> hash_table(table_size, {-1, UINT32_MAX});

        for (auto& [key, pos] : key_to_position) {
            uint64_t h = hash_fn(key);
            size_t idx = h % table_size;
            while (hash_table[idx].key != -1) {
                idx = (idx + 1) % table_size;
            }
            hash_table[idx] = {key, pos};
        }

        // Write to file
        std::ofstream hash_file(gendb_dir + "/indexes/o_orderkey_hash.bin", std::ios::binary);
        uint32_t unique_count = num_unique;
        hash_file.write(reinterpret_cast<const char*>(&unique_count), sizeof(uint32_t));
        uint32_t table_sz = table_size;
        hash_file.write(reinterpret_cast<const char*>(&table_sz), sizeof(uint32_t));

        for (const auto& entry : hash_table) {
            hash_file.write(reinterpret_cast<const char*>(&entry.key), sizeof(int32_t));
            hash_file.write(reinterpret_cast<const char*>(&entry.position), sizeof(uint32_t));
        }

        hash_file.close();
        std::cout << "Built o_orderkey_hash: " << num_unique << " unique keys" << std::endl;
    }

    // o_custkey and c_custkey hash indexes
    {
        MmappedFile o_custkey_file;
        if (o_custkey_file.open(gendb_dir + "/orders/o_custkey.bin")) {
            const int32_t* o_custkey = o_custkey_file.as<int32_t>();
            size_t num_rows = o_custkey_file.count<int32_t>();

            std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;

            #pragma omp parallel for schedule(static, 10000)
            for (size_t i = 0; i < num_rows; ++i) {
                int32_t key = o_custkey[i];
                #pragma omp critical
                {
                    key_to_positions[key].push_back(static_cast<uint32_t>(i));
                }
            }

            size_t num_unique = key_to_positions.size();
            size_t table_size = 1 << (int)std::ceil(std::log2(num_unique / 0.6));
            table_size = std::max(table_size, size_t(1024));

            struct HashEntry {
                int32_t key;
                uint32_t offset;
                uint32_t count;
            };

            std::vector<HashEntry> hash_table(table_size, {-1, 0, 0});
            std::vector<uint32_t> positions_array;

            uint32_t pos_offset = 0;
            for (auto& [key, positions] : key_to_positions) {
                for (uint32_t pos : positions) {
                    positions_array.push_back(pos);
                }

                uint64_t h = hash_fn(key);
                size_t idx = h % table_size;
                while (hash_table[idx].key != -1) {
                    idx = (idx + 1) % table_size;
                }
                hash_table[idx] = {key, pos_offset, static_cast<uint32_t>(positions.size())};
                pos_offset += positions.size();
            }

            std::ofstream hash_file(gendb_dir + "/indexes/o_custkey_hash.bin", std::ios::binary);
            uint32_t unique_count = num_unique;
            hash_file.write(reinterpret_cast<const char*>(&unique_count), sizeof(uint32_t));
            uint32_t table_sz = table_size;
            hash_file.write(reinterpret_cast<const char*>(&table_sz), sizeof(uint32_t));

            for (const auto& entry : hash_table) {
                hash_file.write(reinterpret_cast<const char*>(&entry.key), sizeof(int32_t));
                hash_file.write(reinterpret_cast<const char*>(&entry.offset), sizeof(uint32_t));
                hash_file.write(reinterpret_cast<const char*>(&entry.count), sizeof(uint32_t));
            }

            uint32_t pos_count = positions_array.size();
            hash_file.write(reinterpret_cast<const char*>(&pos_count), sizeof(uint32_t));
            for (uint32_t pos : positions_array) {
                hash_file.write(reinterpret_cast<const char*>(&pos), sizeof(uint32_t));
            }

            hash_file.close();
            std::cout << "Built o_custkey_hash: " << num_unique << " unique keys" << std::endl;
        }
    }

    // c_custkey hash index
    {
        MmappedFile c_custkey_file;
        if (c_custkey_file.open(gendb_dir + "/customer/c_custkey.bin")) {
            const int32_t* c_custkey = c_custkey_file.as<int32_t>();
            size_t num_rows = c_custkey_file.count<int32_t>();

            std::unordered_map<int32_t, uint32_t> key_to_position;

            #pragma omp parallel for schedule(static)
            for (size_t i = 0; i < num_rows; ++i) {
                int32_t key = c_custkey[i];
                #pragma omp critical
                {
                    key_to_position[key] = static_cast<uint32_t>(i);
                }
            }

            size_t num_unique = key_to_position.size();
            size_t table_size = 1 << (int)std::ceil(std::log2(num_unique / 0.6));
            table_size = std::max(table_size, size_t(1024));

            struct HashEntry {
                int32_t key;
                uint32_t position;
            };

            std::vector<HashEntry> hash_table(table_size, {-1, UINT32_MAX});

            for (auto& [key, pos] : key_to_position) {
                uint64_t h = hash_fn(key);
                size_t idx = h % table_size;
                while (hash_table[idx].key != -1) {
                    idx = (idx + 1) % table_size;
                }
                hash_table[idx] = {key, pos};
            }

            std::ofstream hash_file(gendb_dir + "/indexes/c_custkey_hash.bin", std::ios::binary);
            uint32_t unique_count = num_unique;
            hash_file.write(reinterpret_cast<const char*>(&unique_count), sizeof(uint32_t));
            uint32_t table_sz = table_size;
            hash_file.write(reinterpret_cast<const char*>(&table_sz), sizeof(uint32_t));

            for (const auto& entry : hash_table) {
                hash_file.write(reinterpret_cast<const char*>(&entry.key), sizeof(int32_t));
                hash_file.write(reinterpret_cast<const char*>(&entry.position), sizeof(uint32_t));
            }

            hash_file.close();
            std::cout << "Built c_custkey_hash: " << num_unique << " unique keys" << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::cout << "Building indexes for GenDB at: " << gendb_dir << std::endl;

    build_zone_maps(gendb_dir);
    build_hash_indexes(gendb_dir);

    std::cout << "Index building complete!" << std::endl;

    return 0;
}
