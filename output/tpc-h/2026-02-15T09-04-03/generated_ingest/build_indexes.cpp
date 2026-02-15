#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <unordered_map>
#include <omp.h>

namespace fs = std::filesystem;

const uint32_t HASH_LOAD_FACTOR = 2;  // Load factor for hash tables

// Zone map entry: min, max per block
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t block_num;
    uint32_t row_count;
};

// Hash index multi-value entry
struct HashMultiValueIndex {
    uint32_t num_unique_keys;
    uint32_t table_size;
    // Followed by hash table entries, then positions array
};

// Hash index single-value entry
struct HashSingleIndex {
    uint32_t num_entries;
    // Followed by entries: key (int32_t) + position (uint32_t)
};

// Mmap helper
template<typename T>
const T* mmapFile(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return nullptr;
    }

    off_t fileSize = lseek(fd, 0, SEEK_END);
    if (fileSize < 0) {
        std::cerr << "Error seeking " << path << std::endl;
        close(fd);
        return nullptr;
    }

    count = fileSize / sizeof(T);
    const T* data = (const T*)mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Error mmapping " << path << std::endl;
        close(fd);
        return nullptr;
    }

    madvise((void*)data, fileSize, MADV_SEQUENTIAL);
    close(fd);
    return data;
}

void munmapFile(const void* ptr, size_t size) {
    if (ptr) munmap((void*)ptr, size);
}

// Build zone map for int32_t column
void buildZoneMapInt32(const std::string& column_path, const std::string& output_path, uint32_t block_size) {
    size_t count = 0;
    const int32_t* data = mmapFile<int32_t>(column_path, count);
    if (!data) return;

    std::cout << "  Building zone map for " << fs::path(column_path).filename().string()
              << " (" << count << " rows, block_size=" << block_size << ")..." << std::endl;

    std::vector<ZoneMapEntry> zones;
    uint32_t num_blocks = (count + block_size - 1) / block_size;

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        size_t start = (size_t)b * block_size;
        size_t end = std::min(start + block_size, count);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        ZoneMapEntry entry;
        entry.min_val = min_val;
        entry.max_val = max_val;
        entry.block_num = b;
        entry.row_count = end - start;

        #pragma omp critical
        zones.push_back(entry);
    }

    std::sort(zones.begin(), zones.end(), [](const ZoneMapEntry& a, const ZoneMapEntry& b) {
        return a.block_num < b.block_num;
    });

    // Write zone map
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_zones = zones.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(uint32_t));
    for (const auto& zone : zones) {
        out.write(reinterpret_cast<const char*>(&zone.min_val), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&zone.max_val), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&zone.block_num), sizeof(uint32_t));
        out.write(reinterpret_cast<const char*>(&zone.row_count), sizeof(uint32_t));
    }
    out.close();

    std::cout << "  Zone map written: " << num_zones << " zones" << std::endl;
}

// Build hash index (single-value) for primary keys
void buildHashIndexSingle(const std::string& key_path, const std::string& output_path) {
    size_t count = 0;
    const int32_t* keys = mmapFile<int32_t>(key_path, count);
    if (!keys) return;

    std::cout << "  Building single-value hash index for " << fs::path(key_path).filename().string()
              << " (" << count << " rows)..." << std::endl;

    std::vector<std::pair<int32_t, uint32_t>> entries;
    entries.reserve(count);

    for (size_t i = 0; i < count; i++) {
        entries.push_back({keys[i], (uint32_t)i});
    }

    std::sort(entries.begin(), entries.end());

    // Write hash index: count followed by (key, position) pairs
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_entries = count;
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));

    for (const auto& entry : entries) {
        out.write(reinterpret_cast<const char*>(&entry.first), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&entry.second), sizeof(uint32_t));
    }
    out.close();

    std::cout << "  Single-value hash index written: " << num_entries << " entries" << std::endl;
}

// Build hash index (multi-value) for foreign keys
void buildHashIndexMultiValue(const std::string& key_path, const std::string& output_path) {
    size_t count = 0;
    const int32_t* keys = mmapFile<int32_t>(key_path, count);
    if (!keys) return;

    std::cout << "  Building multi-value hash index for " << fs::path(key_path).filename().string()
              << " (" << count << " rows)..." << std::endl;

    // Step 1: Single-pass histogram with local atomic counters
    std::unordered_map<int32_t, uint32_t> key_count;
    std::vector<std::unordered_map<int32_t, uint32_t>> thread_counts(omp_get_max_threads());

    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < count; i++) {
        int tid = omp_get_thread_num();
        thread_counts[tid][keys[i]]++;
    }

    // Merge thread-local counts
    for (const auto& tc : thread_counts) {
        for (const auto& [key, cnt] : tc) {
            key_count[key] += cnt;
        }
    }

    uint32_t num_unique = key_count.size();
    uint32_t table_size = std::max(num_unique * HASH_LOAD_FACTOR, 16U);

    // Step 2: Allocate positions array and compute offsets
    std::vector<uint32_t> positions(count);
    std::unordered_map<int32_t, uint32_t> offset_map;

    uint32_t current_offset = 0;
    for (auto& [key, cnt] : key_count) {
        offset_map[key] = current_offset;
        current_offset += cnt;
    }

    // Step 3: Build key->index map for faster lookup
    std::vector<std::pair<int32_t, uint32_t>> key_index_map(key_count.begin(), key_count.end());
    std::sort(key_index_map.begin(), key_index_map.end());

    // Scatter with thread-local counters
    std::vector<std::unordered_map<int32_t, uint32_t>> counters(omp_get_max_threads());

    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < count; i++) {
        int32_t key = keys[i];
        int tid = omp_get_thread_num();
        uint32_t offset = offset_map[key];
        uint32_t local_pos;

        #pragma omp critical
        {
            local_pos = counters[tid][key]++;
        }
        positions[offset + local_pos] = (uint32_t)i;
    }

    // Rebuild counters for proper offset
    std::vector<uint32_t> offsets(num_unique, 0);
    std::vector<int32_t> unique_keys_vec;
    for (auto& [key, _] : key_count) unique_keys_vec.push_back(key);

    #pragma omp parallel for schedule(static)
    for (int tid = 0; tid < omp_get_max_threads(); tid++) {
        for (auto& [key, cnt] : counters[tid]) {
            #pragma omp critical
            {
                // Reset for actual consolidation
            }
        }
    }

    // Step 4: Build hash table on unique keys
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> hash_table(table_size, {-1, 0, 0});

    auto hash_fn = [](int32_t key) -> uint32_t {
        return (uint32_t)((uint64_t)key * 0x9E3779B97F4A7C15ULL >> 32);
    };

    for (const auto& [key, cnt] : key_count) {
        uint32_t h = hash_fn(key) % table_size;
        while (hash_table[h].key != -1 && hash_table[h].key != key) {
            h = (h + 1) % table_size;  // Linear probing
        }
        hash_table[h].key = key;
        hash_table[h].offset = offset_map[key];
        hash_table[h].count = cnt;
    }

    // Step 5: Write hash index
    std::ofstream out(output_path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(&num_unique), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&table_size), sizeof(uint32_t));

    for (const auto& entry : hash_table) {
        out.write(reinterpret_cast<const char*>(&entry.key), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(uint32_t));
        out.write(reinterpret_cast<const char*>(&entry.count), sizeof(uint32_t));
    }

    out.write(reinterpret_cast<const char*>(positions.data()), positions.size() * sizeof(uint32_t));
    out.close();

    std::cout << "  Multi-value hash index written: " << num_unique << " unique keys, table_size=" << table_size << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendbDir = argv[1];
    std::string indexDir = gendbDir + "/indexes";
    fs::create_directories(indexDir);

    std::cout << "Building indexes..." << std::endl;

    // Lineitem zone maps and indexes
    std::cout << "\n=== Lineitem Indexes ===" << std::endl;
    buildZoneMapInt32(gendbDir + "/lineitem/l_shipdate.bin", indexDir + "/lineitem_shipdate_zone.bin", 500000);
    buildHashIndexMultiValue(gendbDir + "/lineitem/l_orderkey.bin", indexDir + "/lineitem_orderkey_hash.bin");

    // Orders zone maps and indexes
    std::cout << "\n=== Orders Indexes ===" << std::endl;
    buildZoneMapInt32(gendbDir + "/orders/o_orderdate.bin", indexDir + "/orders_orderdate_zone.bin", 300000);
    buildHashIndexSingle(gendbDir + "/orders/o_orderkey.bin", indexDir + "/orders_orderkey_hash.bin");
    buildHashIndexMultiValue(gendbDir + "/orders/o_custkey.bin", indexDir + "/orders_custkey_hash.bin");

    // Customer indexes
    std::cout << "\n=== Customer Indexes ===" << std::endl;
    buildHashIndexSingle(gendbDir + "/customer/c_custkey.bin", indexDir + "/customer_custkey_hash.bin");

    // Part indexes
    std::cout << "\n=== Part Indexes ===" << std::endl;
    buildHashIndexSingle(gendbDir + "/part/p_partkey.bin", indexDir + "/part_partkey_hash.bin");

    std::cout << "\n\nIndex building complete!" << std::endl;
    std::cout << "Indexes written to: " << indexDir << std::endl;

    return 0;
}
