#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <filesystem>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace fs = std::filesystem;
using std::string;
using std::vector;

// Structure for multi-value hash index
struct MultiValueHashIndex {
    struct Entry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    vector<Entry> entries;
    vector<uint32_t> positions;
};

// Structure for zone map
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
};

// Memory map helper
template<typename T>
class MmapedArray {
public:
    int fd = -1;
    void* ptr = nullptr;
    size_t size = 0;

    MmapedArray(const string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open " + path);
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            throw std::runtime_error("Cannot stat " + path);
        }

        size = sb.st_size / sizeof(T);
        ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            throw std::runtime_error("Cannot mmap " + path);
        }

        // Hint: sequential access
        madvise(ptr, sb.st_size, MADV_SEQUENTIAL);
    }

    T* get() const { return (T*)ptr; }
    size_t count() const { return size; }

    ~MmapedArray() {
        if (ptr) munmap(ptr, size * sizeof(T));
        if (fd >= 0) close(fd);
    }
};

// Build single-value hash index (for primary keys)
void build_hash_single(const string& gendb_dir, const string& table_name,
                       const string& col_name) {
    std::cout << "Building hash_single index: " << table_name << "." << col_name << "\n";

    string col_file = gendb_dir + "/" + table_name + "/" + col_name + ".bin";
    MmapedArray<int32_t> col_data(col_file);

    // Build hash table (simple open addressing, power-of-2 sizing)
    size_t n_rows = col_data.count();
    size_t capacity = 1;
    while (capacity < n_rows * 2) capacity *= 2;

    struct HashSlot {
        int32_t key;
        uint32_t row_id;
    };

    vector<HashSlot> hash_table(capacity, {-1, UINT32_MAX});

    // Insert all rows
    auto* data = col_data.get();
    for (uint32_t i = 0; i < n_rows; i++) {
        int32_t key = data[i];
        uint64_t h = ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t pos = h & (capacity - 1);

        // Linear probing
        while (hash_table[pos].row_id != UINT32_MAX) {
            pos = (pos + 1) & (capacity - 1);
        }
        hash_table[pos] = {key, i};
    }

    // Write index
    string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    string idx_file = idx_dir + "/" + table_name + "_" + col_name + "_hash.bin";
    std::ofstream out(idx_file, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to write " << idx_file << "\n";
        return;
    }

    uint32_t cap = capacity;
    out.write((const char*)&cap, sizeof(uint32_t));

    for (const auto& slot : hash_table) {
        out.write((const char*)&slot.key, sizeof(int32_t));
        out.write((const char*)&slot.row_id, sizeof(uint32_t));
    }
    out.close();

    std::cout << "  Wrote " << idx_file << "\n";
}

// Build multi-value hash index (for foreign keys with duplicates)
void build_hash_multi_value(const string& gendb_dir, const string& table_name,
                            const string& col_name) {
    std::cout << "Building hash_multi_value index: " << table_name << "." << col_name << "\n";

    string col_file = gendb_dir + "/" + table_name + "/" + col_name + ".bin";
    MmapedArray<int32_t> col_data(col_file);

    size_t n_rows = col_data.count();
    auto* data = col_data.get();

    // Count unique keys and group positions
    std::map<int32_t, vector<uint32_t>> key_positions;
    for (uint32_t i = 0; i < n_rows; i++) {
        key_positions[data[i]].push_back(i);
    }

    // Build positions array and hash table
    vector<uint32_t> positions;
    vector<std::pair<int32_t, std::pair<uint32_t, uint32_t>>> entries; // key, (offset, count)

    for (auto& [key, pos_list] : key_positions) {
        uint32_t offset = positions.size();
        for (uint32_t pos : pos_list) {
            positions.push_back(pos);
        }
        entries.push_back({key, {offset, (uint32_t)pos_list.size()}});
    }

    // Size hash table
    size_t capacity = 1;
    while (capacity < entries.size() * 2) capacity *= 2;

    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    vector<HashEntry> hash_table(capacity, {-1, UINT32_MAX, 0});

    // Insert into hash table
    for (const auto& [key, offset_count] : entries) {
        uint64_t h = ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t pos = h & (capacity - 1);

        while (hash_table[pos].key != -1) {
            pos = (pos + 1) & (capacity - 1);
        }
        hash_table[pos] = {key, offset_count.first, offset_count.second};
    }

    // Write index
    string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    string idx_file = idx_dir + "/" + table_name + "_" + col_name + "_hash.bin";
    std::ofstream out(idx_file, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to write " << idx_file << "\n";
        return;
    }

    // Header: num_entries, table_size, then hash table, then positions
    uint32_t num_entries = entries.size();
    uint32_t table_size = capacity;
    uint32_t pos_count = positions.size();

    out.write((const char*)&num_entries, sizeof(uint32_t));
    out.write((const char*)&table_size, sizeof(uint32_t));

    // Write hash table slots
    for (const auto& slot : hash_table) {
        out.write((const char*)&slot.key, sizeof(int32_t));
        out.write((const char*)&slot.offset, sizeof(uint32_t));
        out.write((const char*)&slot.count, sizeof(uint32_t));
    }

    // Write positions count and array
    out.write((const char*)&pos_count, sizeof(uint32_t));
    out.write((const char*)positions.data(), positions.size() * sizeof(uint32_t));

    out.close();

    std::cout << "  Wrote " << idx_file << " (hash table: " << capacity << " slots, "
              << "positions: " << positions.size() << " entries)\n";
}

// Build zone map (min/max index) for range filtering
void build_zone_map(const string& gendb_dir, const string& table_name,
                    const string& col_name, uint32_t block_size) {
    std::cout << "Building zone_map index: " << table_name << "." << col_name << "\n";

    string col_file = gendb_dir + "/" + table_name + "/" + col_name + ".bin";
    MmapedArray<int32_t> col_data(col_file);

    size_t n_rows = col_data.count();
    auto* data = col_data.get();

    // Build zone maps per block
    vector<ZoneMapEntry> zones;
    for (uint32_t block_start = 0; block_start < n_rows; block_start += block_size) {
        uint32_t block_end = std::min((uint32_t)n_rows, block_start + block_size);
        uint32_t block_rows = block_end - block_start;

        int32_t min_val = data[block_start];
        int32_t max_val = data[block_start];

        for (uint32_t i = block_start; i < block_end; i++) {
            min_val = std::min(min_val, data[i]);
            max_val = std::max(max_val, data[i]);
        }

        zones.push_back({min_val, max_val, block_rows});
    }

    // Write zone map
    string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    string idx_file = idx_dir + "/" + table_name + "_" + col_name + "_zonemap.bin";
    std::ofstream out(idx_file, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to write " << idx_file << "\n";
        return;
    }

    uint32_t num_zones = zones.size();
    out.write((const char*)&num_zones, sizeof(uint32_t));

    for (const auto& zone : zones) {
        out.write((const char*)&zone.min_val, sizeof(int32_t));
        out.write((const char*)&zone.max_val, sizeof(int32_t));
        out.write((const char*)&zone.count, sizeof(uint32_t));
    }
    out.close();

    std::cout << "  Wrote " << idx_file << " (" << zones.size() << " zones)\n";
}

// Main index building
int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    string gendb_dir = argv[1];
    std::cout << "GenDB Index Building\n";
    std::cout << "GenDB Dir: " << gendb_dir << "\n\n";

    // Build indexes for each table
    // Table: nation
    build_hash_single(gendb_dir, "nation", "n_nationkey");

    // Table: region
    build_hash_single(gendb_dir, "region", "r_regionkey");

    // Table: supplier
    build_hash_single(gendb_dir, "supplier", "s_suppkey");
    build_hash_multi_value(gendb_dir, "supplier", "s_nationkey");

    // Table: part
    build_hash_single(gendb_dir, "part", "p_partkey");

    // Table: partsupp
    build_hash_multi_value(gendb_dir, "partsupp", "ps_partkey");
    build_hash_multi_value(gendb_dir, "partsupp", "ps_suppkey");

    // Table: customer
    build_hash_single(gendb_dir, "customer", "c_custkey");
    build_hash_multi_value(gendb_dir, "customer", "c_nationkey");
    build_hash_multi_value(gendb_dir, "customer", "c_mktsegment");

    // Table: orders
    build_hash_single(gendb_dir, "orders", "o_orderkey");
    build_hash_multi_value(gendb_dir, "orders", "o_custkey");
    build_zone_map(gendb_dir, "orders", "o_orderdate", 100000);

    // Table: lineitem
    build_hash_multi_value(gendb_dir, "lineitem", "l_orderkey");
    build_hash_multi_value(gendb_dir, "lineitem", "l_partkey");  // Will be combined with l_suppkey in query execution
    build_hash_multi_value(gendb_dir, "lineitem", "l_suppkey");  // Same here
    build_zone_map(gendb_dir, "lineitem", "l_shipdate", 200000);

    std::cout << "\nIndex building complete!\n";
    return 0;
}
