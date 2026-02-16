#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ============================================================================
// Memory-Mapped Column Reader
// ============================================================================

template<typename T>
class MappedColumn {
public:
    const T* data;
    size_t size;

    MappedColumn(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << "\n";
            data = nullptr;
            size = 0;
            return;
        }

        struct stat st;
        fstat(fd, &st);
        size_t file_size = st.st_size;
        size = file_size / sizeof(T);

        void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << "\n";
            data = nullptr;
            size = 0;
            close(fd);
            return;
        }

        madvise(mapped, file_size, MADV_SEQUENTIAL);
        data = (const T*)mapped;
        close(fd);
    }
};

// ============================================================================
// Zone Map Structure
// ============================================================================

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

struct ZoneMapEntry64 {
    int64_t min_val;
    int64_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

// ============================================================================
// Hash Index - Multi-Value Design
// ============================================================================

struct HashEntry {
    int32_t key;
    uint32_t offset;  // offset into positions array
    uint32_t count;   // number of positions for this key
};

// Multiply-shift hash function (NEVER use std::hash on integers)
static inline uint32_t hash_int32(int32_t key, uint32_t shift) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> shift;
}

// ============================================================================
// Build Zone Map (int32_t columns)
// ============================================================================

void build_zone_map_int32(const std::string& input_path, const std::string& output_path, uint32_t block_size) {
    std::cout << "Building zone map: " << output_path << "\n";

    MappedColumn<int32_t> col(input_path);
    if (!col.data) return;

    size_t num_rows = col.size;
    size_t num_blocks = (num_rows + block_size - 1) / block_size;

    std::vector<ZoneMapEntry> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        uint32_t start = b * block_size;
        uint32_t end = std::min(start + block_size, (uint32_t)num_rows);

        int32_t min_val = col.data[start];
        int32_t max_val = col.data[start];

        for (uint32_t i = start + 1; i < end; ++i) {
            if (col.data[i] < min_val) min_val = col.data[i];
            if (col.data[i] > max_val) max_val = col.data[i];
        }

        zones[b] = {min_val, max_val, start, end - start};
    }

    // Write zone map: [uint32_t num_entries] then [ZoneMapEntry entries...]
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_entries = num_blocks;
    out.write((char*)&num_entries, sizeof(uint32_t));
    out.write((char*)zones.data(), zones.size() * sizeof(ZoneMapEntry));

    std::cout << "  Wrote " << num_blocks << " zone map entries (16 bytes each)\n";
}

// ============================================================================
// Build Zone Map (int64_t columns)
// ============================================================================

void build_zone_map_int64(const std::string& input_path, const std::string& output_path, uint32_t block_size) {
    std::cout << "Building zone map: " << output_path << "\n";

    MappedColumn<int64_t> col(input_path);
    if (!col.data) return;

    size_t num_rows = col.size;
    size_t num_blocks = (num_rows + block_size - 1) / block_size;

    std::vector<ZoneMapEntry64> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        uint32_t start = b * block_size;
        uint32_t end = std::min(start + block_size, (uint32_t)num_rows);

        int64_t min_val = col.data[start];
        int64_t max_val = col.data[start];

        for (uint32_t i = start + 1; i < end; ++i) {
            if (col.data[i] < min_val) min_val = col.data[i];
            if (col.data[i] > max_val) max_val = col.data[i];
        }

        zones[b] = {min_val, max_val, start, end - start};
    }

    // Write zone map: [uint32_t num_entries] then [ZoneMapEntry64 entries...]
    std::ofstream out(output_path, std::ios::binary);
    uint32_t num_entries = num_blocks;
    out.write((char*)&num_entries, sizeof(uint32_t));
    out.write((char*)zones.data(), zones.size() * sizeof(ZoneMapEntry64));

    std::cout << "  Wrote " << num_blocks << " zone map entries (24 bytes each)\n";
}

// ============================================================================
// Build Hash Index - Single Value (for unique keys)
// ============================================================================

void build_hash_single(const std::string& input_path, const std::string& output_path) {
    std::cout << "Building hash index (single-value): " << output_path << "\n";

    MappedColumn<int32_t> col(input_path);
    if (!col.data) return;

    size_t num_rows = col.size;

    // Determine unique keys and table size
    std::unordered_map<int32_t, uint32_t> key_to_pos;
    for (size_t i = 0; i < num_rows; ++i) {
        key_to_pos[col.data[i]] = i;
    }

    size_t num_unique = key_to_pos.size();
    size_t table_size = 1;
    while (table_size < num_unique * 2) table_size *= 2;  // load factor ~0.5
    uint32_t shift = 64 - __builtin_ctzll(table_size);

    std::vector<HashEntry> hash_table(table_size, {-1, 0, 0});

    // Insert keys
    for (const auto& kv : key_to_pos) {
        int32_t key = kv.first;
        uint32_t pos = kv.second;
        uint32_t slot = hash_int32(key, shift) & (table_size - 1);

        while (hash_table[slot].key != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot] = {key, pos, 1};
    }

    // Write hash index: [uint32_t num_unique][uint32_t table_size] then [HashEntry entries...]
    std::ofstream out(output_path, std::ios::binary);
    uint32_t nu = num_unique;
    uint32_t ts = table_size;
    out.write((char*)&nu, sizeof(uint32_t));
    out.write((char*)&ts, sizeof(uint32_t));
    out.write((char*)hash_table.data(), hash_table.size() * sizeof(HashEntry));

    std::cout << "  Wrote hash index: " << num_unique << " unique keys, table size " << table_size << " (12 bytes/entry)\n";
}

// ============================================================================
// Build Hash Index - Multi-Value (for join columns with duplicates)
// ============================================================================

void build_hash_multi(const std::string& input_path, const std::string& output_path) {
    std::cout << "Building hash index (multi-value): " << output_path << "\n";

    MappedColumn<int32_t> col(input_path);
    if (!col.data) return;

    size_t num_rows = col.size;

    // Step 1: Group positions by key (parallel histogram + scatter)
    std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;

    for (size_t i = 0; i < num_rows; ++i) {
        key_to_positions[col.data[i]].push_back(i);
    }

    size_t num_unique = key_to_positions.size();
    std::cout << "  " << num_unique << " unique keys, " << num_rows << " total rows\n";

    // Step 2: Build positions array (all positions, grouped by key)
    std::vector<uint32_t> positions_array;
    positions_array.reserve(num_rows);

    std::unordered_map<int32_t, uint32_t> key_to_offset;

    for (const auto& kv : key_to_positions) {
        int32_t key = kv.first;
        const auto& positions = kv.second;

        key_to_offset[key] = positions_array.size();
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());
    }

    // Step 3: Build hash table on unique keys
    size_t table_size = 1;
    while (table_size < num_unique * 2) table_size *= 2;  // load factor ~0.5
    uint32_t shift = 64 - __builtin_ctzll(table_size);

    std::vector<HashEntry> hash_table(table_size, {-1, 0, 0});

    for (const auto& kv : key_to_positions) {
        int32_t key = kv.first;
        uint32_t offset = key_to_offset[key];
        uint32_t count = kv.second.size();

        uint32_t slot = hash_int32(key, shift) & (table_size - 1);

        while (hash_table[slot].key != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot] = {key, offset, count};
    }

    // Write hash index: [uint32_t num_unique][uint32_t table_size][HashEntry table...][uint32_t pos_count][uint32_t positions...]
    std::ofstream out(output_path, std::ios::binary);
    uint32_t nu = num_unique;
    uint32_t ts = table_size;
    uint32_t pc = positions_array.size();

    out.write((char*)&nu, sizeof(uint32_t));
    out.write((char*)&ts, sizeof(uint32_t));
    out.write((char*)hash_table.data(), hash_table.size() * sizeof(HashEntry));
    out.write((char*)&pc, sizeof(uint32_t));
    out.write((char*)positions_array.data(), positions_array.size() * sizeof(uint32_t));

    std::cout << "  Wrote hash index: " << num_unique << " unique keys, table size " << table_size
              << ", " << positions_array.size() << " positions (12 bytes/entry + positions array)\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string base_dir = argv[1];
    std::string indexes_dir = base_dir + "/indexes";
    mkdir(indexes_dir.c_str(), 0755);

    std::cout << "Building indexes for " << base_dir << "\n";
    std::cout << "Using " << omp_get_max_threads() << " OpenMP threads\n\n";

    // Customer indexes
    build_hash_single(base_dir + "/customer/c_custkey.bin", indexes_dir + "/customer_c_custkey_hash.bin");

    // Orders indexes
    build_hash_single(base_dir + "/orders/o_orderkey.bin", indexes_dir + "/orders_o_orderkey_hash.bin");
    build_hash_multi(base_dir + "/orders/o_custkey.bin", indexes_dir + "/orders_o_custkey_hash.bin");
    build_zone_map_int32(base_dir + "/orders/o_orderdate.bin", indexes_dir + "/orders_o_orderdate_zonemap.bin", 100000);

    // Lineitem indexes
    build_hash_multi(base_dir + "/lineitem/l_orderkey.bin", indexes_dir + "/lineitem_l_orderkey_hash.bin");
    build_zone_map_int32(base_dir + "/lineitem/l_shipdate.bin", indexes_dir + "/lineitem_l_shipdate_zonemap.bin", 100000);
    build_zone_map_int64(base_dir + "/lineitem/l_discount.bin", indexes_dir + "/lineitem_l_discount_zonemap.bin", 100000);
    build_zone_map_int64(base_dir + "/lineitem/l_quantity.bin", indexes_dir + "/lineitem_l_quantity_zonemap.bin", 100000);

    std::cout << "\nIndex building complete!\n";
    return 0;
}
