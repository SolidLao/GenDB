#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Hash function (multiply-shift)
inline uint32_t hash_int32(int32_t key) {
    uint64_t x = static_cast<uint64_t>(key);
    x = x * 0x9E3779B97F4A7C15ULL;
    return static_cast<uint32_t>(x >> 32);
}

// Hash function for composite key (partkey, suppkey)
inline uint32_t hash_composite(int32_t k1, int32_t k2) {
    uint64_t combined = (static_cast<uint64_t>(k1) << 32) | static_cast<uint64_t>(k2);
    combined = combined * 0x9E3779B97F4A7C15ULL;
    return static_cast<uint32_t>(combined >> 32);
}

// Zone map entry
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
};

// Hash table entry for single-value hash (primary keys)
struct HashEntrySingle {
    int32_t key;
    uint32_t position;
};

// Hash table entry for multi-value hash (foreign keys)
struct HashEntryMulti {
    int32_t key;
    uint32_t offset;  // Offset into positions array
    uint32_t count;   // Number of positions
};

// Hash table entry for composite key
struct HashEntryComposite {
    int32_t key1;
    int32_t key2;
    uint32_t offset;
    uint32_t count;
};

// Build zone map for lineitem.l_shipdate (already sorted)
void build_lineitem_shipdate_zone(const std::string& base_dir) {
    std::cout << "Building lineitem_shipdate_zone..." << std::endl;

    std::string col_file = base_dir + "/lineitem/l_shipdate.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* data = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    const size_t BLOCK_SIZE = 100000;
    size_t num_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<ZoneMapEntry> zones(num_blocks);

    #pragma omp parallel for
    for (size_t block = 0; block < num_blocks; block++) {
        size_t start = block * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, num_rows);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zones[block].min_value = min_val;
        zones[block].max_value = max_val;
    }

    munmap(data, sb.st_size);
    close(fd);

    // Write zone map
    std::string idx_file = base_dir + "/indexes/lineitem_shipdate_zone.bin";
    std::ofstream out(idx_file, std::ios::binary);
    uint32_t num_entries = zones.size();
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    out.write(reinterpret_cast<const char*>(zones.data()), zones.size() * sizeof(ZoneMapEntry));
    out.close();

    std::cout << "Built lineitem_shipdate_zone: " << num_blocks << " zones" << std::endl;
}

// Build zone map for orders.o_orderdate (already sorted)
void build_orders_orderdate_zone(const std::string& base_dir) {
    std::cout << "Building orders_orderdate_zone..." << std::endl;

    std::string col_file = base_dir + "/orders/o_orderdate.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* data = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    const size_t BLOCK_SIZE = 100000;
    size_t num_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<ZoneMapEntry> zones(num_blocks);

    #pragma omp parallel for
    for (size_t block = 0; block < num_blocks; block++) {
        size_t start = block * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, num_rows);

        int32_t min_val = data[start];
        int32_t max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zones[block].min_value = min_val;
        zones[block].max_value = max_val;
    }

    munmap(data, sb.st_size);
    close(fd);

    std::string idx_file = base_dir + "/indexes/orders_orderdate_zone.bin";
    std::ofstream out(idx_file, std::ios::binary);
    uint32_t num_entries = zones.size();
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    out.write(reinterpret_cast<const char*>(zones.data()), zones.size() * sizeof(ZoneMapEntry));
    out.close();

    std::cout << "Built orders_orderdate_zone: " << num_blocks << " zones" << std::endl;
}

// Build multi-value hash index for lineitem.l_orderkey
void build_lineitem_orderkey_hash(const std::string& base_dir) {
    std::cout << "Building lineitem_orderkey_hash..." << std::endl;

    std::string col_file = base_dir + "/lineitem/l_orderkey.bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* keys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (keys == MAP_FAILED) {
        std::cerr << "Failed to mmap " << col_file << std::endl;
        close(fd);
        return;
    }

    // Count occurrences per key
    std::unordered_map<int32_t, uint32_t> key_counts;
    for (size_t i = 0; i < num_rows; i++) {
        key_counts[keys[i]]++;
    }

    size_t num_unique = key_counts.size();
    std::cout << "  Unique keys: " << num_unique << std::endl;

    // Allocate positions array and hash table
    std::vector<uint32_t> positions(num_rows);
    std::vector<int32_t> unique_keys;
    unique_keys.reserve(num_unique);
    for (const auto& kv : key_counts) {
        unique_keys.push_back(kv.first);
    }

    // Build offsets
    std::unordered_map<int32_t, uint32_t> key_offsets;
    uint32_t offset = 0;
    for (int32_t key : unique_keys) {
        key_offsets[key] = offset;
        offset += key_counts[key];
    }

    // Scatter positions
    std::unordered_map<int32_t, uint32_t> current_offsets = key_offsets;
    for (size_t i = 0; i < num_rows; i++) {
        int32_t key = keys[i];
        positions[current_offsets[key]++] = i;
    }

    munmap(keys, sb.st_size);
    close(fd);

    // Build hash table (open addressing with linear probing)
    size_t table_size = num_unique * 2;  // Load factor 0.5
    table_size = 1 << static_cast<int>(__builtin_clzll(table_size - 1) ^ 63);  // Round to power of 2

    std::vector<HashEntryMulti> hash_table(table_size, {-1, 0, 0});

    for (int32_t key : unique_keys) {
        uint32_t hash = hash_int32(key);
        uint32_t slot = hash & (table_size - 1);

        while (hash_table[slot].key != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot].key = key;
        hash_table[slot].offset = key_offsets[key];
        hash_table[slot].count = key_counts[key];
    }

    // Write to disk
    std::string idx_file = base_dir + "/indexes/lineitem_orderkey_hash.bin";
    std::ofstream out(idx_file, std::ios::binary);

    uint32_t num_unique_u32 = num_unique;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_unique_u32), sizeof(num_unique_u32));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));
    out.write(reinterpret_cast<const char*>(hash_table.data()), table_size * sizeof(HashEntryMulti));

    uint32_t pos_count = positions.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions.data()), positions.size() * sizeof(uint32_t));

    out.close();

    std::cout << "Built lineitem_orderkey_hash: " << num_unique << " unique keys, table size " << table_size << std::endl;
}

// Build multi-value hash index for lineitem (l_partkey, l_suppkey)
void build_lineitem_partkey_suppkey_hash(const std::string& base_dir) {
    std::cout << "Building lineitem_partkey_suppkey_hash..." << std::endl;

    std::string partkey_file = base_dir + "/lineitem/l_partkey.bin";
    std::string suppkey_file = base_dir + "/lineitem/l_suppkey.bin";

    int fd1 = open(partkey_file.c_str(), O_RDONLY);
    int fd2 = open(suppkey_file.c_str(), O_RDONLY);
    if (fd1 < 0 || fd2 < 0) {
        std::cerr << "Failed to open lineitem partkey/suppkey files" << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd1, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* partkeys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd1, 0));
    int32_t* suppkeys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd2, 0));

    // Count occurrences per composite key
    std::map<std::pair<int32_t, int32_t>, uint32_t> key_counts;
    for (size_t i = 0; i < num_rows; i++) {
        key_counts[{partkeys[i], suppkeys[i]}]++;
    }

    size_t num_unique = key_counts.size();
    std::cout << "  Unique composite keys: " << num_unique << std::endl;

    // Build positions array
    std::vector<uint32_t> positions(num_rows);
    std::map<std::pair<int32_t, int32_t>, uint32_t> key_offsets;
    uint32_t offset = 0;
    for (const auto& kv : key_counts) {
        key_offsets[kv.first] = offset;
        offset += kv.second;
    }

    std::map<std::pair<int32_t, int32_t>, uint32_t> current_offsets = key_offsets;
    for (size_t i = 0; i < num_rows; i++) {
        std::pair<int32_t, int32_t> key = {partkeys[i], suppkeys[i]};
        positions[current_offsets[key]++] = i;
    }

    munmap(partkeys, sb.st_size);
    munmap(suppkeys, sb.st_size);
    close(fd1);
    close(fd2);

    // Build hash table
    size_t table_size = num_unique * 2;
    table_size = 1 << static_cast<int>(__builtin_clzll(table_size - 1) ^ 63);

    std::vector<HashEntryComposite> hash_table(table_size, {-1, -1, 0, 0});

    for (const auto& kv : key_counts) {
        uint32_t hash = hash_composite(kv.first.first, kv.first.second);
        uint32_t slot = hash & (table_size - 1);

        while (hash_table[slot].key1 != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot].key1 = kv.first.first;
        hash_table[slot].key2 = kv.first.second;
        hash_table[slot].offset = key_offsets[kv.first];
        hash_table[slot].count = kv.second;
    }

    // Write to disk
    std::string idx_file = base_dir + "/indexes/lineitem_partkey_suppkey_hash.bin";
    std::ofstream out(idx_file, std::ios::binary);

    uint32_t num_unique_u32 = num_unique;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_unique_u32), sizeof(num_unique_u32));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));
    out.write(reinterpret_cast<const char*>(hash_table.data()), table_size * sizeof(HashEntryComposite));

    uint32_t pos_count = positions.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions.data()), positions.size() * sizeof(uint32_t));

    out.close();

    std::cout << "Built lineitem_partkey_suppkey_hash: " << num_unique << " unique keys, table size " << table_size << std::endl;
}

// Build single-value hash index (for primary keys)
void build_single_hash(const std::string& base_dir, const std::string& table,
                       const std::string& column, const std::string& idx_name) {
    std::cout << "Building " << idx_name << "..." << std::endl;

    std::string col_file = base_dir + "/" + table + "/" + column + ".bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* keys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (keys == MAP_FAILED) {
        close(fd);
        return;
    }

    // Build hash table
    size_t table_size = num_rows * 2;
    table_size = 1 << static_cast<int>(__builtin_clzll(table_size - 1) ^ 63);

    std::vector<HashEntrySingle> hash_table(table_size, {-1, 0});

    for (size_t i = 0; i < num_rows; i++) {
        uint32_t hash = hash_int32(keys[i]);
        uint32_t slot = hash & (table_size - 1);

        while (hash_table[slot].key != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot].key = keys[i];
        hash_table[slot].position = i;
    }

    munmap(keys, sb.st_size);
    close(fd);

    // Write to disk
    std::string idx_file = base_dir + "/indexes/" + idx_name + ".bin";
    std::ofstream out(idx_file, std::ios::binary);

    uint32_t num_entries = num_rows;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));
    out.write(reinterpret_cast<const char*>(hash_table.data()), table_size * sizeof(HashEntrySingle));

    out.close();

    std::cout << "Built " << idx_name << ": " << num_rows << " keys, table size " << table_size << std::endl;
}

// Build multi-value hash index
void build_multi_hash(const std::string& base_dir, const std::string& table,
                      const std::string& column, const std::string& idx_name) {
    std::cout << "Building " << idx_name << "..." << std::endl;

    std::string col_file = base_dir + "/" + table + "/" + column + ".bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << col_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* keys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (keys == MAP_FAILED) {
        close(fd);
        return;
    }

    // Count occurrences
    std::unordered_map<int32_t, uint32_t> key_counts;
    for (size_t i = 0; i < num_rows; i++) {
        key_counts[keys[i]]++;
    }

    size_t num_unique = key_counts.size();

    // Build positions array
    std::vector<uint32_t> positions(num_rows);
    std::vector<int32_t> unique_keys;
    unique_keys.reserve(num_unique);
    for (const auto& kv : key_counts) {
        unique_keys.push_back(kv.first);
    }

    std::unordered_map<int32_t, uint32_t> key_offsets;
    uint32_t offset = 0;
    for (int32_t key : unique_keys) {
        key_offsets[key] = offset;
        offset += key_counts[key];
    }

    std::unordered_map<int32_t, uint32_t> current_offsets = key_offsets;
    for (size_t i = 0; i < num_rows; i++) {
        int32_t key = keys[i];
        positions[current_offsets[key]++] = i;
    }

    munmap(keys, sb.st_size);
    close(fd);

    // Build hash table
    size_t table_size = num_unique * 2;
    table_size = 1 << static_cast<int>(__builtin_clzll(table_size - 1) ^ 63);

    std::vector<HashEntryMulti> hash_table(table_size, {-1, 0, 0});

    for (int32_t key : unique_keys) {
        uint32_t hash = hash_int32(key);
        uint32_t slot = hash & (table_size - 1);

        while (hash_table[slot].key != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot].key = key;
        hash_table[slot].offset = key_offsets[key];
        hash_table[slot].count = key_counts[key];
    }

    // Write to disk
    std::string idx_file = base_dir + "/indexes/" + idx_name + ".bin";
    std::ofstream out(idx_file, std::ios::binary);

    uint32_t num_unique_u32 = num_unique;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_unique_u32), sizeof(num_unique_u32));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));
    out.write(reinterpret_cast<const char*>(hash_table.data()), table_size * sizeof(HashEntryMulti));

    uint32_t pos_count = positions.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions.data()), positions.size() * sizeof(uint32_t));

    out.close();

    std::cout << "Built " << idx_name << ": " << num_unique << " unique keys, table size " << table_size << std::endl;
}

// Build composite single-value hash (for partsupp primary key)
void build_partsupp_hash(const std::string& base_dir) {
    std::cout << "Building partsupp_partkey_suppkey_hash..." << std::endl;

    std::string partkey_file = base_dir + "/partsupp/ps_partkey.bin";
    std::string suppkey_file = base_dir + "/partsupp/ps_suppkey.bin";

    int fd1 = open(partkey_file.c_str(), O_RDONLY);
    int fd2 = open(suppkey_file.c_str(), O_RDONLY);
    if (fd1 < 0 || fd2 < 0) {
        std::cerr << "Failed to open partsupp files" << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd1, &sb);
    size_t num_rows = sb.st_size / sizeof(int32_t);

    int32_t* partkeys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd1, 0));
    int32_t* suppkeys = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd2, 0));

    // Build hash table (single-value, since it's a primary key)
    size_t table_size = num_rows * 2;
    table_size = 1 << static_cast<int>(__builtin_clzll(table_size - 1) ^ 63);

    std::vector<HashEntryComposite> hash_table(table_size, {-1, -1, 0, 0});

    for (size_t i = 0; i < num_rows; i++) {
        uint32_t hash = hash_composite(partkeys[i], suppkeys[i]);
        uint32_t slot = hash & (table_size - 1);

        while (hash_table[slot].key1 != -1) {
            slot = (slot + 1) & (table_size - 1);
        }

        hash_table[slot].key1 = partkeys[i];
        hash_table[slot].key2 = suppkeys[i];
        hash_table[slot].offset = i;
        hash_table[slot].count = 1;  // Single-value
    }

    munmap(partkeys, sb.st_size);
    munmap(suppkeys, sb.st_size);
    close(fd1);
    close(fd2);

    // Write to disk
    std::string idx_file = base_dir + "/indexes/partsupp_partkey_suppkey_hash.bin";
    std::ofstream out(idx_file, std::ios::binary);

    uint32_t num_entries = num_rows;
    uint32_t table_size_u32 = table_size;
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    out.write(reinterpret_cast<const char*>(&table_size_u32), sizeof(table_size_u32));
    out.write(reinterpret_cast<const char*>(hash_table.data()), table_size * sizeof(HashEntryComposite));

    out.close();

    std::cout << "Built partsupp_partkey_suppkey_hash: " << num_rows << " keys, table size " << table_size << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <base_dir>" << std::endl;
        return 1;
    }

    std::string base_dir = argv[1];

    std::cout << "Building indexes for GenDB storage at: " << base_dir << std::endl;

    // Zone maps
    build_lineitem_shipdate_zone(base_dir);
    build_orders_orderdate_zone(base_dir);

    // Multi-value hash indexes
    build_lineitem_orderkey_hash(base_dir);
    build_lineitem_partkey_suppkey_hash(base_dir);
    build_multi_hash(base_dir, "orders", "o_custkey", "orders_custkey_hash");

    // Single-value hash indexes
    build_single_hash(base_dir, "orders", "o_orderkey", "orders_orderkey_hash");
    build_single_hash(base_dir, "customer", "c_custkey", "customer_custkey_hash");
    build_single_hash(base_dir, "part", "p_partkey", "part_partkey_hash");
    build_single_hash(base_dir, "supplier", "s_suppkey", "supplier_suppkey_hash");

    // Composite key hash
    build_partsupp_hash(base_dir);

    std::cout << "\n=== Index Building Complete ===" << std::endl;
    return 0;
}
