#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <map>
#include <omp.h>

// Multiply-shift hash function for int32_t
inline uint64_t hash_int32(int32_t key) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
}

// Load binary column file
template <typename T>
std::pair<char*, size_t> load_column(const std::string& filename, size_t& row_count) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << filename << "\n";
        return {nullptr, 0};
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    row_count = file_size / sizeof(T);

    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << filename << "\n";
        close(fd);
        return {nullptr, 0};
    }

    madvise(data, file_size, MADV_SEQUENTIAL);
    close(fd);

    return {data, file_size};
}

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    uint32_t block_idx;
};

// Build zone map for int32_t column
void build_zone_map(const std::string& col_filename, const std::string& index_filename, uint32_t block_size) {
    std::cout << "Building zone map for " << col_filename << "...\n";

    size_t row_count = 0;
    auto [data, file_size] = load_column<int32_t>(col_filename, row_count);
    if (data == nullptr) return;

    int32_t* col = (int32_t*)data;

    std::vector<ZoneMap> zones;
    uint32_t block_idx = 0;
    size_t pos = 0;

    while (pos < row_count) {
        size_t block_end = std::min(pos + block_size, row_count);
        int32_t min_val = col[pos];
        int32_t max_val = col[pos];

        for (size_t i = pos + 1; i < block_end; ++i) {
            min_val = std::min(min_val, col[i]);
            max_val = std::max(max_val, col[i]);
        }

        zones.push_back({min_val, max_val, block_idx});

        pos = block_end;
        block_idx++;
    }

    // Write zones to file: [num_blocks] [block_0] [block_1] ...
    std::ofstream out(index_filename, std::ios::binary);
    uint32_t num_blocks = zones.size();
    out.write((char*)&num_blocks, sizeof(uint32_t));

    for (const auto& z : zones) {
        out.write((char*)&z.min_val, sizeof(int32_t));
        out.write((char*)&z.max_val, sizeof(int32_t));
        uint32_t bs = block_size;
        out.write((char*)&bs, sizeof(uint32_t));
    }

    out.close();

    std::cout << "Zone map: " << zones.size() << " blocks\n";

    munmap(data, file_size);
}

// Hash index entry (for multi-value hash index)
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;   // Offset into positions array
    uint32_t count;    // Number of positions for this key
};

// Build hash index for int32_t column
void build_hash_index(const std::string& col_filename, const std::string& index_filename, uint32_t min_threshold = 10000) {
    std::cout << "Building hash index for " << col_filename << "...\n";

    size_t row_count = 0;
    auto [data, file_size] = load_column<int32_t>(col_filename, row_count);
    if (data == nullptr) return;

    // Skip small tables
    if (row_count < min_threshold) {
        std::cout << "  Skipping: table too small (" << row_count << " rows < " << min_threshold << ")\n";
        munmap(data, file_size);
        return;
    }

    int32_t* col = (int32_t*)data;

    // Create position array [0, 1, 2, ..., N-1]
    std::vector<uint32_t> positions(row_count);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < row_count; ++i) {
        positions[i] = i;
    }

    // Sort positions by column value
    std::cout << "  Sorting positions by key...\n";
    std::sort(positions.begin(), positions.end(), [&col](uint32_t a, uint32_t b) {
        return col[a] < col[b];
    });

    // Find group boundaries: scan sorted positions, identify where key changes
    std::vector<HashIndexEntry> entries;
    uint32_t current_key = col[positions[0]];
    uint32_t offset = 0;
    uint32_t count = 1;

    for (uint32_t i = 1; i < row_count; ++i) {
        uint32_t key = col[positions[i]];
        if (key != current_key) {
            entries.push_back({(int32_t)current_key, offset, count});
            current_key = key;
            offset = i;
            count = 1;
        } else {
            count++;
        }
    }
    entries.push_back({(int32_t)current_key, offset, count});

    std::cout << "  Found " << entries.size() << " unique keys\n";

    // Build hash table (open addressing with multiply-shift hash)
    uint32_t capacity = 1;
    while (capacity < entries.size() * 2) capacity *= 2;  // Load factor ~0.5

    std::vector<int32_t> hash_keys(capacity, INT32_MIN);  // Sentinel value
    std::vector<uint32_t> hash_offsets(capacity);
    std::vector<uint32_t> hash_counts(capacity);

    std::cout << "  Building hash table (capacity=" << capacity << ")...\n";

    for (const auto& entry : entries) {
        uint64_t h = hash_int32(entry.key) & (capacity - 1);
        while (hash_keys[h] != INT32_MIN) {
            h = (h + 1) & (capacity - 1);
        }
        hash_keys[h] = entry.key;
        hash_offsets[h] = entry.offset;
        hash_counts[h] = entry.count;
    }

    // Write index: [capacity] [num_unique] [positions...] [hash_table]
    std::ofstream out(index_filename, std::ios::binary);
    out.write((char*)&capacity, sizeof(uint32_t));
    uint32_t num_entries = entries.size();
    out.write((char*)&num_entries, sizeof(uint32_t));

    // Write positions array
    out.write((char*)positions.data(), positions.size() * sizeof(uint32_t));

    // Write hash table
    out.write((char*)hash_keys.data(), hash_keys.size() * sizeof(int32_t));
    out.write((char*)hash_offsets.data(), hash_offsets.size() * sizeof(uint32_t));
    out.write((char*)hash_counts.data(), hash_counts.size() * sizeof(uint32_t));

    out.close();

    std::cout << "Hash index built: " << entries.size() << " unique keys, capacity=" << capacity << "\n";

    munmap(data, file_size);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    // Set OpenMP thread count
    omp_set_num_threads(64);

    std::cout << "Building indexes in " << gendb_dir << "...\n\n";

    // Zone maps for all filter columns
    const uint32_t BLOCK_SIZE = 100000;

    // Lineitem zone maps
    build_zone_map(gendb_dir + "/lineitem/l_shipdate.bin", gendb_dir + "/lineitem/l_shipdate_zone.idx", BLOCK_SIZE);

    // Orders zone maps
    build_zone_map(gendb_dir + "/orders/o_orderdate.bin", gendb_dir + "/orders/o_orderdate_zone.idx", BLOCK_SIZE);

    std::cout << "\n";

    // Hash indexes on join columns (skip tables < 10K rows)
    build_hash_index(gendb_dir + "/supplier/s_nationkey.bin", gendb_dir + "/supplier/s_nationkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/part/p_partkey.bin", gendb_dir + "/part/p_partkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/partsupp/ps_partkey.bin", gendb_dir + "/partsupp/ps_partkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/partsupp/ps_suppkey.bin", gendb_dir + "/partsupp/ps_suppkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/customer/c_custkey.bin", gendb_dir + "/customer/c_custkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/orders/o_orderkey.bin", gendb_dir + "/orders/o_orderkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/orders/o_custkey.bin", gendb_dir + "/orders/o_custkey_hash.idx", 10000);
    build_hash_index(gendb_dir + "/lineitem/l_orderkey.bin", gendb_dir + "/lineitem/l_orderkey_hash.idx", 10000);

    std::cout << "\nIndex building complete!\n";

    return 0;
}
