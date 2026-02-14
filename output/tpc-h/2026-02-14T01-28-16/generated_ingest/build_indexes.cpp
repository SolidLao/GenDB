#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// Read binary column file
template<typename T>
std::vector<T> read_column_binary(const std::string& filepath) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << "\n";
        return {};
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t count = file_size / sizeof(T);

    T* mapped = static_cast<T*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << "\n";
        close(fd);
        return {};
    }

    std::vector<T> data(mapped, mapped + count);

    munmap(mapped, file_size);
    close(fd);

    return data;
}

// Write binary index file
template<typename T>
void write_index_binary(const std::string& filepath, const std::vector<T>& data) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open " << filepath << " for writing\n";
        return;
    }

    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
}

// Build zone map index (min/max per block)
template<typename T>
void build_zone_map(const std::string& gendb_dir, const std::string& table, const std::string& column, size_t block_size) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string col_path = gendb_dir + "/" + table + "_" + column + ".bin";
    std::vector<T> data = read_column_binary<T>(col_path);

    if (data.empty()) {
        std::cerr << "Failed to load " << col_path << "\n";
        return;
    }

    std::cout << "Building zone map for " << table << "." << column << " (" << data.size() << " rows, block_size=" << block_size << ")...\n";

    size_t num_blocks = (data.size() + block_size - 1) / block_size;

    struct ZoneMapEntry {
        T min_val;
        T max_val;
        size_t start_row;
        size_t end_row;
    };

    std::vector<ZoneMapEntry> zone_map;
    zone_map.reserve(num_blocks);

    for (size_t block_id = 0; block_id < num_blocks; block_id++) {
        size_t start = block_id * block_size;
        size_t end = std::min(start + block_size, data.size());

        T min_val = data[start];
        T max_val = data[start];

        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        zone_map.push_back({min_val, max_val, start, end});
    }

    std::string idx_path = gendb_dir + "/" + table + "_" + column + "_zonemap.idx";
    std::ofstream out(idx_path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(zone_map.data()), zone_map.size() * sizeof(ZoneMapEntry));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start).count();

    std::cout << "  Zone map created: " << num_blocks << " blocks, min=" << zone_map.front().min_val
              << ", max=" << zone_map.back().max_val << " (" << duration << "ms)\n";
}

// Build hash index (value -> row_ids)
template<typename T>
void build_hash_index(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string col_path = gendb_dir + "/" + table + "_" + column + ".bin";
    std::vector<T> data = read_column_binary<T>(col_path);

    if (data.empty()) {
        std::cerr << "Failed to load " << col_path << "\n";
        return;
    }

    std::cout << "Building hash index for " << table << "." << column << " (" << data.size() << " rows)...\n";

    // Build hash map: value -> list of row_ids
    std::unordered_map<T, std::vector<size_t>> hash_map;

    for (size_t i = 0; i < data.size(); i++) {
        hash_map[data[i]].push_back(i);
    }

    std::cout << "  Hash index created: " << hash_map.size() << " unique values\n";

    // Write hash index to file
    // Format: [num_entries] [key1][count1][row_id1][row_id2]... [key2][count2]...
    std::string idx_path = gendb_dir + "/" + table + "_" + column + "_hash.idx";
    std::ofstream out(idx_path, std::ios::binary);

    size_t num_entries = hash_map.size();
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(size_t));

    for (const auto& [key, row_ids] : hash_map) {
        out.write(reinterpret_cast<const char*>(&key), sizeof(T));
        size_t count = row_ids.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
        out.write(reinterpret_cast<const char*>(row_ids.data()), count * sizeof(size_t));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start).count();

    std::cout << "  Hash index written to " << idx_path << " (" << duration << "ms)\n";
}

// Build sorted index (row_id permutation sorted by column value)
template<typename T>
void build_sorted_index(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string col_path = gendb_dir + "/" + table + "_" + column + ".bin";
    std::vector<T> data = read_column_binary<T>(col_path);

    if (data.empty()) {
        std::cerr << "Failed to load " << col_path << "\n";
        return;
    }

    std::cout << "Building sorted index for " << table << "." << column << " (" << data.size() << " rows)...\n";

    // Create permutation array
    std::vector<size_t> perm(data.size());
    for (size_t i = 0; i < perm.size(); i++) perm[i] = i;

    // Sort by value
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return data[a] < data[b];
    });

    std::string idx_path = gendb_dir + "/" + table + "_" + column + "_sorted.idx";
    write_index_binary(idx_path, perm);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start).count();

    std::cout << "  Sorted index created (" << duration << "ms)\n";
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "=== GenDB Index Building ===\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n\n";

    auto total_start = std::chrono::high_resolution_clock::now();

    // Build indexes according to storage_design.json

    // lineitem indexes
    std::cout << "Building lineitem indexes...\n";
    build_zone_map<int32_t>(gendb_dir, "lineitem", "l_shipdate", 100000);
    build_hash_index<int32_t>(gendb_dir, "lineitem", "l_orderkey");

    // orders indexes
    std::cout << "\nBuilding orders indexes...\n";
    build_zone_map<int32_t>(gendb_dir, "orders", "o_orderdate", 100000);
    build_sorted_index<int32_t>(gendb_dir, "orders", "o_orderkey");
    build_hash_index<int32_t>(gendb_dir, "orders", "o_custkey");

    // customer indexes
    std::cout << "\nBuilding customer indexes...\n";
    build_sorted_index<int32_t>(gendb_dir, "customer", "c_custkey");
    build_hash_index<uint8_t>(gendb_dir, "customer", "c_mktsegment");

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(total_end - total_start).count();

    std::cout << "\n=== Index Building Complete ===\n";
    std::cout << "Total time: " << total_duration << "s\n";

    return 0;
}
