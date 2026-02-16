#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <chrono>

// ============================================================================
// Zone Map Index Structure
// ============================================================================

struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    uint32_t row_count;
};

// ============================================================================
// Hash Multi-Value Index Structure (for join columns)
// ============================================================================

struct HashMultiValueEntry {
    int32_t key;
    uint32_t offset;      // offset into positions array
    uint32_t count;       // number of positions for this key
};

// ============================================================================
// Utility: Memory Map File
// ============================================================================

template <typename T>
class MmappedFile {
public:
    MmappedFile(const std::string& path) : fd_(-1), data_(nullptr), size_(0) {
        fd_ = open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            std::cerr << "ERROR: Cannot open " << path << "\n";
            return;
        }

        size_ = lseek(fd_, 0, SEEK_END);
        if (size_ <= 0) {
            std::cerr << "ERROR: File is empty: " << path << "\n";
            close(fd_);
            fd_ = -1;
            return;
        }

        data_ = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (data_ == MAP_FAILED) {
            std::cerr << "ERROR: mmap failed for " << path << "\n";
            close(fd_);
            fd_ = -1;
            data_ = nullptr;
            size_ = 0;
            return;
        }

        madvise(data_, size_, MADV_SEQUENTIAL);
    }

    ~MmappedFile() {
        if (data_ && fd_ >= 0) {
            munmap(data_, size_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    const T* data() const {
        return static_cast<const T*>(data_);
    }

    size_t count() const {
        return size_ / sizeof(T);
    }

    bool valid() const {
        return fd_ >= 0 && data_ != nullptr;
    }

private:
    int fd_;
    void* data_;
    size_t size_;
};

// ============================================================================
// Zone Map Builder (for int32_t columns like l_shipdate, o_orderdate)
// ============================================================================

void build_zone_map_index(const std::string& input_bin, const std::string& output_file, uint32_t block_size) {
    std::cout << "Building zone map for " << input_bin << " (block_size=" << block_size << ")...\n";
    auto start = std::chrono::high_resolution_clock::now();

    MmappedFile<int32_t> column(input_bin);
    if (!column.valid()) {
        std::cerr << "ERROR: Cannot mmap column file\n";
        return;
    }

    const int32_t* data = column.data();
    size_t total_rows = column.count();
    size_t num_blocks = (total_rows + block_size - 1) / block_size;

    std::vector<ZoneMapEntry> zone_maps;
    zone_maps.reserve(num_blocks);

#pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start_row = b * block_size;
        size_t end_row = std::min((b + 1) * block_size, total_rows);
        size_t block_rows = end_row - start_row;

        int32_t block_min = data[start_row];
        int32_t block_max = data[start_row];

        for (size_t i = start_row; i < end_row; ++i) {
            if (data[i] < block_min) block_min = data[i];
            if (data[i] > block_max) block_max = data[i];
        }

        ZoneMapEntry entry;
        entry.min_value = block_min;
        entry.max_value = block_max;
        entry.row_count = block_rows;

#pragma omp critical
        {
            zone_maps.push_back(entry);
        }
    }

    std::sort(zone_maps.begin(), zone_maps.end(), [](const ZoneMapEntry& a, const ZoneMapEntry& b) {
        return a.min_value < b.min_value;
    });

    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_zones = zone_maps.size();
    out.write(reinterpret_cast<const char*>(&num_zones), sizeof(num_zones));
    out.write(reinterpret_cast<const char*>(zone_maps.data()), zone_maps.size() * sizeof(ZoneMapEntry));

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "  Zone map built: " << num_zones << " zones in " << elapsed.count() << "ms\n";
}

// ============================================================================
// Hash Multi-Value Index Builder
// ============================================================================

void build_hash_multi_value_index(const std::string& input_bin, const std::string& output_file) {
    std::cout << "Building hash multi-value index for " << input_bin << "...\n";
    auto start = std::chrono::high_resolution_clock::now();

    MmappedFile<int32_t> column(input_bin);
    if (!column.valid()) {
        std::cerr << "ERROR: Cannot mmap column file\n";
        return;
    }

    const int32_t* data = column.data();
    size_t total_rows = column.count();

    // Step 1: Group positions by key using parallel histogram + scatter
    std::unordered_map<int32_t, std::vector<uint32_t>> key_positions;

#pragma omp parallel
    {
        std::unordered_map<int32_t, std::vector<uint32_t>> local_key_positions;

#pragma omp for nowait
        for (uint32_t i = 0; i < total_rows; ++i) {
            local_key_positions[data[i]].push_back(i);
        }

#pragma omp critical
        {
            for (auto& [key, positions] : local_key_positions) {
                key_positions[key].insert(key_positions[key].end(), positions.begin(), positions.end());
            }
        }
    }

    // Step 2: Build hash table and positions array
    uint32_t num_unique = key_positions.size();
    uint32_t table_size = num_unique * 2;  // Load factor 0.5

    std::vector<HashMultiValueEntry> hash_table(table_size, {-1, 0, 0});
    std::vector<uint32_t> positions_array;

    for (auto& [key, positions] : key_positions) {
        uint32_t offset = positions_array.size();
        positions_array.insert(positions_array.end(), positions.begin(), positions.end());

        // Linear probing
        uint32_t hash = ((uint64_t)key * 0x9E3779B97F4A7C15ULL) % table_size;
        while (hash_table[hash].key != -1) {
            hash = (hash + 1) % table_size;
        }
        hash_table[hash] = {key, offset, (uint32_t)positions.size()};
    }

    // Step 3: Write to file
    std::ofstream out(output_file, std::ios::binary);
    out.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));
    out.write(reinterpret_cast<const char*>(&table_size), sizeof(table_size));
    out.write(reinterpret_cast<const char*>(hash_table.data()), hash_table.size() * sizeof(HashMultiValueEntry));
    uint32_t pos_count = positions_array.size();
    out.write(reinterpret_cast<const char*>(&pos_count), sizeof(pos_count));
    out.write(reinterpret_cast<const char*>(positions_array.data()), positions_array.size() * sizeof(uint32_t));

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "  Hash multi-value index built: " << num_unique << " unique keys, "
              << total_rows << " total positions in " << elapsed.count() << "ms\n";
}

// ============================================================================
// Main: Build All Indexes
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string indexes_dir = gendb_dir + "/indexes";

    std::cout << "=== Building Indexes for TPC-H SF10 ===\n";
    auto start_total = std::chrono::high_resolution_clock::now();

    // ---- LINEITEM INDEXES ----

    // Zone map on l_shipdate (200K block size per storage_design.json)
    build_zone_map_index(gendb_dir + "/lineitem/l_shipdate.bin",
                         indexes_dir + "/lineitem_l_shipdate_zone.bin", 200000);

    // Hash multi-value indexes on join columns
    build_hash_multi_value_index(gendb_dir + "/lineitem/l_orderkey.bin",
                                 indexes_dir + "/lineitem_l_orderkey_hash.bin");
    build_hash_multi_value_index(gendb_dir + "/lineitem/l_suppkey.bin",
                                 indexes_dir + "/lineitem_l_suppkey_hash.bin");
    build_hash_multi_value_index(gendb_dir + "/lineitem/l_partkey.bin",
                                 indexes_dir + "/lineitem_l_partkey_hash.bin");

    // ---- ORDERS INDEXES ----

    // Zone map on o_orderdate (150K block size per storage_design.json)
    build_zone_map_index(gendb_dir + "/orders/o_orderdate.bin",
                         indexes_dir + "/orders_o_orderdate_zone.bin", 150000);

    // Hash multi-value index on o_custkey
    build_hash_multi_value_index(gendb_dir + "/orders/o_custkey.bin",
                                 indexes_dir + "/orders_o_custkey_hash.bin");

    // ---- CUSTOMER INDEXES ----

    // Hash multi-value index on c_nationkey
    build_hash_multi_value_index(gendb_dir + "/customer/c_nationkey.bin",
                                 indexes_dir + "/customer_c_nationkey_hash.bin");

    // ---- PART INDEXES ----

    // Hash multi-value index on p_brand and p_type (compound hash)
    // For now, we'll just build on p_brand for prototype
    build_hash_multi_value_index(gendb_dir + "/part/p_brand.bin",
                                 indexes_dir + "/part_p_brand_hash.bin");

    // ---- PARTSUPP INDEXES ----

    // Hash multi-value indexes on join columns
    build_hash_multi_value_index(gendb_dir + "/partsupp/ps_suppkey.bin",
                                 indexes_dir + "/partsupp_ps_suppkey_hash.bin");
    build_hash_multi_value_index(gendb_dir + "/partsupp/ps_partkey.bin",
                                 indexes_dir + "/partsupp_ps_partkey_hash.bin");

    // ---- SUPPLIER INDEXES ----

    // Hash multi-value index on s_nationkey
    build_hash_multi_value_index(gendb_dir + "/supplier/s_nationkey.bin",
                                 indexes_dir + "/supplier_s_nationkey_hash.bin");

    // ---- NATION INDEXES ----

    // Hash multi-value index on n_regionkey
    build_hash_multi_value_index(gendb_dir + "/nation/n_regionkey.bin",
                                 indexes_dir + "/nation_n_regionkey_hash.bin");

    auto end_total = std::chrono::high_resolution_clock::now();
    auto elapsed_total = std::chrono::duration_cast<std::chrono::seconds>(end_total - start_total);

    std::cout << "\n=== Index Building Complete: " << elapsed_total.count() << "s ===\n";
    return 0;
}
