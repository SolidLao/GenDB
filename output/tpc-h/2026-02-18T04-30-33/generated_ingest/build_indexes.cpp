#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <algorithm>
#include <cmath>

// Multi-value hash index: [uint32_t num_entries][uint32_t table_size]
// then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12B/slot)
// then [uint32_t pos_count][uint32_t positions...]

struct HashIndexBuilder {
    std::string table_dir;
    std::string col_name;
    std::string index_name;

    // mmap column file
    int32_t* col_data = nullptr;
    size_t col_size = 0;
    int col_fd = -1;

    // Index structures
    std::unordered_map<int32_t, std::vector<uint32_t>> key_to_positions;
    std::vector<int32_t> unique_keys;

    HashIndexBuilder(const std::string& tdir, const std::string& cname, const std::string& iname)
        : table_dir(tdir), col_name(cname), index_name(iname) {}

    ~HashIndexBuilder() {
        if (col_data) munmap(col_data, col_size);
        if (col_fd != -1) close(col_fd);
    }

    bool open_column() {
        std::string col_path = table_dir + "/" + col_name + ".bin";
        col_fd = ::open(col_path.c_str(), O_RDONLY);
        if (col_fd < 0) {
            std::cerr << "Failed to open " << col_path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(col_fd, &sb) < 0) {
            std::cerr << "fstat failed" << std::endl;
            return false;
        }

        col_size = sb.st_size;
        col_data = (int32_t*)mmap(nullptr, col_size, PROT_READ, MAP_SHARED, col_fd, 0);
        if (col_data == MAP_FAILED) {
            std::cerr << "mmap failed" << std::endl;
            return false;
        }

        madvise(col_data, col_size, MADV_SEQUENTIAL);
        return true;
    }

    void build_index() {
        size_t num_rows = col_size / sizeof(int32_t);

        // Step 1: Group positions by key (parallel histogram + scatter)
        #pragma omp parallel for
        for (size_t i = 0; i < num_rows; i++) {
            int32_t key = col_data[i];
            #pragma omp critical
            {
                key_to_positions[key].push_back(i);
            }
        }

        // Collect unique keys
        for (const auto& [key, positions] : key_to_positions) {
            unique_keys.push_back(key);
        }

        std::cout << "  " << index_name << ": " << num_rows << " rows, " << unique_keys.size() << " unique keys" << std::endl;
    }

    void write_index() {
        std::string base_dir = table_dir + "/../indexes";
        mkdir(base_dir.c_str(), 0755);
        std::string index_path = base_dir + "/" + index_name + ".bin";

        std::ofstream out(index_path, std::ios::binary);
        if (!out) {
            std::cerr << "Failed to create " << index_path << std::endl;
            return;
        }

        // Simple hash table (power-of-2 size)
        uint32_t num_unique = unique_keys.size();
        uint32_t table_size = 1;
        while (table_size < num_unique * 2) table_size *= 2;  // Load factor ~0.5

        // Write header
        out.write(reinterpret_cast<const char*>(&num_unique), sizeof(num_unique));
        out.write(reinterpret_cast<const char*>(&table_size), sizeof(table_size));

        // Build hash table using simple modulo probing
        std::vector<uint32_t> slot_index(table_size, 0xFFFFFFFF);  // Sentinel for empty
        std::vector<std::pair<int32_t, std::pair<uint32_t, uint32_t>>> hash_entries;

        for (int32_t key : unique_keys) {
            const auto& positions = key_to_positions[key];
            hash_entries.push_back({key, {0, static_cast<uint32_t>(positions.size())}});
        }

        // Simple hash function: multiply-shift
        uint32_t entry_idx = 0;
        for (auto& [key, offset_count] : hash_entries) {
            uint64_t hash_val = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
            uint32_t slot = (hash_val >> 32) % table_size;

            // Linear probing
            while (slot_index[slot] != 0xFFFFFFFF) {
                slot = (slot + 1) % table_size;
            }
            slot_index[slot] = entry_idx;
            entry_idx++;
        }

        // Write hash table slots [key, offset, count]
        uint32_t positions_offset = 0;
        for (uint32_t slot = 0; slot < table_size; slot++) {
            if (slot_index[slot] != 0xFFFFFFFF) {
                const auto& [key, offset_count] = hash_entries[slot_index[slot]];
                int32_t k = key;
                uint32_t off = positions_offset;
                uint32_t cnt = offset_count.second;

                out.write(reinterpret_cast<const char*>(&k), sizeof(k));
                out.write(reinterpret_cast<const char*>(&off), sizeof(off));
                out.write(reinterpret_cast<const char*>(&cnt), sizeof(cnt));

                positions_offset += cnt;
            } else {
                // Empty slot sentinel
                int32_t empty_key = 0x7FFFFFFF;
                uint32_t zero_off = 0;
                uint32_t zero_cnt = 0;
                out.write(reinterpret_cast<const char*>(&empty_key), sizeof(empty_key));
                out.write(reinterpret_cast<const char*>(&zero_off), sizeof(zero_off));
                out.write(reinterpret_cast<const char*>(&zero_cnt), sizeof(zero_cnt));
            }
        }

        // Write all positions
        for (int32_t key : unique_keys) {
            const auto& positions = key_to_positions[key];
            for (uint32_t pos : positions) {
                out.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
            }
        }

        out.close();
    }
};

struct ZoneMapBuilder {
    std::string table_dir;
    std::string col_name;
    std::string zone_name;
    uint32_t block_size;

    int32_t* col_data = nullptr;
    size_t col_size = 0;
    int col_fd = -1;

    ZoneMapBuilder(const std::string& tdir, const std::string& cname, const std::string& zname, uint32_t bsize)
        : table_dir(tdir), col_name(cname), zone_name(zname), block_size(bsize) {}

    ~ZoneMapBuilder() {
        if (col_data) munmap(col_data, col_size);
        if (col_fd != -1) close(col_fd);
    }

    bool open_column() {
        std::string col_path = table_dir + "/" + col_name + ".bin";
        col_fd = ::open(col_path.c_str(), O_RDONLY);
        if (col_fd < 0) {
            std::cerr << "Failed to open " << col_path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(col_fd, &sb) < 0) {
            return false;
        }

        col_size = sb.st_size;
        col_data = (int32_t*)mmap(nullptr, col_size, PROT_READ, MAP_SHARED, col_fd, 0);
        if (col_data == MAP_FAILED) {
            return false;
        }

        madvise(col_data, col_size, MADV_SEQUENTIAL);
        return true;
    }

    void build_zone_map() {
        size_t num_rows = col_size / sizeof(int32_t);
        uint32_t num_blocks = (num_rows + block_size - 1) / block_size;

        std::cout << "  " << zone_name << ": " << num_rows << " rows, " << num_blocks << " blocks" << std::endl;

        std::string base_dir = table_dir + "/../indexes";
        mkdir(base_dir.c_str(), 0755);
        std::string zone_path = base_dir + "/" + zone_name + ".bin";

        std::ofstream out(zone_path, std::ios::binary);
        if (!out) {
            std::cerr << "Failed to create " << zone_path << std::endl;
            return;
        }

        // Write zone map: [num_blocks] then per block [min, max]
        out.write(reinterpret_cast<const char*>(&num_blocks), sizeof(num_blocks));

        #pragma omp parallel for
        for (uint32_t block = 0; block < num_blocks; block++) {
            size_t start = (size_t)block * block_size;
            size_t end = std::min(start + block_size, num_rows);

            int32_t min_val = col_data[start];
            int32_t max_val = col_data[start];

            for (size_t i = start + 1; i < end; i++) {
                min_val = std::min(min_val, col_data[i]);
                max_val = std::max(max_val, col_data[i]);
            }

            // Write zone entry for this block
            #pragma omp critical
            {
                out.write(reinterpret_cast<const char*>(&min_val), sizeof(min_val));
                out.write(reinterpret_cast<const char*>(&max_val), sizeof(max_val));
            }
        }

        out.close();
    }
};

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: build_indexes <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    // Build indexes for lineitem
    {
        std::string table_dir = gendb_dir + "/lineitem";

        // l_shipdate zone map
        ZoneMapBuilder zm_shipdate(table_dir, "l_shipdate", "lineitem_l_shipdate_zone", 500000);
        if (zm_shipdate.open_column()) {
            zm_shipdate.build_zone_map();
        }

        // l_orderkey hash
        HashIndexBuilder hi_orderkey(table_dir, "l_orderkey", "lineitem_l_orderkey_hash");
        if (hi_orderkey.open_column()) {
            hi_orderkey.build_index();
            hi_orderkey.write_index();
        }

        // l_partkey hash
        HashIndexBuilder hi_partkey(table_dir, "l_partkey", "lineitem_l_partkey_hash");
        if (hi_partkey.open_column()) {
            hi_partkey.build_index();
            hi_partkey.write_index();
        }
    }

    // Build indexes for orders
    {
        std::string table_dir = gendb_dir + "/orders";

        // o_orderkey hash
        HashIndexBuilder hi_orderkey(table_dir, "o_orderkey", "orders_o_orderkey_hash");
        if (hi_orderkey.open_column()) {
            hi_orderkey.build_index();
            hi_orderkey.write_index();
        }

        // o_custkey hash
        HashIndexBuilder hi_custkey(table_dir, "o_custkey", "orders_o_custkey_hash");
        if (hi_custkey.open_column()) {
            hi_custkey.build_index();
            hi_custkey.write_index();
        }

        // o_orderdate zone map
        ZoneMapBuilder zm_orderdate(table_dir, "o_orderdate", "orders_o_orderdate_zone", 300000);
        if (zm_orderdate.open_column()) {
            zm_orderdate.build_zone_map();
        }
    }

    // Build indexes for customer
    {
        std::string table_dir = gendb_dir + "/customer";

        // c_custkey hash
        HashIndexBuilder hi_custkey(table_dir, "c_custkey", "customer_c_custkey_hash");
        if (hi_custkey.open_column()) {
            hi_custkey.build_index();
            hi_custkey.write_index();
        }

        // c_mktsegment hash
        HashIndexBuilder hi_mktseg(table_dir, "c_mktsegment", "customer_c_mktsegment_hash");
        if (hi_mktseg.open_column()) {
            hi_mktseg.build_index();
            hi_mktseg.write_index();
        }
    }

    // Build indexes for part
    {
        std::string table_dir = gendb_dir + "/part";

        // p_partkey hash
        HashIndexBuilder hi_partkey(table_dir, "p_partkey", "part_p_partkey_hash");
        if (hi_partkey.open_column()) {
            hi_partkey.build_index();
            hi_partkey.write_index();
        }
    }

    // Build indexes for partsupp
    {
        std::string table_dir = gendb_dir + "/partsupp";

        // ps_partkey hash
        HashIndexBuilder hi_partkey(table_dir, "ps_partkey", "partsupp_ps_partkey_hash");
        if (hi_partkey.open_column()) {
            hi_partkey.build_index();
            hi_partkey.write_index();
        }

        // ps_suppkey hash
        HashIndexBuilder hi_suppkey(table_dir, "ps_suppkey", "partsupp_ps_suppkey_hash");
        if (hi_suppkey.open_column()) {
            hi_suppkey.build_index();
            hi_suppkey.write_index();
        }
    }

    // Build indexes for supplier
    {
        std::string table_dir = gendb_dir + "/supplier";

        // s_suppkey hash
        HashIndexBuilder hi_suppkey(table_dir, "s_suppkey", "supplier_s_suppkey_hash");
        if (hi_suppkey.open_column()) {
            hi_suppkey.build_index();
            hi_suppkey.write_index();
        }

        // s_nationkey hash
        HashIndexBuilder hi_nationkey(table_dir, "s_nationkey", "supplier_s_nationkey_hash");
        if (hi_nationkey.open_column()) {
            hi_nationkey.build_index();
            hi_nationkey.write_index();
        }
    }

    // Build indexes for nation
    {
        std::string table_dir = gendb_dir + "/nation";

        // n_nationkey hash
        HashIndexBuilder hi_nationkey(table_dir, "n_nationkey", "nation_n_nationkey_hash");
        if (hi_nationkey.open_column()) {
            hi_nationkey.build_index();
            hi_nationkey.write_index();
        }
    }

    std::cout << "Index building completed" << std::endl;

    return 0;
}
