#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <algorithm>
#include <cstring>
#include <unordered_map>
#include <chrono>
#include <numeric>

namespace fs = std::filesystem;

// ============= Index Building Utilities =============

// Zone map: min, max values per block
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Build zone maps for a sorted int32_t column
void build_zone_maps_int32(const std::string& col_file, const std::string& index_file, uint32_t block_size) {
    std::cout << "  Building zone map: " << index_file << std::endl;

    // Read binary column data
    std::ifstream col_stream(col_file, std::ios::binary);
    if (!col_stream) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    std::vector<int32_t> data;
    int32_t value;
    while (col_stream.read(reinterpret_cast<char*>(&value), sizeof(int32_t))) {
        data.push_back(value);
    }
    col_stream.close();

    // Build zone maps per block
    std::vector<ZoneMapEntry> zone_maps;
    for (size_t i = 0; i < data.size(); i += block_size) {
        size_t end = std::min(i + block_size, data.size());
        int32_t min_val = *std::min_element(data.begin() + i, data.begin() + end);
        int32_t max_val = *std::max_element(data.begin() + i, data.begin() + end);
        zone_maps.push_back({min_val, max_val, (uint32_t)(end - i)});
    }

    // Write zone maps to file
    std::ofstream idx_stream(index_file, std::ios::binary);
    uint32_t num_zones = zone_maps.size();
    idx_stream.write(reinterpret_cast<char*>(&num_zones), sizeof(num_zones));

    for (const auto& zm : zone_maps) {
        idx_stream.write(reinterpret_cast<const char*>(&zm.min_val), sizeof(zm.min_val));
        idx_stream.write(reinterpret_cast<const char*>(&zm.max_val), sizeof(zm.max_val));
        idx_stream.write(reinterpret_cast<const char*>(&zm.row_count), sizeof(zm.row_count));
    }
    idx_stream.close();

    std::cout << "    Created " << zone_maps.size() << " zone map entries" << std::endl;
}

// Build sorted index for int32_t column (array of sorted (key, rowid) pairs)
void build_sorted_index_int32(const std::string& col_file, const std::string& index_file) {
    std::cout << "  Building sorted index: " << index_file << std::endl;

    // Read column data
    std::ifstream col_stream(col_file, std::ios::binary);
    if (!col_stream) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    std::vector<std::pair<int32_t, uint32_t>> pairs;
    int32_t value;
    uint32_t row_id = 0;

    while (col_stream.read(reinterpret_cast<char*>(&value), sizeof(int32_t))) {
        pairs.push_back({value, row_id});
        row_id++;
    }
    col_stream.close();

    // Sort by key
    std::sort(pairs.begin(), pairs.end());

    // Write to index file
    std::ofstream idx_stream(index_file, std::ios::binary);
    uint32_t num_entries = pairs.size();
    idx_stream.write(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));

    for (const auto& [key, rid] : pairs) {
        idx_stream.write(reinterpret_cast<const char*>(&key), sizeof(key));
        idx_stream.write(reinterpret_cast<const char*>(&rid), sizeof(rid));
    }
    idx_stream.close();

    std::cout << "    Created sorted index with " << pairs.size() << " entries" << std::endl;
}

// Build hash index for int32_t column
void build_hash_index_int32(const std::string& col_file, const std::string& index_file) {
    std::cout << "  Building hash index: " << index_file << std::endl;

    // Read column data
    std::ifstream col_stream(col_file, std::ios::binary);
    if (!col_stream) {
        std::cerr << "Cannot open " << col_file << std::endl;
        return;
    }

    std::unordered_map<int32_t, std::vector<uint32_t>> hash_index;
    int32_t value;
    uint32_t row_id = 0;

    while (col_stream.read(reinterpret_cast<char*>(&value), sizeof(int32_t))) {
        hash_index[value].push_back(row_id);
        row_id++;
    }
    col_stream.close();

    // Write hash index (simplified: bucket-based format)
    std::ofstream idx_stream(index_file, std::ios::binary);
    uint32_t num_buckets = hash_index.size();
    idx_stream.write(reinterpret_cast<char*>(&num_buckets), sizeof(num_buckets));

    for (const auto& [key, row_ids] : hash_index) {
        idx_stream.write(reinterpret_cast<const char*>(&key), sizeof(key));
        uint32_t count = row_ids.size();
        idx_stream.write(reinterpret_cast<char*>(&count), sizeof(count));
        for (uint32_t rid : row_ids) {
            idx_stream.write(reinterpret_cast<char*>(&rid), sizeof(rid));
        }
    }
    idx_stream.close();

    std::cout << "    Created hash index with " << num_buckets << " distinct keys" << std::endl;
}

// Build sorted index for composite keys (two int32_t columns)
void build_sorted_index_composite(const std::string& col1_file, const std::string& col2_file, const std::string& index_file) {
    std::cout << "  Building composite sorted index: " << index_file << std::endl;

    // Read both columns
    std::ifstream col1_stream(col1_file, std::ios::binary);
    std::ifstream col2_stream(col2_file, std::ios::binary);
    if (!col1_stream || !col2_stream) {
        std::cerr << "Cannot open column files" << std::endl;
        return;
    }

    std::vector<std::tuple<int32_t, int32_t, uint32_t>> tuples;
    int32_t val1, val2;
    uint32_t row_id = 0;

    while (col1_stream.read(reinterpret_cast<char*>(&val1), sizeof(int32_t)) &&
           col2_stream.read(reinterpret_cast<char*>(&val2), sizeof(int32_t))) {
        tuples.push_back({val1, val2, row_id});
        row_id++;
    }
    col1_stream.close();
    col2_stream.close();

    // Sort by (col1, col2)
    std::sort(tuples.begin(), tuples.end());

    // Write to index file
    std::ofstream idx_stream(index_file, std::ios::binary);
    uint32_t num_entries = tuples.size();
    idx_stream.write(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));

    for (const auto& [k1, k2, rid] : tuples) {
        idx_stream.write(reinterpret_cast<const char*>(&k1), sizeof(k1));
        idx_stream.write(reinterpret_cast<const char*>(&k2), sizeof(k2));
        idx_stream.write(reinterpret_cast<const char*>(&rid), sizeof(rid));
    }
    idx_stream.close();

    std::cout << "    Created composite index with " << tuples.size() << " entries" << std::endl;
}

// ============= Per-Table Index Building =============

void build_lineitem_indexes(const std::string& table_dir) {
    std::cout << "[lineitem] Building indexes..." << std::endl;
    build_zone_maps_int32(table_dir + "/l_shipdate.bin", table_dir + "/l_shipdate_zone.idx", 262144);
    build_sorted_index_int32(table_dir + "/l_orderkey.bin", table_dir + "/l_orderkey_sorted.idx");
    std::cout << "  Hash index skipped (l_returnflag/l_linestatus composite would require more infrastructure)" << std::endl;
}

void build_orders_indexes(const std::string& table_dir) {
    std::cout << "[orders] Building indexes..." << std::endl;
    build_zone_maps_int32(table_dir + "/o_orderdate.bin", table_dir + "/o_orderdate_zone.idx", 262144);
    build_sorted_index_int32(table_dir + "/o_orderkey.bin", table_dir + "/o_orderkey_sorted.idx");
    build_hash_index_int32(table_dir + "/o_custkey.bin", table_dir + "/o_custkey_hash.idx");
}

void build_customer_indexes(const std::string& table_dir) {
    std::cout << "[customer] Building indexes..." << std::endl;
    build_sorted_index_int32(table_dir + "/c_custkey.bin", table_dir + "/c_custkey_sorted.idx");
    std::cout << "  Hash index on c_mktsegment would need dictionary mapping (skipped for now)" << std::endl;
}

void build_part_indexes(const std::string& table_dir) {
    std::cout << "[part] Building indexes..." << std::endl;
    std::cout << "  Note: part table not accessed in hot queries, minimal indexing" << std::endl;
}

void build_partsupp_indexes(const std::string& table_dir) {
    std::cout << "[partsupp] Building indexes..." << std::endl;
    if (fs::exists(table_dir + "/partsupp_col0.bin") && fs::exists(table_dir + "/partsupp_col1.bin")) {
        std::cout << "  Note: composite index (ps_partkey, ps_suppkey) would require reading generic column files" << std::endl;
    }
}

void build_supplier_indexes(const std::string& table_dir) {
    std::cout << "[supplier] Building indexes..." << std::endl;
    std::cout << "  Note: supplier table not accessed in hot queries, minimal indexing" << std::endl;
}

void build_nation_indexes(const std::string& table_dir) {
    std::cout << "[nation] Building indexes..." << std::endl;
    std::cout << "  Note: nation is tiny (25 rows), no indexing needed" << std::endl;
}

void build_region_indexes(const std::string& table_dir) {
    std::cout << "[region] Building indexes..." << std::endl;
    std::cout << "  Note: region is tiny (5 rows), no indexing needed" << std::endl;
}

// ============= Main =============
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "=== Building Indexes ===" << std::endl;

    build_lineitem_indexes(gendb_dir + "/lineitem");
    build_orders_indexes(gendb_dir + "/orders");
    build_customer_indexes(gendb_dir + "/customer");
    build_part_indexes(gendb_dir + "/part");
    build_partsupp_indexes(gendb_dir + "/partsupp");
    build_supplier_indexes(gendb_dir + "/supplier");
    build_nation_indexes(gendb_dir + "/nation");
    build_region_indexes(gendb_dir + "/region");

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "\n=== Index Building Complete ===" << std::endl;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
