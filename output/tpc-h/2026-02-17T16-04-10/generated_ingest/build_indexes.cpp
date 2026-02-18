#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <filesystem>
#include <algorithm>
#include <cstdint>
#include <omp.h>

namespace fs = std::filesystem;

// Load int32_t column from binary file
std::vector<int32_t> load_int32_column(const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    in.seekg(0, std::ios::end);
    size_t size = in.tellg() / sizeof(int32_t);
    in.seekg(0, std::ios::beg);

    std::vector<int32_t> data(size);
    in.read(reinterpret_cast<char*>(data.data()), size * sizeof(int32_t));
    in.close();
    return data;
}

// Load int64_t column from binary file
std::vector<int64_t> load_int64_column(const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    in.seekg(0, std::ios::end);
    size_t size = in.tellg() / sizeof(int64_t);
    in.seekg(0, std::ios::beg);

    std::vector<int64_t> data(size);
    in.read(reinterpret_cast<char*>(data.data()), size * sizeof(int64_t));
    in.close();
    return data;
}

// Write index file
void write_index(const std::string& filename, const std::vector<char>& data) {
    std::ofstream out(filename, std::ios::binary);
    out.write(data.data(), data.size());
    out.close();
}

// Hash function for int32_t
inline uint64_t hash_int32(int32_t key) {
    return ((uint64_t)key * 0x9E3779B97F4A7C15ULL);
}

// ===== ZONE MAP BUILDER =====
// Zone maps: block-level min/max for range predicate pruning
class ZoneMapBuilder {
public:
    struct ZoneMapEntry {
        int32_t min_val, max_val;
    };

    static void build_zone_map(const std::vector<int32_t>& data, uint32_t block_size,
                               const std::string& output_file) {
        std::vector<ZoneMapEntry> zones;

        size_t num_blocks = (data.size() + block_size - 1) / block_size;
        zones.reserve(num_blocks);

        #pragma omp parallel for
        for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
            size_t start = block_id * block_size;
            size_t end = std::min(start + block_size, data.size());

            int32_t min_val = data[start];
            int32_t max_val = data[start];

            for (size_t i = start; i < end; ++i) {
                min_val = std::min(min_val, data[i]);
                max_val = std::max(max_val, data[i]);
            }

            zones[block_id] = {min_val, max_val};
        }

        // Write zone maps
        std::ofstream out(output_file, std::ios::binary);
        uint32_t num_zones = zones.size();
        out.write(reinterpret_cast<const char*>(&num_zones), sizeof(uint32_t));
        for (const auto& zone : zones) {
            out.write(reinterpret_cast<const char*>(&zone.min_val), sizeof(int32_t));
            out.write(reinterpret_cast<const char*>(&zone.max_val), sizeof(int32_t));
        }
        out.close();

        std::cout << "  Zone maps: " << num_zones << " blocks" << std::endl;
    }
};

// ===== HASH INDEX BUILDER (SINGLE-VALUE) =====
class HashIndexBuilderSingle {
public:
    static void build(const std::vector<int32_t>& keys, const std::string& output_file) {
        // Simple hash table for single-value lookups
        std::unordered_map<int32_t, uint32_t> hash_table;

        std::cout << "  Building single-value hash index: " << keys.size() << " entries" << std::endl;

        for (uint32_t i = 0; i < keys.size(); ++i) {
            hash_table[keys[i]] = i;
        }

        // Write hash table
        std::ofstream out(output_file, std::ios::binary);
        uint32_t num_entries = hash_table.size();
        out.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));

        for (const auto& [key, pos] : hash_table) {
            out.write(reinterpret_cast<const char*>(&key), sizeof(int32_t));
            out.write(reinterpret_cast<const char*>(&pos), sizeof(uint32_t));
        }
        out.close();

        std::cout << "  Wrote " << num_entries << " unique keys" << std::endl;
    }
};

// ===== HASH INDEX BUILDER (MULTI-VALUE) =====
class HashIndexBuilderMulti {
public:
    static void build(const std::vector<int32_t>& keys, const std::string& output_file) {
        std::cout << "  Building multi-value hash index: " << keys.size() << " rows" << std::endl;

        // Step 1: Count unique keys and positions
        std::map<int32_t, std::vector<uint32_t>> key_positions;
        for (uint32_t i = 0; i < keys.size(); ++i) {
            key_positions[keys[i]].push_back(i);
        }

        std::cout << "    Unique keys: " << key_positions.size() << std::endl;

        // Step 2: Build flattened positions array
        std::vector<uint32_t> positions_array;
        std::vector<uint32_t> offsets;
        std::vector<uint32_t> counts;
        std::vector<int32_t> unique_keys;

        offsets.reserve(key_positions.size());
        counts.reserve(key_positions.size());
        unique_keys.reserve(key_positions.size());

        for (const auto& [key, positions] : key_positions) {
            offsets.push_back(positions_array.size());
            counts.push_back(positions.size());
            unique_keys.push_back(key);
            for (uint32_t pos : positions) {
                positions_array.push_back(pos);
            }
        }

        // Step 3: Write multi-value hash index
        std::ofstream out(output_file, std::ios::binary);
        uint32_t num_unique = unique_keys.size();
        uint32_t num_positions = positions_array.size();

        out.write(reinterpret_cast<const char*>(&num_unique), sizeof(uint32_t));
        out.write(reinterpret_cast<const char*>(&num_positions), sizeof(uint32_t));

        // Write hash table entries: [key, offset, count] per unique key
        for (uint32_t i = 0; i < num_unique; ++i) {
            out.write(reinterpret_cast<const char*>(&unique_keys[i]), sizeof(int32_t));
            out.write(reinterpret_cast<const char*>(&offsets[i]), sizeof(uint32_t));
            out.write(reinterpret_cast<const char*>(&counts[i]), sizeof(uint32_t));
        }

        // Write positions array
        out.write(reinterpret_cast<const char*>(positions_array.data()),
                  num_positions * sizeof(uint32_t));
        out.close();

        std::cout << "    Positions array: " << num_positions << " total positions" << std::endl;
    }
};

// ===== SORTED INDEX BUILDER =====
class SortedIndexBuilder {
public:
    static void build(const std::vector<int32_t>& keys, const std::string& output_file) {
        std::cout << "  Building sorted index: " << keys.size() << " entries" << std::endl;

        // Create (key, position) pairs
        std::vector<std::pair<int32_t, uint32_t>> key_pos_pairs;
        key_pos_pairs.reserve(keys.size());
        for (uint32_t i = 0; i < keys.size(); ++i) {
            key_pos_pairs.push_back({keys[i], i});
        }

        // Sort by key
        #pragma omp parallel sections
        {
            #pragma omp section
            {
                std::sort(key_pos_pairs.begin(), key_pos_pairs.end(),
                         [](const auto& a, const auto& b) { return a.first < b.first; });
            }
        }

        // Write sorted index: [num_entries][key_0, pos_0, key_1, pos_1, ...]
        std::ofstream out(output_file, std::ios::binary);
        uint32_t num_entries = key_pos_pairs.size();
        out.write(reinterpret_cast<const char*>(&num_entries), sizeof(uint32_t));

        for (const auto& [key, pos] : key_pos_pairs) {
            out.write(reinterpret_cast<const char*>(&key), sizeof(int32_t));
            out.write(reinterpret_cast<const char*>(&pos), sizeof(uint32_t));
        }
        out.close();

        std::cout << "    Sorted " << num_entries << " entries" << std::endl;
    }
};

// Main index building routine
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Index Building (TPC-H SF10)" << std::endl;
    std::cout << "GenDB dir: " << gendb_dir << std::endl << std::endl;

    fs::create_directories(gendb_dir + "/indexes");

    // ===== LINEITEM INDEXES =====
    std::cout << "[lineitem]" << std::endl;
    auto li_orderkey = load_int32_column(gendb_dir + "/lineitem/l_orderkey.bin");
    auto li_suppkey = load_int32_column(gendb_dir + "/lineitem/l_suppkey.bin");
    auto li_shipdate = load_int32_column(gendb_dir + "/lineitem/l_shipdate.bin");

    std::cout << "  l_orderkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(li_orderkey, gendb_dir + "/indexes/lineitem_l_orderkey_hash.bin");

    std::cout << "  l_suppkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(li_suppkey, gendb_dir + "/indexes/lineitem_l_suppkey_hash.bin");

    std::cout << "  l_shipdate (zone map)" << std::endl;
    ZoneMapBuilder::build_zone_map(li_shipdate, 100000, gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

    std::cout << "  l_shipdate (sorted)" << std::endl;
    SortedIndexBuilder::build(li_shipdate, gendb_dir + "/indexes/lineitem_l_shipdate_sorted.bin");

    // ===== ORDERS INDEXES =====
    std::cout << "[orders]" << std::endl;
    auto o_custkey = load_int32_column(gendb_dir + "/orders/o_custkey.bin");
    auto o_orderkey = load_int32_column(gendb_dir + "/orders/o_orderkey.bin");
    auto o_orderdate = load_int32_column(gendb_dir + "/orders/o_orderdate.bin");

    std::cout << "  o_custkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(o_custkey, gendb_dir + "/indexes/orders_o_custkey_hash.bin");

    std::cout << "  o_orderkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(o_orderkey, gendb_dir + "/indexes/orders_o_orderkey_hash.bin");

    std::cout << "  o_orderdate (zone map)" << std::endl;
    ZoneMapBuilder::build_zone_map(o_orderdate, 100000, gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin");

    // ===== CUSTOMER INDEXES =====
    std::cout << "[customer]" << std::endl;
    auto c_custkey = load_int32_column(gendb_dir + "/customer/c_custkey.bin");
    auto c_mktsegment = load_int32_column(gendb_dir + "/customer/c_mktsegment.bin");

    std::cout << "  c_custkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(c_custkey, gendb_dir + "/indexes/customer_c_custkey_hash.bin");

    std::cout << "  c_mktsegment (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(c_mktsegment, gendb_dir + "/indexes/customer_c_mktsegment_hash.bin");

    // ===== PART INDEXES =====
    std::cout << "[part]" << std::endl;
    auto p_partkey = load_int32_column(gendb_dir + "/part/p_partkey.bin");
    auto p_brand = load_int32_column(gendb_dir + "/part/p_brand.bin");

    std::cout << "  p_partkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(p_partkey, gendb_dir + "/indexes/part_p_partkey_hash.bin");

    std::cout << "  p_brand (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(p_brand, gendb_dir + "/indexes/part_p_brand_hash.bin");

    // ===== PARTSUPP INDEXES =====
    std::cout << "[partsupp]" << std::endl;
    auto ps_partkey = load_int32_column(gendb_dir + "/partsupp/ps_partkey.bin");
    auto ps_suppkey = load_int32_column(gendb_dir + "/partsupp/ps_suppkey.bin");

    std::cout << "  ps_partkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(ps_partkey, gendb_dir + "/indexes/partsupp_ps_partkey_hash.bin");

    std::cout << "  ps_suppkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(ps_suppkey, gendb_dir + "/indexes/partsupp_ps_suppkey_hash.bin");

    // ===== SUPPLIER INDEXES =====
    std::cout << "[supplier]" << std::endl;
    auto s_suppkey = load_int32_column(gendb_dir + "/supplier/s_suppkey.bin");
    auto s_nationkey = load_int32_column(gendb_dir + "/supplier/s_nationkey.bin");

    std::cout << "  s_suppkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(s_suppkey, gendb_dir + "/indexes/supplier_s_suppkey_hash.bin");

    std::cout << "  s_nationkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(s_nationkey, gendb_dir + "/indexes/supplier_s_nationkey_hash.bin");

    // ===== NATION INDEXES =====
    std::cout << "[nation]" << std::endl;
    auto n_nationkey = load_int32_column(gendb_dir + "/nation/n_nationkey.bin");
    auto n_regionkey = load_int32_column(gendb_dir + "/nation/n_regionkey.bin");

    std::cout << "  n_nationkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(n_nationkey, gendb_dir + "/indexes/nation_n_nationkey_hash.bin");

    std::cout << "  n_regionkey (multi-value hash)" << std::endl;
    HashIndexBuilderMulti::build(n_regionkey, gendb_dir + "/indexes/nation_n_regionkey_hash.bin");

    // ===== REGION INDEXES =====
    std::cout << "[region]" << std::endl;
    auto r_regionkey = load_int32_column(gendb_dir + "/region/r_regionkey.bin");

    std::cout << "  r_regionkey (single-value hash)" << std::endl;
    HashIndexBuilderSingle::build(r_regionkey, gendb_dir + "/indexes/region_r_regionkey_hash.bin");

    std::cout << "\n=== INDEX BUILDING COMPLETE ===" << std::endl;

    return 0;
}
