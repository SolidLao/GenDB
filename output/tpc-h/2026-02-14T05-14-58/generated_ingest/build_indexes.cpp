#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <cstdint>

namespace fs = std::filesystem;

// ============================================================================
// Zone Map Builder
// ============================================================================

struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

void build_zone_map(const std::string& col_file, const std::string& output_file,
                    uint32_t block_size = 262144) {
    std::ifstream in(col_file, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open: " << col_file << std::endl;
        return;
    }

    std::ofstream out(output_file, std::ios::binary);
    std::vector<int32_t> buffer(block_size);
    size_t rows_read = 0;

    while (true) {
        in.read(reinterpret_cast<char*>(buffer.data()), block_size * sizeof(int32_t));
        size_t bytes_read = in.gcount();
        size_t values_read = bytes_read / sizeof(int32_t);

        if (values_read == 0) break;

        int32_t min_val = buffer[0];
        int32_t max_val = buffer[0];

        for (size_t i = 1; i < values_read; ++i) {
            min_val = std::min(min_val, buffer[i]);
            max_val = std::max(max_val, buffer[i]);
        }

        ZoneMap zone = {min_val, max_val, (uint32_t)values_read};
        out.write(reinterpret_cast<char*>(&zone), sizeof(ZoneMap));
        rows_read += values_read;
    }

    in.close();
    out.close();
    std::cout << "  Zone map written to " << output_file << " (" << rows_read << " rows in zones)" << std::endl;
}

// ============================================================================
// Sorted Index Builder
// ============================================================================

struct SortedIndexEntry {
    int32_t value;
    uint32_t row_id;
};

void build_sorted_index(const std::string& col_file, const std::string& output_file) {
    std::ifstream in(col_file, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open: " << col_file << std::endl;
        return;
    }

    // Read all values
    std::vector<int32_t> values;
    int32_t val;
    uint32_t row_id = 0;
    while (in.read(reinterpret_cast<char*>(&val), sizeof(int32_t))) {
        values.push_back(val);
        row_id++;
    }
    in.close();

    // Build sorted index entries
    std::vector<SortedIndexEntry> entries(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        entries[i] = {values[i], (uint32_t)i};
    }

    // Sort by value
    std::sort(entries.begin(), entries.end(), [](const SortedIndexEntry& a, const SortedIndexEntry& b) {
        return a.value < b.value;
    });

    // Write index
    std::ofstream out(output_file, std::ios::binary);
    for (const auto& entry : entries) {
        out.write(reinterpret_cast<const char*>(&entry), sizeof(SortedIndexEntry));
    }
    out.close();

    std::cout << "  Sorted index written to " << output_file << " (" << entries.size() << " entries)" << std::endl;
}

// ============================================================================
// Hash Index Builder
// ============================================================================

struct HashIndexEntry {
    uint32_t row_id;
};

void build_hash_index(const std::string& col_file, const std::string& output_file) {
    std::ifstream in(col_file, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open: " << col_file << std::endl;
        return;
    }

    // Read all values and build map
    std::map<uint8_t, std::vector<uint32_t>> value_to_rows;
    uint8_t val;
    uint32_t row_id = 0;
    while (in.read(reinterpret_cast<char*>(&val), sizeof(uint8_t))) {
        value_to_rows[val].push_back(row_id);
        row_id++;
    }
    in.close();

    // Write hash index: value -> [row_ids...]
    std::ofstream out(output_file, std::ios::binary);
    uint32_t num_buckets = value_to_rows.size();
    out.write(reinterpret_cast<char*>(&num_buckets), sizeof(uint32_t));

    for (const auto& [val, row_ids] : value_to_rows) {
        uint32_t count = row_ids.size();
        uint8_t val_copy = val;
        out.write(reinterpret_cast<char*>(&val_copy), sizeof(uint8_t));
        out.write(reinterpret_cast<char*>(&count), sizeof(uint32_t));
        for (uint32_t rid : row_ids) {
            out.write(reinterpret_cast<char*>(&rid), sizeof(uint32_t));
        }
    }
    out.close();

    std::cout << "  Hash index written to " << output_file << " (" << value_to_rows.size() << " buckets)" << std::endl;
}

// ============================================================================
// Index Building for Each Table
// ============================================================================

void build_table_indexes(const std::string& gendb_dir, const std::string& table_name) {
    std::string table_dir = gendb_dir + "/" + table_name;
    fs::create_directories(table_dir);

    std::cout << "Building indexes for " << table_name << "..." << std::endl;

    // lineitem: zone_map on l_shipdate (col 10), sorted on l_orderkey (col 0)
    if (table_name == "lineitem") {
        std::string shipdate_col = gendb_dir + "/lineitem_l_shipdate.col";
        std::string orderkey_col = gendb_dir + "/lineitem_l_orderkey.col";

        build_zone_map(shipdate_col, table_dir + "/idx_lineitem_shipdate.zm");
        build_sorted_index(orderkey_col, table_dir + "/idx_lineitem_orderkey.idx");
    }
    // orders: zone_map on o_orderdate (col 4), sorted on o_custkey (col 1)
    else if (table_name == "orders") {
        std::string orderdate_col = gendb_dir + "/orders_o_orderdate.col";
        std::string custkey_col = gendb_dir + "/orders_o_custkey.col";

        build_zone_map(orderdate_col, table_dir + "/idx_orders_orderdate.zm");
        build_sorted_index(custkey_col, table_dir + "/idx_orders_custkey.idx");
    }
    // customer: sorted on c_custkey (col 0), hash on c_mktsegment (col 6)
    else if (table_name == "customer") {
        std::string custkey_col = gendb_dir + "/customer_c_custkey.col";
        std::string mktseg_col = gendb_dir + "/customer_c_mktsegment.col";

        build_sorted_index(custkey_col, table_dir + "/idx_customer_custkey.idx");
        build_hash_index(mktseg_col, table_dir + "/idx_customer_mktsegment.idx");
    }
    // partsupp: sorted on ps_partkey (col 0) and ps_suppkey (col 1)
    else if (table_name == "partsupp") {
        std::string partkey_col = gendb_dir + "/partsupp_ps_partkey.col";
        std::string suppkey_col = gendb_dir + "/partsupp_ps_suppkey.col";

        build_sorted_index(partkey_col, table_dir + "/idx_partsupp_partkey.idx");
        build_sorted_index(suppkey_col, table_dir + "/idx_partsupp_suppkey.idx");
    }
    // part: sorted on p_partkey (col 0)
    else if (table_name == "part") {
        std::string partkey_col = gendb_dir + "/part_p_partkey.col";
        build_sorted_index(partkey_col, table_dir + "/idx_part_partkey.idx");
    }
    // supplier: sorted on s_suppkey (col 0)
    else if (table_name == "supplier") {
        std::string suppkey_col = gendb_dir + "/supplier_s_suppkey.col";
        build_sorted_index(suppkey_col, table_dir + "/idx_supplier_suppkey.idx");
    }
    // nation: sorted on n_nationkey (col 0)
    else if (table_name == "nation") {
        std::string nationkey_col = gendb_dir + "/nation_n_nationkey.col";
        build_sorted_index(nationkey_col, table_dir + "/idx_nation_nationkey.idx");
    }
    // region: sorted on r_regionkey (col 0)
    else if (table_name == "region") {
        std::string regionkey_col = gendb_dir + "/region_r_regionkey.col";
        build_sorted_index(regionkey_col, table_dir + "/idx_region_regionkey.idx");
    }

    std::cout << "  Table " << table_name << " index building complete" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "GenDB Index Building" << std::endl;
    std::cout << "GenDB dir: " << gendb_dir << std::endl;

    // Build indexes for all tables
    build_table_indexes(gendb_dir, "lineitem");
    build_table_indexes(gendb_dir, "orders");
    build_table_indexes(gendb_dir, "customer");
    build_table_indexes(gendb_dir, "partsupp");
    build_table_indexes(gendb_dir, "part");
    build_table_indexes(gendb_dir, "supplier");
    build_table_indexes(gendb_dir, "nation");
    build_table_indexes(gendb_dir, "region");

    std::cout << "\nIndex building complete" << std::endl;

    return 0;
}
