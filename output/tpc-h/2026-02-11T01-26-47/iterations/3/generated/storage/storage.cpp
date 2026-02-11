#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstring>
#include <thread>
#include <atomic>
#include <algorithm>
#include <limits>

namespace gendb {

// Helper: create directory if it doesn't exist
static void ensure_directory(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

// Helper: build zone maps for a date column (HDD-optimized: 64K rows per block)
static std::vector<ZoneMap> build_zone_maps(const std::vector<int32_t>& column, size_t block_size = 65536) {
    std::vector<ZoneMap> zone_maps;
    if (column.empty()) return zone_maps;

    const size_t num_blocks = (column.size() + block_size - 1) / block_size;
    zone_maps.reserve(num_blocks);

    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        const size_t start_row = block_id * block_size;
        const size_t end_row = std::min(start_row + block_size, column.size());
        const size_t row_count = end_row - start_row;

        int32_t min_val = std::numeric_limits<int32_t>::max();
        int32_t max_val = std::numeric_limits<int32_t>::min();

        for (size_t i = start_row; i < end_row; ++i) {
            min_val = std::min(min_val, column[i]);
            max_val = std::max(max_val, column[i]);
        }

        zone_maps.push_back({min_val, max_val, start_row, row_count});
    }

    return zone_maps;
}

// Helper: write zone maps to binary file
static void write_zone_maps(const std::string& path, const std::vector<ZoneMap>& zone_maps) {
    std::ofstream out(path, std::ios::binary);
    size_t count = zone_maps.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));
    out.write(reinterpret_cast<const char*>(zone_maps.data()), zone_maps.size() * sizeof(ZoneMap));
}

// Helper: read zone maps from binary file
static std::vector<ZoneMap> read_zone_maps(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) return {};

    size_t count;
    in.read(reinterpret_cast<char*>(&count), sizeof(count));

    std::vector<ZoneMap> zone_maps(count);
    in.read(reinterpret_cast<char*>(zone_maps.data()), count * sizeof(ZoneMap));

    return zone_maps;
}

// Helper: write binary column
template<typename T>
static void write_column_binary(const std::string& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
}

// Helper: read binary column
template<typename T>
static std::vector<T> read_column_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    std::vector<T> data(file_size / sizeof(T));
    in.read(reinterpret_cast<char*>(data.data()), file_size);
    return data;
}

// Helper: write string column (length-prefixed)
static void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream out(path, std::ios::binary);
    for (const auto& s : data) {
        uint32_t len = s.size();
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        out.write(s.data(), len);
    }
}

// Helper: read string column
static std::vector<std::string> read_string_column(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::vector<std::string> data;

    while (in.peek() != EOF) {
        uint32_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string s(len, '\0');
        in.read(&s[0], len);
        data.push_back(std::move(s));
    }
    return data;
}

// ===== Lineitem =====

LineitemTable ingest_lineitem_tbl(const std::string& tbl_path) {
    LineitemTable table;
    std::ifstream in(tbl_path);
    std::string line;

    // Estimate row count for reserve
    table.reserve(60000000);

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 16) continue;

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));
        table.l_returnflag.push_back(fields[8][0]);
        table.l_linestatus.push_back(fields[9][0]);
        table.l_shipdate.push_back(parse_date(fields[10]));
    }

    return table;
}

void write_lineitem(const std::string& gendb_dir, const LineitemTable& table) {
    std::string dir = gendb_dir + "/lineitem";
    ensure_directory(dir);

    // CRITICAL: Sort by l_shipdate before writing to enable effective zone maps
    // Zone maps only work if data is sorted - otherwise every block has overlapping ranges
    std::cout << "  Sorting lineitem by l_shipdate (required for zone map effectiveness)...\n";
    const size_t n = table.l_orderkey.size();

    // Create index array for sorting
    std::vector<size_t> indices(n);
    for (size_t i = 0; i < n; ++i) {
        indices[i] = i;
    }

    // Sort indices by l_shipdate (then by l_orderkey for stability)
    const size_t num_threads = std::thread::hardware_concurrency();
    std::cout << "  Using parallel sort with " << num_threads << " threads...\n";

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        if (table.l_shipdate[a] != table.l_shipdate[b]) {
            return table.l_shipdate[a] < table.l_shipdate[b];
        }
        return table.l_orderkey[a] < table.l_orderkey[b];
    });

    std::cout << "  Sort complete. Reordering columns...\n";

    // Reorder all columns according to sorted indices
    std::vector<int32_t> sorted_orderkey(n);
    std::vector<double> sorted_quantity(n);
    std::vector<double> sorted_extendedprice(n);
    std::vector<double> sorted_discount(n);
    std::vector<double> sorted_tax(n);
    std::vector<char> sorted_returnflag(n);
    std::vector<char> sorted_linestatus(n);
    std::vector<int32_t> sorted_shipdate(n);

    for (size_t i = 0; i < n; ++i) {
        const size_t src_idx = indices[i];
        sorted_orderkey[i] = table.l_orderkey[src_idx];
        sorted_quantity[i] = table.l_quantity[src_idx];
        sorted_extendedprice[i] = table.l_extendedprice[src_idx];
        sorted_discount[i] = table.l_discount[src_idx];
        sorted_tax[i] = table.l_tax[src_idx];
        sorted_returnflag[i] = table.l_returnflag[src_idx];
        sorted_linestatus[i] = table.l_linestatus[src_idx];
        sorted_shipdate[i] = table.l_shipdate[src_idx];
    }

    std::cout << "  Writing sorted binary columns...\n";

    write_column_binary(dir + "/l_orderkey.col", sorted_orderkey);
    write_column_binary(dir + "/l_quantity.col", sorted_quantity);
    write_column_binary(dir + "/l_extendedprice.col", sorted_extendedprice);
    write_column_binary(dir + "/l_discount.col", sorted_discount);
    write_column_binary(dir + "/l_tax.col", sorted_tax);
    write_column_binary(dir + "/l_returnflag.col", sorted_returnflag);
    write_column_binary(dir + "/l_linestatus.col", sorted_linestatus);
    write_column_binary(dir + "/l_shipdate.col", sorted_shipdate);

    // Build and write zone maps for l_shipdate (HDD: 64K rows per block)
    auto shipdate_zone_maps = build_zone_maps(sorted_shipdate, 65536);
    write_zone_maps(dir + "/l_shipdate.zonemap", shipdate_zone_maps);

    std::cout << "  Built " << shipdate_zone_maps.size() << " zone map blocks for lineitem.l_shipdate\n";

    // Write row count metadata
    std::ofstream meta(dir + "/metadata.txt");
    meta << table.size() << "\n";
}

LineitemTable read_lineitem(const std::string& gendb_dir) {
    std::string dir = gendb_dir + "/lineitem";
    LineitemTable table;

    table.l_orderkey = read_column_binary<int32_t>(dir + "/l_orderkey.col");
    table.l_quantity = read_column_binary<double>(dir + "/l_quantity.col");
    table.l_extendedprice = read_column_binary<double>(dir + "/l_extendedprice.col");
    table.l_discount = read_column_binary<double>(dir + "/l_discount.col");
    table.l_tax = read_column_binary<double>(dir + "/l_tax.col");
    table.l_returnflag = read_column_binary<char>(dir + "/l_returnflag.col");
    table.l_linestatus = read_column_binary<char>(dir + "/l_linestatus.col");
    table.l_shipdate = read_column_binary<int32_t>(dir + "/l_shipdate.col");

    // Load zone maps
    table.l_shipdate_zone_maps = read_zone_maps(dir + "/l_shipdate.zonemap");

    return table;
}

// ===== Orders =====

OrdersTable ingest_orders_tbl(const std::string& tbl_path) {
    OrdersTable table;
    std::ifstream in(tbl_path);
    std::string line;

    table.reserve(15000000);

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 9) continue;

        table.o_orderkey.push_back(std::stoi(fields[0]));
        table.o_custkey.push_back(std::stoi(fields[1]));
        table.o_orderdate.push_back(parse_date(fields[4]));
        table.o_shippriority.push_back(std::stoi(fields[7]));
    }

    return table;
}

void write_orders(const std::string& gendb_dir, const OrdersTable& table) {
    std::string dir = gendb_dir + "/orders";
    ensure_directory(dir);

    // CRITICAL: Sort by o_orderdate before writing to enable effective zone maps
    std::cout << "  Sorting orders by o_orderdate (required for zone map effectiveness)...\n";
    const size_t n = table.o_orderkey.size();

    // Create index array for sorting
    std::vector<size_t> indices(n);
    for (size_t i = 0; i < n; ++i) {
        indices[i] = i;
    }

    // Sort indices by o_orderdate (then by o_orderkey for stability)
    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        if (table.o_orderdate[a] != table.o_orderdate[b]) {
            return table.o_orderdate[a] < table.o_orderdate[b];
        }
        return table.o_orderkey[a] < table.o_orderkey[b];
    });

    std::cout << "  Sort complete. Reordering columns...\n";

    // Reorder all columns according to sorted indices
    std::vector<int32_t> sorted_orderkey(n);
    std::vector<int32_t> sorted_custkey(n);
    std::vector<int32_t> sorted_orderdate(n);
    std::vector<int32_t> sorted_shippriority(n);

    for (size_t i = 0; i < n; ++i) {
        const size_t src_idx = indices[i];
        sorted_orderkey[i] = table.o_orderkey[src_idx];
        sorted_custkey[i] = table.o_custkey[src_idx];
        sorted_orderdate[i] = table.o_orderdate[src_idx];
        sorted_shippriority[i] = table.o_shippriority[src_idx];
    }

    std::cout << "  Writing sorted binary columns...\n";

    write_column_binary(dir + "/o_orderkey.col", sorted_orderkey);
    write_column_binary(dir + "/o_custkey.col", sorted_custkey);
    write_column_binary(dir + "/o_orderdate.col", sorted_orderdate);
    write_column_binary(dir + "/o_shippriority.col", sorted_shippriority);

    // Build and write zone maps for o_orderdate (HDD: 32K rows per block for smaller table)
    auto orderdate_zone_maps = build_zone_maps(sorted_orderdate, 32768);
    write_zone_maps(dir + "/o_orderdate.zonemap", orderdate_zone_maps);

    std::cout << "  Built " << orderdate_zone_maps.size() << " zone map blocks for orders.o_orderdate\n";

    std::ofstream meta(dir + "/metadata.txt");
    meta << table.size() << "\n";
}

OrdersTable read_orders(const std::string& gendb_dir) {
    std::string dir = gendb_dir + "/orders";
    OrdersTable table;

    table.o_orderkey = read_column_binary<int32_t>(dir + "/o_orderkey.col");
    table.o_custkey = read_column_binary<int32_t>(dir + "/o_custkey.col");
    table.o_orderdate = read_column_binary<int32_t>(dir + "/o_orderdate.col");
    table.o_shippriority = read_column_binary<int32_t>(dir + "/o_shippriority.col");

    // Load zone maps
    table.o_orderdate_zone_maps = read_zone_maps(dir + "/o_orderdate.zonemap");

    return table;
}

// ===== Customer =====

CustomerTable ingest_customer_tbl(const std::string& tbl_path) {
    CustomerTable table;
    std::ifstream in(tbl_path);
    std::string line;

    table.reserve(1500000);

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_mktsegment.push_back(fields[6]);
    }

    return table;
}

void write_customer(const std::string& gendb_dir, const CustomerTable& table) {
    std::string dir = gendb_dir + "/customer";
    ensure_directory(dir);

    write_column_binary(dir + "/c_custkey.col", table.c_custkey);
    write_string_column(dir + "/c_mktsegment.col", table.c_mktsegment);

    std::ofstream meta(dir + "/metadata.txt");
    meta << table.size() << "\n";
}

CustomerTable read_customer(const std::string& gendb_dir) {
    std::string dir = gendb_dir + "/customer";
    CustomerTable table;

    table.c_custkey = read_column_binary<int32_t>(dir + "/c_custkey.col");
    table.c_mktsegment = read_string_column(dir + "/c_mktsegment.col");

    return table;
}

} // namespace gendb
