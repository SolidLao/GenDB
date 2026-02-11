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

namespace gendb {

// Helper: create directory if it doesn't exist
static void ensure_directory(const std::string& path) {
    mkdir(path.c_str(), 0755);
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

    write_column_binary(dir + "/l_orderkey.col", table.l_orderkey);
    write_column_binary(dir + "/l_quantity.col", table.l_quantity);
    write_column_binary(dir + "/l_extendedprice.col", table.l_extendedprice);
    write_column_binary(dir + "/l_discount.col", table.l_discount);
    write_column_binary(dir + "/l_tax.col", table.l_tax);
    write_column_binary(dir + "/l_returnflag.col", table.l_returnflag);
    write_column_binary(dir + "/l_linestatus.col", table.l_linestatus);
    write_column_binary(dir + "/l_shipdate.col", table.l_shipdate);

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

    write_column_binary(dir + "/o_orderkey.col", table.o_orderkey);
    write_column_binary(dir + "/o_custkey.col", table.o_custkey);
    write_column_binary(dir + "/o_orderdate.col", table.o_orderdate);
    write_column_binary(dir + "/o_shippriority.col", table.o_shippriority);

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
