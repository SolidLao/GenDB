#include "storage.h"
#include "../utils/date_utils.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <thread>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <sys/types.h>

namespace gendb {

// Helper: parse a pipe-delimited line from mmap'd buffer
static const char* parse_field(const char* ptr, const char* end, std::string& out) {
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    if (!delim) delim = end;
    out.assign(ptr, delim - ptr);
    return (delim < end) ? delim + 1 : end;
}

static const char* parse_int32(const char* ptr, const char* end, int32_t& out) {
    char* next;
    out = strtol(ptr, &next, 10);
    // Skip to delimiter
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    return (delim && delim < end) ? delim + 1 : end;
}

static const char* parse_double(const char* ptr, const char* end, double& out) {
    char* next;
    out = strtod(ptr, &next);
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    return (delim && delim < end) ? delim + 1 : end;
}

static const char* parse_char(const char* ptr, const char* end, char& out) {
    out = *ptr;
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    return (delim && delim < end) ? delim + 1 : end;
}

static const char* parse_date(const char* ptr, const char* end, int32_t& out) {
    // Parse YYYY-MM-DD
    int y, m, d;
    sscanf(ptr, "%d-%d-%d", &y, &m, &d);
    out = date_to_days(y, m, d);
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    return (delim && delim < end) ? delim + 1 : end;
}

// Skip field
static const char* skip_field(const char* ptr, const char* end) {
    const char* delim = (const char*)memchr(ptr, '|', end - ptr);
    return (delim && delim < end) ? delim + 1 : end;
}

// Helper: write column to binary file
template<typename T>
static void write_column_binary(const std::vector<T>& col, const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        std::cerr << "Failed to open " << path << " for writing\n";
        return;
    }
    setvbuf(f, nullptr, _IOFBF, 1 << 20); // 1MB buffer
    fwrite(col.data(), sizeof(T), col.size(), f);
    fclose(f);
}

// Helper: write string column (length-prefixed)
static void write_string_column(const std::vector<std::string>& col, const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        std::cerr << "Failed to open " << path << " for writing\n";
        return;
    }
    setvbuf(f, nullptr, _IOFBF, 1 << 20);
    for (const auto& s : col) {
        uint32_t len = s.size();
        fwrite(&len, sizeof(len), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: write dictionary mapping
template<typename K>
static void write_dictionary(const std::unordered_map<K, uint8_t>& dict, const std::string& path) {
    std::ofstream f(path);
    for (const auto& [k, v] : dict) {
        f << (int)v << "," << k << "\n";
    }
}

static void write_char_dictionary(const std::unordered_map<char, uint8_t>& dict, const std::string& path) {
    std::ofstream f(path);
    for (const auto& [k, v] : dict) {
        f << (int)v << "," << k << "\n";
    }
}

// Lineitem ingestion
void ingest_lineitem(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // mmap input file
    int fd = open(tbl_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << tbl_path << "\n";
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << tbl_path << "\n";
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Estimate row count
    size_t est_rows = file_size / 150; // avg ~150 bytes/line for lineitem

    LineitemTable table;
    table.l_orderkey.reserve(est_rows);
    table.l_partkey.reserve(est_rows);
    table.l_suppkey.reserve(est_rows);
    table.l_linenumber.reserve(est_rows);
    table.l_quantity.reserve(est_rows);
    table.l_extendedprice.reserve(est_rows);
    table.l_discount.reserve(est_rows);
    table.l_tax.reserve(est_rows);
    table.l_returnflag.reserve(est_rows);
    table.l_linestatus.reserve(est_rows);
    table.l_shipdate.reserve(est_rows);
    table.l_commitdate.reserve(est_rows);
    table.l_receiptdate.reserve(est_rows);
    table.l_shipinstruct.reserve(est_rows);
    table.l_shipmode.reserve(est_rows);
    table.l_comment.reserve(est_rows);

    // Initialize dictionaries
    uint8_t returnflag_next = 0, linestatus_next = 0;

    // Parse line by line
    const char* ptr = data;
    const char* end = data + file_size;

    while (ptr < end) {
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        if (line_end > ptr) {
            int32_t i32;
            double d;
            char c;
            std::string s;

            ptr = parse_int32(ptr, line_end, i32); table.l_orderkey.push_back(i32);
            ptr = parse_int32(ptr, line_end, i32); table.l_partkey.push_back(i32);
            ptr = parse_int32(ptr, line_end, i32); table.l_suppkey.push_back(i32);
            ptr = parse_int32(ptr, line_end, i32); table.l_linenumber.push_back(i32);
            ptr = parse_double(ptr, line_end, d); table.l_quantity.push_back(d);
            ptr = parse_double(ptr, line_end, d); table.l_extendedprice.push_back(d);
            ptr = parse_double(ptr, line_end, d); table.l_discount.push_back(d);
            ptr = parse_double(ptr, line_end, d); table.l_tax.push_back(d);

            // returnflag (dictionary encoding)
            ptr = parse_char(ptr, line_end, c);
            if (table.returnflag_dict.find(c) == table.returnflag_dict.end()) {
                table.returnflag_dict[c] = returnflag_next;
                table.returnflag_rev[returnflag_next] = c;
                returnflag_next++;
            }
            table.l_returnflag.push_back(table.returnflag_dict[c]);

            // linestatus (dictionary encoding)
            ptr = parse_char(ptr, line_end, c);
            if (table.linestatus_dict.find(c) == table.linestatus_dict.end()) {
                table.linestatus_dict[c] = linestatus_next;
                table.linestatus_rev[linestatus_next] = c;
                linestatus_next++;
            }
            table.l_linestatus.push_back(table.linestatus_dict[c]);

            ptr = parse_date(ptr, line_end, i32); table.l_shipdate.push_back(i32);
            ptr = parse_date(ptr, line_end, i32); table.l_commitdate.push_back(i32);
            ptr = parse_date(ptr, line_end, i32); table.l_receiptdate.push_back(i32);
            ptr = parse_field(ptr, line_end, s); table.l_shipinstruct.push_back(s);
            ptr = parse_field(ptr, line_end, s); table.l_shipmode.push_back(s);
            ptr = parse_field(ptr, line_end, s); table.l_comment.push_back(s);
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);
    close(fd);

    // Sort by l_shipdate for zone map effectiveness
    std::vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns
    auto reorder_int32 = [&](std::vector<int32_t>& col) {
        std::vector<int32_t> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_double = [&](std::vector<double>& col) {
        std::vector<double> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_uint8 = [&](std::vector<uint8_t>& col) {
        std::vector<uint8_t> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_string = [&](std::vector<std::string>& col) {
        std::vector<std::string> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };

    reorder_int32(table.l_orderkey);
    reorder_int32(table.l_partkey);
    reorder_int32(table.l_suppkey);
    reorder_int32(table.l_linenumber);
    reorder_double(table.l_quantity);
    reorder_double(table.l_extendedprice);
    reorder_double(table.l_discount);
    reorder_double(table.l_tax);
    reorder_uint8(table.l_returnflag);
    reorder_uint8(table.l_linestatus);
    reorder_int32(table.l_shipdate);
    reorder_int32(table.l_commitdate);
    reorder_int32(table.l_receiptdate);
    reorder_string(table.l_shipinstruct);
    reorder_string(table.l_shipmode);
    reorder_string(table.l_comment);

    // Write binary columns
    std::string table_dir = gendb_dir + "/lineitem";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.l_orderkey, table_dir + "/l_orderkey.bin");
    write_column_binary(table.l_partkey, table_dir + "/l_partkey.bin");
    write_column_binary(table.l_suppkey, table_dir + "/l_suppkey.bin");
    write_column_binary(table.l_linenumber, table_dir + "/l_linenumber.bin");
    write_column_binary(table.l_quantity, table_dir + "/l_quantity.bin");
    write_column_binary(table.l_extendedprice, table_dir + "/l_extendedprice.bin");
    write_column_binary(table.l_discount, table_dir + "/l_discount.bin");
    write_column_binary(table.l_tax, table_dir + "/l_tax.bin");
    write_column_binary(table.l_returnflag, table_dir + "/l_returnflag.bin");
    write_column_binary(table.l_linestatus, table_dir + "/l_linestatus.bin");
    write_column_binary(table.l_shipdate, table_dir + "/l_shipdate.bin");
    write_column_binary(table.l_commitdate, table_dir + "/l_commitdate.bin");
    write_column_binary(table.l_receiptdate, table_dir + "/l_receiptdate.bin");
    write_string_column(table.l_shipinstruct, table_dir + "/l_shipinstruct.bin");
    write_string_column(table.l_shipmode, table_dir + "/l_shipmode.bin");
    write_string_column(table.l_comment, table_dir + "/l_comment.bin");

    // Write dictionaries
    write_char_dictionary(table.returnflag_dict, table_dir + "/l_returnflag.dict");
    write_char_dictionary(table.linestatus_dict, table_dir + "/l_linestatus.dict");

    // Write metadata
    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "lineitem: " << table.size() << " rows in " << elapsed << "s\n";
}

// Orders ingestion (similar pattern, simplified)
void ingest_orders(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    int fd = open(tbl_path.c_str(), O_RDONLY);
    if (fd < 0) return;

    size_t file_size = lseek(fd, 0, SEEK_END);
    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) { close(fd); return; }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    size_t est_rows = file_size / 120;

    OrdersTable table;
    table.o_orderkey.reserve(est_rows);
    table.o_custkey.reserve(est_rows);
    table.o_orderstatus.reserve(est_rows);
    table.o_totalprice.reserve(est_rows);
    table.o_orderdate.reserve(est_rows);
    table.o_orderpriority.reserve(est_rows);
    table.o_clerk.reserve(est_rows);
    table.o_shippriority.reserve(est_rows);
    table.o_comment.reserve(est_rows);

    uint8_t orderstatus_next = 0;

    const char* ptr = data;
    const char* end = data + file_size;

    while (ptr < end) {
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        if (line_end > ptr) {
            int32_t i32;
            double d;
            char c;
            std::string s;

            ptr = parse_int32(ptr, line_end, i32); table.o_orderkey.push_back(i32);
            ptr = parse_int32(ptr, line_end, i32); table.o_custkey.push_back(i32);

            ptr = parse_char(ptr, line_end, c);
            if (table.orderstatus_dict.find(c) == table.orderstatus_dict.end()) {
                table.orderstatus_dict[c] = orderstatus_next;
                table.orderstatus_rev[orderstatus_next] = c;
                orderstatus_next++;
            }
            table.o_orderstatus.push_back(table.orderstatus_dict[c]);

            ptr = parse_double(ptr, line_end, d); table.o_totalprice.push_back(d);
            ptr = parse_date(ptr, line_end, i32); table.o_orderdate.push_back(i32);
            ptr = parse_field(ptr, line_end, s); table.o_orderpriority.push_back(s);
            ptr = parse_field(ptr, line_end, s); table.o_clerk.push_back(s);
            ptr = parse_int32(ptr, line_end, i32); table.o_shippriority.push_back(i32);
            ptr = parse_field(ptr, line_end, s); table.o_comment.push_back(s);
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);
    close(fd);

    // Sort by o_orderdate
    std::vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.o_orderdate[a] < table.o_orderdate[b];
    });

    auto reorder_int32 = [&](std::vector<int32_t>& col) {
        std::vector<int32_t> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_double = [&](std::vector<double>& col) {
        std::vector<double> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_uint8 = [&](std::vector<uint8_t>& col) {
        std::vector<uint8_t> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };
    auto reorder_string = [&](std::vector<std::string>& col) {
        std::vector<std::string> tmp(col.size());
        for (size_t i = 0; i < indices.size(); i++) tmp[i] = col[indices[i]];
        col = std::move(tmp);
    };

    reorder_int32(table.o_orderkey);
    reorder_int32(table.o_custkey);
    reorder_uint8(table.o_orderstatus);
    reorder_double(table.o_totalprice);
    reorder_int32(table.o_orderdate);
    reorder_string(table.o_orderpriority);
    reorder_string(table.o_clerk);
    reorder_int32(table.o_shippriority);
    reorder_string(table.o_comment);

    std::string table_dir = gendb_dir + "/orders";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.o_orderkey, table_dir + "/o_orderkey.bin");
    write_column_binary(table.o_custkey, table_dir + "/o_custkey.bin");
    write_column_binary(table.o_orderstatus, table_dir + "/o_orderstatus.bin");
    write_column_binary(table.o_totalprice, table_dir + "/o_totalprice.bin");
    write_column_binary(table.o_orderdate, table_dir + "/o_orderdate.bin");
    write_string_column(table.o_orderpriority, table_dir + "/o_orderpriority.bin");
    write_string_column(table.o_clerk, table_dir + "/o_clerk.bin");
    write_column_binary(table.o_shippriority, table_dir + "/o_shippriority.bin");
    write_string_column(table.o_comment, table_dir + "/o_comment.bin");

    write_char_dictionary(table.orderstatus_dict, table_dir + "/o_orderstatus.dict");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "orders: " << table.size() << " rows in " << elapsed << "s\n";
}

// Customer ingestion
void ingest_customer(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    int fd = open(tbl_path.c_str(), O_RDONLY);
    if (fd < 0) return;

    size_t file_size = lseek(fd, 0, SEEK_END);
    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) { close(fd); return; }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    size_t est_rows = file_size / 180;

    CustomerTable table;
    table.c_custkey.reserve(est_rows);
    table.c_name.reserve(est_rows);
    table.c_address.reserve(est_rows);
    table.c_nationkey.reserve(est_rows);
    table.c_phone.reserve(est_rows);
    table.c_acctbal.reserve(est_rows);
    table.c_mktsegment.reserve(est_rows);
    table.c_comment.reserve(est_rows);

    uint8_t mktsegment_next = 0;

    const char* ptr = data;
    const char* end = data + file_size;

    while (ptr < end) {
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        if (line_end > ptr) {
            int32_t i32;
            double d;
            std::string s;

            ptr = parse_int32(ptr, line_end, i32); table.c_custkey.push_back(i32);
            ptr = parse_field(ptr, line_end, s); table.c_name.push_back(s);
            ptr = parse_field(ptr, line_end, s); table.c_address.push_back(s);
            ptr = parse_int32(ptr, line_end, i32); table.c_nationkey.push_back(i32);
            ptr = parse_field(ptr, line_end, s); table.c_phone.push_back(s);
            ptr = parse_double(ptr, line_end, d); table.c_acctbal.push_back(d);

            ptr = parse_field(ptr, line_end, s);
            if (table.mktsegment_dict.find(s) == table.mktsegment_dict.end()) {
                table.mktsegment_dict[s] = mktsegment_next;
                table.mktsegment_rev[mktsegment_next] = s;
                mktsegment_next++;
            }
            table.c_mktsegment.push_back(table.mktsegment_dict[s]);

            ptr = parse_field(ptr, line_end, s); table.c_comment.push_back(s);
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);
    close(fd);

    std::string table_dir = gendb_dir + "/customer";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.c_custkey, table_dir + "/c_custkey.bin");
    write_string_column(table.c_name, table_dir + "/c_name.bin");
    write_string_column(table.c_address, table_dir + "/c_address.bin");
    write_column_binary(table.c_nationkey, table_dir + "/c_nationkey.bin");
    write_string_column(table.c_phone, table_dir + "/c_phone.bin");
    write_column_binary(table.c_acctbal, table_dir + "/c_acctbal.bin");
    write_column_binary(table.c_mktsegment, table_dir + "/c_mktsegment.bin");
    write_string_column(table.c_comment, table_dir + "/c_comment.bin");

    write_dictionary(table.mktsegment_dict, table_dir + "/c_mktsegment.dict");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "customer: " << table.size() << " rows in " << elapsed << "s\n";
}

// Simplified ingest for small tables (nation, region, supplier, part, partsupp)
void ingest_nation(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream in(tbl_path);
    NationTable table;
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int32_t i32;

        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.n_nationkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.n_name.push_back(field);
        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.n_regionkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.n_comment.push_back(field);
    }

    std::string table_dir = gendb_dir + "/nation";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.n_nationkey, table_dir + "/n_nationkey.bin");
    write_string_column(table.n_name, table_dir + "/n_name.bin");
    write_column_binary(table.n_regionkey, table_dir + "/n_regionkey.bin");
    write_string_column(table.n_comment, table_dir + "/n_comment.bin");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "nation: " << table.size() << " rows in " << elapsed << "s\n";
}

void ingest_region(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream in(tbl_path);
    RegionTable table;
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int32_t i32;

        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.r_regionkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.r_name.push_back(field);
        if (std::getline(ss, field, '|')) table.r_comment.push_back(field);
    }

    std::string table_dir = gendb_dir + "/region";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.r_regionkey, table_dir + "/r_regionkey.bin");
    write_string_column(table.r_name, table_dir + "/r_name.bin");
    write_string_column(table.r_comment, table_dir + "/r_comment.bin");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "region: " << table.size() << " rows in " << elapsed << "s\n";
}

void ingest_supplier(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream in(tbl_path);
    SupplierTable table;
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int32_t i32;
        double d;

        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.s_suppkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.s_name.push_back(field);
        if (std::getline(ss, field, '|')) table.s_address.push_back(field);
        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.s_nationkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.s_phone.push_back(field);
        if (std::getline(ss, field, '|')) { d = std::stod(field); table.s_acctbal.push_back(d); }
        if (std::getline(ss, field, '|')) table.s_comment.push_back(field);
    }

    std::string table_dir = gendb_dir + "/supplier";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.s_suppkey, table_dir + "/s_suppkey.bin");
    write_string_column(table.s_name, table_dir + "/s_name.bin");
    write_string_column(table.s_address, table_dir + "/s_address.bin");
    write_column_binary(table.s_nationkey, table_dir + "/s_nationkey.bin");
    write_string_column(table.s_phone, table_dir + "/s_phone.bin");
    write_column_binary(table.s_acctbal, table_dir + "/s_acctbal.bin");
    write_string_column(table.s_comment, table_dir + "/s_comment.bin");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "supplier: " << table.size() << " rows in " << elapsed << "s\n";
}

void ingest_part(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream in(tbl_path);
    PartTable table;
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int32_t i32;
        double d;

        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.p_partkey.push_back(i32); }
        if (std::getline(ss, field, '|')) table.p_name.push_back(field);
        if (std::getline(ss, field, '|')) table.p_mfgr.push_back(field);
        if (std::getline(ss, field, '|')) table.p_brand.push_back(field);
        if (std::getline(ss, field, '|')) table.p_type.push_back(field);
        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.p_size.push_back(i32); }
        if (std::getline(ss, field, '|')) table.p_container.push_back(field);
        if (std::getline(ss, field, '|')) { d = std::stod(field); table.p_retailprice.push_back(d); }
        if (std::getline(ss, field, '|')) table.p_comment.push_back(field);
    }

    std::string table_dir = gendb_dir + "/part";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.p_partkey, table_dir + "/p_partkey.bin");
    write_string_column(table.p_name, table_dir + "/p_name.bin");
    write_string_column(table.p_mfgr, table_dir + "/p_mfgr.bin");
    write_string_column(table.p_brand, table_dir + "/p_brand.bin");
    write_string_column(table.p_type, table_dir + "/p_type.bin");
    write_column_binary(table.p_size, table_dir + "/p_size.bin");
    write_string_column(table.p_container, table_dir + "/p_container.bin");
    write_column_binary(table.p_retailprice, table_dir + "/p_retailprice.bin");
    write_string_column(table.p_comment, table_dir + "/p_comment.bin");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "part: " << table.size() << " rows in " << elapsed << "s\n";
}

void ingest_partsupp(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream in(tbl_path);
    PartsuppTable table;
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int32_t i32;
        double d;

        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.ps_partkey.push_back(i32); }
        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.ps_suppkey.push_back(i32); }
        if (std::getline(ss, field, '|')) { i32 = std::stoi(field); table.ps_availqty.push_back(i32); }
        if (std::getline(ss, field, '|')) { d = std::stod(field); table.ps_supplycost.push_back(d); }
        if (std::getline(ss, field, '|')) table.ps_comment.push_back(field);
    }

    std::string table_dir = gendb_dir + "/partsupp";
    system(("mkdir -p " + table_dir).c_str());

    write_column_binary(table.ps_partkey, table_dir + "/ps_partkey.bin");
    write_column_binary(table.ps_suppkey, table_dir + "/ps_suppkey.bin");
    write_column_binary(table.ps_availqty, table_dir + "/ps_availqty.bin");
    write_column_binary(table.ps_supplycost, table_dir + "/ps_supplycost.bin");
    write_string_column(table.ps_comment, table_dir + "/ps_comment.bin");

    std::ofstream meta(table_dir + "/metadata.txt");
    meta << "rows=" << table.size() << "\n";
    meta.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "partsupp: " << table.size() << " rows in " << elapsed << "s\n";
}

// mmap column loader
template<typename T>
T* mmap_column(const std::string& gendb_dir, const std::string& table_name,
               const std::string& column_name, size_t& out_size) {
    std::string path = gendb_dir + "/" + table_name + "/" + column_name + ".bin";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << "\n";
        out_size = 0;
        return nullptr;
    }

    struct stat st;
    fstat(fd, &st);
    out_size = st.st_size / sizeof(T);

    T* data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << "\n";
        out_size = 0;
        return nullptr;
    }

    madvise(data, st.st_size, MADV_SEQUENTIAL);
    return data;
}

// Explicit template instantiations
template int32_t* mmap_column<int32_t>(const std::string&, const std::string&, const std::string&, size_t&);
template double* mmap_column<double>(const std::string&, const std::string&, const std::string&, size_t&);
template uint8_t* mmap_column<uint8_t>(const std::string&, const std::string&, const std::string&, size_t&);

std::unordered_map<uint8_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream in(dict_path);
    std::string line;
    while (std::getline(in, line)) {
        size_t comma = line.find(',');
        if (comma != std::string::npos) {
            uint8_t key = std::stoi(line.substr(0, comma));
            std::string val = line.substr(comma + 1);
            dict[key] = val;
        }
    }
    return dict;
}

std::unordered_map<uint8_t, char> load_char_dictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, char> dict;
    std::ifstream in(dict_path);
    std::string line;
    while (std::getline(in, line)) {
        size_t comma = line.find(',');
        if (comma != std::string::npos) {
            uint8_t key = std::stoi(line.substr(0, comma));
            char val = line[comma + 1];
            dict[key] = val;
        }
    }
    return dict;
}

} // namespace gendb
