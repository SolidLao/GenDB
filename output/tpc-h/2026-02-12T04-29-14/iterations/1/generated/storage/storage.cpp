#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <thread>
#include <iostream>

namespace gendb {

// Helper: write binary column file
template<typename T>
static void write_column_file(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open " + path);

    // Use 1MB write buffer
    char buffer[1 << 20];
    setvbuf(f, buffer, _IOFBF, sizeof(buffer));

    fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// Helper: write string column (length-prefixed)
static void write_string_column_file(const std::string& path, const std::vector<std::string>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open " + path);

    char buffer[1 << 20];
    setvbuf(f, buffer, _IOFBF, sizeof(buffer));

    for (const auto& s : data) {
        uint32_t len = static_cast<uint32_t>(s.size());
        fwrite(&len, sizeof(len), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: write metadata (row count)
static void write_metadata(const std::string& gendb_dir, const std::string& table_name, size_t row_count) {
    std::string meta_path = gendb_dir + "/" + table_name + ".meta";
    FILE* f = fopen(meta_path.c_str(), "w");
    if (!f) throw std::runtime_error("Cannot write metadata");
    fprintf(f, "%zu\n", row_count);
    fclose(f);
}

void write_lineitem(const std::string& gendb_dir, const LineItemTable& table) {
    write_column_file(gendb_dir + "/lineitem.l_orderkey", table.l_orderkey);
    write_column_file(gendb_dir + "/lineitem.l_partkey", table.l_partkey);
    write_column_file(gendb_dir + "/lineitem.l_suppkey", table.l_suppkey);
    write_column_file(gendb_dir + "/lineitem.l_linenumber", table.l_linenumber);
    write_column_file(gendb_dir + "/lineitem.l_quantity", table.l_quantity);
    write_column_file(gendb_dir + "/lineitem.l_extendedprice", table.l_extendedprice);
    write_column_file(gendb_dir + "/lineitem.l_discount", table.l_discount);
    write_column_file(gendb_dir + "/lineitem.l_tax", table.l_tax);
    write_column_file(gendb_dir + "/lineitem.l_returnflag", table.l_returnflag);
    write_column_file(gendb_dir + "/lineitem.l_linestatus", table.l_linestatus);
    write_column_file(gendb_dir + "/lineitem.l_shipdate", table.l_shipdate);
    write_column_file(gendb_dir + "/lineitem.l_commitdate", table.l_commitdate);
    write_column_file(gendb_dir + "/lineitem.l_receiptdate", table.l_receiptdate);
    write_string_column_file(gendb_dir + "/lineitem.l_shipinstruct", table.l_shipinstruct);
    write_string_column_file(gendb_dir + "/lineitem.l_shipmode", table.l_shipmode);
    write_string_column_file(gendb_dir + "/lineitem.l_comment", table.l_comment);
    write_metadata(gendb_dir, "lineitem", table.size());
}

void write_orders(const std::string& gendb_dir, const OrdersTable& table) {
    write_column_file(gendb_dir + "/orders.o_orderkey", table.o_orderkey);
    write_column_file(gendb_dir + "/orders.o_custkey", table.o_custkey);
    write_column_file(gendb_dir + "/orders.o_orderstatus", table.o_orderstatus);
    write_column_file(gendb_dir + "/orders.o_totalprice", table.o_totalprice);
    write_column_file(gendb_dir + "/orders.o_orderdate", table.o_orderdate);
    write_string_column_file(gendb_dir + "/orders.o_orderpriority", table.o_orderpriority);
    write_string_column_file(gendb_dir + "/orders.o_clerk", table.o_clerk);
    write_column_file(gendb_dir + "/orders.o_shippriority", table.o_shippriority);
    write_string_column_file(gendb_dir + "/orders.o_comment", table.o_comment);
    write_metadata(gendb_dir, "orders", table.size());
}

void write_customer(const std::string& gendb_dir, const CustomerTable& table) {
    write_column_file(gendb_dir + "/customer.c_custkey", table.c_custkey);
    write_string_column_file(gendb_dir + "/customer.c_name", table.c_name);
    write_string_column_file(gendb_dir + "/customer.c_address", table.c_address);
    write_column_file(gendb_dir + "/customer.c_nationkey", table.c_nationkey);
    write_string_column_file(gendb_dir + "/customer.c_phone", table.c_phone);
    write_column_file(gendb_dir + "/customer.c_acctbal", table.c_acctbal);
    write_column_file(gendb_dir + "/customer.c_mktsegment", table.c_mktsegment);
    write_string_column_file(gendb_dir + "/customer.c_comment", table.c_comment);
    write_metadata(gendb_dir, "customer", table.size());
}

void write_part(const std::string& gendb_dir, const PartTable& table) {
    write_column_file(gendb_dir + "/part.p_partkey", table.p_partkey);
    write_string_column_file(gendb_dir + "/part.p_name", table.p_name);
    write_string_column_file(gendb_dir + "/part.p_mfgr", table.p_mfgr);
    write_string_column_file(gendb_dir + "/part.p_brand", table.p_brand);
    write_string_column_file(gendb_dir + "/part.p_type", table.p_type);
    write_column_file(gendb_dir + "/part.p_size", table.p_size);
    write_string_column_file(gendb_dir + "/part.p_container", table.p_container);
    write_column_file(gendb_dir + "/part.p_retailprice", table.p_retailprice);
    write_string_column_file(gendb_dir + "/part.p_comment", table.p_comment);
    write_metadata(gendb_dir, "part", table.size());
}

void write_partsupp(const std::string& gendb_dir, const PartSuppTable& table) {
    write_column_file(gendb_dir + "/partsupp.ps_partkey", table.ps_partkey);
    write_column_file(gendb_dir + "/partsupp.ps_suppkey", table.ps_suppkey);
    write_column_file(gendb_dir + "/partsupp.ps_availqty", table.ps_availqty);
    write_column_file(gendb_dir + "/partsupp.ps_supplycost", table.ps_supplycost);
    write_string_column_file(gendb_dir + "/partsupp.ps_comment", table.ps_comment);
    write_metadata(gendb_dir, "partsupp", table.size());
}

void write_supplier(const std::string& gendb_dir, const SupplierTable& table) {
    write_column_file(gendb_dir + "/supplier.s_suppkey", table.s_suppkey);
    write_string_column_file(gendb_dir + "/supplier.s_name", table.s_name);
    write_string_column_file(gendb_dir + "/supplier.s_address", table.s_address);
    write_column_file(gendb_dir + "/supplier.s_nationkey", table.s_nationkey);
    write_string_column_file(gendb_dir + "/supplier.s_phone", table.s_phone);
    write_column_file(gendb_dir + "/supplier.s_acctbal", table.s_acctbal);
    write_string_column_file(gendb_dir + "/supplier.s_comment", table.s_comment);
    write_metadata(gendb_dir, "supplier", table.size());
}

void write_nation(const std::string& gendb_dir, const NationTable& table) {
    write_column_file(gendb_dir + "/nation.n_nationkey", table.n_nationkey);
    write_string_column_file(gendb_dir + "/nation.n_name", table.n_name);
    write_column_file(gendb_dir + "/nation.n_regionkey", table.n_regionkey);
    write_string_column_file(gendb_dir + "/nation.n_comment", table.n_comment);
    write_metadata(gendb_dir, "nation", table.size());
}

void write_region(const std::string& gendb_dir, const RegionTable& table) {
    write_column_file(gendb_dir + "/region.r_regionkey", table.r_regionkey);
    write_string_column_file(gendb_dir + "/region.r_name", table.r_name);
    write_string_column_file(gendb_dir + "/region.r_comment", table.r_comment);
    write_metadata(gendb_dir, "region", table.size());
}

// Read row count from metadata
size_t read_row_count(const std::string& gendb_dir, const std::string& table) {
    std::string meta_path = gendb_dir + "/" + table + ".meta";
    FILE* f = fopen(meta_path.c_str(), "r");
    if (!f) throw std::runtime_error("Cannot read metadata for " + table);
    size_t row_count;
    if (fscanf(f, "%zu", &row_count) != 1) {
        fclose(f);
        throw std::runtime_error("Invalid metadata format");
    }
    fclose(f);
    return row_count;
}

// mmap a typed column file
template<typename T>
const T* mmap_column(const std::string& gendb_dir, const std::string& table,
                     const std::string& column, size_t& row_count) {
    row_count = read_row_count(gendb_dir, table);
    std::string path = gendb_dir + "/" + table + "." + column;

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open " + path);

    size_t file_size = row_count * sizeof(T);
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed for " + path);
    }

    // Hint for sequential access
    madvise(addr, file_size, MADV_SEQUENTIAL);

    close(fd);  // Can close fd after mmap
    return static_cast<const T*>(addr);
}

// Explicit template instantiations
template const int32_t* mmap_column<int32_t>(const std::string&, const std::string&, const std::string&, size_t&);
template const uint8_t* mmap_column<uint8_t>(const std::string&, const std::string&, const std::string&, size_t&);
template const double* mmap_column<double>(const std::string&, const std::string&, const std::string&, size_t&);

} // namespace gendb
