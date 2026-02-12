#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <memory>

namespace gendb {
namespace storage {

// Table structures with columnar layout
struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<char> l_returnflag;
    std::vector<char> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<std::string> l_shipinstruct;
    std::vector<std::string> l_shipmode;
    std::vector<std::string> l_comment;

    size_t size() const { return l_orderkey.size(); }
};

struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<char> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<std::string> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    size_t size() const { return o_orderkey.size(); }
};

struct CustomerTable {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;
    std::vector<std::string> c_mktsegment;
    std::vector<std::string> c_comment;

    size_t size() const { return c_custkey.size(); }
};

struct NationTable {
    std::vector<int32_t> n_nationkey;
    std::vector<std::string> n_name;
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;

    size_t size() const { return n_nationkey.size(); }
};

struct RegionTable {
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;

    size_t size() const { return r_regionkey.size(); }
};

struct SupplierTable {
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<double> s_acctbal;
    std::vector<std::string> s_comment;

    size_t size() const { return s_suppkey.size(); }
};

struct PartTable {
    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<std::string> p_mfgr;
    std::vector<std::string> p_brand;
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<std::string> p_container;
    std::vector<double> p_retailprice;
    std::vector<std::string> p_comment;

    size_t size() const { return p_partkey.size(); }
};

struct PartsuppTable {
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<double> ps_supplycost;
    std::vector<std::string> ps_comment;

    size_t size() const { return ps_partkey.size(); }
};

// Ingestion functions: parse .tbl files and write binary columns
void ingest_lineitem(const std::string& tbl_file, const std::string& output_dir);
void ingest_orders(const std::string& tbl_file, const std::string& output_dir);
void ingest_customer(const std::string& tbl_file, const std::string& output_dir);
void ingest_nation(const std::string& tbl_file, const std::string& output_dir);
void ingest_region(const std::string& tbl_file, const std::string& output_dir);
void ingest_supplier(const std::string& tbl_file, const std::string& output_dir);
void ingest_part(const std::string& tbl_file, const std::string& output_dir);
void ingest_partsupp(const std::string& tbl_file, const std::string& output_dir);

// Query-time loading: mmap binary columns on-demand
// Returns raw pointer to mmap'd data (caller must munmap)
template<typename T>
T* mmap_column(const std::string& file_path, size_t& num_elements);

// Helper to read metadata
size_t read_row_count(const std::string& metadata_file);

} // namespace storage
} // namespace gendb
