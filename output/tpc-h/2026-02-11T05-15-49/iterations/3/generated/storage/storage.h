#pragma once

#include <vector>
#include <string>
#include <cstdint>

namespace gendb {

// Columnar table structures
// Each table stores columns as separate vectors

struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<uint8_t> l_returnflag;  // Char(1) stored as uint8_t
    std::vector<uint8_t> l_linestatus;  // Char(1) stored as uint8_t
    std::vector<int32_t> l_shipdate;    // Days since epoch
    std::vector<int32_t> l_commitdate;  // Days since epoch
    std::vector<int32_t> l_receiptdate; // Days since epoch
    std::vector<std::string> l_shipinstruct;
    std::vector<std::string> l_shipmode;
    std::vector<std::string> l_comment;

    size_t size() const { return l_orderkey.size(); }
};

struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;  // Char(1)
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;    // Days since epoch
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

// Forward declarations for I/O functions

// Ingestion functions (parse .tbl files → write binary columns)
void ingest_lineitem(const std::string& tbl_path, const std::string& output_dir);
void ingest_orders(const std::string& tbl_path, const std::string& output_dir);
void ingest_customer(const std::string& tbl_path, const std::string& output_dir);
void ingest_part(const std::string& tbl_path, const std::string& output_dir);
void ingest_partsupp(const std::string& tbl_path, const std::string& output_dir);
void ingest_supplier(const std::string& tbl_path, const std::string& output_dir);
void ingest_nation(const std::string& tbl_path, const std::string& output_dir);
void ingest_region(const std::string& tbl_path, const std::string& output_dir);

// Query-time functions (mmap binary columns for specific columns)
// These return raw pointers to mmap'd data and size
template<typename T>
struct MappedColumn {
    T* data;
    size_t size;
    void* mmap_ptr;   // For unmapping later
    size_t mmap_size; // For unmapping later

    MappedColumn() : data(nullptr), size(0), mmap_ptr(nullptr), mmap_size(0) {}
};

// Mmap individual columns on-demand
MappedColumn<int32_t> mmap_int32_column(const std::string& gendb_dir, const std::string& table, const std::string& column);
MappedColumn<double> mmap_double_column(const std::string& gendb_dir, const std::string& table, const std::string& column);
MappedColumn<uint8_t> mmap_uint8_column(const std::string& gendb_dir, const std::string& table, const std::string& column);

// Mmap string column (returns vector, not MappedColumn since strings are variable-length)
std::vector<std::string> mmap_string_column(const std::string& gendb_dir, const std::string& table, const std::string& column);

// Helper to unmap columns
void unmap_column(void* mmap_ptr, size_t mmap_size);

// Zone map structures for block skipping
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    size_t block_start;  // Row index where this block starts
    size_t block_size;   // Number of rows in this block
};

struct ZoneMap {
    std::vector<ZoneMapEntry> blocks;
    size_t block_granularity;  // Rows per block (typically 65536)
};

// Load zone map for a column (if exists)
ZoneMap load_zone_map(const std::string& gendb_dir, const std::string& table, const std::string& column);

// Helper: check if a range [query_min, query_max) overlaps with block [block_min, block_max]
// Block range is INCLUSIVE on both ends [block_min, block_max]
// Query range is INCLUSIVE on left, EXCLUSIVE on right [query_min, query_max)
inline bool zone_overlaps(int32_t block_min, int32_t block_max, int32_t query_min, int32_t query_max) {
    // Overlap exists if: block_max >= query_min AND block_min < query_max
    // No overlap if: block_max < query_min OR block_min >= query_max
    return (block_max >= query_min) && (block_min < query_max);
}

}  // namespace gendb
