#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

namespace gendb {

// Table structures with columnar layout
struct LineItemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<uint8_t> l_returnflag;  // Dictionary-encoded
    std::vector<uint8_t> l_linestatus;  // Dictionary-encoded
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
    std::vector<uint8_t> o_orderstatus;  // Dictionary-encoded
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
    std::vector<uint8_t> c_mktsegment;  // Dictionary-encoded
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

struct PartSuppTable {
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

// Dictionary for encoding/decoding
struct Dictionary {
    std::vector<std::string> values;

    uint8_t encode(const std::string& s) {
        for (size_t i = 0; i < values.size(); i++) {
            if (values[i] == s) return static_cast<uint8_t>(i);
        }
        values.push_back(s);
        return static_cast<uint8_t>(values.size() - 1);
    }

    const std::string& decode(uint8_t code) const {
        return values[code];
    }
};

// Function declarations for ingestion (write binary)
void write_lineitem(const std::string& gendb_dir, const LineItemTable& table);
void write_orders(const std::string& gendb_dir, const OrdersTable& table);
void write_customer(const std::string& gendb_dir, const CustomerTable& table);
void write_part(const std::string& gendb_dir, const PartTable& table);
void write_partsupp(const std::string& gendb_dir, const PartSuppTable& table);
void write_supplier(const std::string& gendb_dir, const SupplierTable& table);
void write_nation(const std::string& gendb_dir, const NationTable& table);
void write_region(const std::string& gendb_dir, const RegionTable& table);

// Function declarations for query execution (mmap columns on-demand)
template<typename T>
const T* mmap_column(const std::string& gendb_dir, const std::string& table,
                     const std::string& column, size_t& row_count);

size_t read_row_count(const std::string& gendb_dir, const std::string& table);

} // namespace gendb
