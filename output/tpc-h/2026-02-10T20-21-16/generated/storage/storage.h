#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <memory>

namespace gendb {

// Columnar table structures with dictionary encoding support

struct LineitemTable {
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
    std::vector<int32_t> l_shipdate;    // Days since epoch
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<std::string> l_shipinstruct;
    std::vector<std::string> l_shipmode;
    std::vector<std::string> l_comment;

    // Dictionaries for encoded columns
    std::vector<std::string> returnflag_dict;
    std::vector<std::string> linestatus_dict;
    std::unordered_map<std::string, uint8_t> returnflag_lookup;
    std::unordered_map<std::string, uint8_t> linestatus_lookup;

    size_t size() const { return l_orderkey.size(); }
};

struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<std::string> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;  // Days since epoch
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

    // Dictionary for encoded columns
    std::vector<std::string> mktsegment_dict;
    std::unordered_map<std::string, uint8_t> mktsegment_lookup;

    size_t size() const { return c_custkey.size(); }
};

// Forward declarations for ingestion/loading functions
void ingest_lineitem(const std::string& tbl_file, const std::string& gendb_dir);
void ingest_orders(const std::string& tbl_file, const std::string& gendb_dir);
void ingest_customer(const std::string& tbl_file, const std::string& gendb_dir);

void load_lineitem(const std::string& gendb_dir, LineitemTable& table,
                   const std::vector<std::string>& columns_needed);
void load_orders(const std::string& gendb_dir, OrdersTable& table,
                 const std::vector<std::string>& columns_needed);
void load_customer(const std::string& gendb_dir, CustomerTable& table,
                   const std::vector<std::string>& columns_needed);

} // namespace gendb
