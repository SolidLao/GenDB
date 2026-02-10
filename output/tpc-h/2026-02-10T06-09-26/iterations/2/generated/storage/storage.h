#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace gendb {

// Zone map for block-level pruning
struct ZoneMap {
    std::vector<int32_t> block_min;
    std::vector<int32_t> block_max;
    size_t block_size = 65536;
};

// Lineitem table - columnar storage with dictionary encoding
struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;

    // Dictionary-encoded columns (replaces string vectors)
    std::vector<uint8_t> l_returnflag_code;
    std::vector<std::string> l_returnflag_dict;
    std::unordered_map<std::string, uint8_t> l_returnflag_lookup;

    std::vector<uint8_t> l_linestatus_code;
    std::vector<std::string> l_linestatus_dict;
    std::unordered_map<std::string, uint8_t> l_linestatus_lookup;

    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<std::string> l_shipinstruct;
    std::vector<std::string> l_shipmode;
    std::vector<std::string> l_comment;

    // Zone map for l_shipdate (data will be sorted by shipdate)
    ZoneMap shipdate_zonemap;

    size_t size() const { return l_orderkey.size(); }
};

// Orders table - columnar storage with zone maps
struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<std::string> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<std::string> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    // Zone map for o_orderdate (data will be sorted by orderdate)
    ZoneMap orderdate_zonemap;

    size_t size() const { return o_orderkey.size(); }
};

// Customer table - columnar storage with dictionary encoding
struct CustomerTable {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;

    // Dictionary-encoded c_mktsegment
    std::vector<uint8_t> c_mktsegment_code;
    std::vector<std::string> c_mktsegment_dict;
    std::unordered_map<std::string, uint8_t> c_mktsegment_lookup;

    std::vector<std::string> c_comment;

    size_t size() const { return c_custkey.size(); }
};

// Loader functions
void load_lineitem(const std::string& filepath, LineitemTable& table);
void load_orders(const std::string& filepath, OrdersTable& table);
void load_customer(const std::string& filepath, CustomerTable& table);

} // namespace gendb
