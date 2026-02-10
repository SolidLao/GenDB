#pragma once

#include <vector>
#include <string>
#include <cstdint>

// Columnar storage structures for TPC-H tables
// Each table uses struct-of-arrays (SoA) layout for cache efficiency

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
    std::vector<uint8_t> l_returnflag_code;  // Dictionary-encoded returnflag
    std::vector<uint8_t> l_linestatus_code;  // Dictionary-encoded linestatus
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
    std::vector<uint8_t> c_mktsegment_code;  // Dictionary-encoded market segment
    std::vector<std::string> c_comment;

    size_t size() const { return c_custkey.size(); }
};

// Loader functions
void load_lineitem(const std::string& filepath, LineitemTable& table);
void load_orders(const std::string& filepath, OrdersTable& table);
void load_customer(const std::string& filepath, CustomerTable& table);
