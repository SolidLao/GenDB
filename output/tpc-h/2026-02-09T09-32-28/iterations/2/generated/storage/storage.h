#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>

// Columnar storage structures for TPC-H tables

struct LineitemTable {
    // Only load columns used in workload (Q1, Q3, Q6)
    std::vector<int32_t> l_orderkey;      // Used in Q3
    // l_partkey - SKIPPED (unused)
    // l_suppkey - SKIPPED (unused)
    // l_linenumber - SKIPPED (unused)
    std::vector<double> l_quantity;       // Used in Q1, Q6
    std::vector<double> l_extendedprice;  // Used in Q1, Q3, Q6
    std::vector<double> l_discount;       // Used in Q1, Q3, Q6
    std::vector<double> l_tax;            // Used in Q1
    std::vector<uint8_t> l_returnflag;    // Used in Q1 - integer encoded (A->0, N->1, R->2)
    std::vector<uint8_t> l_linestatus;    // Used in Q1 - integer encoded (F->0, O->1)
    std::vector<int32_t> l_shipdate;      // Used in Q1, Q3, Q6
    // l_commitdate - SKIPPED (unused)
    // l_receiptdate - SKIPPED (unused)
    // l_shipinstruct - SKIPPED (unused)
    // l_shipmode - SKIPPED (unused)
    // l_comment - SKIPPED (unused)

    // Sorted index on l_shipdate for efficient range scans
    // Contains row indices sorted by l_shipdate (ascending)
    std::vector<size_t> l_shipdate_sorted_index;

    size_t size() const { return l_orderkey.size(); }
};

struct CustomerTable {
    // Only load columns used in workload (Q3)
    std::vector<int32_t> c_custkey;       // Used in Q3
    // c_name - SKIPPED (unused)
    // c_address - SKIPPED (unused)
    // c_nationkey - SKIPPED (unused)
    // c_phone - SKIPPED (unused)
    // c_acctbal - SKIPPED (unused)
    std::vector<std::string> c_mktsegment; // Used in Q3
    // c_comment - SKIPPED (unused)

    // Pre-built hash index: c_custkey -> row index
    std::unordered_map<int32_t, size_t> c_custkey_index;

    size_t size() const { return c_custkey.size(); }
};

struct OrdersTable {
    // Only load columns used in workload (Q3)
    std::vector<int32_t> o_orderkey;      // Used in Q3
    std::vector<int32_t> o_custkey;       // Used in Q3
    // o_orderstatus - SKIPPED (unused)
    // o_totalprice - SKIPPED (unused)
    std::vector<int32_t> o_orderdate;     // Used in Q3
    // o_orderpriority - SKIPPED (unused)
    // o_clerk - SKIPPED (unused)
    std::vector<int32_t> o_shippriority;  // Used in Q3
    // o_comment - SKIPPED (unused)

    // Pre-built hash indexes for Q3
    std::unordered_map<int32_t, size_t> o_orderkey_index;  // o_orderkey -> row index
    std::unordered_multimap<int32_t, size_t> o_custkey_index;  // o_custkey -> row index(es)

    size_t size() const { return o_orderkey.size(); }
};

// Loader function declarations
void load_lineitem(const std::string& filepath, LineitemTable& table);
void load_customer(const std::string& filepath, CustomerTable& table);
void load_orders(const std::string& filepath, OrdersTable& table);
