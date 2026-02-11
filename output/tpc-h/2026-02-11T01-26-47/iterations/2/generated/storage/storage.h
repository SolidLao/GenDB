#pragma once

#include <vector>
#include <string>
#include <cstdint>

namespace gendb {

// Zone map metadata for block skipping
struct ZoneMap {
    int32_t min_value;
    int32_t max_value;
    size_t block_start_row;
    size_t block_row_count;
};

// Columnar table structures (only accessed columns for this workload)

struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<char> l_returnflag;
    std::vector<char> l_linestatus;
    std::vector<int32_t> l_shipdate;

    // Zone maps for block skipping
    std::vector<ZoneMap> l_shipdate_zone_maps;

    size_t size() const { return l_orderkey.size(); }

    void reserve(size_t n) {
        l_orderkey.reserve(n);
        l_quantity.reserve(n);
        l_extendedprice.reserve(n);
        l_discount.reserve(n);
        l_tax.reserve(n);
        l_returnflag.reserve(n);
        l_linestatus.reserve(n);
        l_shipdate.reserve(n);
    }
};

struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<int32_t> o_orderdate;
    std::vector<int32_t> o_shippriority;

    // Zone maps for block skipping
    std::vector<ZoneMap> o_orderdate_zone_maps;

    size_t size() const { return o_orderkey.size(); }

    void reserve(size_t n) {
        o_orderkey.reserve(n);
        o_custkey.reserve(n);
        o_orderdate.reserve(n);
        o_shippriority.reserve(n);
    }
};

struct CustomerTable {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_mktsegment;

    size_t size() const { return c_custkey.size(); }

    void reserve(size_t n) {
        c_custkey.reserve(n);
        c_mktsegment.reserve(n);
    }
};

// Write functions (used by ingest)
void write_lineitem(const std::string& gendb_dir, const LineitemTable& table);
void write_orders(const std::string& gendb_dir, const OrdersTable& table);
void write_customer(const std::string& gendb_dir, const CustomerTable& table);

// Read functions (used by main)
LineitemTable read_lineitem(const std::string& gendb_dir);
OrdersTable read_orders(const std::string& gendb_dir);
CustomerTable read_customer(const std::string& gendb_dir);

// Ingestion functions (parse .tbl files)
LineitemTable ingest_lineitem_tbl(const std::string& tbl_path);
OrdersTable ingest_orders_tbl(const std::string& tbl_path);
CustomerTable ingest_customer_tbl(const std::string& tbl_path);

} // namespace gendb
