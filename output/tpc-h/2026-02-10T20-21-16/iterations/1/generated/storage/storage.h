#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <memory>
#include <sys/mman.h>

namespace gendb {

// Zone map structure for block-level pruning
struct ZoneMap {
    int32_t min_value;
    int32_t max_value;
    uint32_t block_id;
    uint64_t row_offset;
    uint32_t row_count;
};

// Columnar table structures with zero-copy mmap pointers

struct LineitemTable {
    // Raw pointers to mmap'd data (zero-copy)
    const int32_t* l_orderkey;
    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const int32_t* l_linenumber;
    const double* l_quantity;
    const double* l_extendedprice;
    const double* l_discount;
    const double* l_tax;
    const uint8_t* l_returnflag;  // Dictionary-encoded
    const uint8_t* l_linestatus;  // Dictionary-encoded
    const int32_t* l_shipdate;    // Days since epoch
    const int32_t* l_commitdate;
    const int32_t* l_receiptdate;

    size_t row_count;

    // Zone maps for block-level pruning
    std::vector<ZoneMap> shipdate_zones;

    // Dictionaries for encoded columns
    std::vector<std::string> returnflag_dict;
    std::vector<std::string> linestatus_dict;
    std::unordered_map<std::string, uint8_t> returnflag_lookup;
    std::unordered_map<std::string, uint8_t> linestatus_lookup;

    // Track mmap'd regions for cleanup
    std::vector<std::pair<void*, size_t>> mmap_regions;

    LineitemTable() : l_orderkey(nullptr), l_partkey(nullptr), l_suppkey(nullptr),
                      l_linenumber(nullptr), l_quantity(nullptr), l_extendedprice(nullptr),
                      l_discount(nullptr), l_tax(nullptr), l_returnflag(nullptr),
                      l_linestatus(nullptr), l_shipdate(nullptr), l_commitdate(nullptr),
                      l_receiptdate(nullptr), row_count(0) {}

    ~LineitemTable() {
        for (auto& [ptr, size] : mmap_regions) {
            if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        }
    }

    size_t size() const { return row_count; }
};

struct OrdersTable {
    // Raw pointers to mmap'd data (zero-copy)
    const int32_t* o_orderkey;
    const int32_t* o_custkey;
    const int32_t* o_orderdate;  // Days since epoch
    const int32_t* o_shippriority;

    size_t row_count;

    // Zone maps for block-level pruning
    std::vector<ZoneMap> orderdate_zones;

    // Track mmap'd regions for cleanup
    std::vector<std::pair<void*, size_t>> mmap_regions;

    OrdersTable() : o_orderkey(nullptr), o_custkey(nullptr),
                    o_orderdate(nullptr), o_shippriority(nullptr), row_count(0) {}

    ~OrdersTable() {
        for (auto& [ptr, size] : mmap_regions) {
            if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        }
    }

    size_t size() const { return row_count; }
};

struct CustomerTable {
    // Raw pointers to mmap'd data (zero-copy)
    const int32_t* c_custkey;
    const uint8_t* c_mktsegment;  // Dictionary-encoded

    size_t row_count;

    // Dictionary for encoded columns
    std::vector<std::string> mktsegment_dict;
    std::unordered_map<std::string, uint8_t> mktsegment_lookup;

    // Track mmap'd regions for cleanup
    std::vector<std::pair<void*, size_t>> mmap_regions;

    CustomerTable() : c_custkey(nullptr), c_mktsegment(nullptr), row_count(0) {}

    ~CustomerTable() {
        for (auto& [ptr, size] : mmap_regions) {
            if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        }
    }

    size_t size() const { return row_count; }
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
