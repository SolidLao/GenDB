#ifndef STORAGE_H
#define STORAGE_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

// Column data structures for all TPC-H tables
// Columnar storage: one vector per column, binary serialization

namespace storage {

// Dictionary for encoding low-cardinality string columns
struct Dictionary {
    std::vector<std::string> values;
    std::unordered_map<std::string, uint8_t> value_to_code;

    uint8_t encode(const std::string& s) {
        auto it = value_to_code.find(s);
        if (it != value_to_code.end()) {
            return it->second;
        }
        uint8_t code = static_cast<uint8_t>(values.size());
        values.push_back(s);
        value_to_code[s] = code;
        return code;
    }

    const std::string& decode(uint8_t code) const {
        return values[code];
    }
};

// LINEITEM table columns (~60M rows at SF=10)
struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;       // scaled × 100
    std::vector<int64_t> l_extendedprice;  // scaled × 100
    std::vector<int64_t> l_discount;       // scaled × 100
    std::vector<int64_t> l_tax;            // scaled × 100
    std::vector<uint8_t> l_returnflag;     // dictionary encoded
    std::vector<uint8_t> l_linestatus;     // dictionary encoded
    std::vector<int32_t> l_shipdate;       // days since epoch
    std::vector<int32_t> l_commitdate;     // days since epoch
    std::vector<int32_t> l_receiptdate;    // days since epoch
    std::vector<uint8_t> l_shipinstruct;   // dictionary encoded
    std::vector<uint8_t> l_shipmode;       // dictionary encoded
    std::vector<std::string> l_comment;

    Dictionary dict_returnflag;
    Dictionary dict_linestatus;
    Dictionary dict_shipinstruct;
    Dictionary dict_shipmode;

    size_t size() const { return l_orderkey.size(); }

    void reserve(size_t n) {
        l_orderkey.reserve(n);
        l_partkey.reserve(n);
        l_suppkey.reserve(n);
        l_linenumber.reserve(n);
        l_quantity.reserve(n);
        l_extendedprice.reserve(n);
        l_discount.reserve(n);
        l_tax.reserve(n);
        l_returnflag.reserve(n);
        l_linestatus.reserve(n);
        l_shipdate.reserve(n);
        l_commitdate.reserve(n);
        l_receiptdate.reserve(n);
        l_shipinstruct.reserve(n);
        l_shipmode.reserve(n);
        l_comment.reserve(n);
    }
};

// ORDERS table columns (~15M rows at SF=10)
struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;    // dictionary encoded
    std::vector<int64_t> o_totalprice;     // scaled × 100
    std::vector<int32_t> o_orderdate;      // days since epoch
    std::vector<std::string> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    Dictionary dict_orderstatus;

    size_t size() const { return o_orderkey.size(); }

    void reserve(size_t n) {
        o_orderkey.reserve(n);
        o_custkey.reserve(n);
        o_orderstatus.reserve(n);
        o_totalprice.reserve(n);
        o_orderdate.reserve(n);
        o_orderpriority.reserve(n);
        o_clerk.reserve(n);
        o_shippriority.reserve(n);
        o_comment.reserve(n);
    }
};

// CUSTOMER table columns (~1.5M rows at SF=10)
struct CustomerTable {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<int64_t> c_acctbal;        // scaled × 100
    std::vector<uint8_t> c_mktsegment;     // dictionary encoded
    std::vector<std::string> c_comment;

    Dictionary dict_mktsegment;

    size_t size() const { return c_custkey.size(); }

    void reserve(size_t n) {
        c_custkey.reserve(n);
        c_name.reserve(n);
        c_address.reserve(n);
        c_nationkey.reserve(n);
        c_phone.reserve(n);
        c_acctbal.reserve(n);
        c_mktsegment.reserve(n);
        c_comment.reserve(n);
    }
};

// PART table columns (~2M rows at SF=10)
struct PartTable {
    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<std::string> p_mfgr;
    std::vector<uint8_t> p_brand;          // dictionary encoded
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<uint8_t> p_container;      // dictionary encoded
    std::vector<int64_t> p_retailprice;    // scaled × 100
    std::vector<std::string> p_comment;

    Dictionary dict_brand;
    Dictionary dict_container;

    size_t size() const { return p_partkey.size(); }

    void reserve(size_t n) {
        p_partkey.reserve(n);
        p_name.reserve(n);
        p_mfgr.reserve(n);
        p_brand.reserve(n);
        p_type.reserve(n);
        p_size.reserve(n);
        p_container.reserve(n);
        p_retailprice.reserve(n);
        p_comment.reserve(n);
    }
};

// PARTSUPP table columns (~8M rows at SF=10)
struct PartsuppTable {
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<int64_t> ps_supplycost;    // scaled × 100
    std::vector<std::string> ps_comment;

    size_t size() const { return ps_partkey.size(); }

    void reserve(size_t n) {
        ps_partkey.reserve(n);
        ps_suppkey.reserve(n);
        ps_availqty.reserve(n);
        ps_supplycost.reserve(n);
        ps_comment.reserve(n);
    }
};

// SUPPLIER table columns (~100K rows at SF=10)
struct SupplierTable {
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<int64_t> s_acctbal;        // scaled × 100
    std::vector<std::string> s_comment;

    size_t size() const { return s_suppkey.size(); }

    void reserve(size_t n) {
        s_suppkey.reserve(n);
        s_name.reserve(n);
        s_address.reserve(n);
        s_nationkey.reserve(n);
        s_phone.reserve(n);
        s_acctbal.reserve(n);
        s_comment.reserve(n);
    }
};

// NATION table columns (25 rows)
struct NationTable {
    std::vector<int32_t> n_nationkey;
    std::vector<std::string> n_name;
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;

    size_t size() const { return n_nationkey.size(); }
};

// REGION table columns (5 rows)
struct RegionTable {
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;

    size_t size() const { return r_regionkey.size(); }
};

// Binary I/O functions (implementation in storage.cpp)
void write_column_int32(const std::string& path, const std::vector<int32_t>& data);
void write_column_int64(const std::string& path, const std::vector<int64_t>& data);
void write_column_uint8(const std::string& path, const std::vector<uint8_t>& data);
void write_column_string(const std::string& path, const std::vector<std::string>& data);
void write_dictionary(const std::string& path, const Dictionary& dict);

std::vector<int32_t> read_column_int32(const std::string& path);
std::vector<int64_t> read_column_int64(const std::string& path);
std::vector<uint8_t> read_column_uint8(const std::string& path);
std::vector<std::string> read_column_string(const std::string& path);
Dictionary read_dictionary(const std::string& path);

// Memory-mapped read (for queries - zero-copy)
struct MappedColumn {
    void* data;
    size_t size;
    int fd;

    MappedColumn() : data(nullptr), size(0), fd(-1) {}
    ~MappedColumn();

    void open(const std::string& path);
    void close();

    template<typename T>
    const T* as() const { return static_cast<const T*>(data); }

    template<typename T>
    size_t count() const { return size / sizeof(T); }
};

} // namespace storage

#endif // STORAGE_H
