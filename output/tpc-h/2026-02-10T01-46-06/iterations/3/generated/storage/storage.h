#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <unordered_set>
#include <unordered_map>
#include <memory>

// Columnar storage structures for TPC-H tables

struct LineItem {
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

struct Orders {
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

struct Customer {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;
    std::vector<std::string> c_mktsegment;
    std::vector<std::string> c_comment;

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

// String interning pool for low-cardinality string columns
class StringPool {
private:
    std::unordered_map<std::string, const char*> pool;
    std::vector<std::unique_ptr<char[]>> storage;

public:
    const char* intern(const std::string& str) {
        auto it = pool.find(str);
        if (it != pool.end()) return it->second;

        // Allocate new string
        auto buf = std::make_unique<char[]>(str.size() + 1);
        memcpy(buf.get(), str.data(), str.size());
        buf[str.size()] = '\0';

        const char* ptr = buf.get();
        storage.push_back(std::move(buf));
        pool[str] = ptr;
        return ptr;
    }
};

// Forward declarations for loader functions with selective column loading
void load_lineitem(const std::string& filepath, LineItem& table,
                   const std::unordered_set<std::string>& columns_to_load);
void load_orders(const std::string& filepath, Orders& table,
                 const std::unordered_set<std::string>& columns_to_load);
void load_customer(const std::string& filepath, Customer& table,
                   const std::unordered_set<std::string>& columns_to_load,
                   StringPool* string_pool = nullptr);
