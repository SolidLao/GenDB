#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

struct StringColumn {
    std::vector<uint64_t> offsets{0};
    std::vector<char> data;

    void reserve(size_t rows, size_t chars) {
        offsets.reserve(rows + 1);
        data.reserve(chars);
    }

    void append(std::string_view value) {
        data.insert(data.end(), value.begin(), value.end());
        offsets.push_back(static_cast<uint64_t>(data.size()));
    }
};

template <typename CodeT>
struct DictColumn {
    std::unordered_map<std::string, CodeT> code_by_value;
    std::vector<std::string> dictionary;
    std::vector<CodeT> codes;

    void reserve(size_t rows, size_t distinct_hint = 0) {
        codes.reserve(rows);
        if (distinct_hint != 0) {
            dictionary.reserve(distinct_hint);
            code_by_value.reserve(distinct_hint * 2);
        }
    }

    void append(std::string_view value) {
        std::string key(value);
        auto it = code_by_value.find(key);
        if (it == code_by_value.end()) {
            CodeT code = static_cast<CodeT>(dictionary.size());
            dictionary.push_back(key);
            code_by_value.emplace(dictionary.back(), code);
            codes.push_back(code);
        } else {
            codes.push_back(it->second);
        }
    }
};

template <typename T>
void reserve_if_possible(std::vector<T>& values, size_t count) {
    values.reserve(count);
}

template <typename T>
void write_vector_file(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
}

void write_u64_value(const fs::path& path, uint64_t value) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void write_string_column(const fs::path& dir, const std::string& name, const StringColumn& col) {
    write_vector_file(dir / (name + ".offsets.bin"), col.offsets);
    std::ofstream out(dir / (name + ".data.bin"), std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open string data file for " + name);
    }
    if (!col.data.empty()) {
        out.write(col.data.data(), static_cast<std::streamsize>(col.data.size()));
    }
}

template <typename CodeT>
void write_dict_column(const fs::path& dir, const std::string& name, const DictColumn<CodeT>& col) {
    write_vector_file(dir / (name + ".bin"), col.codes);

    StringColumn dict_values;
    size_t chars = 0;
    for (const auto& entry : col.dictionary) {
        chars += entry.size();
    }
    dict_values.reserve(col.dictionary.size(), chars);
    for (const auto& entry : col.dictionary) {
        dict_values.append(entry);
    }
    write_string_column(dir, name + ".dict", dict_values);
}

static inline std::string_view next_field(const char*& p) {
    const char* start = p;
    while (*p != '|' && *p != '\0' && *p != '\n' && *p != '\r') {
        ++p;
    }
    const char* end = p;
    if (*p == '|') {
        ++p;
    }
    return std::string_view(start, static_cast<size_t>(end - start));
}

int32_t parse_int32(std::string_view sv) {
    int32_t value = 0;
    auto result = std::from_chars(sv.data(), sv.data() + sv.size(), value);
    if (result.ec != std::errc()) {
        throw std::runtime_error("invalid int32 field");
    }
    return value;
}

double parse_double(std::string_view sv) {
    std::string tmp(sv);
    char* end = nullptr;
    double value = std::strtod(tmp.c_str(), &end);
    if (end != tmp.c_str() + static_cast<std::ptrdiff_t>(tmp.size())) {
        throw std::runtime_error("invalid double field");
    }
    return value;
}

int32_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? static_cast<unsigned>(-3) : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

int32_t parse_date(std::string_view sv) {
    if (sv.size() != 10 || sv[4] != '-' || sv[7] != '-') {
        throw std::runtime_error("invalid date field");
    }
    int year = (sv[0] - '0') * 1000 + (sv[1] - '0') * 100 + (sv[2] - '0') * 10 + (sv[3] - '0');
    unsigned month = static_cast<unsigned>((sv[5] - '0') * 10 + (sv[6] - '0'));
    unsigned day = static_cast<unsigned>((sv[8] - '0') * 10 + (sv[9] - '0'));
    return days_from_civil(year, month, day);
}

void ensure_dir(const fs::path& path) {
    fs::create_directories(path);
}

void write_row_count(const fs::path& dir, uint64_t row_count) {
    write_u64_value(dir / "row_count.bin", row_count);
}

void parse_nation(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "nation";
    ensure_dir(table_dir);

    std::vector<int32_t> n_nationkey;
    std::vector<int32_t> n_regionkey;
    StringColumn n_comment;
    DictColumn<uint32_t> n_name;

    n_nationkey.reserve(25);
    n_regionkey.reserve(25);
    n_comment.reserve(25, 2048);
    n_name.reserve(25, 25);

    std::ifstream in(src_dir / "nation.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        n_nationkey.push_back(parse_int32(next_field(p)));
        n_name.append(next_field(p));
        n_regionkey.push_back(parse_int32(next_field(p)));
        n_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "n_nationkey.bin", n_nationkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "n_name", n_name); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "n_regionkey.bin", n_regionkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "n_comment", n_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, n_nationkey.size());
}

void parse_region(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "region";
    ensure_dir(table_dir);

    std::vector<int32_t> r_regionkey;
    StringColumn r_comment;
    DictColumn<uint32_t> r_name;

    r_regionkey.reserve(5);
    r_comment.reserve(5, 512);
    r_name.reserve(5, 5);

    std::ifstream in(src_dir / "region.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        r_regionkey.push_back(parse_int32(next_field(p)));
        r_name.append(next_field(p));
        r_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "r_regionkey.bin", r_regionkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "r_name", r_name); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "r_comment", r_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, r_regionkey.size());
}

void parse_supplier(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "supplier";
    ensure_dir(table_dir);

    const size_t rows = 100000;
    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<double> s_acctbal;
    StringColumn s_name, s_address, s_phone, s_comment;

    reserve_if_possible(s_suppkey, rows);
    reserve_if_possible(s_nationkey, rows);
    reserve_if_possible(s_acctbal, rows);
    s_name.reserve(rows, rows * 25);
    s_address.reserve(rows, rows * 40);
    s_phone.reserve(rows, rows * 15);
    s_comment.reserve(rows, rows * 101);

    std::ifstream in(src_dir / "supplier.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        s_suppkey.push_back(parse_int32(next_field(p)));
        s_name.append(next_field(p));
        s_address.append(next_field(p));
        s_nationkey.push_back(parse_int32(next_field(p)));
        s_phone.append(next_field(p));
        s_acctbal.push_back(parse_double(next_field(p)));
        s_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "s_suppkey.bin", s_suppkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "s_name", s_name); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "s_address", s_address); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "s_nationkey.bin", s_nationkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "s_phone", s_phone); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "s_acctbal.bin", s_acctbal); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "s_comment", s_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, s_suppkey.size());
}

void parse_part(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "part";
    ensure_dir(table_dir);

    const size_t rows = 2000000;
    std::vector<int32_t> p_partkey, p_size;
    std::vector<double> p_retailprice;
    StringColumn p_name, p_comment;
    DictColumn<uint32_t> p_mfgr, p_brand, p_type, p_container;

    reserve_if_possible(p_partkey, rows);
    reserve_if_possible(p_size, rows);
    reserve_if_possible(p_retailprice, rows);
    p_name.reserve(rows, rows * 55);
    p_comment.reserve(rows, rows * 23);
    p_mfgr.reserve(rows, 16);
    p_brand.reserve(rows, 64);
    p_type.reserve(rows, 96);
    p_container.reserve(rows, 48);

    std::ifstream in(src_dir / "part.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        p_partkey.push_back(parse_int32(next_field(p)));
        p_name.append(next_field(p));
        p_mfgr.append(next_field(p));
        p_brand.append(next_field(p));
        p_type.append(next_field(p));
        p_size.push_back(parse_int32(next_field(p)));
        p_container.append(next_field(p));
        p_retailprice.push_back(parse_double(next_field(p)));
        p_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "p_partkey.bin", p_partkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "p_name", p_name); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "p_mfgr", p_mfgr); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "p_brand", p_brand); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "p_type", p_type); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "p_size.bin", p_size); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "p_container", p_container); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "p_retailprice.bin", p_retailprice); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "p_comment", p_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, p_partkey.size());
}

void parse_partsupp(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "partsupp";
    ensure_dir(table_dir);

    const size_t rows = 8000000;
    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<double> ps_supplycost;
    StringColumn ps_comment;

    reserve_if_possible(ps_partkey, rows);
    reserve_if_possible(ps_suppkey, rows);
    reserve_if_possible(ps_availqty, rows);
    reserve_if_possible(ps_supplycost, rows);
    ps_comment.reserve(rows, rows * 64);

    std::ifstream in(src_dir / "partsupp.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        ps_partkey.push_back(parse_int32(next_field(p)));
        ps_suppkey.push_back(parse_int32(next_field(p)));
        ps_availqty.push_back(parse_int32(next_field(p)));
        ps_supplycost.push_back(parse_double(next_field(p)));
        ps_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "ps_partkey.bin", ps_partkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "ps_suppkey.bin", ps_suppkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "ps_availqty.bin", ps_availqty); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "ps_supplycost.bin", ps_supplycost); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "ps_comment", ps_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, ps_partkey.size());
}

void parse_customer(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "customer";
    ensure_dir(table_dir);

    const size_t rows = 1500000;
    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<double> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    StringColumn c_name, c_address, c_phone, c_comment;
    DictColumn<uint8_t> c_mktsegment_dict;

    reserve_if_possible(c_custkey, rows);
    reserve_if_possible(c_nationkey, rows);
    reserve_if_possible(c_acctbal, rows);
    c_name.reserve(rows, rows * 25);
    c_address.reserve(rows, rows * 40);
    c_phone.reserve(rows, rows * 15);
    c_comment.reserve(rows, rows * 64);
    c_mktsegment_dict.reserve(rows, 8);

    std::ifstream in(src_dir / "customer.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        c_custkey.push_back(parse_int32(next_field(p)));
        c_name.append(next_field(p));
        c_address.append(next_field(p));
        c_nationkey.push_back(parse_int32(next_field(p)));
        c_phone.append(next_field(p));
        c_acctbal.push_back(parse_double(next_field(p)));
        c_mktsegment_dict.append(next_field(p));
        c_comment.append(next_field(p));
    }
    c_mktsegment = std::move(c_mktsegment_dict.codes);

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "c_custkey.bin", c_custkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "c_name", c_name); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "c_address", c_address); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "c_nationkey.bin", c_nationkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "c_phone", c_phone); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "c_acctbal.bin", c_acctbal); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "c_mktsegment.bin", c_mktsegment); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "c_mktsegment.dict", [&]() {
        StringColumn dict_values;
        size_t chars = 0;
        for (const auto& v : c_mktsegment_dict.dictionary) chars += v.size();
        dict_values.reserve(c_mktsegment_dict.dictionary.size(), chars);
        for (const auto& v : c_mktsegment_dict.dictionary) dict_values.append(v);
        return dict_values;
    }()); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "c_comment", c_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, c_custkey.size());
}

void parse_orders(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "orders";
    ensure_dir(table_dir);

    const size_t rows = 15000000;
    std::vector<int32_t> o_orderkey, o_custkey, o_shippriority, o_orderdate;
    std::vector<uint8_t> o_orderstatus;
    std::vector<double> o_totalprice;
    StringColumn o_clerk, o_comment;
    DictColumn<uint32_t> o_orderpriority;

    reserve_if_possible(o_orderkey, rows);
    reserve_if_possible(o_custkey, rows);
    reserve_if_possible(o_shippriority, rows);
    reserve_if_possible(o_orderdate, rows);
    reserve_if_possible(o_orderstatus, rows);
    reserve_if_possible(o_totalprice, rows);
    o_clerk.reserve(rows, rows * 15);
    o_comment.reserve(rows, rows * 79);
    o_orderpriority.reserve(rows, 8);

    std::ifstream in(src_dir / "orders.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        o_orderkey.push_back(parse_int32(next_field(p)));
        o_custkey.push_back(parse_int32(next_field(p)));
        auto status = next_field(p);
        o_orderstatus.push_back(status.empty() ? 0 : static_cast<uint8_t>(status[0]));
        o_totalprice.push_back(parse_double(next_field(p)));
        o_orderdate.push_back(parse_date(next_field(p)));
        o_orderpriority.append(next_field(p));
        o_clerk.append(next_field(p));
        o_shippriority.push_back(parse_int32(next_field(p)));
        o_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_orderkey.bin", o_orderkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_custkey.bin", o_custkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_orderstatus.bin", o_orderstatus); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_totalprice.bin", o_totalprice); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_orderdate.bin", o_orderdate); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "o_orderpriority", o_orderpriority); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "o_clerk", o_clerk); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "o_shippriority.bin", o_shippriority); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "o_comment", o_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, o_orderkey.size());
}

void parse_lineitem(const fs::path& src_dir, const fs::path& out_dir) {
    const fs::path table_dir = out_dir / "lineitem";
    ensure_dir(table_dir);

    const size_t rows = 59986052;
    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber, l_shipdate, l_commitdate, l_receiptdate;
    std::vector<double> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    StringColumn l_comment;
    DictColumn<uint32_t> l_shipinstruct, l_shipmode;

    reserve_if_possible(l_orderkey, rows);
    reserve_if_possible(l_partkey, rows);
    reserve_if_possible(l_suppkey, rows);
    reserve_if_possible(l_linenumber, rows);
    reserve_if_possible(l_shipdate, rows);
    reserve_if_possible(l_commitdate, rows);
    reserve_if_possible(l_receiptdate, rows);
    reserve_if_possible(l_quantity, rows);
    reserve_if_possible(l_extendedprice, rows);
    reserve_if_possible(l_discount, rows);
    reserve_if_possible(l_tax, rows);
    reserve_if_possible(l_returnflag, rows);
    reserve_if_possible(l_linestatus, rows);
    l_comment.reserve(rows, rows * 32);
    l_shipinstruct.reserve(rows, 8);
    l_shipmode.reserve(rows, 8);

    std::ifstream in(src_dir / "lineitem.tbl");
    std::string line;
    while (std::getline(in, line)) {
        const char* p = line.c_str();
        l_orderkey.push_back(parse_int32(next_field(p)));
        l_partkey.push_back(parse_int32(next_field(p)));
        l_suppkey.push_back(parse_int32(next_field(p)));
        l_linenumber.push_back(parse_int32(next_field(p)));
        l_quantity.push_back(parse_double(next_field(p)));
        l_extendedprice.push_back(parse_double(next_field(p)));
        l_discount.push_back(parse_double(next_field(p)));
        l_tax.push_back(parse_double(next_field(p)));
        auto returnflag = next_field(p);
        auto linestatus = next_field(p);
        l_returnflag.push_back(returnflag.empty() ? 0 : static_cast<uint8_t>(returnflag[0]));
        l_linestatus.push_back(linestatus.empty() ? 0 : static_cast<uint8_t>(linestatus[0]));
        l_shipdate.push_back(parse_date(next_field(p)));
        l_commitdate.push_back(parse_date(next_field(p)));
        l_receiptdate.push_back(parse_date(next_field(p)));
        l_shipinstruct.append(next_field(p));
        l_shipmode.append(next_field(p));
        l_comment.append(next_field(p));
    }

    std::vector<std::future<void>> writes;
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_orderkey.bin", l_orderkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_partkey.bin", l_partkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_suppkey.bin", l_suppkey); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_linenumber.bin", l_linenumber); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_quantity.bin", l_quantity); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_extendedprice.bin", l_extendedprice); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_discount.bin", l_discount); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_tax.bin", l_tax); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_returnflag.bin", l_returnflag); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_linestatus.bin", l_linestatus); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_shipdate.bin", l_shipdate); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_commitdate.bin", l_commitdate); }));
    writes.push_back(std::async(std::launch::async, [&] { write_vector_file(table_dir / "l_receiptdate.bin", l_receiptdate); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "l_shipinstruct", l_shipinstruct); }));
    writes.push_back(std::async(std::launch::async, [&] { write_dict_column(table_dir, "l_shipmode", l_shipmode); }));
    writes.push_back(std::async(std::launch::async, [&] { write_string_column(table_dir, "l_comment", l_comment); }));
    for (auto& fut : writes) fut.get();
    write_row_count(table_dir, l_orderkey.size());
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: ingest <src_dir> <out_dir>\n";
        return 1;
    }

    try {
        const fs::path src_dir = argv[1];
        const fs::path out_dir = argv[2];
        fs::remove_all(out_dir);
        ensure_dir(out_dir);

        std::vector<std::future<void>> tasks;
        tasks.push_back(std::async(std::launch::async, parse_nation, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_region, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_supplier, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_part, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_partsupp, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_customer, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_orders, src_dir, out_dir));
        tasks.push_back(std::async(std::launch::async, parse_lineitem, src_dir, out_dir));

        for (auto& task : tasks) {
            task.get();
        }
    } catch (const std::exception& ex) {
        std::cerr << "ingest failed: " << ex.what() << '\n';
        return 2;
    }

    return 0;
}
