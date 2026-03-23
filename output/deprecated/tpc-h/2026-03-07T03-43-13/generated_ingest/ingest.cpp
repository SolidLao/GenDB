#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

struct LineCursor {
    std::string_view line;
    size_t pos = 0;

    std::string_view next() {
        size_t end = line.find('|', pos);
        if (end == std::string_view::npos) {
            end = line.size();
        }
        std::string_view value = line.substr(pos, end - pos);
        pos = std::min(end + 1, line.size());
        return value;
    }
};

static inline int64_t parse_int64(std::string_view sv) {
    bool neg = false;
    size_t i = 0;
    if (!sv.empty() && sv[0] == '-') {
        neg = true;
        i = 1;
    }
    int64_t value = 0;
    for (; i < sv.size(); ++i) {
        value = value * 10 + (sv[i] - '0');
    }
    return neg ? -value : value;
}

static inline int32_t parse_int32(std::string_view sv) {
    return static_cast<int32_t>(parse_int64(sv));
}

static inline int64_t parse_scaled_2(std::string_view sv) {
    bool neg = false;
    size_t i = 0;
    if (!sv.empty() && sv[0] == '-') {
        neg = true;
        i = 1;
    }
    int64_t whole = 0;
    while (i < sv.size() && sv[i] != '.') {
        whole = whole * 10 + (sv[i] - '0');
        ++i;
    }
    int64_t frac = 0;
    if (i < sv.size() && sv[i] == '.') {
        ++i;
        int digits = 0;
        while (i < sv.size() && digits < 2) {
            frac = frac * 10 + (sv[i] - '0');
            ++i;
            ++digits;
        }
        while (digits < 2) {
            frac *= 10;
            ++digits;
        }
    }
    int64_t value = whole * 100 + frac;
    return neg ? -value : value;
}

static inline int32_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? static_cast<unsigned>(-3) : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

static inline int32_t parse_date(std::string_view sv) {
    int y = static_cast<int>(parse_int64(sv.substr(0, 4)));
    unsigned m = static_cast<unsigned>(parse_int64(sv.substr(5, 2)));
    unsigned d = static_cast<unsigned>(parse_int64(sv.substr(8, 2)));
    return days_from_civil(y, m, d);
}

template <typename T>
void write_vector(const fs::path& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!data.empty()) {
        out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size() * sizeof(T)));
    }
}

void write_bytes(const fs::path& path, const std::vector<char>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!data.empty()) {
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
    }
}

void write_text(const fs::path& path, const std::string& text) {
    std::ofstream out(path);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    out << text;
}

template <typename Fn>
void parallel_run(std::vector<Fn>& tasks, size_t max_threads) {
    if (tasks.empty()) {
        return;
    }
    std::atomic<size_t> next{0};
    size_t thread_count = std::max<size_t>(1, std::min(max_threads, tasks.size()));
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t tid = 0; tid < thread_count; ++tid) {
        threads.emplace_back([&]() {
            while (true) {
                size_t idx = next.fetch_add(1, std::memory_order_relaxed);
                if (idx >= tasks.size()) {
                    break;
                }
                tasks[idx]();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

struct VarLenColumn {
    std::vector<uint64_t> offsets{0};
    std::vector<char> bytes;

    void reserve(size_t rows, size_t byte_capacity) {
        offsets.reserve(rows + 1);
        bytes.reserve(byte_capacity);
    }

    void append(std::string_view sv) {
        bytes.insert(bytes.end(), sv.begin(), sv.end());
        offsets.push_back(static_cast<uint64_t>(bytes.size()));
    }

    void write(const fs::path& dir, const std::string& name) const {
        write_bytes(dir / (name + ".data.bin"), bytes);
        write_vector(dir / (name + ".offsets.bin"), offsets);
    }
};

template <typename CodeT>
struct DictColumn {
    std::unordered_map<std::string, CodeT> ids;
    std::vector<std::string> values;
    std::vector<CodeT> codes;

    void reserve(size_t rows, size_t dict_hint) {
        codes.reserve(rows);
        ids.reserve(dict_hint);
        values.reserve(dict_hint);
    }

    void append(std::string_view sv) {
        std::string key(sv);
        auto it = ids.find(key);
        if (it == ids.end()) {
            CodeT code = static_cast<CodeT>(values.size());
            values.push_back(key);
            ids.emplace(values.back(), code);
            codes.push_back(code);
        } else {
            codes.push_back(it->second);
        }
    }

    void write(const fs::path& dir, const std::string& name) const {
        write_vector(dir / (name + ".codes.bin"), codes);
        std::vector<uint64_t> offsets;
        offsets.reserve(values.size() + 1);
        offsets.push_back(0);
        std::vector<char> bytes;
        size_t total = 0;
        for (const auto& value : values) {
            total += value.size();
        }
        bytes.reserve(total);
        for (const auto& value : values) {
            bytes.insert(bytes.end(), value.begin(), value.end());
            offsets.push_back(static_cast<uint64_t>(bytes.size()));
        }
        write_bytes(dir / (name + ".dict.data.bin"), bytes);
        write_vector(dir / (name + ".dict.offsets.bin"), offsets);
    }
};

void write_row_count(const fs::path& dir, size_t rows) {
    write_text(dir / "row_count.txt", std::to_string(rows) + "\n");
}

std::ifstream open_input(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    return in;
}

void parse_region(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "region";
    fs::create_directories(out_dir);

    std::vector<int32_t> r_regionkey;
    DictColumn<uint8_t> r_name;
    VarLenColumn r_comment;
    r_regionkey.reserve(5);
    r_name.reserve(5, 8);
    r_comment.reserve(5, 1024);

    auto in = open_input(src_dir / "region.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        r_regionkey.push_back(parse_int32(cur.next()));
        r_name.append(cur.next());
        r_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "r_regionkey.bin", r_regionkey); });
    writes.emplace_back([&]() { r_name.write(out_dir, "r_name"); });
    writes.emplace_back([&]() { r_comment.write(out_dir, "r_comment"); });
    parallel_run(writes, 3);
    write_row_count(out_dir, r_regionkey.size());
}

void parse_nation(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "nation";
    fs::create_directories(out_dir);

    std::vector<int32_t> n_nationkey;
    DictColumn<uint8_t> n_name;
    std::vector<int32_t> n_regionkey;
    VarLenColumn n_comment;
    n_nationkey.reserve(25);
    n_name.reserve(25, 32);
    n_regionkey.reserve(25);
    n_comment.reserve(25, 4096);

    auto in = open_input(src_dir / "nation.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        n_nationkey.push_back(parse_int32(cur.next()));
        n_name.append(cur.next());
        n_regionkey.push_back(parse_int32(cur.next()));
        n_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "n_nationkey.bin", n_nationkey); });
    writes.emplace_back([&]() { n_name.write(out_dir, "n_name"); });
    writes.emplace_back([&]() { write_vector(out_dir / "n_regionkey.bin", n_regionkey); });
    writes.emplace_back([&]() { n_comment.write(out_dir, "n_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, n_nationkey.size());
}

void parse_supplier(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "supplier";
    fs::create_directories(out_dir);

    constexpr size_t rows = 100000;
    std::vector<int32_t> s_suppkey;
    VarLenColumn s_name;
    VarLenColumn s_address;
    std::vector<int32_t> s_nationkey;
    VarLenColumn s_phone;
    std::vector<int64_t> s_acctbal;
    VarLenColumn s_comment;
    s_suppkey.reserve(rows);
    s_name.reserve(rows, rows * 18);
    s_address.reserve(rows, rows * 30);
    s_nationkey.reserve(rows);
    s_phone.reserve(rows, rows * 15);
    s_acctbal.reserve(rows);
    s_comment.reserve(rows, rows * 70);

    auto in = open_input(src_dir / "supplier.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        s_suppkey.push_back(parse_int32(cur.next()));
        s_name.append(cur.next());
        s_address.append(cur.next());
        s_nationkey.push_back(parse_int32(cur.next()));
        s_phone.append(cur.next());
        s_acctbal.push_back(parse_scaled_2(cur.next()));
        s_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "s_suppkey.bin", s_suppkey); });
    writes.emplace_back([&]() { s_name.write(out_dir, "s_name"); });
    writes.emplace_back([&]() { s_address.write(out_dir, "s_address"); });
    writes.emplace_back([&]() { write_vector(out_dir / "s_nationkey.bin", s_nationkey); });
    writes.emplace_back([&]() { s_phone.write(out_dir, "s_phone"); });
    writes.emplace_back([&]() { write_vector(out_dir / "s_acctbal.bin", s_acctbal); });
    writes.emplace_back([&]() { s_comment.write(out_dir, "s_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, s_suppkey.size());
}

void parse_part(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "part";
    fs::create_directories(out_dir);

    constexpr size_t rows = 2000000;
    std::vector<int32_t> p_partkey;
    VarLenColumn p_name;
    VarLenColumn p_mfgr;
    VarLenColumn p_brand;
    VarLenColumn p_type;
    std::vector<int32_t> p_size;
    VarLenColumn p_container;
    std::vector<int64_t> p_retailprice;
    VarLenColumn p_comment;
    p_partkey.reserve(rows);
    p_name.reserve(rows, rows * 28);
    p_mfgr.reserve(rows, rows * 14);
    p_brand.reserve(rows, rows * 9);
    p_type.reserve(rows, rows * 18);
    p_size.reserve(rows);
    p_container.reserve(rows, rows * 9);
    p_retailprice.reserve(rows);
    p_comment.reserve(rows, rows * 18);

    auto in = open_input(src_dir / "part.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        p_partkey.push_back(parse_int32(cur.next()));
        p_name.append(cur.next());
        p_mfgr.append(cur.next());
        p_brand.append(cur.next());
        p_type.append(cur.next());
        p_size.push_back(parse_int32(cur.next()));
        p_container.append(cur.next());
        p_retailprice.push_back(parse_scaled_2(cur.next()));
        p_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "p_partkey.bin", p_partkey); });
    writes.emplace_back([&]() { p_name.write(out_dir, "p_name"); });
    writes.emplace_back([&]() { p_mfgr.write(out_dir, "p_mfgr"); });
    writes.emplace_back([&]() { p_brand.write(out_dir, "p_brand"); });
    writes.emplace_back([&]() { p_type.write(out_dir, "p_type"); });
    writes.emplace_back([&]() { write_vector(out_dir / "p_size.bin", p_size); });
    writes.emplace_back([&]() { p_container.write(out_dir, "p_container"); });
    writes.emplace_back([&]() { write_vector(out_dir / "p_retailprice.bin", p_retailprice); });
    writes.emplace_back([&]() { p_comment.write(out_dir, "p_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, p_partkey.size());
}

void parse_partsupp(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "partsupp";
    fs::create_directories(out_dir);

    constexpr size_t rows = 8000000;
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<int64_t> ps_supplycost;
    VarLenColumn ps_comment;
    ps_partkey.reserve(rows);
    ps_suppkey.reserve(rows);
    ps_availqty.reserve(rows);
    ps_supplycost.reserve(rows);
    ps_comment.reserve(rows, rows * 100);

    auto in = open_input(src_dir / "partsupp.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        ps_partkey.push_back(parse_int32(cur.next()));
        ps_suppkey.push_back(parse_int32(cur.next()));
        ps_availqty.push_back(parse_int32(cur.next()));
        ps_supplycost.push_back(parse_scaled_2(cur.next()));
        ps_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "ps_partkey.bin", ps_partkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "ps_suppkey.bin", ps_suppkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "ps_availqty.bin", ps_availqty); });
    writes.emplace_back([&]() { write_vector(out_dir / "ps_supplycost.bin", ps_supplycost); });
    writes.emplace_back([&]() { ps_comment.write(out_dir, "ps_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, ps_partkey.size());
}

void parse_customer(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "customer";
    fs::create_directories(out_dir);

    constexpr size_t rows = 1500000;
    std::vector<int32_t> c_custkey;
    VarLenColumn c_name;
    VarLenColumn c_address;
    std::vector<int32_t> c_nationkey;
    VarLenColumn c_phone;
    std::vector<int64_t> c_acctbal;
    DictColumn<uint8_t> c_mktsegment;
    VarLenColumn c_comment;
    c_custkey.reserve(rows);
    c_name.reserve(rows, rows * 18);
    c_address.reserve(rows, rows * 30);
    c_nationkey.reserve(rows);
    c_phone.reserve(rows, rows * 15);
    c_acctbal.reserve(rows);
    c_mktsegment.reserve(rows, 8);
    c_comment.reserve(rows, rows * 60);

    auto in = open_input(src_dir / "customer.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        c_custkey.push_back(parse_int32(cur.next()));
        c_name.append(cur.next());
        c_address.append(cur.next());
        c_nationkey.push_back(parse_int32(cur.next()));
        c_phone.append(cur.next());
        c_acctbal.push_back(parse_scaled_2(cur.next()));
        c_mktsegment.append(cur.next());
        c_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "c_custkey.bin", c_custkey); });
    writes.emplace_back([&]() { c_name.write(out_dir, "c_name"); });
    writes.emplace_back([&]() { c_address.write(out_dir, "c_address"); });
    writes.emplace_back([&]() { write_vector(out_dir / "c_nationkey.bin", c_nationkey); });
    writes.emplace_back([&]() { c_phone.write(out_dir, "c_phone"); });
    writes.emplace_back([&]() { write_vector(out_dir / "c_acctbal.bin", c_acctbal); });
    writes.emplace_back([&]() { c_mktsegment.write(out_dir, "c_mktsegment"); });
    writes.emplace_back([&]() { c_comment.write(out_dir, "c_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, c_custkey.size());
}

void parse_orders(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "orders";
    fs::create_directories(out_dir);

    constexpr size_t rows = 15000000;
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    VarLenColumn o_orderpriority;
    VarLenColumn o_clerk;
    std::vector<int32_t> o_shippriority;
    VarLenColumn o_comment;
    o_orderkey.reserve(rows);
    o_custkey.reserve(rows);
    o_orderstatus.reserve(rows);
    o_totalprice.reserve(rows);
    o_orderdate.reserve(rows);
    o_orderpriority.reserve(rows, rows * 8);
    o_clerk.reserve(rows, rows * 15);
    o_shippriority.reserve(rows);
    o_comment.reserve(rows, rows * 45);

    auto in = open_input(src_dir / "orders.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        o_orderkey.push_back(parse_int32(cur.next()));
        o_custkey.push_back(parse_int32(cur.next()));
        o_orderstatus.push_back(static_cast<uint8_t>(cur.next()[0]));
        o_totalprice.push_back(parse_scaled_2(cur.next()));
        o_orderdate.push_back(parse_date(cur.next()));
        o_orderpriority.append(cur.next());
        o_clerk.append(cur.next());
        o_shippriority.push_back(parse_int32(cur.next()));
        o_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "o_orderkey.bin", o_orderkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "o_custkey.bin", o_custkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "o_orderstatus.bin", o_orderstatus); });
    writes.emplace_back([&]() { write_vector(out_dir / "o_totalprice.bin", o_totalprice); });
    writes.emplace_back([&]() { write_vector(out_dir / "o_orderdate.bin", o_orderdate); });
    writes.emplace_back([&]() { o_orderpriority.write(out_dir, "o_orderpriority"); });
    writes.emplace_back([&]() { o_clerk.write(out_dir, "o_clerk"); });
    writes.emplace_back([&]() { write_vector(out_dir / "o_shippriority.bin", o_shippriority); });
    writes.emplace_back([&]() { o_comment.write(out_dir, "o_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, o_orderkey.size());
}

void parse_lineitem(const fs::path& src_dir, const fs::path& dst_dir) {
    fs::path out_dir = dst_dir / "lineitem";
    fs::create_directories(out_dir);

    constexpr size_t rows = 59986052;
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;
    std::vector<int64_t> l_extendedprice;
    std::vector<int64_t> l_discount;
    std::vector<int64_t> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    VarLenColumn l_shipinstruct;
    VarLenColumn l_shipmode;
    VarLenColumn l_comment;
    l_orderkey.reserve(rows);
    l_partkey.reserve(rows);
    l_suppkey.reserve(rows);
    l_linenumber.reserve(rows);
    l_quantity.reserve(rows);
    l_extendedprice.reserve(rows);
    l_discount.reserve(rows);
    l_tax.reserve(rows);
    l_returnflag.reserve(rows);
    l_linestatus.reserve(rows);
    l_shipdate.reserve(rows);
    l_commitdate.reserve(rows);
    l_receiptdate.reserve(rows);
    l_shipinstruct.reserve(rows, rows * 17);
    l_shipmode.reserve(rows, rows * 6);
    l_comment.reserve(rows, rows * 28);

    auto in = open_input(src_dir / "lineitem.tbl");
    std::string line;
    while (std::getline(in, line)) {
        LineCursor cur{line};
        l_orderkey.push_back(parse_int32(cur.next()));
        l_partkey.push_back(parse_int32(cur.next()));
        l_suppkey.push_back(parse_int32(cur.next()));
        l_linenumber.push_back(parse_int32(cur.next()));
        l_quantity.push_back(parse_scaled_2(cur.next()));
        l_extendedprice.push_back(parse_scaled_2(cur.next()));
        l_discount.push_back(parse_scaled_2(cur.next()));
        l_tax.push_back(parse_scaled_2(cur.next()));
        l_returnflag.push_back(static_cast<uint8_t>(cur.next()[0]));
        l_linestatus.push_back(static_cast<uint8_t>(cur.next()[0]));
        l_shipdate.push_back(parse_date(cur.next()));
        l_commitdate.push_back(parse_date(cur.next()));
        l_receiptdate.push_back(parse_date(cur.next()));
        l_shipinstruct.append(cur.next());
        l_shipmode.append(cur.next());
        l_comment.append(cur.next());
    }

    std::vector<std::function<void()>> writes;
    writes.emplace_back([&]() { write_vector(out_dir / "l_orderkey.bin", l_orderkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_partkey.bin", l_partkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_suppkey.bin", l_suppkey); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_linenumber.bin", l_linenumber); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_quantity.bin", l_quantity); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_extendedprice.bin", l_extendedprice); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_discount.bin", l_discount); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_tax.bin", l_tax); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_returnflag.bin", l_returnflag); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_linestatus.bin", l_linestatus); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_shipdate.bin", l_shipdate); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_commitdate.bin", l_commitdate); });
    writes.emplace_back([&]() { write_vector(out_dir / "l_receiptdate.bin", l_receiptdate); });
    writes.emplace_back([&]() { l_shipinstruct.write(out_dir, "l_shipinstruct"); });
    writes.emplace_back([&]() { l_shipmode.write(out_dir, "l_shipmode"); });
    writes.emplace_back([&]() { l_comment.write(out_dir, "l_comment"); });
    parallel_run(writes, 4);
    write_row_count(out_dir, l_orderkey.size());
}

int main(int argc, char** argv) {
    try {
        if (argc != 3) {
            std::cerr << "usage: ingest <src_dir> <dst_dir>\n";
            return 1;
        }
        fs::path src_dir = argv[1];
        fs::path dst_dir = argv[2];
        fs::create_directories(dst_dir);

        std::vector<std::function<void()>> tasks;
        tasks.emplace_back([&]() { parse_region(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_nation(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_supplier(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_part(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_partsupp(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_customer(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_orders(src_dir, dst_dir); });
        tasks.emplace_back([&]() { parse_lineitem(src_dir, dst_dir); });
        parallel_run(tasks, 3);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "ingest failed: " << ex.what() << '\n';
        return 2;
    }
}
