#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
static std::vector<T> read_col(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + p.string());
    in.seekg(0, std::ios::end);
    const auto sz = in.tellg();
    in.seekg(0, std::ios::beg);
    std::vector<T> v(static_cast<size_t>(sz) / sizeof(T));
    if (!v.empty()) {
        in.read(reinterpret_cast<char*>(v.data()), static_cast<std::streamsize>(v.size() * sizeof(T)));
    }
    return v;
}

static uint64_t next_pow2(uint64_t x) {
    uint64_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static void write_hash_index_i32(const fs::path& out, const std::vector<int32_t>& keys) {
    const uint64_t n = keys.size();
    const uint64_t buckets = next_pow2(std::max<uint64_t>(1024, n / 2));

    std::vector<uint64_t> counts(buckets, 0);
    for (uint32_t row = 0; row < n; ++row) {
        const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
        ++counts[h];
    }

    std::vector<uint64_t> offsets(buckets + 1, 0);
    for (uint64_t b = 0; b < buckets; ++b) offsets[b + 1] = offsets[b] + counts[b];

    std::vector<uint64_t> cursor = offsets;
    struct Entry { int64_t key; uint32_t rowid; };
    std::vector<Entry> entries(n);

    for (uint32_t row = 0; row < n; ++row) {
        const int64_t key = keys[row];
        const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
        const uint64_t pos = cursor[h]++;
        entries[pos] = Entry{key, row};
    }

    std::ofstream f(out, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + out.string());
    f.write(reinterpret_cast<const char*>(&buckets), sizeof(buckets));
    f.write(reinterpret_cast<const char*>(&n), sizeof(n));
    f.write(reinterpret_cast<const char*>(offsets.data()), static_cast<std::streamsize>(offsets.size() * sizeof(uint64_t)));
    f.write(reinterpret_cast<const char*>(entries.data()), static_cast<std::streamsize>(entries.size() * sizeof(Entry)));
}

static void write_hash_index_u32(const fs::path& out, const std::vector<uint32_t>& keys) {
    const uint64_t n = keys.size();
    const uint64_t buckets = next_pow2(std::max<uint64_t>(1024, n / 2));

    std::vector<uint64_t> counts(buckets, 0);
    for (uint32_t row = 0; row < n; ++row) {
        const uint64_t h = mix64(keys[row]) & (buckets - 1);
        ++counts[h];
    }

    std::vector<uint64_t> offsets(buckets + 1, 0);
    for (uint64_t b = 0; b < buckets; ++b) offsets[b + 1] = offsets[b] + counts[b];

    std::vector<uint64_t> cursor = offsets;
    struct Entry { uint64_t key; uint32_t rowid; };
    std::vector<Entry> entries(n);

    for (uint32_t row = 0; row < n; ++row) {
        const uint64_t key = keys[row];
        const uint64_t h = mix64(keys[row]) & (buckets - 1);
        const uint64_t pos = cursor[h]++;
        entries[pos] = Entry{key, row};
    }

    std::ofstream f(out, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + out.string());
    f.write(reinterpret_cast<const char*>(&buckets), sizeof(buckets));
    f.write(reinterpret_cast<const char*>(&n), sizeof(n));
    f.write(reinterpret_cast<const char*>(offsets.data()), static_cast<std::streamsize>(offsets.size() * sizeof(uint64_t)));
    f.write(reinterpret_cast<const char*>(entries.data()), static_cast<std::streamsize>(entries.size() * sizeof(Entry)));
}

static void write_hash_index_pair_i32(const fs::path& out, const std::vector<int32_t>& a, const std::vector<int32_t>& b) {
    const uint64_t n = a.size();
    const uint64_t buckets = next_pow2(std::max<uint64_t>(1024, n / 2));

    std::vector<uint64_t> counts(buckets, 0);
    for (uint32_t row = 0; row < n; ++row) {
        const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(a[row])) << 32) |
                             static_cast<uint32_t>(b[row]);
        const uint64_t h = mix64(key) & (buckets - 1);
        ++counts[h];
    }

    std::vector<uint64_t> offsets(buckets + 1, 0);
    for (uint64_t i = 0; i < buckets; ++i) offsets[i + 1] = offsets[i] + counts[i];

    std::vector<uint64_t> cursor = offsets;
    struct Entry { uint64_t key; uint32_t rowid; };
    std::vector<Entry> entries(n);

    for (uint32_t row = 0; row < n; ++row) {
        const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(a[row])) << 32) |
                             static_cast<uint32_t>(b[row]);
        const uint64_t h = mix64(key) & (buckets - 1);
        const uint64_t pos = cursor[h]++;
        entries[pos] = Entry{key, row};
    }

    std::ofstream f(out, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + out.string());
    f.write(reinterpret_cast<const char*>(&buckets), sizeof(buckets));
    f.write(reinterpret_cast<const char*>(&n), sizeof(n));
    f.write(reinterpret_cast<const char*>(offsets.data()), static_cast<std::streamsize>(offsets.size() * sizeof(uint64_t)));
    f.write(reinterpret_cast<const char*>(entries.data()), static_cast<std::streamsize>(entries.size() * sizeof(Entry)));
}

template <typename T>
static void write_zone_map(const fs::path& out, const std::vector<T>& col, uint32_t block_size) {
    const uint64_t n = col.size();
    const uint64_t blocks = (n + block_size - 1) / block_size;
    std::vector<T> mins(blocks);
    std::vector<T> maxs(blocks);

    for (uint64_t b = 0; b < blocks; ++b) {
        const uint64_t start = b * block_size;
        const uint64_t end = std::min<uint64_t>(n, start + block_size);
        T mn = std::numeric_limits<T>::max();
        T mx = std::numeric_limits<T>::lowest();
        for (uint64_t i = start; i < end; ++i) {
            mn = std::min(mn, col[i]);
            mx = std::max(mx, col[i]);
        }
        mins[b] = mn;
        maxs[b] = mx;
    }

    std::ofstream f(out, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + out.string());
    f.write(reinterpret_cast<const char*>(&block_size), sizeof(block_size));
    f.write(reinterpret_cast<const char*>(&n), sizeof(n));
    f.write(reinterpret_cast<const char*>(&blocks), sizeof(blocks));
    f.write(reinterpret_cast<const char*>(mins.data()), static_cast<std::streamsize>(mins.size() * sizeof(T)));
    f.write(reinterpret_cast<const char*>(maxs.data()), static_cast<std::streamsize>(maxs.size() * sizeof(T)));
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: build_indexes <gendb_dir>\n";
        return 1;
    }

    fs::path base = argv[1];
    const uint32_t block_size = 100000;

    std::vector<std::future<void>> tasks;

    tasks.push_back(std::async(std::launch::async, [=] {
        auto c_custkey = read_col<int32_t>(base / "customer/c_custkey.bin");
        write_hash_index_i32(base / "customer/customer_pk_hash.idx", c_custkey);
        auto c_mktsegment = read_col<uint32_t>(base / "customer/c_mktsegment.bin");
        write_hash_index_u32(base / "customer/customer_mktsegment_hash.idx", c_mktsegment);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto o_orderkey = read_col<int32_t>(base / "orders/o_orderkey.bin");
        write_hash_index_i32(base / "orders/orders_pk_hash.idx", o_orderkey);
        auto o_custkey = read_col<int32_t>(base / "orders/o_custkey.bin");
        write_hash_index_i32(base / "orders/orders_custkey_hash.idx", o_custkey);
        auto o_orderdate = read_col<int32_t>(base / "orders/o_orderdate.bin");
        write_zone_map(base / "orders/orders_orderdate_zonemap.idx", o_orderdate, block_size);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto l_orderkey = read_col<int32_t>(base / "lineitem/l_orderkey.bin");
        write_hash_index_i32(base / "lineitem/lineitem_orderkey_hash.idx", l_orderkey);
        auto l_partkey = read_col<int32_t>(base / "lineitem/l_partkey.bin");
        auto l_suppkey = read_col<int32_t>(base / "lineitem/l_suppkey.bin");
        write_hash_index_pair_i32(base / "lineitem/lineitem_partsupp_hash.idx", l_partkey, l_suppkey);
        auto l_shipdate = read_col<int32_t>(base / "lineitem/l_shipdate.bin");
        write_zone_map(base / "lineitem/lineitem_shipdate_zonemap.idx", l_shipdate, block_size);
        auto l_discount = read_col<double>(base / "lineitem/l_discount.bin");
        write_zone_map(base / "lineitem/lineitem_discount_zonemap.idx", l_discount, block_size);
        auto l_quantity = read_col<double>(base / "lineitem/l_quantity.bin");
        write_zone_map(base / "lineitem/lineitem_quantity_zonemap.idx", l_quantity, block_size);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto ps_partkey = read_col<int32_t>(base / "partsupp/ps_partkey.bin");
        auto ps_suppkey = read_col<int32_t>(base / "partsupp/ps_suppkey.bin");
        write_hash_index_pair_i32(base / "partsupp/partsupp_pk_hash.idx", ps_partkey, ps_suppkey);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto s_suppkey = read_col<int32_t>(base / "supplier/s_suppkey.bin");
        write_hash_index_i32(base / "supplier/supplier_pk_hash.idx", s_suppkey);
        auto s_nationkey = read_col<int32_t>(base / "supplier/s_nationkey.bin");
        write_hash_index_i32(base / "supplier/supplier_nation_hash.idx", s_nationkey);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto p_partkey = read_col<int32_t>(base / "part/p_partkey.bin");
        write_hash_index_i32(base / "part/part_pk_hash.idx", p_partkey);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto n_nationkey = read_col<int32_t>(base / "nation/n_nationkey.bin");
        write_hash_index_i32(base / "nation/nation_pk_hash.idx", n_nationkey);
    }));

    tasks.push_back(std::async(std::launch::async, [=] {
        auto r_regionkey = read_col<int32_t>(base / "region/r_regionkey.bin");
        write_hash_index_i32(base / "region/region_pk_hash.idx", r_regionkey);
    }));

    for (auto& t : tasks) t.get();
    std::cerr << "index build complete\n";
    return 0;
}
