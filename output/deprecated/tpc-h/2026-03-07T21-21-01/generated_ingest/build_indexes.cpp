#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
std::vector<T> read_vector_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    std::streamsize size = in.tellg();
    in.seekg(0);
    if (size < 0 || size % static_cast<std::streamsize>(sizeof(T)) != 0) {
        throw std::runtime_error("invalid binary file size for " + path.string());
    }
    std::vector<T> values(static_cast<size_t>(size / static_cast<std::streamsize>(sizeof(T))));
    if (size > 0) {
        in.read(reinterpret_cast<char*>(values.data()), size);
    }
    return values;
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

void ensure_dir(const fs::path& path) {
    fs::create_directories(path);
}

struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};

struct ZoneMap3 {
    int32_t shipdate_min;
    int32_t shipdate_max;
    double discount_min;
    double discount_max;
    double quantity_min;
    double quantity_max;
};

template <typename KeyT>
void build_dense_pk(const fs::path& index_dir, const std::string& name, const std::vector<KeyT>& keys) {
    KeyT max_key = 0;
    for (KeyT key : keys) {
        if (key > max_key) {
            max_key = key;
        }
    }
    std::vector<uint64_t> rowids(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint64_t>::max());
    for (uint64_t row = 0; row < keys.size(); ++row) {
        rowids[static_cast<size_t>(keys[row])] = row;
    }
    write_vector_file(index_dir / (name + ".bin"), rowids);
}

template <typename KeyT>
void build_dense_postings(const fs::path& index_dir, const std::string& name, const std::vector<KeyT>& keys) {
    KeyT max_key = 0;
    for (KeyT key : keys) {
        if (key > max_key) {
            max_key = key;
        }
    }
    std::vector<uint64_t> offsets(static_cast<size_t>(max_key) + 2, 0);
    for (KeyT key : keys) {
        offsets[static_cast<size_t>(key) + 1]++;
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
        offsets[i] += offsets[i - 1];
    }
    std::vector<uint64_t> cursors = offsets;
    std::vector<uint64_t> rowids(keys.size(), 0);
    for (uint64_t row = 0; row < keys.size(); ++row) {
        size_t slot = static_cast<size_t>(keys[row]);
        rowids[cursors[slot]++] = row;
    }
    write_vector_file(index_dir / (name + ".offsets.bin"), offsets);
    write_vector_file(index_dir / (name + ".rowids.bin"), rowids);
}

void build_orders_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "orders";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto o_orderkey = read_vector_file<int32_t>(table_dir / "o_orderkey.bin");
    auto o_custkey = read_vector_file<int32_t>(table_dir / "o_custkey.bin");
    auto o_orderdate = read_vector_file<int32_t>(table_dir / "o_orderdate.bin");

    build_dense_pk(index_dir, "orders_pk_dense", o_orderkey);
    build_dense_postings(index_dir, "orders_cust_postings", o_custkey);

    constexpr size_t block_size = 131072;
    std::vector<ZoneMap1I32> zones;
    zones.reserve((o_orderdate.size() + block_size - 1) / block_size);
    for (size_t base = 0; base < o_orderdate.size(); base += block_size) {
        size_t end = std::min(base + block_size, o_orderdate.size());
        int32_t min_v = std::numeric_limits<int32_t>::max();
        int32_t max_v = std::numeric_limits<int32_t>::min();
        for (size_t i = base; i < end; ++i) {
            min_v = std::min(min_v, o_orderdate[i]);
            max_v = std::max(max_v, o_orderdate[i]);
        }
        zones.push_back({min_v, max_v});
    }
    write_vector_file(index_dir / "orders_orderdate_zonemap.bin", zones);
}

void build_customer_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "customer";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto c_custkey = read_vector_file<int32_t>(table_dir / "c_custkey.bin");
    auto c_mktsegment = read_vector_file<uint8_t>(table_dir / "c_mktsegment.bin");

    build_dense_pk(index_dir, "customer_pk_dense", c_custkey);
    build_dense_postings(index_dir, "customer_segment_postings", c_mktsegment);
}

void build_supplier_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "supplier";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto s_suppkey = read_vector_file<int32_t>(table_dir / "s_suppkey.bin");
    auto s_nationkey = read_vector_file<int32_t>(table_dir / "s_nationkey.bin");

    build_dense_pk(index_dir, "supplier_pk_dense", s_suppkey);
    build_dense_postings(index_dir, "supplier_nation_postings", s_nationkey);
}

void build_part_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "part";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto p_partkey = read_vector_file<int32_t>(table_dir / "p_partkey.bin");
    build_dense_pk(index_dir, "part_pk_dense", p_partkey);
}

void build_partsupp_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "partsupp";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto ps_partkey = read_vector_file<int32_t>(table_dir / "ps_partkey.bin");
    build_dense_postings(index_dir, "partsupp_part_postings", ps_partkey);
}

void build_small_dimension_pk(const fs::path& base_dir, const std::string& table_name, const std::string& column_name, const std::string& index_name) {
    const fs::path table_dir = base_dir / table_name;
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);
    auto keys = read_vector_file<int32_t>(table_dir / (column_name + ".bin"));
    build_dense_pk(index_dir, index_name, keys);
}

void build_lineitem_indexes(const fs::path& base_dir) {
    const fs::path table_dir = base_dir / "lineitem";
    const fs::path index_dir = table_dir / "indexes";
    ensure_dir(index_dir);

    auto l_orderkey = read_vector_file<int32_t>(table_dir / "l_orderkey.bin");
    auto l_shipdate = read_vector_file<int32_t>(table_dir / "l_shipdate.bin");
    auto l_discount = read_vector_file<double>(table_dir / "l_discount.bin");
    auto l_quantity = read_vector_file<double>(table_dir / "l_quantity.bin");

    build_dense_postings(index_dir, "lineitem_order_postings", l_orderkey);

    constexpr size_t block_size = 131072;
    std::vector<ZoneMap1I32> shipdate_zones;
    shipdate_zones.reserve((l_shipdate.size() + block_size - 1) / block_size);
    std::vector<ZoneMap3> q6_zones;
    q6_zones.reserve((l_shipdate.size() + block_size - 1) / block_size);

    for (size_t base = 0; base < l_shipdate.size(); base += block_size) {
        size_t end = std::min(base + block_size, l_shipdate.size());
        int32_t ship_min = std::numeric_limits<int32_t>::max();
        int32_t ship_max = std::numeric_limits<int32_t>::min();
        double disc_min = std::numeric_limits<double>::infinity();
        double disc_max = -std::numeric_limits<double>::infinity();
        double qty_min = std::numeric_limits<double>::infinity();
        double qty_max = -std::numeric_limits<double>::infinity();
        for (size_t i = base; i < end; ++i) {
            ship_min = std::min(ship_min, l_shipdate[i]);
            ship_max = std::max(ship_max, l_shipdate[i]);
            disc_min = std::min(disc_min, l_discount[i]);
            disc_max = std::max(disc_max, l_discount[i]);
            qty_min = std::min(qty_min, l_quantity[i]);
            qty_max = std::max(qty_max, l_quantity[i]);
        }
        shipdate_zones.push_back({ship_min, ship_max});
        q6_zones.push_back({ship_min, ship_max, disc_min, disc_max, qty_min, qty_max});
    }

    write_vector_file(index_dir / "lineitem_shipdate_zonemap.bin", shipdate_zones);
    write_vector_file(index_dir / "lineitem_q6_zonemap.bin", q6_zones);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: build_indexes <gendb_dir>\n";
        return 1;
    }

    try {
        const fs::path base_dir = argv[1];
        build_small_dimension_pk(base_dir, "nation", "n_nationkey", "nation_pk_dense");
        build_small_dimension_pk(base_dir, "region", "r_regionkey", "region_pk_dense");
        build_supplier_indexes(base_dir);
        build_part_indexes(base_dir);
        build_partsupp_indexes(base_dir);
        build_customer_indexes(base_dir);
        build_orders_indexes(base_dir);
        build_lineitem_indexes(base_dir);
    } catch (const std::exception& ex) {
        std::cerr << "build_indexes failed: " << ex.what() << '\n';
        return 2;
    }

    return 0;
}
