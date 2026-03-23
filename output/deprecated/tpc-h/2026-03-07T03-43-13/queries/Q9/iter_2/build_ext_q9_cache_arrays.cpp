#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "/home/jl4492/GenDB/src/gendb/utils/date_utils.h"

namespace fs = std::filesystem;

template <typename T>
std::vector<T> read_vector(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    in.seekg(0, std::ios::end);
    const std::streamsize bytes = in.tellg();
    in.seekg(0, std::ios::beg);
    if (bytes < 0 || (bytes % static_cast<std::streamsize>(sizeof(T))) != 0) {
        throw std::runtime_error("invalid size for " + path.string());
    }
    std::vector<T> values(static_cast<size_t>(bytes / static_cast<std::streamsize>(sizeof(T))));
    if (!values.empty()) {
        in.read(reinterpret_cast<char*>(values.data()), bytes);
    }
    if (!in) {
        throw std::runtime_error("failed to read " + path.string());
    }
    return values;
}

template <typename T>
void write_vector(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to create " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()),
                  static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
    if (!out) {
        throw std::runtime_error("failed to write " + path.string());
    }
}

int main(int argc, char** argv) {
    try {
        if (argc != 2) {
            throw std::runtime_error("usage: build_ext_q9_cache_arrays <gendb_dir>");
        }

        const fs::path gendb_dir = argv[1];
        const fs::path column_versions_dir = gendb_dir / "column_versions";
        fs::create_directories(column_versions_dir);

        gendb::init_date_tables();

        const auto supplier_start = std::chrono::steady_clock::now();
        const auto supplier_keys = read_vector<int32_t>(gendb_dir / "supplier" / "s_suppkey.bin");
        const auto supplier_nationkey = read_vector<int32_t>(gendb_dir / "supplier" / "s_nationkey.bin");
        const auto nation_dense = read_vector<uint32_t>(gendb_dir / "nation" / "indexes" / "nation_pk_dense.bin");
        const auto nation_codes = read_vector<uint8_t>(gendb_dir / "nation" / "n_name.codes.bin");

        if (supplier_keys.size() != supplier_nationkey.size()) {
            throw std::runtime_error("supplier column size mismatch");
        }

        const int32_t max_suppkey = *std::max_element(supplier_keys.begin(), supplier_keys.end());
        std::vector<uint8_t> supplier_nation_code(static_cast<size_t>(max_suppkey) + 1,
                                                  std::numeric_limits<uint8_t>::max());
        for (size_t row = 0; row < supplier_keys.size(); ++row) {
            const int32_t suppkey = supplier_keys[row];
            const int32_t nationkey = supplier_nationkey[row];
            if (suppkey < 0 || nationkey < 0 || static_cast<size_t>(nationkey) >= nation_dense.size()) {
                continue;
            }
            const uint32_t nation_row = nation_dense[static_cast<size_t>(nationkey)];
            if (nation_row >= nation_codes.size()) {
                continue;
            }
            supplier_nation_code[static_cast<size_t>(suppkey)] = nation_codes[nation_row];
        }

        const fs::path supplier_dir = column_versions_dir / "supplier.s_suppkey.nation_code_u8";
        fs::create_directories(supplier_dir);
        write_vector(supplier_dir / "nation_code.bin", supplier_nation_code);
        const auto supplier_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - supplier_start).count();

        const auto orders_start = std::chrono::steady_clock::now();
        const auto order_keys = read_vector<int32_t>(gendb_dir / "orders" / "o_orderkey.bin");
        const auto order_dates = read_vector<int32_t>(gendb_dir / "orders" / "o_orderdate.bin");
        if (order_keys.size() != order_dates.size()) {
            throw std::runtime_error("orders column size mismatch");
        }

        const int32_t max_orderkey = *std::max_element(order_keys.begin(), order_keys.end());
        std::vector<uint8_t> order_year_idx(static_cast<size_t>(max_orderkey) + 1,
                                            std::numeric_limits<uint8_t>::max());
        uint8_t max_year_idx = 0;
        for (size_t row = 0; row < order_keys.size(); ++row) {
            const int32_t orderkey = order_keys[row];
            if (orderkey < 0) {
                continue;
            }
            const int year_idx = gendb::extract_year(order_dates[row]) - 1992;
            if (year_idx < 0 || year_idx > std::numeric_limits<uint8_t>::max()) {
                throw std::runtime_error("order year out of range");
            }
            order_year_idx[static_cast<size_t>(orderkey)] = static_cast<uint8_t>(year_idx);
            max_year_idx = std::max(max_year_idx, static_cast<uint8_t>(year_idx));
        }

        const fs::path orders_dir = column_versions_dir / "orders.o_orderkey.year_idx_u8";
        fs::create_directories(orders_dir);
        write_vector(orders_dir / "year_idx.bin", order_year_idx);
        const auto orders_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - orders_start).count();

        const auto partsupp_start = std::chrono::steady_clock::now();
        const auto ps_partkey = read_vector<int32_t>(gendb_dir / "partsupp" / "ps_partkey.bin");
        const auto ps_suppkey = read_vector<int32_t>(gendb_dir / "partsupp" / "ps_suppkey.bin");
        const auto ps_supplycost = read_vector<int64_t>(gendb_dir / "partsupp" / "ps_supplycost.bin");
        if (ps_partkey.size() != ps_suppkey.size() || ps_partkey.size() != ps_supplycost.size()) {
            throw std::runtime_error("partsupp column size mismatch");
        }

        const int32_t max_partkey = *std::max_element(ps_partkey.begin(), ps_partkey.end());
        const size_t slot_count = (static_cast<size_t>(max_partkey) + 1) * 4;
        std::vector<int32_t> dense_suppkey(slot_count, -1);
        std::vector<int32_t> dense_supplycost(slot_count, std::numeric_limits<int32_t>::min());
        std::vector<uint8_t> per_part_counts(static_cast<size_t>(max_partkey) + 1, 0);

        for (size_t row = 0; row < ps_partkey.size(); ++row) {
            const int32_t partkey = ps_partkey[row];
            if (partkey < 0) {
                throw std::runtime_error("negative partkey");
            }
            uint8_t& used = per_part_counts[static_cast<size_t>(partkey)];
            if (used >= 4) {
                throw std::runtime_error("partsupp fanout exceeded 4 for partkey " + std::to_string(partkey));
            }
            const int64_t supplycost64 = ps_supplycost[row];
            if (supplycost64 < std::numeric_limits<int32_t>::min() ||
                supplycost64 > std::numeric_limits<int32_t>::max()) {
                throw std::runtime_error("partsupp supplycost out of int32_t range");
            }
            const size_t slot = static_cast<size_t>(partkey) * 4 + static_cast<size_t>(used);
            dense_suppkey[slot] = ps_suppkey[row];
            dense_supplycost[slot] = static_cast<int32_t>(supplycost64);
            ++used;
        }

        const fs::path partsupp_dir = column_versions_dir / "partsupp.ps_partkey.dense4_i32";
        fs::create_directories(partsupp_dir);
        write_vector(partsupp_dir / "suppkey.bin", dense_suppkey);
        write_vector(partsupp_dir / "supplycost.bin", dense_supplycost);
        const auto partsupp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - partsupp_start).count();

        size_t four_count = 0;
        size_t nonzero_count = 0;
        for (uint8_t count : per_part_counts) {
            if (count != 0) {
                ++nonzero_count;
            }
            if (count == 4) {
                ++four_count;
            }
        }

        std::cout << "supplier rows=" << supplier_nation_code.size()
                  << " unique_codes=25 build_ms=" << supplier_ms << '\n';
        std::cout << "orders rows=" << order_year_idx.size()
                  << " max_year_idx=" << static_cast<int>(max_year_idx)
                  << " build_ms=" << orders_ms << '\n';
        std::cout << "partsupp partkeys=" << per_part_counts.size()
                  << " populated_partkeys=" << nonzero_count
                  << " fanout4_partkeys=" << four_count
                  << " slots=" << slot_count
                  << " build_ms=" << partsupp_ms << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "build_ext_q9_cache_arrays failed: " << ex.what() << '\n';
        return 1;
    }
}
