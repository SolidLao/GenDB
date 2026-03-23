#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "/home/jl4492/GenDB/src/gendb/utils/mmap_utils.h"

namespace fs = std::filesystem;

struct RunIndexStats {
    uint64_t row_count = 0;
    uint64_t unique_count = 0;
    uint32_t max_adsh = 0;
};

template <typename T>
void write_binary(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("cannot open output file: " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()),
                  static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
    if (!out) {
        throw std::runtime_error("failed writing output file: " + path.string());
    }
}

RunIndexStats build_run_index(const std::string& gendb_dir, const std::string& table) {
    gendb::MmapColumn<uint32_t> adsh(gendb_dir + "/" + table + "/adsh.bin");
    gendb::MmapColumn<uint32_t> tag(gendb_dir + "/" + table + "/tag.bin");
    gendb::MmapColumn<uint32_t> version(gendb_dir + "/" + table + "/version.bin");
    gendb::MmapColumn<uint32_t> rowids(gendb_dir + "/" + table + "/indexes/adsh_tag_version.rowids.bin");

    if (adsh.size() != tag.size() || adsh.size() != version.size() || adsh.size() != rowids.size()) {
        throw std::runtime_error("column size mismatch for table " + table);
    }

    const uint64_t row_count = adsh.size();
    std::vector<uint32_t> unique_adsh;
    std::vector<uint32_t> unique_tag;
    std::vector<uint32_t> unique_version;
    std::vector<uint64_t> offsets;
    unique_adsh.reserve(row_count / 4);
    unique_tag.reserve(row_count / 4);
    unique_version.reserve(row_count / 4);
    offsets.reserve(row_count / 4 + 1);

    uint32_t max_adsh = 0;
    offsets.push_back(0);

    if (row_count > 0) {
        uint32_t prev_row = rowids[0];
        uint32_t cur_adsh = adsh[prev_row];
        uint32_t cur_tag = tag[prev_row];
        uint32_t cur_version = version[prev_row];

        unique_adsh.push_back(cur_adsh);
        unique_tag.push_back(cur_tag);
        unique_version.push_back(cur_version);
        max_adsh = cur_adsh;

        for (uint64_t pos = 1; pos < row_count; ++pos) {
            const uint32_t row = rowids[pos];
            const uint32_t a = adsh[row];
            const uint32_t t = tag[row];
            const uint32_t v = version[row];
            if (a != cur_adsh || t != cur_tag || v != cur_version) {
                offsets.push_back(pos);
                unique_adsh.push_back(a);
                unique_tag.push_back(t);
                unique_version.push_back(v);
                cur_adsh = a;
                cur_tag = t;
                cur_version = v;
            }
            if (a > max_adsh) {
                max_adsh = a;
            }
        }
    }
    offsets.push_back(row_count);

    std::vector<uint64_t> adsh_offsets(static_cast<size_t>(max_adsh) + 2, 0);
    for (uint32_t value : unique_adsh) {
        ++adsh_offsets[static_cast<size_t>(value) + 1];
    }
    for (size_t i = 1; i < adsh_offsets.size(); ++i) {
        adsh_offsets[i] += adsh_offsets[i - 1];
    }

    const fs::path out_dir = fs::path(gendb_dir) / "column_versions" / (table + ".adsh_tag_version.runs");
    fs::create_directories(out_dir);
    write_binary(out_dir / "adsh.bin", unique_adsh);
    write_binary(out_dir / "tag.bin", unique_tag);
    write_binary(out_dir / "version.bin", unique_version);
    write_binary(out_dir / "offsets.bin", offsets);
    write_binary(out_dir / "adsh_offsets.bin", adsh_offsets);

    RunIndexStats stats;
    stats.row_count = row_count;
    stats.unique_count = unique_adsh.size();
    stats.max_adsh = max_adsh;
    return stats;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const auto start = std::chrono::steady_clock::now();
        const std::string gendb_dir = argv[1];
        const RunIndexStats num_stats = build_run_index(gendb_dir, "num");
        const RunIndexStats pre_stats = build_run_index(gendb_dir, "pre");
        const auto end = std::chrono::steady_clock::now();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "num row_count=" << num_stats.row_count
                  << " unique_keys=" << num_stats.unique_count
                  << " max_adsh=" << num_stats.max_adsh << "\n";
        std::cout << "pre row_count=" << pre_stats.row_count
                  << " unique_keys=" << pre_stats.unique_count
                  << " max_adsh=" << pre_stats.max_adsh << "\n";
        std::cout << "build_time_ms=" << ms << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
