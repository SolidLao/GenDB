#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kBlockSize = 100000;

template <typename T>
std::vector<T> read_vector(const fs::path& path) {
    const auto bytes = fs::file_size(path);
    std::vector<T> values(bytes / sizeof(T));
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!values.empty()) {
        in.read(reinterpret_cast<char*>(values.data()), static_cast<std::streamsize>(bytes));
    }
    return values;
}

template <typename T>
void write_vector(const fs::path& path, const std::vector<T>& values) {
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

uint64_t next_power_of_two(uint64_t x) {
    uint64_t v = 1;
    while (v < x) {
        v <<= 1;
    }
    return v;
}

uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

template <typename T>
void build_zone_map(const fs::path& source, const fs::path& dest) {
    ensure_dir(dest.parent_path());
    const auto values = read_vector<T>(source);
    std::vector<ZoneRecord<T>> zones;
    zones.reserve((values.size() + kBlockSize - 1) / kBlockSize);
    for (size_t offset = 0; offset < values.size(); offset += kBlockSize) {
        const size_t end = std::min(values.size(), offset + static_cast<size_t>(kBlockSize));
        T min_v = values[offset];
        T max_v = values[offset];
        for (size_t i = offset + 1; i < end; ++i) {
            min_v = std::min(min_v, values[i]);
            max_v = std::max(max_v, values[i]);
        }
        zones.push_back({min_v, max_v});
    }
    write_vector(dest, zones);
}

void build_sub_adsh_lookup(const fs::path& base_dir) {
    ensure_dir(base_dir / "sub" / "indexes");
    const auto sub_adsh = read_vector<uint32_t>(base_dir / "sub" / "adsh.bin");
    const auto adsh_offsets = read_vector<uint64_t>(base_dir / "shared" / "adsh.offsets.bin");
    std::vector<uint32_t> lookup(adsh_offsets.size() - 1, kEmpty32);
    for (uint32_t row = 0; row < sub_adsh.size(); ++row) {
        lookup[sub_adsh[row]] = row;
    }
    write_vector(base_dir / "sub" / "indexes" / "adsh_to_rowid.bin", lookup);
}

void build_tag_hash(const fs::path& base_dir) {
    ensure_dir(base_dir / "tag" / "indexes");
    const auto tags = read_vector<uint32_t>(base_dir / "tag" / "tag.bin");
    const auto versions = read_vector<uint32_t>(base_dir / "tag" / "version.bin");
    const uint64_t capacity = next_power_of_two(static_cast<uint64_t>(tags.size()) * 2 + 1);
    std::vector<uint32_t> key_tag(capacity, kEmpty32);
    std::vector<uint32_t> key_version(capacity, kEmpty32);
    std::vector<uint32_t> rowids(capacity, kEmpty32);
    for (uint32_t row = 0; row < tags.size(); ++row) {
        uint64_t slot = mix64((static_cast<uint64_t>(tags[row]) << 32) | versions[row]) & (capacity - 1);
        while (rowids[slot] != kEmpty32) {
            ++slot;
            slot &= (capacity - 1);
        }
        key_tag[slot] = tags[row];
        key_version[slot] = versions[row];
        rowids[slot] = row;
    }
    write_vector(base_dir / "tag" / "indexes" / "tag_version_hash.tag.bin", key_tag);
    write_vector(base_dir / "tag" / "indexes" / "tag_version_hash.version.bin", key_version);
    write_vector(base_dir / "tag" / "indexes" / "tag_version_hash.rowid.bin", rowids);
}

void build_sorted_triple_index(const fs::path& table_dir) {
    ensure_dir(table_dir / "indexes");
    const auto adsh = read_vector<uint32_t>(table_dir / "adsh.bin");
    const auto tag = read_vector<uint32_t>(table_dir / "tag.bin");
    const auto version = read_vector<uint32_t>(table_dir / "version.bin");
    std::vector<uint32_t> rowids(adsh.size());
    std::iota(rowids.begin(), rowids.end(), 0);
    std::sort(rowids.begin(), rowids.end(), [&](uint32_t lhs, uint32_t rhs) {
        return std::tie(adsh[lhs], tag[lhs], version[lhs], lhs) < std::tie(adsh[rhs], tag[rhs], version[rhs], rhs);
    });
    write_vector(table_dir / "indexes" / "adsh_tag_version.rowids.bin", rowids);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: build_indexes <gendb_dir>\n";
        return 1;
    }
    const fs::path base_dir = argv[1];

    auto sub_lookup = std::async(std::launch::async, build_sub_adsh_lookup, base_dir);
    auto tag_hash = std::async(std::launch::async, build_tag_hash, base_dir);
    auto num_sorted = std::async(std::launch::async, build_sorted_triple_index, base_dir / "num");
    auto pre_sorted = std::async(std::launch::async, build_sorted_triple_index, base_dir / "pre");

    auto sub_fy_zone = std::async(std::launch::async, build_zone_map<int32_t>, base_dir / "sub" / "fy.bin", base_dir / "sub" / "indexes" / "fy.zone_map.bin");
    auto sub_sic_zone = std::async(std::launch::async, build_zone_map<int32_t>, base_dir / "sub" / "sic.bin", base_dir / "sub" / "indexes" / "sic.zone_map.bin");
    auto tag_abs_zone = std::async(std::launch::async, build_zone_map<uint8_t>, base_dir / "tag" / "abstract.bin", base_dir / "tag" / "indexes" / "abstract.zone_map.bin");
    auto num_uom_zone = std::async(std::launch::async, build_zone_map<uint16_t>, base_dir / "num" / "uom.bin", base_dir / "num" / "indexes" / "uom.zone_map.bin");
    auto num_ddate_zone = std::async(std::launch::async, build_zone_map<int32_t>, base_dir / "num" / "ddate.bin", base_dir / "num" / "indexes" / "ddate.zone_map.bin");
    auto pre_stmt_zone = std::async(std::launch::async, build_zone_map<uint16_t>, base_dir / "pre" / "stmt.bin", base_dir / "pre" / "indexes" / "stmt.zone_map.bin");

    sub_lookup.get();
    tag_hash.get();
    num_sorted.get();
    pre_sorted.get();
    sub_fy_zone.get();
    sub_sic_zone.get();
    tag_abs_zone.get();
    num_uom_zone.get();
    num_ddate_zone.get();
    pre_stmt_zone.get();

    std::cout << "index build complete\n";
    return 0;
}
