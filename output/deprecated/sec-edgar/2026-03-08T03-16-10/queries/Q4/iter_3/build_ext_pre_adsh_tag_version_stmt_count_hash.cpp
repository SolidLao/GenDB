#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "/home/jl4492/GenDB/src/gendb/utils/mmap_utils.h"

namespace fs = std::filesystem;

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint16_t kEmpty16 = std::numeric_limits<uint16_t>::max();

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

struct Entry {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
    uint16_t stmt;
    uint32_t count;
};

template <typename T>
void write_binary(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open output file: " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()),
                  static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
    if (!out) {
        throw std::runtime_error("failed to write output file: " + path.string());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const auto start = std::chrono::steady_clock::now();

    try {
        const fs::path gendb_dir = argv[1];
        const fs::path out_dir =
            gendb_dir / "column_versions" / "pre.adsh_tag_version_stmt.count_hash";
        fs::create_directories(out_dir);

        gendb::MmapColumn<uint32_t> pre_adsh((gendb_dir / "pre" / "adsh.bin").string());
        gendb::MmapColumn<uint32_t> pre_tag((gendb_dir / "pre" / "tag.bin").string());
        gendb::MmapColumn<uint32_t> pre_version((gendb_dir / "pre" / "version.bin").string());
        gendb::MmapColumn<uint16_t> pre_stmt((gendb_dir / "pre" / "stmt.bin").string());
        gendb::MmapColumn<uint32_t> sorted_rowids(
            (gendb_dir / "pre" / "indexes" / "adsh_tag_version.rowids.bin").string());

        if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
            pre_adsh.size() != pre_stmt.size() || pre_adsh.size() != sorted_rowids.size()) {
            throw std::runtime_error("pre column size mismatch");
        }

        std::vector<Entry> entries;
        entries.reserve(pre_adsh.size() / 2);

        std::vector<std::pair<uint16_t, uint32_t>> stmt_counts;
        stmt_counts.reserve(8);

        size_t pos = 0;
        while (pos < sorted_rowids.size()) {
            const uint32_t first_row = sorted_rowids[pos];
            const uint32_t adsh = pre_adsh[first_row];
            const uint32_t tag = pre_tag[first_row];
            const uint32_t version = pre_version[first_row];
            stmt_counts.clear();

            while (pos < sorted_rowids.size()) {
                const uint32_t row = sorted_rowids[pos];
                if (pre_adsh[row] != adsh || pre_tag[row] != tag || pre_version[row] != version) {
                    break;
                }
                const uint16_t stmt = pre_stmt[row];
                bool found = false;
                for (auto& kv : stmt_counts) {
                    if (kv.first == stmt) {
                        ++kv.second;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    stmt_counts.push_back({stmt, 1});
                }
                ++pos;
            }

            for (const auto& kv : stmt_counts) {
                entries.push_back(Entry{adsh, tag, version, kv.first, kv.second});
            }
        }

        const uint64_t capacity = next_power_of_two(static_cast<uint64_t>(entries.size()) * 2 + 1);
        std::vector<uint32_t> key_adsh(static_cast<size_t>(capacity), kEmpty32);
        std::vector<uint32_t> key_tag(static_cast<size_t>(capacity), kEmpty32);
        std::vector<uint32_t> key_version(static_cast<size_t>(capacity), kEmpty32);
        std::vector<uint16_t> key_stmt(static_cast<size_t>(capacity), kEmpty16);
        std::vector<uint32_t> counts(static_cast<size_t>(capacity), 0);

        for (const Entry& entry : entries) {
            uint64_t h = mix64(static_cast<uint64_t>(entry.adsh) * 0x9e3779b185ebca87ULL ^
                               (static_cast<uint64_t>(entry.tag) << 32) ^
                               static_cast<uint64_t>(entry.version));
            h = mix64(h ^ (static_cast<uint64_t>(entry.stmt) * 0xc2b2ae3d27d4eb4fULL));
            size_t slot = static_cast<size_t>(h) & (capacity - 1);
            while (key_adsh[slot] != kEmpty32) {
                ++slot;
                slot &= (capacity - 1);
            }
            key_adsh[slot] = entry.adsh;
            key_tag[slot] = entry.tag;
            key_version[slot] = entry.version;
            key_stmt[slot] = entry.stmt;
            counts[slot] = entry.count;
        }

        write_binary(out_dir / "key_adsh.bin", key_adsh);
        write_binary(out_dir / "key_tag.bin", key_tag);
        write_binary(out_dir / "key_version.bin", key_version);
        write_binary(out_dir / "key_stmt.bin", key_stmt);
        write_binary(out_dir / "counts.bin", counts);

        const auto end = std::chrono::steady_clock::now();
        const auto build_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "row_count=" << pre_adsh.size() << "\n";
        std::cout << "unique_keys=" << entries.size() << "\n";
        std::cout << "capacity=" << capacity << "\n";
        std::cout << "build_time_ms=" << build_ms << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
