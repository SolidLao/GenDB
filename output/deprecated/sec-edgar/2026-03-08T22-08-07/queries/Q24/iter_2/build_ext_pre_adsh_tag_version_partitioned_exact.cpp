#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint32_t kPartitionBits = 19;
constexpr uint64_t kPartitionCount = 1ull << kPartitionBits;

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}

template <typename T>
std::vector<T> read_binary(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open input file: " + path.string());
    }
    const uint64_t bytes = fs::file_size(path);
    if (bytes % sizeof(T) != 0) {
        throw std::runtime_error("input file has unexpected size: " + path.string());
    }
    std::vector<T> values(bytes / sizeof(T));
    if (!values.empty()) {
        in.read(reinterpret_cast<char*>(values.data()), static_cast<std::streamsize>(bytes));
    }
    if (!in) {
        throw std::runtime_error("failed reading input file: " + path.string());
    }
    return values;
}

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

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const auto start = std::chrono::steady_clock::now();
        const fs::path gendb_dir = argv[1];
        const fs::path input_path = gendb_dir / "indexes/pre/pre_adsh_tag_version_groups.keys.bin";
        const fs::path out_dir = gendb_dir / "column_versions/pre.adsh_tag_version.partitioned_exact_p19";
        fs::create_directories(out_dir);

        std::vector<TripleKey> input_keys = read_binary<TripleKey>(input_path);
        std::vector<uint64_t> offsets(kPartitionCount + 1, 0);

        for (const TripleKey& key : input_keys) {
            const uint64_t hash = hash_triple(key.a, key.b, key.c);
            const uint64_t part = hash >> (64 - kPartitionBits);
            offsets[part + 1] += 1;
        }
        for (uint64_t part = 1; part <= kPartitionCount; ++part) {
            offsets[part] += offsets[part - 1];
        }

        std::vector<uint64_t> write_pos = offsets;
        std::vector<TripleKey> partitioned_keys(input_keys.size());
        for (const TripleKey& key : input_keys) {
            const uint64_t hash = hash_triple(key.a, key.b, key.c);
            const uint64_t part = hash >> (64 - kPartitionBits);
            partitioned_keys[write_pos[part]++] = key;
        }

        write_binary(out_dir / "offsets.bin", offsets);
        write_binary(out_dir / "keys.bin", partitioned_keys);

        uint64_t max_partition = 0;
        for (uint64_t part = 0; part < kPartitionCount; ++part) {
            max_partition = std::max(max_partition, offsets[part + 1] - offsets[part]);
        }

        const auto end = std::chrono::steady_clock::now();
        const auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cout << "row_count=" << input_keys.size() << '\n';
        std::cout << "partition_bits=" << kPartitionBits << '\n';
        std::cout << "partition_count=" << kPartitionCount << '\n';
        std::cout << "avg_partition_size=" << (input_keys.size() / static_cast<double>(kPartitionCount)) << '\n';
        std::cout << "max_partition_size=" << max_partition << '\n';
        std::cout << "build_time_ms=" << build_ms << '\n';
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << '\n';
        return 1;
    }

    return 0;
}
