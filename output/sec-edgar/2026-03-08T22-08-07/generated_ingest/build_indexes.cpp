#include <algorithm>
#include <cmath>
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

namespace {

constexpr int32_t kInt32Null = std::numeric_limits<int32_t>::min();
constexpr int16_t kInt16Null = std::numeric_limits<int16_t>::min();
constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();

template <typename T>
std::vector<T> read_vector_binary(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("failed to open: " + path.string());
    }
    in.seekg(0, std::ios::end);
    const size_t bytes = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    if (bytes % sizeof(T) != 0) {
        throw std::runtime_error("file size mismatch for: " + path.string());
    }
    std::vector<T> values(bytes / sizeof(T));
    if (!values.empty()) {
        in.read(reinterpret_cast<char*>(values.data()), static_cast<std::streamsize>(bytes));
    }
    return values;
}

template <typename T>
void write_vector_binary(const fs::path& path, const std::vector<T>& values) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open for write: " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
}

void write_text_file(const fs::path& path, const std::string& content) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to write text file: " + path.string());
    }
    out << content;
}

uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

uint64_t hash_pair(uint32_t a, uint32_t b) {
    return mix64((static_cast<uint64_t>(a) << 32) ^ static_cast<uint64_t>(b));
}

uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}

template <typename T>
void build_value_postings(const std::vector<T>& values, T null_value, const fs::path& index_dir, const std::string& name) {
    std::vector<uint32_t> rowids(values.size());
    std::iota(rowids.begin(), rowids.end(), 0);
    std::sort(rowids.begin(), rowids.end(), [&](uint32_t left, uint32_t right) {
        if (values[left] != values[right]) {
            return values[left] < values[right];
        }
        return left < right;
    });

    std::vector<T> unique_values;
    std::vector<uint64_t> offsets{0};
    std::vector<uint32_t> grouped_rowids;
    grouped_rowids.reserve(values.size());

    bool have_group = false;
    T current{};
    for (uint32_t rowid : rowids) {
        const T value = values[rowid];
        if (value == null_value) {
            continue;
        }
        if (!have_group || value != current) {
            current = value;
            have_group = true;
            unique_values.push_back(value);
            offsets.push_back(offsets.back());
        }
        grouped_rowids.push_back(rowid);
        ++offsets.back();
    }

    write_vector_binary(index_dir / (name + ".values.bin"), unique_values);
    write_vector_binary(index_dir / (name + ".offsets.bin"), offsets);
    write_vector_binary(index_dir / (name + ".rowids.bin"), grouped_rowids);
}

template <typename T>
void build_value_postings_no_null(const std::vector<T>& values, const fs::path& index_dir, const std::string& name) {
    std::vector<uint32_t> rowids(values.size());
    std::iota(rowids.begin(), rowids.end(), 0);
    std::sort(rowids.begin(), rowids.end(), [&](uint32_t left, uint32_t right) {
        if (values[left] != values[right]) {
            return values[left] < values[right];
        }
        return left < right;
    });

    std::vector<T> unique_values;
    std::vector<uint64_t> offsets{0};
    std::vector<uint32_t> grouped_rowids;
    grouped_rowids.reserve(values.size());

    bool have_group = false;
    T current{};
    for (uint32_t rowid : rowids) {
        const T value = values[rowid];
        if (!have_group || value != current) {
            current = value;
            have_group = true;
            unique_values.push_back(value);
            offsets.push_back(offsets.back());
        }
        grouped_rowids.push_back(rowid);
        ++offsets.back();
    }

    write_vector_binary(index_dir / (name + ".values.bin"), unique_values);
    write_vector_binary(index_dir / (name + ".offsets.bin"), offsets);
    write_vector_binary(index_dir / (name + ".rowids.bin"), grouped_rowids);
}

void build_sub_dense_lookup(const fs::path& table_dir, const fs::path& index_dir) {
    const auto adsh = read_vector_binary<uint32_t>(table_dir / "adsh.bin");
    uint32_t max_code = 0;
    for (uint32_t code : adsh) {
        max_code = std::max(max_code, code);
    }
    std::vector<uint32_t> lookup(static_cast<size_t>(max_code) + 1, std::numeric_limits<uint32_t>::max());
    for (uint32_t rowid = 0; rowid < adsh.size(); ++rowid) {
        lookup[adsh[rowid]] = rowid;
    }
    write_vector_binary(index_dir / "sub_adsh_dense_lookup.bin", lookup);
}

struct PairKey {
    uint32_t a;
    uint32_t b;
};

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct PairBucket {
    uint32_t a;
    uint32_t b;
    uint64_t group_index;
};

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
};

void build_pair_group_index(const std::vector<uint32_t>& first, const std::vector<uint32_t>& second, const fs::path& index_dir, const std::string& name) {
    std::vector<uint32_t> rowids(first.size());
    std::iota(rowids.begin(), rowids.end(), 0);
    std::sort(rowids.begin(), rowids.end(), [&](uint32_t left, uint32_t right) {
        return std::tie(first[left], second[left], left) < std::tie(first[right], second[right], right);
    });

    std::vector<PairKey> keys;
    std::vector<uint64_t> offsets{0};
    for (uint32_t rowid : rowids) {
        const PairKey key{first[rowid], second[rowid]};
        if (keys.empty() || key.a != keys.back().a || key.b != keys.back().b) {
            keys.push_back(key);
            offsets.push_back(offsets.back());
        }
        ++offsets.back();
    }

    size_t bucket_count = 1;
    while (bucket_count < keys.size() * 2 + 1) {
        bucket_count <<= 1;
    }
    std::vector<PairBucket> buckets(bucket_count, PairBucket{0, 0, kEmptyGroup});
    for (uint64_t group = 0; group < keys.size(); ++group) {
        const auto& key = keys[group];
        size_t slot = static_cast<size_t>(hash_pair(key.a, key.b) & (bucket_count - 1));
        while (buckets[slot].group_index != kEmptyGroup) {
            slot = (slot + 1) & (bucket_count - 1);
        }
        buckets[slot] = PairBucket{key.a, key.b, group};
    }

    write_vector_binary(index_dir / (name + ".rowids.bin"), rowids);
    write_vector_binary(index_dir / (name + ".keys.bin"), keys);
    write_vector_binary(index_dir / (name + ".offsets.bin"), offsets);
    write_vector_binary(index_dir / (name + ".hash.bin"), buckets);
}

void build_triple_group_index(const std::vector<uint32_t>& first, const std::vector<uint32_t>& second, const std::vector<uint32_t>& third,
                              const fs::path& index_dir, const std::string& name) {
    std::vector<uint32_t> rowids(first.size());
    std::iota(rowids.begin(), rowids.end(), 0);
    std::sort(rowids.begin(), rowids.end(), [&](uint32_t left, uint32_t right) {
        return std::tie(first[left], second[left], third[left], left) < std::tie(first[right], second[right], third[right], right);
    });

    std::vector<TripleKey> keys;
    std::vector<uint64_t> offsets{0};
    for (uint32_t rowid : rowids) {
        const TripleKey key{first[rowid], second[rowid], third[rowid]};
        if (keys.empty() || key.a != keys.back().a || key.b != keys.back().b || key.c != keys.back().c) {
            keys.push_back(key);
            offsets.push_back(offsets.back());
        }
        ++offsets.back();
    }

    size_t bucket_count = 1;
    while (bucket_count < keys.size() * 2 + 1) {
        bucket_count <<= 1;
    }
    std::vector<TripleBucket> buckets(bucket_count, TripleBucket{0, 0, 0, kEmptyGroup});
    for (uint64_t group = 0; group < keys.size(); ++group) {
        const auto& key = keys[group];
        size_t slot = static_cast<size_t>(hash_triple(key.a, key.b, key.c) & (bucket_count - 1));
        while (buckets[slot].group_index != kEmptyGroup) {
            slot = (slot + 1) & (bucket_count - 1);
        }
        buckets[slot] = TripleBucket{key.a, key.b, key.c, group};
    }

    write_vector_binary(index_dir / (name + ".rowids.bin"), rowids);
    write_vector_binary(index_dir / (name + ".keys.bin"), keys);
    write_vector_binary(index_dir / (name + ".offsets.bin"), offsets);
    write_vector_binary(index_dir / (name + ".hash.bin"), buckets);
}

void build_sub_indexes(const fs::path& root) {
    const fs::path table_dir = root / "sub";
    const fs::path index_dir = root / "indexes" / "sub";
    fs::create_directories(index_dir);
    build_sub_dense_lookup(table_dir, index_dir);
    const auto fy = read_vector_binary<int16_t>(table_dir / "fy.bin");
    const auto sic = read_vector_binary<int32_t>(table_dir / "sic.bin");
    build_value_postings<int16_t>(fy, kInt16Null, index_dir, "sub_fy_postings");
    build_value_postings<int32_t>(sic, kInt32Null, index_dir, "sub_sic_postings");
    write_text_file(index_dir / "manifest.txt", "sub_adsh_dense_lookup\nsub_fy_postings\nsub_sic_postings\n");
}

void build_num_indexes(const fs::path& root) {
    const fs::path table_dir = root / "num";
    const fs::path index_dir = root / "indexes" / "num";
    fs::create_directories(index_dir);
    const auto uom = read_vector_binary<uint16_t>(table_dir / "uom.bin");
    const auto ddate = read_vector_binary<int32_t>(table_dir / "ddate.bin");
    const auto adsh = read_vector_binary<uint32_t>(table_dir / "adsh.bin");
    const auto tag = read_vector_binary<uint32_t>(table_dir / "tag.bin");
    const auto version = read_vector_binary<uint32_t>(table_dir / "version.bin");
    auto postings_future = std::async(std::launch::async, [&] { build_value_postings_no_null<uint16_t>(uom, index_dir, "num_uom_postings"); });
    auto ddate_future = std::async(std::launch::async, [&] { build_value_postings<int32_t>(ddate, kInt32Null, index_dir, "num_ddate_postings"); });
    auto triple_future = std::async(std::launch::async, [&] { build_triple_group_index(adsh, tag, version, index_dir, "num_adsh_tag_version_groups"); });
    postings_future.get();
    ddate_future.get();
    triple_future.get();
    write_text_file(index_dir / "manifest.txt", "num_uom_postings\nnum_ddate_postings\nnum_adsh_tag_version_groups\n");
}

void build_tag_indexes(const fs::path& root) {
    const fs::path table_dir = root / "tag";
    const fs::path index_dir = root / "indexes" / "tag";
    fs::create_directories(index_dir);
    const auto tag = read_vector_binary<uint32_t>(table_dir / "tag.bin");
    const auto version = read_vector_binary<uint32_t>(table_dir / "version.bin");
    build_pair_group_index(tag, version, index_dir, "tag_tag_version_hash");
    write_text_file(index_dir / "manifest.txt", "tag_tag_version_hash\n");
}

void build_pre_indexes(const fs::path& root) {
    const fs::path table_dir = root / "pre";
    const fs::path index_dir = root / "indexes" / "pre";
    fs::create_directories(index_dir);
    const auto stmt = read_vector_binary<uint16_t>(table_dir / "stmt.bin");
    const auto adsh = read_vector_binary<uint32_t>(table_dir / "adsh.bin");
    const auto tag = read_vector_binary<uint32_t>(table_dir / "tag.bin");
    const auto version = read_vector_binary<uint32_t>(table_dir / "version.bin");
    auto stmt_future = std::async(std::launch::async, [&] { build_value_postings_no_null<uint16_t>(stmt, index_dir, "pre_stmt_postings"); });
    auto triple_future = std::async(std::launch::async, [&] { build_triple_group_index(adsh, tag, version, index_dir, "pre_adsh_tag_version_groups"); });
    stmt_future.get();
    triple_future.get();
    write_text_file(index_dir / "manifest.txt", "pre_stmt_postings\npre_adsh_tag_version_groups\n");
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2) {
            std::cerr << "usage: build_indexes <gendb_dir>\n";
            return 1;
        }
        const fs::path root = argv[1];
        fs::create_directories(root / "indexes");

        auto sub_future = std::async(std::launch::async, build_sub_indexes, root);
        auto num_future = std::async(std::launch::async, build_num_indexes, root);
        auto tag_future = std::async(std::launch::async, build_tag_indexes, root);
        auto pre_future = std::async(std::launch::async, build_pre_indexes, root);

        sub_future.get();
        num_future.get();
        tag_future.get();
        pre_future.get();

        write_text_file(root / "indexes" / "manifest.txt", "sub\nnum\ntag\npre\n");
        std::cout << "Index build complete\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "build_indexes failed: " << ex.what() << "\n";
        return 1;
    }
}
