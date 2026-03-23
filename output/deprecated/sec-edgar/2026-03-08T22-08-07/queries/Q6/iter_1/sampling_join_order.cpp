#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "mmap_utils.h"

namespace fs = std::filesystem;
using gendb::MmapColumn;

constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;
    void open(const std::string& base) {
        offsets.open(base + ".offsets.bin");
        data.open(base + ".data.bin");
        if (offsets.size() == 0) throw std::runtime_error("empty dict");
    }
    std::string_view decode(uint32_t code) const {
        return std::string_view(data.data + offsets[code], static_cast<size_t>(offsets[code + 1] - offsets[code]));
    }
    template <typename CodeType>
    CodeType find_code(std::string_view needle) const {
        for (uint32_t code = 0; code + 1 < offsets.size(); ++code) {
            if (decode(code) == needle) return static_cast<CodeType>(code);
        }
        throw std::runtime_error("dict value not found");
    }
};

template <typename T>
struct PostingSlice { bool found=false; uint64_t begin=0; uint64_t end=0; };

template <typename T>
PostingSlice<T> find_posting_slice(const MmapColumn<T>& values, const MmapColumn<uint64_t>& offsets, T needle) {
    PostingSlice<T> out;
    const T* it = std::lower_bound(values.data, values.data + values.size(), needle);
    if (it == values.data + values.size() || *it != needle) return out;
    size_t idx = static_cast<size_t>(it - values.data);
    out.found = true;
    out.begin = offsets[idx];
    out.end = offsets[idx + 1];
    return out;
}

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

struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };
struct TripleBucket { uint32_t a; uint32_t b; uint32_t c; uint64_t group_index; };

struct Stats {
    double ms = 0.0;
    uint64_t sampled = 0;
    uint64_t sub_pass = 0;
    uint64_t pre_groups = 0;
    uint64_t pre_rows = 0;
    uint64_t joined = 0;
};

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }
    const std::string gendb_dir = argv[1];

    MmapColumn<uint32_t> num_adsh, num_tag, num_version;
    MmapColumn<double> num_value;
    MmapColumn<uint16_t> num_uom_values;
    MmapColumn<uint64_t> num_uom_offsets;
    MmapColumn<uint32_t> num_uom_rowids;

    MmapColumn<uint32_t> sub_adsh_dense_lookup;
    MmapColumn<int16_t> sub_fy;
    MmapColumn<uint32_t> sub_name;

    MmapColumn<uint16_t> pre_stmt;
    MmapColumn<uint32_t> pre_group_rowids;
    MmapColumn<TripleKey> pre_group_keys;
    MmapColumn<uint64_t> pre_group_offsets;
    MmapColumn<TripleBucket> pre_group_hash;

    DictView num_uom_dict, pre_stmt_dict;

    num_adsh.open(gendb_dir + "/num/adsh.bin");
    num_tag.open(gendb_dir + "/num/tag.bin");
    num_version.open(gendb_dir + "/num/version.bin");
    num_value.open(gendb_dir + "/num/value.bin");
    num_uom_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
    num_uom_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
    num_uom_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

    sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");
    sub_fy.open(gendb_dir + "/sub/fy.bin");
    sub_name.open(gendb_dir + "/sub/name.bin");

    pre_stmt.open(gendb_dir + "/pre/stmt.bin");
    pre_group_rowids.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.rowids.bin");
    pre_group_keys.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.keys.bin");
    pre_group_offsets.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.offsets.bin");
    pre_group_hash.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.hash.bin");

    num_uom_dict.open(gendb_dir + "/dicts/num_uom");
    pre_stmt_dict.open(gendb_dir + "/dicts/pre_stmt");

    const uint16_t usd_code = num_uom_dict.find_code<uint16_t>("USD");
    const uint16_t is_code = pre_stmt_dict.find_code<uint16_t>("IS");
    const auto usd_slice = find_posting_slice(num_uom_values, num_uom_offsets, usd_code);
    if (!usd_slice.found) throw std::runtime_error("USD slice missing");

    const uint64_t usd_rows = usd_slice.end - usd_slice.begin;
    const uint64_t target_samples = 1000000;
    const uint64_t step = std::max<uint64_t>(1, usd_rows / target_samples);

    auto probe_pre = [&](uint32_t adsh, uint32_t tag, uint32_t version, uint64_t& group_rows, uint64_t& joined_rows) -> bool {
        const uint64_t bucket_count = pre_group_hash.size();
        size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & (bucket_count - 1));
        while (true) {
            const TripleBucket& bucket = pre_group_hash[slot];
            if (bucket.group_index == kEmpty64) return false;
            if (bucket.a == adsh && bucket.b == tag && bucket.c == version) {
                const uint64_t begin = pre_group_offsets[bucket.group_index];
                const uint64_t end = pre_group_offsets[bucket.group_index + 1];
                group_rows += (end - begin);
                bool matched = false;
                for (uint64_t i = begin; i < end; ++i) {
                    const uint32_t pre_row = pre_group_rowids[i];
                    if (pre_stmt[pre_row] == is_code) {
                        ++joined_rows;
                        matched = true;
                    }
                }
                return matched;
            }
            slot = (slot + 1) & (bucket_count - 1);
        }
    };

    auto run_sub_first = [&]() {
        Stats st;
        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t pos = usd_slice.begin; pos < usd_slice.end; pos += step) {
            const uint32_t rowid = num_uom_rowids[pos];
            ++st.sampled;
            const double value = num_value[rowid];
            if (std::isnan(value)) continue;
            const uint32_t adsh = num_adsh[rowid];
            if (adsh >= sub_adsh_dense_lookup.size()) continue;
            const uint32_t sub_row = sub_adsh_dense_lookup[adsh];
            if (sub_row == kEmpty32) continue;
            if (sub_fy[sub_row] != 2023) continue;
            ++st.sub_pass;
            const bool matched = probe_pre(adsh, num_tag[rowid], num_version[rowid], st.pre_rows, st.joined);
            if (matched) ++st.pre_groups;
        }
        auto t1 = std::chrono::steady_clock::now();
        st.ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        return st;
    };

    auto run_pre_first = [&]() {
        Stats st;
        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t pos = usd_slice.begin; pos < usd_slice.end; pos += step) {
            const uint32_t rowid = num_uom_rowids[pos];
            ++st.sampled;
            const double value = num_value[rowid];
            if (std::isnan(value)) continue;
            const uint32_t adsh = num_adsh[rowid];
            const bool matched = probe_pre(adsh, num_tag[rowid], num_version[rowid], st.pre_rows, st.joined);
            if (!matched) continue;
            ++st.pre_groups;
            if (adsh >= sub_adsh_dense_lookup.size()) continue;
            const uint32_t sub_row = sub_adsh_dense_lookup[adsh];
            if (sub_row == kEmpty32) continue;
            if (sub_fy[sub_row] != 2023) continue;
            ++st.sub_pass;
        }
        auto t1 = std::chrono::steady_clock::now();
        st.ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        return st;
    };

    const Stats s1 = run_sub_first();
    const Stats s2 = run_pre_first();

    std::cout << "usd_rows=" << usd_rows << " step=" << step << " sampled=" << s1.sampled << "\n";
    std::cout << "sub_first ms=" << s1.ms << " sub_pass=" << s1.sub_pass << " pre_groups=" << s1.pre_groups
              << " pre_rows_scanned=" << s1.pre_rows << " joined_rows=" << s1.joined << "\n";
    std::cout << "pre_first ms=" << s2.ms << " sub_pass=" << s2.sub_pass << " pre_groups=" << s2.pre_groups
              << " pre_rows_scanned=" << s2.pre_rows << " joined_rows=" << s2.joined << "\n";
    std::cout << "winner=" << (s1.ms <= s2.ms ? "sub_first" : "pre_first") << "\n";
    return 0;
}
