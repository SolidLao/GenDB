#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "/home/jl4492/GenDB/src/gendb/utils/mmap_utils.h"

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kSicLo = 4000;
constexpr int32_t kSicHi = 4999;
constexpr size_t kMaxSamples = 2000000;
constexpr size_t kRowStride = 16;

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct DictData {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    std::string_view at(size_t idx) const {
        return std::string_view(data.data + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

    size_t size() const {
        return offsets.size() > 0 ? offsets.size() - 1 : 0;
    }
};

uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

DictData load_dict(const std::string& offsets_path, const std::string& data_path) {
    DictData dict;
    dict.offsets.open(offsets_path);
    dict.data.open(data_path);
    return dict;
}

uint16_t resolve_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary code not found");
}

template <typename T>
std::vector<ZoneRecord<T>> load_zone_map(const std::string& path) {
    gendb::MmapColumn<ZoneRecord<T>> zones(path);
    return std::vector<ZoneRecord<T>>(zones.data, zones.data + zones.size());
}

inline uint32_t probe_tag_rowid(const uint32_t* key_tag, const uint32_t* key_version,
                                const uint32_t* rowids, size_t mask,
                                uint32_t tag, uint32_t version) {
    size_t slot = static_cast<size_t>(mix64((static_cast<uint64_t>(tag) << 32) | version)) & mask;
    while (true) {
        const uint32_t rowid = rowids[slot];
        if (rowid == kEmpty32) {
            return kEmpty32;
        }
        if (key_tag[slot] == tag && key_version[slot] == version) {
            return rowid;
        }
        slot = (slot + 1) & mask;
    }
}

struct PreIndexProbe {
    const uint32_t* rowids = nullptr;
    const uint32_t* adsh = nullptr;
    const uint32_t* tag = nullptr;
    const uint32_t* version = nullptr;
    const uint16_t* stmt = nullptr;
    size_t size = 0;
    uint16_t eq_code = 0;

    int compare(size_t pos, uint32_t adsh_key, uint32_t tag_key, uint32_t version_key) const {
        const uint32_t row = rowids[pos];
        if (adsh[row] != adsh_key) {
            return adsh[row] < adsh_key ? -1 : 1;
        }
        if (tag[row] != tag_key) {
            return tag[row] < tag_key ? -1 : 1;
        }
        if (version[row] != version_key) {
            return version[row] < version_key ? -1 : 1;
        }
        return 0;
    }

    uint32_t count_eq(uint32_t adsh_key, uint32_t tag_key, uint32_t version_key) const {
        size_t lo = 0;
        size_t hi = size;
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (compare(mid, adsh_key, tag_key, version_key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= size || compare(lo, adsh_key, tag_key, version_key) != 0) {
            return 0;
        }
        uint32_t matches = 0;
        for (size_t pos = lo; pos < size; ++pos) {
            const uint32_t row = rowids[pos];
            if (adsh[row] != adsh_key || tag[row] != tag_key || version[row] != version_key) {
                break;
            }
            matches += static_cast<uint32_t>(stmt[row] == eq_code);
        }
        return matches;
    }
};

struct SampleRow {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
    size_t num_row;
};

struct Context {
    const uint32_t* sub_lookup = nullptr;
    size_t sub_lookup_size = 0;
    const int32_t* sub_sic = nullptr;
    const uint32_t* tag_hash_tag = nullptr;
    const uint32_t* tag_hash_version = nullptr;
    const uint32_t* tag_hash_rowid = nullptr;
    size_t tag_hash_mask = 0;
    const uint8_t* tag_abstract = nullptr;
    PreIndexProbe pre_probe;
};

bool pass_sub(const Context& ctx, uint32_t adsh) {
    if (adsh >= ctx.sub_lookup_size) {
        return false;
    }
    const uint32_t sub_row = ctx.sub_lookup[adsh];
    if (sub_row == kEmpty32) {
        return false;
    }
    const int32_t sic = ctx.sub_sic[sub_row];
    return sic >= kSicLo && sic <= kSicHi;
}

bool pass_tag(const Context& ctx, uint32_t tag, uint32_t version) {
    const uint32_t rowid = probe_tag_rowid(ctx.tag_hash_tag, ctx.tag_hash_version,
                                           ctx.tag_hash_rowid, ctx.tag_hash_mask,
                                           tag, version);
    return rowid != kEmpty32 && ctx.tag_abstract[rowid] == 0;
}

bool pass_pre(const Context& ctx, uint32_t adsh, uint32_t tag, uint32_t version) {
    return ctx.pre_probe.count_eq(adsh, tag, version) != 0;
}

using CheckFn = bool (*)(const Context&, const SampleRow&);

bool check_sub(const Context& ctx, const SampleRow& row) {
    return pass_sub(ctx, row.adsh);
}

bool check_tag(const Context& ctx, const SampleRow& row) {
    return pass_tag(ctx, row.tag, row.version);
}

bool check_pre(const Context& ctx, const SampleRow& row) {
    return pass_pre(ctx, row.adsh, row.tag, row.version);
}

struct OrderDef {
    const char* name;
    CheckFn checks[3];
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];

        const auto num_uom_dict = load_dict(gendb_dir + "/num/dict_uom.offsets.bin",
                                            gendb_dir + "/num/dict_uom.data.bin");
        const auto pre_stmt_dict = load_dict(gendb_dir + "/pre/dict_stmt.offsets.bin",
                                             gendb_dir + "/pre/dict_stmt.data.bin");
        const uint16_t usd_code = resolve_code(num_uom_dict, "USD");
        const uint16_t eq_code = resolve_code(pre_stmt_dict, "EQ");

        gendb::MmapColumn<uint32_t> num_adsh(gendb_dir + "/num/adsh.bin");
        gendb::MmapColumn<uint32_t> num_tag(gendb_dir + "/num/tag.bin");
        gendb::MmapColumn<uint32_t> num_version(gendb_dir + "/num/version.bin");
        gendb::MmapColumn<uint16_t> num_uom(gendb_dir + "/num/uom.bin");
        gendb::MmapColumn<uint32_t> sub_lookup(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");
        gendb::MmapColumn<int32_t> sub_sic(gendb_dir + "/sub/sic.bin");
        gendb::MmapColumn<uint32_t> tag_hash_tag(gendb_dir + "/tag/indexes/tag_version_hash.tag.bin");
        gendb::MmapColumn<uint32_t> tag_hash_version(gendb_dir + "/tag/indexes/tag_version_hash.version.bin");
        gendb::MmapColumn<uint32_t> tag_hash_rowid(gendb_dir + "/tag/indexes/tag_version_hash.rowid.bin");
        gendb::MmapColumn<uint8_t> tag_abstract(gendb_dir + "/tag/abstract.bin");
        gendb::MmapColumn<uint32_t> pre_rowids(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");
        gendb::MmapColumn<uint32_t> pre_adsh(gendb_dir + "/pre/adsh.bin");
        gendb::MmapColumn<uint32_t> pre_tag(gendb_dir + "/pre/tag.bin");
        gendb::MmapColumn<uint32_t> pre_version(gendb_dir + "/pre/version.bin");
        gendb::MmapColumn<uint16_t> pre_stmt(gendb_dir + "/pre/stmt.bin");

        const auto num_uom_zone = load_zone_map<uint16_t>(gendb_dir + "/num/indexes/uom.zone_map.bin");

        std::vector<SampleRow> sample_rows;
        sample_rows.reserve(kMaxSamples);
        for (size_t block = 0; block < num_uom_zone.size() && sample_rows.size() < kMaxSamples; ++block) {
            const auto& zone = num_uom_zone[block];
            if (zone.min > usd_code || zone.max < usd_code) {
                continue;
            }
            const size_t begin = block * kBlockSize;
            const size_t end = std::min(num_uom.size(), begin + kBlockSize);
            for (size_t row = begin; row < end && sample_rows.size() < kMaxSamples; row += kRowStride) {
                if (num_uom[row] != usd_code) {
                    continue;
                }
                sample_rows.push_back(SampleRow{num_adsh[row], num_tag[row], num_version[row], row});
            }
        }

        Context ctx;
        ctx.sub_lookup = sub_lookup.data;
        ctx.sub_lookup_size = sub_lookup.size();
        ctx.sub_sic = sub_sic.data;
        ctx.tag_hash_tag = tag_hash_tag.data;
        ctx.tag_hash_version = tag_hash_version.data;
        ctx.tag_hash_rowid = tag_hash_rowid.data;
        ctx.tag_hash_mask = tag_hash_rowid.size() - 1;
        ctx.tag_abstract = tag_abstract.data;
        ctx.pre_probe = PreIndexProbe{pre_rowids.data, pre_adsh.data, pre_tag.data, pre_version.data,
                                      pre_stmt.data, pre_rowids.size(), eq_code};

        const std::vector<OrderDef> orders = {
            {"sub->tag->pre", {check_sub, check_tag, check_pre}},
            {"sub->pre->tag", {check_sub, check_pre, check_tag}},
            {"tag->sub->pre", {check_tag, check_sub, check_pre}},
            {"tag->pre->sub", {check_tag, check_pre, check_sub}},
            {"pre->sub->tag", {check_pre, check_sub, check_tag}},
            {"pre->tag->sub", {check_pre, check_tag, check_sub}},
        };

        std::cout << "sample_rows=" << sample_rows.size() << "\n";
        for (const auto& order : orders) {
            uint64_t passed = 0;
            uint64_t stage1 = 0;
            uint64_t stage2 = 0;
            const auto start = std::chrono::steady_clock::now();
            for (const SampleRow& row : sample_rows) {
                if (!order.checks[0](ctx, row)) {
                    continue;
                }
                ++stage1;
                if (!order.checks[1](ctx, row)) {
                    continue;
                }
                ++stage2;
                if (!order.checks[2](ctx, row)) {
                    continue;
                }
                ++passed;
            }
            const auto end = std::chrono::steady_clock::now();
            const double ms = std::chrono::duration<double, std::milli>(end - start).count();
            std::cout << order.name
                      << " time_ms=" << ms
                      << " stage1=" << stage1
                      << " stage2=" << stage2
                      << " passed=" << passed
                      << "\n";
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
