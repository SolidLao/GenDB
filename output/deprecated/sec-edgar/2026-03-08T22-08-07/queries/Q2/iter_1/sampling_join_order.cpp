#include "mmap_utils.h"
#include "hash_utils.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

using gendb::CompactHashMap;
using gendb::MmapColumn;

namespace {

constexpr uint32_t kRowidNull = std::numeric_limits<uint32_t>::max();
constexpr int16_t kFiscalYear = 2022;

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& offsets_path, const std::string& data_path) {
        offsets.open(offsets_path);
        data.open(data_path);
    }

    size_t size() const { return offsets.size() ? offsets.size() - 1 : 0; }

    std::string_view view(uint32_t code) const {
        if (code + 1 >= offsets.size()) return {};
        uint64_t begin = offsets[code];
        uint64_t end = offsets[code + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }
};

template <typename CodeT>
CodeT resolve_code(const DictView& dict, std::string_view target) {
    for (uint32_t code = 0; code < dict.size(); ++code) {
        if (dict.view(code) == target) return static_cast<CodeT>(code);
    }
    return std::numeric_limits<CodeT>::max();
}

template <typename T>
bool find_postings_group(const MmapColumn<T>& values, T needle, size_t& group_idx) {
    const T* begin = values.data;
    const T* end = values.data + values.size();
    const T* it = std::lower_bound(begin, end, needle);
    if (it == end || *it != needle) return false;
    group_idx = static_cast<size_t>(it - begin);
    return true;
}

inline uint64_t pack_key(uint32_t adsh_code, uint32_t tag_code) {
    return (static_cast<uint64_t>(adsh_code) << 32) | static_cast<uint64_t>(tag_code);
}

struct Timer {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    double ms() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];

    MmapColumn<uint32_t> num_adsh;
    MmapColumn<uint32_t> num_tag;
    MmapColumn<double> num_value;

    MmapColumn<uint16_t> num_uom_postings_values;
    MmapColumn<uint64_t> num_uom_postings_offsets;
    MmapColumn<uint32_t> num_uom_postings_rowids;

    MmapColumn<int16_t> sub_fy_postings_values;
    MmapColumn<uint64_t> sub_fy_postings_offsets;
    MmapColumn<uint32_t> sub_fy_postings_rowids;
    MmapColumn<uint32_t> sub_adsh;
    MmapColumn<uint32_t> sub_adsh_dense_lookup;

    DictView num_uom_dict;

    num_adsh.open(gendb_dir + "/num/adsh.bin");
    num_tag.open(gendb_dir + "/num/tag.bin");
    num_value.open(gendb_dir + "/num/value.bin");

    num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
    num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
    num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

    sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
    sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
    sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");
    sub_adsh.open(gendb_dir + "/sub/adsh.bin");
    sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

    num_uom_dict.open(gendb_dir + "/dicts/num_uom.offsets.bin", gendb_dir + "/dicts/num_uom.data.bin");

    const uint16_t pure_code = resolve_code<uint16_t>(num_uom_dict, "pure");
    if (pure_code == std::numeric_limits<uint16_t>::max()) {
        std::fprintf(stderr, "Failed to resolve pure code\n");
        return 2;
    }

    size_t pure_group_idx = 0;
    if (!find_postings_group<uint16_t>(num_uom_postings_values, pure_code, pure_group_idx)) {
        std::fprintf(stderr, "pure postings group missing\n");
        return 3;
    }
    const uint64_t pure_begin = num_uom_postings_offsets[pure_group_idx];
    const uint64_t pure_end = num_uom_postings_offsets[pure_group_idx + 1];
    const uint32_t* pure_rowids = num_uom_postings_rowids.data + pure_begin;
    const size_t pure_row_count = static_cast<size_t>(pure_end - pure_begin);

    size_t fy_group_idx = 0;
    if (!find_postings_group<int16_t>(sub_fy_postings_values, kFiscalYear, fy_group_idx)) {
        std::fprintf(stderr, "fy postings group missing\n");
        return 4;
    }
    const uint64_t fy_begin = sub_fy_postings_offsets[fy_group_idx];
    const uint64_t fy_end = sub_fy_postings_offsets[fy_group_idx + 1];

    std::vector<uint32_t> fy2022_sub_rowid_by_adsh(sub_adsh_dense_lookup.size(), kRowidNull);
    for (uint64_t pos = fy_begin; pos < fy_end; ++pos) {
        const uint32_t sub_rowid = sub_fy_postings_rowids[pos];
        const uint32_t adsh_code = sub_adsh[sub_rowid];
        if (adsh_code < fy2022_sub_rowid_by_adsh.size()) fy2022_sub_rowid_by_adsh[adsh_code] = sub_rowid;
    }

    const size_t stride = 1;
    std::vector<uint32_t> sample_rowids;
    sample_rowids.reserve((pure_row_count + stride - 1) / stride);
    for (size_t idx = 0; idx < pure_row_count; idx += stride) sample_rowids.push_back(pure_rowids[idx]);

    std::fprintf(stdout, "pure_rows=%zu sample_rows=%zu fy2022_rows=%llu stride=%zu\n",
                 pure_row_count, sample_rowids.size(),
                 static_cast<unsigned long long>(fy_end - fy_begin), stride);

    {
        CompactHashMap<uint64_t, double> map(std::max<size_t>(131072, sample_rowids.size() / 2));
        size_t qualified_rows = 0;
        Timer timer;
        for (uint32_t rowid : sample_rowids) {
            const uint32_t adsh_code = num_adsh[rowid];
            if (adsh_code >= fy2022_sub_rowid_by_adsh.size()) continue;
            if (fy2022_sub_rowid_by_adsh[adsh_code] == kRowidNull) continue;
            const double value = num_value[rowid];
            if (std::isnan(value)) continue;
            ++qualified_rows;
            const uint64_t key = pack_key(adsh_code, num_tag[rowid]);
            if (double* slot = map.find(key)) {
                if (value > *slot) *slot = value;
            } else {
                map.insert(key, value);
            }
        }
        std::fprintf(stdout, "candidate=sub_first time_ms=%.3f qualified_rows=%zu groups=%zu\n",
                     timer.ms(), qualified_rows, map.size());
    }


    {
        struct GroupState { uint32_t tag; double max_value; uint32_t tie_count; };
        std::vector<std::vector<GroupState>> groups_by_adsh(fy2022_sub_rowid_by_adsh.size());
        size_t qualified_rows = 0;
        size_t group_count = 0;
        Timer timer;
        for (uint32_t rowid : sample_rowids) {
            const uint32_t adsh_code = num_adsh[rowid];
            if (adsh_code >= fy2022_sub_rowid_by_adsh.size()) continue;
            if (fy2022_sub_rowid_by_adsh[adsh_code] == kRowidNull) continue;
            const double value = num_value[rowid];
            if (std::isnan(value)) continue;
            ++qualified_rows;
            const uint32_t tag_code = num_tag[rowid];
            auto& bucket = groups_by_adsh[adsh_code];
            bool found = false;
            for (auto& state : bucket) {
                if (state.tag != tag_code) continue;
                found = true;
                if (value > state.max_value) {
                    state.max_value = value;
                    state.tie_count = 1;
                } else if (value == state.max_value) {
                    state.tie_count += 1;
                }
                break;
            }
            if (!found) {
                bucket.push_back(GroupState{tag_code, value, 1});
                ++group_count;
            }
        }
        size_t active_adsh = 0;
        size_t max_fanout = 0;
        for (const auto& bucket : groups_by_adsh) {
            if (!bucket.empty()) {
                ++active_adsh;
                if (bucket.size() > max_fanout) max_fanout = bucket.size();
            }
        }
        std::fprintf(stdout, "candidate=sub_first_adsh_vectors time_ms=%.3f qualified_rows=%zu groups=%zu active_adsh=%zu max_tags_per_adsh=%zu\\n",
                     timer.ms(), qualified_rows, group_count, active_adsh, max_fanout);
    }

    {
        CompactHashMap<uint64_t, double> map(std::max<size_t>(1048576, sample_rowids.size()));
        size_t input_rows = 0;
        Timer timer;
        for (uint32_t rowid : sample_rowids) {
            const double value = num_value[rowid];
            if (std::isnan(value)) continue;
            ++input_rows;
            const uint32_t adsh_code = num_adsh[rowid];
            const uint64_t key = pack_key(adsh_code, num_tag[rowid]);
            if (double* slot = map.find(key)) {
                if (value > *slot) *slot = value;
            } else {
                map.insert(key, value);
            }
        }
        size_t surviving_groups = 0;
        for (const auto& kv : map) {
            const uint32_t adsh_code = static_cast<uint32_t>(kv.first >> 32);
            if (adsh_code < fy2022_sub_rowid_by_adsh.size() && fy2022_sub_rowid_by_adsh[adsh_code] != kRowidNull) {
                ++surviving_groups;
            }
        }
        std::fprintf(stdout, "candidate=agg_first time_ms=%.3f input_rows=%zu all_groups=%zu surviving_groups=%zu\n",
                     timer.ms(), input_rows, map.size(), surviving_groups);
    }

    return 0;
}
