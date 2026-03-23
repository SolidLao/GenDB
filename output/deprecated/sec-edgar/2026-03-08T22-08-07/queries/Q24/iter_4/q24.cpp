#include <algorithm>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>

#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace fs = std::filesystem;

namespace {

using gendb::CompactHashMap;
using gendb::MmapColumn;

constexpr int32_t kDateLo = 19358;
constexpr int32_t kDateHi = 19722;
constexpr size_t kPartitionBits = 19;
constexpr size_t kPartitionCount = 1ull << kPartitionBits;
constexpr size_t kTopShift = 64 - kPartitionBits;
constexpr size_t kTopK = 100;
constexpr size_t kExpectedDateRuns = 12;
constexpr uint32_t kMorselRows = 1u << 18;
constexpr size_t kLocalAggReserve = 1 << 13;

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct AggState {
    uint64_t count = 0;
    double sum = 0.0;
};

struct OutputRow {
    uint32_t tag;
    uint32_t version;
    uint64_t count;
    double sum;
};

struct RowidSlice {
    const uint32_t* ptr = nullptr;
    const uint32_t* end = nullptr;
};

struct RangeSlice {
    uint32_t begin;
    uint32_t end;
};

struct RunSpan {
    const uint32_t* data = nullptr;
    uint32_t size = 0;
};

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& base_path) {
        offsets.open(base_path + ".offsets.bin");
        data.open(base_path + ".data.bin");
        if (offsets.size() < 2) {
            throw std::runtime_error("dictionary too small: " + base_path);
        }
    }

    std::string_view lookup(uint32_t code) const {
        const size_t idx = static_cast<size_t>(code);
        if (idx + 1 >= offsets.size()) {
            throw std::out_of_range("dictionary code out of range");
        }
        const uint64_t start = offsets[idx];
        const uint64_t end = offsets[idx + 1];
        return std::string_view(data.data + start, static_cast<size_t>(end - start));
    }

    template <typename CodeType>
    CodeType find_code(std::string_view needle, const char* dict_name) const {
        for (uint32_t code = 0; static_cast<size_t>(code + 1) < offsets.size(); ++code) {
            if (lookup(code) == needle) {
                return static_cast<CodeType>(code);
            }
        }
        throw std::runtime_error(std::string("dictionary value not found in ") + dict_name + ": " + std::string(needle));
    }
};

inline uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

inline uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}

inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

inline bool partitioned_pre_contains(const uint64_t* offsets,
                                     const TripleKey* keys,
                                     uint32_t adsh,
                                     uint32_t tag,
                                     uint32_t version) {
    const uint64_t hash = hash_triple(adsh, tag, version);
    const size_t partition = static_cast<size_t>(hash >> kTopShift);
    const uint64_t begin = offsets[partition];
    const uint64_t end = offsets[partition + 1];
    for (uint64_t idx = begin; idx < end; ++idx) {
        const TripleKey& candidate = keys[idx];
        if (candidate.a == adsh && candidate.b == tag && candidate.c == version) {
            return true;
        }
    }
    return false;
}

inline uint32_t* merge_two_runs(const uint32_t* left,
                                const uint32_t* left_end,
                                const uint32_t* right,
                                const uint32_t* right_end,
                                uint32_t* out) {
    while (left != left_end && right != right_end) {
        const uint32_t left_value = *left;
        const uint32_t right_value = *right;
        const bool take_left = left_value <= right_value;
        *out++ = take_left ? left_value : right_value;
        left += take_left;
        right += !take_left;
    }
    out = std::copy(left, left_end, out);
    return std::copy(right, right_end, out);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    omp_set_dynamic(0);

    try {
        GENDB_PHASE("total");

        MmapColumn<uint16_t> num_uom;
        MmapColumn<uint32_t> num_adsh;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<uint32_t> num_version;
        MmapColumn<double> num_value;

        MmapColumn<int32_t> num_ddate_postings_values;
        MmapColumn<uint64_t> num_ddate_postings_offsets;
        MmapColumn<uint32_t> num_ddate_postings_rowids;

        MmapColumn<uint64_t> pre_partition_offsets;
        MmapColumn<TripleKey> pre_partition_keys;

        DictView num_uom_dict;
        DictView global_tag_dict;
        DictView global_version_dict;

        std::vector<uint32_t> merged_rowids;
        std::vector<RangeSlice> morsels;
        CompactHashMap<uint64_t, AggState> global_rollup;

        {
            GENDB_PHASE("data_loading");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            num_ddate_postings_values.open(gendb_dir + "/indexes/num/num_ddate_postings.values.bin");
            num_ddate_postings_offsets.open(gendb_dir + "/indexes/num/num_ddate_postings.offsets.bin");
            num_ddate_postings_rowids.open(gendb_dir + "/indexes/num/num_ddate_postings.rowids.bin");

            pre_partition_offsets.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/offsets.bin");
            pre_partition_keys.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/keys.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");
            global_version_dict.open(gendb_dir + "/dicts/global_version");

            num_uom.advise_sequential();
            num_adsh.advise_sequential();
            num_tag.advise_sequential();
            num_version.advise_sequential();
            num_value.advise_sequential();
            num_ddate_postings_values.advise_sequential();
            num_ddate_postings_offsets.advise_sequential();
            num_ddate_postings_rowids.advise_sequential();
            pre_partition_offsets.advise_random();
            pre_partition_keys.advise_random();

            gendb::mmap_prefetch_all(
                num_uom,
                num_adsh,
                num_tag,
                num_version,
                num_value,
                num_ddate_postings_values,
                num_ddate_postings_offsets,
                num_ddate_postings_rowids,
                pre_partition_offsets,
                pre_partition_keys);

            if (num_uom.size() != num_adsh.size() ||
                num_tag.size() != num_adsh.size() ||
                num_version.size() != num_adsh.size() ||
                num_value.size() != num_adsh.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (pre_partition_offsets.size() != kPartitionCount + 1) {
                throw std::runtime_error("unexpected pre partition offsets size");
            }
            if (pre_partition_offsets[pre_partition_offsets.size() - 1] != pre_partition_keys.size()) {
                throw std::runtime_error("pre partition offsets/key size mismatch");
            }
        }

        {
            GENDB_PHASE("dim_filter");
            const int32_t* value_begin = num_ddate_postings_values.data;
            const int32_t* value_end = value_begin + num_ddate_postings_values.size();
            const int32_t* lo_it = std::lower_bound(value_begin, value_end, kDateLo);
            const int32_t* hi_it = std::upper_bound(value_begin, value_end, kDateHi);
            const size_t run_count = static_cast<size_t>(hi_it - lo_it);
            if (run_count == 0) {
                throw std::runtime_error("no date posting runs for 2023 range");
            }
            if (run_count != kExpectedDateRuns) {
                throw std::runtime_error("unexpected number of 2023 date runs");
            }

            std::vector<RowidSlice> runs(run_count);
            uint64_t total_candidates = 0;
            for (const int32_t* it = lo_it; it != hi_it; ++it) {
                const size_t group_idx = static_cast<size_t>(it - value_begin);
                const uint64_t begin = num_ddate_postings_offsets[group_idx];
                const uint64_t end = num_ddate_postings_offsets[group_idx + 1];
                const size_t run_idx = static_cast<size_t>(it - lo_it);
                runs[run_idx] = RowidSlice{
                    num_ddate_postings_rowids.data + begin,
                    num_ddate_postings_rowids.data + end,
                };
                total_candidates += end - begin;
            }

            std::vector<RunSpan> stage0(run_count);
            for (size_t run_idx = 0; run_idx < run_count; ++run_idx) {
                stage0[run_idx] = RunSpan{
                    runs[run_idx].ptr,
                    static_cast<uint32_t>(runs[run_idx].end - runs[run_idx].ptr),
                };
            }

            std::vector<uint32_t> buffer_a(static_cast<size_t>(total_candidates));
            std::vector<uint32_t> buffer_b(static_cast<size_t>(total_candidates));

            std::vector<RunSpan> stage1((run_count + 1) / 2);
            uint32_t stage1_offset = 0;
            for (size_t out_idx = 0; out_idx < stage1.size(); ++out_idx) {
                const size_t left_idx = out_idx * 2;
                const size_t right_idx = left_idx + 1;
                const uint32_t merged_size = stage0[left_idx].size + (right_idx < stage0.size() ? stage0[right_idx].size : 0u);
                stage1[out_idx] = RunSpan{buffer_a.data() + stage1_offset, merged_size};
                stage1_offset += merged_size;
            }

            #pragma omp parallel for schedule(static)
            for (int64_t out_idx = 0; out_idx < static_cast<int64_t>(stage1.size()); ++out_idx) {
                const size_t left_idx = static_cast<size_t>(out_idx) * 2;
                const size_t right_idx = left_idx + 1;
                uint32_t* out = const_cast<uint32_t*>(stage1[static_cast<size_t>(out_idx)].data);
                const RunSpan left = stage0[left_idx];
                if (right_idx < stage0.size()) {
                    const RunSpan right = stage0[right_idx];
                    merge_two_runs(left.data, left.data + left.size, right.data, right.data + right.size, out);
                } else {
                    std::copy(left.data, left.data + left.size, out);
                }
            }

            std::vector<RunSpan> stage2((stage1.size() + 1) / 2);
            uint32_t stage2_offset = 0;
            for (size_t out_idx = 0; out_idx < stage2.size(); ++out_idx) {
                const size_t left_idx = out_idx * 2;
                const size_t right_idx = left_idx + 1;
                const uint32_t merged_size = stage1[left_idx].size + (right_idx < stage1.size() ? stage1[right_idx].size : 0u);
                stage2[out_idx] = RunSpan{buffer_b.data() + stage2_offset, merged_size};
                stage2_offset += merged_size;
            }

            #pragma omp parallel for schedule(static)
            for (int64_t out_idx = 0; out_idx < static_cast<int64_t>(stage2.size()); ++out_idx) {
                const size_t left_idx = static_cast<size_t>(out_idx) * 2;
                const size_t right_idx = left_idx + 1;
                uint32_t* out = const_cast<uint32_t*>(stage2[static_cast<size_t>(out_idx)].data);
                const RunSpan left = stage1[left_idx];
                if (right_idx < stage1.size()) {
                    const RunSpan right = stage1[right_idx];
                    merge_two_runs(left.data, left.data + left.size, right.data, right.data + right.size, out);
                } else {
                    std::copy(left.data, left.data + left.size, out);
                }
            }

            const RunSpan merged01 = RunSpan{buffer_a.data(), stage2[0].size + stage2[1].size};
            merge_two_runs(stage2[0].data,
                           stage2[0].data + stage2[0].size,
                           stage2[1].data,
                           stage2[1].data + stage2[1].size,
                           const_cast<uint32_t*>(merged01.data));
            const RunSpan carried2 = RunSpan{buffer_a.data() + merged01.size, stage2[2].size};
            std::copy(stage2[2].data, stage2[2].data + stage2[2].size, const_cast<uint32_t*>(carried2.data));

            merged_rowids.resize(static_cast<size_t>(total_candidates));
            merge_two_runs(merged01.data,
                           merged01.data + merged01.size,
                           carried2.data,
                           carried2.data + carried2.size,
                           merged_rowids.data());

            for (uint32_t begin = 0; begin < merged_rowids.size(); begin += kMorselRows) {
                const uint32_t end = std::min<uint32_t>(begin + kMorselRows, static_cast<uint32_t>(merged_rowids.size()));
                morsels.push_back(RangeSlice{begin, end});
            }

            if (merged_rowids.empty()) {
                fs::create_directories(results_dir);
                std::ofstream out(results_dir + "/Q24.csv", std::ios::out | std::ios::trunc);
                out << "tag,version,cnt,total\n";
                return 0;
            }

        }

        {
            GENDB_PHASE("build_joins");
            global_rollup.reserve(1 << 14);
        }

        {
            GENDB_PHASE("main_scan");
            const int max_threads = omp_get_max_threads();
            std::vector<CompactHashMap<uint64_t, AggState>> local_rollups;
            local_rollups.reserve(static_cast<size_t>(max_threads));
            for (int tid = 0; tid < max_threads; ++tid) {
                local_rollups.emplace_back(kLocalAggReserve);
            }

            const uint16_t usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            const uint16_t* uom_data = num_uom.data;
            const uint32_t* adsh_data = num_adsh.data;
            const uint32_t* tag_data = num_tag.data;
            const uint32_t* version_data = num_version.data;
            const double* value_data = num_value.data;
            const uint32_t* rowids_data = merged_rowids.data();
            const uint64_t* pre_offsets = pre_partition_offsets.data;
            const TripleKey* pre_keys = pre_partition_keys.data;

            #pragma omp parallel
            {
                CompactHashMap<uint64_t, AggState>& local_rollup = local_rollups[omp_get_thread_num()];

                #pragma omp for schedule(static)
                for (int64_t morsel_idx = 0; morsel_idx < static_cast<int64_t>(morsels.size()); ++morsel_idx) {
                    const RangeSlice slice = morsels[static_cast<size_t>(morsel_idx)];
                    for (uint32_t pos = slice.begin; pos < slice.end; ++pos) {
                        const uint32_t rowid = rowids_data[pos];
                        if (uom_data[rowid] != usd_code) {
                            continue;
                        }

                        const double value = value_data[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint32_t adsh = adsh_data[rowid];
                        const uint32_t tag = tag_data[rowid];
                        const uint32_t version = version_data[rowid];
                        if (partitioned_pre_contains(pre_offsets, pre_keys, adsh, tag, version)) {
                            continue;
                        }

                        AggState& state = local_rollup[pack_tag_version(tag, version)];
                        state.count += 1;
                        state.sum += value;
                    }
                }
            }

            size_t total_local_groups = 0;
            for (const auto& local_rollup : local_rollups) {
                total_local_groups += local_rollup.size();
            }
            global_rollup.reserve(std::max<size_t>(1 << 14, total_local_groups));

            for (const auto& local_rollup : local_rollups) {
                for (auto it = local_rollup.begin(); it != local_rollup.end(); ++it) {
                    const auto kv = *it;
                    AggState& dst = global_rollup[kv.first];
                    dst.count += kv.second.count;
                    dst.sum += kv.second.sum;
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<OutputRow> rows;
            rows.reserve(global_rollup.size());
            for (auto it = global_rollup.begin(); it != global_rollup.end(); ++it) {
                const auto kv = *it;
                if (kv.second.count <= 10) {
                    continue;
                }
                rows.push_back(OutputRow{
                    static_cast<uint32_t>(kv.first >> 32),
                    static_cast<uint32_t>(kv.first & 0xffffffffu),
                    kv.second.count,
                    kv.second.sum,
                });
            }

            std::sort(rows.begin(), rows.end(), [&](const OutputRow& lhs, const OutputRow& rhs) {
                if (lhs.count != rhs.count) {
                    return lhs.count > rhs.count;
                }
                const std::string_view lhs_tag = global_tag_dict.lookup(lhs.tag);
                const std::string_view rhs_tag = global_tag_dict.lookup(rhs.tag);
                if (lhs_tag != rhs_tag) {
                    return lhs_tag < rhs_tag;
                }
                const std::string_view lhs_version = global_version_dict.lookup(lhs.version);
                const std::string_view rhs_version = global_version_dict.lookup(rhs.version);
                return lhs_version < rhs_version;
            });

            if (rows.size() > kTopK) {
                rows.resize(kTopK);
            }

            fs::create_directories(results_dir);
            std::ofstream out(results_dir + "/Q24.csv", std::ios::out | std::ios::trunc);
            if (!out) {
                throw std::runtime_error("cannot open output file");
            }

            out << "tag,version,cnt,total\n";
            out << std::fixed << std::setprecision(2);
            for (const OutputRow& row : rows) {
                out << global_tag_dict.lookup(row.tag) << ','
                    << global_version_dict.lookup(row.version) << ','
                    << row.count << ','
                    << row.sum << '\n';
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << '\n';
        return 1;
    }

    return 0;
}
