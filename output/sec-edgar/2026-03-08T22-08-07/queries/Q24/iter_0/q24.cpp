#include <algorithm>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <omp.h>

#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace fs = std::filesystem;

namespace {

using gendb::CompactHashMap;
using gendb::MmapColumn;

constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();
constexpr int32_t kDateLo = 19358;
constexpr int32_t kDateHi = 19722;

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
};

struct AggState {
    uint64_t count;
    double sum;
};

struct OutputRow {
    uint32_t tag;
    uint32_t version;
    uint64_t count;
    double sum;
};

struct RangeSlice {
    uint64_t begin;
    uint64_t end;
};

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    explicit DictView(const std::string& base_path)
        : offsets(base_path + ".offsets.bin"), data(base_path + ".data.bin") {}

    std::string_view lookup(uint32_t code) const {
        if (static_cast<size_t>(code + 1) >= offsets.size()) {
            throw std::out_of_range("dictionary code out of range");
        }
        const uint64_t start = offsets[code];
        const uint64_t end = offsets[code + 1];
        return std::string_view(data.data + start, end - start);
    }

    uint32_t find_code(std::string_view needle) const {
        for (size_t code = 0; code + 1 < offsets.size(); ++code) {
            if (lookup(static_cast<uint32_t>(code)) == needle) {
                return static_cast<uint32_t>(code);
            }
        }
        throw std::runtime_error("dictionary value not found: " + std::string(needle));
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

inline bool pre_contains(const TripleBucket* buckets, size_t bucket_mask,
                         uint32_t adsh, uint32_t tag, uint32_t version) {
    size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & bucket_mask);
    while (true) {
        const TripleBucket& bucket = buckets[slot];
        if (bucket.group_index == kEmptyGroup) {
            return false;
        }
        if (bucket.a == adsh && bucket.b == tag && bucket.c == version) {
            return true;
        }
        slot = (slot + 1) & bucket_mask;
    }
}

inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

inline bool bitset_contains(const std::vector<uint64_t>& bits, uint32_t rowid) {
    return (bits[rowid >> 6] >> (rowid & 63)) & 1ull;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        MmapColumn<uint32_t> num_adsh;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<uint32_t> num_version;
        MmapColumn<double> num_value;
        MmapColumn<uint16_t> num_uom_postings_values;
        MmapColumn<uint64_t> num_uom_postings_offsets;
        MmapColumn<uint32_t> num_uom_postings_rowids;
        MmapColumn<int32_t> num_ddate_postings_values;
        MmapColumn<uint64_t> num_ddate_postings_offsets;
        MmapColumn<uint32_t> num_ddate_postings_rowids;
        MmapColumn<TripleBucket> pre_hash_buckets;
        std::unique_ptr<DictView> num_uom_dict;
        std::unique_ptr<DictView> global_tag_dict;
        std::unique_ptr<DictView> global_version_dict;

        std::vector<uint64_t> usd_membership_bits;
        std::vector<RangeSlice> date_slices;

        {
            GENDB_PHASE("data_loading");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            num_ddate_postings_values.open(gendb_dir + "/indexes/num/num_ddate_postings.values.bin");
            num_ddate_postings_offsets.open(gendb_dir + "/indexes/num/num_ddate_postings.offsets.bin");
            num_ddate_postings_rowids.open(gendb_dir + "/indexes/num/num_ddate_postings.rowids.bin");

            pre_hash_buckets.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.hash.bin");

            num_uom_dict = std::make_unique<DictView>(gendb_dir + "/dicts/num_uom");
            global_tag_dict = std::make_unique<DictView>(gendb_dir + "/dicts/global_tag");
            global_version_dict = std::make_unique<DictView>(gendb_dir + "/dicts/global_version");

            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_value.advise_random();
            pre_hash_buckets.advise_random();

            gendb::mmap_prefetch_all(
                num_adsh,
                num_tag,
                num_version,
                num_value,
                num_uom_postings_values,
                num_uom_postings_offsets,
                num_uom_postings_rowids,
                num_ddate_postings_values,
                num_ddate_postings_offsets,
                num_ddate_postings_rowids,
                pre_hash_buckets,
                num_uom_dict->offsets,
                num_uom_dict->data,
                global_tag_dict->offsets,
                global_tag_dict->data,
                global_version_dict->offsets,
                global_version_dict->data);

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
        }

        {
            GENDB_PHASE("dim_filter");
            const uint32_t usd_code = num_uom_dict->find_code("USD");

            const auto* uom_begin = num_uom_postings_values.data;
            const auto* uom_end = num_uom_postings_values.data + num_uom_postings_values.size();
            const auto* uom_it = std::lower_bound(uom_begin, uom_end, static_cast<uint16_t>(usd_code));
            if (uom_it == uom_end || *uom_it != static_cast<uint16_t>(usd_code)) {
                throw std::runtime_error("USD code missing from num_uom_postings index");
            }
            const size_t uom_group = static_cast<size_t>(uom_it - uom_begin);
            const uint64_t usd_begin = num_uom_postings_offsets[uom_group];
            const uint64_t usd_end = num_uom_postings_offsets[uom_group + 1];

            usd_membership_bits.assign((num_adsh.size() + 63) >> 6, 0ull);
            for (uint64_t i = usd_begin; i < usd_end; ++i) {
                const uint32_t rowid = num_uom_postings_rowids[i];
                usd_membership_bits[rowid >> 6] |= (1ull << (rowid & 63));
            }

            const auto* date_begin = num_ddate_postings_values.data;
            const auto* date_end = num_ddate_postings_values.data + num_ddate_postings_values.size();
            const auto* lo_it = std::lower_bound(date_begin, date_end, kDateLo);
            const auto* hi_it = std::upper_bound(date_begin, date_end, kDateHi);
            const size_t lo_group = static_cast<size_t>(lo_it - date_begin);
            const size_t hi_group = static_cast<size_t>(hi_it - date_begin);

            date_slices.reserve(hi_group - lo_group);
            for (size_t group = lo_group; group < hi_group; ++group) {
                date_slices.push_back({num_ddate_postings_offsets[group], num_ddate_postings_offsets[group + 1]});
            }
        }

        size_t pre_bucket_mask = 0;
        {
            GENDB_PHASE("build_joins");
            if (pre_hash_buckets.size() == 0 || (pre_hash_buckets.size() & (pre_hash_buckets.size() - 1)) != 0) {
                throw std::runtime_error("pre hash bucket count must be power of two");
            }
            pre_bucket_mask = pre_hash_buckets.size() - 1;
        }

        CompactHashMap<uint64_t, AggState> global_agg(1 << 16);

        {
            GENDB_PHASE("main_scan");
            const int max_threads = omp_get_max_threads();
            std::vector<CompactHashMap<uint64_t, AggState>> thread_aggs;
            thread_aggs.reserve(max_threads);
            for (int i = 0; i < max_threads; ++i) {
                thread_aggs.emplace_back(1 << 15);
            }

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& local_agg = thread_aggs[tid];

                #pragma omp for schedule(dynamic, 1)
                for (size_t slice_idx = 0; slice_idx < date_slices.size(); ++slice_idx) {
                    const RangeSlice slice = date_slices[slice_idx];
                    for (uint64_t pos = slice.begin; pos < slice.end; ++pos) {
                        const uint32_t rowid = num_ddate_postings_rowids[pos];
                        if (!bitset_contains(usd_membership_bits, rowid)) {
                            continue;
                        }

                        const double value = num_value[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint32_t adsh = num_adsh[rowid];
                        const uint32_t tag = num_tag[rowid];
                        const uint32_t version = num_version[rowid];
                        if (pre_contains(pre_hash_buckets.data, pre_bucket_mask, adsh, tag, version)) {
                            continue;
                        }

                        AggState& state = local_agg[pack_tag_version(tag, version)];
                        state.count += 1;
                        state.sum += value;
                    }
                }
            }

            for (const auto& local_agg : thread_aggs) {
                for (auto it = local_agg.begin(); it != local_agg.end(); ++it) {
                    const auto kv = *it;
                    AggState& dst = global_agg[kv.first];
                    dst.count += kv.second.count;
                    dst.sum += kv.second.sum;
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<OutputRow> rows;
            rows.reserve(global_agg.size());
            for (auto it = global_agg.begin(); it != global_agg.end(); ++it) {
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

            std::sort(rows.begin(), rows.end(), [](const OutputRow& lhs, const OutputRow& rhs) {
                if (lhs.count != rhs.count) return lhs.count > rhs.count;
                if (lhs.tag != rhs.tag) return lhs.tag < rhs.tag;
                return lhs.version < rhs.version;
            });
            if (rows.size() > 100) {
                rows.resize(100);
            }

            fs::create_directories(results_dir);
            std::ofstream out(results_dir + "/Q24.csv", std::ios::out | std::ios::trunc);
            if (!out) {
                throw std::runtime_error("cannot open output file");
            }
            out << "tag,version,cnt,total\n";
            out << std::fixed << std::setprecision(2);
            for (const OutputRow& row : rows) {
                out << global_tag_dict->lookup(row.tag) << ','
                    << global_version_dict->lookup(row.version) << ','
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
