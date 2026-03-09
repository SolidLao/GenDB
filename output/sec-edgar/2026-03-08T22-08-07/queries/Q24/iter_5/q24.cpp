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
constexpr size_t kTopK = 100;
constexpr uint64_t kMorselRows = 1ull << 15;
constexpr size_t kLocalAggReserve = 1 << 13;

struct PairKey {
    uint32_t tag;
    uint32_t version;
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

struct RangeSlice {
    uint64_t begin;
    uint64_t end;
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
        const uint64_t begin = offsets[idx];
        const uint64_t end = offsets[idx + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
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

inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

inline bool bitset_contains(const std::vector<uint64_t>& bits, uint32_t rowid) {
    return ((bits[rowid >> 6] >> (rowid & 63)) & 1ull) != 0;
}

inline void bitset_set(std::vector<uint64_t>& bits, uint32_t rowid) {
    bits[rowid >> 6] |= 1ull << (rowid & 63);
}

inline bool pair_less(const PairKey& lhs, const PairKey& rhs) {
    return (lhs.tag < rhs.tag) || (lhs.tag == rhs.tag && lhs.version < rhs.version);
}

inline bool pair_equals(const PairKey& lhs, const PairKey& rhs) {
    return lhs.tag == rhs.tag && lhs.version == rhs.version;
}

inline bool adsh_slice_contains(const PairKey* begin, const PairKey* end, uint32_t tag, uint32_t version) {
    const PairKey needle{tag, version};
    const PairKey* it = std::lower_bound(begin, end, needle, [](const PairKey& lhs, const PairKey& rhs) {
        return pair_less(lhs, rhs);
    });
    return it != end && pair_equals(*it, needle);
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

        MmapColumn<uint64_t> pre_adsh_offsets;
        MmapColumn<PairKey> pre_adsh_pairs;

        DictView num_uom_dict;
        DictView global_tag_dict;
        DictView global_version_dict;

        std::vector<uint64_t> usd_membership_bits;
        std::vector<RangeSlice> morsels;
        CompactHashMap<uint64_t, AggState> global_rollup;

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

            pre_adsh_offsets.open(gendb_dir + "/column_versions/pre.adsh_tag_version.by_adsh_pairs/offsets.bin");
            pre_adsh_pairs.open(gendb_dir + "/column_versions/pre.adsh_tag_version.by_adsh_pairs/pairs.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");
            global_version_dict.open(gendb_dir + "/dicts/global_version");

            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_value.advise_random();
            num_uom_postings_rowids.advise_sequential();
            num_ddate_postings_values.advise_sequential();
            num_ddate_postings_offsets.advise_sequential();
            num_ddate_postings_rowids.advise_sequential();
            pre_adsh_offsets.advise_random();
            pre_adsh_pairs.advise_random();
        }

        {
            GENDB_PHASE("dim_filter");
            const uint16_t usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");

            const uint16_t* uom_begin = num_uom_postings_values.data;
            const uint16_t* uom_end = uom_begin + num_uom_postings_values.size();
            const uint16_t* usd_it = std::lower_bound(uom_begin, uom_end, usd_code);
            if (usd_it == uom_end || *usd_it != usd_code) {
                throw std::runtime_error("USD code missing from num_uom_postings index");
            }

            const size_t usd_group = static_cast<size_t>(usd_it - uom_begin);
            const uint64_t usd_begin = num_uom_postings_offsets[usd_group];
            const uint64_t usd_end = num_uom_postings_offsets[usd_group + 1];

            usd_membership_bits.assign((num_adsh.size() + 63) >> 6, 0);
            for (uint64_t pos = usd_begin; pos < usd_end; ++pos) {
                bitset_set(usd_membership_bits, num_uom_postings_rowids[pos]);
            }

            const int32_t* ddate_begin = num_ddate_postings_values.data;
            const int32_t* ddate_end = ddate_begin + num_ddate_postings_values.size();
            const int32_t* lo_it = std::lower_bound(ddate_begin, ddate_end, kDateLo);
            const int32_t* hi_it = std::upper_bound(ddate_begin, ddate_end, kDateHi);

            uint64_t candidate_rows = 0;
            for (const int32_t* it = lo_it; it != hi_it; ++it) {
                const size_t group_idx = static_cast<size_t>(it - ddate_begin);
                const uint64_t begin = num_ddate_postings_offsets[group_idx];
                const uint64_t end = num_ddate_postings_offsets[group_idx + 1];
                candidate_rows += (end - begin);
            }

            morsels.reserve(static_cast<size_t>((candidate_rows + kMorselRows - 1) / kMorselRows));
            for (const int32_t* it = lo_it; it != hi_it; ++it) {
                const size_t group_idx = static_cast<size_t>(it - ddate_begin);
                uint64_t begin = num_ddate_postings_offsets[group_idx];
                const uint64_t end = num_ddate_postings_offsets[group_idx + 1];
                while (begin < end) {
                    const uint64_t chunk_end = std::min(begin + kMorselRows, end);
                    morsels.push_back(RangeSlice{begin, chunk_end});
                    begin = chunk_end;
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            if (pre_adsh_offsets.size() < 2) {
                throw std::runtime_error("pre adsh offsets extension is too small");
            }
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

            const uint32_t* adsh_data = num_adsh.data;
            const uint32_t* tag_data = num_tag.data;
            const uint32_t* version_data = num_version.data;
            const double* value_data = num_value.data;
            const uint32_t* ddate_rowids = num_ddate_postings_rowids.data;
            const uint64_t* adsh_offsets = pre_adsh_offsets.data;
            const PairKey* adsh_pairs = pre_adsh_pairs.data;
            const size_t adsh_offset_count = pre_adsh_offsets.size();

            #pragma omp parallel
            {
                CompactHashMap<uint64_t, AggState>& local_rollup = local_rollups[omp_get_thread_num()];
                uint32_t cached_adsh = std::numeric_limits<uint32_t>::max();
                const PairKey* cached_begin = nullptr;
                const PairKey* cached_end = nullptr;

                #pragma omp for schedule(dynamic, 1)
                for (int64_t morsel_idx = 0; morsel_idx < static_cast<int64_t>(morsels.size()); ++morsel_idx) {
                    const RangeSlice slice = morsels[static_cast<size_t>(morsel_idx)];
                    for (uint64_t pos = slice.begin; pos < slice.end; ++pos) {
                        const uint32_t rowid = ddate_rowids[pos];
                        if (!bitset_contains(usd_membership_bits, rowid)) {
                            continue;
                        }

                        const uint32_t adsh = adsh_data[rowid];
                        if (adsh != cached_adsh) {
                            cached_adsh = adsh;
                            if (static_cast<size_t>(adsh) + 1 < adsh_offset_count) {
                                const uint64_t begin = adsh_offsets[adsh];
                                const uint64_t end = adsh_offsets[adsh + 1];
                                cached_begin = adsh_pairs + begin;
                                cached_end = adsh_pairs + end;
                            } else {
                                cached_begin = adsh_pairs;
                                cached_end = adsh_pairs;
                            }
                        }

                        const uint32_t tag = tag_data[rowid];
                        const uint32_t version = version_data[rowid];
                        if (cached_begin != cached_end && adsh_slice_contains(cached_begin, cached_end, tag, version)) {
                            continue;
                        }

                        const double value = value_data[rowid];
                        if (std::isnan(value)) {
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
