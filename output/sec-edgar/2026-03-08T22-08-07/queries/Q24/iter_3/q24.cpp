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
constexpr uint64_t kMorselRows = 1ull << 15;
constexpr size_t kLocalAggReserve = 1 << 13;

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct CandidateRow {
    double value;
    uint32_t adsh;
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

inline bool bitset_contains(const std::vector<uint64_t>& bits, uint32_t rowid) {
    return ((bits[rowid >> 6] >> (rowid & 63)) & 1ull) != 0;
}

inline bool triple_less(const CandidateRow& lhs, const CandidateRow& rhs) {
    if (lhs.adsh != rhs.adsh) {
        return lhs.adsh < rhs.adsh;
    }
    if (lhs.tag != rhs.tag) {
        return lhs.tag < rhs.tag;
    }
    return lhs.version < rhs.version;
}

inline void sort_partition_rows(CandidateRow* rows, size_t count) {
    if (count < 2) {
        return;
    }
    if (count <= 64) {
        for (size_t i = 1; i < count; ++i) {
            CandidateRow key = rows[i];
            size_t j = i;
            while (j > 0 && triple_less(key, rows[j - 1])) {
                rows[j] = rows[j - 1];
                --j;
            }
            rows[j] = key;
        }
        return;
    }
    std::sort(rows, rows + count, triple_less);
}

inline bool pre_partition_contains(const TripleKey* pre_keys,
                                   uint64_t pre_begin,
                                   uint64_t pre_end,
                                   uint32_t adsh,
                                   uint32_t tag,
                                   uint32_t version) {
    for (uint64_t idx = pre_begin; idx < pre_end; ++idx) {
        const TripleKey& candidate = pre_keys[idx];
        if (candidate.a == adsh && candidate.b == tag && candidate.c == version) {
            return true;
        }
    }
    return false;
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

        MmapColumn<uint64_t> pre_partition_offsets;
        MmapColumn<TripleKey> pre_partition_keys;

        DictView num_uom_dict;
        DictView global_tag_dict;
        DictView global_version_dict;

        std::vector<uint64_t> usd_membership_bits;
        std::vector<RangeSlice> morsels;
        std::vector<uint32_t> partition_positions;
        std::vector<uint32_t> partition_offsets;
        std::vector<CandidateRow> partitioned_rows;
        std::vector<uint32_t> nonempty_partitions;
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

            pre_partition_offsets.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/offsets.bin");
            pre_partition_keys.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/keys.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");
            global_version_dict.open(gendb_dir + "/dicts/global_version");

            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_value.advise_random();
            pre_partition_offsets.advise_random();
            pre_partition_keys.advise_random();

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
                pre_partition_offsets,
                pre_partition_keys);

            if (num_tag.size() != num_adsh.size() ||
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
            const uint16_t usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            const uint16_t* uom_begin = num_uom_postings_values.data;
            const uint16_t* uom_end = uom_begin + num_uom_postings_values.size();
            const uint16_t* usd_it = std::lower_bound(uom_begin, uom_end, usd_code);
            if (usd_it == uom_end || *usd_it != usd_code) {
                throw std::runtime_error("USD code missing from num_uom_postings");
            }
            const size_t usd_idx = static_cast<size_t>(usd_it - uom_begin);
            const uint64_t usd_begin = num_uom_postings_offsets[usd_idx];
            const uint64_t usd_end = num_uom_postings_offsets[usd_idx + 1];

            usd_membership_bits.assign((num_adsh.size() + 63) >> 6, 0ull);
            for (uint64_t pos = usd_begin; pos < usd_end; ++pos) {
                const uint32_t rowid = num_uom_postings_rowids[pos];
                usd_membership_bits[rowid >> 6] |= 1ull << (rowid & 63);
            }

            const int32_t* ddate_begin = num_ddate_postings_values.data;
            const int32_t* ddate_end = ddate_begin + num_ddate_postings_values.size();
            const int32_t* lo_it = std::lower_bound(ddate_begin, ddate_end, kDateLo);
            const int32_t* hi_it = std::upper_bound(ddate_begin, ddate_end, kDateHi);
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

        const int num_threads = omp_get_max_threads();

        {
            GENDB_PHASE("build_joins");
            partition_positions.assign(static_cast<size_t>(num_threads) * kPartitionCount, 0u);
            partition_offsets.assign(kPartitionCount + 1, 0u);
            global_rollup.reserve(1 << 14);
        }

        {
            GENDB_PHASE("main_scan");
            const uint32_t* adsh_data = num_adsh.data;
            const uint32_t* tag_data = num_tag.data;
            const uint32_t* version_data = num_version.data;
            const double* value_data = num_value.data;
            const uint32_t* ddate_rowids = num_ddate_postings_rowids.data;
            const uint64_t* pre_offsets = pre_partition_offsets.data;
            const TripleKey* pre_keys = pre_partition_keys.data;

            #pragma omp parallel num_threads(num_threads)
            {
                const int tid = omp_get_thread_num();
                uint32_t* local_counts = partition_positions.data() + static_cast<size_t>(tid) * kPartitionCount;

                #pragma omp for schedule(static)
                for (int64_t morsel_idx = 0; morsel_idx < static_cast<int64_t>(morsels.size()); ++morsel_idx) {
                    const RangeSlice slice = morsels[static_cast<size_t>(morsel_idx)];
                    for (uint64_t pos = slice.begin; pos < slice.end; ++pos) {
                        const uint32_t rowid = ddate_rowids[pos];
                        if (!bitset_contains(usd_membership_bits, rowid)) {
                            continue;
                        }

                        const double value = value_data[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint64_t hash = hash_triple(adsh_data[rowid], tag_data[rowid], version_data[rowid]);
                        const size_t partition = static_cast<size_t>(hash >> kTopShift);
                        ++local_counts[partition];
                    }
                }
            }

            uint32_t total_filtered = 0;
            for (size_t partition = 0; partition < kPartitionCount; ++partition) {
                uint32_t partition_count = 0;
                for (int tid = 0; tid < num_threads; ++tid) {
                    partition_count += partition_positions[static_cast<size_t>(tid) * kPartitionCount + partition];
                }
                partition_offsets[partition] = total_filtered;
                total_filtered += partition_count;
            }
            partition_offsets[kPartitionCount] = total_filtered;

            partitioned_rows.resize(total_filtered);
            nonempty_partitions.reserve(kPartitionCount / 2);
            for (size_t partition = 0; partition < kPartitionCount; ++partition) {
                uint32_t write_pos = partition_offsets[partition];
                for (int tid = 0; tid < num_threads; ++tid) {
                    uint32_t& slot = partition_positions[static_cast<size_t>(tid) * kPartitionCount + partition];
                    const uint32_t count = slot;
                    slot = write_pos;
                    write_pos += count;
                }
                if (partition_offsets[partition] != partition_offsets[partition + 1]) {
                    nonempty_partitions.push_back(static_cast<uint32_t>(partition));
                }
            }

            #pragma omp parallel num_threads(num_threads)
            {
                const int tid = omp_get_thread_num();
                uint32_t* local_positions = partition_positions.data() + static_cast<size_t>(tid) * kPartitionCount;

                #pragma omp for schedule(static)
                for (int64_t morsel_idx = 0; morsel_idx < static_cast<int64_t>(morsels.size()); ++morsel_idx) {
                    const RangeSlice slice = morsels[static_cast<size_t>(morsel_idx)];
                    for (uint64_t pos = slice.begin; pos < slice.end; ++pos) {
                        const uint32_t rowid = ddate_rowids[pos];
                        if (!bitset_contains(usd_membership_bits, rowid)) {
                            continue;
                        }

                        const double value = value_data[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint32_t adsh = adsh_data[rowid];
                        const uint32_t tag = tag_data[rowid];
                        const uint32_t version = version_data[rowid];
                        const uint64_t hash = hash_triple(adsh, tag, version);
                        const size_t partition = static_cast<size_t>(hash >> kTopShift);
                        const uint32_t dst = local_positions[partition]++;
                        partitioned_rows[dst] = CandidateRow{value, adsh, tag, version};
                    }
                }
            }

            std::vector<CompactHashMap<uint64_t, AggState>> local_rollups;
            local_rollups.reserve(static_cast<size_t>(num_threads));
            for (int tid = 0; tid < num_threads; ++tid) {
                local_rollups.emplace_back(kLocalAggReserve);
            }

            #pragma omp parallel num_threads(num_threads)
            {
                CompactHashMap<uint64_t, AggState>& local_rollup = local_rollups[omp_get_thread_num()];

                #pragma omp for schedule(dynamic, 1024)
                for (int64_t idx = 0; idx < static_cast<int64_t>(nonempty_partitions.size()); ++idx) {
                    const uint32_t partition = nonempty_partitions[static_cast<size_t>(idx)];
                    const uint32_t begin = partition_offsets[partition];
                    const uint32_t end = partition_offsets[partition + 1];
                    CandidateRow* rows = partitioned_rows.data() + begin;
                    const size_t row_count = static_cast<size_t>(end - begin);
                    sort_partition_rows(rows, row_count);

                    const uint64_t pre_begin = pre_offsets[partition];
                    const uint64_t pre_end = pre_offsets[partition + 1];

                    size_t i = 0;
                    while (i < row_count) {
                        const uint32_t adsh = rows[i].adsh;
                        const uint32_t tag = rows[i].tag;
                        const uint32_t version = rows[i].version;
                        uint64_t count = 0;
                        double sum = 0.0;
                        do {
                            ++count;
                            sum += rows[i].value;
                            ++i;
                        } while (i < row_count &&
                                 rows[i].adsh == adsh &&
                                 rows[i].tag == tag &&
                                 rows[i].version == version);

                        if (pre_partition_contains(pre_keys, pre_begin, pre_end, adsh, tag, version)) {
                            continue;
                        }

                        AggState& state = local_rollup[pack_tag_version(tag, version)];
                        state.count += count;
                        state.sum += sum;
                    }
                }
            }

            size_t total_groups = 0;
            for (const auto& local_rollup : local_rollups) {
                total_groups += local_rollup.size();
            }
            global_rollup.reserve(std::max<size_t>(1 << 14, total_groups));
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
