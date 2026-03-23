#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
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

constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();
constexpr int32_t kDateLo = 19358;
constexpr int32_t kDateHi = 19722;
constexpr size_t kProbePartitions = 16;
constexpr size_t kProbePartitionBits = 4;
constexpr size_t kTopK = 100;
constexpr size_t kLocalTripleReserve = 100000;

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
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
        if (offsets.size() == 0) {
            throw std::runtime_error("empty dictionary offsets: " + base_path);
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

inline size_t probe_region_from_hash(uint64_t hash, size_t bucket_mask, size_t region_shift) {
    return static_cast<size_t>((hash & bucket_mask) >> region_shift);
}

inline bool pre_contains_hashed(const TripleBucket* buckets, size_t bucket_mask,
                                uint64_t hash, uint32_t adsh, uint32_t tag, uint32_t version) {
    size_t slot = static_cast<size_t>(hash & bucket_mask);
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

class TripleAggMap {
public:
    struct Entry {
        uint32_t adsh = 0;
        uint32_t tag = 0;
        uint32_t version = 0;
        uint32_t pad = 0;
        uint64_t hash = 0;
        uint64_t count = 0;
        double sum = 0.0;
        uint8_t occupied = 0;
    };

    TripleAggMap() = default;

    explicit TripleAggMap(size_t expected) {
        reserve(expected);
    }

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) {
            cap <<= 1;
        }
        table_.assign(cap, Entry{});
        mask_ = cap - 1;
        count_ = 0;
    }

    size_t size() const {
        return count_;
    }

    const std::vector<Entry>& table() const {
        return table_;
    }

    void release() {
        std::vector<Entry>().swap(table_);
        mask_ = 0;
        count_ = 0;
    }

    void add_row(uint32_t adsh, uint32_t tag, uint32_t version, uint64_t hash, double value) {
        Entry& entry = find_or_insert(adsh, tag, version, hash);
        entry.count += 1;
        entry.sum += value;
    }

    void add_agg(uint32_t adsh, uint32_t tag, uint32_t version, uint64_t hash, uint64_t count, double sum) {
        Entry& entry = find_or_insert(adsh, tag, version, hash);
        entry.count += count;
        entry.sum += sum;
    }

private:
    Entry& find_or_insert(uint32_t adsh, uint32_t tag, uint32_t version, uint64_t hash) {
        if (table_.empty()) {
            reserve(16);
        } else if (count_ >= (table_.size() * 3) / 4) {
            rehash(table_.size() * 2);
        }

        size_t slot = static_cast<size_t>(hash & mask_);
        while (true) {
            Entry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = 1;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.hash = hash;
                entry.count = 0;
                entry.sum = 0.0;
                ++count_;
                return entry;
            }
            if (entry.hash == hash && entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    void rehash(size_t new_cap) {
        std::vector<Entry> old = std::move(table_);
        table_.assign(new_cap, Entry{});
        mask_ = new_cap - 1;
        count_ = 0;
        for (const Entry& entry : old) {
            if (!entry.occupied) {
                continue;
            }
            Entry& dst = find_or_insert(entry.adsh, entry.tag, entry.version, entry.hash);
            dst.count = entry.count;
            dst.sum = entry.sum;
        }
    }

    std::vector<Entry> table_;
    size_t mask_ = 0;
    size_t count_ = 0;
};

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
        DictView num_uom_dict;
        DictView global_tag_dict;
        DictView global_version_dict;

        std::vector<uint64_t> usd_membership_bits;
        std::vector<RangeSlice> date_slices;
        size_t pre_bucket_mask = 0;
        size_t pre_region_shift = 0;

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

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");
            global_version_dict.open(gendb_dir + "/dicts/global_version");

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }

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
                num_uom_dict.offsets,
                num_uom_dict.data,
                global_tag_dict.offsets,
                global_tag_dict.data,
                global_version_dict.offsets,
                global_version_dict.data);
        }

        {
            GENDB_PHASE("dim_filter");
            const uint16_t usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");

            const uint16_t* uom_begin = num_uom_postings_values.data;
            const uint16_t* uom_end = num_uom_postings_values.data + num_uom_postings_values.size();
            const uint16_t* uom_it = std::lower_bound(uom_begin, uom_end, usd_code);
            if (uom_it == uom_end || *uom_it != usd_code) {
                throw std::runtime_error("USD not found in num_uom_postings");
            }
            const size_t uom_group = static_cast<size_t>(uom_it - uom_begin);
            const uint64_t usd_begin = num_uom_postings_offsets[uom_group];
            const uint64_t usd_end = num_uom_postings_offsets[uom_group + 1];

            usd_membership_bits.assign((num_adsh.size() + 63) >> 6, 0ull);
            for (uint64_t idx = usd_begin; idx < usd_end; ++idx) {
                const uint32_t rowid = num_uom_postings_rowids[idx];
                usd_membership_bits[rowid >> 6] |= (1ull << (rowid & 63));
            }

            const int32_t* date_begin = num_ddate_postings_values.data;
            const int32_t* date_end = num_ddate_postings_values.data + num_ddate_postings_values.size();
            const int32_t* lo_it = std::lower_bound(date_begin, date_end, kDateLo);
            const int32_t* hi_it = std::upper_bound(date_begin, date_end, kDateHi);
            const size_t lo_group = static_cast<size_t>(lo_it - date_begin);
            const size_t hi_group = static_cast<size_t>(hi_it - date_begin);

            date_slices.reserve(hi_group - lo_group);
            for (size_t group = lo_group; group < hi_group; ++group) {
                date_slices.push_back(RangeSlice{
                    num_ddate_postings_offsets[group],
                    num_ddate_postings_offsets[group + 1],
                });
            }
        }

        {
            GENDB_PHASE("build_joins");
            if (pre_hash_buckets.size() == 0 || (pre_hash_buckets.size() & (pre_hash_buckets.size() - 1)) != 0) {
                throw std::runtime_error("pre hash bucket count must be a power of two");
            }
            pre_bucket_mask = pre_hash_buckets.size() - 1;

            size_t bucket_bits = 0;
            size_t bucket_count = pre_hash_buckets.size();
            while ((static_cast<size_t>(1) << bucket_bits) < bucket_count) {
                ++bucket_bits;
            }
            if (bucket_bits < kProbePartitionBits) {
                throw std::runtime_error("pre hash bucket count too small for partitioning");
            }
            pre_region_shift = bucket_bits - kProbePartitionBits;
        }

        CompactHashMap<uint64_t, AggState> global_rollup(1 << 14);

        {
            GENDB_PHASE("main_scan");
            const int max_threads = omp_get_max_threads();

            std::vector<TripleAggMap> local_triples;
            local_triples.reserve(static_cast<size_t>(max_threads));
            for (int tid = 0; tid < max_threads; ++tid) {
                local_triples.emplace_back(kLocalTripleReserve);
            }

            #pragma omp parallel
            {
                TripleAggMap& local_map = local_triples[omp_get_thread_num()];

                #pragma omp for schedule(dynamic, 1)
                for (int64_t slice_idx = 0; slice_idx < static_cast<int64_t>(date_slices.size()); ++slice_idx) {
                    const RangeSlice slice = date_slices[static_cast<size_t>(slice_idx)];
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
                        const uint64_t hash = hash_triple(adsh, tag, version);
                        local_map.add_row(adsh, tag, version, hash, value);
                    }
                }
            }

            size_t total_unique = 0;
            for (const TripleAggMap& local_map : local_triples) {
                total_unique += local_map.size();
            }

            std::array<TripleAggMap, kProbePartitions> region_triples;
            const size_t per_region_reserve = (total_unique / kProbePartitions) + 1024;
            for (size_t region = 0; region < kProbePartitions; ++region) {
                region_triples[region].reserve(per_region_reserve);
            }

            for (TripleAggMap& local_map : local_triples) {
                for (const TripleAggMap::Entry& entry : local_map.table()) {
                    if (!entry.occupied) {
                        continue;
                    }
                    const size_t region = probe_region_from_hash(entry.hash, pre_bucket_mask, pre_region_shift);
                    region_triples[region].add_agg(
                        entry.adsh,
                        entry.tag,
                        entry.version,
                        entry.hash,
                        entry.count,
                        entry.sum);
                }
                local_map.release();
            }

            for (size_t region = 0; region < kProbePartitions; ++region) {
                const std::vector<TripleAggMap::Entry>& region_table = region_triples[region].table();

                std::vector<CompactHashMap<uint64_t, AggState>> thread_rollups;
                thread_rollups.reserve(static_cast<size_t>(max_threads));
                for (int tid = 0; tid < max_threads; ++tid) {
                    thread_rollups.emplace_back(1 << 10);
                }

                #pragma omp parallel
                {
                    CompactHashMap<uint64_t, AggState>& local_rollup = thread_rollups[omp_get_thread_num()];

                    #pragma omp for schedule(static)
                    for (int64_t slot = 0; slot < static_cast<int64_t>(region_table.size()); ++slot) {
                        const TripleAggMap::Entry& entry = region_table[static_cast<size_t>(slot)];
                        if (!entry.occupied) {
                            continue;
                        }
                        if (pre_contains_hashed(pre_hash_buckets.data, pre_bucket_mask,
                                                entry.hash, entry.adsh, entry.tag, entry.version)) {
                            continue;
                        }
                        AggState& state = local_rollup[pack_tag_version(entry.tag, entry.version)];
                        state.count += entry.count;
                        state.sum += entry.sum;
                    }
                }

                for (const CompactHashMap<uint64_t, AggState>& local_rollup : thread_rollups) {
                    for (auto it = local_rollup.begin(); it != local_rollup.end(); ++it) {
                        const auto kv = *it;
                        AggState& dst = global_rollup[kv.first];
                        dst.count += kv.second.count;
                        dst.sum += kv.second.sum;
                    }
                }

                region_triples[region].release();
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

            std::sort(rows.begin(), rows.end(), [](const OutputRow& lhs, const OutputRow& rhs) {
                if (lhs.count != rhs.count) {
                    return lhs.count > rhs.count;
                }
                if (lhs.tag != rhs.tag) {
                    return lhs.tag < rhs.tag;
                }
                return lhs.version < rhs.version;
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
