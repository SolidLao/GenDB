#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMax = 4999;
constexpr uint32_t kSubMissing = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();
constexpr int kMaxThreads = 64;
constexpr int kMorselRows = 262144;

struct PairBucket {
    uint32_t a;
    uint32_t b;
    uint64_t group_index;
};

static_assert(sizeof(PairBucket) == 16, "unexpected PairBucket size");

struct TripleCountBucket {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
    uint32_t count;
};

struct GroupKey {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;

    bool operator==(const GroupKey& other) const {
        return sic == other.sic && tlabel == other.tlabel && stmt == other.stmt;
    }
};

struct DistinctKey {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;
    int32_t cik;

    bool operator==(const DistinctKey& other) const {
        return sic == other.sic && tlabel == other.tlabel && stmt == other.stmt && cik == other.cik;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const {
        uint64_t seed = static_cast<uint32_t>(key.sic);
        seed ^= static_cast<uint64_t>(key.tlabel) + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2);
        seed ^= static_cast<uint64_t>(key.stmt) + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2);
        return static_cast<size_t>(seed);
    }
};

struct DistinctKeyHash {
    size_t operator()(const DistinctKey& key) const {
        uint64_t seed = static_cast<uint32_t>(key.sic);
        seed ^= static_cast<uint64_t>(key.tlabel) + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2);
        seed ^= static_cast<uint64_t>(key.stmt) + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2);
        seed ^= static_cast<uint64_t>(static_cast<uint32_t>(key.cik)) + 0x9e3779b97f4a7c15ull +
                (seed << 6) + (seed >> 2);
        return static_cast<size_t>(seed);
    }
};

struct AggState {
    double sum_value = 0.0;
    uint64_t count_rows = 0;
};

struct FinalState {
    double sum_value = 0.0;
    uint64_t count_rows = 0;
    uint64_t num_companies = 0;
};

struct ResultRow {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;
    uint64_t num_companies;
    double total_value;
    double avg_value;
};

struct LocalState {
    std::unordered_map<GroupKey, AggState, GroupKeyHash> agg;
    std::unordered_set<DistinctKey, DistinctKeyHash> distinct;
};

template <typename T>
void require_non_empty(const char* name, const gendb::MmapColumn<T>& col) {
    if (col.empty()) {
        throw std::runtime_error(std::string("empty file: ") + name);
    }
}

inline uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

inline uint64_t hash_pair(uint32_t a, uint32_t b) {
    return mix64((static_cast<uint64_t>(a) << 32) ^ static_cast<uint64_t>(b));
}

inline uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}

size_t next_power_of_two(size_t n) {
    size_t out = 1;
    while (out < n) out <<= 1;
    return out;
}

class TripleCountMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, TripleCountBucket{std::numeric_limits<uint32_t>::max(), 0, 0, 0});
        mask_ = capacity - 1;
    }

    void insert_or_increment(uint32_t adsh, uint32_t tag, uint32_t version) {
        size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & mask_);
        while (true) {
            TripleCountBucket& bucket = buckets_[slot];
            if (bucket.adsh == std::numeric_limits<uint32_t>::max()) {
                bucket.adsh = adsh;
                bucket.tag = tag;
                bucket.version = version;
                bucket.count = 1;
                return;
            }
            if (bucket.adsh == adsh && bucket.tag == tag && bucket.version == version) {
                ++bucket.count;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    uint32_t find_count(uint32_t adsh, uint32_t tag, uint32_t version) const {
        size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & mask_);
        while (true) {
            const TripleCountBucket& bucket = buckets_[slot];
            if (bucket.adsh == std::numeric_limits<uint32_t>::max()) return 0;
            if (bucket.adsh == adsh && bucket.tag == tag && bucket.version == version) return bucket.count;
            slot = (slot + 1) & mask_;
        }
    }

private:
    std::vector<TripleCountBucket> buckets_;
    size_t mask_ = 0;
};

std::string_view dict_value(const gendb::MmapColumn<uint64_t>& offsets,
                            const gendb::MmapColumn<char>& data,
                            uint32_t code) {
    if (static_cast<size_t>(code + 1) >= offsets.size()) {
        throw std::runtime_error("dictionary code out of bounds");
    }
    const uint64_t begin = offsets[code];
    const uint64_t end = offsets[code + 1];
    return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
}

uint32_t find_dict_code(const gendb::MmapColumn<uint64_t>& offsets,
                        const gendb::MmapColumn<char>& data,
                        std::string_view needle) {
    for (uint32_t code = 0; static_cast<size_t>(code + 1) < offsets.size(); ++code) {
        if (dict_value(offsets, data, code) == needle) return code;
    }
    throw std::runtime_error(std::string("dictionary value not found: ") + std::string(needle));
}

template <typename T>
bool find_posting_group(const gendb::MmapColumn<T>& values,
                        const gendb::MmapColumn<uint64_t>& offsets,
                        T needle,
                        uint64_t& begin,
                        uint64_t& end) {
    const T* it = std::lower_bound(values.data, values.data + values.size(), needle);
    if (it == values.data + values.size() || *it != needle) return false;
    const size_t group = static_cast<size_t>(it - values.data);
    begin = offsets[group];
    end = offsets[group + 1];
    return true;
}

uint32_t probe_tag_rowid(const gendb::MmapColumn<PairBucket>& buckets,
                        const gendb::MmapColumn<uint64_t>& offsets,
                        const gendb::MmapColumn<uint32_t>& rowids,
                        const gendb::MmapColumn<int8_t>& tag_abstract,
                        uint32_t tag,
                        uint32_t version) {
    const size_t mask = buckets.size() - 1;
    size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask);
    while (true) {
        const PairBucket& bucket = buckets[slot];
        if (bucket.group_index == kEmptyGroup) return kSubMissing;
        if (bucket.a == tag && bucket.b == version) {
            const uint64_t begin = offsets[bucket.group_index];
            const uint64_t end = offsets[bucket.group_index + 1];
            for (uint64_t i = begin; i < end; ++i) {
                const uint32_t rowid = rowids[i];
                if (tag_abstract[rowid] == 0) return rowid;
            }
            return kSubMissing;
        }
        slot = (slot + 1) & mask;
    }
}

void write_csv_field(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char c : value) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (!needs_quotes) {
        std::fwrite(value.data(), 1, value.size(), out);
        return;
    }
    std::fputc('"', out);
    for (char c : value) {
        if (c == '"') std::fputc('"', out);
        std::fputc(c, out);
    }
    std::fputc('"', out);
}

int plan_thread_count() {
    return std::max(1, std::min(kMaxThreads, omp_get_num_procs()));
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        const int thread_count = plan_thread_count();
        omp_set_num_threads(thread_count);

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;

        gendb::MmapColumn<uint32_t> tag_tlabel;
        gendb::MmapColumn<int8_t> tag_abstract;

        gendb::MmapColumn<uint64_t> global_adsh_offsets;
        gendb::MmapColumn<uint64_t> num_uom_dict_offsets;
        gendb::MmapColumn<char> num_uom_dict_data;
        gendb::MmapColumn<uint64_t> pre_stmt_dict_offsets;
        gendb::MmapColumn<char> pre_stmt_dict_data;
        gendb::MmapColumn<uint64_t> tag_tlabel_dict_offsets;
        gendb::MmapColumn<char> tag_tlabel_dict_data;

        gendb::MmapColumn<int32_t> sub_sic_postings_values;
        gendb::MmapColumn<uint64_t> sub_sic_postings_offsets;
        gendb::MmapColumn<uint32_t> sub_sic_postings_rowids;

        gendb::MmapColumn<uint16_t> num_uom_postings_values;
        gendb::MmapColumn<uint64_t> num_uom_postings_offsets;
        gendb::MmapColumn<uint32_t> num_uom_postings_rowids;

        gendb::MmapColumn<uint16_t> pre_stmt_postings_values;
        gendb::MmapColumn<uint64_t> pre_stmt_postings_offsets;
        gendb::MmapColumn<uint32_t> pre_stmt_postings_rowids;

        gendb::MmapColumn<PairBucket> tag_hash_buckets;
        gendb::MmapColumn<uint64_t> tag_hash_offsets;
        gendb::MmapColumn<uint32_t> tag_hash_rowids;

        uint16_t usd_code = 0;
        uint16_t eq_code = 0;
        std::vector<uint32_t> sub_lookup;
        TripleCountMap pre_eq_counts;
        uint64_t usd_rowid_begin = 0;
        uint64_t usd_rowid_end = 0;

        {
            GENDB_PHASE("data_loading");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");

            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");
            tag_abstract.open(gendb_dir + "/tag/abstract.bin");

            global_adsh_offsets.open(gendb_dir + "/dicts/global_adsh.offsets.bin");
            num_uom_dict_offsets.open(gendb_dir + "/dicts/num_uom.offsets.bin");
            num_uom_dict_data.open(gendb_dir + "/dicts/num_uom.data.bin");
            pre_stmt_dict_offsets.open(gendb_dir + "/dicts/pre_stmt.offsets.bin");
            pre_stmt_dict_data.open(gendb_dir + "/dicts/pre_stmt.data.bin");
            tag_tlabel_dict_offsets.open(gendb_dir + "/dicts/tag_tlabel.offsets.bin");
            tag_tlabel_dict_data.open(gendb_dir + "/dicts/tag_tlabel.data.bin");

            sub_sic_postings_values.open(gendb_dir + "/indexes/sub/sub_sic_postings.values.bin");
            sub_sic_postings_offsets.open(gendb_dir + "/indexes/sub/sub_sic_postings.offsets.bin");
            sub_sic_postings_rowids.open(gendb_dir + "/indexes/sub/sub_sic_postings.rowids.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            pre_stmt_postings_values.open(gendb_dir + "/indexes/pre/pre_stmt_postings.values.bin");
            pre_stmt_postings_offsets.open(gendb_dir + "/indexes/pre/pre_stmt_postings.offsets.bin");
            pre_stmt_postings_rowids.open(gendb_dir + "/indexes/pre/pre_stmt_postings.rowids.bin");

            tag_hash_buckets.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.hash.bin");
            tag_hash_offsets.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.offsets.bin");
            tag_hash_rowids.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.rowids.bin");

            require_non_empty("num/adsh.bin", num_adsh);
            require_non_empty("num/tag.bin", num_tag);
            require_non_empty("num/version.bin", num_version);
            require_non_empty("num/value.bin", num_value);
            require_non_empty("sub/adsh.bin", sub_adsh);
            require_non_empty("pre/adsh.bin", pre_adsh);
            require_non_empty("tag/tlabel.bin", tag_tlabel);

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_adsh.size() != sub_sic.size() || sub_adsh.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size()) {
                throw std::runtime_error("pre column size mismatch");
            }

            num_adsh.advise_sequential();
            num_tag.advise_sequential();
            num_version.advise_sequential();
            num_value.advise_sequential();

            sub_adsh.advise_random();
            sub_sic.advise_random();
            sub_cik.advise_random();
            pre_adsh.advise_random();
            pre_tag.advise_random();
            pre_version.advise_random();
            tag_tlabel.advise_random();
            tag_abstract.advise_random();
            tag_hash_buckets.advise_random();
            tag_hash_offsets.advise_random();
            tag_hash_rowids.advise_random();

            gendb::mmap_prefetch_all(num_adsh,
                                     num_tag,
                                     num_version,
                                     num_value,
                                     sub_adsh,
                                     sub_sic,
                                     sub_cik,
                                     pre_adsh,
                                     pre_tag,
                                     pre_version,
                                     tag_tlabel,
                                     tag_abstract,
                                     sub_sic_postings_values,
                                     sub_sic_postings_offsets,
                                     sub_sic_postings_rowids,
                                     num_uom_postings_values,
                                     num_uom_postings_offsets,
                                     num_uom_postings_rowids,
                                     pre_stmt_postings_values,
                                     pre_stmt_postings_offsets,
                                     pre_stmt_postings_rowids,
                                     tag_hash_buckets,
                                     tag_hash_offsets,
                                     tag_hash_rowids,
                                     global_adsh_offsets,
                                     num_uom_dict_offsets,
                                     num_uom_dict_data,
                                     pre_stmt_dict_offsets,
                                     pre_stmt_dict_data,
                                     tag_tlabel_dict_offsets,
                                     tag_tlabel_dict_data);
        }

        {
            GENDB_PHASE("dim_filter");

            usd_code = static_cast<uint16_t>(find_dict_code(num_uom_dict_offsets, num_uom_dict_data, "USD"));
            eq_code = static_cast<uint16_t>(find_dict_code(pre_stmt_dict_offsets, pre_stmt_dict_data, "EQ"));

            if (!find_posting_group(num_uom_postings_values,
                                    num_uom_postings_offsets,
                                    usd_code,
                                    usd_rowid_begin,
                                    usd_rowid_end)) {
                throw std::runtime_error("USD postings group not found");
            }

            sub_lookup.assign(global_adsh_offsets.size() - 1, kSubMissing);
            for (size_t g = 0; g < sub_sic_postings_values.size(); ++g) {
                const int32_t sic_value = sub_sic_postings_values[g];
                if (sic_value < kSicMin) continue;
                if (sic_value > kSicMax) break;
                const uint64_t begin = sub_sic_postings_offsets[g];
                const uint64_t end = sub_sic_postings_offsets[g + 1];
                for (uint64_t i = begin; i < end; ++i) {
                    const uint32_t sub_rowid = sub_sic_postings_rowids[i];
                    const uint32_t adsh_code = sub_adsh[sub_rowid];
                    if (adsh_code < sub_lookup.size()) sub_lookup[adsh_code] = sub_rowid;
                }
            }
        }

        {
            GENDB_PHASE("build_joins");

            uint64_t eq_begin = 0;
            uint64_t eq_end = 0;
            if (!find_posting_group(pre_stmt_postings_values,
                                    pre_stmt_postings_offsets,
                                    eq_code,
                                    eq_begin,
                                    eq_end)) {
                throw std::runtime_error("EQ postings group not found");
            }

            pre_eq_counts.init(static_cast<size_t>(eq_end - eq_begin));
            for (uint64_t i = eq_begin; i < eq_end; ++i) {
                const uint32_t rowid = pre_stmt_postings_rowids[i];
                pre_eq_counts.insert_or_increment(pre_adsh[rowid], pre_tag[rowid], pre_version[rowid]);
            }
        }

        std::vector<LocalState> local_states(static_cast<size_t>(thread_count));

        {
            GENDB_PHASE("main_scan");

            for (LocalState& state : local_states) {
                state.agg.reserve(4096);
                state.distinct.reserve(8192);
            }

            const uint32_t* __restrict usd_rowids = num_uom_postings_rowids.data;
            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;
            const uint32_t* __restrict sub_adsh_lookup = sub_lookup.data();
            const int32_t* __restrict sub_sic_data = sub_sic.data;
            const int32_t* __restrict sub_cik_data = sub_cik.data;
            const uint32_t* __restrict tag_tlabel_data = tag_tlabel.data;

            const int64_t total_usd_rows = static_cast<int64_t>(usd_rowid_end - usd_rowid_begin);

            #pragma omp parallel
            {
                LocalState& local = local_states[static_cast<size_t>(omp_get_thread_num())];

                #pragma omp for schedule(dynamic, kMorselRows)
                for (int64_t pos = 0; pos < total_usd_rows; ++pos) {
                    const uint32_t rowid = usd_rowids[usd_rowid_begin + static_cast<uint64_t>(pos)];
                    const double value = num_value_data[rowid];
                    if (std::isnan(value)) continue;

                    const uint32_t adsh = num_adsh_data[rowid];
                    if (adsh >= sub_lookup.size()) continue;
                    const uint32_t sub_rowid = sub_adsh_lookup[adsh];
                    if (sub_rowid == kSubMissing) continue;

                    const uint32_t tag = num_tag_data[rowid];
                    const uint32_t version = num_version_data[rowid];

                    const uint32_t eq_count = pre_eq_counts.find_count(adsh, tag, version);
                    if (eq_count == 0) continue;

                    const uint32_t tag_rowid = probe_tag_rowid(tag_hash_buckets,
                                                               tag_hash_offsets,
                                                               tag_hash_rowids,
                                                               tag_abstract,
                                                               tag,
                                                               version);
                    if (tag_rowid == kSubMissing) continue;

                    const GroupKey group_key{sub_sic_data[sub_rowid], tag_tlabel_data[tag_rowid], eq_code};
                    AggState& agg = local.agg[group_key];
                    agg.sum_value += value * static_cast<double>(eq_count);
                    agg.count_rows += eq_count;

                    local.distinct.insert(DistinctKey{
                        sub_sic_data[sub_rowid],
                        tag_tlabel_data[tag_rowid],
                        eq_code,
                        sub_cik_data[sub_rowid]});
                }
            }
        }

        std::unordered_map<GroupKey, FinalState, GroupKeyHash> merged_groups;
        std::unordered_set<DistinctKey, DistinctKeyHash> merged_distinct;
        std::vector<ResultRow> results;

        {
            GENDB_PHASE("output");

            size_t agg_entries = 0;
            size_t distinct_entries = 0;
            for (const LocalState& state : local_states) {
                agg_entries += state.agg.size();
                distinct_entries += state.distinct.size();
            }
            merged_groups.reserve(agg_entries);
            merged_distinct.reserve(distinct_entries);

            for (const LocalState& state : local_states) {
                for (const auto& entry : state.agg) {
                    FinalState& dst = merged_groups[entry.first];
                    dst.sum_value += entry.second.sum_value;
                    dst.count_rows += entry.second.count_rows;
                }
            }

            for (const LocalState& state : local_states) {
                for (const DistinctKey& key : state.distinct) {
                    const auto inserted = merged_distinct.insert(key);
                    if (inserted.second) {
                        merged_groups[GroupKey{key.sic, key.tlabel, key.stmt}].num_companies += 1;
                    }
                }
            }

            results.reserve(merged_groups.size());
            for (const auto& entry : merged_groups) {
                if (entry.second.num_companies < 2) continue;
                const double avg_value =
                    entry.second.count_rows == 0 ? 0.0 : (entry.second.sum_value / entry.second.count_rows);
                results.push_back(ResultRow{entry.first.sic,
                                            entry.first.tlabel,
                                            entry.first.stmt,
                                            entry.second.num_companies,
                                            entry.second.sum_value,
                                            avg_value});
            }

            std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.sic != b.sic) return a.sic < b.sic;
                if (a.tlabel != b.tlabel) return a.tlabel < b.tlabel;
                return a.stmt < b.stmt;
            });
            if (results.size() > 500) results.resize(500);

            std::filesystem::create_directories(results_dir);
            FILE* out = std::fopen((results_dir + "/Q4.csv").c_str(), "w");
            if (!out) throw std::runtime_error("failed to open output CSV");

            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            for (const ResultRow& row : results) {
                std::fprintf(out, "%d,", row.sic);
                write_csv_field(out, dict_value(tag_tlabel_dict_offsets, tag_tlabel_dict_data, row.tlabel));
                std::fputc(',', out);
                write_csv_field(out, dict_value(pre_stmt_dict_offsets, pre_stmt_dict_data, row.stmt));
                std::fprintf(out, ",%llu,%.2f,%.2f\n",
                             static_cast<unsigned long long>(row.num_companies),
                             row.total_value,
                             row.avg_value);
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Error: %s\n", ex.what());
        return 1;
    }
}
