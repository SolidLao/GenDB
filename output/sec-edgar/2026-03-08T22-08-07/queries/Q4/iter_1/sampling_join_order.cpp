#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "mmap_utils.h"

namespace {

constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMax = 4999;
constexpr uint32_t kMissing = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();
constexpr size_t kSampleBudget = 2000000;

struct PairBucket {
    uint32_t a;
    uint32_t b;
    uint64_t group_index;
};

struct TripleCountBucket {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
    uint32_t count;
};

struct CandidateResult {
    const char* name;
    double ms = 0.0;
    uint64_t passed = 0;
};

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

std::string_view dict_value(const gendb::MmapColumn<uint64_t>& offsets,
                            const gendb::MmapColumn<char>& data,
                            uint32_t code) {
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
    throw std::runtime_error("dictionary value not found");
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

class TripleCountMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, TripleCountBucket{kMissing, 0, 0, 0});
        mask_ = capacity - 1;
        size_ = 0;
    }

    void insert_or_increment(uint32_t adsh, uint32_t tag, uint32_t version) {
        size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & mask_);
        while (true) {
            TripleCountBucket& bucket = buckets_[slot];
            if (bucket.adsh == kMissing) {
                bucket.adsh = adsh;
                bucket.tag = tag;
                bucket.version = version;
                bucket.count = 1;
                ++size_;
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
            if (bucket.adsh == kMissing) return 0;
            if (bucket.adsh == adsh && bucket.tag == tag && bucket.version == version) return bucket.count;
            slot = (slot + 1) & mask_;
        }
    }

    size_t capacity() const { return buckets_.size(); }
    size_t size() const { return size_; }

private:
    std::vector<TripleCountBucket> buckets_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

bool probe_tag_exists(const gendb::MmapColumn<PairBucket>& buckets,
                      const gendb::MmapColumn<uint64_t>& offsets,
                      const gendb::MmapColumn<uint32_t>& rowids,
                      const gendb::MmapColumn<int8_t>& tag_abstract,
                      uint32_t tag,
                      uint32_t version) {
    const size_t mask = buckets.size() - 1;
    size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask);
    while (true) {
        const PairBucket& bucket = buckets[slot];
        if (bucket.group_index == kEmptyGroup) return false;
        if (bucket.a == tag && bucket.b == version) {
            const uint64_t begin = offsets[bucket.group_index];
            const uint64_t end = offsets[bucket.group_index + 1];
            for (uint64_t i = begin; i < end; ++i) {
                if (tag_abstract[rowids[i]] == 0) return true;
            }
            return false;
        }
        slot = (slot + 1) & mask;
    }
}

enum class Step : uint8_t { Sub, Pre, Tag };

struct RowCtx {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
};

bool check_sub(const RowCtx& row, const std::vector<uint32_t>& sub_lookup) {
    return row.adsh < sub_lookup.size() && sub_lookup[row.adsh] != kMissing;
}

bool check_pre(const RowCtx& row, const TripleCountMap& pre_counts) {
    return pre_counts.find_count(row.adsh, row.tag, row.version) != 0;
}

bool check_tag(const RowCtx& row,
               const gendb::MmapColumn<PairBucket>& tag_hash_buckets,
               const gendb::MmapColumn<uint64_t>& tag_hash_offsets,
               const gendb::MmapColumn<uint32_t>& tag_hash_rowids,
               const gendb::MmapColumn<int8_t>& tag_abstract) {
    return probe_tag_exists(tag_hash_buckets, tag_hash_offsets, tag_hash_rowids, tag_abstract, row.tag, row.version);
}

CandidateResult run_candidate(const char* name,
                              const std::vector<RowCtx>& sample_rows,
                              const std::vector<uint32_t>& sub_lookup,
                              const TripleCountMap& pre_counts,
                              const gendb::MmapColumn<PairBucket>& tag_hash_buckets,
                              const gendb::MmapColumn<uint64_t>& tag_hash_offsets,
                              const gendb::MmapColumn<uint32_t>& tag_hash_rowids,
                              const gendb::MmapColumn<int8_t>& tag_abstract,
                              Step s1,
                              Step s2,
                              Step s3) {
    auto step_ok = [&](Step step, const RowCtx& row) -> bool {
        switch (step) {
            case Step::Sub:
                return check_sub(row, sub_lookup);
            case Step::Pre:
                return check_pre(row, pre_counts);
            case Step::Tag:
                return check_tag(row, tag_hash_buckets, tag_hash_offsets, tag_hash_rowids, tag_abstract);
        }
        return false;
    };

    uint64_t passed = 0;
    auto start = std::chrono::steady_clock::now();
    for (const RowCtx& row : sample_rows) {
        if (!step_ok(s1, row)) continue;
        if (!step_ok(s2, row)) continue;
        if (!step_ok(s3, row)) continue;
        ++passed;
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    return CandidateResult{name, elapsed.count(), passed};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }
    const std::string gendb_dir = argv[1];

    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint32_t> num_tag;
    gendb::MmapColumn<uint32_t> num_version;
    gendb::MmapColumn<double> num_value;
    gendb::MmapColumn<uint32_t> sub_adsh;
    gendb::MmapColumn<int32_t> sub_sic_postings_values;
    gendb::MmapColumn<uint64_t> sub_sic_postings_offsets;
    gendb::MmapColumn<uint32_t> sub_sic_postings_rowids;
    gendb::MmapColumn<uint16_t> num_uom_postings_values;
    gendb::MmapColumn<uint64_t> num_uom_postings_offsets;
    gendb::MmapColumn<uint32_t> num_uom_postings_rowids;
    gendb::MmapColumn<uint16_t> pre_stmt_postings_values;
    gendb::MmapColumn<uint64_t> pre_stmt_postings_offsets;
    gendb::MmapColumn<uint32_t> pre_stmt_postings_rowids;
    gendb::MmapColumn<uint32_t> pre_adsh;
    gendb::MmapColumn<uint32_t> pre_tag;
    gendb::MmapColumn<uint32_t> pre_version;
    gendb::MmapColumn<PairBucket> tag_hash_buckets;
    gendb::MmapColumn<uint64_t> tag_hash_offsets;
    gendb::MmapColumn<uint32_t> tag_hash_rowids;
    gendb::MmapColumn<int8_t> tag_abstract;
    gendb::MmapColumn<uint64_t> global_adsh_offsets;
    gendb::MmapColumn<uint64_t> num_uom_dict_offsets;
    gendb::MmapColumn<char> num_uom_dict_data;
    gendb::MmapColumn<uint64_t> pre_stmt_dict_offsets;
    gendb::MmapColumn<char> pre_stmt_dict_data;

    num_adsh.open(gendb_dir + "/num/adsh.bin");
    num_tag.open(gendb_dir + "/num/tag.bin");
    num_version.open(gendb_dir + "/num/version.bin");
    num_value.open(gendb_dir + "/num/value.bin");
    sub_adsh.open(gendb_dir + "/sub/adsh.bin");
    sub_sic_postings_values.open(gendb_dir + "/indexes/sub/sub_sic_postings.values.bin");
    sub_sic_postings_offsets.open(gendb_dir + "/indexes/sub/sub_sic_postings.offsets.bin");
    sub_sic_postings_rowids.open(gendb_dir + "/indexes/sub/sub_sic_postings.rowids.bin");
    num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
    num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
    num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");
    pre_stmt_postings_values.open(gendb_dir + "/indexes/pre/pre_stmt_postings.values.bin");
    pre_stmt_postings_offsets.open(gendb_dir + "/indexes/pre/pre_stmt_postings.offsets.bin");
    pre_stmt_postings_rowids.open(gendb_dir + "/indexes/pre/pre_stmt_postings.rowids.bin");
    pre_adsh.open(gendb_dir + "/pre/adsh.bin");
    pre_tag.open(gendb_dir + "/pre/tag.bin");
    pre_version.open(gendb_dir + "/pre/version.bin");
    tag_hash_buckets.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.hash.bin");
    tag_hash_offsets.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.offsets.bin");
    tag_hash_rowids.open(gendb_dir + "/indexes/tag/tag_tag_version_hash.rowids.bin");
    tag_abstract.open(gendb_dir + "/tag/abstract.bin");
    global_adsh_offsets.open(gendb_dir + "/dicts/global_adsh.offsets.bin");
    num_uom_dict_offsets.open(gendb_dir + "/dicts/num_uom.offsets.bin");
    num_uom_dict_data.open(gendb_dir + "/dicts/num_uom.data.bin");
    pre_stmt_dict_offsets.open(gendb_dir + "/dicts/pre_stmt.offsets.bin");
    pre_stmt_dict_data.open(gendb_dir + "/dicts/pre_stmt.data.bin");

    const uint16_t usd_code = static_cast<uint16_t>(find_dict_code(num_uom_dict_offsets, num_uom_dict_data, "USD"));
    const uint16_t eq_code = static_cast<uint16_t>(find_dict_code(pre_stmt_dict_offsets, pre_stmt_dict_data, "EQ"));

    uint64_t usd_begin = 0;
    uint64_t usd_end = 0;
    uint64_t eq_begin = 0;
    uint64_t eq_end = 0;
    if (!find_posting_group(num_uom_postings_values, num_uom_postings_offsets, usd_code, usd_begin, usd_end)) {
        throw std::runtime_error("USD postings not found");
    }
    if (!find_posting_group(pre_stmt_postings_values, pre_stmt_postings_offsets, eq_code, eq_begin, eq_end)) {
        throw std::runtime_error("EQ postings not found");
    }

    std::vector<uint32_t> sub_lookup(global_adsh_offsets.size() - 1, kMissing);
    for (size_t g = 0; g < sub_sic_postings_values.size(); ++g) {
        const int32_t sic = sub_sic_postings_values[g];
        if (sic < kSicMin) continue;
        if (sic > kSicMax) break;
        const uint64_t begin = sub_sic_postings_offsets[g];
        const uint64_t end = sub_sic_postings_offsets[g + 1];
        for (uint64_t i = begin; i < end; ++i) {
            const uint32_t sub_rowid = sub_sic_postings_rowids[i];
            sub_lookup[sub_adsh[sub_rowid]] = sub_rowid;
        }
    }

    const size_t full_pre_capacity = next_power_of_two(std::max<size_t>(16, static_cast<size_t>(eq_end - eq_begin) * 2 + 1));
    const size_t full_pre_bytes = full_pre_capacity * sizeof(TripleCountBucket);

    uint64_t relevant_pre_rows = 0;
    for (uint64_t i = eq_begin; i < eq_end; ++i) {
        const uint32_t rowid = pre_stmt_postings_rowids[i];
        const uint32_t adsh = pre_adsh[rowid];
        if (adsh < sub_lookup.size() && sub_lookup[adsh] != kMissing) ++relevant_pre_rows;
    }

    TripleCountMap filtered_pre_counts;
    filtered_pre_counts.init(static_cast<size_t>(relevant_pre_rows));
    for (uint64_t i = eq_begin; i < eq_end; ++i) {
        const uint32_t rowid = pre_stmt_postings_rowids[i];
        const uint32_t adsh = pre_adsh[rowid];
        if (adsh >= sub_lookup.size() || sub_lookup[adsh] == kMissing) continue;
        filtered_pre_counts.insert_or_increment(adsh, pre_tag[rowid], pre_version[rowid]);
    }
    const size_t filtered_pre_bytes = filtered_pre_counts.capacity() * sizeof(TripleCountBucket);

    const uint64_t total_usd_rows = usd_end - usd_begin;
    const uint64_t stride = std::max<uint64_t>(1, total_usd_rows / kSampleBudget);
    std::vector<RowCtx> sample_rows;
    sample_rows.reserve(std::min<uint64_t>(kSampleBudget, total_usd_rows));
    for (uint64_t pos = 0; pos < total_usd_rows; pos += stride) {
        const uint32_t rowid = num_uom_postings_rowids[usd_begin + pos];
        if (std::isnan(num_value[rowid])) continue;
        sample_rows.push_back(RowCtx{num_adsh[rowid], num_tag[rowid], num_version[rowid]});
    }

    std::vector<CandidateResult> results;
    const struct {
        const char* name;
        Step s1;
        Step s2;
        Step s3;
    } candidates[] = {
        {"sub->pre->tag", Step::Sub, Step::Pre, Step::Tag},
        {"sub->tag->pre", Step::Sub, Step::Tag, Step::Pre},
        {"pre->sub->tag", Step::Pre, Step::Sub, Step::Tag},
        {"pre->tag->sub", Step::Pre, Step::Tag, Step::Sub},
        {"tag->sub->pre", Step::Tag, Step::Sub, Step::Pre},
        {"tag->pre->sub", Step::Tag, Step::Pre, Step::Sub},
    };

    for (const auto& candidate : candidates) {
        (void)run_candidate(candidate.name,
                            sample_rows,
                            sub_lookup,
                            filtered_pre_counts,
                            tag_hash_buckets,
                            tag_hash_offsets,
                            tag_hash_rowids,
                            tag_abstract,
                            candidate.s1,
                            candidate.s2,
                            candidate.s3);
        results.push_back(run_candidate(candidate.name,
                                        sample_rows,
                                        sub_lookup,
                                        filtered_pre_counts,
                                        tag_hash_buckets,
                                        tag_hash_offsets,
                                        tag_hash_rowids,
                                        tag_abstract,
                                        candidate.s1,
                                        candidate.s2,
                                        candidate.s3));
    }

    std::sort(results.begin(), results.end(), [](const CandidateResult& a, const CandidateResult& b) {
        return a.ms < b.ms;
    });

    std::cout << "usd_rows_total=" << total_usd_rows << "\n";
    std::cout << "sample_rows=" << sample_rows.size() << "\n";
    std::cout << "sample_stride=" << stride << "\n";
    std::cout << "sub_lookup_entries=" << sub_lookup.size() << "\n";
    std::cout << "sub_lookup_bytes=" << (sub_lookup.size() * sizeof(uint32_t)) << "\n";
    std::cout << "pre_eq_rows_total=" << (eq_end - eq_begin) << "\n";
    std::cout << "pre_eq_full_capacity=" << full_pre_capacity << "\n";
    std::cout << "pre_eq_full_bytes=" << full_pre_bytes << "\n";
    std::cout << "pre_eq_rows_after_sub=" << relevant_pre_rows << "\n";
    std::cout << "pre_eq_filtered_triples=" << filtered_pre_counts.size() << "\n";
    std::cout << "pre_eq_filtered_capacity=" << filtered_pre_counts.capacity() << "\n";
    std::cout << "pre_eq_filtered_bytes=" << filtered_pre_bytes << "\n";
    for (const CandidateResult& result : results) {
        std::cout << result.name << ": ms=" << result.ms << ", passed=" << result.passed << "\n";
    }

    return 0;
}
