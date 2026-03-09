#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMax = 4999;
constexpr uint32_t kMissingU32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmptyGroup = std::numeric_limits<uint64_t>::max();
constexpr int32_t kMissingI32 = std::numeric_limits<int32_t>::min();
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

struct PairKeyEntry {
    uint32_t tag;
    uint32_t version;
};

struct PairLabelBucket {
    uint32_t tag;
    uint32_t version;
    uint32_t tlabel;
};

struct CompanyAggBucket {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;
    uint16_t pad0;
    int32_t cik;
    int32_t pad1;
    double sum_value;
    uint64_t count_rows;
};

struct RollupBucket {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;
    uint16_t pad0;
    double sum_value;
    uint64_t count_rows;
    uint64_t num_companies;
};

struct ResultRow {
    int32_t sic;
    uint32_t tlabel;
    uint16_t stmt;
    uint64_t num_companies;
    double total_value;
    double avg_value;
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

inline uint64_t hash_company(int32_t sic, uint32_t tlabel, uint16_t stmt, int32_t cik) {
    uint64_t seed = mix64(static_cast<uint32_t>(sic));
    seed ^= mix64(static_cast<uint64_t>(tlabel) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64(static_cast<uint64_t>(stmt) + 0xbf58476d1ce4e5b9ull);
    seed ^= mix64(static_cast<uint32_t>(cik) + 0x94d049bb133111ebull);
    return mix64(seed);
}

inline uint64_t hash_group(int32_t sic, uint32_t tlabel, uint16_t stmt) {
    uint64_t seed = mix64(static_cast<uint32_t>(sic));
    seed ^= mix64(static_cast<uint64_t>(tlabel) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64(static_cast<uint64_t>(stmt) + 0xbf58476d1ce4e5b9ull);
    return mix64(seed);
}

size_t next_power_of_two(size_t n) {
    size_t out = 1;
    while (out < n) {
        out <<= 1;
    }
    return out;
}

template <typename T>
void require_non_empty(const char* name, const gendb::MmapColumn<T>& col) {
    if (col.empty()) {
        throw std::runtime_error(std::string("empty file: ") + name);
    }
}

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
        if (dict_value(offsets, data, code) == needle) {
            return code;
        }
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
    if (it == values.data + values.size() || *it != needle) {
        return false;
    }
    const size_t group = static_cast<size_t>(it - values.data);
    begin = offsets[group];
    end = offsets[group + 1];
    return true;
}

class PairSet {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, PairKeyEntry{kMissingU32, 0});
        keys_.clear();
        keys_.reserve(expected_entries);
        mask_ = capacity - 1;
    }

    void insert(uint32_t tag, uint32_t version) {
        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            PairKeyEntry& bucket = buckets_[slot];
            if (bucket.tag == kMissingU32) {
                bucket.tag = tag;
                bucket.version = version;
                keys_.push_back(PairKeyEntry{tag, version});
                return;
            }
            if (bucket.tag == tag && bucket.version == version) {
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const std::vector<PairKeyEntry>& keys() const { return keys_; }
    size_t size() const { return keys_.size(); }

private:
    std::vector<PairKeyEntry> buckets_;
    std::vector<PairKeyEntry> keys_;
    size_t mask_ = 0;
};

class TripleCountMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, TripleCountBucket{kMissingU32, 0, 0, 0});
        mask_ = capacity - 1;
    }

    void insert_or_increment(uint32_t adsh, uint32_t tag, uint32_t version) {
        size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & mask_);
        while (true) {
            TripleCountBucket& bucket = buckets_[slot];
            if (bucket.adsh == kMissingU32) {
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
            if (bucket.adsh == kMissingU32) {
                return 0;
            }
            if (bucket.adsh == adsh && bucket.tag == tag && bucket.version == version) {
                return bucket.count;
            }
            slot = (slot + 1) & mask_;
        }
    }

private:
    std::vector<TripleCountBucket> buckets_;
    size_t mask_ = 0;
};

class PairLabelMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, PairLabelBucket{kMissingU32, 0, kMissingU32});
        mask_ = capacity - 1;
    }

    void insert(uint32_t tag, uint32_t version, uint32_t tlabel) {
        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            PairLabelBucket& bucket = buckets_[slot];
            if (bucket.tag == kMissingU32) {
                bucket.tag = tag;
                bucket.version = version;
                bucket.tlabel = tlabel;
                return;
            }
            if (bucket.tag == tag && bucket.version == version) {
                bucket.tlabel = tlabel;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    uint32_t find_tlabel(uint32_t tag, uint32_t version) const {
        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            const PairLabelBucket& bucket = buckets_[slot];
            if (bucket.tag == kMissingU32) {
                return kMissingU32;
            }
            if (bucket.tag == tag && bucket.version == version) {
                return bucket.tlabel;
            }
            slot = (slot + 1) & mask_;
        }
    }

private:
    std::vector<PairLabelBucket> buckets_;
    size_t mask_ = 0;
};

class CompanyAggMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, CompanyAggBucket{kSicMin - 1, 0, 0, 0, kMissingI32, 0, 0.0, 0});
        mask_ = capacity - 1;
        size_ = 0;
    }

    void add(int32_t sic, uint32_t tlabel, uint16_t stmt, int32_t cik, double sum_delta, uint32_t count_delta) {
        if ((size_ + 1) * 10 >= buckets_.size() * 7) {
            rehash(buckets_.size() << 1);
        }
        insert_into(buckets_, mask_, size_, sic, tlabel, stmt, cik, sum_delta, count_delta);
    }

    const std::vector<CompanyAggBucket>& buckets() const { return buckets_; }
    size_t size() const { return size_; }

private:
    static void insert_into(std::vector<CompanyAggBucket>& buckets,
                            size_t mask,
                            size_t& size,
                            int32_t sic,
                            uint32_t tlabel,
                            uint16_t stmt,
                            int32_t cik,
                            double sum_delta,
                            uint32_t count_delta) {
        size_t slot = static_cast<size_t>(hash_company(sic, tlabel, stmt, cik) & mask);
        while (true) {
            CompanyAggBucket& bucket = buckets[slot];
            if (bucket.cik == kMissingI32) {
                bucket.sic = sic;
                bucket.tlabel = tlabel;
                bucket.stmt = stmt;
                bucket.cik = cik;
                bucket.sum_value = sum_delta;
                bucket.count_rows = count_delta;
                ++size;
                return;
            }
            if (bucket.sic == sic && bucket.tlabel == tlabel && bucket.stmt == stmt && bucket.cik == cik) {
                bucket.sum_value += sum_delta;
                bucket.count_rows += count_delta;
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    void rehash(size_t new_capacity) {
        std::vector<CompanyAggBucket> old = std::move(buckets_);
        new_capacity = next_power_of_two(std::max<size_t>(16, new_capacity));
        buckets_.assign(new_capacity, CompanyAggBucket{kSicMin - 1, 0, 0, 0, kMissingI32, 0, 0.0, 0});
        mask_ = new_capacity - 1;
        size_ = 0;
        for (const CompanyAggBucket& bucket : old) {
            if (bucket.cik != kMissingI32) {
                insert_into(buckets_, mask_, size_, bucket.sic, bucket.tlabel, bucket.stmt, bucket.cik, bucket.sum_value,
                            static_cast<uint32_t>(bucket.count_rows));
            }
        }
    }

    std::vector<CompanyAggBucket> buckets_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

class RollupMap {
public:
    void init(size_t expected_entries) {
        const size_t capacity = next_power_of_two(std::max<size_t>(16, expected_entries * 2 + 1));
        buckets_.assign(capacity, RollupBucket{kMissingI32, 0, 0, 0, 0.0, 0, 0});
        mask_ = capacity - 1;
    }

    void add(int32_t sic, uint32_t tlabel, uint16_t stmt, double sum_delta, uint64_t count_delta, uint64_t company_delta) {
        size_t slot = static_cast<size_t>(hash_group(sic, tlabel, stmt) & mask_);
        while (true) {
            RollupBucket& bucket = buckets_[slot];
            if (bucket.sic == kMissingI32) {
                bucket.sic = sic;
                bucket.tlabel = tlabel;
                bucket.stmt = stmt;
                bucket.sum_value = sum_delta;
                bucket.count_rows = count_delta;
                bucket.num_companies = company_delta;
                return;
            }
            if (bucket.sic == sic && bucket.tlabel == tlabel && bucket.stmt == stmt) {
                bucket.sum_value += sum_delta;
                bucket.count_rows += count_delta;
                bucket.num_companies += company_delta;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const std::vector<RollupBucket>& buckets() const { return buckets_; }

private:
    std::vector<RollupBucket> buckets_;
    size_t mask_ = 0;
};

uint32_t probe_tag_tlabel(const gendb::MmapColumn<PairBucket>& tag_hash_buckets,
                          const gendb::MmapColumn<uint64_t>& tag_hash_offsets,
                          const gendb::MmapColumn<uint32_t>& tag_hash_rowids,
                          const gendb::MmapColumn<int8_t>& tag_abstract,
                          const gendb::MmapColumn<uint32_t>& tag_tlabel,
                          uint32_t tag,
                          uint32_t version) {
    const size_t mask = tag_hash_buckets.size() - 1;
    size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask);
    while (true) {
        const PairBucket& bucket = tag_hash_buckets[slot];
        if (bucket.group_index == kEmptyGroup) {
            return kMissingU32;
        }
        if (bucket.a == tag && bucket.b == version) {
            const uint64_t begin = tag_hash_offsets[bucket.group_index];
            const uint64_t end = tag_hash_offsets[bucket.group_index + 1];
            for (uint64_t i = begin; i < end; ++i) {
                const uint32_t rowid = tag_hash_rowids[i];
                if (tag_abstract[rowid] == 0) {
                    return tag_tlabel[rowid];
                }
            }
            return kMissingU32;
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
        if (c == '"') {
            std::fputc('"', out);
        }
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
        uint64_t usd_rowid_begin = 0;
        uint64_t usd_rowid_end = 0;
        uint64_t eq_rowid_begin = 0;
        uint64_t eq_rowid_end = 0;

        std::vector<uint32_t> filtered_sub_lookup;
        TripleCountMap filtered_pre_eq_counts;
        PairLabelMap filtered_tag_map;
        uint64_t reachable_eq_rows = 0;

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
            require_non_empty("sub/sic.bin", sub_sic);
            require_non_empty("sub/cik.bin", sub_cik);
            require_non_empty("pre/adsh.bin", pre_adsh);
            require_non_empty("pre/tag.bin", pre_tag);
            require_non_empty("pre/version.bin", pre_version);
            require_non_empty("tag/tlabel.bin", tag_tlabel);
            require_non_empty("tag/abstract.bin", tag_abstract);
            require_non_empty("dicts/global_adsh.offsets.bin", global_adsh_offsets);
            require_non_empty("indexes/sub/sub_sic_postings.values.bin", sub_sic_postings_values);
            require_non_empty("indexes/sub/sub_sic_postings.offsets.bin", sub_sic_postings_offsets);
            require_non_empty("indexes/sub/sub_sic_postings.rowids.bin", sub_sic_postings_rowids);
            require_non_empty("indexes/num/num_uom_postings.values.bin", num_uom_postings_values);
            require_non_empty("indexes/num/num_uom_postings.offsets.bin", num_uom_postings_offsets);
            require_non_empty("indexes/num/num_uom_postings.rowids.bin", num_uom_postings_rowids);
            require_non_empty("indexes/pre/pre_stmt_postings.values.bin", pre_stmt_postings_values);
            require_non_empty("indexes/pre/pre_stmt_postings.offsets.bin", pre_stmt_postings_offsets);
            require_non_empty("indexes/pre/pre_stmt_postings.rowids.bin", pre_stmt_postings_rowids);
            require_non_empty("indexes/tag/tag_tag_version_hash.hash.bin", tag_hash_buckets);
            require_non_empty("indexes/tag/tag_tag_version_hash.offsets.bin", tag_hash_offsets);
            require_non_empty("indexes/tag/tag_tag_version_hash.rowids.bin", tag_hash_rowids);

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
                                     num_uom_postings_rowids,
                                     pre_stmt_postings_rowids);

            tag_hash_buckets.advise_random();
            tag_hash_offsets.advise_random();
            tag_hash_rowids.advise_random();
            tag_tlabel.advise_random();
            tag_abstract.advise_random();
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
            if (!find_posting_group(pre_stmt_postings_values,
                                    pre_stmt_postings_offsets,
                                    eq_code,
                                    eq_rowid_begin,
                                    eq_rowid_end)) {
                throw std::runtime_error("EQ postings group not found");
            }

            if (global_adsh_offsets.size() < 2) {
                throw std::runtime_error("global_adsh dictionary is empty");
            }
            filtered_sub_lookup.assign(global_adsh_offsets.size() - 1, kMissingU32);

            for (size_t group = 0; group < sub_sic_postings_values.size(); ++group) {
                const int32_t sic = sub_sic_postings_values[group];
                if (sic < kSicMin) {
                    continue;
                }
                if (sic > kSicMax) {
                    break;
                }
                const uint64_t begin = sub_sic_postings_offsets[group];
                const uint64_t end = sub_sic_postings_offsets[group + 1];
                for (uint64_t i = begin; i < end; ++i) {
                    const uint32_t sub_rowid = sub_sic_postings_rowids[i];
                    const uint32_t adsh = sub_adsh[sub_rowid];
                    if (adsh < filtered_sub_lookup.size()) {
                        filtered_sub_lookup[adsh] = sub_rowid;
                    }
                }
            }
        }

        {
            GENDB_PHASE("build_joins");

            const uint32_t* __restrict pre_stmt_rowids = pre_stmt_postings_rowids.data;
            const uint32_t* __restrict pre_adsh_data = pre_adsh.data;
            const uint32_t* __restrict pre_tag_data = pre_tag.data;
            const uint32_t* __restrict pre_version_data = pre_version.data;
            const uint32_t* __restrict sub_lookup_data = filtered_sub_lookup.data();

            PairSet reachable_pairs;
            reachable_pairs.init(static_cast<size_t>(eq_rowid_end - eq_rowid_begin));

            for (uint64_t i = eq_rowid_begin; i < eq_rowid_end; ++i) {
                const uint32_t rowid = pre_stmt_rowids[i];
                const uint32_t adsh = pre_adsh_data[rowid];
                if (adsh >= filtered_sub_lookup.size() || sub_lookup_data[adsh] == kMissingU32) {
                    continue;
                }
                ++reachable_eq_rows;
                reachable_pairs.insert(pre_tag_data[rowid], pre_version_data[rowid]);
            }

            filtered_pre_eq_counts.init(static_cast<size_t>(reachable_eq_rows));
            for (uint64_t i = eq_rowid_begin; i < eq_rowid_end; ++i) {
                const uint32_t rowid = pre_stmt_rowids[i];
                const uint32_t adsh = pre_adsh_data[rowid];
                if (adsh >= filtered_sub_lookup.size() || sub_lookup_data[adsh] == kMissingU32) {
                    continue;
                }
                filtered_pre_eq_counts.insert_or_increment(adsh, pre_tag_data[rowid], pre_version_data[rowid]);
            }

            filtered_tag_map.init(reachable_pairs.size());
            for (const PairKeyEntry& pair : reachable_pairs.keys()) {
                const uint32_t tlabel =
                    probe_tag_tlabel(tag_hash_buckets, tag_hash_offsets, tag_hash_rowids, tag_abstract, tag_tlabel,
                                     pair.tag, pair.version);
                if (tlabel != kMissingU32) {
                    filtered_tag_map.insert(pair.tag, pair.version, tlabel);
                }
            }
        }

        std::vector<CompanyAggMap> local_states(static_cast<size_t>(thread_count));
        {
            GENDB_PHASE("main_scan");

            const size_t local_expected = std::max<size_t>(2048, reachable_eq_rows / static_cast<uint64_t>(thread_count) + 1024);
            for (CompanyAggMap& state : local_states) {
                state.init(local_expected);
            }

            const uint32_t* __restrict usd_rowids = num_uom_postings_rowids.data;
            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;
            const uint32_t* __restrict sub_lookup_data = filtered_sub_lookup.data();
            const int32_t* __restrict sub_sic_data = sub_sic.data;
            const int32_t* __restrict sub_cik_data = sub_cik.data;
            const uint16_t stmt_code = eq_code;

            const int64_t total_usd_rows = static_cast<int64_t>(usd_rowid_end - usd_rowid_begin);

#pragma omp parallel
            {
                CompanyAggMap& local = local_states[static_cast<size_t>(omp_get_thread_num())];

#pragma omp for schedule(dynamic, kMorselRows)
                for (int64_t pos = 0; pos < total_usd_rows; ++pos) {
                    const uint32_t rowid = usd_rowids[usd_rowid_begin + static_cast<uint64_t>(pos)];
                    const double value = num_value_data[rowid];
                    if (std::isnan(value)) {
                        continue;
                    }

                    const uint32_t adsh = num_adsh_data[rowid];
                    if (adsh >= filtered_sub_lookup.size()) {
                        continue;
                    }
                    const uint32_t sub_rowid = sub_lookup_data[adsh];
                    if (sub_rowid == kMissingU32) {
                        continue;
                    }

                    const uint32_t tag = num_tag_data[rowid];
                    const uint32_t version = num_version_data[rowid];

                    const uint32_t eq_count = filtered_pre_eq_counts.find_count(adsh, tag, version);
                    if (eq_count == 0) {
                        continue;
                    }

                    const uint32_t tlabel = filtered_tag_map.find_tlabel(tag, version);
                    if (tlabel == kMissingU32) {
                        continue;
                    }

                    local.add(sub_sic_data[sub_rowid], tlabel, stmt_code, sub_cik_data[sub_rowid],
                              value * static_cast<double>(eq_count), eq_count);
                }
            }
        }

        CompanyAggMap merged_company;
        RollupMap rolled_up_groups;
        std::vector<ResultRow> results;

        {
            GENDB_PHASE("output");

            size_t merged_company_expected = 0;
            for (const CompanyAggMap& state : local_states) {
                merged_company_expected += state.size();
            }
            merged_company.init(std::max<size_t>(16, merged_company_expected));

            for (const CompanyAggMap& state : local_states) {
                for (const CompanyAggBucket& bucket : state.buckets()) {
                    if (bucket.cik == kMissingI32) {
                        continue;
                    }
                    merged_company.add(bucket.sic, bucket.tlabel, bucket.stmt, bucket.cik, bucket.sum_value,
                                       static_cast<uint32_t>(bucket.count_rows));
                }
            }

            rolled_up_groups.init(std::max<size_t>(16, merged_company.size()));
            for (const CompanyAggBucket& bucket : merged_company.buckets()) {
                if (bucket.cik == kMissingI32) {
                    continue;
                }
                rolled_up_groups.add(bucket.sic, bucket.tlabel, bucket.stmt, bucket.sum_value, bucket.count_rows, 1);
            }

            for (const RollupBucket& bucket : rolled_up_groups.buckets()) {
                if (bucket.sic == kMissingI32 || bucket.num_companies < 2) {
                    continue;
                }
                const double avg_value =
                    bucket.count_rows == 0 ? 0.0 : bucket.sum_value / static_cast<double>(bucket.count_rows);
                results.push_back(ResultRow{bucket.sic, bucket.tlabel, bucket.stmt, bucket.num_companies,
                                            bucket.sum_value, avg_value});
            }

            std::sort(results.begin(), results.end(), [](const ResultRow& left, const ResultRow& right) {
                if (left.total_value != right.total_value) {
                    return left.total_value > right.total_value;
                }
                if (left.sic != right.sic) {
                    return left.sic < right.sic;
                }
                if (left.tlabel != right.tlabel) {
                    return left.tlabel < right.tlabel;
                }
                return left.stmt < right.stmt;
            });
            if (results.size() > 500) {
                results.resize(500);
            }

            std::filesystem::create_directories(results_dir);
            FILE* out = std::fopen((results_dir + "/Q4.csv").c_str(), "w");
            if (out == nullptr) {
                throw std::runtime_error("failed to open output CSV");
            }

            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            for (const ResultRow& row : results) {
                std::fprintf(out, "%d,", row.sic);
                write_csv_field(out, dict_value(tag_tlabel_dict_offsets, tag_tlabel_dict_data, row.tlabel));
                std::fputc(',', out);
                write_csv_field(out, dict_value(pre_stmt_dict_offsets, pre_stmt_dict_data, row.stmt));
                std::fprintf(out,
                             ",%llu,%.2f,%.2f\n",
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
