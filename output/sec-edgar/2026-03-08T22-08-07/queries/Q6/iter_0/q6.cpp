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

namespace fs = std::filesystem;

namespace {

using gendb::MmapColumn;

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr size_t kTopK = 200;
constexpr uint64_t kMorselRows = 100000;

template <typename T>
void require_non_empty(const char* name, const MmapColumn<T>& col) {
    if (col.size() == 0) {
        throw std::runtime_error(std::string("empty mmap column: ") + name);
    }
}

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& base_path) {
        offsets.open(base_path + ".offsets.bin");
        data.open(base_path + ".data.bin");
        require_non_empty("dict.offsets", offsets);
    }

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
    }

    std::string_view decode(uint32_t code) const {
        const size_t idx = static_cast<size_t>(code);
        if (idx + 1 >= offsets.size()) {
            throw std::runtime_error("dictionary code out of range");
        }
        const uint64_t begin = offsets[idx];
        const uint64_t end = offsets[idx + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }

    template <typename CodeType>
    CodeType find_code(std::string_view needle, const char* dict_name) const {
        for (uint32_t code = 0; code < size(); ++code) {
            if (decode(code) == needle) {
                return static_cast<CodeType>(code);
            }
        }
        throw std::runtime_error(std::string("dictionary value not found in ") + dict_name);
    }
};

template <typename T>
struct PostingSlice {
    bool found = false;
    uint64_t begin = 0;
    uint64_t end = 0;
};

template <typename T>
PostingSlice<T> find_posting_slice(const MmapColumn<T>& values,
                                   const MmapColumn<uint64_t>& offsets,
                                   T needle) {
    PostingSlice<T> slice;
    const T* begin = values.data;
    const T* end = values.data + values.size();
    const T* it = std::lower_bound(begin, end, needle);
    if (it == end || *it != needle) {
        return slice;
    }

    const size_t idx = static_cast<size_t>(it - begin);
    if (idx + 1 >= offsets.size()) {
        throw std::runtime_error("posting index offsets out of range");
    }

    slice.found = true;
    slice.begin = offsets[idx];
    slice.end = offsets[idx + 1];
    return slice;
}

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

inline uint64_t hash_group(uint32_t name, uint32_t tag, uint32_t plabel) {
    uint64_t seed = mix64(static_cast<uint64_t>(name) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(tag) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(plabel) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}

struct PreBucket {
    uint32_t a = 0;
    uint32_t b = 0;
    uint32_t c = 0;
    uint32_t group_index = kEmpty32;
    uint32_t count = 0;
};

class PreTripleHash {
public:
    void reserve(size_t expected_rows) {
        size_t cap = 16;
        while (cap < expected_rows * 2 + 1) {
            cap <<= 1;
        }
        buckets_.assign(cap, PreBucket{});
        mask_ = cap - 1;
        group_count_ = 0;
    }

    uint32_t find_or_insert(uint32_t a, uint32_t b, uint32_t c) {
        size_t slot = static_cast<size_t>(hash_triple(a, b, c) & mask_);
        while (true) {
            PreBucket& bucket = buckets_[slot];
            if (bucket.group_index == kEmpty32) {
                bucket.a = a;
                bucket.b = b;
                bucket.c = c;
                bucket.group_index = group_count_++;
                bucket.count = 1;
                return bucket.group_index;
            }
            if (bucket.a == a && bucket.b == b && bucket.c == c) {
                ++bucket.count;
                return bucket.group_index;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const PreBucket* find(uint32_t a, uint32_t b, uint32_t c) const {
        size_t slot = static_cast<size_t>(hash_triple(a, b, c) & mask_);
        while (true) {
            const PreBucket& bucket = buckets_[slot];
            if (bucket.group_index == kEmpty32) {
                return nullptr;
            }
            if (bucket.a == a && bucket.b == b && bucket.c == c) {
                return &bucket;
            }
            slot = (slot + 1) & mask_;
        }
    }

    uint32_t group_count() const {
        return group_count_;
    }

    const std::vector<PreBucket>& buckets() const {
        return buckets_;
    }

private:
    std::vector<PreBucket> buckets_;
    size_t mask_ = 0;
    uint32_t group_count_ = 0;
};

struct AggEntry {
    uint32_t name = kEmpty32;
    uint32_t tag = 0;
    uint32_t plabel = 0;
    uint64_t cnt = 0;
    double sum = 0.0;
};

class AggHashMap {
public:
    AggHashMap() = default;

    explicit AggHashMap(size_t expected) {
        reserve(expected);
    }

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2 + 1) {
            cap <<= 1;
        }
        table_.assign(cap, AggEntry{});
        mask_ = cap - 1;
        size_ = 0;
    }

    size_t size() const {
        return size_;
    }

    void add(uint32_t name, uint32_t tag, uint32_t plabel, double value) {
        if (table_.empty()) {
            reserve(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = static_cast<size_t>(hash_group(name, tag, plabel) & mask_);
        while (true) {
            AggEntry& entry = table_[slot];
            if (entry.name == kEmpty32) {
                entry.name = name;
                entry.tag = tag;
                entry.plabel = plabel;
                entry.cnt = 1;
                entry.sum = value;
                ++size_;
                return;
            }
            if (entry.name == name && entry.tag == tag && entry.plabel == plabel) {
                ++entry.cnt;
                entry.sum += value;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    void add_merged(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double sum) {
        if (table_.empty()) {
            reserve(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = static_cast<size_t>(hash_group(name, tag, plabel) & mask_);
        while (true) {
            AggEntry& entry = table_[slot];
            if (entry.name == kEmpty32) {
                entry.name = name;
                entry.tag = tag;
                entry.plabel = plabel;
                entry.cnt = cnt;
                entry.sum = sum;
                ++size_;
                return;
            }
            if (entry.name == name && entry.tag == tag && entry.plabel == plabel) {
                entry.cnt += cnt;
                entry.sum += sum;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (const AggEntry& entry : table_) {
            if (entry.name != kEmpty32) {
                fn(entry);
            }
        }
    }

private:
    void rehash(size_t new_cap) {
        std::vector<AggEntry> old = std::move(table_);
        table_.assign(new_cap, AggEntry{});
        mask_ = new_cap - 1;
        size_ = 0;
        for (const AggEntry& entry : old) {
            if (entry.name != kEmpty32) {
                add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.sum);
            }
        }
    }

    std::vector<AggEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

struct ResultRow {
    uint32_t name = 0;
    uint32_t tag = 0;
    uint32_t plabel = 0;
    double total_value = 0.0;
    uint64_t cnt = 0;
};

void write_csv_field(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
            break;
        }
    }

    if (!needs_quotes) {
        std::fwrite(value.data(), 1, value.size(), out);
        return;
    }

    std::fputc('"', out);
    for (char ch : value) {
        if (ch == '"') {
            std::fputc('"', out);
        }
        std::fputc(ch, out);
    }
    std::fputc('"', out);
}

void write_empty_output(const std::string& results_dir) {
    fs::create_directories(results_dir);
    const std::string out_path = results_dir + "/Q6.csv";
    FILE* out = std::fopen(out_path.c_str(), "w");
    if (!out) {
        throw std::runtime_error("cannot open output file: " + out_path);
    }
    std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
    std::fclose(out);
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

        MmapColumn<uint32_t> num_adsh;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<uint32_t> num_version;
        MmapColumn<double> num_value;

        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;

        MmapColumn<uint32_t> pre_adsh;
        MmapColumn<uint32_t> pre_tag;
        MmapColumn<uint32_t> pre_version;
        MmapColumn<uint32_t> pre_plabel;

        MmapColumn<uint16_t> num_uom_postings_values;
        MmapColumn<uint64_t> num_uom_postings_offsets;
        MmapColumn<uint32_t> num_uom_postings_rowids;

        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;

        MmapColumn<uint16_t> pre_stmt_postings_values;
        MmapColumn<uint64_t> pre_stmt_postings_offsets;
        MmapColumn<uint32_t> pre_stmt_postings_rowids;

        DictView num_uom_dict;
        DictView pre_stmt_dict;
        DictView sub_name_dict;
        DictView pre_plabel_dict;
        DictView global_tag_dict;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;

        PostingSlice<int16_t> fy_2023_slice;
        PostingSlice<uint16_t> num_usd_slice;
        PostingSlice<uint16_t> pre_is_slice;

        std::vector<uint32_t> adsh_to_name;
        PreTripleHash pre_hash;
        std::vector<uint64_t> pre_group_offsets;
        std::vector<uint32_t> pre_group_plabels;

        {
            GENDB_PHASE("data_loading");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");

            pre_stmt_postings_values.open(gendb_dir + "/indexes/pre/pre_stmt_postings.values.bin");
            pre_stmt_postings_offsets.open(gendb_dir + "/indexes/pre/pre_stmt_postings.offsets.bin");
            pre_stmt_postings_rowids.open(gendb_dir + "/indexes/pre/pre_stmt_postings.rowids.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            pre_stmt_dict.open(gendb_dir + "/dicts/pre_stmt");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name");
            pre_plabel_dict.open(gendb_dir + "/dicts/pre_plabel");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");

            require_non_empty("num.adsh", num_adsh);
            require_non_empty("num.tag", num_tag);
            require_non_empty("num.version", num_version);
            require_non_empty("num.value", num_value);
            require_non_empty("sub.adsh", sub_adsh);
            require_non_empty("sub.name", sub_name);
            require_non_empty("sub.adsh_dense_lookup", sub_adsh_dense_lookup);
            require_non_empty("pre.adsh", pre_adsh);
            require_non_empty("pre.tag", pre_tag);
            require_non_empty("pre.version", pre_version);
            require_non_empty("pre.plabel", pre_plabel);

            require_non_empty("num_uom_postings.values", num_uom_postings_values);
            require_non_empty("num_uom_postings.offsets", num_uom_postings_offsets);
            require_non_empty("num_uom_postings.rowids", num_uom_postings_rowids);
            require_non_empty("sub_fy_postings.values", sub_fy_postings_values);
            require_non_empty("sub_fy_postings.offsets", sub_fy_postings_offsets);
            require_non_empty("sub_fy_postings.rowids", sub_fy_postings_rowids);
            require_non_empty("pre_stmt_postings.values", pre_stmt_postings_values);
            require_non_empty("pre_stmt_postings.offsets", pre_stmt_postings_offsets);
            require_non_empty("pre_stmt_postings.rowids", pre_stmt_postings_rowids);

            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_value.advise_random();
            pre_adsh.advise_random();
            pre_tag.advise_random();
            pre_version.advise_random();
            pre_plabel.advise_random();

            gendb::mmap_prefetch_all(
                num_adsh,
                num_tag,
                num_version,
                num_value,
                sub_adsh,
                sub_name,
                sub_adsh_dense_lookup,
                pre_adsh,
                pre_tag,
                pre_version,
                pre_plabel,
                num_uom_postings_values,
                num_uom_postings_offsets,
                num_uom_postings_rowids,
                sub_fy_postings_values,
                sub_fy_postings_offsets,
                sub_fy_postings_rowids,
                pre_stmt_postings_values,
                pre_stmt_postings_offsets,
                pre_stmt_postings_rowids
            );
        }

        {
            GENDB_PHASE("dim_filter");

            usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            is_code = pre_stmt_dict.find_code<uint16_t>("IS", "pre_stmt");

            fy_2023_slice = find_posting_slice(sub_fy_postings_values, sub_fy_postings_offsets, static_cast<int16_t>(2023));
            num_usd_slice = find_posting_slice(num_uom_postings_values, num_uom_postings_offsets, usd_code);
            pre_is_slice = find_posting_slice(pre_stmt_postings_values, pre_stmt_postings_offsets, is_code);

            if (!fy_2023_slice.found || !num_usd_slice.found || !pre_is_slice.found) {
                write_empty_output(results_dir);
                return 0;
            }

            if (fy_2023_slice.end > sub_fy_postings_rowids.size() ||
                num_usd_slice.end > num_uom_postings_rowids.size() ||
                pre_is_slice.end > pre_stmt_postings_rowids.size()) {
                throw std::runtime_error("posting slice exceeds rowid array");
            }

            adsh_to_name.assign(sub_adsh_dense_lookup.size(), kEmpty32);
            for (uint64_t pos = fy_2023_slice.begin; pos < fy_2023_slice.end; ++pos) {
                const uint32_t rowid = sub_fy_postings_rowids[pos];
                if (rowid >= sub_adsh.size() || rowid >= sub_name.size()) {
                    throw std::runtime_error("sub_fy_postings rowid out of range");
                }
                const uint32_t adsh = sub_adsh[rowid];
                if (adsh >= adsh_to_name.size()) {
                    throw std::runtime_error("sub.adsh code exceeds dense lookup domain");
                }
                adsh_to_name[adsh] = sub_name[rowid];
            }
        }

        {
            GENDB_PHASE("build_joins");

            const uint64_t is_rows = pre_is_slice.end - pre_is_slice.begin;
            pre_hash.reserve(static_cast<size_t>(is_rows));

            for (uint64_t pos = pre_is_slice.begin; pos < pre_is_slice.end; ++pos) {
                const uint32_t rowid = pre_stmt_postings_rowids[pos];
                if (rowid >= pre_adsh.size() || rowid >= pre_tag.size() ||
                    rowid >= pre_version.size() || rowid >= pre_plabel.size()) {
                    throw std::runtime_error("pre_stmt_postings rowid out of range");
                }
                pre_hash.find_or_insert(pre_adsh[rowid], pre_tag[rowid], pre_version[rowid]);
            }

            pre_group_offsets.assign(static_cast<size_t>(pre_hash.group_count()) + 1, 0);
            for (const PreBucket& bucket : pre_hash.buckets()) {
                if (bucket.group_index != kEmpty32) {
                    pre_group_offsets[static_cast<size_t>(bucket.group_index) + 1] = bucket.count;
                }
            }
            for (size_t i = 1; i < pre_group_offsets.size(); ++i) {
                pre_group_offsets[i] += pre_group_offsets[i - 1];
            }

            if (pre_group_offsets.back() != is_rows) {
                throw std::runtime_error("pre group payload size mismatch");
            }

            pre_group_plabels.assign(static_cast<size_t>(is_rows), 0);
            std::vector<uint64_t> next_offset = pre_group_offsets;

            for (uint64_t pos = pre_is_slice.begin; pos < pre_is_slice.end; ++pos) {
                const uint32_t rowid = pre_stmt_postings_rowids[pos];
                const PreBucket* bucket = pre_hash.find(pre_adsh[rowid], pre_tag[rowid], pre_version[rowid]);
                if (bucket == nullptr) {
                    throw std::runtime_error("pre hash lookup failed during payload fill");
                }
                const uint32_t group = bucket->group_index;
                pre_group_plabels[static_cast<size_t>(next_offset[group]++)] = pre_plabel[rowid];
            }
        }

        std::vector<AggHashMap> local_aggs;

        {
            GENDB_PHASE("main_scan");

            const int thread_count = std::max(1, omp_get_num_procs());
            const uint64_t drive_rows = num_usd_slice.end - num_usd_slice.begin;
            const size_t reserve_per_thread = static_cast<size_t>(std::max<uint64_t>(16384, drive_rows / static_cast<uint64_t>(thread_count) / 64));
            local_aggs.resize(static_cast<size_t>(thread_count));
            for (AggHashMap& map : local_aggs) {
                map.reserve(reserve_per_thread);
            }

            const uint32_t* __restrict usd_rowids = num_uom_postings_rowids.data;
            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;
            const uint32_t* __restrict adsh_to_name_data = adsh_to_name.data();
            const size_t adsh_to_name_size = adsh_to_name.size();
            const uint32_t* __restrict plabel_payload = pre_group_plabels.data();
            const uint64_t* __restrict group_offsets = pre_group_offsets.data();

            const uint64_t morsels = (drive_rows + kMorselRows - 1) / kMorselRows;

#pragma omp parallel for schedule(dynamic, 1) num_threads(thread_count)
            for (uint64_t morsel = 0; morsel < morsels; ++morsel) {
                AggHashMap& local = local_aggs[static_cast<size_t>(omp_get_thread_num())];
                const uint64_t lo = num_usd_slice.begin + morsel * kMorselRows;
                const uint64_t hi = std::min(lo + kMorselRows, num_usd_slice.end);

                for (uint64_t pos = lo; pos < hi; ++pos) {
                    const uint32_t rowid = usd_rowids[pos];
                    if (rowid >= num_value.size() || rowid >= num_adsh.size() ||
                        rowid >= num_tag.size() || rowid >= num_version.size()) {
                        throw std::runtime_error("num_uom_postings rowid out of range");
                    }

                    const double value = num_value_data[rowid];
                    if (std::isnan(value)) {
                        continue;
                    }

                    const uint32_t adsh = num_adsh_data[rowid];
                    if (adsh >= adsh_to_name_size) {
                        continue;
                    }
                    const uint32_t name = adsh_to_name_data[adsh];
                    if (name == kEmpty32) {
                        continue;
                    }

                    const uint32_t tag = num_tag_data[rowid];
                    const uint32_t version = num_version_data[rowid];
                    const PreBucket* bucket = pre_hash.find(adsh, tag, version);
                    if (bucket == nullptr) {
                        continue;
                    }

                    const uint64_t payload_begin = group_offsets[bucket->group_index];
                    const uint64_t payload_end = group_offsets[static_cast<size_t>(bucket->group_index) + 1];
                    for (uint64_t payload = payload_begin; payload < payload_end; ++payload) {
                        local.add(name, tag, plabel_payload[payload], value);
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            size_t total_groups = 0;
            for (const AggHashMap& local : local_aggs) {
                total_groups += local.size();
            }

            AggHashMap global_agg(std::max<size_t>(16, total_groups));
            for (const AggHashMap& local : local_aggs) {
                local.for_each([&](const AggEntry& entry) {
                    global_agg.add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.sum);
                });
            }

            std::vector<ResultRow> rows;
            rows.reserve(global_agg.size());
            global_agg.for_each([&](const AggEntry& entry) {
                rows.push_back(ResultRow{entry.name, entry.tag, entry.plabel, entry.sum, entry.cnt});
            });

            auto cmp = [](const ResultRow& lhs, const ResultRow& rhs) {
                if (lhs.total_value != rhs.total_value) return lhs.total_value > rhs.total_value;
                if (lhs.name != rhs.name) return lhs.name < rhs.name;
                if (lhs.tag != rhs.tag) return lhs.tag < rhs.tag;
                if (lhs.plabel != rhs.plabel) return lhs.plabel < rhs.plabel;
                return lhs.cnt < rhs.cnt;
            };

            if (rows.size() > kTopK) {
                std::partial_sort(rows.begin(), rows.begin() + kTopK, rows.end(), cmp);
                rows.resize(kTopK);
            } else {
                std::sort(rows.begin(), rows.end(), cmp);
            }

            fs::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q6.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("cannot open output file: " + out_path);
            }

            std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
            const std::string_view stmt_value = pre_stmt_dict.decode(is_code);
            for (const ResultRow& row : rows) {
                write_csv_field(out, sub_name_dict.decode(row.name));
                std::fputc(',', out);
                write_csv_field(out, stmt_value);
                std::fputc(',', out);
                write_csv_field(out, global_tag_dict.decode(row.tag));
                std::fputc(',', out);
                write_csv_field(out, pre_plabel_dict.decode(row.plabel));
                std::fprintf(out, ",%.2f,%llu\n", row.total_value, static_cast<unsigned long long>(row.cnt));
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
