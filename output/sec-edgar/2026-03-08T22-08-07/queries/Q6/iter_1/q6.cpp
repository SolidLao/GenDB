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

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
};

inline const TripleBucket* probe_pre_group(const TripleBucket* buckets,
                                           size_t bucket_mask,
                                           uint32_t adsh,
                                           uint32_t tag,
                                           uint32_t version) {
    size_t slot = static_cast<size_t>(hash_triple(adsh, tag, version) & bucket_mask);
    while (true) {
        const TripleBucket& bucket = buckets[slot];
        if (bucket.group_index == kEmpty64) {
            return nullptr;
        }
        if (bucket.a == adsh && bucket.b == tag && bucket.c == version) {
            return &bucket;
        }
        slot = (slot + 1) & bucket_mask;
    }
}

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

int main(int argc, char** argv) {
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

        MmapColumn<uint16_t> num_uom_postings_values;
        MmapColumn<uint64_t> num_uom_postings_offsets;
        MmapColumn<uint32_t> num_uom_postings_rowids;

        MmapColumn<uint32_t> sub_name;
        MmapColumn<int16_t> sub_fy;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;

        MmapColumn<uint16_t> pre_stmt;
        MmapColumn<uint32_t> pre_plabel;
        MmapColumn<uint32_t> pre_group_rowids;
        MmapColumn<uint64_t> pre_group_offsets;
        MmapColumn<TripleBucket> pre_group_hash;

        DictView num_uom_dict;
        DictView pre_stmt_dict;
        DictView sub_name_dict;
        DictView pre_plabel_dict;
        DictView global_tag_dict;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        PostingSlice<uint16_t> num_usd_slice;
        size_t pre_bucket_mask = 0;
        std::vector<AggHashMap> local_aggs;

        {
            GENDB_PHASE("data_loading");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_group_rowids.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.rowids.bin");
            pre_group_offsets.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.offsets.bin");
            pre_group_hash.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.hash.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            pre_stmt_dict.open(gendb_dir + "/dicts/pre_stmt");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name");
            pre_plabel_dict.open(gendb_dir + "/dicts/pre_plabel");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");

            require_non_empty("num.adsh", num_adsh);
            require_non_empty("num.tag", num_tag);
            require_non_empty("num.version", num_version);
            require_non_empty("num.value", num_value);
            require_non_empty("num_uom_postings.values", num_uom_postings_values);
            require_non_empty("num_uom_postings.offsets", num_uom_postings_offsets);
            require_non_empty("num_uom_postings.rowids", num_uom_postings_rowids);
            require_non_empty("sub.name", sub_name);
            require_non_empty("sub.fy", sub_fy);
            require_non_empty("sub.adsh_dense_lookup", sub_adsh_dense_lookup);
            require_non_empty("pre.stmt", pre_stmt);
            require_non_empty("pre.plabel", pre_plabel);
            require_non_empty("pre_group_rowids", pre_group_rowids);
            require_non_empty("pre_group_offsets", pre_group_offsets);
            require_non_empty("pre_group_hash", pre_group_hash);

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_name.size() != sub_fy.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_stmt.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }

            num_uom_postings_values.prefetch();
            num_uom_postings_offsets.prefetch();
            num_uom_postings_rowids.prefetch();
            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_value.prefetch();

            sub_name.advise_random();
            sub_fy.advise_random();
            sub_adsh_dense_lookup.advise_random();
            sub_name.prefetch();
            sub_fy.prefetch();
            sub_adsh_dense_lookup.prefetch();

            pre_stmt.advise_random();
            pre_plabel.advise_random();
            pre_group_rowids.advise_random();
            pre_group_offsets.advise_random();
            pre_group_hash.advise_random();
            pre_stmt.prefetch();
            pre_plabel.prefetch();
            pre_group_rowids.prefetch();
            pre_group_offsets.prefetch();
            pre_group_hash.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");

            usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            is_code = pre_stmt_dict.find_code<uint16_t>("IS", "pre_stmt");
            num_usd_slice = find_posting_slice(num_uom_postings_values, num_uom_postings_offsets, usd_code);
            if (!num_usd_slice.found || num_usd_slice.begin == num_usd_slice.end) {
                write_empty_output(results_dir);
                return 0;
            }
            if (num_usd_slice.end > num_uom_postings_rowids.size()) {
                throw std::runtime_error("USD posting slice exceeds rowid array");
            }
        }

        {
            GENDB_PHASE("build_joins");

            if (pre_group_offsets.size() < 2) {
                throw std::runtime_error("invalid pre group offsets");
            }
            if (pre_group_offsets[pre_group_offsets.size() - 1] > pre_group_rowids.size()) {
                throw std::runtime_error("pre group offsets exceed rowids payload");
            }
            if ((pre_group_hash.size() & (pre_group_hash.size() - 1)) != 0) {
                throw std::runtime_error("pre group hash bucket count is not a power of two");
            }
            pre_bucket_mask = pre_group_hash.size() - 1;
        }

        {
            GENDB_PHASE("main_scan");

            const int thread_count = std::max(1, omp_get_num_procs());
            const uint64_t drive_rows = num_usd_slice.end - num_usd_slice.begin;
            const size_t reserve_per_thread = static_cast<size_t>(
                std::max<uint64_t>(16384, drive_rows / static_cast<uint64_t>(thread_count) / 64));

            local_aggs.resize(static_cast<size_t>(thread_count));
            for (AggHashMap& map : local_aggs) {
                map.reserve(reserve_per_thread);
            }

            const uint32_t* __restrict usd_rowids = num_uom_postings_rowids.data;
            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;

            const uint32_t* __restrict sub_lookup = sub_adsh_dense_lookup.data;
            const size_t sub_lookup_size = sub_adsh_dense_lookup.size();
            const int16_t* __restrict sub_fy_data = sub_fy.data;
            const uint32_t* __restrict sub_name_data = sub_name.data;

            const TripleBucket* __restrict pre_hash_buckets = pre_group_hash.data;
            const uint64_t* __restrict pre_offsets = pre_group_offsets.data;
            const uint32_t* __restrict pre_rowids = pre_group_rowids.data;
            const uint16_t* __restrict pre_stmt_data = pre_stmt.data;
            const uint32_t* __restrict pre_plabel_data = pre_plabel.data;

            const uint64_t morsels = (drive_rows + kMorselRows - 1) / kMorselRows;

#pragma omp parallel for schedule(dynamic, 1) num_threads(thread_count)
            for (uint64_t morsel = 0; morsel < morsels; ++morsel) {
                AggHashMap& local = local_aggs[static_cast<size_t>(omp_get_thread_num())];
                const uint64_t lo = num_usd_slice.begin + morsel * kMorselRows;
                const uint64_t hi = std::min(lo + kMorselRows, num_usd_slice.end);

                for (uint64_t pos = lo; pos < hi; ++pos) {
                    const uint32_t rowid = usd_rowids[pos];
                    const double value = num_value_data[rowid];
                    if (std::isnan(value)) {
                        continue;
                    }

                    const uint32_t adsh = num_adsh_data[rowid];
                    if (adsh >= sub_lookup_size) {
                        continue;
                    }

                    const uint32_t sub_rowid = sub_lookup[adsh];
                    if (sub_rowid == kEmpty32 || sub_fy_data[sub_rowid] != 2023) {
                        continue;
                    }

                    const uint32_t tag = num_tag_data[rowid];
                    const uint32_t version = num_version_data[rowid];
                    const TripleBucket* bucket = probe_pre_group(pre_hash_buckets, pre_bucket_mask, adsh, tag, version);
                    if (bucket == nullptr) {
                        continue;
                    }

                    const uint32_t name = sub_name_data[sub_rowid];
                    const uint64_t begin = pre_offsets[bucket->group_index];
                    const uint64_t end = pre_offsets[bucket->group_index + 1];
                    for (uint64_t idx = begin; idx < end; ++idx) {
                        const uint32_t pre_rowid = pre_rowids[idx];
                        if (pre_stmt_data[pre_rowid] == is_code) {
                            local.add(name, tag, pre_plabel_data[pre_rowid], value);
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<AggHashMap> reduction_maps = std::move(local_aggs);
            const int merge_threads = std::max(1, omp_get_num_procs());
            while (reduction_maps.size() > 1) {
                const size_t pair_count = reduction_maps.size() / 2;
                const bool has_odd = (reduction_maps.size() & 1) != 0;
                std::vector<AggHashMap> next_maps(pair_count + (has_odd ? 1 : 0));

#pragma omp parallel for schedule(static) num_threads(merge_threads)
                for (size_t pair_idx = 0; pair_idx < pair_count; ++pair_idx) {
                    const AggHashMap& lhs = reduction_maps[pair_idx * 2];
                    const AggHashMap& rhs = reduction_maps[pair_idx * 2 + 1];
                    AggHashMap merged(std::max<size_t>(16, lhs.size() + rhs.size()));
                    lhs.for_each([&](const AggEntry& entry) {
                        merged.add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.sum);
                    });
                    rhs.for_each([&](const AggEntry& entry) {
                        merged.add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.sum);
                    });
                    next_maps[pair_idx] = std::move(merged);
                }

                if (has_odd) {
                    next_maps[pair_count] = std::move(reduction_maps.back());
                }

                reduction_maps = std::move(next_maps);
            }

            AggHashMap& global_agg = reduction_maps.front();

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
