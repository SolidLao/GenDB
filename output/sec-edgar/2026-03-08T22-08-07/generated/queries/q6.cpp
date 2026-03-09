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
constexpr size_t kTopK = 200;
constexpr uint64_t kTargetMorselRows = 1ull << 17;

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

    size_t size() const {
        return offsets.size() - 1;
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
void require_non_empty(const char* name, const MmapColumn<T>& col) {
    if (col.size() == 0) {
        throw std::runtime_error(std::string("empty mmap column: ") + name);
    }
}

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
        throw std::runtime_error("posting offsets out of range");
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

inline uint64_t hash_pair(uint32_t a, uint32_t b) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    return mix64(seed);
}

inline uint64_t hash_group(uint32_t name, uint32_t tag, uint32_t plabel) {
    uint64_t seed = mix64(static_cast<uint64_t>(name) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(tag) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(plabel) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
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

    void add_merged(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double sum) {
        if (cnt == 0) {
            return;
        }
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

class FilingPairAgg {
public:
    void reset(size_t expected_keys) {
        size_t desired = 8;
        while (desired < expected_keys * 2 + 1) {
            desired <<= 1;
        }

        if (tags_.size() < desired) {
            tags_.assign(desired, kEmpty32);
            versions_.assign(desired, 0);
            cnts_.assign(desired, 0);
            sums_.assign(desired, 0.0);
            used_slots_.clear();
        } else {
            for (uint32_t slot : used_slots_) {
                tags_[slot] = kEmpty32;
            }
            used_slots_.clear();
        }

        mask_ = tags_.empty() ? 0 : (tags_.size() - 1);
        size_ = 0;
    }

    bool empty() const {
        return size_ == 0;
    }

    void insert_key(uint32_t tag, uint32_t version) {
        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            if (tags_[slot] == kEmpty32) {
                tags_[slot] = tag;
                versions_[slot] = version;
                cnts_[slot] = 0;
                sums_[slot] = 0.0;
                used_slots_.push_back(static_cast<uint32_t>(slot));
                ++size_;
                return;
            }
            if (tags_[slot] == tag && versions_[slot] == version) {
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    inline void add_if_present(uint32_t tag, uint32_t version, double value) {
        if (size_ == 0) {
            return;
        }

        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            const uint32_t stored_tag = tags_[slot];
            if (stored_tag == kEmpty32) {
                return;
            }
            if (stored_tag == tag && versions_[slot] == version) {
                ++cnts_[slot];
                sums_[slot] += value;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    template <typename Fn>
    inline void probe(uint32_t tag, uint32_t version, Fn&& fn) const {
        if (size_ == 0) {
            return;
        }

        size_t slot = static_cast<size_t>(hash_pair(tag, version) & mask_);
        while (true) {
            const uint32_t stored_tag = tags_[slot];
            if (stored_tag == kEmpty32) {
                return;
            }
            if (stored_tag == tag && versions_[slot] == version) {
                fn(cnts_[slot], sums_[slot]);
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

private:
    std::vector<uint32_t> tags_;
    std::vector<uint32_t> versions_;
    std::vector<uint64_t> cnts_;
    std::vector<double> sums_;
    std::vector<uint32_t> used_slots_;
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

        MmapColumn<uint64_t> num_dense_offsets;
        MmapColumn<uint32_t> num_dense_rowids;
        MmapColumn<uint16_t> num_uom;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<uint32_t> num_version;
        MmapColumn<double> num_value;

        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;
        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;

        MmapColumn<uint32_t> pre_adsh;
        MmapColumn<uint32_t> pre_tag;
        MmapColumn<uint32_t> pre_version;
        MmapColumn<uint32_t> pre_plabel;
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
        PostingSlice<int16_t> sub_2023_slice;
        PostingSlice<uint16_t> pre_is_slice;

        std::vector<uint32_t> filtered_name_by_adsh;
        std::vector<uint32_t> qualifying_adsh_codes;
        std::vector<uint32_t> pre_is_offsets_by_adsh;
        std::vector<uint32_t> pre_is_tag;
        std::vector<uint32_t> pre_is_version;
        std::vector<uint32_t> pre_is_plabel;
        std::vector<uint32_t> morsel_bounds;
        std::vector<AggHashMap> local_aggs;

        {
            GENDB_PHASE("data_loading");

            num_dense_offsets.open(gendb_dir + "/column_versions/num.adsh.postings_dense/offsets.bin");
            num_dense_rowids.open(gendb_dir + "/column_versions/num.adsh.postings_dense/rowids.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_postings_values.open(gendb_dir + "/indexes/pre/pre_stmt_postings.values.bin");
            pre_stmt_postings_offsets.open(gendb_dir + "/indexes/pre/pre_stmt_postings.offsets.bin");
            pre_stmt_postings_rowids.open(gendb_dir + "/indexes/pre/pre_stmt_postings.rowids.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            pre_stmt_dict.open(gendb_dir + "/dicts/pre_stmt");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name");
            pre_plabel_dict.open(gendb_dir + "/dicts/pre_plabel");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag");

            require_non_empty("num.adsh.postings_dense.offsets", num_dense_offsets);
            require_non_empty("num.adsh.postings_dense.rowids", num_dense_rowids);
            require_non_empty("num.uom", num_uom);
            require_non_empty("num.tag", num_tag);
            require_non_empty("num.version", num_version);
            require_non_empty("num.value", num_value);
            require_non_empty("sub.adsh", sub_adsh);
            require_non_empty("sub.name", sub_name);
            require_non_empty("sub_fy_postings.values", sub_fy_postings_values);
            require_non_empty("sub_fy_postings.offsets", sub_fy_postings_offsets);
            require_non_empty("sub_fy_postings.rowids", sub_fy_postings_rowids);
            require_non_empty("pre.adsh", pre_adsh);
            require_non_empty("pre.tag", pre_tag);
            require_non_empty("pre.version", pre_version);
            require_non_empty("pre.plabel", pre_plabel);
            require_non_empty("pre_stmt_postings.values", pre_stmt_postings_values);
            require_non_empty("pre_stmt_postings.offsets", pre_stmt_postings_offsets);
            require_non_empty("pre_stmt_postings.rowids", pre_stmt_postings_rowids);

            if (num_uom.size() != num_tag.size() || num_uom.size() != num_version.size() ||
                num_uom.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (num_dense_rowids.size() != num_uom.size()) {
                throw std::runtime_error("dense postings rowids size mismatch");
            }
            if (sub_adsh.size() != sub_name.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }

            sub_fy_postings_values.prefetch();
            sub_fy_postings_offsets.prefetch();
            sub_fy_postings_rowids.prefetch();
            sub_adsh.prefetch();
            sub_name.prefetch();

            pre_stmt_postings_values.prefetch();
            pre_stmt_postings_offsets.prefetch();
            pre_stmt_postings_rowids.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_plabel.prefetch();

            num_dense_offsets.prefetch();
            num_dense_rowids.prefetch();
            num_uom.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_value.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");

            usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            is_code = pre_stmt_dict.find_code<uint16_t>("IS", "pre_stmt");

            sub_2023_slice = find_posting_slice(sub_fy_postings_values, sub_fy_postings_offsets,
                                                static_cast<int16_t>(2023));
            pre_is_slice = find_posting_slice(pre_stmt_postings_values, pre_stmt_postings_offsets,
                                              is_code);

            if (!sub_2023_slice.found || sub_2023_slice.begin == sub_2023_slice.end ||
                !pre_is_slice.found || pre_is_slice.begin == pre_is_slice.end) {
                write_empty_output(results_dir);
                return 0;
            }
        }

        {
            GENDB_PHASE("build_joins");

            const uint32_t adsh_count = static_cast<uint32_t>(num_dense_offsets.size() - 1);
            filtered_name_by_adsh.assign(static_cast<size_t>(adsh_count), kEmpty32);
            qualifying_adsh_codes.reserve(static_cast<size_t>(sub_2023_slice.end - sub_2023_slice.begin));

            const uint32_t* __restrict sub_fy_rowids = sub_fy_postings_rowids.data;
            const uint32_t* __restrict sub_adsh_data = sub_adsh.data;
            const uint32_t* __restrict sub_name_data = sub_name.data;
            for (uint64_t pos = sub_2023_slice.begin; pos < sub_2023_slice.end; ++pos) {
                const uint32_t rowid = sub_fy_rowids[pos];
                const uint32_t adsh = sub_adsh_data[rowid];
                if (adsh >= adsh_count) {
                    continue;
                }
                if (filtered_name_by_adsh[adsh] == kEmpty32) {
                    qualifying_adsh_codes.push_back(adsh);
                }
                filtered_name_by_adsh[adsh] = sub_name_data[rowid];
            }

            if (qualifying_adsh_codes.empty()) {
                write_empty_output(results_dir);
                return 0;
            }

            std::sort(qualifying_adsh_codes.begin(), qualifying_adsh_codes.end());

            pre_is_offsets_by_adsh.assign(static_cast<size_t>(adsh_count) + 1, 0);
            const uint32_t* __restrict pre_is_rowids = pre_stmt_postings_rowids.data;
            const uint32_t* __restrict pre_adsh_data = pre_adsh.data;
            for (uint64_t pos = pre_is_slice.begin; pos < pre_is_slice.end; ++pos) {
                const uint32_t rowid = pre_is_rowids[pos];
                const uint32_t adsh = pre_adsh_data[rowid];
                if (adsh < adsh_count && filtered_name_by_adsh[adsh] != kEmpty32) {
                    ++pre_is_offsets_by_adsh[static_cast<size_t>(adsh) + 1];
                }
            }

            for (size_t i = 1; i < pre_is_offsets_by_adsh.size(); ++i) {
                pre_is_offsets_by_adsh[i] += pre_is_offsets_by_adsh[i - 1];
            }

            const size_t qualified_pre_count = pre_is_offsets_by_adsh.back();
            if (qualified_pre_count == 0) {
                write_empty_output(results_dir);
                return 0;
            }

            pre_is_tag.resize(qualified_pre_count);
            pre_is_version.resize(qualified_pre_count);
            pre_is_plabel.resize(qualified_pre_count);

            std::vector<uint32_t> fill_offsets = pre_is_offsets_by_adsh;
            const uint32_t* __restrict pre_tag_data = pre_tag.data;
            const uint32_t* __restrict pre_version_data = pre_version.data;
            const uint32_t* __restrict pre_plabel_data = pre_plabel.data;
            for (uint64_t pos = pre_is_slice.begin; pos < pre_is_slice.end; ++pos) {
                const uint32_t rowid = pre_is_rowids[pos];
                const uint32_t adsh = pre_adsh_data[rowid];
                if (adsh >= adsh_count || filtered_name_by_adsh[adsh] == kEmpty32) {
                    continue;
                }
                const uint32_t dst = fill_offsets[adsh]++;
                pre_is_tag[dst] = pre_tag_data[rowid];
                pre_is_version[dst] = pre_version_data[rowid];
                pre_is_plabel[dst] = pre_plabel_data[rowid];
            }

            morsel_bounds.clear();
            morsel_bounds.push_back(0);
            uint64_t rows_in_morsel = 0;
            for (size_t idx = 0; idx < qualifying_adsh_codes.size(); ++idx) {
                const uint32_t adsh = qualifying_adsh_codes[idx];
                const uint64_t begin = num_dense_offsets[adsh];
                const uint64_t end = num_dense_offsets[adsh + 1];
                if (end < begin || end > num_dense_rowids.size()) {
                    throw std::runtime_error("dense postings slice out of range");
                }
                rows_in_morsel += (end - begin);
                if (rows_in_morsel >= kTargetMorselRows) {
                    morsel_bounds.push_back(static_cast<uint32_t>(idx + 1));
                    rows_in_morsel = 0;
                }
            }
            if (morsel_bounds.back() != qualifying_adsh_codes.size()) {
                morsel_bounds.push_back(static_cast<uint32_t>(qualifying_adsh_codes.size()));
            }
        }

        {
            GENDB_PHASE("main_scan");

            const int thread_count = std::max(1, std::min(omp_get_num_procs(), 16));

            uint64_t qualifying_num_rows = 0;
            for (uint32_t adsh : qualifying_adsh_codes) {
                qualifying_num_rows += (num_dense_offsets[adsh + 1] - num_dense_offsets[adsh]);
            }
            const size_t reserve_per_thread = static_cast<size_t>(
                std::max<uint64_t>(16384, qualifying_num_rows / static_cast<uint64_t>(thread_count) / 64));

            local_aggs.resize(static_cast<size_t>(thread_count));
            for (AggHashMap& map : local_aggs) {
                map.reserve(reserve_per_thread);
            }
            std::vector<FilingPairAgg> pair_aggs(static_cast<size_t>(thread_count));

            const uint32_t* __restrict dense_rowids = num_dense_rowids.data;
            const uint16_t* __restrict num_uom_data = num_uom.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;

            const uint32_t* __restrict filtered_name = filtered_name_by_adsh.data();
            const uint32_t* __restrict pre_offsets = pre_is_offsets_by_adsh.data();
            const uint32_t* __restrict pre_tag_payload = pre_is_tag.data();
            const uint32_t* __restrict pre_version_payload = pre_is_version.data();
            const uint32_t* __restrict pre_plabel_payload = pre_is_plabel.data();
            const uint32_t* __restrict qualifying_codes = qualifying_adsh_codes.data();
            const uint32_t* __restrict morsels = morsel_bounds.data();
            const size_t morsel_count = morsel_bounds.empty() ? 0 : (morsel_bounds.size() - 1);

#pragma omp parallel for schedule(dynamic, 1) num_threads(thread_count)
            for (size_t morsel_idx = 0; morsel_idx < morsel_count; ++morsel_idx) {
                const int tid = omp_get_thread_num();
                AggHashMap& local = local_aggs[static_cast<size_t>(tid)];
                FilingPairAgg& pair_agg = pair_aggs[static_cast<size_t>(tid)];

                const uint32_t filing_lo = morsels[morsel_idx];
                const uint32_t filing_hi = morsels[morsel_idx + 1];
                for (uint32_t filing_idx = filing_lo; filing_idx < filing_hi; ++filing_idx) {
                    const uint32_t adsh = qualifying_codes[filing_idx];
                    const uint32_t name = filtered_name[adsh];
                    const uint32_t pre_begin = pre_offsets[adsh];
                    const uint32_t pre_end = pre_offsets[adsh + 1];
                    if (name == kEmpty32 || pre_begin == pre_end) {
                        continue;
                    }

                    const uint64_t num_begin = num_dense_offsets[adsh];
                    const uint64_t num_end = num_dense_offsets[adsh + 1];
                    if (num_begin == num_end) {
                        continue;
                    }

                    pair_agg.reset(static_cast<size_t>(pre_end - pre_begin));
                    for (uint32_t pre_idx = pre_begin; pre_idx < pre_end; ++pre_idx) {
                        pair_agg.insert_key(pre_tag_payload[pre_idx], pre_version_payload[pre_idx]);
                    }

                    for (uint64_t pos = num_begin; pos < num_end; ++pos) {
                        const uint32_t rowid = dense_rowids[pos];
                        if (num_uom_data[rowid] != usd_code) {
                            continue;
                        }

                        const double value = num_value_data[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        pair_agg.add_if_present(num_tag_data[rowid], num_version_data[rowid], value);
                    }

                    if (pair_agg.empty()) {
                        continue;
                    }

                    for (uint32_t pre_idx = pre_begin; pre_idx < pre_end; ++pre_idx) {
                        const uint32_t tag = pre_tag_payload[pre_idx];
                        const uint32_t version = pre_version_payload[pre_idx];
                        const uint32_t plabel = pre_plabel_payload[pre_idx];
                        pair_agg.probe(tag, version, [&](uint64_t cnt, double sum) {
                            if (cnt != 0) {
                                local.add_merged(name, tag, plabel, cnt, sum);
                            }
                        });
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            size_t merged_size_hint = 16;
            for (const AggHashMap& local : local_aggs) {
                merged_size_hint += local.size();
            }

            AggHashMap global_agg(merged_size_hint);
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

            const std::string_view stmt_value = pre_stmt_dict.decode(is_code);
            std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const ResultRow& row : rows) {
                write_csv_field(out, sub_name_dict.decode(row.name));
                std::fputc(',', out);
                write_csv_field(out, stmt_value);
                std::fputc(',', out);
                write_csv_field(out, global_tag_dict.decode(row.tag));
                std::fputc(',', out);
                write_csv_field(out, pre_plabel_dict.decode(row.plabel));
                std::fprintf(out, ",%.2f,%llu\n", row.total_value,
                             static_cast<unsigned long long>(row.cnt));
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
