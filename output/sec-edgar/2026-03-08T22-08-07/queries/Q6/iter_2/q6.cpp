#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace fs = std::filesystem;

namespace {

using gendb::MmapColumn;

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kTopK = 200;
constexpr uint64_t kMorselRows = 1ull << 15;
constexpr size_t kPartitionBits = 19;
constexpr size_t kPartitionCount = 1ull << kPartitionBits;
constexpr size_t kPartitionShift = 64 - kPartitionBits;

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
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

template <typename T>
void require_non_empty(const char* name, const MmapColumn<T>& col) {
    if (col.size() == 0) {
        throw std::runtime_error(std::string("empty mmap column: ") + name);
    }
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

inline size_t pre_partition(uint32_t adsh, uint32_t tag, uint32_t version) {
    return static_cast<size_t>(hash_triple(adsh, tag, version) >> kPartitionShift);
}

inline bool bitset_contains(const std::vector<uint64_t>& bits, uint32_t rowid) {
    return ((bits[rowid >> 6] >> (rowid & 63)) & 1ull) != 0;
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

struct Survivor {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t name = 0;
    uint32_t partition = 0;
    double value = 0.0;
};

struct AlignedU32Array {
    uint32_t* data = nullptr;
    size_t size = 0;

    ~AlignedU32Array() {
        std::free(data);
    }

    void allocate(size_t count, size_t alignment = 64) {
        std::free(data);
        data = nullptr;
        size = count;
        if (count == 0) {
            return;
        }
        const size_t bytes = count * sizeof(uint32_t);
        const size_t padded = ((bytes + alignment - 1) / alignment) * alignment;
        void* ptr = nullptr;
        if (posix_memalign(&ptr, alignment, padded) != 0 || ptr == nullptr) {
            throw std::runtime_error("posix_memalign failed for group permutation");
        }
        data = static_cast<uint32_t*>(ptr);
    }

    uint32_t& operator[](size_t idx) {
        return data[idx];
    }

    const uint32_t& operator[](size_t idx) const {
        return data[idx];
    }
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

        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;
        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;

        MmapColumn<uint32_t> pre_plabel;
        MmapColumn<uint16_t> pre_stmt_postings_values;
        MmapColumn<uint64_t> pre_stmt_postings_offsets;
        MmapColumn<uint32_t> pre_stmt_postings_rowids;
        MmapColumn<TripleKey> pre_group_keys;
        MmapColumn<uint64_t> pre_group_offsets;
        MmapColumn<uint32_t> pre_group_rowids;
        MmapColumn<uint64_t> pre_partition_offsets;
        MmapColumn<TripleKey> pre_partition_keys;

        DictView num_uom_dict;
        DictView pre_stmt_dict;
        DictView sub_name_dict;
        DictView pre_plabel_dict;
        DictView global_tag_dict;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        PostingSlice<uint16_t> num_usd_slice;
        PostingSlice<int16_t> sub_2023_slice;
        PostingSlice<uint16_t> pre_is_slice;
        std::vector<uint32_t> filtered_name_by_adsh;
        std::vector<uint64_t> pre_is_bits;
        std::vector<uint32_t> group_is_offsets;
        std::vector<uint32_t> group_is_plabels;
        AlignedU32Array partitioned_group_index;
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

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");
            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");

            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_postings_values.open(gendb_dir + "/indexes/pre/pre_stmt_postings.values.bin");
            pre_stmt_postings_offsets.open(gendb_dir + "/indexes/pre/pre_stmt_postings.offsets.bin");
            pre_stmt_postings_rowids.open(gendb_dir + "/indexes/pre/pre_stmt_postings.rowids.bin");
            pre_group_keys.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.keys.bin");
            pre_group_offsets.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.offsets.bin");
            pre_group_rowids.open(gendb_dir + "/indexes/pre/pre_adsh_tag_version_groups.rowids.bin");
            pre_partition_offsets.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/offsets.bin");
            pre_partition_keys.open(gendb_dir + "/column_versions/pre.adsh_tag_version.partitioned_exact_p19/keys.bin");

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
            require_non_empty("sub.adsh", sub_adsh);
            require_non_empty("sub.name", sub_name);
            require_non_empty("sub_adsh_dense_lookup", sub_adsh_dense_lookup);
            require_non_empty("sub_fy_postings.values", sub_fy_postings_values);
            require_non_empty("sub_fy_postings.offsets", sub_fy_postings_offsets);
            require_non_empty("sub_fy_postings.rowids", sub_fy_postings_rowids);
            require_non_empty("pre.plabel", pre_plabel);
            require_non_empty("pre_stmt_postings.values", pre_stmt_postings_values);
            require_non_empty("pre_stmt_postings.offsets", pre_stmt_postings_offsets);
            require_non_empty("pre_stmt_postings.rowids", pre_stmt_postings_rowids);
            require_non_empty("pre_group_keys", pre_group_keys);
            require_non_empty("pre_group_offsets", pre_group_offsets);
            require_non_empty("pre_group_rowids", pre_group_rowids);
            require_non_empty("pre_partition_offsets", pre_partition_offsets);
            require_non_empty("pre_partition_keys", pre_partition_keys);

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_adsh.size() != sub_name.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_group_offsets.size() != pre_group_keys.size() + 1) {
                throw std::runtime_error("pre group offsets/key size mismatch");
            }
            if (pre_partition_offsets.size() != kPartitionCount + 1) {
                throw std::runtime_error("unexpected partition offsets size");
            }
            if (pre_partition_offsets[pre_partition_offsets.size() - 1] != pre_partition_keys.size()) {
                throw std::runtime_error("partition offsets/key size mismatch");
            }
            if (pre_group_keys.size() != pre_partition_keys.size()) {
                throw std::runtime_error("group keys/partition keys size mismatch");
            }

            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_value.prefetch();
            num_uom_postings_values.prefetch();
            num_uom_postings_offsets.prefetch();
            num_uom_postings_rowids.prefetch();
            sub_adsh.prefetch();
            sub_name.prefetch();
            sub_adsh_dense_lookup.prefetch();
            sub_fy_postings_values.prefetch();
            sub_fy_postings_offsets.prefetch();
            sub_fy_postings_rowids.prefetch();
            pre_plabel.prefetch();
            pre_stmt_postings_values.prefetch();
            pre_stmt_postings_offsets.prefetch();
            pre_stmt_postings_rowids.prefetch();
            pre_group_keys.prefetch();
            pre_group_offsets.prefetch();
            pre_group_rowids.prefetch();
            pre_partition_offsets.prefetch();
            pre_partition_keys.prefetch();

            pre_partition_offsets.advise_random();
            pre_partition_keys.advise_random();
        }

        {
            GENDB_PHASE("dim_filter");

            usd_code = num_uom_dict.find_code<uint16_t>("USD", "num_uom");
            is_code = pre_stmt_dict.find_code<uint16_t>("IS", "pre_stmt");

            num_usd_slice = find_posting_slice(num_uom_postings_values, num_uom_postings_offsets, usd_code);
            sub_2023_slice = find_posting_slice(sub_fy_postings_values, sub_fy_postings_offsets, static_cast<int16_t>(2023));
            pre_is_slice = find_posting_slice(pre_stmt_postings_values, pre_stmt_postings_offsets, is_code);

            if (!num_usd_slice.found || !sub_2023_slice.found || !pre_is_slice.found) {
                write_empty_output(results_dir);
                return 0;
            }

            filtered_name_by_adsh.assign(sub_adsh_dense_lookup.size(), kEmpty32);
            const uint32_t* sub_adsh_data = sub_adsh.data;
            const uint32_t* sub_name_data = sub_name.data;
            const uint32_t* sub_2023_rowids = sub_fy_postings_rowids.data;
            for (uint64_t pos = sub_2023_slice.begin; pos < sub_2023_slice.end; ++pos) {
                const uint32_t sub_rowid = sub_2023_rowids[pos];
                filtered_name_by_adsh[sub_adsh_data[sub_rowid]] = sub_name_data[sub_rowid];
            }

            pre_is_bits.assign((pre_plabel.size() + 63) >> 6, 0ull);
            const uint32_t* pre_is_rowids = pre_stmt_postings_rowids.data;
            for (uint64_t pos = pre_is_slice.begin; pos < pre_is_slice.end; ++pos) {
                const uint32_t pre_rowid = pre_is_rowids[pos];
                pre_is_bits[pre_rowid >> 6] |= 1ull << (pre_rowid & 63);
            }
        }

        {
            GENDB_PHASE("build_joins");

            const size_t group_count = pre_group_keys.size();
            partitioned_group_index.allocate(group_count);

            std::vector<uint64_t> write_pos(pre_partition_offsets.data,
                                            pre_partition_offsets.data + pre_partition_offsets.size());
            const TripleKey* group_keys = pre_group_keys.data;
            const TripleKey* partition_keys = pre_partition_keys.data;
            for (uint32_t group_index = 0; group_index < static_cast<uint32_t>(group_count); ++group_index) {
                const TripleKey& key = group_keys[group_index];
                const size_t partition = pre_partition(key.a, key.b, key.c);
                const uint64_t pos = write_pos[partition]++;
                if (pos >= group_count) {
                    throw std::runtime_error("partition permutation out of range");
                }
                const TripleKey& check = partition_keys[pos];
                if (check.a != key.a || check.b != key.b || check.c != key.c) {
                    throw std::runtime_error("partition permutation verification failed");
                }
                partitioned_group_index[pos] = group_index;
            }

            group_is_offsets.resize(group_count + 1);
            const uint64_t* group_offsets = pre_group_offsets.data;
            const uint32_t* group_rowids = pre_group_rowids.data;
            uint64_t total_is_rows = 0;
            for (size_t group_index = 0; group_index < group_count; ++group_index) {
                if (total_is_rows > std::numeric_limits<uint32_t>::max()) {
                    throw std::runtime_error("IS payload exceeds uint32_t offsets capacity");
                }
                group_is_offsets[group_index] = static_cast<uint32_t>(total_is_rows);
                const uint64_t begin = group_offsets[group_index];
                const uint64_t end = group_offsets[group_index + 1];
                for (uint64_t pos = begin; pos < end; ++pos) {
                    total_is_rows += bitset_contains(pre_is_bits, group_rowids[pos]) ? 1ull : 0ull;
                }
            }
            if (total_is_rows > std::numeric_limits<uint32_t>::max()) {
                throw std::runtime_error("IS payload exceeds uint32_t offsets capacity");
            }
            group_is_offsets[group_count] = static_cast<uint32_t>(total_is_rows);
            group_is_plabels.resize(static_cast<size_t>(total_is_rows));

            const uint32_t* pre_plabel_data = pre_plabel.data;
            for (size_t group_index = 0; group_index < group_count; ++group_index) {
                uint32_t write_idx = group_is_offsets[group_index];
                const uint64_t begin = group_offsets[group_index];
                const uint64_t end = group_offsets[group_index + 1];
                for (uint64_t pos = begin; pos < end; ++pos) {
                    const uint32_t pre_rowid = group_rowids[pos];
                    if (bitset_contains(pre_is_bits, pre_rowid)) {
                        group_is_plabels[write_idx++] = pre_plabel_data[pre_rowid];
                    }
                }
            }
        }

        {
            GENDB_PHASE("main_scan");

            const int thread_count = std::max(1, omp_get_max_threads());
            local_aggs.resize(static_cast<size_t>(thread_count));
            for (AggHashMap& agg : local_aggs) {
                agg.reserve(1 << 14);
            }

            const uint64_t drive_rows = num_usd_slice.end - num_usd_slice.begin;
            const uint32_t* __restrict usd_rowids = num_uom_postings_rowids.data;
            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const double* __restrict num_value_data = num_value.data;
            const uint32_t* __restrict filtered_name = filtered_name_by_adsh.data();
            const size_t filtered_name_size = filtered_name_by_adsh.size();
            const uint64_t* __restrict partition_offsets = pre_partition_offsets.data;
            const TripleKey* __restrict partition_keys = pre_partition_keys.data;
            const uint32_t* __restrict partition_to_group = partitioned_group_index.data;
            const uint32_t* __restrict is_offsets = group_is_offsets.data();
            const uint32_t* __restrict is_plabels = group_is_plabels.data();

            const uint64_t morsels = (drive_rows + kMorselRows - 1) / kMorselRows;

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                AggHashMap& local = local_aggs[static_cast<size_t>(tid)];

                std::vector<uint32_t> part_counts(kPartitionCount, 0);
                std::vector<uint32_t> part_slots(kPartitionCount, kEmpty32);
                std::vector<uint32_t> touched;
                std::vector<uint32_t> local_offsets;
                std::vector<uint32_t> local_cursors;
                std::vector<Survivor> survivors;
                std::vector<Survivor> partitioned;

                touched.reserve(4096);
                local_offsets.reserve(4097);
                local_cursors.reserve(4096);
                survivors.reserve(kMorselRows);
                partitioned.reserve(kMorselRows);

#pragma omp for schedule(dynamic, 1)
                for (uint64_t morsel = 0; morsel < morsels; ++morsel) {
                    touched.clear();
                    survivors.clear();

                    const uint64_t lo = num_usd_slice.begin + morsel * kMorselRows;
                    const uint64_t hi = std::min(lo + kMorselRows, num_usd_slice.end);

                    for (uint64_t pos = lo; pos < hi; ++pos) {
                        const uint32_t rowid = usd_rowids[pos];
                        const double value = num_value_data[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint32_t adsh = num_adsh_data[rowid];
                        if (adsh >= filtered_name_size) {
                            continue;
                        }

                        const uint32_t name = filtered_name[adsh];
                        if (name == kEmpty32) {
                            continue;
                        }

                        const uint32_t tag = num_tag_data[rowid];
                        const uint32_t version = num_version_data[rowid];
                        const uint32_t partition = static_cast<uint32_t>(pre_partition(adsh, tag, version));
                        uint32_t slot = part_slots[partition];
                        if (slot == kEmpty32) {
                            slot = static_cast<uint32_t>(touched.size());
                            part_slots[partition] = slot;
                            touched.push_back(partition);
                        }
                        ++part_counts[partition];
                        survivors.push_back(Survivor{adsh, tag, version, name, partition, value});
                    }

                    if (survivors.empty()) {
                        continue;
                    }

                    local_offsets.resize(touched.size() + 1);
                    local_cursors.resize(touched.size());
                    uint32_t total = 0;
                    for (size_t i = 0; i < touched.size(); ++i) {
                        const uint32_t partition = touched[i];
                        local_offsets[i] = total;
                        local_cursors[i] = total;
                        total += part_counts[partition];
                    }
                    local_offsets[touched.size()] = total;
                    partitioned.resize(total);

                    for (const Survivor& survivor : survivors) {
                        const uint32_t slot = part_slots[survivor.partition];
                        partitioned[local_cursors[slot]++] = survivor;
                    }

                    for (size_t i = 0; i < touched.size(); ++i) {
                        const uint32_t partition = touched[i];
                        const uint64_t key_begin = partition_offsets[partition];
                        const uint64_t key_end = partition_offsets[partition + 1];
                        const uint32_t begin = local_offsets[i];
                        const uint32_t end = local_offsets[i + 1];

                        for (uint32_t idx = begin; idx < end; ++idx) {
                            const Survivor& survivor = partitioned[idx];
                            uint32_t matched_group = kEmpty32;
                            for (uint64_t key_idx = key_begin; key_idx < key_end; ++key_idx) {
                                const TripleKey& candidate = partition_keys[key_idx];
                                if (candidate.a == survivor.adsh &&
                                    candidate.b == survivor.tag &&
                                    candidate.c == survivor.version) {
                                    matched_group = partition_to_group[key_idx];
                                    break;
                                }
                            }
                            if (matched_group == kEmpty32) {
                                continue;
                            }

                            const uint32_t payload_begin = is_offsets[matched_group];
                            const uint32_t payload_end = is_offsets[matched_group + 1];
                            for (uint32_t payload_idx = payload_begin; payload_idx < payload_end; ++payload_idx) {
                                local.add(survivor.name, survivor.tag, is_plabels[payload_idx], survivor.value);
                            }
                        }

                        part_counts[partition] = 0;
                        part_slots[partition] = kEmpty32;
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            if (local_aggs.empty()) {
                write_empty_output(results_dir);
                return 0;
            }

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
