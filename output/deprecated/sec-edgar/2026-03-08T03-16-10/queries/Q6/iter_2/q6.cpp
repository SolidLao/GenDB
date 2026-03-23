#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr size_t kTopK = 200;
constexpr size_t kPartitionBits = 5;
constexpr size_t kPartitionCount = size_t{1} << kPartitionBits;

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct DictData {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    void open(const std::string& offsets_path, const std::string& data_path) {
        offsets.open(offsets_path);
        data.open(data_path);
    }

    std::string_view at(uint32_t code) const {
        if (code + 1 >= offsets.size()) {
            throw std::runtime_error("dictionary code out of range");
        }
        return std::string_view(
            data.data + offsets[code],
            static_cast<size_t>(offsets[code + 1] - offsets[code])
        );
    }

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
    }
};

static uint16_t find_dict_code(const DictData& dict, std::string_view needle, const char* dict_name) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(static_cast<uint32_t>(i)) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error(std::string("value not found in dictionary: ") + dict_name);
}

static inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static inline uint64_t hash_triple(uint32_t adsh, uint32_t tag, uint32_t version) {
    uint64_t h = mix64(adsh);
    h ^= mix64((uint64_t(tag) << 1) | 1ULL);
    h ^= mix64((uint64_t(version) << 1) | 3ULL);
    return mix64(h);
}

static inline uint64_t hash_group(uint32_t name, uint32_t tag, uint32_t plabel) {
    uint64_t h = mix64(name);
    h ^= mix64((uint64_t(tag) << 1) | 1ULL);
    h ^= mix64((uint64_t(plabel) << 1) | 3ULL);
    return mix64(h);
}

struct NumAggEntry {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t name = 0;
    uint64_t cnt = 0;
    double total = 0.0;
    bool occupied = false;
};

class NumAggHashMap {
public:
    NumAggHashMap() = default;

    explicit NumAggHashMap(size_t expected) {
        reset(expected);
    }

    void reserve(size_t expected) {
        if (table_.empty()) {
            reset(expected);
        }
    }

    void add(uint32_t adsh, uint32_t tag, uint32_t version, uint32_t name, double value) {
        if (table_.empty()) {
            reset(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            NumAggEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.name = name;
                entry.cnt = 1;
                entry.total = value;
                ++size_;
                return;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                entry.cnt += 1;
                entry.total += value;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    void merge_from(const NumAggHashMap& other) {
        if (other.size_ == 0) {
            return;
        }
        reserve(other.size_ * 2);
        for (const NumAggEntry& src : other.table_) {
            if (!src.occupied) {
                continue;
            }
            add_merged(src.adsh, src.tag, src.version, src.name, src.cnt, src.total);
        }
    }

    const NumAggEntry* find(uint32_t adsh, uint32_t tag, uint32_t version) const {
        if (table_.empty()) {
            return nullptr;
        }
        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            const NumAggEntry& entry = table_[slot];
            if (!entry.occupied) {
                return nullptr;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return &entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    size_t size() const {
        return size_;
    }

private:
    void reset(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        table_.assign(cap, NumAggEntry{});
        mask_ = cap - 1;
        size_ = 0;
    }

    void rehash(size_t new_cap) {
        std::vector<NumAggEntry> old = std::move(table_);
        table_.assign(new_cap, NumAggEntry{});
        mask_ = new_cap - 1;
        size_ = 0;
        for (const NumAggEntry& entry : old) {
            if (!entry.occupied) {
                continue;
            }
            add_merged(entry.adsh, entry.tag, entry.version, entry.name, entry.cnt, entry.total);
        }
    }

    void add_merged(uint32_t adsh, uint32_t tag, uint32_t version, uint32_t name, uint64_t cnt, double total) {
        if (table_.empty()) {
            reset(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            NumAggEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.name = name;
                entry.cnt = cnt;
                entry.total = total;
                ++size_;
                return;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                entry.cnt += cnt;
                entry.total += total;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    std::vector<NumAggEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

struct FinalAggEntry {
    uint32_t name = 0;
    uint32_t tag = 0;
    uint32_t plabel = 0;
    uint64_t cnt = 0;
    double total = 0.0;
    bool occupied = false;
};

class FinalAggHashMap {
public:
    FinalAggHashMap() = default;

    explicit FinalAggHashMap(size_t expected) {
        reset(expected);
    }

    void add(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double total) {
        if (table_.empty()) {
            reset(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = hash_group(name, tag, plabel) & mask_;
        for (;;) {
            FinalAggEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.name = name;
                entry.tag = tag;
                entry.plabel = plabel;
                entry.cnt = cnt;
                entry.total = total;
                ++size_;
                return;
            }
            if (entry.name == name && entry.tag == tag && entry.plabel == plabel) {
                entry.cnt += cnt;
                entry.total += total;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    void merge_from(const FinalAggHashMap& other) {
        if (other.size_ == 0) {
            return;
        }
        for (const FinalAggEntry& entry : other.table_) {
            if (entry.occupied) {
                add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
            }
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (const FinalAggEntry& entry : table_) {
            if (!entry.occupied) {
                continue;
            }
            fn(entry.name, entry.tag, entry.plabel, entry.total, entry.cnt);
        }
    }

private:
    void reset(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        table_.assign(cap, FinalAggEntry{});
        mask_ = cap - 1;
        size_ = 0;
    }

    void rehash(size_t new_cap) {
        std::vector<FinalAggEntry> old = std::move(table_);
        table_.assign(new_cap, FinalAggEntry{});
        mask_ = new_cap - 1;
        size_ = 0;
        for (const FinalAggEntry& entry : old) {
            if (!entry.occupied) {
                continue;
            }
            add_merged(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
        }
    }

    void add_merged(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double total) {
        if (table_.empty()) {
            reset(16);
        } else if ((size_ + 1) * 10 >= table_.size() * 7) {
            rehash(table_.size() * 2);
        }

        size_t slot = hash_group(name, tag, plabel) & mask_;
        for (;;) {
            FinalAggEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.name = name;
                entry.tag = tag;
                entry.plabel = plabel;
                entry.cnt = cnt;
                entry.total = total;
                ++size_;
                return;
            }
            if (entry.name == name && entry.tag == tag && entry.plabel == plabel) {
                entry.cnt += cnt;
                entry.total += total;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    std::vector<FinalAggEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

struct ResultRow {
    uint32_t name = 0;
    uint32_t stmt = 0;
    uint32_t tag = 0;
    uint32_t plabel = 0;
    double total_value = 0.0;
    uint64_t cnt = 0;
};

static inline uint32_t triple_partition(uint32_t adsh, uint32_t tag, uint32_t version) {
    return static_cast<uint32_t>(hash_triple(adsh, tag, version) & (kPartitionCount - 1));
}

static void write_csv_field(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char c : value) {
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
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

}  // namespace

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");

    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        fs::create_directories(results_dir);

        DictData num_uom_dict;
        DictData pre_stmt_dict;
        DictData sub_name_dict;
        DictData tag_dict;
        DictData pre_plabel_dict;

        gendb::MmapColumn<uint32_t> sub_name;
        gendb::MmapColumn<int32_t> sub_fy;
        gendb::MmapColumn<uint32_t> sub_adsh_lookup;

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<ZoneRecord<uint16_t>> num_uom_zone;

        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_plabel;
        gendb::MmapColumn<ZoneRecord<uint16_t>> pre_stmt_zone;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        std::vector<uint32_t> adsh_to_name_2023;
        std::vector<NumAggHashMap> partition_maps;
        std::vector<ResultRow> results;

        {
            GENDB_PHASE("data_loading");
            num_uom_dict.open(gendb_dir + "/num/dict_uom.offsets.bin", gendb_dir + "/num/dict_uom.data.bin");
            pre_stmt_dict.open(gendb_dir + "/pre/dict_stmt.offsets.bin", gendb_dir + "/pre/dict_stmt.data.bin");
            sub_name_dict.open(gendb_dir + "/sub/dict_name.offsets.bin", gendb_dir + "/sub/dict_name.data.bin");
            tag_dict.open(gendb_dir + "/shared/tag.offsets.bin", gendb_dir + "/shared/tag.data.bin");
            pre_plabel_dict.open(gendb_dir + "/pre/dict_plabel.offsets.bin", gendb_dir + "/pre/dict_plabel.data.bin");

            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_adsh_lookup.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");

            num_uom.open(gendb_dir + "/num/uom.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            num_uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");

            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_zone.open(gendb_dir + "/pre/indexes/stmt.zone_map.bin");

            if (sub_name.size() != sub_fy.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (num_uom.size() != num_adsh.size() || num_uom.size() != num_tag.size() ||
                num_uom.size() != num_version.size() || num_uom.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (pre_stmt.size() != pre_adsh.size() || pre_stmt.size() != pre_tag.size() ||
                pre_stmt.size() != pre_version.size() || pre_stmt.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (num_uom_zone.size() != (num_uom.size() + kBlockSize - 1) / kBlockSize) {
                throw std::runtime_error("num uom zone map size mismatch");
            }
            if (pre_stmt_zone.size() != (pre_stmt.size() + kBlockSize - 1) / kBlockSize) {
                throw std::runtime_error("pre stmt zone map size mismatch");
            }

            usd_code = find_dict_code(num_uom_dict, "USD", "num.uom");
            is_code = find_dict_code(pre_stmt_dict, "IS", "pre.stmt");

            sub_name.prefetch();
            sub_fy.prefetch();
            sub_adsh_lookup.prefetch();
            num_uom.prefetch();
            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_value.prefetch();
            pre_stmt.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_plabel.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");
            adsh_to_name_2023.assign(sub_adsh_lookup.size(), kEmpty32);
            for (size_t adsh = 0; adsh < sub_adsh_lookup.size(); ++adsh) {
                const uint32_t sub_row = sub_adsh_lookup[adsh];
                if (sub_row != kEmpty32 && sub_fy[sub_row] == 2023) {
                    adsh_to_name_2023[adsh] = sub_name[sub_row];
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::vector<NumAggHashMap>> local_maps(
                static_cast<size_t>(nthreads),
                std::vector<NumAggHashMap>(kPartitionCount)
            );

            #pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                std::vector<NumAggHashMap>& local = local_maps[static_cast<size_t>(tid)];

                #pragma omp for schedule(static)
                for (size_t block = 0; block < num_uom_zone.size(); ++block) {
                    const ZoneRecord<uint16_t> zone = num_uom_zone[block];
                    if (zone.max < usd_code || zone.min > usd_code) {
                        continue;
                    }

                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, num_uom.size());
                    for (size_t row = begin; row < end; ++row) {
                        if (num_uom[row] != usd_code) {
                            continue;
                        }
                        const uint32_t adsh = num_adsh[row];
                        if (adsh >= adsh_to_name_2023.size()) {
                            continue;
                        }
                        const uint32_t name = adsh_to_name_2023[adsh];
                        if (name == kEmpty32) {
                            continue;
                        }
                        const uint32_t tag = num_tag[row];
                        const uint32_t version = num_version[row];
                        const uint32_t part = triple_partition(adsh, tag, version);
                        local[part].add(adsh, tag, version, name, num_value[row]);
                    }
                }
            }

            partition_maps.assign(kPartitionCount, NumAggHashMap{});
            for (size_t part = 0; part < kPartitionCount; ++part) {
                size_t expected = 0;
                for (int tid = 0; tid < nthreads; ++tid) {
                    expected += local_maps[static_cast<size_t>(tid)][part].size();
                }
                if (expected != 0) {
                    partition_maps[part].reserve(expected);
                }
                for (int tid = 0; tid < nthreads; ++tid) {
                    partition_maps[part].merge_from(local_maps[static_cast<size_t>(tid)][part]);
                }
            }
        }

        {
            GENDB_PHASE("main_scan");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<FinalAggHashMap> locals(static_cast<size_t>(nthreads), FinalAggHashMap(1u << 15));

            #pragma omp parallel num_threads(nthreads)
            {
                FinalAggHashMap& local = locals[static_cast<size_t>(omp_get_thread_num())];

                #pragma omp for schedule(static)
                for (size_t block = 0; block < pre_stmt_zone.size(); ++block) {
                    const ZoneRecord<uint16_t> zone = pre_stmt_zone[block];
                    if (zone.max < is_code || zone.min > is_code) {
                        continue;
                    }

                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, pre_stmt.size());
                    for (size_t row = begin; row < end; ++row) {
                        if (pre_stmt[row] != is_code) {
                            continue;
                        }

                        const uint32_t adsh = pre_adsh[row];
                        if (adsh >= adsh_to_name_2023.size() || adsh_to_name_2023[adsh] == kEmpty32) {
                            continue;
                        }

                        const uint32_t tag = pre_tag[row];
                        const uint32_t version = pre_version[row];
                        const uint32_t part = triple_partition(adsh, tag, version);
                        const NumAggEntry* match = partition_maps[part].find(adsh, tag, version);
                        if (match == nullptr) {
                            continue;
                        }
                        local.add(match->name, tag, pre_plabel[row], match->cnt, match->total);
                    }
                }
            }

            FinalAggHashMap merged(1u << 16);
            for (const FinalAggHashMap& local : locals) {
                merged.merge_from(local);
            }

            results.reserve(20000);
            merged.for_each([&](uint32_t name, uint32_t tag, uint32_t plabel, double total, uint64_t cnt) {
                results.push_back(ResultRow{
                    name,
                    is_code,
                    tag,
                    plabel,
                    total,
                    cnt
                });
            });
        }

        {
            GENDB_PHASE("output");
            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.name != b.name) return a.name < b.name;
                if (a.tag != b.tag) return a.tag < b.tag;
                if (a.plabel != b.plabel) return a.plabel < b.plabel;
                return a.cnt < b.cnt;
            };

            if (results.size() > kTopK) {
                std::partial_sort(results.begin(), results.begin() + kTopK, results.end(), cmp);
                results.resize(kTopK);
            } else {
                std::sort(results.begin(), results.end(), cmp);
            }

            const std::string out_path = results_dir + "/Q6.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                std::perror(out_path.c_str());
                return 1;
            }

            std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const ResultRow& row : results) {
                write_csv_field(out, sub_name_dict.at(row.name));
                std::fputc(',', out);
                write_csv_field(out, pre_stmt_dict.at(row.stmt));
                std::fputc(',', out);
                write_csv_field(out, tag_dict.at(row.tag));
                std::fputc(',', out);
                write_csv_field(out, pre_plabel_dict.at(row.plabel));
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
