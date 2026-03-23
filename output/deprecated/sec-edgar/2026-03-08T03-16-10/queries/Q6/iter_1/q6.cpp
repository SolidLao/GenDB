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
constexpr size_t kBlockSize = 100000;
constexpr size_t kTopK = 200;

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
};

static inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static uint16_t find_dict_code(const DictData& dict, std::string_view needle, const char* dict_name) {
    const size_t n = dict.offsets.size() > 0 ? dict.offsets.size() - 1 : 0;
    for (size_t i = 0; i < n; ++i) {
        if (dict.at(static_cast<uint32_t>(i)) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error(std::string("value not found in dictionary: ") + dict_name);
}

static inline uint64_t hash_join(uint32_t adsh, uint32_t tag, uint32_t version) {
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

struct PreNode {
    uint32_t plabel = 0;
    uint32_t next = kEmpty32;
};

struct PreEntry {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t name = 0;
    uint32_t head = kEmpty32;
    uint32_t start = 0;
    uint32_t count = 0;
    bool occupied = false;
};

class PreHashMap {
public:
    explicit PreHashMap(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        table_.resize(cap);
        mask_ = cap - 1;
    }

    PreEntry& find_or_insert(uint32_t adsh, uint32_t tag, uint32_t version, uint32_t name) {
        size_t slot = hash_join(adsh, tag, version) & mask_;
        for (;;) {
            PreEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.name = name;
                ++size_;
                return entry;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    std::vector<PreEntry>& entries() { return table_; }
    size_t size() const { return size_; }

private:
    std::vector<PreEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

struct ProbeTask {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t name = 0;
    uint32_t payload_start = 0;
    uint32_t payload_count = 0;
};

struct AggEntry {
    uint32_t name = 0;
    uint32_t tag = 0;
    uint32_t plabel = 0;
    uint64_t cnt = 0;
    double total = 0.0;
    bool occupied = false;
};

class AggHashMap {
public:
    AggHashMap() = default;

    void add(uint32_t name, uint32_t tag, uint32_t plabel, double value) {
        if (table_.empty()) {
            reset(1u << 15);
        } else {
            maybe_grow();
        }
        insert_or_add(name, tag, plabel, 1, value);
    }

    void merge_from(const AggHashMap& other) {
        for (const AggEntry& entry : other.table_) {
            if (!entry.occupied) {
                continue;
            }
            if (table_.empty()) {
                reset(1u << 15);
            } else {
                maybe_grow();
            }
            insert_or_add(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
        }
    }

    const std::vector<AggEntry>& entries() const { return table_; }

private:
    void reset(size_t cap) {
        table_.assign(cap, AggEntry{});
        mask_ = cap - 1;
        size_ = 0;
    }

    void maybe_grow() {
        if ((size_ + 1) * 10 < table_.size() * 7) {
            return;
        }
        std::vector<AggEntry> old = std::move(table_);
        table_.assign(old.size() * 2, AggEntry{});
        mask_ = table_.size() - 1;
        size_ = 0;
        for (const AggEntry& entry : old) {
            if (entry.occupied) {
                insert_or_add(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
            }
        }
    }

    void insert_or_add(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double total) {
        size_t slot = hash_group(name, tag, plabel) & mask_;
        for (;;) {
            AggEntry& entry = table_[slot];
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

    std::vector<AggEntry> table_;
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

static inline int compare_num_key(const uint32_t* num_adsh, const uint32_t* num_tag,
                                  const uint32_t* num_version, const uint32_t* rowids,
                                  size_t pos, uint32_t adsh, uint32_t tag, uint32_t version) {
    const uint32_t rid = rowids[pos];
    const uint32_t a = num_adsh[rid];
    if (a < adsh) return -1;
    if (a > adsh) return 1;
    const uint32_t t = num_tag[rid];
    if (t < tag) return -1;
    if (t > tag) return 1;
    const uint32_t v = num_version[rid];
    if (v < version) return -1;
    if (v > version) return 1;
    return 0;
}

static inline size_t lower_bound_num_key(const uint32_t* num_adsh, const uint32_t* num_tag,
                                         const uint32_t* num_version, const uint32_t* rowids,
                                         size_t row_count, uint32_t adsh, uint32_t tag,
                                         uint32_t version) {
    size_t lo = 0;
    size_t hi = row_count;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        const int cmp = compare_num_key(num_adsh, num_tag, num_version, rowids, mid, adsh, tag, version);
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static inline size_t upper_bound_num_key(const uint32_t* num_adsh, const uint32_t* num_tag,
                                         const uint32_t* num_version, const uint32_t* rowids,
                                         size_t row_count, uint32_t adsh, uint32_t tag,
                                         uint32_t version) {
    size_t lo = 0;
    size_t hi = row_count;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        const int cmp = compare_num_key(num_adsh, num_tag, num_version, rowids, mid, adsh, tag, version);
        if (cmp <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
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

        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_plabel;
        gendb::MmapColumn<ZoneRecord<uint16_t>> pre_stmt_zone;

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint32_t> num_rowids;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        std::vector<uint32_t> sub_lookup_2023;
        std::vector<uint32_t> pre_payloads;
        std::vector<ProbeTask> tasks;
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

            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_zone.open(gendb_dir + "/pre/indexes/stmt.zone_map.bin");

            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_rowids.open(gendb_dir + "/num/indexes/adsh_tag_version.rowids.bin");

            if (sub_name.size() != sub_fy.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_stmt.size() != pre_adsh.size() || pre_stmt.size() != pre_tag.size() ||
                pre_stmt.size() != pre_version.size() || pre_stmt.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (num_uom.size() != num_value.size() || num_uom.size() != num_adsh.size() ||
                num_uom.size() != num_tag.size() || num_uom.size() != num_version.size() ||
                num_uom.size() != num_rowids.size()) {
                throw std::runtime_error("num column size mismatch");
            }

            usd_code = find_dict_code(num_uom_dict, "USD", "num.uom");
            is_code = find_dict_code(pre_stmt_dict, "IS", "pre.stmt");

            sub_name.prefetch();
            sub_fy.prefetch();
            sub_adsh_lookup.prefetch();
            pre_stmt.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_plabel.prefetch();
            num_uom.prefetch();
            num_value.prefetch();
            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_rowids.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");
            sub_lookup_2023.assign(sub_adsh_lookup.size(), kEmpty32);
            for (size_t adsh = 0; adsh < sub_adsh_lookup.size(); ++adsh) {
                const uint32_t row = sub_adsh_lookup[adsh];
                if (row != kEmpty32 && sub_fy[row] == 2023) {
                    sub_lookup_2023[adsh] = row;
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            const size_t pre_rows = pre_stmt.size();
            const size_t pre_blocks = (pre_rows + kBlockSize - 1) / kBlockSize;
            if (pre_stmt_zone.size() != pre_blocks) {
                throw std::runtime_error("pre stmt zone map size mismatch");
            }

            PreHashMap pre_map(1u << 19);
            std::vector<PreNode> temp_nodes;
            temp_nodes.reserve(600000);

            for (size_t block = 0; block < pre_blocks; ++block) {
                const ZoneRecord<uint16_t> z = pre_stmt_zone[block];
                if (z.max < is_code || z.min > is_code) {
                    continue;
                }

                const size_t begin = block * kBlockSize;
                const size_t end = std::min(begin + kBlockSize, pre_rows);
                for (size_t row = begin; row < end; ++row) {
                    if (pre_stmt[row] != is_code) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[row];
                    if (adsh >= sub_lookup_2023.size()) {
                        continue;
                    }
                    const uint32_t sub_row = sub_lookup_2023[adsh];
                    if (sub_row == kEmpty32) {
                        continue;
                    }

                    PreEntry& entry = pre_map.find_or_insert(adsh, pre_tag[row], pre_version[row], sub_name[sub_row]);
                    const uint32_t node_idx = static_cast<uint32_t>(temp_nodes.size());
                    temp_nodes.push_back(PreNode{pre_plabel[row], entry.head});
                    entry.head = node_idx;
                    entry.count += 1;
                }
            }

            pre_payloads.resize(temp_nodes.size());
            tasks.reserve(pre_map.size());

            uint32_t payload_pos = 0;
            for (PreEntry& entry : pre_map.entries()) {
                if (!entry.occupied || entry.count == 0) {
                    continue;
                }

                entry.start = payload_pos;
                std::vector<uint32_t> reversed;
                reversed.reserve(entry.count);
                uint32_t node = entry.head;
                while (node != kEmpty32) {
                    reversed.push_back(temp_nodes[node].plabel);
                    node = temp_nodes[node].next;
                }
                for (auto it = reversed.rbegin(); it != reversed.rend(); ++it) {
                    pre_payloads[payload_pos++] = *it;
                }

                tasks.push_back(ProbeTask{
                    entry.adsh,
                    entry.tag,
                    entry.version,
                    entry.name,
                    entry.start,
                    entry.count
                });
            }
            pre_payloads.resize(payload_pos);

            num_rowids.advise_random();
            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_uom.advise_random();
            num_value.advise_random();
        }

        {
            GENDB_PHASE("main_scan");
            if (!tasks.empty()) {
                const int nthreads = std::max(1, omp_get_max_threads());
                std::vector<AggHashMap> locals(static_cast<size_t>(nthreads));

                const uint32_t* num_adsh_ptr = num_adsh.data;
                const uint32_t* num_tag_ptr = num_tag.data;
                const uint32_t* num_version_ptr = num_version.data;
                const uint32_t* num_rowids_ptr = num_rowids.data;
                const uint16_t* num_uom_ptr = num_uom.data;
                const double* num_value_ptr = num_value.data;
                const size_t num_rows = num_rowids.size();

                #pragma omp parallel num_threads(nthreads)
                {
                    AggHashMap& local = locals[static_cast<size_t>(omp_get_thread_num())];

                    #pragma omp for schedule(dynamic, 256)
                    for (size_t i = 0; i < tasks.size(); ++i) {
                        const ProbeTask& task = tasks[i];
                        const size_t begin = lower_bound_num_key(
                            num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, num_rows,
                            task.adsh, task.tag, task.version
                        );
                        if (begin == num_rows ||
                            compare_num_key(
                                num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, begin,
                                task.adsh, task.tag, task.version
                            ) != 0) {
                            continue;
                        }

                        const size_t end = upper_bound_num_key(
                            num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, num_rows,
                            task.adsh, task.tag, task.version
                        );
                        for (size_t pos = begin; pos < end; ++pos) {
                            const uint32_t num_row = num_rowids_ptr[pos];
                            if (num_uom_ptr[num_row] != usd_code) {
                                continue;
                            }

                            const double value = num_value_ptr[num_row];
                            const uint32_t payload_start = task.payload_start;
                            const uint32_t payload_end = payload_start + task.payload_count;
                            for (uint32_t p = payload_start; p < payload_end; ++p) {
                                local.add(task.name, task.tag, pre_payloads[p], value);
                            }
                        }
                    }
                }

                AggHashMap merged;
                for (const AggHashMap& local : locals) {
                    merged.merge_from(local);
                }

                results.reserve(merged.entries().size());
                for (const AggEntry& entry : merged.entries()) {
                    if (!entry.occupied) {
                        continue;
                    }
                    results.push_back(ResultRow{
                        entry.name,
                        is_code,
                        entry.tag,
                        entry.plabel,
                        entry.total,
                        entry.cnt
                    });
                }
            }
        }

        {
            GENDB_PHASE("output");
            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.name != b.name) return a.name < b.name;
                if (a.tag != b.tag) return a.tag < b.tag;
                return a.plabel < b.plabel;
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
