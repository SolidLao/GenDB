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

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
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

static uint16_t find_dict_code(const DictData& dict, std::string_view needle, const char* dict_name) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(static_cast<uint32_t>(i)) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error(std::string("value not found in dictionary: ") + dict_name);
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

struct AdshTask {
    uint32_t adsh = 0;
    uint32_t name = 0;
    uint64_t run_begin = 0;
    uint64_t run_end = 0;
};

struct OverflowNode {
    uint32_t plabel = 0;
    uint32_t count = 0;
    uint32_t next = kEmpty32;
};

struct PreHashEntry {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t inline_plabel = 0;
    uint32_t inline_count = 0;
    uint32_t overflow_head = kEmpty32;
    bool occupied = false;
};

class PreHashMap {
public:
    explicit PreHashMap(size_t expected) { reset(expected); }

    void add(uint32_t adsh, uint32_t tag, uint32_t version, uint32_t plabel) {
        maybe_grow();
        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            PreHashEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.inline_plabel = plabel;
                entry.inline_count = 1;
                entry.overflow_head = kEmpty32;
                ++size_;
                return;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                if (entry.inline_plabel == plabel) {
                    ++entry.inline_count;
                    return;
                }
                uint32_t node_idx = entry.overflow_head;
                while (node_idx != kEmpty32) {
                    OverflowNode& node = overflow_[node_idx];
                    if (node.plabel == plabel) {
                        ++node.count;
                        return;
                    }
                    node_idx = node.next;
                }
                overflow_.push_back(OverflowNode{plabel, 1, entry.overflow_head});
                entry.overflow_head = static_cast<uint32_t>(overflow_.size() - 1);
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const PreHashEntry* find(uint32_t adsh, uint32_t tag, uint32_t version) const {
        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            const PreHashEntry& entry = table_[slot];
            if (!entry.occupied) {
                return nullptr;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return &entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const OverflowNode* overflow_data() const {
        return overflow_.data();
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
        table_.assign(cap, PreHashEntry{});
        mask_ = cap - 1;
        size_ = 0;
        overflow_.clear();
        overflow_.reserve(expected / 4);
    }

    void maybe_grow() {
        if ((size_ + 1) * 10 < table_.size() * 7) {
            return;
        }
        std::vector<PreHashEntry> old = std::move(table_);
        table_.assign(old.size() * 2, PreHashEntry{});
        mask_ = table_.size() - 1;
        size_ = 0;
        for (const PreHashEntry& entry : old) {
            if (!entry.occupied) {
                continue;
            }
            size_t slot = hash_triple(entry.adsh, entry.tag, entry.version) & mask_;
            while (table_[slot].occupied) {
                slot = (slot + 1) & mask_;
            }
            table_[slot] = entry;
            ++size_;
        }
    }

    std::vector<PreHashEntry> table_;
    std::vector<OverflowNode> overflow_;
    size_t mask_ = 0;
    size_t size_ = 0;
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
    explicit AggHashMap(size_t initial_capacity = 1u << 15) { reset(initial_capacity); }

    void add_bulk(uint32_t name, uint32_t tag, uint32_t plabel, uint64_t cnt, double total) {
        if (cnt == 0) {
            return;
        }
        maybe_grow();
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

    void merge_from(const AggHashMap& other) {
        for (const AggEntry& entry : other.table_) {
            if (entry.occupied) {
                add_bulk(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
            }
        }
    }

    const std::vector<AggEntry>& entries() const {
        return table_;
    }

private:
    void reset(size_t cap) {
        size_t actual = 16;
        while (actual < cap) {
            actual <<= 1;
        }
        table_.assign(actual, AggEntry{});
        mask_ = actual - 1;
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
                add_bulk(entry.name, entry.tag, entry.plabel, entry.cnt, entry.total);
            }
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

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_plabel;
        gendb::MmapColumn<ZoneRecord<uint16_t>> pre_stmt_zone_map;

        gendb::MmapColumn<uint32_t> num_rowids;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint32_t> num_run_tag;
        gendb::MmapColumn<uint32_t> num_run_version;
        gendb::MmapColumn<uint64_t> num_run_offsets;
        gendb::MmapColumn<uint64_t> num_run_adsh_offsets;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        std::vector<AdshTask> tasks;
        std::vector<uint32_t> name_by_adsh;
        std::vector<ResultRow> results;
        PreHashMap pre_hash(1u << 20);

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

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_zone_map.open(gendb_dir + "/pre/indexes/stmt.zone_map.bin");

            num_rowids.open(gendb_dir + "/num/indexes/adsh_tag_version.rowids.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            num_run_tag.open(gendb_dir + "/column_versions/num.adsh_tag_version.runs/tag.bin");
            num_run_version.open(gendb_dir + "/column_versions/num.adsh_tag_version.runs/version.bin");
            num_run_offsets.open(gendb_dir + "/column_versions/num.adsh_tag_version.runs/offsets.bin");
            num_run_adsh_offsets.open(gendb_dir + "/column_versions/num.adsh_tag_version.runs/adsh_offsets.bin");

            if (sub_name.size() != sub_fy.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size() || pre_adsh.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (num_uom.size() != num_value.size() || num_uom.size() != num_rowids.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (num_run_tag.size() != num_run_version.size()) {
                throw std::runtime_error("num run key size mismatch");
            }
            if (num_run_offsets.size() != num_run_tag.size() + 1) {
                throw std::runtime_error("num run offsets size mismatch");
            }

            usd_code = find_dict_code(num_uom_dict, "USD", "num.uom");
            is_code = find_dict_code(pre_stmt_dict, "IS", "pre.stmt");

            gendb::mmap_prefetch_all(
                sub_name, sub_fy, sub_adsh_lookup,
                pre_adsh, pre_tag, pre_version, pre_stmt, pre_plabel, pre_stmt_zone_map,
                num_rowids, num_uom, num_value, num_run_tag, num_run_version,
                num_run_offsets, num_run_adsh_offsets
            );
        }

        {
            GENDB_PHASE("dim_filter");
            const size_t max_adsh = sub_adsh_lookup.size();
            name_by_adsh.assign(max_adsh, kEmpty32);
            tasks.reserve(max_adsh / 3);
            for (uint32_t adsh = 0; adsh < max_adsh; ++adsh) {
                const uint32_t sub_row = sub_adsh_lookup[adsh];
                if (sub_row == kEmpty32 || sub_fy[sub_row] != 2023) {
                    continue;
                }
                const uint32_t name = sub_name[sub_row];
                name_by_adsh[adsh] = name;
                if (adsh + 1 >= num_run_adsh_offsets.size()) {
                    continue;
                }
                const uint64_t run_begin = num_run_adsh_offsets[adsh];
                const uint64_t run_end = num_run_adsh_offsets[adsh + 1];
                if (run_begin == run_end) {
                    continue;
                }
                tasks.push_back(AdshTask{adsh, name, run_begin, run_end});
            }
        }

        {
            GENDB_PHASE("build_joins");
            const size_t n_pre = pre_adsh.size();
            const size_t n_blocks = pre_stmt_zone_map.size();
            const uint32_t* name_by_adsh_ptr = name_by_adsh.data();
            const size_t name_by_adsh_size = name_by_adsh.size();
            for (size_t block = 0; block < n_blocks; ++block) {
                const ZoneRecord<uint16_t>& zone = pre_stmt_zone_map[block];
                if (is_code < zone.min || is_code > zone.max) {
                    continue;
                }
                const size_t row_begin = block * kBlockSize;
                const size_t row_end = std::min(row_begin + kBlockSize, n_pre);
                for (size_t row = row_begin; row < row_end; ++row) {
                    if (pre_stmt[row] != is_code) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[row];
                    if (adsh >= name_by_adsh_size) {
                        continue;
                    }
                    if (name_by_adsh_ptr[adsh] == kEmpty32) {
                        continue;
                    }
                    pre_hash.add(adsh, pre_tag[row], pre_version[row], pre_plabel[row]);
                }
            }

            num_rowids.advise_random();
            num_uom.advise_random();
            num_value.advise_random();
        }

        {
            GENDB_PHASE("main_scan");
            if (!tasks.empty() && pre_hash.size() != 0) {
                const int nthreads = std::max(1, omp_get_max_threads());
                std::vector<AggHashMap> locals;
                locals.reserve(static_cast<size_t>(nthreads));
                for (int i = 0; i < nthreads; ++i) {
                    locals.emplace_back(1u << 14);
                }

                const uint32_t* num_run_tag_ptr = num_run_tag.data;
                const uint32_t* num_run_version_ptr = num_run_version.data;
                const uint64_t* num_run_offsets_ptr = num_run_offsets.data;
                const uint32_t* num_rowids_ptr = num_rowids.data;
                const uint16_t* num_uom_ptr = num_uom.data;
                const double* num_value_ptr = num_value.data;
                const OverflowNode* overflow_ptr = pre_hash.overflow_data();

                #pragma omp parallel num_threads(nthreads)
                {
                    AggHashMap& local = locals[static_cast<size_t>(omp_get_thread_num())];

                    #pragma omp for schedule(dynamic, 128)
                    for (size_t task_idx = 0; task_idx < tasks.size(); ++task_idx) {
                        const AdshTask& task = tasks[task_idx];
                        for (uint64_t run_idx = task.run_begin; run_idx < task.run_end; ++run_idx) {
                            const uint32_t tag = num_run_tag_ptr[run_idx];
                            const uint32_t version = num_run_version_ptr[run_idx];
                            const PreHashEntry* entry = pre_hash.find(task.adsh, tag, version);
                            if (!entry) {
                                continue;
                            }

                            uint64_t qualifying_cnt = 0;
                            double qualifying_sum = 0.0;
                            const uint64_t row_begin = num_run_offsets_ptr[run_idx];
                            const uint64_t row_end = num_run_offsets_ptr[run_idx + 1];
                            for (uint64_t pos = row_begin; pos < row_end; ++pos) {
                                const uint32_t rowid = num_rowids_ptr[pos];
                                if (num_uom_ptr[rowid] != usd_code) {
                                    continue;
                                }
                                ++qualifying_cnt;
                                qualifying_sum += num_value_ptr[rowid];
                            }
                            if (qualifying_cnt == 0) {
                                continue;
                            }

                            local.add_bulk(
                                task.name,
                                tag,
                                entry->inline_plabel,
                                qualifying_cnt * static_cast<uint64_t>(entry->inline_count),
                                qualifying_sum * static_cast<double>(entry->inline_count)
                            );
                            uint32_t node_idx = entry->overflow_head;
                            while (node_idx != kEmpty32) {
                                const OverflowNode& node = overflow_ptr[node_idx];
                                local.add_bulk(
                                    task.name,
                                    tag,
                                    node.plabel,
                                    qualifying_cnt * static_cast<uint64_t>(node.count),
                                    qualifying_sum * static_cast<double>(node.count)
                                );
                                node_idx = node.next;
                            }
                        }
                    }
                }

                AggHashMap merged(1u << 18);
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
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Q6 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
