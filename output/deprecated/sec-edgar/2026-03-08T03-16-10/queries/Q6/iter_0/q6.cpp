#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

static inline uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = mix64(a);
    h ^= mix64((uint64_t(b) << 1) | 1ULL);
    h ^= mix64((uint64_t(c) << 1) | 3ULL);
    return mix64(h);
}

static inline uint64_t hash_group(uint32_t name, uint32_t tag, uint32_t plabel) {
    uint64_t h = mix64(name);
    h ^= mix64((uint64_t(tag) << 1) | 1ULL);
    h ^= mix64((uint64_t(plabel) << 1) | 3ULL);
    return mix64(h);
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

struct PreHashEntry {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
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

    PreHashEntry& find_or_insert(uint32_t adsh, uint32_t tag, uint32_t version) {
        size_t slot = hash_triple(adsh, tag, version) & mask_;
        for (;;) {
            PreHashEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.head = kEmpty32;
                entry.start = 0;
                entry.count = 0;
                ++size_;
                return entry;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return entry;
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

    std::vector<PreHashEntry>& entries() {
        return table_;
    }

    size_t size() const {
        return size_;
    }

private:
    std::vector<PreHashEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

struct PreNode {
    uint32_t plabel = 0;
    uint32_t next = kEmpty32;
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
    explicit AggHashMap(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        table_.resize(cap);
        mask_ = cap - 1;
    }

    void add(uint32_t name, uint32_t tag, uint32_t plabel, double value) {
        maybe_grow();
        insert_or_add(name, tag, plabel, 1, value);
    }

    void merge_from(const AggHashMap& other) {
        for (const AggEntry& src : other.table_) {
            if (!src.occupied) {
                continue;
            }
            maybe_grow();
            insert_or_add(src.name, src.tag, src.plabel, src.cnt, src.total);
        }
    }

    const std::vector<AggEntry>& entries() const {
        return table_;
    }

private:
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

        DictData num_uom_dict;
        DictData pre_stmt_dict;
        DictData sub_name_dict;
        DictData tag_dict;
        DictData pre_plabel_dict;

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<ZoneRecord<uint16_t>> num_uom_zone;

        gendb::MmapColumn<int32_t> sub_fy;
        gendb::MmapColumn<uint32_t> sub_name;
        gendb::MmapColumn<uint32_t> sub_adsh_lookup;

        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_plabel;
        gendb::MmapColumn<ZoneRecord<uint16_t>> pre_stmt_zone;

        uint16_t usd_code = 0;
        uint16_t is_code = 0;
        size_t num_rows = 0;
        std::vector<uint32_t> qualifying_num_blocks;
        std::vector<uint32_t> sub_lookup_2023;
        std::vector<uint32_t> pre_payloads;

        {
            GENDB_PHASE("data_loading");
            num_uom_dict.open(gendb_dir + "/num/dict_uom.offsets.bin", gendb_dir + "/num/dict_uom.data.bin");
            pre_stmt_dict.open(gendb_dir + "/pre/dict_stmt.offsets.bin", gendb_dir + "/pre/dict_stmt.data.bin");
            sub_name_dict.open(gendb_dir + "/sub/dict_name.offsets.bin", gendb_dir + "/sub/dict_name.data.bin");
            tag_dict.open(gendb_dir + "/shared/tag.offsets.bin", gendb_dir + "/shared/tag.data.bin");
            pre_plabel_dict.open(gendb_dir + "/pre/dict_plabel.offsets.bin", gendb_dir + "/pre/dict_plabel.data.bin");

            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");

            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_adsh_lookup.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");

            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");
            pre_stmt_zone.open(gendb_dir + "/pre/indexes/stmt.zone_map.bin");

            if (num_uom.size() != num_value.size() || num_uom.size() != num_adsh.size() ||
                num_uom.size() != num_tag.size() || num_uom.size() != num_version.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_fy.size() != sub_name.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_stmt.size() != pre_adsh.size() || pre_stmt.size() != pre_tag.size() ||
                pre_stmt.size() != pre_version.size() || pre_stmt.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }

            usd_code = find_dict_code(num_uom_dict, "USD", "num.uom");
            is_code = find_dict_code(pre_stmt_dict, "IS", "pre.stmt");
            num_rows = num_uom.size();

            num_uom.prefetch();
            num_value.prefetch();
            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            sub_fy.prefetch();
            sub_name.prefetch();
            sub_adsh_lookup.prefetch();
            pre_stmt.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_plabel.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");
            sub_lookup_2023.assign(sub_adsh_lookup.size(), kEmpty32);
            for (size_t adsh_id = 0; adsh_id < sub_adsh_lookup.size(); ++adsh_id) {
                const uint32_t row = sub_adsh_lookup[adsh_id];
                if (row == kEmpty32) {
                    continue;
                }
                if (sub_fy[row] == 2023) {
                    sub_lookup_2023[adsh_id] = row;
                }
            }

            const size_t num_blocks = (num_rows + kBlockSize - 1) / kBlockSize;
            if (num_uom_zone.size() != num_blocks) {
                throw std::runtime_error("num uom zone map size mismatch");
            }
            qualifying_num_blocks.reserve(num_blocks);
            for (size_t block = 0; block < num_blocks; ++block) {
                const ZoneRecord<uint16_t> z = num_uom_zone[block];
                if (z.min <= usd_code && usd_code <= z.max) {
                    qualifying_num_blocks.push_back(static_cast<uint32_t>(block));
                }
            }
        }

        PreHashMap pre_hash(1767049);
        {
            GENDB_PHASE("build_joins");
            std::vector<PreNode> temp_nodes;
            temp_nodes.reserve(1767049);

            const size_t pre_rows = pre_stmt.size();
            const size_t pre_blocks = (pre_rows + kBlockSize - 1) / kBlockSize;
            if (pre_stmt_zone.size() != pre_blocks) {
                throw std::runtime_error("pre stmt zone map size mismatch");
            }

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
                    PreHashEntry& entry = pre_hash.find_or_insert(pre_adsh[row], pre_tag[row], pre_version[row]);
                    const uint32_t node_idx = static_cast<uint32_t>(temp_nodes.size());
                    temp_nodes.push_back(PreNode{pre_plabel[row], entry.head});
                    entry.head = node_idx;
                    entry.count += 1;
                }
            }

            pre_payloads.resize(temp_nodes.size());
            uint32_t payload_pos = 0;
            uint64_t linked_rows = 0;
            uint64_t counted_rows = 0;
            for (PreHashEntry& entry : pre_hash.entries()) {
                if (!entry.occupied || entry.count == 0) {
                    continue;
                }
                counted_rows += entry.count;
                const uint32_t head = entry.head;
                const uint32_t start = payload_pos;
                std::vector<uint32_t> reverse_payload;
                reverse_payload.reserve(entry.count);
                uint32_t node = head;
                while (node != kEmpty32) {
                    reverse_payload.push_back(temp_nodes[node].plabel);
                    node = temp_nodes[node].next;
                }
                linked_rows += reverse_payload.size();
                for (auto it = reverse_payload.rbegin(); it != reverse_payload.rend(); ++it) {
                    pre_payloads[payload_pos++] = *it;
                }
                entry.start = start;
            }
            if (payload_pos != pre_payloads.size()) {
                throw std::runtime_error(
                    "pre payload size mismatch: payload_pos=" + std::to_string(payload_pos) +
                    " payload_size=" + std::to_string(pre_payloads.size()) +
                    " counted_rows=" + std::to_string(counted_rows) +
                    " linked_rows=" + std::to_string(linked_rows)
                );
            }
        }

        std::vector<ResultRow> results;
        {
            GENDB_PHASE("main_scan");
            if (!qualifying_num_blocks.empty() && pre_hash.size() > 0) {
                const int nthreads = std::max(1, omp_get_max_threads());
                std::vector<AggHashMap> thread_aggs;
                thread_aggs.reserve(nthreads);
                for (int i = 0; i < nthreads; ++i) {
                    thread_aggs.emplace_back(1u << 18);
                }

                #pragma omp parallel num_threads(nthreads)
                {
                    AggHashMap& local = thread_aggs[omp_get_thread_num()];

                    #pragma omp for schedule(dynamic, 1)
                    for (size_t block_idx = 0; block_idx < qualifying_num_blocks.size(); ++block_idx) {
                        const size_t block = qualifying_num_blocks[block_idx];
                        const size_t begin = block * kBlockSize;
                        const size_t end = std::min(begin + kBlockSize, num_rows);

                        for (size_t row = begin; row < end; ++row) {
                            if (num_uom[row] != usd_code) {
                                continue;
                            }

                            const uint32_t adsh = num_adsh[row];
                            if (adsh >= sub_lookup_2023.size()) {
                                continue;
                            }
                            const uint32_t sub_row = sub_lookup_2023[adsh];
                            if (sub_row == kEmpty32) {
                                continue;
                            }

                            const PreHashEntry* pre_entry =
                                pre_hash.find(adsh, num_tag[row], num_version[row]);
                            if (!pre_entry) {
                                continue;
                            }

                            const uint32_t name_id = sub_name[sub_row];
                            const double value = num_value[row];
                            const uint32_t start = pre_entry->start;
                            const uint32_t count = pre_entry->count;
                            for (uint32_t j = 0; j < count; ++j) {
                                local.add(name_id, num_tag[row], pre_payloads[start + j], value);
                            }
                        }
                    }
                }

                AggHashMap merged(1u << 20);
                for (const AggHashMap& local : thread_aggs) {
                    merged.merge_from(local);
                }

                const uint32_t stmt_id = static_cast<uint32_t>(is_code);
                for (const AggEntry& entry : merged.entries()) {
                    if (!entry.occupied) {
                        continue;
                    }
                    results.push_back(ResultRow{
                        entry.name,
                        stmt_id,
                        entry.tag,
                        entry.plabel,
                        entry.total,
                        entry.cnt,
                    });
                }
            }
        }

        {
            GENDB_PHASE("output");
            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) {
                    return a.total_value > b.total_value;
                }
                if (a.name != b.name) {
                    return a.name < b.name;
                }
                if (a.tag != b.tag) {
                    return a.tag < b.tag;
                }
                return a.plabel < b.plabel;
            };
            if (results.size() > kTopK) {
                std::partial_sort(results.begin(), results.begin() + kTopK, results.end(), cmp);
                results.resize(kTopK);
            } else {
                std::sort(results.begin(), results.end(), cmp);
            }

            fs::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q6.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
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
