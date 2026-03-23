#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <omp.h>

#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

#pragma pack(push, 1)
struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct TriplePostingEntry {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t start;
    uint32_t count;
};
#pragma pack(pop)

struct PostingIndexView {
    const PostingEntry* entries = nullptr;
    uint64_t entry_count = 0;
    const uint32_t* rowids = nullptr;
    uint64_t rowid_count = 0;
};

struct TriplePostingIndexView {
    const TriplePostingEntry* entries = nullptr;
    uint64_t entry_count = 0;
    const uint32_t* rowids = nullptr;
    uint64_t rowid_count = 0;
};

struct TripleKey {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;

    bool operator==(const TripleKey& o) const {
        return adsh == o.adsh && tag == o.tag && version == o.version;
    }
};

struct TripleKeyHash {
    size_t operator()(const TripleKey& k) const noexcept {
        uint64_t h = static_cast<uint64_t>(k.adsh) * 0x9E3779B97F4A7C15ULL;
        h ^= (static_cast<uint64_t>(k.tag) + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2));
        h ^= (static_cast<uint64_t>(k.version) + 0x94D049BB133111EBULL + (h << 6) + (h >> 2));
        return static_cast<size_t>(h);
    }
};

struct AggKey {
    uint32_t name;
    uint32_t tag;
    uint32_t plabel;

    bool operator==(const AggKey& o) const {
        return name == o.name && tag == o.tag && plabel == o.plabel;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const noexcept {
        uint64_t h = static_cast<uint64_t>(k.name) * 0x9E3779B97F4A7C15ULL;
        h ^= (static_cast<uint64_t>(k.tag) + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2));
        h ^= (static_cast<uint64_t>(k.plabel) + 0x94D049BB133111EBULL + (h << 6) + (h >> 2));
        return static_cast<size_t>(h);
    }
};

struct AggVal {
    double sum = 0.0;
    uint64_t cnt = 0;
};

struct ResultRow {
    uint32_t name;
    uint32_t tag;
    uint32_t plabel;
    double total_value;
    uint64_t cnt;
};

std::vector<std::string> load_dict(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open dict: " + path);
    }

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) {
        throw std::runtime_error("cannot read dict header: " + path);
    }

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) {
            throw std::runtime_error("cannot read dict len: " + path);
        }
        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) {
                throw std::runtime_error("cannot read dict bytes: " + path);
            }
        }
        out.push_back(std::move(s));
    }
    return out;
}

uint32_t find_code(const std::vector<std::string>& dict, std::string_view v) {
    for (uint32_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == v) return i;
    }
    throw std::runtime_error("dictionary value not found: " + std::string(v));
}

PostingIndexView parse_posting_index(const gendb::MmapColumn<uint8_t>& blob, const std::string& path) {
    PostingIndexView out;
    if (blob.file_size < sizeof(uint64_t) * 2) {
        throw std::runtime_error("posting index too small: " + path);
    }

    const uint8_t* p = blob.data;
    std::memcpy(&out.entry_count, p, sizeof(uint64_t));
    std::memcpy(&out.rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

    const size_t entries_bytes = static_cast<size_t>(out.entry_count) * sizeof(PostingEntry);
    const size_t rowids_bytes = static_cast<size_t>(out.rowid_count) * sizeof(uint32_t);
    const size_t needed = sizeof(uint64_t) * 2 + entries_bytes + rowids_bytes;
    if (blob.file_size < needed) {
        throw std::runtime_error("posting index truncated: " + path);
    }

    out.entries = reinterpret_cast<const PostingEntry*>(p + sizeof(uint64_t) * 2);
    out.rowids = reinterpret_cast<const uint32_t*>(p + sizeof(uint64_t) * 2 + entries_bytes);
    return out;
}

TriplePostingIndexView parse_triple_posting_index(const gendb::MmapColumn<uint8_t>& blob,
                                                  const std::string& path) {
    TriplePostingIndexView out;
    if (blob.file_size < sizeof(uint64_t) * 2) {
        throw std::runtime_error("triple posting index too small: " + path);
    }

    const uint8_t* p = blob.data;
    std::memcpy(&out.entry_count, p, sizeof(uint64_t));
    std::memcpy(&out.rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

    const size_t entries_bytes = static_cast<size_t>(out.entry_count) * sizeof(TriplePostingEntry);
    const size_t rowids_bytes = static_cast<size_t>(out.rowid_count) * sizeof(uint32_t);
    const size_t needed = sizeof(uint64_t) * 2 + entries_bytes + rowids_bytes;
    if (blob.file_size < needed) {
        throw std::runtime_error("triple posting index truncated: " + path);
    }

    out.entries = reinterpret_cast<const TriplePostingEntry*>(p + sizeof(uint64_t) * 2);
    out.rowids = reinterpret_cast<const uint32_t*>(p + sizeof(uint64_t) * 2 + entries_bytes);
    return out;
}

const PostingEntry* find_posting_entry(const PostingEntry* entries, uint64_t n, uint32_t key) {
    uint64_t lo = 0;
    uint64_t hi = n;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const uint32_t mk = entries[mid].key;
        if (mk < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo < n && entries[lo].key == key) {
        return &entries[lo];
    }
    return nullptr;
}

bool needs_csv_quote(std::string_view s) {
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') return true;
    }
    return false;
}

void write_csv_field(FILE* out, std::string_view s) {
    if (!needs_csv_quote(s)) {
        std::fwrite(s.data(), 1, s.size(), out);
        return;
    }

    std::fputc('"', out);
    for (char c : s) {
        if (c == '"') std::fputc('"', out);
        std::fputc(c, out);
    }
    std::fputc('"', out);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];

        constexpr int32_t kFyTarget = 2023;
        constexpr uint32_t kTopK = 200;
        constexpr size_t kMorselSize = 1U << 15;

        std::vector<std::string> name_dict;
        std::vector<std::string> stmt_dict;
        std::vector<std::string> tag_dict;
        std::vector<std::string> plabel_dict;
        uint32_t usd_code = 0;
        uint32_t is_code = 0;

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_fy;
        gendb::MmapColumn<uint32_t> sub_name;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint8_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_plabel;

        gendb::MmapColumn<uint8_t> num_uom_hash_blob;
        gendb::MmapColumn<uint8_t> sub_fy_zonemap_blob;
        gendb::MmapColumn<uint8_t> pre_stmt_hash_blob;
        gendb::MmapColumn<uint8_t> pre_triple_hash_blob;

        PostingIndexView num_uom_idx;
        PostingIndexView pre_stmt_idx;
        TriplePostingIndexView pre_triple_idx;

        gendb::CompactHashMap<uint32_t, uint32_t> sub_adsh_to_name;
        std::unordered_map<TripleKey, std::vector<uint32_t>, TripleKeyHash> pre_triple_to_plabels;
        std::unordered_map<AggKey, AggVal, AggKeyHash> global_agg;

        const PostingEntry* usd_entry = nullptr;
        const PostingEntry* is_entry = nullptr;
        bool use_num_uom_postings = false;
        bool use_pre_stmt_postings = false;

        uint64_t sub_zm_block_size = 0;
        uint64_t sub_zm_blocks = 0;
        const int32_t* sub_zm_minmax = nullptr;

        int nthreads = 1;

        GENDB_PHASE("total");

        {
            GENDB_PHASE("data_loading");

            const std::string dict_dir = gendb_dir + "/dicts";
            name_dict = load_dict(gendb_dir + "/sub/name.dict");
            stmt_dict = load_dict(dict_dir + "/stmt.dict");
            tag_dict = load_dict(dict_dir + "/tag.dict");
            plabel_dict = load_dict(gendb_dir + "/pre/plabel.dict");
            const std::vector<std::string> uom_dict = load_dict(dict_dir + "/uom.dict");

            usd_code = find_code(uom_dict, "USD");
            is_code = find_code(stmt_dict, "IS");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_uom.size() || num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_adsh.size() != sub_fy.size() || sub_adsh.size() != sub_name.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size() || pre_adsh.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }

            num_uom_hash_blob.open(gendb_dir + "/num/indexes/num_uom_hash.bin");
            sub_fy_zonemap_blob.open(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin");
            pre_stmt_hash_blob.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            pre_triple_hash_blob.open(gendb_dir + "/pre/indexes/pre_adsh_tag_version_hash.bin");

            num_uom_idx = parse_posting_index(num_uom_hash_blob, "num_uom_hash.bin");
            pre_stmt_idx = parse_posting_index(pre_stmt_hash_blob, "pre_stmt_hash.bin");
            pre_triple_idx = parse_triple_posting_index(pre_triple_hash_blob, "pre_adsh_tag_version_hash.bin");
            (void)pre_triple_idx;

            // Some generated SEC-EDGAR index files can be mislabeled.
            // Only use posting index when key domain matches dictionary cardinality.
            if (num_uom_idx.entry_count <= static_cast<uint64_t>(uom_dict.size() + 32)) {
                usd_entry = find_posting_entry(num_uom_idx.entries, num_uom_idx.entry_count, usd_code);
                use_num_uom_postings = (usd_entry != nullptr);
            }
            if (pre_stmt_idx.entry_count <= static_cast<uint64_t>(stmt_dict.size() + 64)) {
                is_entry = find_posting_entry(pre_stmt_idx.entries, pre_stmt_idx.entry_count, is_code);
                use_pre_stmt_postings = (is_entry != nullptr);
            }

            if (sub_fy_zonemap_blob.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("sub_fy_zonemap too small");
            }
            const uint8_t* zm = sub_fy_zonemap_blob.data;
            std::memcpy(&sub_zm_block_size, zm, sizeof(uint64_t));
            std::memcpy(&sub_zm_blocks, zm + sizeof(uint64_t), sizeof(uint64_t));
            const size_t needed = sizeof(uint64_t) * 2 + static_cast<size_t>(sub_zm_blocks) * sizeof(int32_t) * 2;
            if (sub_fy_zonemap_blob.file_size < needed) {
                throw std::runtime_error("sub_fy_zonemap payload truncated");
            }
            sub_zm_minmax = reinterpret_cast<const int32_t*>(zm + sizeof(uint64_t) * 2);

            nthreads = omp_get_max_threads();
            if (nthreads < 1) nthreads = 1;
            if (nthreads > 64) nthreads = 64;

            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_uom.prefetch();
            num_value.prefetch();
            sub_adsh.prefetch();
            sub_fy.prefetch();
            sub_name.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_stmt.prefetch();
            pre_plabel.prefetch();
            num_uom_hash_blob.prefetch();
            pre_stmt_hash_blob.prefetch();
            pre_triple_hash_blob.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");

            const size_t sub_rows = sub_adsh.size();
            const size_t block_size = static_cast<size_t>(sub_zm_block_size == 0 ? 100000 : sub_zm_block_size);
            sub_adsh_to_name.reserve(30000);

            for (uint64_t b = 0; b < sub_zm_blocks; ++b) {
                const int32_t mn = sub_zm_minmax[2 * b];
                const int32_t mx = sub_zm_minmax[2 * b + 1];
                if (kFyTarget < mn || kFyTarget > mx) continue;

                const size_t start = static_cast<size_t>(b) * block_size;
                const size_t end = std::min(start + block_size, sub_rows);
                for (size_t i = start; i < end; ++i) {
                    if (sub_fy[i] != kFyTarget) continue;
                    sub_adsh_to_name[sub_adsh[i]] = sub_name[i];
                }
            }
        }

        {
            GENDB_PHASE("build_joins");

            if (use_pre_stmt_postings) {
                pre_triple_to_plabels.reserve(static_cast<size_t>(is_entry->count / 2 + 1));

                const uint64_t start = is_entry->start;
                const uint64_t end = is_entry->start + is_entry->count;
                if (end > pre_stmt_idx.rowid_count) {
                    throw std::runtime_error("pre_stmt postings out of bounds");
                }

                for (uint64_t p = start; p < end; ++p) {
                    const uint32_t rid = pre_stmt_idx.rowids[p];
                    if (rid >= pre_adsh.size()) continue;
                    if (pre_stmt[rid] != static_cast<uint8_t>(is_code)) continue;

                    const uint32_t adsh = pre_adsh[rid];
                    if (!sub_adsh_to_name.contains(adsh)) continue;

                    TripleKey k{adsh, pre_tag[rid], pre_version[rid]};
                    pre_triple_to_plabels[k].push_back(pre_plabel[rid]);
                }
            } else {
                pre_triple_to_plabels.reserve(400000);
                for (uint32_t rid = 0; rid < pre_adsh.size(); ++rid) {
                    if (pre_stmt[rid] != static_cast<uint8_t>(is_code)) continue;
                    const uint32_t adsh = pre_adsh[rid];
                    if (!sub_adsh_to_name.contains(adsh)) continue;
                    TripleKey k{adsh, pre_tag[rid], pre_version[rid]};
                    pre_triple_to_plabels[k].push_back(pre_plabel[rid]);
                }
            }
        }

        {
            GENDB_PHASE("main_scan");

            const uint32_t* __restrict num_adsh_data = num_adsh.data;
            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const uint16_t* __restrict num_uom_data = num_uom.data;
            const double* __restrict num_value_data = num_value.data;

            num_adsh.advise_random();
            num_tag.advise_random();
            num_version.advise_random();
            num_uom.advise_random();
            num_value.advise_random();

            std::vector<std::unordered_map<AggKey, AggVal, AggKeyHash>> local_aggs(static_cast<size_t>(nthreads));
            const uint64_t drive_rows = use_num_uom_postings ? usd_entry->count : static_cast<uint64_t>(num_adsh.size());
            const size_t expected_local = static_cast<size_t>((drive_rows / static_cast<uint64_t>(nthreads)) / 16 + 1);
            for (int t = 0; t < nthreads; ++t) {
                local_aggs[static_cast<size_t>(t)].reserve(expected_local);
            }

            if (use_num_uom_postings) {
                if (usd_entry->start + usd_entry->count > num_uom_idx.rowid_count) {
                    throw std::runtime_error("num_uom postings out of bounds");
                }

                const uint64_t posting_begin = usd_entry->start;
                const uint64_t posting_end = usd_entry->start + usd_entry->count;
                const uint64_t total = posting_end - posting_begin;
                const uint64_t morsels = (total + kMorselSize - 1) / kMorselSize;

                #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
                for (uint64_t m = 0; m < morsels; ++m) {
                    const uint64_t lo = posting_begin + m * kMorselSize;
                    const uint64_t hi = std::min(lo + static_cast<uint64_t>(kMorselSize), posting_end);
                    auto& local = local_aggs[static_cast<size_t>(omp_get_thread_num())];

                    for (uint64_t p = lo; p < hi; ++p) {
                        const uint32_t rid = num_uom_idx.rowids[p];
                        if (rid >= num_adsh.size()) continue;

                        if (num_uom_data[rid] != static_cast<uint16_t>(usd_code)) continue;
                        const double v = num_value_data[rid];
                        if (std::isnan(v)) continue;

                        const uint32_t adsh = num_adsh_data[rid];
                        const uint32_t* name_code = sub_adsh_to_name.find(adsh);
                        if (!name_code) continue;

                        const TripleKey tk{adsh, num_tag_data[rid], num_version_data[rid]};
                        auto pit = pre_triple_to_plabels.find(tk);
                        if (pit == pre_triple_to_plabels.end()) continue;

                        const uint32_t name = *name_code;
                        const uint32_t tag = num_tag_data[rid];
                        const std::vector<uint32_t>& plabels = pit->second;
                        for (uint32_t plabel : plabels) {
                            const AggKey gk{name, tag, plabel};
                            AggVal& a = local[gk];
                            a.sum += v;
                            a.cnt += 1;
                        }
                    }
                }
            } else {
                const uint64_t total = num_adsh.size();
                const uint64_t morsels = (total + kMorselSize - 1) / kMorselSize;

                #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
                for (uint64_t m = 0; m < morsels; ++m) {
                    const uint64_t lo = m * kMorselSize;
                    const uint64_t hi = std::min(lo + static_cast<uint64_t>(kMorselSize), total);
                    auto& local = local_aggs[static_cast<size_t>(omp_get_thread_num())];

                    for (uint64_t rid = lo; rid < hi; ++rid) {
                        if (num_uom_data[rid] != static_cast<uint16_t>(usd_code)) continue;
                        const double v = num_value_data[rid];
                        if (std::isnan(v)) continue;

                        const uint32_t adsh = num_adsh_data[rid];
                        const uint32_t* name_code = sub_adsh_to_name.find(adsh);
                        if (!name_code) continue;

                        const TripleKey tk{adsh, num_tag_data[rid], num_version_data[rid]};
                        auto pit = pre_triple_to_plabels.find(tk);
                        if (pit == pre_triple_to_plabels.end()) continue;

                        const uint32_t name = *name_code;
                        const uint32_t tag = num_tag_data[rid];
                        const std::vector<uint32_t>& plabels = pit->second;
                        for (uint32_t plabel : plabels) {
                            const AggKey gk{name, tag, plabel};
                            AggVal& a = local[gk];
                            a.sum += v;
                            a.cnt += 1;
                        }
                    }
                }
            }

            size_t reserve_size = 0;
            for (const auto& l : local_aggs) {
                reserve_size += l.size();
            }
            global_agg.reserve(reserve_size + 1);

            for (const auto& l : local_aggs) {
                for (const auto& kv : l) {
                    AggVal& dst = global_agg[kv.first];
                    dst.sum += kv.second.sum;
                    dst.cnt += kv.second.cnt;
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<ResultRow> rows;
            rows.reserve(global_agg.size());
            for (const auto& kv : global_agg) {
                rows.push_back(ResultRow{kv.first.name, kv.first.tag, kv.first.plabel, kv.second.sum, kv.second.cnt});
            }

            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.name != b.name) return a.name < b.name;
                if (a.tag != b.tag) return a.tag < b.tag;
                return a.plabel < b.plabel;
            };

            if (rows.size() > kTopK) {
                std::partial_sort(rows.begin(), rows.begin() + kTopK, rows.end(), cmp);
                rows.resize(kTopK);
            } else {
                std::sort(rows.begin(), rows.end(), cmp);
            }

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q6.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("cannot open output file: " + out_path);
            }

            const std::string stmt_s = (is_code < stmt_dict.size()) ? stmt_dict[is_code] : std::string();
            std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const ResultRow& r : rows) {
                const std::string_view name_s = (r.name < name_dict.size()) ? std::string_view(name_dict[r.name]) : std::string_view();
                const std::string_view tag_s = (r.tag < tag_dict.size()) ? std::string_view(tag_dict[r.tag]) : std::string_view();
                const std::string_view plabel_s = (r.plabel < plabel_dict.size()) ? std::string_view(plabel_dict[r.plabel]) : std::string_view();

                write_csv_field(out, name_s);
                std::fputc(',', out);
                write_csv_field(out, stmt_s);
                std::fputc(',', out);
                write_csv_field(out, tag_s);
                std::fputc(',', out);
                write_csv_field(out, plabel_s);
                std::fprintf(out, ",%.2f,%llu\n", r.total_value, static_cast<unsigned long long>(r.cnt));
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
