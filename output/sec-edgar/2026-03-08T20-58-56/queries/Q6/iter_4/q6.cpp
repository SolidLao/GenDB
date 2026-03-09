#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

struct PostingIndexView {
    const uint8_t* entries = nullptr;
    uint64_t entry_count = 0;
    const uint32_t* rowids = nullptr;
    uint64_t rowid_count = 0;
};

struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
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
        h ^= static_cast<uint64_t>(k.tag) + 0xC2B2AE3D27D4EB4FULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x165667B19E3779F9ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggKey {
    uint32_t name;
    uint8_t stmt;
    uint32_t tag;
    uint32_t plabel;

    bool operator==(const AggKey& o) const {
        return name == o.name && stmt == o.stmt && tag == o.tag && plabel == o.plabel;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const noexcept {
        uint64_t h = static_cast<uint64_t>(k.name) * 0x9E3779B97F4A7C15ULL;
        h ^= static_cast<uint64_t>(k.stmt) + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.tag) + 0x94D049BB133111EBULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.plabel) + 0xD6E8FEB86659FD93ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggVal {
    double sum = 0.0;
    uint64_t cnt = 0;
};

struct ResultRow {
    uint32_t name;
    uint8_t stmt;
    uint32_t tag;
    uint32_t plabel;
    double total_value;
    uint64_t cnt;
};

struct AdshPosting {
    uint64_t start = 0;
    uint32_t count = 0;
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

uint32_t find_code(const std::vector<std::string>& dict, std::string_view value) {
    for (uint32_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == value) {
            return i;
        }
    }
    throw std::runtime_error("dictionary value not found: " + std::string(value));
}

PostingIndexView parse_posting_index(const gendb::MmapColumn<uint8_t>& blob, const std::string& path) {
    PostingIndexView out;
    if (blob.file_size < sizeof(uint64_t) * 2) {
        throw std::runtime_error("posting index too small: " + path);
    }

    std::memcpy(&out.entry_count, blob.data, sizeof(uint64_t));
    std::memcpy(&out.rowid_count, blob.data + sizeof(uint64_t), sizeof(uint64_t));

    constexpr size_t kEntryBytes = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);  // 16
    const size_t entries_bytes = static_cast<size_t>(out.entry_count) * kEntryBytes;
    const size_t rowids_bytes = static_cast<size_t>(out.rowid_count) * sizeof(uint32_t);
    const size_t need = sizeof(uint64_t) * 2 + entries_bytes + rowids_bytes;
    if (blob.file_size < need) {
        throw std::runtime_error("posting index truncated: " + path);
    }

    out.entries = blob.data + sizeof(uint64_t) * 2;
    out.rowids = reinterpret_cast<const uint32_t*>(blob.data + sizeof(uint64_t) * 2 + entries_bytes);
    return out;
}

inline PostingEntry read_posting_entry(const PostingIndexView& idx, uint64_t i) {
    constexpr size_t kEntryBytes = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);  // 16
    const uint8_t* p = idx.entries + i * kEntryBytes;
    PostingEntry e{};
    std::memcpy(&e.key, p + 0, sizeof(uint32_t));
    std::memcpy(&e.start, p + 4, sizeof(uint64_t));
    std::memcpy(&e.count, p + 12, sizeof(uint32_t));
    return e;
}

bool find_posting_span(const PostingIndexView& idx, uint32_t key, uint64_t& out_start, uint32_t& out_count) {
    uint64_t lo = 0;
    uint64_t hi = idx.entry_count;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const PostingEntry e = read_posting_entry(idx, mid);
        if (e.key < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo < idx.entry_count) {
        const PostingEntry e = read_posting_entry(idx, lo);
        if (e.key == key) {
            out_start = e.start;
            out_count = e.count;
            return true;
        }
    }
    return false;
}

bool needs_csv_quote(std::string_view s) {
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            return true;
        }
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
        if (c == '"') {
            std::fputc('"', out);
        }
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
        constexpr uint32_t kMissing = std::numeric_limits<uint32_t>::max();

        std::vector<std::string> name_dict;
        std::vector<std::string> stmt_dict;
        std::vector<std::string> tag_dict;
        std::vector<std::string> plabel_dict;

        uint16_t usd_code = 0;
        uint8_t is_code = 0;

        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_fy;
        gendb::MmapColumn<uint32_t> sub_name;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint8_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_plabel;

        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint8_t> sub_fy_zonemap_blob;
        gendb::MmapColumn<uint8_t> sub_adsh_pk_hash_blob;
        gendb::MmapColumn<uint8_t> pre_stmt_hash_blob;
        gendb::MmapColumn<uint8_t> num_adsh_fk_hash_blob;

        PostingIndexView pre_stmt_idx;
        PostingIndexView num_adsh_idx;

        uint64_t sub_zm_block_size = 0;
        uint64_t sub_zm_blocks = 0;
        const int32_t* sub_zm_minmax = nullptr;

        uint64_t sub_pk_lut_size = 0;

        std::vector<uint8_t> adsh_in_fy;
        std::vector<uint32_t> adsh_to_name;
        std::vector<uint32_t> filtered_adsh;
        std::vector<AdshPosting> num_postings_by_adsh;

        std::unordered_map<TripleKey, std::vector<uint32_t>, TripleKeyHash> pre_hash;
        std::unordered_map<AggKey, AggVal, AggKeyHash> global_agg;

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

            usd_code = static_cast<uint16_t>(find_code(uom_dict, "USD"));
            is_code = static_cast<uint8_t>(find_code(stmt_dict, "IS"));

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");
            pre_plabel.open(gendb_dir + "/pre/plabel.bin");

            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            if (sub_adsh.size() != sub_fy.size() || sub_adsh.size() != sub_name.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size() ||
                pre_adsh.size() != pre_plabel.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (num_tag.size() != num_version.size() || num_tag.size() != num_uom.size() ||
                num_tag.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }

            sub_fy_zonemap_blob.open(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin");
            sub_adsh_pk_hash_blob.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
            pre_stmt_hash_blob.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            num_adsh_fk_hash_blob.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");

            pre_stmt_idx = parse_posting_index(pre_stmt_hash_blob, "pre_stmt_hash.bin");
            num_adsh_idx = parse_posting_index(num_adsh_fk_hash_blob, "num_adsh_fk_hash.bin");

            if (sub_fy_zonemap_blob.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("sub_fy_zonemap too small");
            }
            std::memcpy(&sub_zm_block_size, sub_fy_zonemap_blob.data, sizeof(uint64_t));
            std::memcpy(&sub_zm_blocks, sub_fy_zonemap_blob.data + sizeof(uint64_t), sizeof(uint64_t));
            const size_t zm_need = sizeof(uint64_t) * 2 + static_cast<size_t>(sub_zm_blocks) * sizeof(int32_t) * 2;
            if (sub_fy_zonemap_blob.file_size < zm_need) {
                throw std::runtime_error("sub_fy_zonemap truncated");
            }
            sub_zm_minmax = reinterpret_cast<const int32_t*>(sub_fy_zonemap_blob.data + sizeof(uint64_t) * 2);

            if (sub_adsh_pk_hash_blob.file_size < sizeof(uint64_t)) {
                throw std::runtime_error("sub_adsh_pk_hash too small");
            }
            std::memcpy(&sub_pk_lut_size, sub_adsh_pk_hash_blob.data, sizeof(uint64_t));
            const size_t lut_bytes = static_cast<size_t>(sub_pk_lut_size) * sizeof(uint32_t);
            if (sub_adsh_pk_hash_blob.file_size < sizeof(uint64_t) + lut_bytes) {
                throw std::runtime_error("sub_adsh_pk_hash truncated");
            }
            nthreads = omp_get_max_threads();
            if (nthreads < 1) {
                nthreads = 1;
            }

            sub_adsh.prefetch();
            sub_fy.prefetch();
            sub_name.prefetch();
            pre_adsh.prefetch();
            pre_tag.prefetch();
            pre_version.prefetch();
            pre_stmt.prefetch();
            pre_plabel.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            num_uom.prefetch();
            num_value.prefetch();
            sub_fy_zonemap_blob.prefetch();
            sub_adsh_pk_hash_blob.prefetch();
            pre_stmt_hash_blob.prefetch();
            num_adsh_fk_hash_blob.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");

            adsh_in_fy.assign(static_cast<size_t>(sub_pk_lut_size), 0);
            adsh_to_name.assign(static_cast<size_t>(sub_pk_lut_size), kMissing);
            filtered_adsh.reserve(32000);

            const size_t block_size = static_cast<size_t>(sub_zm_block_size == 0 ? 100000 : sub_zm_block_size);
            const size_t sub_rows = sub_adsh.size();

            for (uint64_t b = 0; b < sub_zm_blocks; ++b) {
                const int32_t mn = sub_zm_minmax[2 * b + 0];
                const int32_t mx = sub_zm_minmax[2 * b + 1];
                if (kFyTarget < mn || kFyTarget > mx) {
                    continue;
                }

                const size_t begin = static_cast<size_t>(b) * block_size;
                const size_t end = std::min(begin + block_size, sub_rows);
                for (size_t i = begin; i < end; ++i) {
                    if (sub_fy[i] != kFyTarget) {
                        continue;
                    }
                    const uint32_t adsh = sub_adsh[i];
                    if (adsh >= sub_pk_lut_size) {
                        continue;
                    }
                    if (!adsh_in_fy[adsh]) {
                        adsh_in_fy[adsh] = 1;
                        filtered_adsh.push_back(adsh);
                    }
                    adsh_to_name[adsh] = sub_name[i];
                }
            }
        }

        {
            GENDB_PHASE("build_joins");

            uint64_t is_start = 0;
            uint32_t is_count = 0;
            bool use_stmt_index = find_posting_span(pre_stmt_idx, static_cast<uint32_t>(is_code), is_start, is_count);
            // Known ingest quirk: stmt hash can be built with a u32 reader on a u8 column.
            // If the index hit count is implausibly small, fall back to scanning pre.stmt.
            if (use_stmt_index && pre_stmt.size() > 0 && static_cast<double>(is_count) / static_cast<double>(pre_stmt.size()) < 0.01) {
                use_stmt_index = false;
            }

            if (use_stmt_index) {
                pre_hash.reserve(static_cast<size_t>(is_count / 2 + 1));
                const uint64_t is_end = is_start + is_count;
                if (is_end > pre_stmt_idx.rowid_count) {
                    throw std::runtime_error("pre_stmt_hash rowid span out of range");
                }

                for (uint64_t i = is_start; i < is_end; ++i) {
                    const uint32_t pre_rid = pre_stmt_idx.rowids[i];
                    if (pre_rid >= pre_adsh.size()) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[pre_rid];
                    if (adsh >= adsh_in_fy.size() || !adsh_in_fy[adsh]) {
                        continue;
                    }

                    const TripleKey tk{adsh, pre_tag[pre_rid], pre_version[pre_rid]};
                    pre_hash[tk].push_back(pre_plabel[pre_rid]);
                }
            } else {
                pre_hash.reserve(pre_adsh.size() / 8 + 1);
                for (uint32_t pre_rid = 0; pre_rid < pre_adsh.size(); ++pre_rid) {
                    if (pre_stmt[pre_rid] != is_code) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[pre_rid];
                    if (adsh >= adsh_in_fy.size() || !adsh_in_fy[adsh]) {
                        continue;
                    }
                    const TripleKey tk{adsh, pre_tag[pre_rid], pre_version[pre_rid]};
                    pre_hash[tk].push_back(pre_plabel[pre_rid]);
                }
            }

            num_postings_by_adsh.assign(adsh_in_fy.size(), AdshPosting{});
            for (uint64_t i = 0; i < num_adsh_idx.entry_count; ++i) {
                const PostingEntry e = read_posting_entry(num_adsh_idx, i);
                if (e.key >= adsh_in_fy.size() || !adsh_in_fy[e.key]) {
                    continue;
                }
                num_postings_by_adsh[e.key] = AdshPosting{e.start, e.count};
            }
        }

        {
            GENDB_PHASE("main_scan");

            const uint32_t* __restrict num_tag_data = num_tag.data;
            const uint32_t* __restrict num_version_data = num_version.data;
            const uint16_t* __restrict num_uom_data = num_uom.data;
            const double* __restrict num_value_data = num_value.data;
            const uint32_t stmt_code = static_cast<uint32_t>(is_code);

            std::vector<std::unordered_map<AggKey, AggVal, AggKeyHash>> local_aggs(static_cast<size_t>(nthreads));
            const size_t reserve_per_thread = std::max<size_t>(128, pre_hash.size() / static_cast<size_t>(nthreads) + 1);
            for (int t = 0; t < nthreads; ++t) {
                local_aggs[static_cast<size_t>(t)].reserve(reserve_per_thread);
            }

#pragma omp parallel for schedule(dynamic, 64) num_threads(nthreads)
            for (int64_t i = 0; i < static_cast<int64_t>(filtered_adsh.size()); ++i) {
                const uint32_t adsh = filtered_adsh[static_cast<size_t>(i)];
                const uint32_t name_code = adsh_to_name[adsh];
                if (name_code == kMissing) {
                    continue;
                }

                const AdshPosting post = num_postings_by_adsh[adsh];
                if (post.count == 0) {
                    continue;
                }

                const uint64_t start = post.start;
                const uint64_t end = post.start + post.count;
                if (end > num_adsh_idx.rowid_count) {
                    continue;
                }

                auto& local = local_aggs[static_cast<size_t>(omp_get_thread_num())];
                for (uint64_t p = start; p < end; ++p) {
                    const uint32_t num_rid = num_adsh_idx.rowids[p];
                    if (num_rid >= num_tag.size()) {
                        continue;
                    }
                    if (num_uom_data[num_rid] != usd_code) {
                        continue;
                    }
                    const double v = num_value_data[num_rid];
                    if (std::isnan(v)) {
                        continue;
                    }

                    const uint32_t tag_code = num_tag_data[num_rid];
                    const uint32_t ver_code = num_version_data[num_rid];
                    const TripleKey tk{adsh, tag_code, ver_code};
                    const auto it = pre_hash.find(tk);
                    if (it == pre_hash.end()) {
                        continue;
                    }

                    const std::vector<uint32_t>& plabels = it->second;
                    for (uint32_t plabel_code : plabels) {
                        const AggKey k{name_code, static_cast<uint8_t>(stmt_code), tag_code, plabel_code};
                        AggVal& agg = local[k];
                        agg.sum += v;
                        agg.cnt += 1;
                    }
                }
            }

            size_t total_keys = 0;
            for (const auto& m : local_aggs) {
                total_keys += m.size();
            }
            global_agg.reserve(total_keys + 1);

            for (const auto& m : local_aggs) {
                for (const auto& kv : m) {
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
                rows.push_back(ResultRow{
                    kv.first.name, kv.first.stmt, kv.first.tag, kv.first.plabel, kv.second.sum, kv.second.cnt});
            }

            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) {
                    return a.total_value > b.total_value;
                }
                if (a.name != b.name) {
                    return a.name < b.name;
                }
                if (a.stmt != b.stmt) {
                    return a.stmt < b.stmt;
                }
                if (a.tag != b.tag) {
                    return a.tag < b.tag;
                }
                if (a.plabel != b.plabel) {
                    return a.plabel < b.plabel;
                }
                return a.cnt < b.cnt;
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

            std::fprintf(out, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const ResultRow& r : rows) {
                const std::string_view name_s =
                    (r.name < name_dict.size()) ? std::string_view(name_dict[r.name]) : std::string_view();
                const std::string_view stmt_s =
                    (r.stmt < stmt_dict.size()) ? std::string_view(stmt_dict[r.stmt]) : std::string_view();
                const std::string_view tag_s =
                    (r.tag < tag_dict.size()) ? std::string_view(tag_dict[r.tag]) : std::string_view();
                const std::string_view plabel_s =
                    (r.plabel < plabel_dict.size()) ? std::string_view(plabel_dict[r.plabel]) : std::string_view();

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
