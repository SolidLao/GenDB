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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

// Correctness anchors from plan.json
constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMinAnchor = 4000;
constexpr int32_t kSicMax = 4999;
constexpr uint32_t kPreStmtThresholdAnchor = 100000;

struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct GroupKey {
    int32_t sic;
    uint32_t tlabel;
    uint8_t stmt;

    bool operator==(const GroupKey& o) const {
        return sic == o.sic && tlabel == o.tlabel && stmt == o.stmt;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        uint64_t h = static_cast<uint32_t>(k.sic);
        h ^= (static_cast<uint64_t>(k.tlabel) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        h ^= (static_cast<uint64_t>(k.stmt) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        return static_cast<size_t>(h);
    }
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
    size_t operator()(const TripleKey& k) const {
        uint64_t h = static_cast<uint64_t>(k.adsh) * 0x9e3779b97f4a7c15ULL;
        h ^= static_cast<uint64_t>(k.tag) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggState {
    double sum = 0.0;
    uint64_t count = 0;
    std::unordered_set<int32_t> distinct_cik;
};

struct ResultRow {
    int32_t sic;
    uint32_t tlabel;
    uint8_t stmt;
    uint64_t num_companies;
    double total_value;
    double avg_value;
};

static std::vector<std::string> load_dict_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open dict file: " + path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) throw std::runtime_error("cannot read dict size: " + path);

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) throw std::runtime_error("cannot read dict entry len: " + path);

        std::string s(len, '\0');
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("cannot read dict entry bytes: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

static uint32_t find_dict_code(const std::vector<std::string>& dict, const std::string& needle) {
    for (uint32_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == needle) return i;
    }
    throw std::runtime_error("dictionary value not found: " + needle);
}

static inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

static const PostingEntry* posting_lookup(const PostingEntry* entries, uint64_t n, uint32_t key) {
    uint64_t lo = 0;
    uint64_t hi = n;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const uint32_t mk = entries[mid].key;
        if (mk == key) return &entries[mid];
        if (mk < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return nullptr;
}

static void csv_write_escaped(FILE* out, const std::string& s) {
    bool needs_quotes = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quotes = true;
            break;
        }
    }

    if (!needs_quotes) {
        std::fwrite(s.data(), 1, s.size(), out);
        return;
    }

    std::fputc('"', out);
    for (char c : s) {
        if (c == '"') {
            std::fputc('"', out);
            std::fputc('"', out);
        } else {
            std::fputc(c, out);
        }
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

        std::vector<std::string> uom_dict;
        std::vector<std::string> stmt_dict;
        std::vector<std::string> tlabel_dict;
        uint16_t usd_code = 0;
        uint8_t eq_code_u8 = 0;

        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint8_t> pre_stmt;

        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_tlabel;

        gendb::MmapColumn<uint8_t> sub_pk_raw;
        gendb::MmapColumn<uint8_t> pre_stmt_posting_raw;
        gendb::MmapColumn<uint8_t> num_adsh_posting_raw;
        gendb::MmapColumn<uint8_t> tag_pk_raw;

        uint64_t sub_lut_size = 0;

        std::vector<PostingEntry> pre_stmt_entries;
        const uint32_t* pre_stmt_rowids = nullptr;
        uint64_t pre_stmt_entry_count = 0;

        std::vector<PostingEntry> num_adsh_entries;
        const uint32_t* num_adsh_rowids = nullptr;
        uint64_t num_adsh_entry_count = 0;

        std::vector<uint8_t> filtered_adsh_bitset;
        std::vector<uint32_t> filtered_adsh_list;
        std::vector<int32_t> sic_by_adsh;
        std::vector<int32_t> cik_by_adsh;
        std::vector<uint64_t> num_start_by_adsh;
        std::vector<uint32_t> num_count_by_adsh;

        std::unordered_map<TripleKey, uint32_t, TripleKeyHash> pre_triple_mult;
        std::unordered_set<uint64_t> needed_tag_version;
        std::unordered_map<uint64_t, uint32_t> tag_lookup_needed_only;

        std::unordered_map<GroupKey, AggState, GroupKeyHash> global_agg;

        GENDB_PHASE("total");

        {
            GENDB_PHASE("data_loading");

            uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
            stmt_dict = load_dict_file(gendb_dir + "/dicts/stmt.dict");
            tlabel_dict = load_dict_file(gendb_dir + "/tag/tlabel.dict");

            usd_code = static_cast<uint16_t>(find_dict_code(uom_dict, "USD"));
            const uint32_t eq_code_u32 = find_dict_code(stmt_dict, "EQ");
            if (eq_code_u32 > std::numeric_limits<uint8_t>::max()) {
                throw std::runtime_error("stmt EQ code out of uint8 range");
            }
            eq_code_u8 = static_cast<uint8_t>(eq_code_u32);

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");

            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");

            sub_pk_raw.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
            pre_stmt_posting_raw.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            num_adsh_posting_raw.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");
            tag_pk_raw.open(gendb_dir + "/tag/indexes/tag_tag_version_pk_hash.bin");

            if (sub_adsh.size() != sub_sic.size() || sub_adsh.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (num_tag.size() != num_version.size() || num_tag.size() != num_uom.size() ||
                num_tag.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (tag_abstract.size() != tag_tlabel.size()) {
                throw std::runtime_error("tag column size mismatch");
            }

            {
                if (sub_pk_raw.file_size < sizeof(uint64_t)) {
                    throw std::runtime_error("sub_adsh_pk_hash.bin too small");
                }
                std::memcpy(&sub_lut_size, sub_pk_raw.data, sizeof(uint64_t));
                const size_t need = sizeof(uint64_t) + static_cast<size_t>(sub_lut_size) * sizeof(uint32_t);
                if (sub_pk_raw.file_size < need) {
                    throw std::runtime_error("sub_adsh_pk_hash.bin short payload");
                }
            }

            {
                if (pre_stmt_posting_raw.file_size < sizeof(uint64_t) * 2) {
                    throw std::runtime_error("pre_stmt_hash.bin too small");
                }
                const uint8_t* p = pre_stmt_posting_raw.data;
                uint64_t rowid_count = 0;
                std::memcpy(&pre_stmt_entry_count, p, sizeof(uint64_t));
                std::memcpy(&rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

                const size_t stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
                const size_t entries_bytes = static_cast<size_t>(pre_stmt_entry_count) * stride;
                const size_t entries_off = sizeof(uint64_t) * 2;
                const size_t rowids_off = entries_off + entries_bytes;
                const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
                if (pre_stmt_posting_raw.file_size < rowids_off + rowids_bytes) {
                    throw std::runtime_error("pre_stmt_hash.bin short payload");
                }

                pre_stmt_entries.resize(static_cast<size_t>(pre_stmt_entry_count));
                const uint8_t* cur = p + entries_off;
                for (size_t i = 0; i < pre_stmt_entries.size(); ++i) {
                    PostingEntry e{};
                    std::memcpy(&e.key, cur, sizeof(uint32_t));
                    cur += sizeof(uint32_t);
                    std::memcpy(&e.start, cur, sizeof(uint64_t));
                    cur += sizeof(uint64_t);
                    std::memcpy(&e.count, cur, sizeof(uint32_t));
                    cur += sizeof(uint32_t);
                    pre_stmt_entries[i] = e;
                }
                pre_stmt_rowids = reinterpret_cast<const uint32_t*>(p + rowids_off);
            }

            {
                if (num_adsh_posting_raw.file_size < sizeof(uint64_t) * 2) {
                    throw std::runtime_error("num_adsh_fk_hash.bin too small");
                }
                const uint8_t* p = num_adsh_posting_raw.data;
                uint64_t rowid_count = 0;
                std::memcpy(&num_adsh_entry_count, p, sizeof(uint64_t));
                std::memcpy(&rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

                const size_t stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
                const size_t entries_bytes = static_cast<size_t>(num_adsh_entry_count) * stride;
                const size_t entries_off = sizeof(uint64_t) * 2;
                const size_t rowids_off = entries_off + entries_bytes;
                const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
                if (num_adsh_posting_raw.file_size < rowids_off + rowids_bytes) {
                    throw std::runtime_error("num_adsh_fk_hash.bin short payload");
                }

                num_adsh_entries.resize(static_cast<size_t>(num_adsh_entry_count));
                const uint8_t* cur = p + entries_off;
                for (size_t i = 0; i < num_adsh_entries.size(); ++i) {
                    PostingEntry e{};
                    std::memcpy(&e.key, cur, sizeof(uint32_t));
                    cur += sizeof(uint32_t);
                    std::memcpy(&e.start, cur, sizeof(uint64_t));
                    cur += sizeof(uint64_t);
                    std::memcpy(&e.count, cur, sizeof(uint32_t));
                    cur += sizeof(uint32_t);
                    num_adsh_entries[i] = e;
                }
                num_adsh_rowids = reinterpret_cast<const uint32_t*>(p + rowids_off);
            }

            num_start_by_adsh.assign(static_cast<size_t>(sub_lut_size), 0);
            num_count_by_adsh.assign(static_cast<size_t>(sub_lut_size), 0);
            for (const PostingEntry& e : num_adsh_entries) {
                if (e.key >= sub_lut_size) continue;
                num_start_by_adsh[e.key] = e.start;
                num_count_by_adsh[e.key] = e.count;
            }

            gendb::mmap_prefetch_all(sub_adsh, sub_sic, sub_cik, pre_adsh, pre_tag, pre_version, pre_stmt,
                                     num_tag, num_version, num_uom, num_value,
                                     tag_abstract, tag_tlabel,
                                     sub_pk_raw, pre_stmt_posting_raw, num_adsh_posting_raw, tag_pk_raw);
        }

        {
            GENDB_PHASE("dim_filter");

            if (kSicMin != kSicMinAnchor) {
                throw std::runtime_error("sic anchor mismatch");
            }

            filtered_adsh_bitset.assign(static_cast<size_t>(sub_lut_size), 0);
            sic_by_adsh.assign(static_cast<size_t>(sub_lut_size), 0);
            cik_by_adsh.assign(static_cast<size_t>(sub_lut_size), 0);
            filtered_adsh_list.reserve(8192);

            for (size_t i = 0; i < sub_adsh.size(); ++i) {
                const uint32_t adsh = sub_adsh[i];
                if (adsh >= sub_lut_size) continue;
                const int32_t sic = sub_sic[i];
                if (sic < kSicMin || sic > kSicMax) continue;

                if (!filtered_adsh_bitset[adsh]) {
                    filtered_adsh_bitset[adsh] = 1;
                    filtered_adsh_list.push_back(adsh);
                }
                sic_by_adsh[adsh] = sic;
                cik_by_adsh[adsh] = sub_cik[i];
            }
        }

        {
            GENDB_PHASE("build_joins");

            const PostingEntry* eq_posting = posting_lookup(pre_stmt_entries.data(), pre_stmt_entry_count,
                                                            static_cast<uint32_t>(eq_code_u8));
            const bool use_pre_stmt_posting =
                (eq_posting != nullptr &&
                 eq_posting->count >= kPreStmtThresholdAnchor &&
                 eq_posting->count <= pre_stmt.size());

            if (use_pre_stmt_posting) {
                pre_triple_mult.reserve(static_cast<size_t>(eq_posting->count * 1.2));
                needed_tag_version.reserve(static_cast<size_t>(eq_posting->count * 1.1));

                for (uint64_t i = 0; i < eq_posting->count; ++i) {
                    const uint32_t pre_rowid = pre_stmt_rowids[eq_posting->start + i];
                    const uint32_t adsh = pre_adsh[pre_rowid];
                    if (adsh >= filtered_adsh_bitset.size() || !filtered_adsh_bitset[adsh]) continue;

                    const uint32_t tag = pre_tag[pre_rowid];
                    const uint32_t version = pre_version[pre_rowid];
                    ++pre_triple_mult[TripleKey{adsh, tag, version}];
                    needed_tag_version.insert(pack_tag_version(tag, version));
                }
            } else {
                (void)kPreStmtThresholdAnchor;
                pre_triple_mult.reserve(120000);
                needed_tag_version.reserve(120000);
                for (size_t pre_rowid = 0; pre_rowid < pre_stmt.size(); ++pre_rowid) {
                    if (pre_stmt[pre_rowid] != eq_code_u8) continue;
                    const uint32_t adsh = pre_adsh[pre_rowid];
                    if (adsh >= filtered_adsh_bitset.size() || !filtered_adsh_bitset[adsh]) continue;

                    const uint32_t tag = pre_tag[pre_rowid];
                    const uint32_t version = pre_version[pre_rowid];
                    ++pre_triple_mult[TripleKey{adsh, tag, version}];
                    needed_tag_version.insert(pack_tag_version(tag, version));
                }
            }

            tag_lookup_needed_only.reserve(needed_tag_version.size());

            if (!needed_tag_version.empty()) {
                if (tag_pk_raw.file_size < sizeof(uint64_t)) {
                    throw std::runtime_error("tag_tag_version_pk_hash.bin too small");
                }
                const uint8_t* p = tag_pk_raw.data;
                uint64_t pair_count = 0;
                std::memcpy(&pair_count, p, sizeof(uint64_t));

                const size_t stride = sizeof(uint64_t) + sizeof(uint32_t);
                const size_t off = sizeof(uint64_t);
                const size_t bytes = static_cast<size_t>(pair_count) * stride;
                if (tag_pk_raw.file_size < off + bytes) {
                    throw std::runtime_error("tag_tag_version_pk_hash.bin short payload");
                }

                const uint8_t* cur = p + off;
                for (uint64_t i = 0; i < pair_count; ++i) {
                    uint64_t k = 0;
                    uint32_t rowid = 0;
                    std::memcpy(&k, cur, sizeof(uint64_t));
                    cur += sizeof(uint64_t);
                    std::memcpy(&rowid, cur, sizeof(uint32_t));
                    cur += sizeof(uint32_t);

                    if (needed_tag_version.find(k) == needed_tag_version.end()) continue;
                    if (rowid >= tag_abstract.size()) continue;
                    if (tag_abstract[rowid] != 0) continue;

                    tag_lookup_needed_only.emplace(k, tag_tlabel[rowid]);
                }
            }

            num_tag.advise_random();
            num_version.advise_random();
            num_uom.advise_random();
            num_value.advise_random();
        }

        {
            GENDB_PHASE("main_scan");

            const int thread_count = std::max(1, omp_get_max_threads());
            std::vector<std::unordered_map<GroupKey, AggState, GroupKeyHash>> local_aggs(
                static_cast<size_t>(thread_count));

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                auto& local = local_aggs[static_cast<size_t>(tid)];
                local.reserve(1024);

#pragma omp for schedule(dynamic, 16)
                for (size_t i = 0; i < filtered_adsh_list.size(); ++i) {
                    const uint32_t adsh = filtered_adsh_list[i];
                    const uint32_t adsh_count = num_count_by_adsh[adsh];
                    if (adsh_count == 0) continue;
                    const uint64_t adsh_start = num_start_by_adsh[adsh];

                    const int32_t sic = sic_by_adsh[adsh];
                    const int32_t cik = cik_by_adsh[adsh];

                    for (uint64_t p = adsh_start, pend = adsh_start + adsh_count;
                         p < pend; ++p) {
                        const uint32_t num_rowid = num_adsh_rowids[p];

                        if (num_uom[num_rowid] != usd_code) continue;
                        const double v = num_value[num_rowid];
                        if (std::isnan(v)) continue;

                        const uint32_t tag = num_tag[num_rowid];
                        const uint32_t version = num_version[num_rowid];

                        const auto it_p = pre_triple_mult.find(TripleKey{adsh, tag, version});
                        if (it_p == pre_triple_mult.end()) continue;
                        const uint32_t pre_mult = it_p->second;

                        const uint64_t tv = pack_tag_version(tag, version);
                        const auto it_t = tag_lookup_needed_only.find(tv);
                        if (it_t == tag_lookup_needed_only.end()) continue;

                        GroupKey gk{sic, it_t->second, eq_code_u8};
                        AggState& agg = local[gk];
                        agg.sum += v * static_cast<double>(pre_mult);
                        agg.count += static_cast<uint64_t>(pre_mult);
                        agg.distinct_cik.insert(cik);
                    }
                }
            }

            global_agg.reserve(4096);
            for (auto& local : local_aggs) {
                for (auto& kv : local) {
                    const GroupKey& key = kv.first;
                    AggState& src = kv.second;
                    AggState& dst = global_agg[key];
                    dst.sum += src.sum;
                    dst.count += src.count;
                    if (dst.distinct_cik.empty()) {
                        dst.distinct_cik.reserve(src.distinct_cik.size() * 2 + 1);
                    }
                    for (int32_t cik : src.distinct_cik) {
                        dst.distinct_cik.insert(cik);
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<ResultRow> rows;
            rows.reserve(global_agg.size());

            for (const auto& kv : global_agg) {
                const GroupKey& k = kv.first;
                const AggState& a = kv.second;
                const uint64_t num_companies = static_cast<uint64_t>(a.distinct_cik.size());
                if (num_companies < 2) continue;
                if (a.count == 0) continue;

                rows.push_back(ResultRow{k.sic, k.tlabel, k.stmt, num_companies, a.sum,
                                         a.sum / static_cast<double>(a.count)});
            }

            std::sort(rows.begin(), rows.end(), [](const ResultRow& x, const ResultRow& y) {
                if (x.total_value != y.total_value) return x.total_value > y.total_value;
                if (x.sic != y.sic) return x.sic < y.sic;
                if (x.tlabel != y.tlabel) return x.tlabel < y.tlabel;
                return x.stmt < y.stmt;
            });

            if (rows.size() > 500) rows.resize(500);

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q4.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("cannot open output file: " + out_path);
            }

            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            for (const ResultRow& r : rows) {
                std::fprintf(out, "%d,", r.sic);

                const std::string tlabel = (r.tlabel < tlabel_dict.size()) ? tlabel_dict[r.tlabel] : std::string();
                csv_write_escaped(out, tlabel);
                std::fputc(',', out);

                const std::string stmt = (r.stmt < stmt_dict.size()) ? stmt_dict[r.stmt] : std::string("EQ");
                csv_write_escaped(out, stmt);

                std::fprintf(out, ",%llu,%.2f,%.2f\n",
                             static_cast<unsigned long long>(r.num_companies),
                             r.total_value,
                             r.avg_value);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
