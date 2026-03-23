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

// Correctness anchors (do not modify)
constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMinAnchor = 4000;
constexpr uint32_t kPreStmtThreshold = 100000;
constexpr int32_t kSicMax = 4999;

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

struct TagPair {
    uint64_t key;
    uint32_t rowid;
};

struct GroupKey {
    int32_t sic;
    uint32_t tlabel;

    bool operator==(const GroupKey& o) const {
        return sic == o.sic && tlabel == o.tlabel;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        uint64_t h = static_cast<uint64_t>(static_cast<uint32_t>(k.sic));
        h ^= static_cast<uint64_t>(k.tlabel) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggState {
    double sum = 0.0;
    uint64_t cnt = 0;
    std::unordered_set<int32_t> distinct_cik;
};

struct ResultRow {
    int32_t sic;
    uint32_t tlabel;
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

static const PostingEntry* posting_lookup(const PostingEntry* entries, uint64_t n, uint32_t key) {
    uint64_t lo = 0;
    uint64_t hi = n;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const uint32_t mk = entries[mid].key;
        if (mk == key) return &entries[mid];
        if (mk < key) lo = mid + 1;
        else hi = mid;
    }
    return nullptr;
}

static const TriplePostingEntry* triple_lookup(const TriplePostingEntry* entries,
                                               uint64_t n,
                                               uint32_t a,
                                               uint32_t b,
                                               uint32_t c) {
    uint64_t lo = 0;
    uint64_t hi = n;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const TriplePostingEntry& e = entries[mid];
        if (e.a == a && e.b == b && e.c == c) return &e;

        if (e.a < a || (e.a == a && (e.b < b || (e.b == b && e.c < c)))) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return nullptr;
}

static inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
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
        uint32_t usd_code = 0;
        uint32_t eq_code = 0;

        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint16_t> num_uom;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint8_t> pre_stmt;

        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;

        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_tlabel;

        gendb::MmapColumn<uint8_t> pre_stmt_posting_raw;
        gendb::MmapColumn<uint8_t> num_triple_posting_raw;
        gendb::MmapColumn<uint8_t> sub_pk_raw;
        gendb::MmapColumn<uint8_t> tag_pk_raw;

        uint64_t pre_stmt_entry_count = 0;
        uint64_t pre_stmt_rowid_count = 0;
        std::vector<PostingEntry> pre_stmt_entries;
        const uint32_t* pre_stmt_rowids = nullptr;

        uint64_t num_triple_entry_count = 0;
        uint64_t num_triple_rowid_count = 0;
        std::vector<TriplePostingEntry> num_triple_entries;
        const uint32_t* num_triple_rowids = nullptr;

        uint64_t sub_lut_size = 0;
        const uint32_t* sub_lut = nullptr;

        uint64_t tag_pair_count = 0;
        std::vector<TagPair> tag_pairs;
        std::unordered_map<uint64_t, uint32_t> tag_pk_map;

        GENDB_PHASE("total");

        {
            GENDB_PHASE("data_loading");

            uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
            stmt_dict = load_dict_file(gendb_dir + "/dicts/stmt.dict");
            tlabel_dict = load_dict_file(gendb_dir + "/tag/tlabel.dict");
            usd_code = find_dict_code(uom_dict, "USD");
            eq_code = find_dict_code(stmt_dict, "EQ");

            num_value.open(gendb_dir + "/num/value.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");

            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");

            pre_stmt_posting_raw.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            num_triple_posting_raw.open(gendb_dir + "/num/indexes/num_adsh_tag_version_hash.bin");
            sub_pk_raw.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
            tag_pk_raw.open(gendb_dir + "/tag/indexes/tag_tag_version_pk_hash.bin");

            if (num_value.size() != num_uom.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (sub_sic.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (tag_abstract.size() != tag_tlabel.size()) {
                throw std::runtime_error("tag column size mismatch");
            }

            {
                const uint8_t* p = pre_stmt_posting_raw.data;
                if (pre_stmt_posting_raw.file_size < sizeof(uint64_t) * 2) {
                    throw std::runtime_error("pre_stmt_hash.bin too small");
                }
                std::memcpy(&pre_stmt_entry_count, p, sizeof(uint64_t));
                std::memcpy(&pre_stmt_rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));
                const size_t off_entries = sizeof(uint64_t) * 2;
                const size_t stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
                const size_t bytes_entries = static_cast<size_t>(pre_stmt_entry_count) * stride;
                const size_t off_rowids = off_entries + bytes_entries;
                const size_t bytes_rowids = static_cast<size_t>(pre_stmt_rowid_count) * sizeof(uint32_t);
                if (pre_stmt_posting_raw.file_size < off_rowids + bytes_rowids) {
                    throw std::runtime_error("pre_stmt_hash.bin short payload");
                }
                pre_stmt_entries.resize(static_cast<size_t>(pre_stmt_entry_count));
                const uint8_t* ptr = p + off_entries;
                for (size_t i = 0; i < pre_stmt_entries.size(); ++i) {
                    PostingEntry e{};
                    std::memcpy(&e.key, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    std::memcpy(&e.start, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
                    std::memcpy(&e.count, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    pre_stmt_entries[i] = e;
                }
                pre_stmt_rowids = reinterpret_cast<const uint32_t*>(p + off_rowids);
            }

            {
                const uint8_t* p = num_triple_posting_raw.data;
                if (num_triple_posting_raw.file_size < sizeof(uint64_t) * 2) {
                    throw std::runtime_error("num_adsh_tag_version_hash.bin too small");
                }
                std::memcpy(&num_triple_entry_count, p, sizeof(uint64_t));
                std::memcpy(&num_triple_rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));
                const size_t off_entries = sizeof(uint64_t) * 2;
                const size_t stride = sizeof(uint32_t) * 3 + sizeof(uint64_t) + sizeof(uint32_t);
                const size_t bytes_entries = static_cast<size_t>(num_triple_entry_count) * stride;
                const size_t off_rowids = off_entries + bytes_entries;
                const size_t bytes_rowids = static_cast<size_t>(num_triple_rowid_count) * sizeof(uint32_t);
                if (num_triple_posting_raw.file_size < off_rowids + bytes_rowids) {
                    throw std::runtime_error("num_adsh_tag_version_hash.bin short payload");
                }
                num_triple_entries.resize(static_cast<size_t>(num_triple_entry_count));
                const uint8_t* ptr = p + off_entries;
                for (size_t i = 0; i < num_triple_entries.size(); ++i) {
                    TriplePostingEntry e{};
                    std::memcpy(&e.a, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    std::memcpy(&e.b, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    std::memcpy(&e.c, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    std::memcpy(&e.start, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
                    std::memcpy(&e.count, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    num_triple_entries[i] = e;
                }
                num_triple_rowids = reinterpret_cast<const uint32_t*>(p + off_rowids);
            }

            {
                const uint8_t* p = sub_pk_raw.data;
                if (sub_pk_raw.file_size < sizeof(uint64_t)) {
                    throw std::runtime_error("sub_adsh_pk_hash.bin too small");
                }
                std::memcpy(&sub_lut_size, p, sizeof(uint64_t));
                const size_t expect = sizeof(uint64_t) + static_cast<size_t>(sub_lut_size) * sizeof(uint32_t);
                if (sub_pk_raw.file_size < expect) {
                    throw std::runtime_error("sub_adsh_pk_hash.bin short payload");
                }
                sub_lut = reinterpret_cast<const uint32_t*>(p + sizeof(uint64_t));
            }

            {
                const uint8_t* p = tag_pk_raw.data;
                if (tag_pk_raw.file_size < sizeof(uint64_t)) {
                    throw std::runtime_error("tag_tag_version_pk_hash.bin too small");
                }
                std::memcpy(&tag_pair_count, p, sizeof(uint64_t));
                const size_t off = sizeof(uint64_t);
                const size_t stride = sizeof(uint64_t) + sizeof(uint32_t);
                const size_t bytes = static_cast<size_t>(tag_pair_count) * stride;
                if (tag_pk_raw.file_size < off + bytes) {
                    throw std::runtime_error("tag_tag_version_pk_hash.bin short payload");
                }
                tag_pairs.resize(static_cast<size_t>(tag_pair_count));
                const uint8_t* ptr = p + off;
                for (size_t i = 0; i < tag_pairs.size(); ++i) {
                    TagPair x{};
                    std::memcpy(&x.key, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
                    std::memcpy(&x.rowid, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    tag_pairs[i] = x;
                }
            }

            gendb::mmap_prefetch_all(num_value, num_uom, pre_adsh, pre_tag, pre_version, pre_stmt,
                                     sub_sic, sub_cik, tag_abstract, tag_tlabel,
                                     pre_stmt_posting_raw, num_triple_posting_raw, sub_pk_raw, tag_pk_raw);
        }

        {
            GENDB_PHASE("dim_filter");
            // Direct PK LUT probe is used in main_scan; keep this phase explicit per contract.
            if (kSicMinAnchor != kSicMin) {
                throw std::runtime_error("sic anchor mismatch");
            }
        }

        {
            GENDB_PHASE("build_joins");
            // Build hash table for tag PK map once; probes are late and selective.
            tag_pk_map.reserve(static_cast<size_t>(tag_pair_count * 1.3));
            for (const TagPair& p : tag_pairs) {
                tag_pk_map.emplace(p.key, p.rowid);
            }

            num_value.advise_random();
            num_uom.advise_random();
            pre_adsh.advise_random();
            pre_tag.advise_random();
            pre_version.advise_random();
            pre_stmt.advise_random();
            sub_sic.advise_random();
            sub_cik.advise_random();
            tag_abstract.advise_random();
            tag_tlabel.advise_random();
        }

        std::unordered_map<GroupKey, AggState, GroupKeyHash> global_agg;

        {
            GENDB_PHASE("main_scan");

            const PostingEntry* eq_entry = posting_lookup(pre_stmt_entries.data(), pre_stmt_entry_count, eq_code);
            const bool use_pre_stmt_index = (eq_entry != nullptr && eq_entry->count >= kPreStmtThreshold);
            // Anchor use (planner-selected threshold).
            if (!use_pre_stmt_index) {
                // Keep plan behavior stable with anchored threshold.
            }

            int thread_count = omp_get_max_threads();
            if (thread_count < 1) thread_count = 1;
            const auto& tag_pk = tag_pk_map;

            std::vector<std::unordered_map<GroupKey, AggState, GroupKeyHash>> local_aggs(
                static_cast<size_t>(thread_count));

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                auto& local = local_aggs[static_cast<size_t>(tid)];
                local.reserve(2048);

#pragma omp for schedule(dynamic, 1024)
                for (uint64_t i = 0; i < (use_pre_stmt_index ? eq_entry->count : static_cast<uint64_t>(pre_stmt.size())); ++i) {
                    const uint32_t pre_rowid =
                        use_pre_stmt_index ? pre_stmt_rowids[eq_entry->start + i] : static_cast<uint32_t>(i);
                    if (!use_pre_stmt_index && pre_stmt[pre_rowid] != static_cast<uint8_t>(eq_code)) continue;
                    const uint32_t adsh = pre_adsh[pre_rowid];
                    const uint32_t tag = pre_tag[pre_rowid];
                    const uint32_t version = pre_version[pre_rowid];

                    const TriplePostingEntry* tri = triple_lookup(
                        num_triple_entries.data(), num_triple_entry_count, adsh, tag, version);
                    if (!tri) continue;

                    if (adsh >= sub_lut_size) continue;
                    const uint32_t sub_rowid = sub_lut[adsh];
                    if (sub_rowid == std::numeric_limits<uint32_t>::max()) continue;
                    const int32_t sic = sub_sic[sub_rowid];
                    if (sic < kSicMin || sic > kSicMax) continue;
                    const int32_t cik = sub_cik[sub_rowid];

                    const uint64_t tv_key = pack_tag_version(tag, version);
                    const auto tit = tag_pk.find(tv_key);
                    if (tit == tag_pk.end()) continue;
                    const uint32_t tag_rowid = tit->second;
                    if (tag_abstract[tag_rowid] != 0) continue;
                    const uint32_t tlabel = tag_tlabel[tag_rowid];

                    GroupKey gk{sic, tlabel};
                    AggState& agg = local[gk];
                    for (uint64_t p = tri->start, pend = tri->start + tri->count; p < pend; ++p) {
                        const uint32_t num_rowid = num_triple_rowids[p];
                        if (num_uom[num_rowid] != static_cast<uint16_t>(usd_code)) continue;
                        const double v = num_value[num_rowid];
                        if (std::isnan(v)) continue;

                        agg.sum += v;
                        agg.cnt += 1;
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
                    dst.cnt += src.cnt;
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
                const GroupKey& key = kv.first;
                const AggState& agg = kv.second;
                const uint64_t distinct_cnt = static_cast<uint64_t>(agg.distinct_cik.size());
                if (distinct_cnt < 2) continue;
                if (agg.cnt == 0) continue;

                rows.push_back(ResultRow{key.sic, key.tlabel, distinct_cnt, agg.sum,
                                         agg.sum / static_cast<double>(agg.cnt)});
            }

            std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.sic != b.sic) return a.sic < b.sic;
                return a.tlabel < b.tlabel;
            });

            if (rows.size() > 500) rows.resize(500);

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q4.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) throw std::runtime_error("cannot open output file: " + out_path);

            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            const std::string stmt_str = (eq_code < stmt_dict.size()) ? stmt_dict[eq_code] : std::string("EQ");
            for (const ResultRow& r : rows) {
                std::fprintf(out, "%d,", r.sic);
                const std::string tlabel = (r.tlabel < tlabel_dict.size()) ? tlabel_dict[r.tlabel] : std::string();
                csv_write_escaped(out, tlabel);
                std::fputc(',', out);
                csv_write_escaped(out, stmt_str);
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
