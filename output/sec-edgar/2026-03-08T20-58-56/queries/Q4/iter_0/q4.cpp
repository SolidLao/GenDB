#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

constexpr size_t kMorselRows = 65536;

struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct TagPair {
    uint64_t key;
    uint32_t rowid;
};

struct SubInfo {
    int32_t sic;
    int32_t cik;
};

struct Task {
    uint64_t rowids_start;
    uint32_t rowids_count;
    int32_t sic;
    int32_t cik;
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
        uint64_t h = static_cast<uint64_t>(k.adsh) * 0x9E3779B97F4A7C15ULL;
        h ^= static_cast<uint64_t>(k.tag) + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x94D049BB133111EBULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
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
        uint64_t x = static_cast<uint64_t>(static_cast<uint32_t>(k.sic));
        uint64_t y = static_cast<uint64_t>(k.tlabel);
        uint64_t h = x * 0x9E3779B97F4A7C15ULL;
        h ^= y + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggState {
    double total_value = 0.0;
    uint64_t count_value = 0;
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
    if (!in) {
        throw std::runtime_error("cannot open dict file: " + path);
    }

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) {
        throw std::runtime_error("cannot read dict size: " + path);
    }

    std::vector<std::string> values;
    values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) {
            throw std::runtime_error("cannot read dict entry len: " + path);
        }
        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) {
                throw std::runtime_error("cannot read dict entry bytes: " + path);
            }
        }
        values.push_back(std::move(s));
    }

    return values;
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
        uint32_t usd_code = 0;
        uint32_t eq_code = 0;

        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint8_t> pre_stmt;

        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_tlabel;

        gendb::MmapColumn<uint8_t> sub_sic_zonemap_raw;
        uint64_t sub_sic_block_size = 0;
        uint64_t sub_sic_blocks = 0;
        const int32_t* sub_sic_minmax = nullptr;

        gendb::MmapColumn<uint8_t> num_adsh_posting_raw;
        uint64_t num_adsh_entry_count = 0;
        uint64_t num_adsh_rowid_count = 0;
        std::vector<PostingEntry> num_adsh_entries;
        const uint32_t* num_adsh_rowids = nullptr;

        gendb::MmapColumn<uint8_t> pre_stmt_posting_raw;
        uint64_t pre_stmt_entry_count = 0;
        uint64_t pre_stmt_rowid_count = 0;
        std::vector<PostingEntry> pre_stmt_entries;
        const uint32_t* pre_stmt_rowids = nullptr;

        gendb::MmapColumn<uint8_t> tag_pk_raw;
        uint64_t tag_pair_count = 0;
        std::vector<TagPair> tag_pairs;

        std::unordered_map<uint32_t, SubInfo> sub_dim;
        std::unordered_map<TripleKey, uint32_t, TripleKeyHash> pre_triple_count;
        std::unordered_map<uint64_t, uint32_t> tag_pk;

        std::vector<Task> tasks;

        GENDB_PHASE("total");

        {
            GENDB_PHASE("data_loading");

            uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
            stmt_dict = load_dict_file(gendb_dir + "/dicts/stmt.dict");
            tlabel_dict = load_dict_file(gendb_dir + "/tag/tlabel.dict");
            usd_code = find_dict_code(uom_dict, "USD");
            eq_code = find_dict_code(stmt_dict, "EQ");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");

            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");

            if (sub_adsh.size() != sub_sic.size() || sub_adsh.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_uom.size() || num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (tag_abstract.size() != tag_tlabel.size()) {
                throw std::runtime_error("tag column size mismatch");
            }

            sub_sic_zonemap_raw.open(gendb_dir + "/sub/indexes/sub_sic_zonemap.bin");
            if (sub_sic_zonemap_raw.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("sub_sic_zonemap.bin too small");
            }
            const uint8_t* z = sub_sic_zonemap_raw.data;
            std::memcpy(&sub_sic_block_size, z, sizeof(uint64_t));
            std::memcpy(&sub_sic_blocks, z + sizeof(uint64_t), sizeof(uint64_t));
            const size_t z_expect = sizeof(uint64_t) * 2 + static_cast<size_t>(sub_sic_blocks) * sizeof(int32_t) * 2;
            if (sub_sic_zonemap_raw.file_size < z_expect) {
                throw std::runtime_error("sub_sic_zonemap.bin short payload");
            }
            sub_sic_minmax = reinterpret_cast<const int32_t*>(z + sizeof(uint64_t) * 2);

            num_adsh_posting_raw.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");
            if (num_adsh_posting_raw.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("num_adsh_fk_hash.bin too small");
            }
            const uint8_t* na = num_adsh_posting_raw.data;
            std::memcpy(&num_adsh_entry_count, na, sizeof(uint64_t));
            std::memcpy(&num_adsh_rowid_count, na + sizeof(uint64_t), sizeof(uint64_t));
            const size_t na_entries_off = sizeof(uint64_t) * 2;
            const size_t na_entry_stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
            const size_t na_entries_bytes = static_cast<size_t>(num_adsh_entry_count) * na_entry_stride;
            const size_t na_rowids_off = na_entries_off + na_entries_bytes;
            const size_t na_rowids_bytes = static_cast<size_t>(num_adsh_rowid_count) * sizeof(uint32_t);
            if (num_adsh_posting_raw.file_size < na_rowids_off + na_rowids_bytes) {
                throw std::runtime_error("num_adsh_fk_hash.bin short payload");
            }
            num_adsh_entries.resize(static_cast<size_t>(num_adsh_entry_count));
            const uint8_t* na_ptr = na + na_entries_off;
            for (size_t i = 0; i < num_adsh_entries.size(); ++i) {
                PostingEntry e{};
                std::memcpy(&e.key, na_ptr, sizeof(uint32_t));
                na_ptr += sizeof(uint32_t);
                std::memcpy(&e.start, na_ptr, sizeof(uint64_t));
                na_ptr += sizeof(uint64_t);
                std::memcpy(&e.count, na_ptr, sizeof(uint32_t));
                na_ptr += sizeof(uint32_t);
                num_adsh_entries[i] = e;
            }
            num_adsh_rowids = reinterpret_cast<const uint32_t*>(na + na_rowids_off);

            pre_stmt_posting_raw.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            if (pre_stmt_posting_raw.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("pre_stmt_hash.bin too small");
            }
            const uint8_t* ps = pre_stmt_posting_raw.data;
            std::memcpy(&pre_stmt_entry_count, ps, sizeof(uint64_t));
            std::memcpy(&pre_stmt_rowid_count, ps + sizeof(uint64_t), sizeof(uint64_t));
            const size_t ps_entries_off = sizeof(uint64_t) * 2;
            const size_t ps_entry_stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
            const size_t ps_entries_bytes = static_cast<size_t>(pre_stmt_entry_count) * ps_entry_stride;
            const size_t ps_rowids_off = ps_entries_off + ps_entries_bytes;
            const size_t ps_rowids_bytes = static_cast<size_t>(pre_stmt_rowid_count) * sizeof(uint32_t);
            if (pre_stmt_posting_raw.file_size < ps_rowids_off + ps_rowids_bytes) {
                throw std::runtime_error("pre_stmt_hash.bin short payload");
            }
            pre_stmt_entries.resize(static_cast<size_t>(pre_stmt_entry_count));
            const uint8_t* ps_ptr = ps + ps_entries_off;
            for (size_t i = 0; i < pre_stmt_entries.size(); ++i) {
                PostingEntry e{};
                std::memcpy(&e.key, ps_ptr, sizeof(uint32_t));
                ps_ptr += sizeof(uint32_t);
                std::memcpy(&e.start, ps_ptr, sizeof(uint64_t));
                ps_ptr += sizeof(uint64_t);
                std::memcpy(&e.count, ps_ptr, sizeof(uint32_t));
                ps_ptr += sizeof(uint32_t);
                pre_stmt_entries[i] = e;
            }
            pre_stmt_rowids = reinterpret_cast<const uint32_t*>(ps + ps_rowids_off);

            tag_pk_raw.open(gendb_dir + "/tag/indexes/tag_tag_version_pk_hash.bin");
            if (tag_pk_raw.file_size < sizeof(uint64_t)) {
                throw std::runtime_error("tag_tag_version_pk_hash.bin too small");
            }
            const uint8_t* tp = tag_pk_raw.data;
            std::memcpy(&tag_pair_count, tp, sizeof(uint64_t));
            const size_t tp_pairs_off = sizeof(uint64_t);
            const size_t tp_pair_stride = sizeof(uint64_t) + sizeof(uint32_t);
            const size_t tp_pairs_bytes = static_cast<size_t>(tag_pair_count) * tp_pair_stride;
            if (tag_pk_raw.file_size < tp_pairs_off + tp_pairs_bytes) {
                throw std::runtime_error("tag_tag_version_pk_hash.bin short payload");
            }
            tag_pairs.resize(static_cast<size_t>(tag_pair_count));
            const uint8_t* tp_ptr = tp + tp_pairs_off;
            for (size_t i = 0; i < tag_pairs.size(); ++i) {
                TagPair p{};
                std::memcpy(&p.key, tp_ptr, sizeof(uint64_t));
                tp_ptr += sizeof(uint64_t);
                std::memcpy(&p.rowid, tp_ptr, sizeof(uint32_t));
                tp_ptr += sizeof(uint32_t);
                tag_pairs[i] = p;
            }

            gendb::mmap_prefetch_all(sub_adsh, sub_sic, sub_cik,
                                     num_adsh, num_tag, num_version, num_uom, num_value,
                                     pre_adsh, pre_tag, pre_version, pre_stmt,
                                     tag_abstract, tag_tlabel,
                                     sub_sic_zonemap_raw, num_adsh_posting_raw,
                                     pre_stmt_posting_raw, tag_pk_raw);
        }

        {
            GENDB_PHASE("dim_filter");

            sub_dim.reserve(8192);

            const size_t sub_rows = sub_adsh.size();
            for (uint64_t b = 0; b < sub_sic_blocks; ++b) {
                const int32_t mn = sub_sic_minmax[2 * b + 0];
                const int32_t mx = sub_sic_minmax[2 * b + 1];
                if (mx < 4000 || mn > 4999) continue;

                const size_t row_lo = static_cast<size_t>(b) * static_cast<size_t>(sub_sic_block_size);
                const size_t row_hi = std::min(row_lo + static_cast<size_t>(sub_sic_block_size), sub_rows);
                for (size_t i = row_lo; i < row_hi; ++i) {
                    const int32_t sic = sub_sic[i];
                    if (sic < 4000 || sic > 4999) continue;
                    sub_dim[sub_adsh[i]] = SubInfo{sic, sub_cik[i]};
                }
            }

            tasks.reserve(sub_dim.size() * 4);
            for (const auto& kv : sub_dim) {
                const uint32_t adsh = kv.first;
                const SubInfo info = kv.second;

                const PostingEntry* e = posting_lookup(num_adsh_entries.data(), num_adsh_entry_count, adsh);
                if (!e || e->count == 0) continue;

                uint64_t start = e->start;
                uint32_t remain = e->count;
                while (remain > 0) {
                    const uint32_t chunk = static_cast<uint32_t>(std::min<uint64_t>(remain, kMorselRows));
                    tasks.push_back(Task{start, chunk, info.sic, info.cik});
                    start += chunk;
                    remain -= chunk;
                }
            }
        }

        {
            GENDB_PHASE("build_joins");

            const PostingEntry* eq_entry = posting_lookup(pre_stmt_entries.data(), pre_stmt_entry_count, eq_code);
            const bool use_pre_stmt_index = (eq_entry != nullptr && eq_entry->count >= 100000);
            if (use_pre_stmt_index) {
                pre_triple_count.reserve(static_cast<size_t>(eq_entry->count * 1.5));
                for (uint64_t i = 0; i < eq_entry->count; ++i) {
                    const uint32_t pre_rowid = pre_stmt_rowids[eq_entry->start + i];
                    const TripleKey key{pre_adsh[pre_rowid], pre_tag[pre_rowid], pre_version[pre_rowid]};
                    auto it = pre_triple_count.find(key);
                    if (it == pre_triple_count.end()) {
                        pre_triple_count.emplace(key, 1u);
                    } else {
                        it->second += 1u;
                    }
                }
            } else {
                const size_t pre_rows = pre_stmt.size();
                pre_triple_count.reserve(1200000);
                for (size_t i = 0; i < pre_rows; ++i) {
                    if (pre_stmt[i] != static_cast<uint8_t>(eq_code)) continue;
                    const TripleKey key{pre_adsh[i], pre_tag[i], pre_version[i]};
                    auto it = pre_triple_count.find(key);
                    if (it == pre_triple_count.end()) {
                        pre_triple_count.emplace(key, 1u);
                    } else {
                        it->second += 1u;
                    }
                }
            }

            tag_pk.reserve(static_cast<size_t>(tag_pair_count * 1.3));
            for (uint64_t i = 0; i < tag_pair_count; ++i) {
                tag_pk.emplace(tag_pairs[i].key, tag_pairs[i].rowid);
            }

            num_tag.advise_random();
            num_version.advise_random();
            num_uom.advise_random();
            num_value.advise_random();
            pre_adsh.advise_random();
            pre_tag.advise_random();
            pre_version.advise_random();
            pre_stmt.advise_random();
            tag_abstract.advise_random();
            tag_tlabel.advise_random();
        }

        std::unordered_map<GroupKey, AggState, GroupKeyHash> global_agg;

        {
            GENDB_PHASE("main_scan");

            int thread_count = omp_get_max_threads();
            if (thread_count < 1) thread_count = 1;
            if (thread_count > 64) thread_count = 64;

            std::vector<std::unordered_map<GroupKey, AggState, GroupKeyHash>> local_aggs(
                static_cast<size_t>(thread_count));

            #pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                auto& local = local_aggs[static_cast<size_t>(tid)];
                local.reserve(2048);

                #pragma omp for schedule(dynamic, 1)
                for (size_t ti = 0; ti < tasks.size(); ++ti) {
                    const Task& task = tasks[ti];
                    const uint64_t start = task.rowids_start;
                    const uint64_t end = start + task.rowids_count;

                    for (uint64_t p = start; p < end; ++p) {
                        const uint32_t num_rowid = num_adsh_rowids[p];

                        if (num_uom[num_rowid] != static_cast<uint16_t>(usd_code)) continue;

                        const double val = num_value[num_rowid];
                        if (std::isnan(val)) continue;

                        const uint32_t adsh = num_adsh[num_rowid];
                        const uint32_t tag = num_tag[num_rowid];
                        const uint32_t version = num_version[num_rowid];

                        const TripleKey tri{adsh, tag, version};
                        const auto pit = pre_triple_count.find(tri);
                        if (pit == pre_triple_count.end()) continue;
                        const uint32_t pre_mult = pit->second;

                        const uint64_t tv_key = pack_tag_version(tag, version);
                        const auto tit = tag_pk.find(tv_key);
                        if (tit == tag_pk.end()) continue;
                        const uint32_t tag_rowid = tit->second;
                        if (tag_abstract[tag_rowid] != 0) continue;

                        const uint32_t tlabel_code = tag_tlabel[tag_rowid];
                        GroupKey gk{task.sic, tlabel_code};
                        AggState& g = local[gk];
                        g.total_value += val * static_cast<double>(pre_mult);
                        g.count_value += static_cast<uint64_t>(pre_mult);
                        g.distinct_cik.insert(task.cik);
                    }
                }
            }

            global_agg.reserve(4096);
            for (auto& local : local_aggs) {
                for (auto& kv : local) {
                    const GroupKey& key = kv.first;
                    AggState& src = kv.second;
                    AggState& dst = global_agg[key];
                    dst.total_value += src.total_value;
                    dst.count_value += src.count_value;
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
                if (agg.count_value == 0) continue;

                rows.push_back(ResultRow{
                    key.sic,
                    key.tlabel,
                    distinct_cnt,
                    agg.total_value,
                    agg.total_value / static_cast<double>(agg.count_value)
                });
            }

            std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.sic != b.sic) return a.sic < b.sic;
                return a.tlabel < b.tlabel;
            });

            if (rows.size() > 500) {
                rows.resize(500);
            }

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q4.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("cannot open output file: " + out_path);
            }

            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            const std::string stmt_str = (eq_code < stmt_dict.size()) ? stmt_dict[eq_code] : std::string("EQ");
            for (const auto& r : rows) {
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
