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

constexpr int32_t kSicMin = 4000;
constexpr int32_t kSicMinAnchor = 4000;
constexpr int32_t kSicMax = 4999;
constexpr uint32_t kPreStmtThresholdAnchor = 100000;

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
    size_t operator()(const TripleKey& k) const {
        uint64_t h = static_cast<uint64_t>(k.adsh) * 0x9e3779b97f4a7c15ULL;
        h ^= static_cast<uint64_t>(k.tag) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggKey {
    int32_t sic;
    uint32_t tag;
    uint32_t version;
    uint8_t stmt;

    bool operator==(const AggKey& o) const {
        return sic == o.sic && tag == o.tag && version == o.version && stmt == o.stmt;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        uint64_t h = static_cast<uint32_t>(k.sic);
        h ^= static_cast<uint64_t>(k.tag) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.stmt) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct AggState {
    double sum = 0.0;
    uint64_t count = 0;
};

struct GroupCikKey {
    int32_t sic;
    uint32_t tag;
    uint32_t version;
    uint8_t stmt;
    int32_t cik;

    bool operator==(const GroupCikKey& o) const {
        return sic == o.sic && tag == o.tag && version == o.version && stmt == o.stmt && cik == o.cik;
    }
};

struct GroupCikKeyHash {
    size_t operator()(const GroupCikKey& k) const {
        uint64_t h = static_cast<uint32_t>(k.sic);
        h ^= static_cast<uint64_t>(k.tag) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.version) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.stmt) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(static_cast<uint32_t>(k.cik)) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct FinalKey {
    int32_t sic;
    uint32_t tlabel;
    uint8_t stmt;

    bool operator==(const FinalKey& o) const {
        return sic == o.sic && tlabel == o.tlabel && stmt == o.stmt;
    }
};

struct FinalKeyHash {
    size_t operator()(const FinalKey& k) const {
        uint64_t h = static_cast<uint32_t>(k.sic);
        h ^= static_cast<uint64_t>(k.tlabel) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.stmt) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

struct FinalGroupCikKey {
    int32_t sic;
    uint32_t tlabel;
    uint8_t stmt;
    int32_t cik;

    bool operator==(const FinalGroupCikKey& o) const {
        return sic == o.sic && tlabel == o.tlabel && stmt == o.stmt && cik == o.cik;
    }
};

struct FinalGroupCikKeyHash {
    size_t operator()(const FinalGroupCikKey& k) const {
        uint64_t h = static_cast<uint32_t>(k.sic);
        h ^= static_cast<uint64_t>(k.tlabel) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.stmt) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(static_cast<uint32_t>(k.cik)) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
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
        uint64_t mid = lo + ((hi - lo) >> 1);
        uint32_t mk = entries[mid].key;
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

        gendb::MmapColumn<uint32_t> tag_tag;
        gendb::MmapColumn<uint32_t> tag_version;
        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_tlabel;

        gendb::MmapColumn<uint8_t> pre_stmt_posting_raw;
        gendb::MmapColumn<uint8_t> num_adsh_posting_raw;

        uint64_t max_adsh = 0;
        std::vector<PostingEntry> pre_stmt_entries;
        const uint32_t* pre_stmt_rowids = nullptr;
        uint64_t pre_stmt_entry_count = 0;

        std::vector<PostingEntry> num_adsh_entries;
        const uint32_t* num_adsh_rowids = nullptr;

        std::vector<uint8_t> filtered_adsh_bitset;
        std::vector<uint32_t> filtered_adsh_list;
        std::vector<int32_t> sic_by_adsh;
        std::vector<int32_t> cik_by_adsh;
        std::vector<uint64_t> num_start_by_adsh;
        std::vector<uint32_t> num_count_by_adsh;

        std::unordered_map<TripleKey, uint32_t, TripleKeyHash> pre_triple_mult;

        std::unordered_map<AggKey, AggState, AggKeyHash> stage1_agg;
        std::unordered_set<GroupCikKey, GroupCikKeyHash> stage1_group_cik;

        std::unordered_map<uint64_t, uint32_t> tag_to_tlabel;
        std::unordered_map<FinalKey, AggState, FinalKeyHash> final_agg;
        std::unordered_set<FinalGroupCikKey, FinalGroupCikKeyHash> final_group_cik;

        GENDB_PHASE("total");

        {
            GENDB_PHASE("data_loading");

            uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
            stmt_dict = load_dict_file(gendb_dir + "/dicts/stmt.dict");
            tlabel_dict = load_dict_file(gendb_dir + "/tag/tlabel.dict");

            usd_code = static_cast<uint16_t>(find_dict_code(uom_dict, "USD"));
            uint32_t eq_code_u32 = find_dict_code(stmt_dict, "EQ");
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

            tag_tag.open(gendb_dir + "/tag/tag.bin");
            tag_version.open(gendb_dir + "/tag/version.bin");
            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");

            pre_stmt_posting_raw.open(gendb_dir + "/pre/indexes/pre_stmt_hash.bin");
            num_adsh_posting_raw.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");

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
            if (tag_tag.size() != tag_version.size() || tag_tag.size() != tag_abstract.size() ||
                tag_tag.size() != tag_tlabel.size()) {
                throw std::runtime_error("tag column size mismatch");
            }

            for (size_t i = 0; i < sub_adsh.size(); ++i) {
                if (sub_adsh[i] > max_adsh) max_adsh = sub_adsh[i];
            }
            ++max_adsh;

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
                uint64_t entry_count = 0;
                uint64_t rowid_count = 0;
                std::memcpy(&entry_count, p, sizeof(uint64_t));
                std::memcpy(&rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

                const size_t stride = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
                const size_t entries_bytes = static_cast<size_t>(entry_count) * stride;
                const size_t entries_off = sizeof(uint64_t) * 2;
                const size_t rowids_off = entries_off + entries_bytes;
                const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
                if (num_adsh_posting_raw.file_size < rowids_off + rowids_bytes) {
                    throw std::runtime_error("num_adsh_fk_hash.bin short payload");
                }

                num_adsh_entries.resize(static_cast<size_t>(entry_count));
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

            filtered_adsh_bitset.assign(static_cast<size_t>(max_adsh), 0);
            sic_by_adsh.assign(static_cast<size_t>(max_adsh), 0);
            cik_by_adsh.assign(static_cast<size_t>(max_adsh), 0);
            num_start_by_adsh.assign(static_cast<size_t>(max_adsh), 0);
            num_count_by_adsh.assign(static_cast<size_t>(max_adsh), 0);

            for (const PostingEntry& e : num_adsh_entries) {
                if (e.key >= max_adsh) continue;
                num_start_by_adsh[e.key] = e.start;
                num_count_by_adsh[e.key] = e.count;
            }

            gendb::mmap_prefetch_all(sub_adsh, sub_sic, sub_cik,
                                     pre_adsh, pre_tag, pre_version, pre_stmt,
                                     num_tag, num_version, num_uom, num_value,
                                     tag_tag, tag_version, tag_abstract, tag_tlabel,
                                     pre_stmt_posting_raw, num_adsh_posting_raw);
        }

        {
            GENDB_PHASE("dim_filter");

            if (kSicMin != kSicMinAnchor) {
                throw std::runtime_error("sic anchor mismatch");
            }

            filtered_adsh_list.reserve(8192);
            for (size_t i = 0; i < sub_adsh.size(); ++i) {
                uint32_t adsh = sub_adsh[i];
                if (adsh >= max_adsh) continue;
                int32_t sic = sub_sic[i];
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

            const PostingEntry* eq_posting = posting_lookup(
                pre_stmt_entries.data(), pre_stmt_entry_count, static_cast<uint32_t>(eq_code_u8));
            bool use_pre_stmt_posting = (eq_posting != nullptr &&
                                         eq_posting->count >= kPreStmtThresholdAnchor &&
                                         eq_posting->count <= pre_stmt.size());

            if (use_pre_stmt_posting) {
                pre_triple_mult.reserve(static_cast<size_t>(eq_posting->count * 1.2));
                for (uint64_t i = 0; i < eq_posting->count; ++i) {
                    uint32_t pre_rowid = pre_stmt_rowids[eq_posting->start + i];
                    uint32_t adsh = pre_adsh[pre_rowid];
                    if (adsh >= max_adsh || !filtered_adsh_bitset[adsh]) continue;
                    ++pre_triple_mult[TripleKey{adsh, pre_tag[pre_rowid], pre_version[pre_rowid]}];
                }
            } else {
                (void)kPreStmtThresholdAnchor;
                pre_triple_mult.reserve(120000);
                for (size_t pre_rowid = 0; pre_rowid < pre_stmt.size(); ++pre_rowid) {
                    if (pre_stmt[pre_rowid] != eq_code_u8) continue;
                    uint32_t adsh = pre_adsh[pre_rowid];
                    if (adsh >= max_adsh || !filtered_adsh_bitset[adsh]) continue;
                    ++pre_triple_mult[TripleKey{adsh, pre_tag[pre_rowid], pre_version[pre_rowid]}];
                }
            }

            num_tag.advise_random();
            num_version.advise_random();
            num_uom.advise_random();
            num_value.advise_random();
        }

        {
            GENDB_PHASE("main_scan");

            int thread_count = std::max(1, omp_get_max_threads());
            std::vector<std::unordered_map<AggKey, AggState, AggKeyHash>> local_aggs(static_cast<size_t>(thread_count));
            std::vector<std::unordered_set<GroupCikKey, GroupCikKeyHash>> local_group_ciks(static_cast<size_t>(thread_count));

#pragma omp parallel num_threads(thread_count)
            {
                int tid = omp_get_thread_num();
                auto& local_agg = local_aggs[static_cast<size_t>(tid)];
                auto& local_group_cik = local_group_ciks[static_cast<size_t>(tid)];
                local_agg.reserve(2048);
                local_group_cik.reserve(2048);

                std::unordered_map<uint64_t, AggState> adsh_local;
                adsh_local.reserve(128);

#pragma omp for schedule(dynamic, 16)
                for (size_t i = 0; i < filtered_adsh_list.size(); ++i) {
                    uint32_t adsh = filtered_adsh_list[i];
                    uint32_t ncnt = num_count_by_adsh[adsh];
                    if (ncnt == 0) continue;
                    uint64_t nstart = num_start_by_adsh[adsh];

                    adsh_local.clear();
                    adsh_local.reserve(static_cast<size_t>(ncnt / 2 + 8));

                    for (uint64_t p = nstart, pend = nstart + ncnt; p < pend; ++p) {
                        uint32_t num_rowid = num_adsh_rowids[p];
                        if (num_uom[num_rowid] != usd_code) continue;
                        double v = num_value[num_rowid];
                        if (std::isnan(v)) continue;

                        uint64_t tv = pack_tag_version(num_tag[num_rowid], num_version[num_rowid]);
                        AggState& s = adsh_local[tv];
                        s.sum += v;
                        s.count += 1;
                    }

                    if (adsh_local.empty()) continue;

                    int32_t sic = sic_by_adsh[adsh];
                    int32_t cik = cik_by_adsh[adsh];

                    for (const auto& kv : adsh_local) {
                        uint32_t tag = static_cast<uint32_t>(kv.first >> 32);
                        uint32_t version = static_cast<uint32_t>(kv.first & 0xffffffffu);

                        auto it_pre = pre_triple_mult.find(TripleKey{adsh, tag, version});
                        if (it_pre == pre_triple_mult.end()) continue;
                        uint32_t pre_mult = it_pre->second;

                        AggKey k{sic, tag, version, eq_code_u8};
                        AggState& dst = local_agg[k];
                        dst.sum += kv.second.sum * static_cast<double>(pre_mult);
                        dst.count += kv.second.count * static_cast<uint64_t>(pre_mult);

                        local_group_cik.insert(GroupCikKey{sic, tag, version, eq_code_u8, cik});
                    }
                }
            }

            size_t agg_reserve = 0;
            size_t pair_reserve = 0;
            for (int t = 0; t < thread_count; ++t) {
                agg_reserve += local_aggs[static_cast<size_t>(t)].size();
                pair_reserve += local_group_ciks[static_cast<size_t>(t)].size();
            }
            stage1_agg.reserve(agg_reserve);
            stage1_group_cik.reserve(pair_reserve);

            for (int t = 0; t < thread_count; ++t) {
                auto& la = local_aggs[static_cast<size_t>(t)];
                for (auto& kv : la) {
                    AggState& dst = stage1_agg[kv.first];
                    dst.sum += kv.second.sum;
                    dst.count += kv.second.count;
                }

                auto& ls = local_group_ciks[static_cast<size_t>(t)];
                for (const auto& x : ls) {
                    stage1_group_cik.insert(x);
                }
            }
        }

        {
            GENDB_PHASE("resolve_tag_tlabel_sequential");

            std::unordered_set<uint64_t> needed_tag_versions;
            needed_tag_versions.reserve(stage1_agg.size());
            for (const auto& kv : stage1_agg) {
                needed_tag_versions.insert(pack_tag_version(kv.first.tag, kv.first.version));
            }

            if (!needed_tag_versions.empty()) {
                tag_to_tlabel.reserve(needed_tag_versions.size());
                for (size_t rowid = 0; rowid < tag_tag.size(); ++rowid) {
                    if (tag_abstract[rowid] != 0) continue;
                    uint64_t tv = pack_tag_version(tag_tag[rowid], tag_version[rowid]);
                    if (needed_tag_versions.find(tv) == needed_tag_versions.end()) continue;
                    tag_to_tlabel.emplace(tv, tag_tlabel[rowid]);
                    if (tag_to_tlabel.size() == needed_tag_versions.size()) break;
                }
            }
        }

        {
            GENDB_PHASE("final_aggregate_having");

            final_agg.reserve(stage1_agg.size());
            for (const auto& kv : stage1_agg) {
                uint64_t tv = pack_tag_version(kv.first.tag, kv.first.version);
                auto it_t = tag_to_tlabel.find(tv);
                if (it_t == tag_to_tlabel.end()) continue;

                FinalKey fk{kv.first.sic, it_t->second, kv.first.stmt};
                AggState& dst = final_agg[fk];
                dst.sum += kv.second.sum;
                dst.count += kv.second.count;
            }

            final_group_cik.reserve(stage1_group_cik.size());
            for (const auto& p : stage1_group_cik) {
                uint64_t tv = pack_tag_version(p.tag, p.version);
                auto it_t = tag_to_tlabel.find(tv);
                if (it_t == tag_to_tlabel.end()) continue;
                final_group_cik.insert(FinalGroupCikKey{p.sic, it_t->second, p.stmt, p.cik});
            }
        }

        std::vector<ResultRow> rows;
        {
            GENDB_PHASE("topk");

            std::unordered_map<FinalKey, uint64_t, FinalKeyHash> distinct_counts;
            distinct_counts.reserve(final_agg.size());
            for (const auto& p : final_group_cik) {
                ++distinct_counts[FinalKey{p.sic, p.tlabel, p.stmt}];
            }

            rows.reserve(final_agg.size());
            for (const auto& kv : final_agg) {
                auto it = distinct_counts.find(kv.first);
                uint64_t num_companies = (it == distinct_counts.end()) ? 0ULL : it->second;
                if (num_companies < 2) continue;
                if (kv.second.count == 0) continue;
                rows.push_back(ResultRow{kv.first.sic,
                                         kv.first.tlabel,
                                         kv.first.stmt,
                                         num_companies,
                                         kv.second.sum,
                                         kv.second.sum / static_cast<double>(kv.second.count)});
            }

            std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
                if (a.total_value != b.total_value) return a.total_value > b.total_value;
                if (a.sic != b.sic) return a.sic < b.sic;
                if (a.tlabel != b.tlabel) return a.tlabel < b.tlabel;
                return a.stmt < b.stmt;
            });

            if (rows.size() > 500) rows.resize(500);
        }

        {
            GENDB_PHASE("output");

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
