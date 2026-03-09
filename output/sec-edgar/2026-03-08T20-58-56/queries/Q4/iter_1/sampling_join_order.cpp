#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mmap_utils.h"

namespace {

struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct TripleEntry {
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

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;

    bool operator==(const TripleKey& o) const {
        return a == o.a && b == o.b && c == o.c;
    }
};

struct TripleHash {
    size_t operator()(const TripleKey& k) const {
        uint64_t h = static_cast<uint64_t>(k.a) * 0x9E3779B97F4A7C15ULL;
        h ^= static_cast<uint64_t>(k.b) + 0x517CC1B727220A95ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.c) + 0x94D049BB133111EBULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

static std::vector<std::string> load_dict_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open dict file: " + path);
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) throw std::runtime_error("cannot read dict count: " + path);

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) throw std::runtime_error("cannot read dict len: " + path);
        std::string s(len, '\0');
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("cannot read dict payload: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

static uint32_t find_code(const std::vector<std::string>& dict, const std::string& value) {
    for (uint32_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == value) return i;
    }
    throw std::runtime_error("dictionary value not found: " + value);
}

static const PostingEntry* posting_lookup(const std::vector<PostingEntry>& entries, uint32_t key) {
    size_t lo = 0;
    size_t hi = entries.size();
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if (entries[mid].key == key) return &entries[mid];
        if (entries[mid].key < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return nullptr;
}

static const TripleEntry* triple_lookup(const std::vector<TripleEntry>& entries, const TripleKey& k) {
    size_t lo = 0;
    size_t hi = entries.size();
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        const TripleEntry& e = entries[mid];
        if (e.a == k.a && e.b == k.b && e.c == k.c) return &e;
        if (e.a < k.a || (e.a == k.a && (e.b < k.b || (e.b == k.b && e.c < k.c)))) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return nullptr;
}

static uint64_t pack_tv(uint32_t t, uint32_t v) {
    return (static_cast<uint64_t>(t) << 32) | static_cast<uint64_t>(v);
}

struct RunStats {
    double ms;
    uint64_t rows;
    uint64_t out_matches;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb = argv[1];

    try {
        const auto uom_dict = load_dict_file(gendb + "/dicts/uom.dict");
        const auto stmt_dict = load_dict_file(gendb + "/dicts/stmt.dict");
        const uint32_t usd_code = find_code(uom_dict, "USD");
        const uint32_t eq_code = find_code(stmt_dict, "EQ");

        gendb::MmapColumn<uint32_t> num_adsh(gendb + "/num/adsh.bin");
        gendb::MmapColumn<uint32_t> num_tag(gendb + "/num/tag.bin");
        gendb::MmapColumn<uint32_t> num_version(gendb + "/num/version.bin");
        gendb::MmapColumn<uint16_t> num_uom(gendb + "/num/uom.bin");
        gendb::MmapColumn<double> num_value(gendb + "/num/value.bin");

        gendb::MmapColumn<uint32_t> sub_adsh(gendb + "/sub/adsh.bin");
        gendb::MmapColumn<int32_t> sub_sic(gendb + "/sub/sic.bin");
        gendb::MmapColumn<uint32_t> sub_lut_raw(gendb + "/sub/indexes/sub_adsh_pk_hash.bin");

        gendb::MmapColumn<uint32_t> pre_adsh(gendb + "/pre/adsh.bin");
        gendb::MmapColumn<uint32_t> pre_tag(gendb + "/pre/tag.bin");
        gendb::MmapColumn<uint32_t> pre_version(gendb + "/pre/version.bin");

        gendb::MmapColumn<uint8_t> tag_abstract(gendb + "/tag/abstract.bin");

        gendb::MmapColumn<uint8_t> num_adsh_post_raw(gendb + "/num/indexes/num_adsh_fk_hash.bin");
        gendb::MmapColumn<uint8_t> num_triple_post_raw(gendb + "/num/indexes/num_adsh_tag_version_hash.bin");
        gendb::MmapColumn<uint8_t> pre_stmt_post_raw(gendb + "/pre/indexes/pre_stmt_hash.bin");
        gendb::MmapColumn<uint8_t> tag_pk_raw(gendb + "/tag/indexes/tag_tag_version_pk_hash.bin");

        const uint8_t* na = num_adsh_post_raw.data;
        uint64_t na_entry_count = 0, na_rowid_count = 0;
        std::memcpy(&na_entry_count, na, sizeof(uint64_t));
        std::memcpy(&na_rowid_count, na + sizeof(uint64_t), sizeof(uint64_t));
        std::vector<PostingEntry> num_adsh_entries(static_cast<size_t>(na_entry_count));
        const size_t na_off = sizeof(uint64_t) * 2;
        const uint8_t* na_ptr = na + na_off;
        for (size_t i = 0; i < num_adsh_entries.size(); ++i) {
            PostingEntry e{};
            std::memcpy(&e.key, na_ptr, sizeof(uint32_t)); na_ptr += sizeof(uint32_t);
            std::memcpy(&e.start, na_ptr, sizeof(uint64_t)); na_ptr += sizeof(uint64_t);
            std::memcpy(&e.count, na_ptr, sizeof(uint32_t)); na_ptr += sizeof(uint32_t);
            num_adsh_entries[i] = e;
        }
        const uint32_t* num_adsh_rowids = reinterpret_cast<const uint32_t*>(na + na_off + num_adsh_entries.size() * (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)));

        const uint8_t* nt = num_triple_post_raw.data;
        uint64_t nt_entry_count = 0, nt_rowid_count = 0;
        std::memcpy(&nt_entry_count, nt, sizeof(uint64_t));
        std::memcpy(&nt_rowid_count, nt + sizeof(uint64_t), sizeof(uint64_t));
        std::vector<TripleEntry> num_triple_entries(static_cast<size_t>(nt_entry_count));
        const size_t nt_off = sizeof(uint64_t) * 2;
        const uint8_t* nt_ptr = nt + nt_off;
        for (size_t i = 0; i < num_triple_entries.size(); ++i) {
            TripleEntry e{};
            std::memcpy(&e.a, nt_ptr, sizeof(uint32_t)); nt_ptr += sizeof(uint32_t);
            std::memcpy(&e.b, nt_ptr, sizeof(uint32_t)); nt_ptr += sizeof(uint32_t);
            std::memcpy(&e.c, nt_ptr, sizeof(uint32_t)); nt_ptr += sizeof(uint32_t);
            std::memcpy(&e.start, nt_ptr, sizeof(uint64_t)); nt_ptr += sizeof(uint64_t);
            std::memcpy(&e.count, nt_ptr, sizeof(uint32_t)); nt_ptr += sizeof(uint32_t);
            num_triple_entries[i] = e;
        }
        const uint32_t* num_triple_rowids = reinterpret_cast<const uint32_t*>(nt + nt_off + num_triple_entries.size() * (sizeof(uint32_t) * 3 + sizeof(uint64_t) + sizeof(uint32_t)));

        const uint8_t* ps = pre_stmt_post_raw.data;
        uint64_t ps_entry_count = 0, ps_rowid_count = 0;
        std::memcpy(&ps_entry_count, ps, sizeof(uint64_t));
        std::memcpy(&ps_rowid_count, ps + sizeof(uint64_t), sizeof(uint64_t));
        std::vector<PostingEntry> pre_stmt_entries(static_cast<size_t>(ps_entry_count));
        const size_t ps_off = sizeof(uint64_t) * 2;
        const uint8_t* ps_ptr = ps + ps_off;
        for (size_t i = 0; i < pre_stmt_entries.size(); ++i) {
            PostingEntry e{};
            std::memcpy(&e.key, ps_ptr, sizeof(uint32_t)); ps_ptr += sizeof(uint32_t);
            std::memcpy(&e.start, ps_ptr, sizeof(uint64_t)); ps_ptr += sizeof(uint64_t);
            std::memcpy(&e.count, ps_ptr, sizeof(uint32_t)); ps_ptr += sizeof(uint32_t);
            pre_stmt_entries[i] = e;
        }
        const uint32_t* pre_stmt_rowids = reinterpret_cast<const uint32_t*>(ps + ps_off + pre_stmt_entries.size() * (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)));

        const uint8_t* tp = tag_pk_raw.data;
        uint64_t tp_count = 0;
        std::memcpy(&tp_count, tp, sizeof(uint64_t));
        std::vector<TagPair> tag_pairs(static_cast<size_t>(tp_count));
        const uint8_t* tp_ptr = tp + sizeof(uint64_t);
        for (size_t i = 0; i < tag_pairs.size(); ++i) {
            std::memcpy(&tag_pairs[i].key, tp_ptr, sizeof(uint64_t)); tp_ptr += sizeof(uint64_t);
            std::memcpy(&tag_pairs[i].rowid, tp_ptr, sizeof(uint32_t)); tp_ptr += sizeof(uint32_t);
        }

        if (sub_lut_raw.size() < 2) throw std::runtime_error("sub_adsh_pk_hash malformed");
        const uint32_t* sub_lut = sub_lut_raw.data + 2;
        const uint64_t sub_lut_size = sub_lut_raw.data[0] | (static_cast<uint64_t>(sub_lut_raw.data[1]) << 32);

        const PostingEntry* eq_entry = posting_lookup(pre_stmt_entries, eq_code);
        if (!eq_entry) throw std::runtime_error("EQ entry not found in pre_stmt hash");

        std::unordered_map<uint64_t, uint32_t> tag_pk;
        tag_pk.reserve(tag_pairs.size() * 13 / 10 + 1);
        for (const auto& p : tag_pairs) tag_pk.emplace(p.key, p.rowid);

        std::vector<uint32_t> sub_filtered;
        sub_filtered.reserve(sub_adsh.size() / 10 + 1);
        for (size_t i = 0; i < sub_adsh.size(); ++i) {
            const int32_t sic = sub_sic[i];
            if (sic >= 4000 && sic <= 4999) sub_filtered.push_back(sub_adsh[i]);
        }

        std::unordered_map<TripleKey, uint32_t, TripleHash> pre_eq_count;
        pre_eq_count.reserve(static_cast<size_t>(eq_entry->count * 1.3));
        for (uint64_t i = 0; i < eq_entry->count; ++i) {
            const uint32_t r = pre_stmt_rowids[eq_entry->start + i];
            TripleKey k{pre_adsh[r], pre_tag[r], pre_version[r]};
            auto it = pre_eq_count.find(k);
            if (it == pre_eq_count.end()) pre_eq_count.emplace(k, 1u);
            else it->second += 1u;
        }

        auto check_sub = [&](uint32_t adsh, int32_t* sic_out) -> bool {
            if (adsh >= sub_lut_size) return false;
            const uint32_t rid = sub_lut[adsh];
            if (rid == std::numeric_limits<uint32_t>::max()) return false;
            const int32_t sic = sub_sic[rid];
            if (sic < 4000 || sic > 4999) return false;
            *sic_out = sic;
            return true;
        };

        constexpr uint64_t kMaxEvents = 3000000;

        const auto run_o1 = [&]() {
            const auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t out = 0;
            for (size_t i = 0; i < sub_filtered.size() && touched < kMaxEvents; ++i) {
                const PostingEntry* e = posting_lookup(num_adsh_entries, sub_filtered[i]);
                if (!e) continue;
                for (uint64_t p = e->start; p < e->start + e->count && touched < kMaxEvents; ++p) {
                    const uint32_t nr = num_adsh_rowids[p];
                    ++touched;
                    if (num_uom[nr] != static_cast<uint16_t>(usd_code)) continue;
                    const double v = num_value[nr];
                    if (std::isnan(v)) continue;
                    TripleKey tri{num_adsh[nr], num_tag[nr], num_version[nr]};
                    auto pit = pre_eq_count.find(tri);
                    if (pit == pre_eq_count.end()) continue;
                    auto tit = tag_pk.find(pack_tv(num_tag[nr], num_version[nr]));
                    if (tit == tag_pk.end()) continue;
                    if (tag_abstract[tit->second] != 0) continue;
                    out += pit->second;
                }
            }
            const auto t1 = std::chrono::steady_clock::now();
            return RunStats{std::chrono::duration<double, std::milli>(t1 - t0).count(), touched, out};
        };

        const auto run_o2 = [&]() {
            const auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t out = 0;
            for (uint64_t i = 0; i < eq_entry->count && touched < kMaxEvents; ++i) {
                const uint32_t pr = pre_stmt_rowids[eq_entry->start + i];
                TripleKey tri{pre_adsh[pr], pre_tag[pr], pre_version[pr]};
                const TripleEntry* e = triple_lookup(num_triple_entries, tri);
                if (!e) continue;
                for (uint64_t p = e->start; p < e->start + e->count && touched < kMaxEvents; ++p) {
                    const uint32_t nr = num_triple_rowids[p];
                    ++touched;
                    if (num_uom[nr] != static_cast<uint16_t>(usd_code)) continue;
                    const double v = num_value[nr];
                    if (std::isnan(v)) continue;
                    int32_t sic = 0;
                    if (!check_sub(num_adsh[nr], &sic)) continue;
                    auto tit = tag_pk.find(pack_tv(num_tag[nr], num_version[nr]));
                    if (tit == tag_pk.end()) continue;
                    if (tag_abstract[tit->second] != 0) continue;
                    (void)sic;
                    out += 1;
                }
            }
            const auto t1 = std::chrono::steady_clock::now();
            return RunStats{std::chrono::duration<double, std::milli>(t1 - t0).count(), touched, out};
        };

        const auto run_o3 = [&]() {
            const auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t out = 0;
            const size_t n = num_adsh.size();
            const size_t stride = std::max<size_t>(1, n / kMaxEvents);
            for (size_t nr = 0; nr < n && touched < kMaxEvents; nr += stride) {
                ++touched;
                if (num_uom[nr] != static_cast<uint16_t>(usd_code)) continue;
                const double v = num_value[nr];
                if (std::isnan(v)) continue;
                int32_t sic = 0;
                if (!check_sub(num_adsh[nr], &sic)) continue;
                TripleKey tri{num_adsh[nr], num_tag[nr], num_version[nr]};
                auto pit = pre_eq_count.find(tri);
                if (pit == pre_eq_count.end()) continue;
                auto tit = tag_pk.find(pack_tv(num_tag[nr], num_version[nr]));
                if (tit == tag_pk.end()) continue;
                if (tag_abstract[tit->second] != 0) continue;
                (void)sic;
                out += pit->second;
            }
            const auto t1 = std::chrono::steady_clock::now();
            return RunStats{std::chrono::duration<double, std::milli>(t1 - t0).count(), touched, out};
        };

        const RunStats o1 = run_o1();
        const RunStats o2 = run_o2();
        const RunStats o3 = run_o3();

        std::cout << "order_1_sub_num_pre_tag ms=" << o1.ms << " touched=" << o1.rows << " out=" << o1.out_matches << "\n";
        std::cout << "order_2_pre_num_sub_tag ms=" << o2.ms << " touched=" << o2.rows << " out=" << o2.out_matches << "\n";
        std::cout << "order_3_num_sub_pre_tag ms=" << o3.ms << " touched=" << o3.rows << " out=" << o3.out_matches << "\n";

        std::string best = "order_1_sub_num_pre_tag";
        double best_ms = o1.ms;
        if (o2.ms < best_ms) {
            best_ms = o2.ms;
            best = "order_2_pre_num_sub_tag";
        }
        if (o3.ms < best_ms) {
            best_ms = o3.ms;
            best = "order_3_num_sub_pre_tag";
        }

        std::cout << "best_order=" << best << " best_ms=" << best_ms << "\n";

    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }

    return 0;
}
