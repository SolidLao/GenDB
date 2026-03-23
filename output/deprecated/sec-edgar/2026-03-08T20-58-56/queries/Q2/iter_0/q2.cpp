#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

constexpr uint32_t kSentinelRow = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kTopK = 100;

struct PostingEntryU32 {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct ZoneMinMaxI32 {
    int32_t min_v;
    int32_t max_v;
};

struct DictView {
    gendb::MmapColumn<uint8_t> raw;
    std::vector<std::string_view> values;

    void open(const std::string& path) {
        raw.open(path);
        if (raw.size() < sizeof(uint32_t)) {
            throw std::runtime_error("dict too small: " + path);
        }
        const uint8_t* p = raw.data;
        uint32_t n = 0;
        std::memcpy(&n, p, sizeof(uint32_t));
        p += sizeof(uint32_t);
        const uint8_t* end = raw.data + raw.size();
        values.clear();
        values.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            if (p + sizeof(uint32_t) > end) {
                throw std::runtime_error("dict truncated (len): " + path);
            }
            uint32_t len = 0;
            std::memcpy(&len, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            if (p + len > end) {
                throw std::runtime_error("dict truncated (payload): " + path);
            }
            values.emplace_back(reinterpret_cast<const char*>(p), len);
            p += len;
        }
    }
};

struct PostingIndexU32 {
    gendb::MmapColumn<uint8_t> raw;
    std::vector<PostingEntryU32> entries;
    const uint32_t* rowids = nullptr;
    uint64_t rowid_count = 0;

    void open(const std::string& path) {
        raw.open(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) * 2 > end) {
            throw std::runtime_error("posting index too small: " + path);
        }

        uint64_t entry_count = 0;
        std::memcpy(&entry_count, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        std::memcpy(&rowid_count, p, sizeof(uint64_t));
        p += sizeof(uint64_t);

        entries.clear();
        entries.reserve(static_cast<size_t>(entry_count));
        for (uint64_t i = 0; i < entry_count; ++i) {
            if (p + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) > end) {
                throw std::runtime_error("posting entry truncated: " + path);
            }
            PostingEntryU32 e{};
            std::memcpy(&e.key, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            std::memcpy(&e.start, p, sizeof(uint64_t));
            p += sizeof(uint64_t);
            std::memcpy(&e.count, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            entries.push_back(e);
        }

        const size_t expect_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        if (p + expect_bytes > end) {
            throw std::runtime_error("posting rowids truncated: " + path);
        }
        rowids = reinterpret_cast<const uint32_t*>(p);
    }

    std::pair<const uint32_t*, uint32_t> find(uint32_t key) const {
        size_t lo = 0;
        size_t hi = entries.size();
        while (lo < hi) {
            const size_t mid = lo + ((hi - lo) >> 1);
            if (entries[mid].key < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= entries.size() || entries[lo].key != key) {
            return {nullptr, 0};
        }
        const PostingEntryU32& e = entries[lo];
        return {rowids + e.start, e.count};
    }
};

struct DenseSubAdshPk {
    gendb::MmapColumn<uint8_t> raw;
    const uint32_t* lut = nullptr;
    uint64_t lut_size = 0;

    void open(const std::string& path) {
        raw.open(path);
        if (raw.size() < sizeof(uint64_t)) {
            throw std::runtime_error("dense pk too small: " + path);
        }
        std::memcpy(&lut_size, raw.data, sizeof(uint64_t));
        const size_t need = sizeof(uint64_t) + static_cast<size_t>(lut_size) * sizeof(uint32_t);
        if (need > raw.size()) {
            throw std::runtime_error("dense pk truncated: " + path);
        }
        lut = reinterpret_cast<const uint32_t*>(raw.data + sizeof(uint64_t));
    }

    inline uint32_t lookup(uint32_t adsh) const {
        if (adsh >= lut_size) return kSentinelRow;
        return lut[adsh];
    }
};

struct ZoneMapI32 {
    uint64_t block_size = 0;
    std::vector<ZoneMinMaxI32> blocks;

    void load(const std::string& path) {
        gendb::MmapColumn<uint8_t> raw(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) * 2 > end) {
            throw std::runtime_error("zonemap too small: " + path);
        }
        uint64_t nblocks = 0;
        std::memcpy(&block_size, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        std::memcpy(&nblocks, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        blocks.resize(static_cast<size_t>(nblocks));
        for (uint64_t i = 0; i < nblocks; ++i) {
            if (p + sizeof(int32_t) * 2 > end) {
                throw std::runtime_error("zonemap truncated: " + path);
            }
            std::memcpy(&blocks[static_cast<size_t>(i)].min_v, p, sizeof(int32_t));
            p += sizeof(int32_t);
            std::memcpy(&blocks[static_cast<size_t>(i)].max_v, p, sizeof(int32_t));
            p += sizeof(int32_t);
        }
    }
};

inline uint64_t pack_key(uint32_t adsh, uint32_t tag) {
    return (static_cast<uint64_t>(adsh) << 32) | static_cast<uint64_t>(tag);
}

struct KeyHash {
    size_t operator()(uint64_t x) const noexcept {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return static_cast<size_t>(x);
    }
};

struct Candidate {
    uint32_t name_id;
    uint32_t tag_id;
    double value;
};

struct RowOut {
    std::string_view name;
    std::string_view tag;
    double value;
};

inline void csv_write_escaped(FILE* f, std::string_view s) {
    bool quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            quote = true;
            break;
        }
    }
    if (!quote) {
        std::fwrite(s.data(), 1, s.size(), f);
        return;
    }
    std::fputc('"', f);
    for (char c : s) {
        if (c == '"') std::fputc('"', f);
        std::fputc(c, f);
    }
    std::fputc('"', f);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];

        GENDB_PHASE("total");

        // Loaded assets
        DictView uom_dict;
        DictView tag_dict;
        DictView name_dict;
        PostingIndexU32 num_uom_hash;
        DenseSubAdshPk sub_adsh_pk;
        ZoneMapI32 sub_fy_zonemap;

        gendb::MmapColumn<uint32_t> num_adsh_col;
        gendb::MmapColumn<uint32_t> num_tag_col;
        gendb::MmapColumn<uint16_t> num_uom_col;
        gendb::MmapColumn<double> num_value_col;
        gendb::MmapColumn<int32_t> sub_fy_col;
        gendb::MmapColumn<uint32_t> sub_name_col;

        uint16_t pure_code = std::numeric_limits<uint16_t>::max();
        std::vector<uint32_t> pure_rowids;

        {
            GENDB_PHASE("data_loading");

            uom_dict.open(gendb_dir + "/dicts/uom.dict");
            tag_dict.open(gendb_dir + "/dicts/tag.dict");
            name_dict.open(gendb_dir + "/sub/name.dict");
            num_uom_hash.open(gendb_dir + "/num/indexes/num_uom_hash.bin");
            sub_adsh_pk.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
            sub_fy_zonemap.load(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin");

            for (size_t i = 0; i < uom_dict.values.size(); ++i) {
                if (uom_dict.values[i] == "pure") {
                    pure_code = static_cast<uint16_t>(i);
                    break;
                }
            }
            if (pure_code == std::numeric_limits<uint16_t>::max()) {
                throw std::runtime_error("'pure' not found in dicts/uom.dict");
            }

            num_adsh_col.open(gendb_dir + "/num/adsh.bin");
            num_tag_col.open(gendb_dir + "/num/tag.bin");
            num_uom_col.open(gendb_dir + "/num/uom.bin");
            num_value_col.open(gendb_dir + "/num/value.bin");
            sub_fy_col.open(gendb_dir + "/sub/fy.bin");
            sub_name_col.open(gendb_dir + "/sub/name.bin");

            const size_t num_rows = num_adsh_col.size();
            if (num_tag_col.size() != num_rows || num_uom_col.size() != num_rows || num_value_col.size() != num_rows) {
                throw std::runtime_error("num column length mismatch");
            }
            if (sub_name_col.size() != sub_fy_col.size()) {
                throw std::runtime_error("sub column length mismatch");
            }

            // num_uom_hash is built by reading uom.bin as uint32_t words:
            // key[i].low16=uom[2*i], key[i].high16=uom[2*i+1], posting rowid=i.
            // Reconstruct exact pure rowids from low/high half-word matches.
            const uint32_t half_rows = static_cast<uint32_t>(num_rows / 2);
            pure_rowids.reserve(std::max<size_t>(65536, num_rows / 32));
            for (const auto& e : num_uom_hash.entries) {
                const uint16_t lo = static_cast<uint16_t>(e.key & 0xFFFFu);
                const uint16_t hi = static_cast<uint16_t>((e.key >> 16) & 0xFFFFu);
                const bool lo_match = (lo == pure_code);
                const bool hi_match = (hi == pure_code);
                if (!lo_match && !hi_match) continue;
                const uint32_t* ids = num_uom_hash.rowids + e.start;
                for (uint32_t i = 0; i < e.count; ++i) {
                    const uint32_t word_idx = ids[i];
                    if (word_idx >= half_rows) continue;
                    const uint32_t base = word_idx * 2u;
                    if (lo_match) pure_rowids.push_back(base);
                    if (hi_match && static_cast<size_t>(base + 1u) < num_rows) pure_rowids.push_back(base + 1u);
                }
            }
            std::sort(pure_rowids.begin(), pure_rowids.end());
            pure_rowids.erase(std::unique(pure_rowids.begin(), pure_rowids.end()), pure_rowids.end());
        }

        const size_t sub_rows = sub_fy_col.size();
        std::vector<uint8_t> sub_fy2022(sub_rows, 0);

        {
            GENDB_PHASE("dim_filter");
            const uint64_t block_size = sub_fy_zonemap.block_size;
            const auto& blocks = sub_fy_zonemap.blocks;
            for (size_t b = 0; b < blocks.size(); ++b) {
                const auto& z = blocks[b];
                if (z.min_v > 2022 || z.max_v < 2022) continue;
                const size_t start = b * static_cast<size_t>(block_size);
                const size_t end = std::min(start + static_cast<size_t>(block_size), sub_rows);
                for (size_t r = start; r < end; ++r) {
                    if (sub_fy_col[r] == 2022) sub_fy2022[r] = 1;
                }
            }
        }

        const uint32_t pure_count = static_cast<uint32_t>(pure_rowids.size());

        std::unordered_map<uint64_t, double, KeyHash> max_map;
        {
            GENDB_PHASE("build_joins");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::unordered_map<uint64_t, double, KeyHash>> locals(static_cast<size_t>(nthreads));
            for (auto& m : locals) {
                m.reserve(std::max<size_t>(4096, static_cast<size_t>(pure_count / nthreads) * 2));
            }

            const uint32_t* num_adsh = num_adsh_col.data;
            const uint32_t* num_tag = num_tag_col.data;
            const uint16_t* num_uom = num_uom_col.data;
            const double* num_value = num_value_col.data;

            #pragma omp parallel
            {
                auto& local = locals[static_cast<size_t>(omp_get_thread_num())];
                #pragma omp for schedule(dynamic, 4096)
                for (uint32_t i = 0; i < pure_count; ++i) {
                    const uint32_t rid = pure_rowids[static_cast<size_t>(i)];
                    if (num_uom[rid] != pure_code) continue;
                    const double v = num_value[rid];
                    if (std::isnan(v)) continue;
                    const uint64_t k = pack_key(num_adsh[rid], num_tag[rid]);
                    auto it = local.find(k);
                    if (it == local.end()) {
                        local.emplace(k, v);
                    } else if (v > it->second) {
                        it->second = v;
                    }
                }
            }

            size_t expect = 0;
            for (const auto& m : locals) expect += m.size();
            max_map.reserve(expect + 1024);
            for (const auto& lm : locals) {
                for (const auto& kv : lm) {
                    auto it = max_map.find(kv.first);
                    if (it == max_map.end()) {
                        max_map.emplace(kv.first, kv.second);
                    } else if (kv.second > it->second) {
                        it->second = kv.second;
                    }
                }
            }
        }

        std::vector<Candidate> candidates;
        {
            GENDB_PHASE("main_scan");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::vector<Candidate>> local_out(static_cast<size_t>(nthreads));
            for (auto& v : local_out) {
                v.reserve(std::max<size_t>(256, pure_count / (nthreads * 8)));
            }

            const uint32_t* num_adsh = num_adsh_col.data;
            const uint32_t* num_tag = num_tag_col.data;
            const uint16_t* num_uom = num_uom_col.data;
            const double* num_value = num_value_col.data;
            const uint32_t* sub_name = sub_name_col.data;

            #pragma omp parallel
            {
                auto& out = local_out[static_cast<size_t>(omp_get_thread_num())];
                #pragma omp for schedule(dynamic, 4096)
                for (uint32_t i = 0; i < pure_count; ++i) {
                    const uint32_t rid = pure_rowids[static_cast<size_t>(i)];
                    if (num_uom[rid] != pure_code) continue;
                    const double v = num_value[rid];
                    if (std::isnan(v)) continue;

                    const uint32_t adsh = num_adsh[rid];
                    const uint32_t tag = num_tag[rid];
                    const uint64_t k = pack_key(adsh, tag);
                    auto it = max_map.find(k);
                    if (it == max_map.end()) continue;
                    if (v != it->second) continue;

                    const uint32_t sub_row = sub_adsh_pk.lookup(adsh);
                    if (sub_row == kSentinelRow) continue;
                    if (!sub_fy2022[sub_row]) continue;

                    out.push_back({sub_name[sub_row], tag, v});
                }
            }

            size_t total = 0;
            for (const auto& v : local_out) total += v.size();
            candidates.reserve(total);
            for (auto& v : local_out) {
                candidates.insert(candidates.end(), v.begin(), v.end());
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<RowOut> rows;
            rows.reserve(candidates.size());
            for (const auto& c : candidates) {
                if (c.name_id >= name_dict.values.size()) continue;
                if (c.tag_id >= tag_dict.values.size()) continue;
                rows.push_back({name_dict.values[c.name_id], tag_dict.values[c.tag_id], c.value});
            }

            auto cmp = [](const RowOut& a, const RowOut& b) {
                if (a.value != b.value) return a.value > b.value;
                if (a.name != b.name) return a.name < b.name;
                return a.tag < b.tag;
            };
            if (rows.size() > kTopK) {
                std::nth_element(rows.begin(), rows.begin() + kTopK, rows.end(), cmp);
                rows.resize(kTopK);
            }
            std::sort(rows.begin(), rows.end(), cmp);

            const std::string out_path = results_dir + "/Q2.csv";
            FILE* f = std::fopen(out_path.c_str(), "w");
            if (!f) {
                throw std::runtime_error("failed to open output: " + out_path);
            }
            std::fprintf(f, "name,tag,value\n");
            for (const auto& r : rows) {
                csv_write_escaped(f, r.name);
                std::fputc(',', f);
                csv_write_escaped(f, r.tag);
                std::fprintf(f, ",%.2f\n", r.value);
            }
            std::fclose(f);
        }

    } catch (const std::exception& e) {
        std::fprintf(stderr, "ERROR: %s\n", e.what());
        return 1;
    }

    return 0;
}
