#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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
        const uint8_t* end = raw.data + raw.size();

        uint32_t n = 0;
        std::memcpy(&n, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

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

    void open(const std::string& path) {
        raw.open(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) * 2 > end) {
            throw std::runtime_error("posting index too small: " + path);
        }

        uint64_t entry_count = 0;
        uint64_t rowid_count = 0;
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

struct FilteredSubRow {
    uint32_t adsh;
    uint32_t name_id;
};

struct FilteredNumRow {
    uint32_t adsh;
    uint32_t tag;
    uint32_t name_id;
    double value;
};

struct Candidate {
    uint32_t name_id;
    uint32_t tag_id;
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

        DictView uom_dict;
        DictView tag_dict;
        DictView name_dict;
        PostingIndexU32 num_adsh_fk_hash;
        ZoneMapI32 sub_fy_zonemap;

        gendb::MmapColumn<uint32_t> num_adsh_col;
        gendb::MmapColumn<uint32_t> num_tag_col;
        gendb::MmapColumn<uint16_t> num_uom_col;
        gendb::MmapColumn<double> num_value_col;

        gendb::MmapColumn<uint32_t> sub_adsh_col;
        gendb::MmapColumn<int32_t> sub_fy_col;
        gendb::MmapColumn<uint32_t> sub_name_col;

        uint16_t pure_code = std::numeric_limits<uint16_t>::max();

        {
            GENDB_PHASE("data_loading");

            uom_dict.open(gendb_dir + "/dicts/uom.dict");
            tag_dict.open(gendb_dir + "/dicts/tag.dict");
            name_dict.open(gendb_dir + "/sub/name.dict");

            for (size_t i = 0; i < uom_dict.values.size(); ++i) {
                if (uom_dict.values[i] == "pure") {
                    pure_code = static_cast<uint16_t>(i);
                    break;
                }
            }
            if (pure_code == std::numeric_limits<uint16_t>::max()) {
                throw std::runtime_error("'pure' not found in dicts/uom.dict");
            }

            num_adsh_fk_hash.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");
            sub_fy_zonemap.load(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin");

            num_adsh_col.open(gendb_dir + "/num/adsh.bin");
            num_tag_col.open(gendb_dir + "/num/tag.bin");
            num_uom_col.open(gendb_dir + "/num/uom.bin");
            num_value_col.open(gendb_dir + "/num/value.bin");

            sub_adsh_col.open(gendb_dir + "/sub/adsh.bin");
            sub_fy_col.open(gendb_dir + "/sub/fy.bin");
            sub_name_col.open(gendb_dir + "/sub/name.bin");

            const size_t num_rows = num_adsh_col.size();
            if (num_tag_col.size() != num_rows || num_uom_col.size() != num_rows || num_value_col.size() != num_rows) {
                throw std::runtime_error("num column length mismatch");
            }
            const size_t sub_rows = sub_adsh_col.size();
            if (sub_fy_col.size() != sub_rows || sub_name_col.size() != sub_rows) {
                throw std::runtime_error("sub column length mismatch");
            }
        }

        std::vector<FilteredSubRow> filtered_sub;
        {
            GENDB_PHASE("dim_filter");
            const uint32_t* sub_adsh = sub_adsh_col.data;
            const int32_t* sub_fy = sub_fy_col.data;
            const uint32_t* sub_name = sub_name_col.data;
            const size_t sub_rows = sub_adsh_col.size();

            const uint64_t block_size = sub_fy_zonemap.block_size;
            const auto& blocks = sub_fy_zonemap.blocks;
            filtered_sub.reserve(sub_rows / 4);

            for (size_t b = 0; b < blocks.size(); ++b) {
                const auto& z = blocks[b];
                if (z.min_v > 2022 || z.max_v < 2022) continue;
                const size_t start = b * static_cast<size_t>(block_size);
                const size_t end = std::min(start + static_cast<size_t>(block_size), sub_rows);
                for (size_t r = start; r < end; ++r) {
                    if (sub_fy[r] == 2022) {
                        filtered_sub.push_back({sub_adsh[r], sub_name[r]});
                    }
                }
            }
        }

        std::vector<FilteredNumRow> filtered_num;
        std::unordered_map<uint64_t, double, KeyHash> max_map;

        {
            GENDB_PHASE("build_joins");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::unordered_map<uint64_t, double, KeyHash>> local_max(static_cast<size_t>(nthreads));
            std::vector<std::vector<FilteredNumRow>> local_rows(static_cast<size_t>(nthreads));

            const uint32_t* num_adsh = num_adsh_col.data;
            const uint32_t* num_tag = num_tag_col.data;
            const uint16_t* num_uom = num_uom_col.data;
            const double* num_value = num_value_col.data;

            for (auto& m : local_max) {
                m.reserve(2048);
            }
            for (auto& v : local_rows) {
                v.reserve(256);
            }

            const uint32_t sub_count = static_cast<uint32_t>(filtered_sub.size());

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& lm = local_max[static_cast<size_t>(tid)];
                auto& lr = local_rows[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 128)
                for (uint32_t i = 0; i < sub_count; ++i) {
                    const FilteredSubRow& s = filtered_sub[static_cast<size_t>(i)];
                    const auto post = num_adsh_fk_hash.find(s.adsh);
                    const uint32_t* rowids = post.first;
                    const uint32_t count = post.second;
                    if (!rowids || count == 0) continue;

                    for (uint32_t j = 0; j < count; ++j) {
                        const uint32_t rid = rowids[j];
                        if (num_adsh[rid] != s.adsh) continue;
                        if (num_uom[rid] != pure_code) continue;

                        const double v = num_value[rid];
                        if (std::isnan(v)) continue;

                        const uint32_t tag = num_tag[rid];
                        const uint64_t k = pack_key(s.adsh, tag);

                        auto it = lm.find(k);
                        if (it == lm.end()) {
                            lm.emplace(k, v);
                        } else if (v > it->second) {
                            it->second = v;
                        }

                        lr.push_back({s.adsh, tag, s.name_id, v});
                    }
                }
            }

            size_t total_local_keys = 0;
            size_t total_rows = 0;
            for (const auto& m : local_max) total_local_keys += m.size();
            for (const auto& v : local_rows) total_rows += v.size();

            max_map.reserve(total_local_keys + 1024);
            for (const auto& lm : local_max) {
                for (const auto& kv : lm) {
                    auto it = max_map.find(kv.first);
                    if (it == max_map.end()) {
                        max_map.emplace(kv.first, kv.second);
                    } else if (kv.second > it->second) {
                        it->second = kv.second;
                    }
                }
            }

            filtered_num.reserve(total_rows);
            for (auto& v : local_rows) {
                filtered_num.insert(filtered_num.end(), v.begin(), v.end());
            }
        }

        std::vector<Candidate> candidates;
        {
            GENDB_PHASE("main_scan");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::vector<Candidate>> local_out(static_cast<size_t>(nthreads));
            for (auto& out : local_out) {
                out.reserve(128);
            }

            const uint32_t n = static_cast<uint32_t>(filtered_num.size());
            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& out = local_out[static_cast<size_t>(tid)];

                #pragma omp for schedule(static)
                for (uint32_t i = 0; i < n; ++i) {
                    const FilteredNumRow& r = filtered_num[static_cast<size_t>(i)];
                    const uint64_t k = pack_key(r.adsh, r.tag);
                    auto it = max_map.find(k);
                    if (it == max_map.end()) continue;
                    if (r.value != it->second) continue;
                    out.push_back({r.name_id, r.tag, r.value});
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

            auto cmp = [&](const Candidate& a, const Candidate& b) {
                if (a.value != b.value) return a.value > b.value;
                const std::string_view an = (a.name_id < name_dict.values.size()) ? name_dict.values[a.name_id] : std::string_view{};
                const std::string_view bn = (b.name_id < name_dict.values.size()) ? name_dict.values[b.name_id] : std::string_view{};
                if (an != bn) return an < bn;
                const std::string_view at = (a.tag_id < tag_dict.values.size()) ? tag_dict.values[a.tag_id] : std::string_view{};
                const std::string_view bt = (b.tag_id < tag_dict.values.size()) ? tag_dict.values[b.tag_id] : std::string_view{};
                return at < bt;
            };

            if (candidates.size() > kTopK) {
                std::nth_element(candidates.begin(), candidates.begin() + kTopK, candidates.end(), cmp);
                candidates.resize(kTopK);
            }
            std::sort(candidates.begin(), candidates.end(), cmp);

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q2.csv";
            FILE* f = std::fopen(out_path.c_str(), "w");
            if (!f) {
                throw std::runtime_error("failed to open output: " + out_path);
            }

            std::fprintf(f, "name,tag,value\n");
            for (const auto& r : candidates) {
                const std::string_view name = (r.name_id < name_dict.values.size()) ? name_dict.values[r.name_id] : std::string_view{};
                const std::string_view tag = (r.tag_id < tag_dict.values.size()) ? tag_dict.values[r.tag_id] : std::string_view{};
                csv_write_escaped(f, name);
                std::fputc(',', f);
                csv_write_escaped(f, tag);
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
