#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
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

constexpr size_t kTopK = 100;
constexpr uint32_t kSubPkSentinel = std::numeric_limits<uint32_t>::max();

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
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint32_t) > end) {
            throw std::runtime_error("dictionary too small: " + path);
        }

        uint32_t n = 0;
        std::memcpy(&n, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        values.clear();
        values.reserve(static_cast<size_t>(n));
        for (uint32_t i = 0; i < n; ++i) {
            if (p + sizeof(uint32_t) > end) {
                throw std::runtime_error("dictionary truncated len: " + path);
            }
            uint32_t len = 0;
            std::memcpy(&len, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            if (p + len > end) {
                throw std::runtime_error("dictionary truncated payload: " + path);
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

        entries.resize(static_cast<size_t>(entry_count));
        for (size_t i = 0; i < entries.size(); ++i) {
            if (p + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) > end) {
                throw std::runtime_error("posting index truncated entries: " + path);
            }
            std::memcpy(&entries[i].key, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            std::memcpy(&entries[i].start, p, sizeof(uint64_t));
            p += sizeof(uint64_t);
            std::memcpy(&entries[i].count, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
        }

        const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        if (p + rowids_bytes > end) {
            throw std::runtime_error("posting index truncated rowids: " + path);
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

struct DensePkIndexU32 {
    gendb::MmapColumn<uint8_t> raw;
    uint64_t lut_size = 0;
    const uint32_t* lut = nullptr;

    void open(const std::string& path) {
        raw.open(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) > end) {
            throw std::runtime_error("pk index too small: " + path);
        }
        std::memcpy(&lut_size, p, sizeof(uint64_t));
        p += sizeof(uint64_t);

        const size_t lut_bytes = static_cast<size_t>(lut_size) * sizeof(uint32_t);
        if (p + lut_bytes > end) {
            throw std::runtime_error("pk index truncated: " + path);
        }
        lut = reinterpret_cast<const uint32_t*>(p);
    }

    uint32_t lookup(uint32_t key) const {
        if (key >= lut_size) return kSubPkSentinel;
        return lut[key];
    }
};

struct ZoneMapI32 {
    gendb::MmapColumn<uint8_t> raw;
    uint64_t block_size = 0;
    std::vector<ZoneMinMaxI32> blocks;

    void open(const std::string& path) {
        raw.open(path);
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
        for (size_t i = 0; i < blocks.size(); ++i) {
            if (p + sizeof(int32_t) * 2 > end) {
                throw std::runtime_error("zonemap truncated: " + path);
            }
            std::memcpy(&blocks[i].min_v, p, sizeof(int32_t));
            p += sizeof(int32_t);
            std::memcpy(&blocks[i].max_v, p, sizeof(int32_t));
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
        DensePkIndexU32 sub_adsh_pk_hash;
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
            sub_adsh_pk_hash.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
            sub_fy_zonemap.open(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin");

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

        std::vector<uint8_t> sub_selected;
        std::vector<uint32_t> filtered_distinct_adsh;

        {
            GENDB_PHASE("dim_filter");
            const uint32_t* sub_adsh = sub_adsh_col.data;
            const int32_t* sub_fy = sub_fy_col.data;
            const size_t sub_rows = sub_adsh_col.size();

            sub_selected.assign(sub_rows, 0);
            filtered_distinct_adsh.reserve(sub_rows / 3);

            const uint64_t block_size = sub_fy_zonemap.block_size;
            if (block_size == 0) {
                throw std::runtime_error("invalid sub_fy zonemap block size 0");
            }

            for (size_t b = 0; b < sub_fy_zonemap.blocks.size(); ++b) {
                const ZoneMinMaxI32& z = sub_fy_zonemap.blocks[b];
                if (z.min_v > 2022 || z.max_v < 2022) {
                    continue;
                }
                const size_t start = b * static_cast<size_t>(block_size);
                const size_t end = std::min(start + static_cast<size_t>(block_size), sub_rows);
                for (size_t r = start; r < end; ++r) {
                    if (sub_fy[r] == 2022) {
                        sub_selected[r] = 1;
                        filtered_distinct_adsh.push_back(sub_adsh[r]);
                    }
                }
            }

            std::sort(filtered_distinct_adsh.begin(), filtered_distinct_adsh.end());
            filtered_distinct_adsh.erase(
                std::unique(filtered_distinct_adsh.begin(), filtered_distinct_adsh.end()),
                filtered_distinct_adsh.end());
        }

        std::unordered_map<uint64_t, double, KeyHash> max_map;

        {
            GENDB_PHASE("build_joins");
            const int nthreads = std::max(1, omp_get_max_threads());
            std::vector<std::unordered_map<uint64_t, double, KeyHash>> local_maps(static_cast<size_t>(nthreads));
            for (auto& lm : local_maps) {
                lm.reserve(4096);
            }

            const uint32_t* num_tag = num_tag_col.data;
            const uint16_t* num_uom = num_uom_col.data;
            const double* num_value = num_value_col.data;

            const uint32_t* adsh_keys = filtered_distinct_adsh.data();
            const size_t adsh_count = filtered_distinct_adsh.size();

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& lm = local_maps[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 256)
                for (size_t i = 0; i < adsh_count; ++i) {
                    const uint32_t adsh = adsh_keys[i];
                    const auto posting = num_adsh_fk_hash.find(adsh);
                    const uint32_t* rowids = posting.first;
                    const uint32_t count = posting.second;
                    if (!rowids || count == 0) continue;

                    for (uint32_t j = 0; j < count; ++j) {
                        const uint32_t rid = rowids[j];
                        if (num_uom[rid] != pure_code) continue;
                        const double v = num_value[rid];
                        if (std::isnan(v)) continue;

                        const uint64_t k = pack_key(adsh, num_tag[rid]);
                        auto it = lm.find(k);
                        if (it == lm.end()) {
                            lm.emplace(k, v);
                        } else if (v > it->second) {
                            it->second = v;
                        }
                    }
                }
            }

            size_t total_keys = 0;
            for (const auto& lm : local_maps) {
                total_keys += lm.size();
            }
            max_map.reserve(total_keys + 1024);

            for (const auto& lm : local_maps) {
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
            for (auto& out : local_out) {
                out.reserve(256);
            }

            const uint32_t* num_tag = num_tag_col.data;
            const uint16_t* num_uom = num_uom_col.data;
            const double* num_value = num_value_col.data;
            const uint32_t* sub_name = sub_name_col.data;
            const size_t sub_rows = sub_adsh_col.size();

            const uint32_t* adsh_keys = filtered_distinct_adsh.data();
            const size_t adsh_count = filtered_distinct_adsh.size();

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& out = local_out[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 256)
                for (size_t i = 0; i < adsh_count; ++i) {
                    const uint32_t adsh = adsh_keys[i];
                    const uint32_t sub_row = sub_adsh_pk_hash.lookup(adsh);
                    if (sub_row == kSubPkSentinel || sub_row >= sub_rows || !sub_selected[sub_row]) {
                        continue;
                    }
                    const uint32_t name_id = sub_name[sub_row];

                    const auto posting = num_adsh_fk_hash.find(adsh);
                    const uint32_t* rowids = posting.first;
                    const uint32_t count = posting.second;
                    if (!rowids || count == 0) continue;

                    for (uint32_t j = 0; j < count; ++j) {
                        const uint32_t rid = rowids[j];
                        if (num_uom[rid] != pure_code) continue;
                        const double v = num_value[rid];
                        if (std::isnan(v)) continue;

                        const uint32_t tag_id = num_tag[rid];
                        const uint64_t k = pack_key(adsh, tag_id);
                        auto it = max_map.find(k);
                        if (it == max_map.end()) continue;
                        if (v != it->second) continue;

                        out.push_back({name_id, tag_id, v});
                    }
                }
            }

            size_t total = 0;
            for (const auto& out : local_out) {
                total += out.size();
            }
            candidates.reserve(total);
            for (auto& out : local_out) {
                candidates.insert(candidates.end(), out.begin(), out.end());
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
                throw std::runtime_error("failed to open output file: " + out_path);
            }

            std::fprintf(f, "name,tag,value\n");
            for (const Candidate& c : candidates) {
                const std::string_view name = (c.name_id < name_dict.values.size()) ? name_dict.values[c.name_id] : std::string_view{};
                const std::string_view tag = (c.tag_id < tag_dict.values.size()) ? tag_dict.values[c.tag_id] : std::string_view{};
                csv_write_escaped(f, name);
                std::fputc(',', f);
                csv_write_escaped(f, tag);
                std::fprintf(f, ",%.2f\n", c.value);
            }
            std::fclose(f);
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ERROR: %s\n", e.what());
        return 1;
    }

    return 0;
}
