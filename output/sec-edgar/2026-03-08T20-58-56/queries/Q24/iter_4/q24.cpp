#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

#pragma pack(push, 1)
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
#pragma pack(pop)

static_assert(sizeof(PostingEntry) == 16, "PostingEntry layout mismatch");
static_assert(sizeof(TripleEntry) == 24, "TripleEntry layout mismatch");

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct AggVal {
    uint64_t cnt;
    double total;
};

struct ResultRow {
    uint32_t tag;
    uint32_t version;
    uint64_t cnt;
    double total;
};

static std::vector<std::string> load_dict_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Cannot open dict file: " + path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) throw std::runtime_error("Cannot read dict size: " + path);

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) throw std::runtime_error("Cannot read dict length: " + path);
        std::string s(len, '\0');
        if (len) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("Cannot read dict bytes: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

static std::vector<std::string> load_dict_subset(const std::string& path,
                                                 const std::vector<uint32_t>& needed_codes) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Cannot open dict file: " + path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) throw std::runtime_error("Cannot read dict size: " + path);

    std::vector<std::string> out(needed_codes.size());
    if (needed_codes.empty()) return out;

    size_t need_i = 0;
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) throw std::runtime_error("Cannot read dict length: " + path);

        if (need_i < needed_codes.size() && needed_codes[need_i] == i) {
            std::string s(len, '\0');
            if (len) {
                in.read(&s[0], static_cast<std::streamsize>(len));
                if (!in) throw std::runtime_error("Cannot read dict bytes: " + path);
            }
            out[need_i] = std::move(s);
            ++need_i;
            if (need_i == needed_codes.size()) break;
        } else if (len) {
            in.seekg(static_cast<std::streamoff>(len), std::ios::cur);
        }
    }
    return out;
}

static int32_t find_code(const std::vector<std::string>& dict, const char* needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == needle) return static_cast<int32_t>(i);
    }
    return -1;
}

static inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

static inline int cmp_posting_key(uint32_t key, const PostingEntry& e) {
    if (key < e.key) return -1;
    if (key > e.key) return 1;
    return 0;
}

static inline const PostingEntry* find_posting_entry(const PostingEntry* entries,
                                                     uint64_t n_entries,
                                                     uint32_t key) {
    uint64_t lo = 0;
    uint64_t hi = n_entries;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const int c = cmp_posting_key(key, entries[mid]);
        if (c == 0) return &entries[mid];
        if (c < 0) hi = mid;
        else lo = mid + 1;
    }
    return nullptr;
}

static inline int cmp_triple_key(const TripleKey& key, const TripleEntry& e) {
    if (key.a != e.a) return key.a < e.a ? -1 : 1;
    if (key.b != e.b) return key.b < e.b ? -1 : 1;
    if (key.c != e.c) return key.c < e.c ? -1 : 1;
    return 0;
}

static inline bool pre_index_exists(const TripleEntry* entries,
                                    uint64_t n_entries,
                                    uint32_t adsh,
                                    uint32_t tag,
                                    uint32_t version) {
    const TripleKey key{adsh, tag, version};
    uint64_t lo = 0;
    uint64_t hi = n_entries;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const int c = cmp_triple_key(key, entries[mid]);
        if (c == 0) return true;
        if (c < 0) hi = mid;
        else lo = mid + 1;
    }
    return false;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    constexpr int32_t DDATE_LO = 20230101;
    constexpr int32_t DDATE_HI = 20231231;

    const int nthreads = std::max(1, std::min(omp_get_num_procs(), 16));

    uint64_t zm_block_size = 0;
    uint64_t zm_blocks = 0;
    const int32_t* zm_minmax = nullptr;

    std::vector<uint8_t> candidate_blocks;
    std::vector<uint32_t> usd_block_rowids;

    gendb::MmapColumn<int32_t> num_ddate;
    gendb::MmapColumn<double> num_value;
    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint32_t> num_tag;
    gendb::MmapColumn<uint32_t> num_version;

    const TripleEntry* pre_entries = nullptr;
    uint64_t pre_entry_count = 0;

    gendb::MmapColumn<uint8_t> zm_file;
    gendb::MmapColumn<uint8_t> uom_idx_file;
    gendb::MmapColumn<uint8_t> pre_idx_file;

    gendb::CompactHashMap<uint64_t, AggVal> global_agg;

    GENDB_PHASE("total");

    {
        GENDB_PHASE("data_loading");

        zm_file.open(gendb_dir + "/num/indexes/num_ddate_zonemap.bin");
        if (zm_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("num_ddate_zonemap.bin too small");
        }
        const uint8_t* z = zm_file.data;
        std::memcpy(&zm_block_size, z, sizeof(uint64_t));
        std::memcpy(&zm_blocks, z + sizeof(uint64_t), sizeof(uint64_t));
        if (zm_block_size == 0) throw std::runtime_error("zonemap block_size is zero");
        const size_t zm_expect = sizeof(uint64_t) * 2 + static_cast<size_t>(zm_blocks) * 2 * sizeof(int32_t);
        if (zm_file.file_size < zm_expect) throw std::runtime_error("num_ddate_zonemap.bin truncated");
        zm_minmax = reinterpret_cast<const int32_t*>(z + sizeof(uint64_t) * 2);

        uom_idx_file.open(gendb_dir + "/num/indexes/num_uom_hash.bin");
        if (uom_idx_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("num_uom_hash.bin too small");
        }
    }

    bool has_candidates = false;
    std::vector<uint32_t> usd_rowids;
    {
        GENDB_PHASE("dim_filter");

        candidate_blocks.assign(static_cast<size_t>(zm_blocks), 0);
        for (uint64_t b = 0; b < zm_blocks; ++b) {
            const int32_t mn = zm_minmax[2 * b];
            const int32_t mx = zm_minmax[2 * b + 1];
            if (mx < DDATE_LO || mn > DDATE_HI) continue;
            candidate_blocks[static_cast<size_t>(b)] = 1;
        }

        const std::vector<std::string> uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
        const int32_t usd_code_i32 = find_code(uom_dict, "USD");
        if (usd_code_i32 < 0) {
            throw std::runtime_error("USD not found in uom.dict");
        }
        const uint32_t usd_code = static_cast<uint32_t>(usd_code_i32);

        const uint8_t* p = uom_idx_file.data;
        uint64_t entry_count = 0;
        uint64_t rowid_count = 0;
        std::memcpy(&entry_count, p, sizeof(uint64_t));
        std::memcpy(&rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

        const size_t entries_bytes = static_cast<size_t>(entry_count) * sizeof(PostingEntry);
        const size_t rowids_off = sizeof(uint64_t) * 2 + entries_bytes;
        const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        if (uom_idx_file.file_size < rowids_off + rowids_bytes) {
            throw std::runtime_error("num_uom_hash.bin truncated");
        }

        const PostingEntry* entries = reinterpret_cast<const PostingEntry*>(p + sizeof(uint64_t) * 2);
        const uint32_t* rowids = reinterpret_cast<const uint32_t*>(p + rowids_off);

        const PostingEntry* usd_entry = find_posting_entry(entries, entry_count, usd_code);
        if (usd_entry != nullptr && usd_entry->count > 0) {
            const uint64_t start = usd_entry->start;
            const uint64_t end = start + usd_entry->count;
            if (end > rowid_count) {
                throw std::runtime_error("num_uom_hash.bin posting range out of bounds");
            }

            usd_rowids.assign(rowids + start, rowids + end);
            usd_block_rowids.clear();
            usd_block_rowids.reserve(usd_rowids.size());

            #pragma omp parallel num_threads(nthreads)
            {
                std::vector<uint32_t> local;
                local.reserve(1024);

                #pragma omp for schedule(static)
                for (size_t i = 0; i < usd_rowids.size(); ++i) {
                    const uint32_t rid = usd_rowids[i];
                    const uint64_t block = static_cast<uint64_t>(rid) / zm_block_size;
                    if (block < zm_blocks && candidate_blocks[static_cast<size_t>(block)] != 0) {
                        local.push_back(rid);
                    }
                }

                size_t total_local = local.size();
                size_t offset = 0;
                #pragma omp critical
                {
                    offset = usd_block_rowids.size();
                    usd_block_rowids.resize(offset + total_local);
                }
                if (total_local > 0) {
                    std::memcpy(usd_block_rowids.data() + offset,
                                local.data(),
                                total_local * sizeof(uint32_t));
                }
            }

            has_candidates = !usd_block_rowids.empty();
        }
    }

    if (has_candidates) {
        GENDB_PHASE("build_joins");

        num_ddate.open(gendb_dir + "/num/ddate.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_tag.open(gendb_dir + "/num/tag.bin");
        num_version.open(gendb_dir + "/num/version.bin");

        const size_t nrows = num_ddate.size();
        if (num_value.size() != nrows ||
            num_adsh.size() != nrows ||
            num_tag.size() != nrows ||
            num_version.size() != nrows) {
            throw std::runtime_error("num column size mismatch");
        }

        pre_idx_file.open(gendb_dir + "/pre/indexes/pre_adsh_tag_version_hash.bin");
        if (pre_idx_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("pre_adsh_tag_version_hash.bin too small");
        }

        const uint8_t* p = pre_idx_file.data;
        uint64_t pre_rowid_count = 0;
        std::memcpy(&pre_entry_count, p, sizeof(uint64_t));
        std::memcpy(&pre_rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));

        const size_t pre_entries_bytes = static_cast<size_t>(pre_entry_count) * sizeof(TripleEntry);
        const size_t pre_rowids_off = sizeof(uint64_t) * 2 + pre_entries_bytes;
        const size_t pre_rowids_bytes = static_cast<size_t>(pre_rowid_count) * sizeof(uint32_t);
        if (pre_idx_file.file_size < pre_rowids_off + pre_rowids_bytes) {
            throw std::runtime_error("pre_adsh_tag_version_hash.bin truncated");
        }
        pre_entries = reinterpret_cast<const TripleEntry*>(p + sizeof(uint64_t) * 2);
        pre_idx_file.advise_random();

        global_agg.reserve(std::max<size_t>(1024, usd_block_rowids.size() / 16));
    }

    {
        GENDB_PHASE("main_scan");

        if (has_candidates) {
            const int32_t* __restrict ddate = num_ddate.data;
            const double* __restrict value = num_value.data;
            const uint32_t* __restrict adsh = num_adsh.data;
            const uint32_t* __restrict tag = num_tag.data;
            const uint32_t* __restrict version = num_version.data;

            std::vector<gendb::CompactHashMap<uint64_t, AggVal>> local_aggs(static_cast<size_t>(nthreads));
            const size_t local_reserve = std::max<size_t>(256, usd_block_rowids.size() / static_cast<size_t>(nthreads * 8));
            for (int t = 0; t < nthreads; ++t) {
                local_aggs[static_cast<size_t>(t)].reserve(local_reserve);
            }

            #pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                auto& agg = local_aggs[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 16384)
                for (size_t i = 0; i < usd_block_rowids.size(); ++i) {
                    const uint32_t rid = usd_block_rowids[i];
                    const int32_t d = ddate[rid];
                    if (d < DDATE_LO || d > DDATE_HI) continue;

                    const double v = value[rid];
                    if (std::isnan(v)) continue;

                    const uint32_t a = adsh[rid];
                    const uint32_t t = tag[rid];
                    const uint32_t ver = version[rid];
                    if (pre_entry_count > 0 && pre_index_exists(pre_entries, pre_entry_count, a, t, ver)) {
                        continue;
                    }

                    AggVal& dst = agg[pack_tag_version(t, ver)];
                    dst.cnt += 1;
                    dst.total += v;
                }
            }

            for (int t = 0; t < nthreads; ++t) {
                for (auto kv : local_aggs[static_cast<size_t>(t)]) {
                    AggVal& dst = global_agg[kv.first];
                    dst.cnt += kv.second.cnt;
                    dst.total += kv.second.total;
                }
            }
        }
    }

    {
        GENDB_PHASE("output");

        std::vector<ResultRow> rows;
        rows.reserve(global_agg.size());
        for (auto kv : global_agg) {
            if (kv.second.cnt <= 10) continue;
            rows.push_back(ResultRow{
                static_cast<uint32_t>(kv.first >> 32),
                static_cast<uint32_t>(kv.first & 0xFFFFFFFFULL),
                kv.second.cnt,
                kv.second.total
            });
        }

        auto cmp = [](const ResultRow& x, const ResultRow& y) {
            if (x.cnt != y.cnt) return x.cnt > y.cnt;
            if (x.tag != y.tag) return x.tag < y.tag;
            return x.version < y.version;
        };

        if (rows.size() > 100) {
            std::partial_sort(rows.begin(), rows.begin() + 100, rows.end(), cmp);
            rows.resize(100);
        } else {
            std::sort(rows.begin(), rows.end(), cmp);
        }

        std::vector<uint32_t> tag_codes;
        std::vector<uint32_t> version_codes;
        tag_codes.reserve(rows.size());
        version_codes.reserve(rows.size());
        for (const auto& r : rows) {
            tag_codes.push_back(r.tag);
            version_codes.push_back(r.version);
        }
        std::sort(tag_codes.begin(), tag_codes.end());
        std::sort(version_codes.begin(), version_codes.end());
        tag_codes.erase(std::unique(tag_codes.begin(), tag_codes.end()), tag_codes.end());
        version_codes.erase(std::unique(version_codes.begin(), version_codes.end()), version_codes.end());

        const std::vector<std::string> tag_dict =
            load_dict_subset(gendb_dir + "/dicts/tag.dict", tag_codes);
        const std::vector<std::string> version_dict =
            load_dict_subset(gendb_dir + "/dicts/version.dict", version_codes);

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q24.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) throw std::runtime_error("Cannot open output file: " + out_path);

        std::fprintf(out, "tag,version,cnt,total\n");
        for (const auto& r : rows) {
            const auto tag_it = std::lower_bound(tag_codes.begin(), tag_codes.end(), r.tag);
            const auto ver_it = std::lower_bound(version_codes.begin(), version_codes.end(), r.version);
            const std::string tag_s =
                (tag_it != tag_codes.end() && *tag_it == r.tag)
                    ? tag_dict[static_cast<size_t>(tag_it - tag_codes.begin())]
                    : std::string();
            const std::string ver_s =
                (ver_it != version_codes.end() && *ver_it == r.version)
                    ? version_dict[static_cast<size_t>(ver_it - version_codes.begin())]
                    : std::string();

            std::fprintf(out, "%s,%s,%llu,%.2f\n",
                         tag_s.c_str(),
                         ver_s.c_str(),
                         static_cast<unsigned long long>(r.cnt),
                         r.total);
        }
        std::fclose(out);
    }

    return 0;
}
