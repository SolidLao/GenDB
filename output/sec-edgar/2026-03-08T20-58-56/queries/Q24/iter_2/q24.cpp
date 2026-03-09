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

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

#pragma pack(push, 1)
struct TripleEntry {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t start;
    uint32_t count;
};
#pragma pack(pop)

static_assert(sizeof(TripleEntry) == 24, "TripleEntry must match index layout");

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
        if (!in) throw std::runtime_error("Cannot read dict entry length: " + path);

        std::string s(len, '\0');
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("Cannot read dict entry bytes: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

static int32_t find_code(const std::vector<std::string>& dict, const char* needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == needle) return static_cast<int32_t>(i);
    }
    return -1;
}

static inline int cmp_key_entry(const TripleKey& key, const TripleEntry& e) {
    if (key.a != e.a) return (key.a < e.a) ? -1 : 1;
    if (key.b != e.b) return (key.b < e.b) ? -1 : 1;
    if (key.c != e.c) return (key.c < e.c) ? -1 : 1;
    return 0;
}

static inline bool pre_index_exists(const TripleEntry* entries, uint64_t n_entries,
                                    uint32_t adsh, uint32_t tag, uint32_t version) {
    const TripleKey key{adsh, tag, version};
    uint64_t lo = 0;
    uint64_t hi = n_entries;
    while (lo < hi) {
        const uint64_t mid = lo + ((hi - lo) >> 1);
        const int c = cmp_key_entry(key, entries[mid]);
        if (c == 0) return true;
        if (c < 0) hi = mid;
        else lo = mid + 1;
    }
    return false;
}

static inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
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

    const int nthreads = std::max(1, omp_get_max_threads());

    std::vector<std::string> tag_dict;
    std::vector<std::string> version_dict;
    int32_t usd_code = -1;

    gendb::MmapColumn<int32_t> num_ddate;
    gendb::MmapColumn<uint16_t> num_uom;
    gendb::MmapColumn<double> num_value;

    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint32_t> num_tag;
    gendb::MmapColumn<uint32_t> num_version;

    gendb::MmapColumn<uint8_t> zm_file;
    uint64_t zm_block_size = 0;
    uint64_t zm_blocks = 0;
    const int32_t* zm_minmax = nullptr;

    gendb::MmapColumn<uint8_t> pre_idx_file;
    uint64_t pre_entry_count = 0;
    const TripleEntry* pre_entries = nullptr;

    std::vector<uint32_t> candidate_blocks;
    std::vector<uint32_t> candidates;
    gendb::CompactHashMap<uint64_t, AggVal> global_agg;

    GENDB_PHASE("total");

    {
        GENDB_PHASE("data_loading");

        num_ddate.open(gendb_dir + "/num/ddate.bin");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");

        if (num_ddate.size() != num_uom.size() || num_ddate.size() != num_value.size()) {
            throw std::runtime_error("num base column size mismatch");
        }

        tag_dict = load_dict_file(gendb_dir + "/dicts/tag.dict");
        version_dict = load_dict_file(gendb_dir + "/dicts/version.dict");
        const std::vector<std::string> uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
        usd_code = find_code(uom_dict, "USD");
        if (usd_code < 0 || usd_code > 65535) {
            throw std::runtime_error("USD not found in uom dict");
        }

        zm_file.open(gendb_dir + "/num/indexes/num_ddate_zonemap.bin");
        if (zm_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("num_ddate_zonemap.bin too small");
        }
        const uint8_t* z = zm_file.data;
        std::memcpy(&zm_block_size, z, sizeof(uint64_t));
        std::memcpy(&zm_blocks, z + sizeof(uint64_t), sizeof(uint64_t));
        if (zm_block_size == 0) {
            throw std::runtime_error("zonemap block size is zero");
        }
        const size_t expect = sizeof(uint64_t) * 2 + static_cast<size_t>(zm_blocks) * 2 * sizeof(int32_t);
        if (zm_file.file_size < expect) {
            throw std::runtime_error("num_ddate_zonemap.bin truncated");
        }
        zm_minmax = reinterpret_cast<const int32_t*>(z + sizeof(uint64_t) * 2);

        num_ddate.prefetch();
        num_uom.prefetch();
        num_value.prefetch();
        zm_file.prefetch();
    }

    {
        GENDB_PHASE("dim_filter");

        candidate_blocks.clear();
        candidate_blocks.reserve(static_cast<size_t>(zm_blocks));
        for (uint64_t b = 0; b < zm_blocks; ++b) {
            const int32_t mn = zm_minmax[2 * b];
            const int32_t mx = zm_minmax[2 * b + 1];
            if (mx < DDATE_LO || mn > DDATE_HI) continue;
            candidate_blocks.push_back(static_cast<uint32_t>(b));
        }
    }

    {
        GENDB_PHASE("main_scan");

        const int32_t* __restrict ddate = num_ddate.data;
        const uint16_t* __restrict uom = num_uom.data;
        const double* __restrict value = num_value.data;

        const size_t total_rows = num_ddate.size();
        const size_t block_size = static_cast<size_t>(zm_block_size);
        const uint16_t usd_u16 = static_cast<uint16_t>(usd_code);

        std::vector<std::vector<uint32_t>> local_candidates(static_cast<size_t>(nthreads));

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            auto& lc = local_candidates[static_cast<size_t>(tid)];
            lc.reserve(1024);

            #pragma omp for schedule(dynamic, 1)
            for (size_t bi = 0; bi < candidate_blocks.size(); ++bi) {
                const size_t block = static_cast<size_t>(candidate_blocks[bi]);
                const size_t row_lo = block * block_size;
                const size_t row_hi = std::min(row_lo + block_size, total_rows);
                for (size_t i = row_lo; i < row_hi; ++i) {
                    const int32_t d = ddate[i];
                    if (d < DDATE_LO || d > DDATE_HI) continue;
                    if (uom[i] != usd_u16) continue;
                    if (std::isnan(value[i])) continue;
                    lc.push_back(static_cast<uint32_t>(i));
                }
            }
        }

        size_t total_candidates = 0;
        for (const auto& lc : local_candidates) total_candidates += lc.size();
        candidates.clear();
        candidates.reserve(total_candidates);
        for (auto& lc : local_candidates) {
            candidates.insert(candidates.end(), lc.begin(), lc.end());
            std::vector<uint32_t>().swap(lc);
        }
    }

    {
        GENDB_PHASE("build_joins");

        if (!candidates.empty()) {
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");

            if (num_adsh.size() != num_ddate.size() || num_tag.size() != num_ddate.size() ||
                num_version.size() != num_ddate.size()) {
                throw std::runtime_error("num join-key column size mismatch");
            }

            pre_idx_file.open(gendb_dir + "/pre/indexes/pre_adsh_tag_version_hash.bin");
            if (pre_idx_file.file_size < sizeof(uint64_t) * 2) {
                throw std::runtime_error("pre_adsh_tag_version_hash.bin too small");
            }

            const uint8_t* p = pre_idx_file.data;
            uint64_t pre_rowid_count = 0;
            std::memcpy(&pre_entry_count, p, sizeof(uint64_t));
            std::memcpy(&pre_rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));
            (void)pre_rowid_count;

            const size_t entry_bytes = static_cast<size_t>(pre_entry_count) * sizeof(TripleEntry);
            if (pre_idx_file.file_size < sizeof(uint64_t) * 2 + entry_bytes) {
                throw std::runtime_error("pre_adsh_tag_version_hash entries truncated");
            }
            pre_entries = reinterpret_cast<const TripleEntry*>(p + sizeof(uint64_t) * 2);

            num_adsh.advise_sequential();
            num_tag.advise_sequential();
            num_version.advise_sequential();
            pre_idx_file.advise_random();

            num_adsh.prefetch();
            num_tag.prefetch();
            num_version.prefetch();
            pre_idx_file.prefetch();
        }
    }

    {
        GENDB_PHASE("output");

        if (!candidates.empty()) {
            const uint32_t* __restrict adsh = num_adsh.data;
            const uint32_t* __restrict tag = num_tag.data;
            const uint32_t* __restrict version = num_version.data;
            const double* __restrict value = num_value.data;

            std::vector<gendb::CompactHashMap<uint64_t, AggVal>> local_aggs(static_cast<size_t>(nthreads));
            const size_t local_expect = std::max<size_t>(1024, candidates.size() / static_cast<size_t>(nthreads * 8));
            for (int t = 0; t < nthreads; ++t) {
                local_aggs[static_cast<size_t>(t)].reserve(local_expect);
            }

            #pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                auto& agg = local_aggs[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 16384)
                for (size_t i = 0; i < candidates.size(); ++i) {
                    const uint32_t rid = candidates[i];
                    const uint32_t a = adsh[rid];
                    const uint32_t t = tag[rid];
                    const uint32_t v = version[rid];

                    if (pre_entry_count > 0 && pre_index_exists(pre_entries, pre_entry_count, a, t, v)) {
                        continue;
                    }

                    AggVal& dst = agg[pack_tag_version(t, v)];
                    dst.cnt += 1;
                    dst.total += value[rid];
                }
            }

            global_agg.reserve(std::max<size_t>(2048, candidates.size() / 16));
            for (int t = 0; t < nthreads; ++t) {
                for (auto it : local_aggs[static_cast<size_t>(t)]) {
                    AggVal& dst = global_agg[it.first];
                    dst.cnt += it.second.cnt;
                    dst.total += it.second.total;
                }
            }
        }

        std::vector<ResultRow> rows;
        rows.reserve(global_agg.size());
        for (auto it : global_agg) {
            if (it.second.cnt <= 10) continue;
            rows.push_back(ResultRow{
                static_cast<uint32_t>(it.first >> 32),
                static_cast<uint32_t>(it.first & 0xFFFFFFFFULL),
                it.second.cnt,
                it.second.total
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

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q24.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) throw std::runtime_error("Cannot open output file: " + out_path);

        std::fprintf(out, "tag,version,cnt,total\n");
        for (const auto& r : rows) {
            const std::string& tag_s = (r.tag < tag_dict.size()) ? tag_dict[r.tag] : std::string();
            const std::string& ver_s = (r.version < version_dict.size()) ? version_dict[r.version] : std::string();
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
