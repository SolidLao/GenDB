#include <algorithm>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
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

struct AggVal {
    uint64_t cnt;
    double total;
};
static_assert(sizeof(TripleEntry) == 24, "TripleEntry must match on-disk layout");

struct ResultRow {
    uint32_t tag;
    uint32_t version;
    uint64_t cnt;
    double total;
};

static std::vector<std::string> load_dict_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Cannot open dict file: " + path);
    }

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) {
        throw std::runtime_error("Cannot read dict size: " + path);
    }

    std::vector<std::string> values;
    values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) {
            throw std::runtime_error("Cannot read dict entry len: " + path);
        }
        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) {
                throw std::runtime_error("Cannot read dict entry bytes: " + path);
            }
        }
        values.push_back(std::move(s));
    }
    return values;
}

static int32_t find_uom_code(const std::vector<std::string>& uom_dict, const char* needle) {
    for (size_t i = 0; i < uom_dict.size(); ++i) {
        if (uom_dict[i] == needle) return static_cast<int32_t>(i);
    }
    return -1;
}

static inline int cmp_key_entry(const TripleKey& k, const TripleEntry& e) {
    if (k.a != e.a) return (k.a < e.a) ? -1 : 1;
    if (k.b != e.b) return (k.b < e.b) ? -1 : 1;
    if (k.c != e.c) return (k.c < e.c) ? -1 : 1;
    return 0;
}

static inline bool pre_index_exists(const TripleEntry* entries, uint64_t n_entries,
                                    uint32_t adsh, uint32_t tag, uint32_t version) {
    TripleKey key{adsh, tag, version};
    uint64_t lo = 0;
    uint64_t hi = n_entries;
    while (lo < hi) {
        uint64_t mid = lo + ((hi - lo) >> 1);
        const TripleEntry& e = entries[mid];
        const int c = cmp_key_entry(key, e);
        if (c == 0) return true;
        if (c < 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return false;
}

static inline uint64_t pack_tag_version(uint32_t tag, uint32_t version) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(version);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    constexpr int32_t DDATE_LO = 20230101;
    constexpr int32_t DDATE_HI = 20231231;
    constexpr double NAN_SENTINEL = std::numeric_limits<double>::quiet_NaN();
    (void)NAN_SENTINEL;

    const int nthreads = std::min(64, omp_get_max_threads());

    std::vector<std::string> tag_dict;
    std::vector<std::string> version_dict;
    int32_t usd_code = -1;

    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint32_t> num_tag;
    gendb::MmapColumn<uint32_t> num_version;
    gendb::MmapColumn<int32_t> num_ddate;
    gendb::MmapColumn<uint16_t> num_uom;
    gendb::MmapColumn<double> num_value;

    gendb::MmapColumn<uint8_t> zm_file;
    uint64_t zm_block_size = 0;
    uint64_t zm_blocks = 0;
    const int32_t* zm_minmax = nullptr;

    gendb::MmapColumn<uint8_t> pre_idx_file;
    uint64_t pre_entry_count = 0;
    const TripleEntry* pre_entries = nullptr;

    std::vector<uint8_t> block_keep;
    std::vector<uint32_t> filtered_blocks;

    gendb::CompactHashMap<uint64_t, AggVal> global_agg;

    GENDB_PHASE("total");

    {
        GENDB_PHASE("data_loading");

        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_tag.open(gendb_dir + "/num/tag.bin");
        num_version.open(gendb_dir + "/num/version.bin");
        num_ddate.open(gendb_dir + "/num/ddate.bin");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");

        if (num_adsh.size() != num_tag.size() ||
            num_adsh.size() != num_version.size() ||
            num_adsh.size() != num_ddate.size() ||
            num_adsh.size() != num_uom.size() ||
            num_adsh.size() != num_value.size()) {
            throw std::runtime_error("num column size mismatch");
        }

        tag_dict = load_dict_file(gendb_dir + "/dicts/tag.dict");
        version_dict = load_dict_file(gendb_dir + "/dicts/version.dict");
        const std::vector<std::string> uom_dict = load_dict_file(gendb_dir + "/dicts/uom.dict");
        usd_code = find_uom_code(uom_dict, "USD");
        if (usd_code < 0) {
            throw std::runtime_error("USD not found in uom dictionary");
        }

        zm_file.open(gendb_dir + "/num/indexes/num_ddate_zonemap.bin");
        if (zm_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("num_ddate_zonemap.bin too small");
        }
        const uint8_t* z = zm_file.data;
        std::memcpy(&zm_block_size, z, sizeof(uint64_t));
        std::memcpy(&zm_blocks, z + sizeof(uint64_t), sizeof(uint64_t));
        const size_t expect_bytes = sizeof(uint64_t) * 2 + static_cast<size_t>(zm_blocks) * sizeof(int32_t) * 2;
        if (zm_file.file_size < expect_bytes) {
            throw std::runtime_error("num_ddate_zonemap.bin short for min/max payload");
        }
        zm_minmax = reinterpret_cast<const int32_t*>(z + sizeof(uint64_t) * 2);

        pre_idx_file.open(gendb_dir + "/pre/indexes/pre_adsh_tag_version_hash.bin");
        if (pre_idx_file.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("pre_adsh_tag_version_hash.bin too small");
        }
        const uint8_t* p = pre_idx_file.data;
        uint64_t pre_rowid_count = 0;
        std::memcpy(&pre_entry_count, p, sizeof(uint64_t));
        std::memcpy(&pre_rowid_count, p + sizeof(uint64_t), sizeof(uint64_t));
        (void)pre_rowid_count;

        const size_t entries_offset = sizeof(uint64_t) * 2;
        const size_t entries_bytes = static_cast<size_t>(pre_entry_count) * sizeof(TripleEntry);
        if (pre_idx_file.file_size < entries_offset + entries_bytes) {
            throw std::runtime_error("pre_adsh_tag_version_hash.bin short for entries payload");
        }
        pre_entries = reinterpret_cast<const TripleEntry*>(p + entries_offset);

        num_adsh.prefetch();
        num_tag.prefetch();
        num_version.prefetch();
        num_ddate.prefetch();
        num_uom.prefetch();
        num_value.prefetch();
        zm_file.prefetch();
        pre_idx_file.prefetch();
    }

    {
        GENDB_PHASE("dim_filter");

        block_keep.assign(static_cast<size_t>(zm_blocks), 0);
        filtered_blocks.clear();
        filtered_blocks.reserve(static_cast<size_t>(zm_blocks));

        for (uint64_t b = 0; b < zm_blocks; ++b) {
            const int32_t mn = zm_minmax[2 * b + 0];
            const int32_t mx = zm_minmax[2 * b + 1];
            if (mx < DDATE_LO || mn > DDATE_HI) continue;
            block_keep[static_cast<size_t>(b)] = 1;
            filtered_blocks.push_back(static_cast<uint32_t>(b));
        }
    }

    {
        GENDB_PHASE("build_joins");
        if (pre_entry_count == 0) {
            throw std::runtime_error("pre_adsh_tag_version_hash has zero entries");
        }
        num_adsh.advise_sequential();
        num_tag.advise_sequential();
        num_version.advise_sequential();
        num_ddate.advise_sequential();
        num_uom.advise_sequential();
        num_value.advise_sequential();
        pre_idx_file.advise_random();
    }

    {
        GENDB_PHASE("main_scan");

        const uint32_t* __restrict adsh = num_adsh.data;
        const uint32_t* __restrict tag = num_tag.data;
        const uint32_t* __restrict version = num_version.data;
        const int32_t* __restrict ddate = num_ddate.data;
        const uint16_t* __restrict uom = num_uom.data;
        const double* __restrict value = num_value.data;

        const size_t total_rows = num_adsh.size();
        const size_t block_size = static_cast<size_t>(zm_block_size);

        std::vector<gendb::CompactHashMap<uint64_t, AggVal>> local_aggs(static_cast<size_t>(nthreads));
        for (int t = 0; t < nthreads; ++t) {
            local_aggs[static_cast<size_t>(t)].reserve(2048);
        }

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            auto& local = local_aggs[static_cast<size_t>(tid)];

            #pragma omp for schedule(dynamic, 1)
            for (size_t bi = 0; bi < filtered_blocks.size(); ++bi) {
                const uint32_t b = filtered_blocks[bi];
                size_t row_lo = static_cast<size_t>(b) * block_size;
                size_t row_hi = std::min(row_lo + block_size, total_rows);

                for (size_t i = row_lo; i < row_hi; ++i) {
                    if (uom[i] != static_cast<uint16_t>(usd_code)) continue;

                    const int32_t dd = ddate[i];
                    if (dd < DDATE_LO || dd > DDATE_HI) continue;

                    const double v = value[i];
                    if (std::isnan(v)) continue;

                    const uint32_t a = adsh[i];
                    const uint32_t tcode = tag[i];
                    const uint32_t vcode = version[i];
                    if (pre_index_exists(pre_entries, pre_entry_count, a, tcode, vcode)) continue;

                    const uint64_t gk = pack_tag_version(tcode, vcode);
                    AggVal& slot = local[gk];
                    slot.cnt += 1;
                    slot.total += v;
                }
            }
        }

        global_agg.reserve(8192);
        for (int t = 0; t < nthreads; ++t) {
            for (auto it : local_aggs[static_cast<size_t>(t)]) {
                const uint64_t k = it.first;
                const AggVal& v = it.second;
                AggVal& dst = global_agg[k];
                dst.cnt += v.cnt;
                dst.total += v.total;
            }
        }
    }

    {
        GENDB_PHASE("output");

        std::vector<ResultRow> rows;
        rows.reserve(global_agg.size());

        for (auto it : global_agg) {
            const uint64_t key = it.first;
            const AggVal& agg = it.second;
            if (agg.cnt <= 10) continue;

            rows.push_back(ResultRow{
                static_cast<uint32_t>(key >> 32),
                static_cast<uint32_t>(key & 0xFFFFFFFFu),
                agg.cnt,
                agg.total
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
        if (!out) {
            throw std::runtime_error("Cannot open output file: " + out_path);
        }

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
