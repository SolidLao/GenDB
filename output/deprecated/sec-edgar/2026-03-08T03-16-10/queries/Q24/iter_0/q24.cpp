#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace fs = std::filesystem;

static constexpr size_t BLOCK_SIZE = 100000;
static constexpr int32_t DDATE_LO = 19358;
static constexpr int32_t DDATE_HI = 19722;
static constexpr uint64_t EMPTY_KEY = UINT64_MAX;
static constexpr size_t LOCAL_AGG_CAP = 1u << 14;
static constexpr size_t LOCAL_AGG_MASK = LOCAL_AGG_CAP - 1;

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct AggSlot {
    uint64_t key = EMPTY_KEY;
    uint64_t cnt = 0;
    double total = 0.0;
};

struct ResultRow {
    uint32_t tag = 0;
    uint32_t version = 0;
    uint64_t cnt = 0;
    double total = 0.0;
};

static inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static inline uint64_t pack_group_key(uint32_t tag, uint32_t version) {
    return (uint64_t(tag) << 32) | uint64_t(version);
}

static inline void agg_insert(AggSlot* table, uint32_t tag, uint32_t version, double value) {
    const uint64_t key = pack_group_key(tag, version);
    size_t slot = mix64(key) & LOCAL_AGG_MASK;
    for (;;) {
        if (table[slot].key == EMPTY_KEY) {
            table[slot].key = key;
            table[slot].cnt = 1;
            table[slot].total = value;
            return;
        }
        if (table[slot].key == key) {
            table[slot].cnt += 1;
            table[slot].total += value;
            return;
        }
        slot = (slot + 1) & LOCAL_AGG_MASK;
    }
}

static inline std::string_view dict_at(const uint64_t* offsets, const char* data, size_t code) {
    return std::string_view(data + offsets[code], offsets[code + 1] - offsets[code]);
}

static uint16_t find_uom_code(const uint64_t* offsets, size_t n_offsets, const char* data,
                              const char* needle) {
    const size_t n = n_offsets - 1;
    for (size_t i = 0; i < n; ++i) {
        std::string_view v = dict_at(offsets, data, i);
        if (v == needle) return static_cast<uint16_t>(i);
    }
    throw std::runtime_error("USD not found in num uom dictionary");
}

static inline int compare_pre_key(const uint32_t* pre_adsh, const uint32_t* pre_tag,
                                  const uint32_t* pre_version, const uint32_t* rowids,
                                  size_t pos, uint32_t adsh, uint32_t tag, uint32_t version) {
    const uint32_t rid = rowids[pos];
    const uint32_t a = pre_adsh[rid];
    if (a < adsh) return -1;
    if (a > adsh) return 1;
    const uint32_t t = pre_tag[rid];
    if (t < tag) return -1;
    if (t > tag) return 1;
    const uint32_t v = pre_version[rid];
    if (v < version) return -1;
    if (v > version) return 1;
    return 0;
}

static bool pre_contains_key(const uint32_t* pre_adsh, const uint32_t* pre_tag,
                             const uint32_t* pre_version, const uint32_t* rowids, size_t n_pre,
                             uint32_t adsh, uint32_t tag, uint32_t version) {
    size_t lo = 0;
    size_t hi = n_pre;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        const int cmp = compare_pre_key(pre_adsh, pre_tag, pre_version, rowids, mid,
                                        adsh, tag, version);
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= n_pre) return false;
    return compare_pre_key(pre_adsh, pre_tag, pre_version, rowids, lo, adsh, tag, version) == 0;
}

int main(int argc, char** argv) {
    GENDB_PHASE("total");

    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    fs::create_directories(results_dir);

    gendb::MmapColumn<uint16_t> num_uom;
    gendb::MmapColumn<int32_t> num_ddate;
    gendb::MmapColumn<double> num_value;
    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint32_t> num_tag;
    gendb::MmapColumn<uint32_t> num_version;

    gendb::MmapColumn<uint64_t> uom_offsets;
    gendb::MmapColumn<char> uom_data;
    gendb::MmapColumn<ZoneRecord<uint16_t>> uom_zone;
    gendb::MmapColumn<ZoneRecord<int32_t>> ddate_zone;

    gendb::MmapColumn<uint32_t> pre_adsh;
    gendb::MmapColumn<uint32_t> pre_tag;
    gendb::MmapColumn<uint32_t> pre_version;
    gendb::MmapColumn<uint32_t> pre_rowids;

    gendb::MmapColumn<uint64_t> tag_offsets;
    gendb::MmapColumn<char> tag_data;
    gendb::MmapColumn<uint64_t> version_offsets;
    gendb::MmapColumn<char> version_data;

    size_t num_rows = 0;
    uint16_t usd_code = 0;
    std::vector<uint32_t> qualifying_blocks;

    {
        GENDB_PHASE("data_loading");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_ddate.open(gendb_dir + "/num/ddate.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_tag.open(gendb_dir + "/num/tag.bin");
        num_version.open(gendb_dir + "/num/version.bin");

        uom_offsets.open(gendb_dir + "/num/dict_uom.offsets.bin");
        uom_data.open(gendb_dir + "/num/dict_uom.data.bin");
        uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");
        ddate_zone.open(gendb_dir + "/num/indexes/ddate.zone_map.bin");

        tag_offsets.open(gendb_dir + "/shared/tag.offsets.bin");
        tag_data.open(gendb_dir + "/shared/tag.data.bin");
        version_offsets.open(gendb_dir + "/shared/version.offsets.bin");
        version_data.open(gendb_dir + "/shared/version.data.bin");

        num_rows = num_uom.size();
        num_uom.prefetch();
        num_ddate.prefetch();
        num_value.prefetch();
        num_adsh.prefetch();
        num_tag.prefetch();
        num_version.prefetch();
    }

    {
        GENDB_PHASE("dim_filter");
        usd_code = find_uom_code(uom_offsets.data, uom_offsets.size(), uom_data.data, "USD");

        const size_t n_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if (uom_zone.size() != n_blocks || ddate_zone.size() != n_blocks) {
            throw std::runtime_error("zone map block count mismatch");
        }

        qualifying_blocks.reserve(n_blocks);
        for (size_t b = 0; b < n_blocks; ++b) {
            const auto uz = uom_zone[b];
            const auto dz = ddate_zone[b];
            if (uz.max < usd_code || uz.min > usd_code) continue;
            if (dz.max < DDATE_LO || dz.min > DDATE_HI) continue;
            qualifying_blocks.push_back(static_cast<uint32_t>(b));
        }
    }

    bool need_pre = !qualifying_blocks.empty();
    size_t n_pre = 0;
    {
        GENDB_PHASE("build_joins");
        if (need_pre) {
            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_rowids.open(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");
            pre_rowids.advise_random();
            pre_adsh.advise_random();
            pre_tag.advise_random();
            pre_version.advise_random();
            n_pre = pre_rowids.size();
        }
    }

    const int n_threads = omp_get_max_threads();
    std::vector<std::vector<AggSlot>> local_maps(
        n_threads, std::vector<AggSlot>(LOCAL_AGG_CAP));

    {
        GENDB_PHASE("main_scan");
        if (need_pre) {
            #pragma omp parallel for schedule(static)
            for (int t = 0; t < n_threads; ++t) {
                std::fill(local_maps[t].begin(), local_maps[t].end(), AggSlot{});
            }

            #pragma omp parallel num_threads(n_threads)
            {
                AggSlot* local = local_maps[omp_get_thread_num()].data();

                #pragma omp for schedule(dynamic, 1)
                for (size_t bi = 0; bi < qualifying_blocks.size(); ++bi) {
                    const size_t block = qualifying_blocks[bi];
                    const size_t start = block * BLOCK_SIZE;
                    const size_t end = std::min(start + BLOCK_SIZE, num_rows);

                    for (size_t i = start; i < end; ++i) {
                        if (num_uom[i] != usd_code) continue;
                        const int32_t d = num_ddate[i];
                        if (d < DDATE_LO || d > DDATE_HI) continue;

                        const uint32_t adsh = num_adsh[i];
                        const uint32_t tag = num_tag[i];
                        const uint32_t version = num_version[i];
                        if (pre_contains_key(pre_adsh.data, pre_tag.data, pre_version.data,
                                            pre_rowids.data, n_pre, adsh, tag, version)) {
                            continue;
                        }
                        agg_insert(local, tag, version, num_value[i]);
                    }
                }
            }
        }
    }

    {
        GENDB_PHASE("output");
        std::vector<ResultRow> results;
        if (need_pre) {
            std::vector<AggSlot> merged(LOCAL_AGG_CAP);
            std::fill(merged.begin(), merged.end(), AggSlot{});

            for (int t = 0; t < n_threads; ++t) {
                for (const AggSlot& src : local_maps[t]) {
                    if (src.key == EMPTY_KEY) continue;
                    size_t slot = mix64(src.key) & LOCAL_AGG_MASK;
                    for (;;) {
                        if (merged[slot].key == EMPTY_KEY) {
                            merged[slot] = src;
                            break;
                        }
                        if (merged[slot].key == src.key) {
                            merged[slot].cnt += src.cnt;
                            merged[slot].total += src.total;
                            break;
                        }
                        slot = (slot + 1) & LOCAL_AGG_MASK;
                    }
                }
            }

            results.reserve(128);
            for (const AggSlot& slot : merged) {
                if (slot.key == EMPTY_KEY || slot.cnt <= 10) continue;
                results.push_back(ResultRow{
                    static_cast<uint32_t>(slot.key >> 32),
                    static_cast<uint32_t>(slot.key & 0xffffffffu),
                    slot.cnt,
                    slot.total
                });
            }

            auto cmp = [](const ResultRow& a, const ResultRow& b) {
                if (a.cnt != b.cnt) return a.cnt > b.cnt;
                if (a.tag != b.tag) return a.tag < b.tag;
                return a.version < b.version;
            };
            if (results.size() > 100) {
                std::partial_sort(results.begin(), results.begin() + 100, results.end(), cmp);
                results.resize(100);
            } else {
                std::sort(results.begin(), results.end(), cmp);
            }
        }

        const std::string out_path = results_dir + "/Q24.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            std::perror(out_path.c_str());
            return 1;
        }

        std::fprintf(out, "tag,version,cnt,total\n");
        for (const ResultRow& row : results) {
            const std::string_view tag_sv = dict_at(tag_offsets.data, tag_data.data, row.tag);
            const std::string_view version_sv =
                dict_at(version_offsets.data, version_data.data, row.version);
            std::fprintf(out, "%.*s,%.*s,%llu,%.2f\n",
                         int(tag_sv.size()), tag_sv.data(),
                         int(version_sv.size()), version_sv.data(),
                         static_cast<unsigned long long>(row.cnt), row.total);
        }
        std::fclose(out);
    }

    return 0;
}
