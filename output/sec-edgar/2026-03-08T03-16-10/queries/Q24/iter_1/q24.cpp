#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace fs = std::filesystem;

namespace {

constexpr size_t kBlockSize = 100000;
constexpr int32_t kDdateLo = 19358;
constexpr int32_t kDdateHi = 19722;
constexpr uint64_t kEmptyKey = UINT64_MAX;

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct DictData {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    std::string_view at(size_t idx) const {
        return std::string_view(data.data + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

    size_t size() const {
        return offsets.size() > 0 ? offsets.size() - 1 : 0;
    }
};

struct AggSlot {
    uint64_t key = kEmptyKey;
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

static inline uint64_t rotl64(uint64_t x, unsigned int k) {
    return (x << k) | (x >> (64 - k));
}

static inline uint64_t pack_group_key(uint32_t tag, uint32_t version) {
    return (uint64_t(tag) << 32) | uint64_t(version);
}

static inline uint64_t hash_join_key(uint32_t adsh, uint32_t tag, uint32_t version) {
    uint64_t x = uint64_t(adsh) * 0x9e3779b185ebca87ULL;
    x ^= rotl64(uint64_t(tag) * 0xc2b2ae3d27d4eb4fULL, 21);
    x ^= rotl64(uint64_t(version) * 0x165667b19e3779f9ULL, 43);
    return mix64(x);
}

static uint16_t find_uom_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("USD not found in num uom dictionary");
}

struct BlockedBloomFilter {
    static constexpr size_t kWordsPerBlock = 8;
    static constexpr size_t kBytesPerBlock = kWordsPerBlock * sizeof(uint32_t);

    size_t block_mask = 0;
    std::vector<uint32_t> words;

    void init(size_t build_rows) {
        size_t target_bytes = 1u << 24;
        if (build_rows > 12000000) {
            target_bytes = 1u << 25;
        }
        size_t blocks = target_bytes / kBytesPerBlock;
        size_t pow2_blocks = 1;
        while (pow2_blocks < blocks) {
            pow2_blocks <<= 1;
        }
        block_mask = pow2_blocks - 1;
        words.assign(pow2_blocks * kWordsPerBlock, 0);
    }

    void add_hash(uint64_t h) {
        const size_t block = size_t(h) & block_mask;
        uint32_t* base = words.data() + block * kWordsPerBlock;
        const uint32_t h1 = uint32_t(h >> 32);
        const uint32_t h2 = uint32_t(h);
        base[0] |= uint32_t(1u) << (h1 & 31);
        base[1] |= uint32_t(1u) << ((h1 >> 5) & 31);
        base[2] |= uint32_t(1u) << ((h1 >> 10) & 31);
        base[3] |= uint32_t(1u) << ((h1 >> 15) & 31);
        base[4] |= uint32_t(1u) << (h2 & 31);
        base[5] |= uint32_t(1u) << ((h2 >> 5) & 31);
        base[6] |= uint32_t(1u) << ((h2 >> 10) & 31);
        base[7] |= uint32_t(1u) << ((h2 >> 15) & 31);
    }

    bool maybe_contains_hash(uint64_t h) const {
        const size_t block = size_t(h) & block_mask;
        const uint32_t* base = words.data() + block * kWordsPerBlock;
        const uint32_t h1 = uint32_t(h >> 32);
        const uint32_t h2 = uint32_t(h);
        return (base[0] & (uint32_t(1u) << (h1 & 31))) &&
               (base[1] & (uint32_t(1u) << ((h1 >> 5) & 31))) &&
               (base[2] & (uint32_t(1u) << ((h1 >> 10) & 31))) &&
               (base[3] & (uint32_t(1u) << ((h1 >> 15) & 31))) &&
               (base[4] & (uint32_t(1u) << (h2 & 31))) &&
               (base[5] & (uint32_t(1u) << ((h2 >> 5) & 31))) &&
               (base[6] & (uint32_t(1u) << ((h2 >> 10) & 31))) &&
               (base[7] & (uint32_t(1u) << ((h2 >> 15) & 31)));
    }
};

class CompactHashMap {
public:
    void reserve(size_t expected) {
        size_t cap = 1024;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        slots_.assign(cap, AggSlot{});
        mask_ = cap - 1;
        size_ = 0;
    }

    void add(uint32_t tag, uint32_t version, double value) {
        add_raw(pack_group_key(tag, version), 1, value);
    }

    void merge_slot(const AggSlot& src) {
        add_raw(src.key, src.cnt, src.total);
    }

    const std::vector<AggSlot>& slots() const {
        return slots_;
    }

private:
    void add_raw(uint64_t key, uint64_t cnt, double total) {
        if ((size_ + 1) * 10 >= slots_.size() * 7) {
            rehash(slots_.size() << 1);
        }
        size_t slot = size_t(mix64(key)) & mask_;
        for (;;) {
            AggSlot& s = slots_[slot];
            if (s.key == kEmptyKey) {
                s.key = key;
                s.cnt = cnt;
                s.total = total;
                ++size_;
                return;
            }
            if (s.key == key) {
                s.cnt += cnt;
                s.total += total;
                return;
            }
            slot = (slot + 1) & mask_;
        }
    }
    void rehash(size_t new_cap) {
        std::vector<AggSlot> old = std::move(slots_);
        slots_.assign(new_cap, AggSlot{});
        mask_ = new_cap - 1;
        size_ = 0;
        for (const AggSlot& src : old) {
            if (src.key == kEmptyKey) {
                continue;
            }
            size_t slot = size_t(mix64(src.key)) & mask_;
            for (;;) {
                AggSlot& dst = slots_[slot];
                if (dst.key == kEmptyKey) {
                    dst = src;
                    ++size_;
                    break;
                }
                slot = (slot + 1) & mask_;
            }
        }
    }

    size_t mask_ = 0;
    size_t size_ = 0;
    std::vector<AggSlot> slots_;
};

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
    return lo < n_pre &&
           compare_pre_key(pre_adsh, pre_tag, pre_version, rowids, lo, adsh, tag, version) == 0;
}

}  // namespace

int main(int argc, char** argv) {
    GENDB_PHASE("total");

    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        fs::create_directories(results_dir);

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<int32_t> num_ddate;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<ZoneRecord<uint16_t>> uom_zone;
        gendb::MmapColumn<ZoneRecord<int32_t>> ddate_zone;

        DictData uom_dict;
        DictData tag_dict;
        DictData version_dict;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_rowids;

        size_t num_rows = 0;
        uint16_t usd_code = 0;
        std::vector<uint32_t> qualifying_blocks;

        {
            GENDB_PHASE("data_loading");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_ddate.open(gendb_dir + "/num/ddate.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");
            ddate_zone.open(gendb_dir + "/num/indexes/ddate.zone_map.bin");

            uom_dict.offsets.open(gendb_dir + "/num/dict_uom.offsets.bin");
            uom_dict.data.open(gendb_dir + "/num/dict_uom.data.bin");
            tag_dict.offsets.open(gendb_dir + "/shared/tag.offsets.bin");
            tag_dict.data.open(gendb_dir + "/shared/tag.data.bin");
            version_dict.offsets.open(gendb_dir + "/shared/version.offsets.bin");
            version_dict.data.open(gendb_dir + "/shared/version.data.bin");

            num_rows = num_uom.size();
            if (num_ddate.size() != num_rows || num_adsh.size() != num_rows || num_tag.size() != num_rows ||
                num_version.size() != num_rows || num_value.size() != num_rows) {
                throw std::runtime_error("num column size mismatch");
            }

            gendb::mmap_prefetch_all(num_uom, num_ddate, num_adsh, num_tag, num_version, num_value);
            uom_zone.prefetch();
            ddate_zone.prefetch();
        }

        {
            GENDB_PHASE("dim_filter");
            usd_code = find_uom_code(uom_dict, "USD");
            const size_t num_blocks = (num_rows + kBlockSize - 1) / kBlockSize;
            if (uom_zone.size() != num_blocks || ddate_zone.size() != num_blocks) {
                throw std::runtime_error("zone map block count mismatch");
            }

            qualifying_blocks.reserve(num_blocks);
            for (size_t block = 0; block < num_blocks; ++block) {
                const auto uz = uom_zone[block];
                const auto dz = ddate_zone[block];
                if (uz.max < usd_code || uz.min > usd_code) {
                    continue;
                }
                if (dz.max < kDdateLo || dz.min > kDdateHi) {
                    continue;
                }
                qualifying_blocks.push_back(static_cast<uint32_t>(block));
            }
        }

        BlockedBloomFilter pre_bloom;
        size_t n_pre = 0;
        {
            GENDB_PHASE("build_joins");
            if (!qualifying_blocks.empty()) {
                pre_adsh.open(gendb_dir + "/pre/adsh.bin");
                pre_tag.open(gendb_dir + "/pre/tag.bin");
                pre_version.open(gendb_dir + "/pre/version.bin");
                pre_rowids.open(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");
                n_pre = pre_rowids.size();
                if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size()) {
                    throw std::runtime_error("pre column size mismatch");
                }
                if (pre_adsh.size() != n_pre) {
                    throw std::runtime_error("pre index size mismatch");
                }

                pre_rowids.advise_random();
                pre_adsh.advise_random();
                pre_tag.advise_random();
                pre_version.advise_random();

                pre_bloom.init(n_pre);
                for (size_t i = 0; i < n_pre; ++i) {
                    pre_bloom.add_hash(hash_join_key(pre_adsh[i], pre_tag[i], pre_version[i]));
                }
            }
        }

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<CompactHashMap> locals(static_cast<size_t>(max_threads));
        for (CompactHashMap& map : locals) {
            map.reserve(1024);
        }

        {
            GENDB_PHASE("main_scan");
            if (!qualifying_blocks.empty()) {
                const uint16_t* uom = num_uom.data;
                const int32_t* ddate = num_ddate.data;
                const uint32_t* adsh = num_adsh.data;
                const uint32_t* tag = num_tag.data;
                const uint32_t* version = num_version.data;
                const double* value = num_value.data;

                #pragma omp parallel
                {
                    CompactHashMap& local = locals[omp_get_thread_num()];

                    #pragma omp for schedule(dynamic, 1)
                    for (size_t bi = 0; bi < qualifying_blocks.size(); ++bi) {
                        const size_t block = qualifying_blocks[bi];
                        const size_t begin = block * kBlockSize;
                        const size_t end = std::min(begin + kBlockSize, num_rows);

                        for (size_t i = begin; i < end; ++i) {
                            if (uom[i] != usd_code) {
                                continue;
                            }
                            const int32_t d = ddate[i];
                            if (d < kDdateLo || d > kDdateHi) {
                                continue;
                            }

                            const uint32_t row_adsh = adsh[i];
                            const uint32_t row_tag = tag[i];
                            const uint32_t row_version = version[i];
                            const uint64_t h = hash_join_key(row_adsh, row_tag, row_version);
                            if (pre_bloom.maybe_contains_hash(h) &&
                                pre_contains_key(pre_adsh.data, pre_tag.data, pre_version.data,
                                                 pre_rowids.data, n_pre,
                                                 row_adsh, row_tag, row_version)) {
                                continue;
                            }

                            local.add(row_tag, row_version, value[i]);
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");
            CompactHashMap merged;
            merged.reserve(2048);
            for (const CompactHashMap& local : locals) {
                for (const AggSlot& slot : local.slots()) {
                    if (slot.key == kEmptyKey) {
                        continue;
                    }
                    merged.merge_slot(slot);
                }
            }

            std::vector<ResultRow> results;
            for (const AggSlot& slot : merged.slots()) {
                if (slot.key == kEmptyKey || slot.cnt <= 10) {
                    continue;
                }
                results.push_back(ResultRow{
                    static_cast<uint32_t>(slot.key >> 32),
                    static_cast<uint32_t>(slot.key),
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

            const std::string out_path = results_dir + "/Q24.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                std::perror(out_path.c_str());
                return 1;
            }

            std::fprintf(out, "tag,version,cnt,total\n");
            for (const ResultRow& row : results) {
                const std::string_view tag_sv = tag_dict.at(row.tag);
                const std::string_view version_sv = version_dict.at(row.version);
                std::fprintf(out, "%.*s,%.*s,%llu,%.2f\n",
                             int(tag_sv.size()), tag_sv.data(),
                             int(version_sv.size()), version_sv.data(),
                             static_cast<unsigned long long>(row.cnt),
                             row.total);
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q24 failed: %s\n", e.what());
        return 1;
    }
}
