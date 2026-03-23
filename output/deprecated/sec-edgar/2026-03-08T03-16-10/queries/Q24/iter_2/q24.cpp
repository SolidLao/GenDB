#include <algorithm>
#include <cstdint>
#include <cstdio>
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
constexpr size_t kMorselRows = 1u << 20;

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
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
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

static inline uint64_t pack_group_key(uint32_t tag, uint32_t version) {
    return (uint64_t(tag) << 32) | uint64_t(version);
}

static uint16_t find_uom_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("USD not found in num uom dictionary");
}

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

static inline bool bitmap_get(const std::vector<uint64_t>& bitmap, size_t rowid) {
    return (bitmap[rowid >> 6] >> (rowid & 63)) & 1ULL;
}

static inline void bitmap_set(std::vector<uint64_t>& bitmap, size_t rowid) {
    bitmap[rowid >> 6] |= 1ULL << (rowid & 63);
}

static inline int compare_num_key(const uint32_t* num_adsh, const uint32_t* num_tag,
                                  const uint32_t* num_version, const uint32_t* num_rowids,
                                  size_t pos, uint32_t pre_adsh, uint32_t pre_tag,
                                  uint32_t pre_version) {
    const uint32_t rid = num_rowids[pos];
    const uint32_t a = num_adsh[rid];
    if (a < pre_adsh) return -1;
    if (a > pre_adsh) return 1;
    const uint32_t t = num_tag[rid];
    if (t < pre_tag) return -1;
    if (t > pre_tag) return 1;
    const uint32_t v = num_version[rid];
    if (v < pre_version) return -1;
    if (v > pre_version) return 1;
    return 0;
}

static inline int compare_pre_key(const uint32_t* pre_adsh, const uint32_t* pre_tag,
                                  const uint32_t* pre_version, const uint32_t* pre_rowids,
                                  size_t pos, uint32_t num_adsh, uint32_t num_tag,
                                  uint32_t num_version) {
    const uint32_t rid = pre_rowids[pos];
    const uint32_t a = pre_adsh[rid];
    if (a < num_adsh) return -1;
    if (a > num_adsh) return 1;
    const uint32_t t = pre_tag[rid];
    if (t < num_tag) return -1;
    if (t > num_tag) return 1;
    const uint32_t v = pre_version[rid];
    if (v < num_version) return -1;
    if (v > num_version) return 1;
    return 0;
}

static size_t lower_bound_pre(const uint32_t* pre_adsh, const uint32_t* pre_tag,
                              const uint32_t* pre_version, const uint32_t* pre_rowids,
                              size_t n_pre, uint32_t num_adsh, uint32_t num_tag,
                              uint32_t num_version) {
    size_t lo = 0;
    size_t hi = n_pre;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if (compare_pre_key(pre_adsh, pre_tag, pre_version, pre_rowids, mid,
                            num_adsh, num_tag, num_version) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
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
        gendb::MmapColumn<uint32_t> num_rowids_sorted;
        DictData uom_dict;
        DictData tag_dict;
        DictData version_dict;
        gendb::MmapColumn<ZoneRecord<uint16_t>> uom_zone;
        gendb::MmapColumn<ZoneRecord<int32_t>> ddate_zone;

        size_t num_rows = 0;
        uint16_t usd_code = 0;
        std::vector<uint64_t> qualifying_bitmap;
        size_t qualifying_count = 0;

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_rowids_sorted;

        {
            GENDB_PHASE("data_loading");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_ddate.open(gendb_dir + "/num/ddate.bin");
            num_rowids_sorted.open(gendb_dir + "/num/indexes/adsh_tag_version.rowids.bin");
            uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");
            ddate_zone.open(gendb_dir + "/num/indexes/ddate.zone_map.bin");
            uom_dict.offsets.open(gendb_dir + "/num/dict_uom.offsets.bin");
            uom_dict.data.open(gendb_dir + "/num/dict_uom.data.bin");
            tag_dict.offsets.open(gendb_dir + "/shared/tag.offsets.bin");
            tag_dict.data.open(gendb_dir + "/shared/tag.data.bin");
            version_dict.offsets.open(gendb_dir + "/shared/version.offsets.bin");
            version_dict.data.open(gendb_dir + "/shared/version.data.bin");

            num_rows = num_uom.size();
            if (num_ddate.size() != num_rows || num_rowids_sorted.size() != num_rows) {
                throw std::runtime_error("num column size mismatch");
            }
            gendb::mmap_prefetch_all(num_uom, num_ddate, num_rowids_sorted);
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

            qualifying_bitmap.assign((num_rows + 63) >> 6, 0);
            for (size_t block = 0; block < num_blocks; ++block) {
                const ZoneRecord<uint16_t> uz = uom_zone[block];
                const ZoneRecord<int32_t> dz = ddate_zone[block];
                if (uz.max < usd_code || uz.min > usd_code) {
                    continue;
                }
                if (dz.max < kDdateLo || dz.min > kDdateHi) {
                    continue;
                }

                const size_t begin = block * kBlockSize;
                const size_t end = std::min(begin + kBlockSize, num_rows);
                for (size_t row = begin; row < end; ++row) {
                    if (num_uom[row] == usd_code) {
                        const int32_t d = num_ddate[row];
                        if (d >= kDdateLo && d <= kDdateHi) {
                            bitmap_set(qualifying_bitmap, row);
                            ++qualifying_count;
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            if (qualifying_count != 0) {
                num_adsh.open(gendb_dir + "/num/adsh.bin");
                num_tag.open(gendb_dir + "/num/tag.bin");
                num_version.open(gendb_dir + "/num/version.bin");
                num_value.open(gendb_dir + "/num/value.bin");
                pre_adsh.open(gendb_dir + "/pre/adsh.bin");
                pre_tag.open(gendb_dir + "/pre/tag.bin");
                pre_version.open(gendb_dir + "/pre/version.bin");
                pre_rowids_sorted.open(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");

                if (num_adsh.size() != num_rows || num_tag.size() != num_rows ||
                    num_version.size() != num_rows || num_value.size() != num_rows) {
                    throw std::runtime_error("num payload column size mismatch");
                }
                if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                    pre_adsh.size() != pre_rowids_sorted.size()) {
                    throw std::runtime_error("pre column size mismatch");
                }

                gendb::mmap_prefetch_all(num_adsh, num_tag, num_version, num_value,
                                         pre_adsh, pre_tag, pre_version, pre_rowids_sorted);
            }
        }

        const int thread_count = std::max(1, omp_get_max_threads());
        std::vector<CompactHashMap> locals(static_cast<size_t>(thread_count));
        for (CompactHashMap& map : locals) {
            const size_t reserve = std::max<size_t>(1024, qualifying_count / thread_count / 8);
            map.reserve(reserve);
        }

        {
            GENDB_PHASE("main_scan");
            if (qualifying_count != 0) {
                const size_t n_num_sorted = num_rowids_sorted.size();
                const size_t n_pre_sorted = pre_rowids_sorted.size();
                const size_t morsel_count = (n_num_sorted + kMorselRows - 1) / kMorselRows;

                #pragma omp parallel for schedule(dynamic, 1)
                for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                    CompactHashMap& local = locals[omp_get_thread_num()];
                    const size_t begin = morsel * kMorselRows;
                    const size_t end = std::min(begin + kMorselRows, n_num_sorted);

                    size_t first = begin;
                    while (first < end && !bitmap_get(qualifying_bitmap, num_rowids_sorted[first])) {
                        ++first;
                    }
                    if (first == end) {
                        continue;
                    }

                    const uint32_t first_rowid = num_rowids_sorted[first];
                    size_t pre_pos = lower_bound_pre(pre_adsh.data, pre_tag.data, pre_version.data,
                                                     pre_rowids_sorted.data, n_pre_sorted,
                                                     num_adsh[first_rowid], num_tag[first_rowid],
                                                     num_version[first_rowid]);

                    for (size_t pos = first; pos < end; ++pos) {
                        const uint32_t num_rowid = num_rowids_sorted[pos];
                        if (!bitmap_get(qualifying_bitmap, num_rowid)) {
                            continue;
                        }

                        const uint32_t row_adsh = num_adsh[num_rowid];
                        const uint32_t row_tag = num_tag[num_rowid];
                        const uint32_t row_version = num_version[num_rowid];

                        while (pre_pos < n_pre_sorted &&
                               compare_pre_key(pre_adsh.data, pre_tag.data, pre_version.data,
                                               pre_rowids_sorted.data, pre_pos,
                                               row_adsh, row_tag, row_version) < 0) {
                            ++pre_pos;
                        }

                        const bool matched =
                            pre_pos < n_pre_sorted &&
                            compare_pre_key(pre_adsh.data, pre_tag.data, pre_version.data,
                                            pre_rowids_sorted.data, pre_pos,
                                            row_adsh, row_tag, row_version) == 0;
                        if (!matched) {
                            local.add(row_tag, row_version, num_value[num_rowid]);
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");
            CompactHashMap merged;
            merged.reserve(std::max<size_t>(2048, qualifying_count / 8));
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
