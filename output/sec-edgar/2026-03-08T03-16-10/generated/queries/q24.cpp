#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
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
        if (slots_.empty()) {
            reserve(1024);
        }
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

static bool pre_slice_contains(const uint32_t* pre_tag, const uint32_t* pre_version,
                               const uint32_t* sorted_rowids, uint32_t slice_begin,
                               uint32_t slice_end, uint32_t tag, uint32_t version) {
    uint32_t lo = slice_begin;
    uint32_t hi = slice_end;
    while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) >> 1);
        const uint32_t rid = sorted_rowids[mid];
        const uint32_t mid_tag = pre_tag[rid];
        if (mid_tag < tag) {
            lo = mid + 1;
            continue;
        }
        if (mid_tag > tag) {
            hi = mid;
            continue;
        }
        const uint32_t mid_version = pre_version[rid];
        if (mid_version < version) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= slice_end) {
        return false;
    }
    const uint32_t rid = sorted_rowids[lo];
    return pre_tag[rid] == tag && pre_version[rid] == version;
}

static void write_header_only(const std::string& out_path) {
    FILE* out = std::fopen(out_path.c_str(), "w");
    if (!out) {
        throw std::runtime_error("failed to open output file");
    }
    std::fprintf(out, "tag,version,cnt,total\n");
    std::fclose(out);
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
        const std::string out_path = results_dir + "/Q24.csv";
        fs::create_directories(results_dir);

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<int32_t> num_ddate;
        gendb::MmapColumn<ZoneRecord<uint16_t>> uom_zone;
        gendb::MmapColumn<ZoneRecord<int32_t>> ddate_zone;
        DictData uom_dict;

        size_t num_rows = 0;
        uint16_t usd_code = 0;
        std::vector<uint32_t> surviving_blocks;

        {
            GENDB_PHASE("data_loading");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_ddate.open(gendb_dir + "/num/ddate.bin");
            uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");
            ddate_zone.open(gendb_dir + "/num/indexes/ddate.zone_map.bin");
            uom_dict.offsets.open(gendb_dir + "/num/dict_uom.offsets.bin");
            uom_dict.data.open(gendb_dir + "/num/dict_uom.data.bin");

            num_rows = num_uom.size();
            if (num_ddate.size() != num_rows) {
                throw std::runtime_error("num column size mismatch");
            }

            gendb::mmap_prefetch_all(num_uom, num_ddate, uom_zone, ddate_zone);
        }

        {
            GENDB_PHASE("dim_filter");
            usd_code = find_uom_code(uom_dict, "USD");

            const size_t n_blocks = (num_rows + kBlockSize - 1) / kBlockSize;
            if (uom_zone.size() != n_blocks || ddate_zone.size() != n_blocks) {
                throw std::runtime_error("zone map block count mismatch");
            }

            surviving_blocks.reserve(n_blocks);
            for (size_t block = 0; block < n_blocks; ++block) {
                const ZoneRecord<uint16_t>& uz = uom_zone[block];
                const ZoneRecord<int32_t>& dz = ddate_zone[block];
                if (uz.max < usd_code || uz.min > usd_code) {
                    continue;
                }
                if (dz.max < kDdateLo || dz.min > kDdateHi) {
                    continue;
                }
                surviving_blocks.push_back(static_cast<uint32_t>(block));
            }
        }

        if (surviving_blocks.empty()) {
            {
                GENDB_PHASE("build_joins");
            }
            {
                GENDB_PHASE("main_scan");
            }
            {
                GENDB_PHASE("output");
                write_header_only(out_path);
            }
            return 0;
        }

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint32_t> pre_sorted_rowids;
        gendb::MmapColumn<uint64_t> adsh_dict_offsets;

        std::vector<uint32_t> adsh_offsets;
        size_t n_pre = 0;

        {
            GENDB_PHASE("build_joins");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_sorted_rowids.open(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");
            adsh_dict_offsets.open(gendb_dir + "/shared/adsh.offsets.bin");

            if (num_adsh.size() != num_rows || num_tag.size() != num_rows ||
                num_version.size() != num_rows || num_value.size() != num_rows) {
                throw std::runtime_error("num payload column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_sorted_rowids.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (adsh_dict_offsets.size() == 0) {
                throw std::runtime_error("empty adsh dictionary");
            }

            n_pre = pre_sorted_rowids.size();
            const size_t adsh_cardinality = adsh_dict_offsets.size() - 1;
            adsh_offsets.assign(adsh_cardinality + 1, static_cast<uint32_t>(n_pre));

            size_t next_adsh = 0;
            for (uint32_t pos = 0; pos < n_pre; ++pos) {
                const uint32_t rid = pre_sorted_rowids[pos];
                const uint32_t adsh = pre_adsh[rid];
                while (next_adsh <= adsh && next_adsh < adsh_offsets.size()) {
                    adsh_offsets[next_adsh] = pos;
                    ++next_adsh;
                }
            }
            while (next_adsh < adsh_offsets.size()) {
                adsh_offsets[next_adsh] = static_cast<uint32_t>(n_pre);
                ++next_adsh;
            }

            gendb::mmap_prefetch_all(num_adsh, num_tag, num_version, num_value);
            pre_tag.advise_random();
            pre_version.advise_random();
            pre_sorted_rowids.advise_random();
        }

        const int thread_count = std::max(1, omp_get_max_threads());
        std::vector<CompactHashMap> locals(static_cast<size_t>(thread_count));
        for (CompactHashMap& map : locals) {
            map.reserve(1024);
        }

        {
            GENDB_PHASE("main_scan");
            #pragma omp parallel for schedule(dynamic, 1)
            for (size_t bi = 0; bi < surviving_blocks.size(); ++bi) {
                CompactHashMap& local = locals[static_cast<size_t>(omp_get_thread_num())];
                const size_t block = surviving_blocks[bi];
                const size_t begin = block * kBlockSize;
                const size_t end = std::min(begin + kBlockSize, num_rows);

                for (size_t row = begin; row < end; ++row) {
                    if (num_uom[row] != usd_code) {
                        continue;
                    }
                    const int32_t ddate = num_ddate[row];
                    if (ddate < kDdateLo || ddate > kDdateHi) {
                        continue;
                    }

                    const uint32_t adsh = num_adsh[row];
                    if (adsh + 1 >= adsh_offsets.size()) {
                        continue;
                    }
                    const uint32_t slice_begin = adsh_offsets[adsh];
                    const uint32_t slice_end = adsh_offsets[adsh + 1];
                    if (slice_begin != slice_end &&
                        pre_slice_contains(pre_tag.data, pre_version.data, pre_sorted_rowids.data,
                                           slice_begin, slice_end, num_tag[row], num_version[row])) {
                        continue;
                    }

                    local.add(num_tag[row], num_version[row], num_value[row]);
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

            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }
            std::fprintf(out, "tag,version,cnt,total\n");

            if (!results.empty()) {
                DictData tag_dict;
                DictData version_dict;
                tag_dict.offsets.open(gendb_dir + "/shared/tag.offsets.bin");
                tag_dict.data.open(gendb_dir + "/shared/tag.data.bin");
                version_dict.offsets.open(gendb_dir + "/shared/version.offsets.bin");
                version_dict.data.open(gendb_dir + "/shared/version.data.bin");

                for (const ResultRow& row : results) {
                    const std::string_view tag_sv = tag_dict.at(row.tag);
                    const std::string_view version_sv = version_dict.at(row.version);
                    std::fprintf(out, "%.*s,%.*s,%llu,%.2f\n",
                                 int(tag_sv.size()), tag_sv.data(),
                                 int(version_sv.size()), version_sv.data(),
                                 static_cast<unsigned long long>(row.cnt),
                                 row.total);
                }
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q24 failed: %s\n", e.what());
        return 1;
    }
}
