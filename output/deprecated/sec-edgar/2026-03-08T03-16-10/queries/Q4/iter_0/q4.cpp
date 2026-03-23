#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <limits>
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

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kSicLo = 4000;
constexpr int32_t kSicHi = 4999;
constexpr size_t kTopK = 500;
constexpr size_t kInitPreExpected = 32768;
constexpr size_t kInitGroupExpected = 16384;
constexpr size_t kInitDistinctExpected = 65536;
constexpr size_t kInitCompanyExpected = 16384;

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

struct ResultRow {
    int32_t sic = 0;
    uint32_t tlabel = 0;
    uint32_t num_companies = 0;
    long double total_value = 0.0L;
    long double avg_value = 0.0L;
};

uint64_t next_power_of_two(uint64_t x) {
    uint64_t v = 1;
    while (v < x) {
        v <<= 1;
    }
    return v;
}

uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

DictData load_dict(const std::string& offsets_path, const std::string& data_path) {
    DictData dict;
    dict.offsets.open(offsets_path);
    dict.data.open(data_path);
    return dict;
}

uint16_t resolve_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary code not found");
}

template <typename T>
std::vector<ZoneRecord<T>> load_zone_map(const std::string& path) {
    gendb::MmapColumn<ZoneRecord<T>> zones(path);
    return std::vector<ZoneRecord<T>>(zones.data, zones.data + zones.size());
}

void csv_write_escaped(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
            break;
        }
    }

    if (!needs_quotes) {
        std::fwrite(value.data(), 1, value.size(), out);
        return;
    }

    std::fputc('"', out);
    for (char ch : value) {
        if (ch == '"') {
            std::fputc('"', out);
        }
        std::fputc(ch, out);
    }
    std::fputc('"', out);
}

inline uint64_t hash_pre_key(uint32_t adsh, uint32_t tag, uint32_t version) {
    uint64_t h = mix64((static_cast<uint64_t>(adsh) << 32) | tag);
    return mix64(h ^ version);
}

inline uint64_t pack_group_key(int32_t sic, uint32_t tlabel) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(sic)) << 32) | tlabel;
}

struct PreHashMap {
    std::vector<uint32_t> adshs;
    std::vector<uint32_t> tags;
    std::vector<uint32_t> versions;
    std::vector<uint16_t> counts;
    size_t mask = 0;
    size_t size_used = 0;

    PreHashMap() {
        reset(kInitPreExpected);
    }

    explicit PreHashMap(size_t expected) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        adshs.assign(capacity, kEmpty32);
        tags.assign(capacity, 0);
        versions.assign(capacity, 0);
        counts.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint32_t> old_adshs = std::move(adshs);
        std::vector<uint32_t> old_tags = std::move(tags);
        std::vector<uint32_t> old_versions = std::move(versions);
        std::vector<uint16_t> old_counts = std::move(counts);
        adshs.assign(new_capacity, kEmpty32);
        tags.assign(new_capacity, 0);
        versions.assign(new_capacity, 0);
        counts.assign(new_capacity, 0);
        mask = new_capacity - 1;
        size_used = 0;
        for (size_t i = 0; i < old_adshs.size(); ++i) {
            if (old_adshs[i] != kEmpty32) {
                add_count(old_adshs[i], old_tags[i], old_versions[i], old_counts[i]);
            }
        }
    }

    void maybe_grow() {
        if ((size_used + 1) * 10 >= adshs.size() * 7) {
            rehash(adshs.size() * 2);
        }
    }

    void add_count(uint32_t adsh, uint32_t tag, uint32_t version, uint16_t delta = 1) {
        maybe_grow();
        size_t slot = static_cast<size_t>(hash_pre_key(adsh, tag, version)) & mask;
        while (true) {
            if (adshs[slot] == kEmpty32) {
                adshs[slot] = adsh;
                tags[slot] = tag;
                versions[slot] = version;
                counts[slot] = delta;
                ++size_used;
                return;
            }
            if (adshs[slot] == adsh && tags[slot] == tag && versions[slot] == version) {
                const uint32_t next = static_cast<uint32_t>(counts[slot]) + delta;
                if (next > std::numeric_limits<uint16_t>::max()) {
                    throw std::runtime_error("pre eq_count overflow");
                }
                counts[slot] = static_cast<uint16_t>(next);
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    uint16_t find_count(uint32_t adsh, uint32_t tag, uint32_t version) const {
        size_t slot = static_cast<size_t>(hash_pre_key(adsh, tag, version)) & mask;
        while (true) {
            if (adshs[slot] == kEmpty32) {
                return 0;
            }
            if (adshs[slot] == adsh && tags[slot] == tag && versions[slot] == version) {
                return counts[slot];
            }
            slot = (slot + 1) & mask;
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < adshs.size(); ++i) {
            if (adshs[i] != kEmpty32) {
                fn(adshs[i], tags[i], versions[i], counts[i]);
            }
        }
    }
};

struct GroupAggMap {
    std::vector<uint64_t> keys;
    std::vector<long double> sums;
    std::vector<uint64_t> counts;
    size_t mask = 0;
    size_t size_used = 0;

    GroupAggMap() {
        reset(kInitGroupExpected);
    }

    explicit GroupAggMap(size_t expected) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        keys.assign(capacity, kEmpty64);
        sums.assign(capacity, 0.0L);
        counts.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint64_t> old_keys = std::move(keys);
        std::vector<long double> old_sums = std::move(sums);
        std::vector<uint64_t> old_counts = std::move(counts);
        keys.assign(new_capacity, kEmpty64);
        sums.assign(new_capacity, 0.0L);
        counts.assign(new_capacity, 0);
        mask = new_capacity - 1;
        size_used = 0;
        for (size_t i = 0; i < old_keys.size(); ++i) {
            if (old_keys[i] != kEmpty64) {
                add(old_keys[i], old_sums[i], old_counts[i]);
            }
        }
    }

    void maybe_grow() {
        if ((size_used + 1) * 10 >= keys.size() * 7) {
            rehash(keys.size() * 2);
        }
    }

    void add(uint64_t key, long double sum_delta, uint64_t count_delta) {
        maybe_grow();
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            if (keys[slot] == kEmpty64) {
                keys[slot] = key;
                sums[slot] = sum_delta;
                counts[slot] = count_delta;
                ++size_used;
                return;
            }
            if (keys[slot] == key) {
                sums[slot] += sum_delta;
                counts[slot] += count_delta;
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    uint64_t find_count(uint64_t key) const {
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            if (keys[slot] == kEmpty64) {
                return 0;
            }
            if (keys[slot] == key) {
                return counts[slot];
            }
            slot = (slot + 1) & mask;
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] != kEmpty64) {
                fn(keys[i], sums[i], counts[i]);
            }
        }
    }
};

struct CountMap {
    std::vector<uint64_t> keys;
    std::vector<uint32_t> values;
    size_t mask = 0;
    size_t size_used = 0;

    CountMap() {
        reset(kInitCompanyExpected);
    }

    explicit CountMap(size_t expected) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        keys.assign(capacity, kEmpty64);
        values.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint64_t> old_keys = std::move(keys);
        std::vector<uint32_t> old_values = std::move(values);
        keys.assign(new_capacity, kEmpty64);
        values.assign(new_capacity, 0);
        mask = new_capacity - 1;
        size_used = 0;
        for (size_t i = 0; i < old_keys.size(); ++i) {
            if (old_keys[i] != kEmpty64) {
                add(old_keys[i], old_values[i]);
            }
        }
    }

    void maybe_grow() {
        if ((size_used + 1) * 10 >= keys.size() * 7) {
            rehash(keys.size() * 2);
        }
    }

    void add(uint64_t key, uint32_t delta) {
        maybe_grow();
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            if (keys[slot] == kEmpty64) {
                keys[slot] = key;
                values[slot] = delta;
                ++size_used;
                return;
            }
            if (keys[slot] == key) {
                values[slot] += delta;
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    uint32_t get(uint64_t key) const {
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            if (keys[slot] == kEmpty64) {
                return 0;
            }
            if (keys[slot] == key) {
                return values[slot];
            }
            slot = (slot + 1) & mask;
        }
    }
};

struct DistinctPairSet {
    std::vector<uint64_t> group_keys;
    std::vector<uint32_t> ciks;
    size_t mask = 0;
    size_t size_used = 0;

    DistinctPairSet() {
        reset(kInitDistinctExpected);
    }

    explicit DistinctPairSet(size_t expected) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        group_keys.assign(capacity, kEmpty64);
        ciks.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint64_t> old_group_keys = std::move(group_keys);
        std::vector<uint32_t> old_ciks = std::move(ciks);
        group_keys.assign(new_capacity, kEmpty64);
        ciks.assign(new_capacity, 0);
        mask = new_capacity - 1;
        size_used = 0;
        for (size_t i = 0; i < old_group_keys.size(); ++i) {
            if (old_group_keys[i] != kEmpty64) {
                insert(old_group_keys[i], old_ciks[i]);
            }
        }
    }

    void maybe_grow() {
        if ((size_used + 1) * 10 >= group_keys.size() * 7) {
            rehash(group_keys.size() * 2);
        }
    }

    bool insert(uint64_t group_key, uint32_t cik) {
        maybe_grow();
        uint64_t hash = mix64(group_key ^ (static_cast<uint64_t>(cik) * 0x9e3779b97f4a7c15ULL));
        size_t slot = static_cast<size_t>(hash) & mask;
        while (true) {
            if (group_keys[slot] == kEmpty64) {
                group_keys[slot] = group_key;
                ciks[slot] = cik;
                ++size_used;
                return true;
            }
            if (group_keys[slot] == group_key && ciks[slot] == cik) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < group_keys.size(); ++i) {
            if (group_keys[i] != kEmpty64) {
                fn(group_keys[i], ciks[i]);
            }
        }
    }
};

struct ThreadAgg {
    GroupAggMap groups;
    DistinctPairSet distinct;
};

inline uint32_t probe_tag_rowid(const uint32_t* key_tag, const uint32_t* key_version,
                                const uint32_t* rowids, size_t mask,
                                uint32_t tag, uint32_t version) {
    size_t slot = static_cast<size_t>(mix64((static_cast<uint64_t>(tag) << 32) | version)) & mask;
    while (true) {
        const uint32_t rowid = rowids[slot];
        if (rowid == kEmpty32) {
            return kEmpty32;
        }
        if (key_tag[slot] == tag && key_version[slot] == version) {
            return rowid;
        }
        slot = (slot + 1) & mask;
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];

        GENDB_PHASE("total");

        DictData num_uom_dict;
        DictData pre_stmt_dict;
        DictData tag_tlabel_dict;

        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;

        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;
        gendb::MmapColumn<uint32_t> sub_adsh_to_rowid;

        gendb::MmapColumn<uint32_t> tag_tlabel;
        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_hash_tag;
        gendb::MmapColumn<uint32_t> tag_hash_version;
        gendb::MmapColumn<uint32_t> tag_hash_rowid;

        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<uint16_t> pre_stmt;

        std::vector<ZoneRecord<uint16_t>> num_uom_zone;
        std::vector<ZoneRecord<uint16_t>> pre_stmt_zone;

        {
            GENDB_PHASE("data_loading");
            num_uom_dict = load_dict(gendb_dir + "/num/dict_uom.offsets.bin",
                                     gendb_dir + "/num/dict_uom.data.bin");
            pre_stmt_dict = load_dict(gendb_dir + "/pre/dict_stmt.offsets.bin",
                                      gendb_dir + "/pre/dict_stmt.data.bin");
            tag_tlabel_dict = load_dict(gendb_dir + "/tag/dict_tlabel.offsets.bin",
                                        gendb_dir + "/tag/dict_tlabel.data.bin");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");
            sub_adsh_to_rowid.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");

            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");
            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_hash_tag.open(gendb_dir + "/tag/indexes/tag_version_hash.tag.bin");
            tag_hash_version.open(gendb_dir + "/tag/indexes/tag_version_hash.version.bin");
            tag_hash_rowid.open(gendb_dir + "/tag/indexes/tag_version_hash.rowid.bin");

            pre_adsh.open(gendb_dir + "/pre/adsh.bin");
            pre_tag.open(gendb_dir + "/pre/tag.bin");
            pre_version.open(gendb_dir + "/pre/version.bin");
            pre_stmt.open(gendb_dir + "/pre/stmt.bin");

            num_uom_zone = load_zone_map<uint16_t>(gendb_dir + "/num/indexes/uom.zone_map.bin");
            pre_stmt_zone = load_zone_map<uint16_t>(gendb_dir + "/pre/indexes/stmt.zone_map.bin");

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_uom.size() || num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (pre_adsh.size() != pre_tag.size() || pre_adsh.size() != pre_version.size() ||
                pre_adsh.size() != pre_stmt.size()) {
                throw std::runtime_error("pre column size mismatch");
            }
            if (tag_tlabel.size() != tag_abstract.size()) {
                throw std::runtime_error("tag column size mismatch");
            }
            if (tag_hash_tag.size() != tag_hash_version.size() ||
                tag_hash_tag.size() != tag_hash_rowid.size()) {
                throw std::runtime_error("tag hash size mismatch");
            }
        }

        uint16_t usd_code = 0;
        uint16_t eq_code = 0;
        std::vector<uint32_t> num_blocks;
        std::vector<uint32_t> pre_blocks;

        {
            GENDB_PHASE("dim_filter");
            usd_code = resolve_code(num_uom_dict, "USD");
            eq_code = resolve_code(pre_stmt_dict, "EQ");

            const size_t num_block_count = (num_uom.size() + kBlockSize - 1) / kBlockSize;
            const size_t pre_block_count = (pre_stmt.size() + kBlockSize - 1) / kBlockSize;
            if (num_uom_zone.size() != num_block_count) {
                throw std::runtime_error("num uom zone map block count mismatch");
            }
            if (pre_stmt_zone.size() != pre_block_count) {
                throw std::runtime_error("pre stmt zone map block count mismatch");
            }

            num_blocks.reserve(num_uom_zone.size());
            for (size_t block = 0; block < num_uom_zone.size(); ++block) {
                const auto& zone = num_uom_zone[block];
                if (zone.min <= usd_code && usd_code <= zone.max) {
                    num_blocks.push_back(static_cast<uint32_t>(block));
                }
            }

            pre_blocks.reserve(pre_stmt_zone.size());
            for (size_t block = 0; block < pre_stmt_zone.size(); ++block) {
                const auto& zone = pre_stmt_zone[block];
                if (zone.min <= eq_code && eq_code <= zone.max) {
                    pre_blocks.push_back(static_cast<uint32_t>(block));
                }
            }
        }

        PreHashMap pre_eq_count_hash(1 << 20);

        {
            GENDB_PHASE("build_joins");
            const int n_threads = std::max(1, omp_get_max_threads());
            std::vector<PreHashMap> local_pre_maps;
            local_pre_maps.reserve(n_threads);
            for (int i = 0; i < n_threads; ++i) {
                local_pre_maps.emplace_back(kInitPreExpected);
            }

            const uint16_t* pre_stmt_ptr = pre_stmt.data;
            const uint32_t* pre_adsh_ptr = pre_adsh.data;
            const uint32_t* pre_tag_ptr = pre_tag.data;
            const uint32_t* pre_version_ptr = pre_version.data;
            const size_t pre_rows = pre_stmt.size();

            #pragma omp parallel num_threads(n_threads)
            {
                PreHashMap& local = local_pre_maps[omp_get_thread_num()];

                #pragma omp for schedule(dynamic, 1)
                for (size_t block_idx = 0; block_idx < pre_blocks.size(); ++block_idx) {
                    const size_t block = pre_blocks[block_idx];
                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(pre_rows, begin + kBlockSize);
                    for (size_t row = begin; row < end; ++row) {
                        if (pre_stmt_ptr[row] != eq_code) {
                            continue;
                        }
                        local.add_count(pre_adsh_ptr[row], pre_tag_ptr[row], pre_version_ptr[row], 1);
                    }
                }
            }

            size_t expected = 0;
            for (const PreHashMap& local : local_pre_maps) {
                expected += local.size_used;
            }
            pre_eq_count_hash.reset(std::max<size_t>(1 << 20, expected));
            for (const PreHashMap& local : local_pre_maps) {
                local.for_each([&](uint32_t adsh, uint32_t tag, uint32_t version, uint16_t count) {
                    pre_eq_count_hash.add_count(adsh, tag, version, count);
                });
            }

            num_adsh.advise_sequential();
            num_tag.advise_sequential();
            num_version.advise_sequential();
            num_uom.advise_sequential();
            num_value.advise_sequential();
            sub_adsh_to_rowid.advise_random();
            sub_sic.advise_random();
            sub_cik.advise_random();
            tag_hash_tag.advise_random();
            tag_hash_version.advise_random();
            tag_hash_rowid.advise_random();
            tag_tlabel.advise_random();
            tag_abstract.advise_random();
            gendb::mmap_prefetch_all(num_adsh, num_tag, num_version, num_uom, num_value,
                                     sub_adsh_to_rowid, sub_sic, sub_cik,
                                     tag_hash_tag, tag_hash_version, tag_hash_rowid,
                                     tag_tlabel, tag_abstract);
        }

        std::vector<ThreadAgg> thread_aggs;
        const int n_threads = std::max(1, omp_get_max_threads());
        thread_aggs.resize(static_cast<size_t>(n_threads));

        {
            GENDB_PHASE("main_scan");
            const uint32_t* num_adsh_ptr = num_adsh.data;
            const uint32_t* num_tag_ptr = num_tag.data;
            const uint32_t* num_version_ptr = num_version.data;
            const uint16_t* num_uom_ptr = num_uom.data;
            const double* num_value_ptr = num_value.data;
            const uint32_t* sub_lookup_ptr = sub_adsh_to_rowid.data;
            const int32_t* sub_sic_ptr = sub_sic.data;
            const int32_t* sub_cik_ptr = sub_cik.data;
            const uint32_t* tag_hash_tag_ptr = tag_hash_tag.data;
            const uint32_t* tag_hash_version_ptr = tag_hash_version.data;
            const uint32_t* tag_hash_rowid_ptr = tag_hash_rowid.data;
            const uint32_t* tag_tlabel_ptr = tag_tlabel.data;
            const uint8_t* tag_abstract_ptr = tag_abstract.data;
            const size_t tag_hash_mask = tag_hash_rowid.size() - 1;
            const size_t num_rows = num_uom.size();

            #pragma omp parallel num_threads(n_threads)
            {
                ThreadAgg& local = thread_aggs[omp_get_thread_num()];

                #pragma omp for schedule(dynamic, 1)
                for (size_t block_idx = 0; block_idx < num_blocks.size(); ++block_idx) {
                    const size_t block = num_blocks[block_idx];
                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(num_rows, begin + kBlockSize);
                    for (size_t row = begin; row < end; ++row) {
                        if (num_uom_ptr[row] != usd_code) {
                            continue;
                        }

                        const uint32_t adsh = num_adsh_ptr[row];
                        if (adsh >= sub_adsh_to_rowid.size()) {
                            continue;
                        }

                        const uint32_t sub_row = sub_lookup_ptr[adsh];
                        if (sub_row == kEmpty32) {
                            continue;
                        }

                        const int32_t sic = sub_sic_ptr[sub_row];
                        if (sic < kSicLo || sic > kSicHi) {
                            continue;
                        }

                        const uint32_t tag = num_tag_ptr[row];
                        const uint32_t version = num_version_ptr[row];
                        const uint32_t tag_row = probe_tag_rowid(tag_hash_tag_ptr, tag_hash_version_ptr,
                                                                 tag_hash_rowid_ptr, tag_hash_mask,
                                                                 tag, version);
                        if (tag_row == kEmpty32) {
                            continue;
                        }
                        if (tag_abstract_ptr[tag_row] != 0) {
                            continue;
                        }

                        const uint16_t eq_count = pre_eq_count_hash.find_count(adsh, tag, version);
                        if (eq_count == 0) {
                            continue;
                        }

                        const uint32_t tlabel = tag_tlabel_ptr[tag_row];
                        const uint64_t group_key = pack_group_key(sic, tlabel);
                        const long double value = static_cast<long double>(num_value_ptr[row]) *
                                                  static_cast<long double>(eq_count);
                        local.groups.add(group_key, value, eq_count);
                        local.distinct.insert(group_key,
                                              static_cast<uint32_t>(sub_cik_ptr[sub_row]));
                    }
                }
            }
        }

        GroupAggMap merged_groups(1 << 15);
        DistinctPairSet merged_distinct(1 << 16);
        CountMap company_counts(1 << 15);

        for (const ThreadAgg& local : thread_aggs) {
            local.groups.for_each([&](uint64_t key, long double sum_value, uint64_t row_count) {
                merged_groups.add(key, sum_value, row_count);
            });
            local.distinct.for_each([&](uint64_t key, uint32_t cik) {
                if (merged_distinct.insert(key, cik)) {
                    company_counts.add(key, 1);
                }
            });
        }

        std::vector<ResultRow> results;
        results.reserve(merged_groups.size_used);
        merged_groups.for_each([&](uint64_t key, long double total_value, uint64_t row_count) {
            const uint32_t num_companies = company_counts.get(key);
            if (num_companies < 2 || row_count == 0) {
                return;
            }
            results.push_back(ResultRow{
                static_cast<int32_t>(static_cast<uint32_t>(key >> 32)),
                static_cast<uint32_t>(key),
                num_companies,
                total_value,
                total_value / static_cast<long double>(row_count)
            });
        });

        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.total_value != b.total_value) {
                return a.total_value > b.total_value;
            }
            if (a.sic != b.sic) {
                return a.sic < b.sic;
            }
            return a.tlabel < b.tlabel;
        };
        if (results.size() > kTopK) {
            std::partial_sort(results.begin(), results.begin() + kTopK, results.end(), cmp);
            results.resize(kTopK);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }

        {
            GENDB_PHASE("output");
            fs::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q4.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file: " + out_path);
            }

            const std::string_view stmt_value = pre_stmt_dict.at(eq_code);
            std::fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            for (const ResultRow& row : results) {
                std::fprintf(out, "%d,", row.sic);
                csv_write_escaped(out, tag_tlabel_dict.at(row.tlabel));
                std::fputc(',', out);
                csv_write_escaped(out, stmt_value);
                std::fprintf(out, ",%u,%.2Lf,%.2Lf\n",
                             row.num_companies, row.total_value, row.avg_value);
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
