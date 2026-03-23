#include <algorithm>
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
constexpr uint16_t kEmpty16 = std::numeric_limits<uint16_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kSicLo = 4000;
constexpr int32_t kSicHi = 4999;
constexpr size_t kTopK = 500;
constexpr size_t kMinPartitions = 256;

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct DictData {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    void open(const std::string& offsets_path, const std::string& data_path) {
        offsets.open(offsets_path);
        data.open(data_path);
    }

    std::string_view at(size_t idx) const {
        return std::string_view(data.data + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
    }
};

struct ResultRow {
    int32_t sic = 0;
    uint32_t tlabel = 0;
    uint32_t num_companies = 0;
    double total_value = 0.0;
    double avg_value = 0.0;
};

struct QualifiedRow {
    uint64_t group_key = 0;
    uint32_t cik = 0;
    double scaled_value = 0.0;
    uint32_t pre_count = 0;
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

uint16_t resolve_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary code not found");
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

inline uint64_t pack_group_key(int32_t sic, uint32_t tlabel) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(sic)) << 32) | tlabel;
}

inline size_t partition_for_key(uint64_t group_key, size_t partition_mask) {
    return static_cast<size_t>(mix64(group_key)) & partition_mask;
}

struct GroupAggMap {
    std::vector<uint64_t> keys;
    std::vector<double> sums;
    std::vector<uint64_t> counts;
    size_t mask = 0;
    size_t size_used = 0;

    void reset(size_t expected) {
        const size_t capacity =
            static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        keys.assign(capacity, kEmpty64);
        sums.assign(capacity, 0.0);
        counts.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint64_t> old_keys = std::move(keys);
        std::vector<double> old_sums = std::move(sums);
        std::vector<uint64_t> old_counts = std::move(counts);
        keys.assign(new_capacity, kEmpty64);
        sums.assign(new_capacity, 0.0);
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

    void add(uint64_t key, double sum_delta, uint64_t count_delta) {
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

    void reset(size_t expected) {
        const size_t capacity =
            static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
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

    void reset(size_t expected) {
        const size_t capacity =
            static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
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
        const uint64_t h = mix64(group_key ^ (static_cast<uint64_t>(cik) * 0x9e3779b97f4a7c15ULL));
        size_t slot = static_cast<size_t>(h) & mask;
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
};

struct PreCountHashProbe {
    const uint32_t* key_adsh = nullptr;
    const uint32_t* key_tag = nullptr;
    const uint32_t* key_version = nullptr;
    const uint16_t* key_stmt = nullptr;
    const uint32_t* counts = nullptr;
    size_t mask = 0;

    inline uint32_t probe(uint32_t adsh, uint32_t tag, uint32_t version, uint16_t stmt) const {
        uint64_t h = mix64(static_cast<uint64_t>(adsh) * 0x9e3779b185ebca87ULL ^
                           (static_cast<uint64_t>(tag) << 32) ^
                           static_cast<uint64_t>(version));
        h = mix64(h ^ (static_cast<uint64_t>(stmt) * 0xc2b2ae3d27d4eb4fULL));
        size_t slot = static_cast<size_t>(h) & mask;
        while (true) {
            if (key_adsh[slot] == kEmpty32) {
                return 0;
            }
            if (key_adsh[slot] == adsh && key_tag[slot] == tag &&
                key_version[slot] == version && key_stmt[slot] == stmt) {
                return counts[slot];
            }
            slot = (slot + 1) & mask;
        }
    }
};

struct ThreadPartitions {
    std::vector<std::vector<QualifiedRow>> partitions;
    uint32_t cached_adsh = 0;
    uint32_t cached_tag = 0;
    uint32_t cached_version = 0;
    uint32_t cached_pre_count = 0;
    uint32_t cached_tag_only = 0;
    uint32_t cached_version_only = 0;
    uint32_t cached_tag_row = kEmpty32;
    bool has_cached_pre = false;
    bool has_cached_tag = false;
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
        gendb::MmapColumn<ZoneRecord<uint16_t>> num_uom_zone;

        gendb::MmapColumn<int32_t> sub_sic;
        gendb::MmapColumn<int32_t> sub_cik;
        gendb::MmapColumn<uint32_t> sub_adsh_to_rowid;

        gendb::MmapColumn<uint32_t> tag_tlabel;
        gendb::MmapColumn<uint8_t> tag_abstract;
        gendb::MmapColumn<uint32_t> tag_hash_tag;
        gendb::MmapColumn<uint32_t> tag_hash_version;
        gendb::MmapColumn<uint32_t> tag_hash_rowid;

        gendb::MmapColumn<uint32_t> pre_count_key_adsh;
        gendb::MmapColumn<uint32_t> pre_count_key_tag;
        gendb::MmapColumn<uint32_t> pre_count_key_version;
        gendb::MmapColumn<uint16_t> pre_count_key_stmt;
        gendb::MmapColumn<uint32_t> pre_count_counts;

        {
            GENDB_PHASE("data_loading");
            num_uom_dict.open(gendb_dir + "/num/dict_uom.offsets.bin",
                              gendb_dir + "/num/dict_uom.data.bin");
            pre_stmt_dict.open(gendb_dir + "/pre/dict_stmt.offsets.bin",
                               gendb_dir + "/pre/dict_stmt.data.bin");
            tag_tlabel_dict.open(gendb_dir + "/tag/dict_tlabel.offsets.bin",
                                 gendb_dir + "/tag/dict_tlabel.data.bin");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_version.open(gendb_dir + "/num/version.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            num_uom_zone.open(gendb_dir + "/num/indexes/uom.zone_map.bin");

            sub_sic.open(gendb_dir + "/sub/sic.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");
            sub_adsh_to_rowid.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");

            tag_tlabel.open(gendb_dir + "/tag/tlabel.bin");
            tag_abstract.open(gendb_dir + "/tag/abstract.bin");
            tag_hash_tag.open(gendb_dir + "/tag/indexes/tag_version_hash.tag.bin");
            tag_hash_version.open(gendb_dir + "/tag/indexes/tag_version_hash.version.bin");
            tag_hash_rowid.open(gendb_dir + "/tag/indexes/tag_version_hash.rowid.bin");

            pre_count_key_adsh.open(
                gendb_dir + "/column_versions/pre.adsh_tag_version_stmt.count_hash/key_adsh.bin");
            pre_count_key_tag.open(
                gendb_dir + "/column_versions/pre.adsh_tag_version_stmt.count_hash/key_tag.bin");
            pre_count_key_version.open(
                gendb_dir + "/column_versions/pre.adsh_tag_version_stmt.count_hash/key_version.bin");
            pre_count_key_stmt.open(
                gendb_dir + "/column_versions/pre.adsh_tag_version_stmt.count_hash/key_stmt.bin");
            pre_count_counts.open(
                gendb_dir + "/column_versions/pre.adsh_tag_version_stmt.count_hash/counts.bin");

            if (num_adsh.size() != num_tag.size() || num_adsh.size() != num_version.size() ||
                num_adsh.size() != num_uom.size() || num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_sic.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
            if (tag_tlabel.size() != tag_abstract.size()) {
                throw std::runtime_error("tag column size mismatch");
            }
            if (tag_hash_tag.size() != tag_hash_version.size() ||
                tag_hash_tag.size() != tag_hash_rowid.size()) {
                throw std::runtime_error("tag hash size mismatch");
            }
            if (pre_count_key_adsh.size() != pre_count_key_tag.size() ||
                pre_count_key_adsh.size() != pre_count_key_version.size() ||
                pre_count_key_adsh.size() != pre_count_key_stmt.size() ||
                pre_count_key_adsh.size() != pre_count_counts.size()) {
                throw std::runtime_error("pre count hash size mismatch");
            }
        }

        uint16_t usd_code = 0;
        uint16_t eq_code = 0;
        std::vector<uint32_t> num_blocks;

        {
            GENDB_PHASE("dim_filter");
            usd_code = resolve_code(num_uom_dict, "USD");
            eq_code = resolve_code(pre_stmt_dict, "EQ");

            const size_t num_block_count = (num_uom.size() + kBlockSize - 1) / kBlockSize;
            if (num_uom_zone.size() != num_block_count) {
                throw std::runtime_error("num zone map block count mismatch");
            }

            num_blocks.reserve(num_uom_zone.size());
            for (size_t block = 0; block < num_uom_zone.size(); ++block) {
                const ZoneRecord<uint16_t> zone = num_uom_zone[block];
                if (zone.min <= usd_code && usd_code <= zone.max) {
                    num_blocks.push_back(static_cast<uint32_t>(block));
                }
            }
        }

        PreCountHashProbe pre_probe;
        {
            GENDB_PHASE("build_joins");
            if (pre_count_key_adsh.size() == 0 ||
                (pre_count_key_adsh.size() & (pre_count_key_adsh.size() - 1)) != 0) {
                throw std::runtime_error("pre count hash capacity must be a power of two");
            }
            pre_probe.key_adsh = pre_count_key_adsh.data;
            pre_probe.key_tag = pre_count_key_tag.data;
            pre_probe.key_version = pre_count_key_version.data;
            pre_probe.key_stmt = pre_count_key_stmt.data;
            pre_probe.counts = pre_count_counts.data;
            pre_probe.mask = pre_count_key_adsh.size() - 1;

            num_adsh.advise_sequential();
            num_tag.advise_sequential();
            num_version.advise_sequential();
            num_uom.advise_sequential();
            num_value.advise_sequential();

            sub_adsh_to_rowid.advise_random();
            sub_sic.advise_random();
            sub_cik.advise_random();

            pre_count_key_adsh.advise_random();
            pre_count_key_tag.advise_random();
            pre_count_key_version.advise_random();
            pre_count_key_stmt.advise_random();
            pre_count_counts.advise_random();

            tag_hash_tag.advise_random();
            tag_hash_version.advise_random();
            tag_hash_rowid.advise_random();
            tag_tlabel.advise_random();
            tag_abstract.advise_random();

            gendb::mmap_prefetch_all(num_adsh, num_tag, num_version, num_uom, num_value,
                                     sub_adsh_to_rowid, sub_sic, sub_cik,
                                     pre_count_key_adsh, pre_count_key_tag, pre_count_key_version,
                                     pre_count_key_stmt, pre_count_counts,
                                     tag_hash_tag, tag_hash_version, tag_hash_rowid,
                                     tag_tlabel, tag_abstract);
        }

        const int n_threads = std::max(1, omp_get_max_threads());
        const size_t partition_count = static_cast<size_t>(
            next_power_of_two(std::max<uint64_t>(kMinPartitions, static_cast<uint64_t>(n_threads) * 4ULL)));
        const size_t partition_mask = partition_count - 1;
        std::vector<ThreadPartitions> thread_partitions(static_cast<size_t>(n_threads));
        for (ThreadPartitions& tp : thread_partitions) {
            tp.partitions.resize(partition_count);
        }

        std::vector<ResultRow> results;

        {
            GENDB_PHASE("main_scan");
            const uint32_t* num_adsh_ptr = num_adsh.data;
            const uint32_t* num_tag_ptr = num_tag.data;
            const uint32_t* num_version_ptr = num_version.data;
            const uint16_t* num_uom_ptr = num_uom.data;
            const double* num_value_ptr = num_value.data;
            const uint32_t* sub_lookup_ptr = sub_adsh_to_rowid.data;
            const size_t sub_lookup_size = sub_adsh_to_rowid.size();
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
                ThreadPartitions& local = thread_partitions[static_cast<size_t>(omp_get_thread_num())];

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
                        if (adsh >= sub_lookup_size) {
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

                        uint32_t pre_count = 0;
                        if (local.has_cached_pre && local.cached_adsh == adsh &&
                            local.cached_tag == tag && local.cached_version == version) {
                            pre_count = local.cached_pre_count;
                        } else {
                            pre_count = pre_probe.probe(adsh, tag, version, eq_code);
                            local.cached_adsh = adsh;
                            local.cached_tag = tag;
                            local.cached_version = version;
                            local.cached_pre_count = pre_count;
                            local.has_cached_pre = true;
                        }
                        if (pre_count == 0) {
                            continue;
                        }

                        uint32_t tag_row = kEmpty32;
                        if (local.has_cached_tag && local.cached_tag_only == tag &&
                            local.cached_version_only == version) {
                            tag_row = local.cached_tag_row;
                        } else {
                            tag_row = probe_tag_rowid(tag_hash_tag_ptr, tag_hash_version_ptr,
                                                      tag_hash_rowid_ptr, tag_hash_mask,
                                                      tag, version);
                            local.cached_tag_only = tag;
                            local.cached_version_only = version;
                            local.cached_tag_row = tag_row;
                            local.has_cached_tag = true;
                        }
                        if (tag_row == kEmpty32 || tag_abstract_ptr[tag_row] != 0) {
                            continue;
                        }

                        const uint64_t group_key = pack_group_key(sic, tag_tlabel_ptr[tag_row]);
                        const size_t partition_id = partition_for_key(group_key, partition_mask);
                        local.partitions[partition_id].push_back(QualifiedRow{
                            group_key,
                            static_cast<uint32_t>(sub_cik_ptr[sub_row]),
                            num_value_ptr[row] * static_cast<double>(pre_count),
                            pre_count,
                        });
                    }
                }
            }

            std::vector<std::vector<ResultRow>> partition_results(static_cast<size_t>(n_threads));

            #pragma omp parallel for schedule(dynamic, 1) num_threads(n_threads)
            for (size_t partition_id = 0; partition_id < partition_count; ++partition_id) {
                size_t rows_in_partition = 0;
                for (const ThreadPartitions& tp : thread_partitions) {
                    rows_in_partition += tp.partitions[partition_id].size();
                }
                if (rows_in_partition == 0) {
                    continue;
                }

                GroupAggMap groups;
                CountMap company_counts;
                DistinctPairSet distinct_pairs;
                groups.reset(std::max<size_t>(16, rows_in_partition / 4));
                company_counts.reset(std::max<size_t>(16, rows_in_partition / 8));
                distinct_pairs.reset(std::max<size_t>(16, rows_in_partition));

                for (const ThreadPartitions& tp : thread_partitions) {
                    const std::vector<QualifiedRow>& rows = tp.partitions[partition_id];
                    for (const QualifiedRow& qr : rows) {
                        groups.add(qr.group_key, qr.scaled_value, static_cast<uint64_t>(qr.pre_count));
                        if (distinct_pairs.insert(qr.group_key, qr.cik)) {
                            company_counts.add(qr.group_key, 1);
                        }
                    }
                }

                std::vector<ResultRow>& out = partition_results[static_cast<size_t>(omp_get_thread_num())];
                groups.for_each([&](uint64_t key, double total_value, uint64_t row_count) {
                    const uint32_t num_companies = company_counts.get(key);
                    if (num_companies < 2 || row_count == 0) {
                        return;
                    }
                    out.push_back(ResultRow{
                        static_cast<int32_t>(static_cast<uint32_t>(key >> 32)),
                        static_cast<uint32_t>(key),
                        num_companies,
                        total_value,
                        total_value / static_cast<double>(row_count),
                    });
                });
            }

            size_t total_results = 0;
            for (const auto& chunk : partition_results) {
                total_results += chunk.size();
            }
            results.reserve(total_results);
            for (auto& chunk : partition_results) {
                results.insert(results.end(), chunk.begin(), chunk.end());
            }
        }

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
                std::fprintf(out, ",%u,%.2f,%.2f\n",
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
