#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "mmap_utils.h"

namespace fs = std::filesystem;

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr size_t kBlockSize = 100000;

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

    std::string_view at(uint32_t code) const {
        return std::string_view(
            data.data + offsets[code],
            static_cast<size_t>(offsets[code + 1] - offsets[code])
        );
    }
};

static uint16_t find_dict_code(const DictData& dict, std::string_view needle) {
    const size_t n = dict.offsets.size() - 1;
    for (size_t i = 0; i < n; ++i) {
        if (dict.at(static_cast<uint32_t>(i)) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary value not found");
}

static inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static inline uint64_t hash_key3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = mix64(a);
    h ^= mix64((uint64_t(b) << 1) | 1ULL);
    h ^= mix64((uint64_t(c) << 1) | 3ULL);
    return mix64(h);
}

struct KeyEntry {
    uint32_t adsh = 0;
    uint32_t tag = 0;
    uint32_t version = 0;
    uint32_t sub_row = 0;
    uint64_t count = 0;
    bool occupied = false;
};

class KeyHashSet {
public:
    explicit KeyHashSet(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        table_.assign(cap, KeyEntry{});
        mask_ = cap - 1;
    }

    KeyEntry& find_or_insert(uint32_t adsh, uint32_t tag, uint32_t version, uint32_t sub_row) {
        size_t slot = hash_key3(adsh, tag, version) & mask_;
        for (;;) {
            KeyEntry& entry = table_[slot];
            if (!entry.occupied) {
                entry.occupied = true;
                entry.adsh = adsh;
                entry.tag = tag;
                entry.version = version;
                entry.sub_row = sub_row;
                ++size_;
                return entry;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    KeyEntry* find(uint32_t adsh, uint32_t tag, uint32_t version) {
        size_t slot = hash_key3(adsh, tag, version) & mask_;
        for (;;) {
            KeyEntry& entry = table_[slot];
            if (!entry.occupied) {
                return nullptr;
            }
            if (entry.adsh == adsh && entry.tag == tag && entry.version == version) {
                return &entry;
            }
            slot = (slot + 1) & mask_;
        }
    }

    const std::vector<KeyEntry>& entries() const { return table_; }
    size_t size() const { return size_; }

private:
    std::vector<KeyEntry> table_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

static inline int compare_key(const uint32_t* adsh, const uint32_t* tag, const uint32_t* version,
                              const uint32_t* rowids, size_t pos,
                              uint32_t key_adsh, uint32_t key_tag, uint32_t key_version) {
    const uint32_t row = rowids[pos];
    if (adsh[row] < key_adsh) return -1;
    if (adsh[row] > key_adsh) return 1;
    if (tag[row] < key_tag) return -1;
    if (tag[row] > key_tag) return 1;
    if (version[row] < key_version) return -1;
    if (version[row] > key_version) return 1;
    return 0;
}

static inline size_t lower_bound_key(const uint32_t* adsh, const uint32_t* tag, const uint32_t* version,
                                     const uint32_t* rowids, size_t row_count,
                                     uint32_t key_adsh, uint32_t key_tag, uint32_t key_version) {
    size_t lo = 0;
    size_t hi = row_count;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if (compare_key(adsh, tag, version, rowids, mid, key_adsh, key_tag, key_version) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static inline size_t upper_bound_key(const uint32_t* adsh, const uint32_t* tag, const uint32_t* version,
                                     const uint32_t* rowids, size_t row_count,
                                     uint32_t key_adsh, uint32_t key_tag, uint32_t key_version) {
    size_t lo = 0;
    size_t hi = row_count;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if (compare_key(adsh, tag, version, rowids, mid, key_adsh, key_tag, key_version) <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

template <class Fn>
double time_ms(Fn&& fn) {
    const auto start = std::chrono::steady_clock::now();
    fn();
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];

        DictData num_uom_dict;
        DictData pre_stmt_dict;
        num_uom_dict.open(gendb_dir + "/num/dict_uom.offsets.bin", gendb_dir + "/num/dict_uom.data.bin");
        pre_stmt_dict.open(gendb_dir + "/pre/dict_stmt.offsets.bin", gendb_dir + "/pre/dict_stmt.data.bin");
        const uint16_t usd_code = find_dict_code(num_uom_dict, "USD");
        const uint16_t is_code = find_dict_code(pre_stmt_dict, "IS");

        gendb::MmapColumn<uint32_t> sub_adsh_lookup;
        gendb::MmapColumn<int32_t> sub_fy;
        sub_adsh_lookup.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");
        sub_fy.open(gendb_dir + "/sub/fy.bin");

        std::vector<uint32_t> sub_lookup_2023(sub_adsh_lookup.size(), kEmpty32);
        size_t sub_rows_2023 = 0;
        for (size_t adsh = 0; adsh < sub_adsh_lookup.size(); ++adsh) {
            const uint32_t row = sub_adsh_lookup[adsh];
            if (row != kEmpty32 && sub_fy[row] == 2023) {
                sub_lookup_2023[adsh] = row;
                ++sub_rows_2023;
            }
        }

        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<uint32_t> pre_tag;
        gendb::MmapColumn<uint32_t> pre_version;
        gendb::MmapColumn<ZoneRecord<uint16_t>> pre_stmt_zone;
        gendb::MmapColumn<uint32_t> pre_rowids;
        pre_stmt.open(gendb_dir + "/pre/stmt.bin");
        pre_adsh.open(gendb_dir + "/pre/adsh.bin");
        pre_tag.open(gendb_dir + "/pre/tag.bin");
        pre_version.open(gendb_dir + "/pre/version.bin");
        pre_stmt_zone.open(gendb_dir + "/pre/indexes/stmt.zone_map.bin");
        pre_rowids.open(gendb_dir + "/pre/indexes/adsh_tag_version.rowids.bin");

        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint32_t> num_tag;
        gendb::MmapColumn<uint32_t> num_version;
        gendb::MmapColumn<uint32_t> num_rowids;
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_tag.open(gendb_dir + "/num/tag.bin");
        num_version.open(gendb_dir + "/num/version.bin");
        num_rowids.open(gendb_dir + "/num/indexes/adsh_tag_version.rowids.bin");

        size_t pre_filtered_rows = 0;
        size_t current_distinct_keys = 0;
        size_t current_join_rows = 0;
        const double current_ms = time_ms([&] {
            KeyHashSet current_keys(1u << 20);
            const size_t pre_rows = pre_stmt.size();
            const size_t pre_blocks = (pre_rows + kBlockSize - 1) / kBlockSize;
            for (size_t block = 0; block < pre_blocks; ++block) {
                const auto z = pre_stmt_zone[block];
                if (z.max < is_code || z.min > is_code) {
                    continue;
                }
                const size_t begin = block * kBlockSize;
                const size_t end = std::min(begin + kBlockSize, pre_rows);
                for (size_t row = begin; row < end; ++row) {
                    if (pre_stmt[row] != is_code) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[row];
                    if (adsh >= sub_lookup_2023.size()) {
                        continue;
                    }
                    const uint32_t sub_row = sub_lookup_2023[adsh];
                    if (sub_row == kEmpty32) {
                        continue;
                    }
                    KeyEntry& entry = current_keys.find_or_insert(adsh, pre_tag[row], pre_version[row], sub_row);
                    ++entry.count;
                    ++pre_filtered_rows;
                }
            }
            current_distinct_keys = current_keys.size();

            const uint32_t* num_adsh_ptr = num_adsh.data;
            const uint32_t* num_tag_ptr = num_tag.data;
            const uint32_t* num_version_ptr = num_version.data;
            const uint32_t* num_rowids_ptr = num_rowids.data;
            const uint16_t* num_uom_ptr = num_uom.data;
            const size_t num_rows = num_rowids.size();

            for (const KeyEntry& entry : current_keys.entries()) {
                if (!entry.occupied) {
                    continue;
                }
                const size_t begin = lower_bound_key(
                    num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, num_rows,
                    entry.adsh, entry.tag, entry.version
                );
                if (begin == num_rows ||
                    compare_key(
                        num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, begin,
                        entry.adsh, entry.tag, entry.version
                    ) != 0) {
                    continue;
                }
                const size_t end = upper_bound_key(
                    num_adsh_ptr, num_tag_ptr, num_version_ptr, num_rowids_ptr, num_rows,
                    entry.adsh, entry.tag, entry.version
                );
                for (size_t pos = begin; pos < end; ++pos) {
                    if (num_uom_ptr[num_rowids_ptr[pos]] == usd_code) {
                        current_join_rows += entry.count;
                    }
                }
            }
        });

        size_t num_filtered_rows = 0;
        size_t num_distinct_keys = 0;
        size_t num_first_join_rows = 0;
        const double num_first_ms = time_ms([&] {
            KeyHashSet num_keys(1u << 21);
            const size_t num_rows = num_uom.size();
            for (size_t row = 0; row < num_rows; ++row) {
                if (num_uom[row] != usd_code) {
                    continue;
                }
                const uint32_t adsh = num_adsh[row];
                if (adsh >= sub_lookup_2023.size()) {
                    continue;
                }
                const uint32_t sub_row = sub_lookup_2023[adsh];
                if (sub_row == kEmpty32) {
                    continue;
                }
                KeyEntry& entry = num_keys.find_or_insert(adsh, num_tag[row], num_version[row], sub_row);
                ++entry.count;
                ++num_filtered_rows;
            }
            num_distinct_keys = num_keys.size();

            const uint32_t* pre_adsh_ptr = pre_adsh.data;
            const uint32_t* pre_tag_ptr = pre_tag.data;
            const uint32_t* pre_version_ptr = pre_version.data;
            const uint32_t* pre_rowids_ptr = pre_rowids.data;
            const size_t pre_rows = pre_rowids.size();

            for (const KeyEntry& entry : num_keys.entries()) {
                if (!entry.occupied) {
                    continue;
                }
                const size_t begin = lower_bound_key(
                    pre_adsh_ptr, pre_tag_ptr, pre_version_ptr, pre_rowids_ptr, pre_rows,
                    entry.adsh, entry.tag, entry.version
                );
                if (begin == pre_rows ||
                    compare_key(
                        pre_adsh_ptr, pre_tag_ptr, pre_version_ptr, pre_rowids_ptr, begin,
                        entry.adsh, entry.tag, entry.version
                    ) != 0) {
                    continue;
                }
                const size_t end = upper_bound_key(
                    pre_adsh_ptr, pre_tag_ptr, pre_version_ptr, pre_rowids_ptr, pre_rows,
                    entry.adsh, entry.tag, entry.version
                );
                for (size_t pos = begin; pos < end; ++pos) {
                    const uint32_t pre_row = pre_rowids_ptr[pos];
                    if (pre_stmt[pre_row] == is_code) {
                        num_first_join_rows += entry.count;
                    }
                }
            }
        });

        size_t num_hash_probe_hits = 0;
        const double num_hash_then_pre_scan_ms = time_ms([&] {
            KeyHashSet num_keys(1u << 21);
            const size_t num_rows = num_uom.size();
            for (size_t row = 0; row < num_rows; ++row) {
                if (num_uom[row] != usd_code) {
                    continue;
                }
                const uint32_t adsh = num_adsh[row];
                if (adsh >= sub_lookup_2023.size()) {
                    continue;
                }
                const uint32_t sub_row = sub_lookup_2023[adsh];
                if (sub_row == kEmpty32) {
                    continue;
                }
                KeyEntry& entry = num_keys.find_or_insert(adsh, num_tag[row], num_version[row], sub_row);
                ++entry.count;
            }

            const size_t pre_rows = pre_stmt.size();
            const size_t pre_blocks = (pre_rows + kBlockSize - 1) / kBlockSize;
            for (size_t block = 0; block < pre_blocks; ++block) {
                const auto z = pre_stmt_zone[block];
                if (z.max < is_code || z.min > is_code) {
                    continue;
                }
                const size_t begin = block * kBlockSize;
                const size_t end = std::min(begin + kBlockSize, pre_rows);
                for (size_t row = begin; row < end; ++row) {
                    if (pre_stmt[row] != is_code) {
                        continue;
                    }
                    const uint32_t adsh = pre_adsh[row];
                    if (adsh >= sub_lookup_2023.size()) {
                        continue;
                    }
                    if (sub_lookup_2023[adsh] == kEmpty32) {
                        continue;
                    }
                    KeyEntry* entry = num_keys.find(adsh, pre_tag[row], pre_version[row]);
                    if (entry != nullptr) {
                        num_hash_probe_hits += entry->count;
                    }
                }
            }
        });

        std::cout << "sub_rows_2023=" << sub_rows_2023 << "\n";
        std::cout << "current_pre_filtered_rows=" << pre_filtered_rows << "\n";
        std::cout << "current_distinct_keys=" << current_distinct_keys << "\n";
        std::cout << "current_join_rows=" << current_join_rows << "\n";
        std::cout << "candidate_current_pre_then_num_index_ms=" << current_ms << "\n";
        std::cout << "num_filtered_rows=" << num_filtered_rows << "\n";
        std::cout << "num_distinct_keys=" << num_distinct_keys << "\n";
        std::cout << "num_first_join_rows=" << num_first_join_rows << "\n";
        std::cout << "candidate_num_then_pre_index_ms=" << num_first_ms << "\n";
        std::cout << "candidate_num_hash_then_pre_scan_ms=" << num_hash_then_pre_scan_ms << "\n";
        std::cout << "num_hash_probe_hits=" << num_hash_probe_hits << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
