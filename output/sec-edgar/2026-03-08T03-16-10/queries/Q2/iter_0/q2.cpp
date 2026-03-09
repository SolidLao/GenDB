#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr size_t kBlockSize = 100000;

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

struct ZoneRecordU16 {
    uint16_t min;
    uint16_t max;
};

struct ResultRow {
    uint32_t name_id;
    uint32_t tag_id;
    double value;
};

class MaxHashMap {
public:
    void init(size_t expected_entries) {
        size_t cap = 1;
        while (cap < expected_entries * 2) {
            cap <<= 1;
        }
        if (cap < 1024) {
            cap = 1024;
        }
        mask_ = cap - 1;
        keys_.assign(cap, 0);
        values_.assign(cap, 0.0);
        used_.assign(cap, 0);
        size_ = 0;
    }

    void update_max(uint64_t key, double value) {
        size_t slot = hash_key(key) & mask_;
        while (used_[slot]) {
            if (keys_[slot] == key) {
                if (value > values_[slot]) {
                    values_[slot] = value;
                }
                return;
            }
            slot = (slot + 1) & mask_;
        }
        used_[slot] = 1;
        keys_[slot] = key;
        values_[slot] = value;
        ++size_;
    }

    const double* find(uint64_t key) const {
        size_t slot = hash_key(key) & mask_;
        while (used_[slot]) {
            if (keys_[slot] == key) {
                return &values_[slot];
            }
            slot = (slot + 1) & mask_;
        }
        return nullptr;
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < used_.size(); ++i) {
            if (used_[i]) {
                fn(keys_[i], values_[i]);
            }
        }
    }

    size_t size() const {
        return size_;
    }

private:
    static inline uint64_t hash_key(uint64_t key) {
        key ^= key >> 33;
        key *= 0xff51afd7ed558ccdULL;
        key ^= key >> 33;
        key *= 0xc4ceb9fe1a85ec53ULL;
        key ^= key >> 33;
        return key;
    }

    std::vector<uint64_t> keys_;
    std::vector<double> values_;
    std::vector<uint8_t> used_;
    size_t mask_ = 0;
    size_t size_ = 0;
};

DictData load_dict(const std::string& offsets_path, const std::string& data_path) {
    DictData dict;
    dict.offsets.open(offsets_path);
    dict.data.open(data_path);
    return dict;
}

uint16_t find_dict_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary code not found");
}

std::vector<ZoneRecordU16> load_u16_zone_map(const std::string& path) {
    gendb::MmapColumn<ZoneRecordU16> zone_col(path);
    return std::vector<ZoneRecordU16>(zone_col.data, zone_col.data + zone_col.size());
}

void ensure_directory(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("failed to create directory: " + path);
    }
}

void write_csv_escaped(FILE* out, std::string_view sv) {
    bool needs_quotes = false;
    for (char c : sv) {
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (!needs_quotes) {
        std::fwrite(sv.data(), 1, sv.size(), out);
        return;
    }

    std::fputc('"', out);
    for (char c : sv) {
        if (c == '"') {
            std::fputc('"', out);
        }
        std::fputc(c, out);
    }
    std::fputc('"', out);
}

inline uint64_t pack_key(uint32_t adsh, uint32_t tag) {
    return (static_cast<uint64_t>(adsh) << 32) | static_cast<uint64_t>(tag);
}

inline void set_bit(std::vector<uint64_t>& bits, uint32_t idx) {
    bits[idx >> 6] |= (uint64_t{1} << (idx & 63));
}

inline bool get_bit(const std::vector<uint64_t>& bits, uint32_t idx) {
    return (bits[idx >> 6] >> (idx & 63)) & 1U;
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
        const std::string num_dir = gendb_dir + "/num";
        const std::string sub_dir = gendb_dir + "/sub";
        const std::string shared_dir = gendb_dir + "/shared";

        GENDB_PHASE("total");

        DictData uom_dict;
        DictData name_dict;
        DictData tag_dict;
        std::vector<ZoneRecordU16> uom_zone_map;

        gendb::MmapColumn<uint32_t> num_adsh_col;
        gendb::MmapColumn<uint32_t> num_tag_col;
        gendb::MmapColumn<uint16_t> num_uom_col;
        gendb::MmapColumn<double> num_value_col;

        gendb::MmapColumn<int32_t> sub_fy_col;
        gendb::MmapColumn<uint32_t> sub_name_col;
        gendb::MmapColumn<uint32_t> adsh_to_rowid_col;

        uint16_t pure_code = 0;

        {
            GENDB_PHASE("data_loading");

            uom_dict = load_dict(num_dir + "/dict_uom.offsets.bin", num_dir + "/dict_uom.data.bin");
            name_dict = load_dict(sub_dir + "/dict_name.offsets.bin", sub_dir + "/dict_name.data.bin");
            tag_dict = load_dict(shared_dir + "/tag.offsets.bin", shared_dir + "/tag.data.bin");
            pure_code = find_dict_code(uom_dict, "pure");
            uom_zone_map = load_u16_zone_map(num_dir + "/indexes/uom.zone_map.bin");

            num_adsh_col.open(num_dir + "/adsh.bin");
            num_tag_col.open(num_dir + "/tag.bin");
            num_uom_col.open(num_dir + "/uom.bin");
            num_value_col.open(num_dir + "/value.bin");

            sub_fy_col.open(sub_dir + "/fy.bin");
            sub_name_col.open(sub_dir + "/name.bin");
            adsh_to_rowid_col.open(sub_dir + "/indexes/adsh_to_rowid.bin");

            const size_t num_rows = num_adsh_col.size();
            if (num_tag_col.size() != num_rows || num_uom_col.size() != num_rows || num_value_col.size() != num_rows) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_fy_col.size() != sub_name_col.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
        }

        const uint32_t adsh_cardinality = static_cast<uint32_t>(adsh_to_rowid_col.size());
        std::vector<uint64_t> fy2022_bits((static_cast<size_t>(adsh_cardinality) + 63) / 64, 0);
        std::vector<uint32_t> name_id_by_adsh(adsh_cardinality, kEmpty32);

        {
            GENDB_PHASE("dim_filter");
            for (uint32_t adsh_id = 0; adsh_id < adsh_cardinality; ++adsh_id) {
                const uint32_t rowid = adsh_to_rowid_col[adsh_id];
                if (rowid == kEmpty32) {
                    continue;
                }
                if (sub_fy_col[rowid] == 2022) {
                    set_bit(fy2022_bits, adsh_id);
                    name_id_by_adsh[adsh_id] = sub_name_col[rowid];
                }
            }
        }

        const uint32_t* num_adsh = num_adsh_col.data;
        const uint32_t* num_tag = num_tag_col.data;
        const uint16_t* num_uom = num_uom_col.data;
        const double* num_value = num_value_col.data;
        const size_t num_rows = num_adsh_col.size();

        std::vector<uint32_t> qualifying_blocks;
        qualifying_blocks.reserve(uom_zone_map.size());
        for (size_t block = 0; block < uom_zone_map.size(); ++block) {
            const ZoneRecordU16 z = uom_zone_map[block];
            if (z.min <= pure_code && pure_code <= z.max) {
                qualifying_blocks.push_back(static_cast<uint32_t>(block));
            }
        }

        gendb::mmap_prefetch_all(num_adsh_col, num_tag_col, num_uom_col, num_value_col, sub_fy_col, sub_name_col, adsh_to_rowid_col);

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<MaxHashMap> local_maps(static_cast<size_t>(max_threads));
        for (int i = 0; i < max_threads; ++i) {
            local_maps[static_cast<size_t>(i)].init(4096);
        }

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel
            {
                MaxHashMap& local_map = local_maps[static_cast<size_t>(omp_get_thread_num())];

                #pragma omp for schedule(dynamic, 1)
                for (size_t block_idx = 0; block_idx < qualifying_blocks.size(); ++block_idx) {
                    const size_t begin = static_cast<size_t>(qualifying_blocks[block_idx]) * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, num_rows);

                    for (size_t row = begin; row < end; ++row) {
                        if (num_uom[row] != pure_code) {
                            continue;
                        }
                        const uint32_t adsh_id = num_adsh[row];
                        if (adsh_id >= adsh_cardinality || !get_bit(fy2022_bits, adsh_id)) {
                            continue;
                        }
                        local_map.update_max(pack_key(adsh_id, num_tag[row]), num_value[row]);
                    }
                }
            }
        }

        MaxHashMap global_max_map;

        {
            GENDB_PHASE("build_joins");
            size_t merged_entries = 0;
            for (const MaxHashMap& local_map : local_maps) {
                merged_entries += local_map.size();
            }
            global_max_map.init(std::max<size_t>(4096, merged_entries));
            for (const MaxHashMap& local_map : local_maps) {
                local_map.for_each([&](uint64_t key, double value) {
                    global_max_map.update_max(key, value);
                });
            }
        }

        std::vector<std::vector<ResultRow>> thread_results(static_cast<size_t>(max_threads));

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel
            {
                std::vector<ResultRow>& local_results = thread_results[static_cast<size_t>(omp_get_thread_num())];
                local_results.reserve(256);

                #pragma omp for schedule(dynamic, 1)
                for (size_t block_idx = 0; block_idx < qualifying_blocks.size(); ++block_idx) {
                    const size_t begin = static_cast<size_t>(qualifying_blocks[block_idx]) * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, num_rows);

                    for (size_t row = begin; row < end; ++row) {
                        if (num_uom[row] != pure_code) {
                            continue;
                        }
                        const uint32_t adsh_id = num_adsh[row];
                        if (adsh_id >= adsh_cardinality || !get_bit(fy2022_bits, adsh_id)) {
                            continue;
                        }

                        const uint64_t key = pack_key(adsh_id, num_tag[row]);
                        const double* max_value = global_max_map.find(key);
                        if (max_value != nullptr && num_value[row] == *max_value) {
                            local_results.push_back(ResultRow{name_id_by_adsh[adsh_id], num_tag[row], num_value[row]});
                        }
                    }
                }
            }
        }

        size_t total_results = 0;
        for (const auto& local_results : thread_results) {
            total_results += local_results.size();
        }

        std::vector<ResultRow> results;
        results.reserve(total_results);
        for (auto& local_results : thread_results) {
            results.insert(results.end(), local_results.begin(), local_results.end());
        }

        auto row_less = [&](const ResultRow& a, const ResultRow& b) {
            if (a.value != b.value) {
                return a.value > b.value;
            }
            const std::string_view a_name = name_dict.at(a.name_id);
            const std::string_view b_name = name_dict.at(b.name_id);
            if (a_name != b_name) {
                return a_name < b_name;
            }
            const std::string_view a_tag = tag_dict.at(a.tag_id);
            const std::string_view b_tag = tag_dict.at(b.tag_id);
            if (a_tag != b_tag) {
                return a_tag < b_tag;
            }
            return false;
        };

        const size_t limit = std::min<size_t>(100, results.size());
        if (results.size() > limit) {
            std::partial_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(limit), results.end(), row_less);
            results.resize(limit);
        } else {
            std::sort(results.begin(), results.end(), row_less);
        }

        {
            GENDB_PHASE("output");
            ensure_directory(results_dir);
            const std::string out_path = results_dir + "/Q2.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file: " + out_path);
            }

            std::fprintf(out, "name,tag,value\n");
            for (const ResultRow& row : results) {
                const std::string_view name_sv = name_dict.at(row.name_id);
                const std::string_view tag_sv = tag_dict.at(row.tag_id);
                write_csv_escaped(out, name_sv);
                std::fputc(',', out);
                write_csv_escaped(out, tag_sv);
                std::fprintf(out, ",%.2f\n", row.value);
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
