#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kTargetFy = 2022;
constexpr size_t kOuterExpectedGroups = 32768;
constexpr size_t kCikExpectedGroups = 16384;

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

template <typename T>
struct ZoneRecord {
    T min;
    T max;
};

struct ResultRow {
    uint32_t name_id;
    int32_t cik;
    double total_value;
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

void ensure_directory(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("failed to create directory: " + path);
    }
}

template <typename T>
std::vector<ZoneRecord<T>> load_zone_map(const std::string& path) {
    gendb::MmapColumn<ZoneRecord<T>> zones(path);
    std::vector<ZoneRecord<T>> out(zones.data, zones.data + zones.size());
    return out;
}

uint16_t resolve_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("dictionary code not found");
}

struct SumHashMap {
    std::vector<uint64_t> keys;
    std::vector<double> values;
    size_t mask = 0;
    size_t size_used = 0;

    SumHashMap() = default;

    explicit SumHashMap(size_t expected) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        keys.assign(capacity, kEmpty64);
        values.assign(capacity, 0.0);
        mask = capacity - 1;
        size_used = 0;
    }

    void rehash(size_t new_capacity) {
        std::vector<uint64_t> old_keys = std::move(keys);
        std::vector<double> old_values = std::move(values);
        keys.assign(new_capacity, kEmpty64);
        values.assign(new_capacity, 0.0);
        mask = new_capacity - 1;
        size_used = 0;
        for (size_t i = 0; i < old_keys.size(); ++i) {
            const uint64_t key = old_keys[i];
            if (key != kEmpty64) {
                add(key, old_values[i]);
            }
        }
    }

    void maybe_grow() {
        if ((size_used + 1) * 10 >= keys.size() * 7) {
            rehash(keys.size() * 2);
        }
    }

    void add(uint64_t key, double delta) {
        maybe_grow();
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            const uint64_t existing = keys[slot];
            if (existing == key) {
                values[slot] += delta;
                return;
            }
            if (existing == kEmpty64) {
                keys[slot] = key;
                values[slot] = delta;
                ++size_used;
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] != kEmpty64) {
                fn(keys[i], values[i]);
            }
        }
    }
};

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

        DictData uom_dict;
        DictData name_dict;
        gendb::MmapColumn<uint32_t> num_adsh;
        gendb::MmapColumn<uint16_t> num_uom;
        gendb::MmapColumn<double> num_value;
        gendb::MmapColumn<uint32_t> sub_adsh;
        gendb::MmapColumn<int32_t> sub_fy;
        gendb::MmapColumn<uint32_t> sub_name;
        gendb::MmapColumn<int32_t> sub_cik;
        gendb::MmapColumn<uint32_t> adsh_to_rowid;
        std::vector<ZoneRecord<uint16_t>> num_uom_zone;

        {
            GENDB_PHASE("data_loading");
            uom_dict = load_dict(gendb_dir + "/num/dict_uom.offsets.bin", gendb_dir + "/num/dict_uom.data.bin");
            name_dict = load_dict(gendb_dir + "/sub/dict_name.offsets.bin", gendb_dir + "/sub/dict_name.data.bin");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_fy.open(gendb_dir + "/sub/fy.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");
            adsh_to_rowid.open(gendb_dir + "/sub/indexes/adsh_to_rowid.bin");
            num_uom_zone = load_zone_map<uint16_t>(gendb_dir + "/num/indexes/uom.zone_map.bin");

            if (num_adsh.size() != num_uom.size() || num_adsh.size() != num_value.size()) {
                throw std::runtime_error("num column size mismatch");
            }
            if (sub_adsh.size() != sub_fy.size() || sub_adsh.size() != sub_name.size() || sub_adsh.size() != sub_cik.size()) {
                throw std::runtime_error("sub column size mismatch");
            }
        }

        uint16_t usd_code = 0;
        std::vector<uint8_t> sub_fy_match;
        std::vector<std::pair<uint32_t, uint32_t>> scan_ranges;

        {
            GENDB_PHASE("dim_filter");
            usd_code = resolve_code(uom_dict, "USD");

            sub_fy_match.assign(sub_fy.size(), 0);
            for (size_t row = 0; row < sub_fy.size(); ++row) {
                sub_fy_match[row] = static_cast<uint8_t>(sub_fy[row] == kTargetFy);
            }

            scan_ranges.reserve(num_uom_zone.size());
            for (size_t block = 0; block < num_uom_zone.size(); ++block) {
                const auto& zone = num_uom_zone[block];
                if (zone.min <= usd_code && usd_code <= zone.max) {
                    const uint32_t begin = static_cast<uint32_t>(block * kBlockSize);
                    const uint32_t end = static_cast<uint32_t>(std::min(num_uom.size(), (block + 1) * kBlockSize));
                    scan_ranges.push_back({begin, end});
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            num_adsh.advise_sequential();
            num_uom.advise_sequential();
            num_value.advise_sequential();
            adsh_to_rowid.advise_random();
            sub_name.advise_random();
            sub_cik.advise_random();
            gendb::mmap_prefetch_all(num_adsh, num_uom, num_value, adsh_to_rowid, sub_name, sub_cik);
        }

        const uint32_t* num_adsh_ptr = num_adsh.data;
        const uint16_t* num_uom_ptr = num_uom.data;
        const double* num_value_ptr = num_value.data;
        const uint32_t* sub_name_ptr = sub_name.data;
        const int32_t* sub_cik_ptr = sub_cik.data;
        const uint32_t* join_lookup = adsh_to_rowid.data;
        const uint8_t* sub_match_ptr = sub_fy_match.data();

        SumHashMap merged_cik(kCikExpectedGroups);
        SumHashMap merged_outer(kOuterExpectedGroups);

        {
            GENDB_PHASE("main_scan");
            for (size_t range_idx = 0; range_idx < scan_ranges.size(); ++range_idx) {
                const uint32_t begin = scan_ranges[range_idx].first;
                const uint32_t end = scan_ranges[range_idx].second;

                for (uint32_t row = begin; row < end; ++row) {
                    if (num_uom_ptr[row] != usd_code) {
                        continue;
                    }

                    const uint32_t adsh_id = num_adsh_ptr[row];
                    if (adsh_id >= adsh_to_rowid.size()) {
                        continue;
                    }

                    const uint32_t sub_row = join_lookup[adsh_id];
                    if (sub_row == kEmpty32 || !sub_match_ptr[sub_row]) {
                        continue;
                    }

                    const double value = num_value_ptr[row];
                    const int32_t cik = sub_cik_ptr[sub_row];
                    const uint32_t name_id = sub_name_ptr[sub_row];
                    const uint64_t cik_key = static_cast<uint64_t>(static_cast<uint32_t>(cik));
                    const uint64_t outer_key =
                        (static_cast<uint64_t>(name_id) << 32) | static_cast<uint32_t>(cik);

                    merged_cik.add(cik_key, value);
                    merged_outer.add(outer_key, value);
                }
            }
        }

        double avg_sub_total = 0.0;
        if (merged_cik.size_used > 0) {
            double total = 0.0;
            merged_cik.for_each([&](uint64_t, double value) {
                total += value;
            });
            avg_sub_total = total / static_cast<double>(merged_cik.size_used);
        }

        std::vector<ResultRow> results;
        results.reserve(merged_outer.size_used);
        merged_outer.for_each([&](uint64_t key, double value) {
            if (value > avg_sub_total) {
                results.push_back(ResultRow{
                    static_cast<uint32_t>(key >> 32),
                    static_cast<int32_t>(static_cast<uint32_t>(key)),
                    value,
                });
            }
        });

        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.total_value != b.total_value) {
                return a.total_value > b.total_value;
            }
            if (a.name_id != b.name_id) {
                return a.name_id < b.name_id;
            }
            return a.cik < b.cik;
        });
        if (results.size() > 100) {
            results.resize(100);
        }

        {
            GENDB_PHASE("output");
            ensure_directory(results_dir);
            const std::string out_path = results_dir + "/Q3.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file: " + out_path);
            }

            std::fprintf(out, "name,cik,total_value\n");
            for (const ResultRow& row : results) {
                const std::string_view name = name_dict.at(row.name_id);
                csv_write_escaped(out, name);
                std::fprintf(out, ",%d,%.2f\n", row.cik, row.total_value);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
