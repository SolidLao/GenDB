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

#include <sys/stat.h>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kEmpty64 = std::numeric_limits<uint64_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kTargetFy = 2022;
constexpr int64_t kValueScale = 10000;

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

struct ResultRow {
    uint32_t name_id;
    int32_t cik;
    int64_t total_value_scaled;
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

struct SumHashMap {
    std::vector<uint64_t> keys;
    std::vector<int64_t> values;
    size_t mask = 0;
    size_t size_used = 0;

    explicit SumHashMap(size_t expected = 16) {
        reset(expected);
    }

    void reset(size_t expected) {
        const size_t capacity = static_cast<size_t>(next_power_of_two(std::max<size_t>(16, expected * 2)));
        keys.assign(capacity, kEmpty64);
        values.assign(capacity, 0);
        mask = capacity - 1;
        size_used = 0;
    }

    void grow() {
        std::vector<uint64_t> old_keys = std::move(keys);
        std::vector<int64_t> old_values = std::move(values);
        keys.assign(old_keys.size() * 2, kEmpty64);
        values.assign(old_values.size() * 2, 0);
        mask = keys.size() - 1;
        size_used = 0;
        for (size_t i = 0; i < old_keys.size(); ++i) {
            if (old_keys[i] != kEmpty64) {
                add(old_keys[i], old_values[i]);
            }
        }
    }

    void add(uint64_t key, int64_t delta) {
        if ((size_used + 1) * 10 >= keys.size() * 7) {
            grow();
        }
        size_t slot = static_cast<size_t>(mix64(key)) & mask;
        while (true) {
            if (keys[slot] == key) {
                values[slot] += delta;
                return;
            }
            if (keys[slot] == kEmpty64) {
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

DictData load_dict(const std::string& offsets_path, const std::string& data_path) {
    DictData dict;
    dict.offsets.open(offsets_path);
    dict.data.open(data_path);
    return dict;
}

template <typename T>
std::vector<ZoneRecord<T>> load_zone_map(const std::string& path) {
    gendb::MmapColumn<ZoneRecord<T>> zones(path);
    return std::vector<ZoneRecord<T>>(zones.data, zones.data + zones.size());
}

void ensure_directory(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("failed to create directory: " + path);
    }
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

uint16_t resolve_uom_code(const DictData& dict, std::string_view needle) {
    for (size_t i = 0; i < dict.size(); ++i) {
        if (dict.at(i) == needle) {
            return static_cast<uint16_t>(i);
        }
    }
    throw std::runtime_error("uom code not found");
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
            if (adsh_to_rowid.size() == 0) {
                throw std::runtime_error("empty adsh_to_rowid index");
            }
        }

        uint16_t usd_code = 0;
        std::vector<uint32_t> filtered_adsh_to_subrow;
        std::vector<uint32_t> filtered_sub_rows;
        std::vector<std::pair<uint32_t, uint32_t>> scan_ranges;

        {
            GENDB_PHASE("dim_filter");
            usd_code = resolve_uom_code(uom_dict, "USD");

            filtered_adsh_to_subrow.assign(adsh_to_rowid.size(), kEmpty32);
            filtered_sub_rows.reserve(sub_fy.size() / 3);
            for (uint32_t row = 0; row < sub_fy.size(); ++row) {
                if (sub_fy[row] == kTargetFy) {
                    const uint32_t filtered_row = static_cast<uint32_t>(filtered_sub_rows.size());
                    filtered_sub_rows.push_back(row);
                    filtered_adsh_to_subrow[sub_adsh[row]] = filtered_row;
                }
            }

            scan_ranges.reserve(num_uom_zone.size());
            for (size_t block = 0; block < num_uom_zone.size(); ++block) {
                const ZoneRecord<uint16_t>& zone = num_uom_zone[block];
                if (zone.min <= usd_code && usd_code <= zone.max) {
                    const uint32_t begin = static_cast<uint32_t>(block * kBlockSize);
                    const uint32_t end = static_cast<uint32_t>(std::min(num_uom.size(), (block + 1) * kBlockSize));
                    scan_ranges.emplace_back(begin, end);
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            num_adsh.advise_sequential();
            num_uom.advise_sequential();
            num_value.advise_sequential();
            sub_name.advise_random();
            sub_cik.advise_random();
            gendb::mmap_prefetch_all(num_adsh, num_uom, num_value, sub_name, sub_cik);
        }

        const uint32_t filtered_count = static_cast<uint32_t>(filtered_sub_rows.size());
        std::vector<int64_t> block_sub_sums(scan_ranges.size() * static_cast<size_t>(filtered_count), 0);
        std::vector<int64_t> sub_sums(filtered_count, 0);

        {
            GENDB_PHASE("main_scan");
            const uint32_t* num_adsh_ptr = num_adsh.data;
            const uint16_t* num_uom_ptr = num_uom.data;
            const double* num_value_ptr = num_value.data;
            const uint32_t* filtered_lookup = filtered_adsh_to_subrow.data();

#pragma omp parallel
            {
#pragma omp for schedule(static)
                for (size_t range_idx = 0; range_idx < scan_ranges.size(); ++range_idx) {
                    int64_t* local_sums = block_sub_sums.data() + range_idx * static_cast<size_t>(filtered_count);
                    const uint32_t begin = scan_ranges[range_idx].first;
                    const uint32_t end = scan_ranges[range_idx].second;
                    for (uint32_t row = begin; row < end; ++row) {
                        if (num_uom_ptr[row] != usd_code) {
                            continue;
                        }
                        const uint32_t adsh_id = num_adsh_ptr[row];
                        if (adsh_id >= filtered_adsh_to_subrow.size()) {
                            continue;
                        }
                        const uint32_t filtered_sub_row = filtered_lookup[adsh_id];
                        if (filtered_sub_row == kEmpty32) {
                            continue;
                        }
                        local_sums[filtered_sub_row] += static_cast<int64_t>(std::llround(num_value_ptr[row] * static_cast<double>(kValueScale)));
                    }
                }
            }

            for (size_t range_idx = 0; range_idx < scan_ranges.size(); ++range_idx) {
                const int64_t* local_sums = block_sub_sums.data() + range_idx * static_cast<size_t>(filtered_count);
                for (uint32_t i = 0; i < filtered_count; ++i) {
                    sub_sums[i] += local_sums[i];
                }
            }
        }

        __int128 total_cik_sum_scaled = 0;
        size_t cik_group_count = 0;
        std::vector<ResultRow> results;

        {
            SumHashMap cik_totals(filtered_count);
            SumHashMap outer_totals(filtered_count);

            for (uint32_t filtered_sub_row = 0; filtered_sub_row < filtered_count; ++filtered_sub_row) {
                const int64_t sub_total = sub_sums[filtered_sub_row];
                const uint32_t sub_row = filtered_sub_rows[filtered_sub_row];
                const int32_t cik = sub_cik[sub_row];
                const uint32_t name_id = sub_name[sub_row];
                const uint64_t cik_key = static_cast<uint32_t>(cik);
                const uint64_t outer_key = (static_cast<uint64_t>(name_id) << 32) | static_cast<uint32_t>(cik);
                cik_totals.add(cik_key, sub_total);
                outer_totals.add(outer_key, sub_total);
            }

            cik_totals.for_each([&](uint64_t, int64_t total) {
                total_cik_sum_scaled += total;
            });
            cik_group_count = cik_totals.size_used;

            results.reserve(outer_totals.size_used);
            outer_totals.for_each([&](uint64_t key, int64_t total) {
                if (cik_group_count != 0 &&
                    static_cast<__int128>(total) * static_cast<__int128>(cik_group_count) >
                        total_cik_sum_scaled) {
                    results.push_back(ResultRow{
                        static_cast<uint32_t>(key >> 32),
                        static_cast<int32_t>(static_cast<uint32_t>(key)),
                        total,
                    });
                }
            });
        }

        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.total_value_scaled != b.total_value_scaled) {
                return a.total_value_scaled > b.total_value_scaled;
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
            const std::string output_path = results_dir + "/Q3.csv";
            FILE* out = std::fopen(output_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file: " + output_path);
            }

            std::fprintf(out, "name,cik,total_value\n");
            for (const ResultRow& row : results) {
                csv_write_escaped(out, name_dict.at(row.name_id));
                std::fprintf(out, ",%d,%.2f\n", row.cik, static_cast<double>(row.total_value_scaled) / static_cast<double>(kValueScale));
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
