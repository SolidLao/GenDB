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
#include <utility>
#include <vector>

#include <sys/stat.h>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
constexpr size_t kBlockSize = 100000;
constexpr int32_t kTargetFy = 2022;
constexpr size_t kTopK = 100;

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
        return offsets.empty() ? 0 : offsets.size() - 1;
    }
};

struct OuterKey {
    uint32_t name_id;
    int32_t cik;

    bool operator<(const OuterKey& other) const {
        if (name_id != other.name_id) {
            return name_id < other.name_id;
        }
        return cik < other.cik;
    }

    bool operator==(const OuterKey& other) const {
        return name_id == other.name_id && cik == other.cik;
    }
};

struct ResultRow {
    uint32_t name_id;
    int32_t cik;
    long double total_value;
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

bool result_row_better(const ResultRow& a, const ResultRow& b) {
    if (a.total_value != b.total_value) {
        return a.total_value > b.total_value;
    }
    if (a.name_id != b.name_id) {
        return a.name_id < b.name_id;
    }
    return a.cik < b.cik;
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
            if (adsh_to_rowid.empty()) {
                throw std::runtime_error("empty adsh_to_rowid index");
            }
        }

        uint16_t usd_code = 0;
        std::vector<uint32_t> filtered_adsh_to_subrow;
        std::vector<uint32_t> filtered_sub_rows;
        std::vector<uint32_t> filtered_subrow_to_cik_group_id;
        std::vector<uint32_t> filtered_subrow_to_outer_group_id;
        std::vector<int32_t> cik_group_values;
        std::vector<OuterKey> outer_group_values;
        std::vector<std::pair<uint32_t, uint32_t>> scan_ranges;

        {
            GENDB_PHASE("dim_filter");
            usd_code = resolve_uom_code(uom_dict, "USD");

            filtered_adsh_to_subrow.assign(adsh_to_rowid.size(), kEmpty32);
            filtered_sub_rows.reserve(sub_fy.size() / 3);
            std::vector<int32_t> cik_keys;
            std::vector<OuterKey> outer_keys;
            cik_keys.reserve(sub_fy.size() / 3);
            outer_keys.reserve(sub_fy.size() / 3);

            for (uint32_t row = 0; row < sub_fy.size(); ++row) {
                if (sub_fy[row] != kTargetFy) {
                    continue;
                }
                const uint32_t filtered_subrow = static_cast<uint32_t>(filtered_sub_rows.size());
                filtered_sub_rows.push_back(row);
                filtered_adsh_to_subrow[sub_adsh[row]] = filtered_subrow;
                cik_keys.push_back(sub_cik[row]);
                outer_keys.push_back(OuterKey{sub_name[row], sub_cik[row]});
            }

            cik_group_values = cik_keys;
            std::sort(cik_group_values.begin(), cik_group_values.end());
            cik_group_values.erase(std::unique(cik_group_values.begin(), cik_group_values.end()), cik_group_values.end());

            outer_group_values = outer_keys;
            std::sort(outer_group_values.begin(), outer_group_values.end());
            outer_group_values.erase(std::unique(outer_group_values.begin(), outer_group_values.end()), outer_group_values.end());

            filtered_subrow_to_cik_group_id.resize(filtered_sub_rows.size());
            filtered_subrow_to_outer_group_id.resize(filtered_sub_rows.size());
            for (uint32_t filtered_subrow = 0; filtered_subrow < filtered_sub_rows.size(); ++filtered_subrow) {
                const uint32_t sub_row = filtered_sub_rows[filtered_subrow];
                const int32_t cik = sub_cik[sub_row];
                const OuterKey outer_key{sub_name[sub_row], cik};

                const auto cik_it = std::lower_bound(cik_group_values.begin(), cik_group_values.end(), cik);
                const auto outer_it = std::lower_bound(outer_group_values.begin(), outer_group_values.end(), outer_key);
                filtered_subrow_to_cik_group_id[filtered_subrow] =
                    static_cast<uint32_t>(cik_it - cik_group_values.begin());
                filtered_subrow_to_outer_group_id[filtered_subrow] =
                    static_cast<uint32_t>(outer_it - outer_group_values.begin());
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
        const uint32_t cik_group_count = static_cast<uint32_t>(cik_group_values.size());
        const uint32_t outer_group_count = static_cast<uint32_t>(outer_group_values.size());

        std::vector<double> subrow_sums(filtered_count, 0.0);
        std::vector<uint8_t> subrow_touched(filtered_count, 0);

        {
            GENDB_PHASE("main_scan");
            const int worker_count = std::max(1, omp_get_max_threads());
            std::vector<double> thread_local_sums(static_cast<size_t>(worker_count) * filtered_count, 0.0);
            std::vector<uint8_t> thread_local_touched(static_cast<size_t>(worker_count) * filtered_count, 0);

            const uint32_t* num_adsh_ptr = num_adsh.data;
            const uint16_t* num_uom_ptr = num_uom.data;
            const double* num_value_ptr = num_value.data;
            const uint32_t* filtered_lookup = filtered_adsh_to_subrow.data();
            const uint32_t lookup_size = static_cast<uint32_t>(filtered_adsh_to_subrow.size());

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                double* local_sums = thread_local_sums.data() + static_cast<size_t>(tid) * filtered_count;
                uint8_t* local_touched = thread_local_touched.data() + static_cast<size_t>(tid) * filtered_count;

#pragma omp for schedule(static)
                for (size_t range_idx = 0; range_idx < scan_ranges.size(); ++range_idx) {
                    const uint32_t begin = scan_ranges[range_idx].first;
                    const uint32_t end = scan_ranges[range_idx].second;
                    for (uint32_t row = begin; row < end; ++row) {
                        if (num_uom_ptr[row] != usd_code) {
                            continue;
                        }
                        const uint32_t adsh_id = num_adsh_ptr[row];
                        if (adsh_id >= lookup_size) {
                            continue;
                        }
                        const uint32_t filtered_subrow = filtered_lookup[adsh_id];
                        if (filtered_subrow == kEmpty32) {
                            continue;
                        }
                        local_touched[filtered_subrow] = 1;
                        local_sums[filtered_subrow] += num_value_ptr[row];
                    }
                }
            }

            for (int worker = 0; worker < worker_count; ++worker) {
                const double* local_sums = thread_local_sums.data() + static_cast<size_t>(worker) * filtered_count;
                const uint8_t* local_touched = thread_local_touched.data() + static_cast<size_t>(worker) * filtered_count;
                for (uint32_t filtered_subrow = 0; filtered_subrow < filtered_count; ++filtered_subrow) {
                    subrow_sums[filtered_subrow] += local_sums[filtered_subrow];
                    subrow_touched[filtered_subrow] |= local_touched[filtered_subrow];
                }
            }
        }

        std::vector<long double> cik_totals(cik_group_count, 0.0L);
        std::vector<uint8_t> cik_touched(cik_group_count, 0);
        std::vector<long double> outer_totals(outer_group_count, 0.0L);
        std::vector<uint8_t> outer_touched(outer_group_count, 0);
        long double total_cik_sum = 0.0L;
        uint32_t active_cik_groups = 0;
        std::vector<ResultRow> results;

        for (uint32_t filtered_subrow = 0; filtered_subrow < filtered_count; ++filtered_subrow) {
            if (!subrow_touched[filtered_subrow]) {
                continue;
            }
            const uint32_t cik_group_id = filtered_subrow_to_cik_group_id[filtered_subrow];
            const uint32_t outer_group_id = filtered_subrow_to_outer_group_id[filtered_subrow];
            cik_touched[cik_group_id] = 1;
            outer_touched[outer_group_id] = 1;
            cik_totals[cik_group_id] += subrow_sums[filtered_subrow];
            outer_totals[outer_group_id] += subrow_sums[filtered_subrow];
        }

        for (uint32_t cik_group_id = 0; cik_group_id < cik_group_count; ++cik_group_id) {
            if (!cik_touched[cik_group_id]) {
                continue;
            }
            total_cik_sum += cik_totals[cik_group_id];
            ++active_cik_groups;
        }

        if (active_cik_groups != 0) {
            results.reserve(outer_group_count);
            for (uint32_t outer_group_id = 0; outer_group_id < outer_group_count; ++outer_group_id) {
                if (!outer_touched[outer_group_id]) {
                    continue;
                }
                const long double total = outer_totals[outer_group_id];
                if (total * static_cast<long double>(active_cik_groups) <= total_cik_sum) {
                    continue;
                }
                const OuterKey& key = outer_group_values[outer_group_id];
                results.push_back(ResultRow{key.name_id, key.cik, total});
            }
        }

        if (results.size() > kTopK) {
            std::nth_element(results.begin(), results.begin() + kTopK, results.end(), result_row_better);
            results.resize(kTopK);
        }
        std::sort(results.begin(), results.end(), result_row_better);

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
                std::fprintf(out, ",%d,%.2Lf\n", row.cik, row.total_value);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
