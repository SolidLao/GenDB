#include "mmap_utils.h"
#include "timing_utils.h"
#include "hash_utils.h"

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
#include <sys/stat.h>
#include <sys/types.h>

using gendb::CompactHashMap;
using gendb::MmapColumn;

namespace {

constexpr uint32_t kRowidNull = std::numeric_limits<uint32_t>::max();
constexpr int16_t kFiscalYear = 2022;
constexpr size_t kTopK = 100;
constexpr size_t kGlobalMaxExpected = 131072;
constexpr size_t kMorselSize = 32768;

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& offsets_path, const std::string& data_path) {
        offsets.open(offsets_path);
        data.open(data_path);
    }

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
    }

    std::string_view view(uint32_t code) const {
        if (code + 1 >= offsets.size()) {
            return {};
        }
        const uint64_t begin = offsets[code];
        const uint64_t end = offsets[code + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }
};

template <typename CodeT>
CodeT resolve_code(const DictView& dict, std::string_view target) {
    for (uint32_t code = 0; code < dict.size(); ++code) {
        if (dict.view(code) == target) {
            return static_cast<CodeT>(code);
        }
    }
    return std::numeric_limits<CodeT>::max();
}

template <typename T>
bool find_postings_group(const MmapColumn<T>& values, T needle, size_t& group_idx) {
    const T* begin = values.data;
    const T* end = values.data + values.size();
    const T* it = std::lower_bound(begin, end, needle);
    if (it == end || *it != needle) {
        return false;
    }
    group_idx = static_cast<size_t>(it - begin);
    return true;
}

void ensure_dir(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("mkdir failed for " + path);
    }
}

void write_csv_escaped(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (!needs_quotes) {
        if (!value.empty()) {
            std::fwrite(value.data(), 1, value.size(), out);
        }
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

struct ResultRow {
    uint32_t name_code;
    uint32_t tag_code;
    double value;
};

struct RankComparator {
    const DictView& name_dict;
    const DictView& tag_dict;

    int compare(const ResultRow& lhs, const ResultRow& rhs) const {
        if (lhs.value > rhs.value) return -1;
        if (lhs.value < rhs.value) return 1;

        const int name_cmp = name_dict.view(lhs.name_code).compare(name_dict.view(rhs.name_code));
        if (name_cmp < 0) return -1;
        if (name_cmp > 0) return 1;

        const int tag_cmp = tag_dict.view(lhs.tag_code).compare(tag_dict.view(rhs.tag_code));
        if (tag_cmp < 0) return -1;
        if (tag_cmp > 0) return 1;

        return 0;
    }

    bool better(const ResultRow& lhs, const ResultRow& rhs) const {
        return compare(lhs, rhs) < 0;
    }

    bool worse(const ResultRow& lhs, const ResultRow& rhs) const {
        return compare(lhs, rhs) > 0;
    }
};

struct TopKBuffer {
    explicit TopKBuffer(const RankComparator& comparator) : cmp(comparator) {
        rows.reserve(kTopK);
    }

    void consider(const ResultRow& row) {
        if (rows.size() < kTopK) {
            rows.push_back(row);
            worst_dirty = true;
            return;
        }
        if (worst_dirty) {
            recompute_worst();
        }
        if (cmp.better(row, rows[worst_idx])) {
            rows[worst_idx] = row;
            worst_dirty = true;
        }
    }

    void finalize() {
        std::sort(rows.begin(), rows.end(), [&](const ResultRow& lhs, const ResultRow& rhs) {
            return cmp.better(lhs, rhs);
        });
        if (rows.size() > kTopK) {
            rows.resize(kTopK);
        }
    }

    std::vector<ResultRow> rows;

private:
    void recompute_worst() {
        worst_idx = 0;
        for (size_t idx = 1; idx < rows.size(); ++idx) {
            if (cmp.worse(rows[idx], rows[worst_idx])) {
                worst_idx = idx;
            }
        }
        worst_dirty = false;
    }

    const RankComparator& cmp;
    size_t worst_idx = 0;
    bool worst_dirty = false;
};

inline uint64_t pack_key(uint32_t adsh_code, uint32_t tag_code) {
    return (static_cast<uint64_t>(adsh_code) << 32) | static_cast<uint64_t>(tag_code);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        MmapColumn<uint32_t> num_adsh;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<double> num_value;
        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;

        MmapColumn<uint16_t> num_uom_postings_values;
        MmapColumn<uint64_t> num_uom_postings_offsets;
        MmapColumn<uint32_t> num_uom_postings_rowids;

        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;

        DictView num_uom_dict;
        DictView sub_name_dict;
        DictView global_tag_dict;

        {
            GENDB_PHASE("data_loading");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom.offsets.bin", gendb_dir + "/dicts/num_uom.data.bin");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name.offsets.bin", gendb_dir + "/dicts/sub_name.data.bin");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag.offsets.bin", gendb_dir + "/dicts/global_tag.data.bin");

            num_adsh.prefetch();
            num_tag.prefetch();
            num_value.prefetch();
            sub_adsh.prefetch();
            sub_name.prefetch();
            num_uom_postings_rowids.prefetch();
            sub_fy_postings_rowids.prefetch();
        }

        uint16_t pure_code = std::numeric_limits<uint16_t>::max();
        const uint32_t* pure_rowids = nullptr;
        size_t pure_row_count = 0;
        std::vector<uint32_t> fy2022_sub_rowid_by_adsh;

        {
            GENDB_PHASE("dim_filter");

            pure_code = resolve_code<uint16_t>(num_uom_dict, "pure");
            if (pure_code == std::numeric_limits<uint16_t>::max()) {
                throw std::runtime_error("Failed to resolve num.uom code for 'pure'");
            }

            size_t pure_group_idx = 0;
            if (!find_postings_group(num_uom_postings_values, pure_code, pure_group_idx)) {
                pure_rowids = nullptr;
                pure_row_count = 0;
            } else {
                const uint64_t pure_begin = num_uom_postings_offsets[pure_group_idx];
                const uint64_t pure_end = num_uom_postings_offsets[pure_group_idx + 1];
                pure_rowids = num_uom_postings_rowids.data + pure_begin;
                pure_row_count = static_cast<size_t>(pure_end - pure_begin);
            }

            fy2022_sub_rowid_by_adsh.assign(sub_adsh_dense_lookup.size(), kRowidNull);
            size_t fy_group_idx = 0;
            if (find_postings_group(sub_fy_postings_values, kFiscalYear, fy_group_idx)) {
                const uint64_t fy_begin = sub_fy_postings_offsets[fy_group_idx];
                const uint64_t fy_end = sub_fy_postings_offsets[fy_group_idx + 1];
                for (uint64_t idx = fy_begin; idx < fy_end; ++idx) {
                    const uint32_t sub_rowid = sub_fy_postings_rowids[idx];
                    const uint32_t adsh_code = sub_adsh[sub_rowid];
                    if (adsh_code < fy2022_sub_rowid_by_adsh.size()) {
                        fy2022_sub_rowid_by_adsh[adsh_code] = sub_rowid;
                    }
                }
            }
        }

        CompactHashMap<uint64_t, double> max_map;
        {
            GENDB_PHASE("build_joins");

            const int thread_count = std::max(1, omp_get_max_threads());
            std::vector<CompactHashMap<uint64_t, double>> thread_maps(static_cast<size_t>(thread_count));

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                const size_t per_thread_expected = std::max<size_t>(4096, pure_row_count / static_cast<size_t>(thread_count * 8) + 1024);
                CompactHashMap<uint64_t, double> local_map(per_thread_expected);

                #pragma omp for schedule(dynamic, 1)
                for (size_t morsel_begin = 0; morsel_begin < pure_row_count; morsel_begin += kMorselSize) {
                    const size_t morsel_end = std::min(morsel_begin + kMorselSize, pure_row_count);
                    for (size_t idx = morsel_begin; idx < morsel_end; ++idx) {
                        const uint32_t rowid = pure_rowids[idx];
                        const uint32_t adsh_code = num_adsh[rowid];
                        if (adsh_code >= fy2022_sub_rowid_by_adsh.size()) {
                            continue;
                        }
                        const uint32_t sub_rowid = fy2022_sub_rowid_by_adsh[adsh_code];
                        if (sub_rowid == kRowidNull) {
                            continue;
                        }
                        const double value = num_value[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }
                        const uint32_t tag_code = num_tag[rowid];
                        const uint64_t key = pack_key(adsh_code, tag_code);
                        if (double* slot = local_map.find(key)) {
                            if (value > *slot) {
                                *slot = value;
                            }
                        } else {
                            local_map.insert(key, value);
                        }
                    }
                }

                thread_maps[static_cast<size_t>(tid)] = std::move(local_map);
            }

            max_map.reserve(kGlobalMaxExpected);
            for (const auto& local_map : thread_maps) {
                for (auto it : local_map) {
                    const uint64_t key = it.first;
                    const double value = it.second;
                    if (double* slot = max_map.find(key)) {
                        if (value > *slot) {
                            *slot = value;
                        }
                    } else {
                        max_map.insert(key, value);
                    }
                }
            }
        }

        std::vector<ResultRow> final_rows;
        {
            GENDB_PHASE("main_scan");

            const RankComparator rank_cmp{sub_name_dict, global_tag_dict};
            const int thread_count = std::max(1, omp_get_max_threads());
            std::vector<std::vector<ResultRow>> thread_topk(static_cast<size_t>(thread_count));

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                TopKBuffer local_topk(rank_cmp);

                #pragma omp for schedule(dynamic, 1)
                for (size_t morsel_begin = 0; morsel_begin < pure_row_count; morsel_begin += kMorselSize) {
                    const size_t morsel_end = std::min(morsel_begin + kMorselSize, pure_row_count);
                    for (size_t idx = morsel_begin; idx < morsel_end; ++idx) {
                        const uint32_t rowid = pure_rowids[idx];
                        const uint32_t adsh_code = num_adsh[rowid];
                        if (adsh_code >= fy2022_sub_rowid_by_adsh.size()) {
                            continue;
                        }
                        const uint32_t sub_rowid = fy2022_sub_rowid_by_adsh[adsh_code];
                        if (sub_rowid == kRowidNull) {
                            continue;
                        }
                        const double value = num_value[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }
                        const uint32_t tag_code = num_tag[rowid];
                        const uint64_t key = pack_key(adsh_code, tag_code);
                        const double* max_value = max_map.find(key);
                        if (max_value == nullptr || value != *max_value) {
                            continue;
                        }
                        local_topk.consider(ResultRow{sub_name[sub_rowid], tag_code, value});
                    }
                }

                local_topk.finalize();
                thread_topk[static_cast<size_t>(tid)] = std::move(local_topk.rows);
            }

            const RankComparator rank_cmp_merge{sub_name_dict, global_tag_dict};
            TopKBuffer global_topk(rank_cmp_merge);
            for (auto& local_rows : thread_topk) {
                for (const ResultRow& row : local_rows) {
                    global_topk.consider(row);
                }
            }
            global_topk.finalize();
            final_rows = std::move(global_topk.rows);
        }

        {
            GENDB_PHASE("output");

            ensure_dir(results_dir);
            FILE* out = std::fopen((results_dir + "/Q2.csv").c_str(), "w");
            if (out == nullptr) {
                throw std::runtime_error("Failed to open output CSV");
            }

            std::fprintf(out, "name,tag,value\n");
            for (const ResultRow& row : final_rows) {
                write_csv_escaped(out, sub_name_dict.view(row.name_code));
                std::fputc(',', out);
                write_csv_escaped(out, global_tag_dict.view(row.tag_code));
                std::fprintf(out, ",%.2f\n", row.value);
            }
            std::fclose(out);
        }
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Error: %s\n", ex.what());
        return 1;
    }

    return 0;
}
