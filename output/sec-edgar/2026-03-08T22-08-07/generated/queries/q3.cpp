#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr size_t kTopK = 100;
constexpr int16_t kTargetFiscalYear = 2022;
constexpr int kTaskChunkSize = 32;

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& base_path) {
        offsets.open(base_path + ".offsets.bin");
        data.open(base_path + ".data.bin");
        if (offsets.size() == 0) {
            throw std::runtime_error("empty dictionary: " + base_path);
        }
    }

    template <typename CodeT>
    bool resolve(std::string_view literal, CodeT& out_code) const {
        const size_t dict_size = offsets.size() - 1;
        for (size_t code = 0; code < dict_size; ++code) {
            const uint64_t begin = offsets[code];
            const uint64_t end = offsets[code + 1];
            const size_t len = static_cast<size_t>(end - begin);
            if (len != literal.size()) {
                continue;
            }
            if (std::char_traits<char>::compare(data.data + begin, literal.data(), len) == 0) {
                out_code = static_cast<CodeT>(code);
                return true;
            }
        }
        return false;
    }

    std::string_view decode(uint32_t code) const {
        const size_t idx = static_cast<size_t>(code);
        if (idx + 1 >= offsets.size()) {
            throw std::runtime_error("dictionary code out of range");
        }
        const uint64_t begin = offsets[idx];
        const uint64_t end = offsets[idx + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }
};

template <typename T>
void require_same_size(const char* name, const MmapColumn<T>& col, size_t expected) {
    if (col.size() != expected) {
        throw std::runtime_error(std::string("unexpected row count for ") + name);
    }
}

template <typename T>
bool find_postings_slice(const MmapColumn<T>& values,
                         const MmapColumn<uint64_t>& offsets,
                         T needle,
                         uint64_t& begin,
                         uint64_t& end) {
    const T* it = std::lower_bound(values.data, values.data + values.size(), needle);
    if (it == values.data + values.size() || *it != needle) {
        begin = 0;
        end = 0;
        return false;
    }
    const size_t idx = static_cast<size_t>(it - values.data);
    begin = offsets[idx];
    end = offsets[idx + 1];
    return true;
}

inline uint64_t pack_outer_key(uint32_t name_code, int32_t cik) {
    return (static_cast<uint64_t>(name_code) << 32) | static_cast<uint32_t>(cik);
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

struct FilingInfo {
    uint32_t sub_rowid;
    uint32_t adsh_code;
    int32_t inner_gid;
    int32_t outer_gid;
};

struct FilingTask {
    uint32_t sub_rowid;
    int32_t inner_gid;
    int32_t outer_gid;
    uint64_t begin;
    uint64_t end;
};

struct OuterGroupInfo {
    uint32_t name_code;
    int32_t cik;
};

struct ResultRow {
    int32_t outer_gid;
    long double total_value;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        MmapColumn<uint16_t> num_uom;
        MmapColumn<double> num_value;

        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;
        MmapColumn<int32_t> sub_cik;

        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;

        MmapColumn<uint64_t> num_adsh_offsets;
        MmapColumn<uint32_t> num_adsh_rowids;

        DictView num_uom_dict;
        DictView sub_name_dict;

        {
            GENDB_PHASE("data_loading");

            num_uom.open(gendb_dir + "/num/uom.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");

            num_adsh_offsets.open(gendb_dir + "/column_versions/num.adsh.postings_dense/offsets.bin");
            num_adsh_rowids.open(gendb_dir + "/column_versions/num.adsh.postings_dense/rowids.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name");

            require_same_size("num.value", num_value, num_uom.size());
            require_same_size("sub.name", sub_name, sub_adsh.size());
            require_same_size("sub.cik", sub_cik, sub_adsh.size());
            require_same_size("num.adsh.postings_dense.rowids", num_adsh_rowids, num_uom.size());

            num_uom.advise_random();
            num_value.advise_random();
            sub_adsh.advise_sequential();
            sub_name.advise_sequential();
            sub_cik.advise_sequential();
            sub_fy_postings_values.advise_sequential();
            sub_fy_postings_offsets.advise_sequential();
            sub_fy_postings_rowids.advise_sequential();
            num_adsh_offsets.advise_random();
            num_adsh_rowids.advise_sequential();

            mmap_prefetch_all(num_uom, num_value, sub_adsh, sub_name, sub_cik,
                              sub_fy_postings_values, sub_fy_postings_offsets,
                              sub_fy_postings_rowids, num_adsh_offsets, num_adsh_rowids,
                              num_uom_dict.offsets, num_uom_dict.data,
                              sub_name_dict.offsets, sub_name_dict.data);
        }

        uint16_t usd_code = 0;
        uint64_t fy_begin = 0;
        uint64_t fy_end = 0;

        std::vector<FilingInfo> filtered_filings;
        std::vector<OuterGroupInfo> outer_groups;
        size_t inner_group_count = 0;

        {
            GENDB_PHASE("dim_filter");

            if (!num_uom_dict.resolve<uint16_t>("USD", usd_code)) {
                throw std::runtime_error("failed to resolve num.uom code for USD");
            }

            const bool has_fy_2022 = find_postings_slice<int16_t>(
                sub_fy_postings_values,
                sub_fy_postings_offsets,
                kTargetFiscalYear,
                fy_begin,
                fy_end);

            if (has_fy_2022) {
                const size_t filing_count = static_cast<size_t>(fy_end - fy_begin);
                filtered_filings.reserve(filing_count);
                outer_groups.reserve(filing_count);

                std::unordered_map<int32_t, int32_t> inner_gid_by_cik;
                std::unordered_map<uint64_t, int32_t> outer_gid_by_key;
                inner_gid_by_cik.reserve(filing_count * 2);
                outer_gid_by_key.reserve(filing_count * 2);

                for (uint64_t idx = fy_begin; idx < fy_end; ++idx) {
                    const uint32_t sub_rowid = sub_fy_postings_rowids[idx];
                    const uint32_t adsh_code = sub_adsh[sub_rowid];
                    const int32_t cik = sub_cik[sub_rowid];
                    const uint32_t name_code = sub_name[sub_rowid];

                    auto inner_it = inner_gid_by_cik.find(cik);
                    int32_t inner_gid;
                    if (inner_it == inner_gid_by_cik.end()) {
                        inner_gid = static_cast<int32_t>(inner_gid_by_cik.size());
                        inner_gid_by_cik.emplace(cik, inner_gid);
                    } else {
                        inner_gid = inner_it->second;
                    }

                    const uint64_t outer_key = pack_outer_key(name_code, cik);
                    auto outer_it = outer_gid_by_key.find(outer_key);
                    int32_t outer_gid;
                    if (outer_it == outer_gid_by_key.end()) {
                        outer_gid = static_cast<int32_t>(outer_groups.size());
                        outer_gid_by_key.emplace(outer_key, outer_gid);
                        outer_groups.push_back(OuterGroupInfo{name_code, cik});
                    } else {
                        outer_gid = outer_it->second;
                    }

                    filtered_filings.push_back(FilingInfo{sub_rowid, adsh_code, inner_gid, outer_gid});
                }

                inner_group_count = inner_gid_by_cik.size();
            }
        }

        std::vector<FilingTask> filing_tasks;
        {
            GENDB_PHASE("build_joins");

            filing_tasks.reserve(filtered_filings.size());
            std::sort(filtered_filings.begin(), filtered_filings.end(), [](const FilingInfo& lhs, const FilingInfo& rhs) {
                return lhs.adsh_code < rhs.adsh_code;
            });

            for (const FilingInfo& filing : filtered_filings) {
                const size_t adsh_idx = static_cast<size_t>(filing.adsh_code);
                if (adsh_idx + 1 >= num_adsh_offsets.size()) {
                    throw std::runtime_error("sub.adsh code out of bounds for num.adsh.postings_dense");
                }
                const uint64_t begin = num_adsh_offsets[adsh_idx];
                const uint64_t end = num_adsh_offsets[adsh_idx + 1];
                filing_tasks.push_back(FilingTask{filing.sub_rowid, filing.inner_gid, filing.outer_gid, begin, end});
            }
        }

        const size_t filing_count = filing_tasks.size();
        const size_t outer_group_count = outer_groups.size();
        const int thread_count = std::max(1, omp_get_num_procs());

        std::vector<long double> filing_totals(filing_count, 0.0L);
        std::vector<uint8_t> filing_present(filing_count, 0);

        {
            GENDB_PHASE("main_scan");

            if (filing_count > 0) {
                const uint32_t* __restrict postings_rowids = num_adsh_rowids.data;
                const uint16_t* __restrict uom = num_uom.data;
                const double* __restrict value = num_value.data;

#pragma omp parallel for schedule(dynamic, kTaskChunkSize) num_threads(thread_count)
                for (size_t filing_gid = 0; filing_gid < filing_count; ++filing_gid) {
                    const FilingTask& task = filing_tasks[filing_gid];
                    long double sum = 0.0L;
                    long double correction = 0.0L;
                    uint8_t present = 0;

                    for (uint64_t pos = task.begin; pos < task.end; ++pos) {
                        const uint32_t rowid = postings_rowids[pos];
                        if (uom[rowid] != usd_code) {
                            continue;
                        }

                        const double raw_value = value[rowid];
                        if (std::isnan(raw_value)) {
                            continue;
                        }

                        const long double y = static_cast<long double>(raw_value) - correction;
                        const long double t = sum + y;
                        correction = (t - sum) - y;
                        sum = t;
                        present = 1;
                    }

                    filing_totals[filing_gid] = sum;
                    filing_present[filing_gid] = present;
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::filesystem::create_directories(results_dir);
            FILE* out = std::fopen((results_dir + "/Q3.csv").c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }

            std::fprintf(out, "name,cik,total_value\n");

            if (filing_count > 0 && inner_group_count > 0 && outer_group_count > 0) {
                std::vector<long double> inner_totals(inner_group_count, 0.0L);
                std::vector<long double> outer_totals(outer_group_count, 0.0L);
                std::vector<uint8_t> inner_present(inner_group_count, 0);
                std::vector<uint8_t> outer_present(outer_group_count, 0);
                size_t inner_present_count = 0;

                for (size_t filing_gid = 0; filing_gid < filing_count; ++filing_gid) {
                    if (!filing_present[filing_gid]) {
                        continue;
                    }

                    const FilingTask& task = filing_tasks[filing_gid];
                    const long double filing_total = filing_totals[filing_gid];

                    inner_totals[static_cast<size_t>(task.inner_gid)] += filing_total;
                    outer_totals[static_cast<size_t>(task.outer_gid)] += filing_total;
                    outer_present[static_cast<size_t>(task.outer_gid)] = 1;
                    if (!inner_present[static_cast<size_t>(task.inner_gid)]) {
                        inner_present[static_cast<size_t>(task.inner_gid)] = 1;
                        ++inner_present_count;
                    }
                }

                long double avg_sub_total = 0.0L;
                if (inner_present_count > 0) {
                    long double sum_sub_totals = 0.0L;
                    for (size_t inner_gid = 0; inner_gid < inner_group_count; ++inner_gid) {
                        if (inner_present[inner_gid]) {
                            sum_sub_totals += inner_totals[inner_gid];
                        }
                    }
                    avg_sub_total = sum_sub_totals / static_cast<long double>(inner_present_count);
                }

                std::vector<ResultRow> rows;
                rows.reserve(outer_group_count);
                for (size_t outer_gid = 0; outer_gid < outer_group_count; ++outer_gid) {
                    if (!outer_present[outer_gid]) {
                        continue;
                    }
                    const long double total_value = outer_totals[outer_gid];
                    if (total_value > avg_sub_total) {
                        rows.push_back(ResultRow{static_cast<int32_t>(outer_gid), total_value});
                    }
                }

                const auto better = [&](const ResultRow& lhs, const ResultRow& rhs) {
                    if (lhs.total_value != rhs.total_value) {
                        return lhs.total_value > rhs.total_value;
                    }
                    const OuterGroupInfo& left_group = outer_groups[static_cast<size_t>(lhs.outer_gid)];
                    const OuterGroupInfo& right_group = outer_groups[static_cast<size_t>(rhs.outer_gid)];
                    if (left_group.name_code != right_group.name_code) {
                        return left_group.name_code < right_group.name_code;
                    }
                    return left_group.cik < right_group.cik;
                };

                if (rows.size() > kTopK) {
                    std::nth_element(rows.begin(), rows.begin() + kTopK, rows.end(), better);
                    rows.resize(kTopK);
                }
                std::sort(rows.begin(), rows.end(), better);

                for (const ResultRow& row : rows) {
                    const OuterGroupInfo& group = outer_groups[static_cast<size_t>(row.outer_gid)];
                    write_csv_escaped(out, sub_name_dict.decode(group.name_code));
                    std::fprintf(out, ",%d,%.2Lf\n", group.cik, row.total_value);
                }
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Error: %s\n", ex.what());
        return 1;
    }
}
