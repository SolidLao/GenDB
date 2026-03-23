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

constexpr int32_t kInvalidGid = -1;
constexpr uint32_t kInvalidRowId = std::numeric_limits<uint32_t>::max();
constexpr size_t kTopK = 100;
constexpr int32_t kTargetFiscalYear = 2022;
constexpr int32_t kMorselRows = 262144;
constexpr int kValueScaleDigits = 4;

constexpr __int128 pow10_i128(int exponent) {
    __int128 value = 1;
    for (int i = 0; i < exponent; ++i) {
        value *= 10;
    }
    return value;
}

constexpr __int128 kValueScale = pow10_i128(kValueScaleDigits);

template <typename T>
void require_same_size(const char* name, const MmapColumn<T>& col, size_t expected) {
    if (col.size() != expected) {
        throw std::runtime_error(std::string("unexpected row count for ") + name);
    }
}

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

__int128 double_to_scaled_decimal(double value) {
    const long double scaled = static_cast<long double>(value) * static_cast<long double>(kValueScale);
    if (scaled >= 0) {
        return static_cast<__int128>(scaled + 0.5L);
    }
    return static_cast<__int128>(scaled - 0.5L);
}

void write_i128(FILE* out, __int128 value) {
    if (value == 0) {
        std::fputc('0', out);
        return;
    }

    char buffer[64];
    int pos = 0;
    while (value > 0) {
        buffer[pos++] = static_cast<char>('0' + static_cast<int>(value % 10));
        value /= 10;
    }
    while (pos > 0) {
        std::fputc(buffer[--pos], out);
    }
}

void write_scaled_money_2(FILE* out, __int128 scaled_value) {
    const bool negative = scaled_value < 0;
    const __int128 abs_value = negative ? -scaled_value : scaled_value;
    const __int128 cent_divisor = kValueScale / 100;
    const __int128 cents = (abs_value + cent_divisor / 2) / cent_divisor;
    const __int128 whole = cents / 100;
    const int frac = static_cast<int>(cents % 100);

    if (negative && cents != 0) {
        std::fputc('-', out);
    }
    write_i128(out, whole);
    std::fprintf(out, ".%02d", frac);
}

struct OuterGroupInfo {
    uint32_t name_code;
    int32_t cik;
};

struct ResultRow {
    int32_t outer_gid;
    __int128 total_scaled;
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
        MmapColumn<uint32_t> num_adsh;
        MmapColumn<double> num_value;

        MmapColumn<uint32_t> sub_name;
        MmapColumn<int32_t> sub_cik;

        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;

        DictView num_uom_dict;
        DictView sub_name_dict;

        {
            GENDB_PHASE("data_loading");

            num_uom.open(gendb_dir + "/num/uom.bin");
            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_value.open(gendb_dir + "/num/value.bin");

            sub_name.open(gendb_dir + "/sub/name.bin");
            sub_cik.open(gendb_dir + "/sub/cik.bin");

            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name");

            require_same_size("num.adsh", num_adsh, num_uom.size());
            require_same_size("num.value", num_value, num_uom.size());
            require_same_size("sub.cik", sub_cik, sub_name.size());

            num_uom.advise_sequential();
            num_adsh.advise_sequential();
            num_value.advise_sequential();
            sub_fy_postings_rowids.advise_sequential();
            sub_adsh_dense_lookup.advise_sequential();

            mmap_prefetch_all(num_uom, num_adsh, num_value, sub_name, sub_cik,
                              sub_fy_postings_rowids, sub_adsh_dense_lookup);
        }

        uint16_t usd_code = 0;
        uint64_t fy_begin = 0;
        uint64_t fy_end = 0;

        std::vector<int32_t> filing_to_inner_gid;
        std::vector<int32_t> filing_to_outer_gid;
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
                static_cast<int16_t>(kTargetFiscalYear),
                fy_begin,
                fy_end);

            if (has_fy_2022) {
                const size_t filing_count = static_cast<size_t>(fy_end - fy_begin);
                filing_to_inner_gid.reserve(filing_count);
                filing_to_outer_gid.reserve(filing_count);
                outer_groups.reserve(filing_count);

                std::unordered_map<int32_t, int32_t> inner_gid_by_cik;
                std::unordered_map<uint64_t, int32_t> outer_gid_by_key;
                inner_gid_by_cik.reserve(filing_count * 2);
                outer_gid_by_key.reserve(filing_count * 2);

                for (uint64_t idx = fy_begin; idx < fy_end; ++idx) {
                    const uint32_t sub_rowid = sub_fy_postings_rowids[idx];
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

                    filing_to_inner_gid.push_back(inner_gid);
                    filing_to_outer_gid.push_back(outer_gid);
                }

                inner_group_count = inner_gid_by_cik.size();
            }
        }

        const size_t filing_count = filing_to_inner_gid.size();
        const size_t outer_group_count = outer_groups.size();
        const int thread_count = std::max(1, omp_get_num_procs());

        std::vector<int32_t> adsh_code_to_filing_gid;
        std::vector<__int128> thread_local_filing_sums;
        std::vector<uint8_t> thread_local_filing_seen;

        {
            GENDB_PHASE("build_joins");

            adsh_code_to_filing_gid.assign(sub_adsh_dense_lookup.size(), kInvalidGid);

            if (filing_count > 0) {
                std::vector<int32_t> filtered_rowid_to_filing_gid(sub_name.size(), kInvalidGid);
                for (size_t filing_gid = 0; filing_gid < filing_count; ++filing_gid) {
                    const uint32_t sub_rowid = sub_fy_postings_rowids[fy_begin + filing_gid];
                    filtered_rowid_to_filing_gid[sub_rowid] = static_cast<int32_t>(filing_gid);
                }

                for (size_t adsh_code = 0; adsh_code < sub_adsh_dense_lookup.size(); ++adsh_code) {
                    const uint32_t sub_rowid = sub_adsh_dense_lookup[adsh_code];
                    if (sub_rowid == kInvalidRowId || sub_rowid >= filtered_rowid_to_filing_gid.size()) {
                        continue;
                    }
                    const int32_t filing_gid = filtered_rowid_to_filing_gid[sub_rowid];
                    if (filing_gid != kInvalidGid) {
                        adsh_code_to_filing_gid[adsh_code] = filing_gid;
                    }
                }
            }

            thread_local_filing_sums.assign(static_cast<size_t>(thread_count) * filing_count, 0);
            thread_local_filing_seen.assign(static_cast<size_t>(thread_count) * filing_count, 0);
        }

        {
            GENDB_PHASE("main_scan");

            if (filing_count > 0) {
                const size_t row_count = num_uom.size();
                const uint16_t* __restrict uom = num_uom.data;
                const uint32_t* __restrict adsh = num_adsh.data;
                const double* __restrict value = num_value.data;
                const int32_t* __restrict filing_gid_lookup = adsh_code_to_filing_gid.data();
                const size_t lookup_size = adsh_code_to_filing_gid.size();

#pragma omp parallel num_threads(thread_count)
                {
                    const int tid = omp_get_thread_num();
                    __int128* __restrict local_sums =
                        thread_local_filing_sums.data() + static_cast<size_t>(tid) * filing_count;
                    uint8_t* __restrict local_seen =
                        thread_local_filing_seen.data() + static_cast<size_t>(tid) * filing_count;

#pragma omp for schedule(static, kMorselRows)
                    for (size_t row = 0; row < row_count; ++row) {
                        if (uom[row] != usd_code) {
                            continue;
                        }

                        const double raw_value = value[row];
                        if (std::isnan(raw_value)) {
                            continue;
                        }

                        const uint32_t adsh_code = adsh[row];
                        if (adsh_code >= lookup_size) {
                            continue;
                        }

                        const int32_t filing_gid = filing_gid_lookup[adsh_code];
                        if (filing_gid == kInvalidGid) {
                            continue;
                        }

                        local_sums[filing_gid] += double_to_scaled_decimal(raw_value);
                        local_seen[filing_gid] = 1;
                    }
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
                std::vector<__int128> filing_totals(filing_count, 0);
                std::vector<uint8_t> filing_present(filing_count, 0);

                std::vector<__int128> inner_totals(inner_group_count, 0);
                std::vector<uint8_t> inner_present(inner_group_count, 0);
                size_t inner_present_count = 0;

                for (size_t filing_gid = 0; filing_gid < filing_count; ++filing_gid) {
                    __int128 filing_total = 0;
                    uint8_t present = 0;

                    for (int tid = 0; tid < thread_count; ++tid) {
                        const size_t idx = static_cast<size_t>(tid) * filing_count + filing_gid;
                        filing_total += thread_local_filing_sums[idx];
                        present |= thread_local_filing_seen[idx];
                    }

                    filing_totals[filing_gid] = filing_total;
                    filing_present[filing_gid] = present;
                    if (!present) {
                        continue;
                    }

                    const int32_t inner_gid = filing_to_inner_gid[filing_gid];
                    inner_totals[inner_gid] += filing_total;
                    if (!inner_present[inner_gid]) {
                        inner_present[inner_gid] = 1;
                        ++inner_present_count;
                    }
                }

                __int128 sum_sub_totals = 0;
                if (inner_present_count > 0) {
                    for (size_t inner_gid = 0; inner_gid < inner_group_count; ++inner_gid) {
                        if (inner_present[inner_gid]) {
                            sum_sub_totals += inner_totals[inner_gid];
                        }
                    }
                }

                std::vector<__int128> outer_totals(outer_group_count, 0);
                std::vector<uint8_t> outer_present(outer_group_count, 0);
                for (size_t filing_gid = 0; filing_gid < filing_count; ++filing_gid) {
                    if (!filing_present[filing_gid]) {
                        continue;
                    }
                    const int32_t outer_gid = filing_to_outer_gid[filing_gid];
                    outer_totals[outer_gid] += filing_totals[filing_gid];
                    outer_present[outer_gid] = 1;
                }

                std::vector<ResultRow> rows;
                rows.reserve(outer_group_count);
                for (size_t outer_gid = 0; outer_gid < outer_group_count; ++outer_gid) {
                    if (!outer_present[outer_gid]) {
                        continue;
                    }
                    const __int128 total_scaled = outer_totals[outer_gid];
                    if (inner_present_count > 0 &&
                        total_scaled * static_cast<__int128>(inner_present_count) > sum_sub_totals) {
                        rows.push_back(ResultRow{static_cast<int32_t>(outer_gid), total_scaled});
                    }
                }

                const auto better = [&](const ResultRow& lhs, const ResultRow& rhs) {
                    if (lhs.total_scaled != rhs.total_scaled) {
                        return lhs.total_scaled > rhs.total_scaled;
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
                    std::fprintf(out, ",%d,", group.cik);
                    write_scaled_money_2(out, row.total_scaled);
                    std::fputc('\n', out);
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
