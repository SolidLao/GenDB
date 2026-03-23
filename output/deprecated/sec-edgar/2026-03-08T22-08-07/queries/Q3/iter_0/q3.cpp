#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr int32_t INVALID_GID = -1;
constexpr uint32_t INVALID_ROWID = std::numeric_limits<uint32_t>::max();

constexpr int VALUE_SCALE_DIGITS = 12;

constexpr __int128 pow10_i128(int exponent) {
    __int128 value = 1;
    for (int i = 0; i < exponent; ++i) {
        value *= 10;
    }
    return value;
}

constexpr __int128 VALUE_SCALE = pow10_i128(VALUE_SCALE_DIGITS);

struct OuterGroupInfo {
    uint32_t name_code;
    int32_t cik;
};

struct ResultRow {
    uint32_t gid;
    __int128 total_scaled;
};

static inline uint64_t make_outer_key(uint32_t name_code, int32_t cik) {
    return (static_cast<uint64_t>(name_code) << 32) | static_cast<uint32_t>(cik);
}

template <typename T>
bool find_postings_slice(const MmapColumn<T>& values,
                        const MmapColumn<uint64_t>& offsets,
                        T needle,
                        uint64_t& begin,
                        uint64_t& end) {
    const T* it = std::lower_bound(values.data, values.data + values.count, needle);
    if (it == values.data + values.count || *it != needle) {
        begin = 0;
        end = 0;
        return false;
    }
    const size_t idx = static_cast<size_t>(it - values.data);
    begin = offsets[idx];
    end = offsets[idx + 1];
    return true;
}

template <typename CodeT>
bool resolve_dict_code(const MmapColumn<uint64_t>& offsets,
                      const MmapColumn<char>& data,
                      std::string_view literal,
                      CodeT& out_code) {
    if (offsets.count == 0) {
        return false;
    }
    const size_t dict_size = offsets.count - 1;
    for (size_t code = 0; code < dict_size; ++code) {
        const uint64_t start = offsets[code];
        const uint64_t stop = offsets[code + 1];
        const size_t len = static_cast<size_t>(stop - start);
        if (len != literal.size()) {
            continue;
        }
        if (std::char_traits<char>::compare(data.data + start, literal.data(), len) == 0) {
            out_code = static_cast<CodeT>(code);
            return true;
        }
    }
    return false;
}

std::string_view decode_dict_string(const MmapColumn<uint64_t>& offsets,
                                    const MmapColumn<char>& data,
                                    uint32_t code) {
    if (offsets.count == 0 || static_cast<size_t>(code + 1) >= offsets.count) {
        return {};
    }
    const uint64_t start = offsets[code];
    const uint64_t stop = offsets[code + 1];
    return std::string_view(data.data + start, static_cast<size_t>(stop - start));
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


__int128 parse_decimal_to_scaled(const char* begin, const char* end) {
    bool negative = false;
    if (begin != end && (*begin == '-' || *begin == '+')) {
        negative = (*begin == '-');
        ++begin;
    }

    __int128 digits = 0;
    int fractional_digits = 0;
    bool seen_decimal = false;
    const char* ptr = begin;
    while (ptr != end && *ptr != 'e' && *ptr != 'E') {
        if (*ptr == '.') {
            seen_decimal = true;
        } else {
            digits = digits * 10 + static_cast<unsigned>(*ptr - '0');
            if (seen_decimal) {
                ++fractional_digits;
            }
        }
        ++ptr;
    }

    int exponent10 = 0;
    if (ptr != end && (*ptr == 'e' || *ptr == 'E')) {
        ++ptr;
        bool exponent_negative = false;
        if (ptr != end && (*ptr == '-' || *ptr == '+')) {
            exponent_negative = (*ptr == '-');
            ++ptr;
        }
        while (ptr != end) {
            exponent10 = exponent10 * 10 + static_cast<unsigned>(*ptr - '0');
            ++ptr;
        }
        if (exponent_negative) {
            exponent10 = -exponent10;
        }
    }

    const int shift = VALUE_SCALE_DIGITS + exponent10 - fractional_digits;
    __int128 scaled = 0;
    if (shift >= 0) {
        scaled = digits * pow10_i128(shift);
    } else {
        const __int128 divisor = pow10_i128(-shift);
        scaled = (digits + divisor / 2) / divisor;
    }
    return negative ? -scaled : scaled;
}

__int128 double_to_scaled_decimal(double value) {
    char buffer[64];
    auto result = std::to_chars(buffer, buffer + sizeof(buffer), value);
    return parse_decimal_to_scaled(buffer, result.ptr);
}

void write_i128(FILE* out, __int128 value) {
    if (value == 0) {
        std::fputc('0', out);
        return;
    }

    char buffer[64];
    int pos = 0;
    while (value > 0) {
        const int digit = static_cast<int>(value % 10);
        buffer[pos++] = static_cast<char>('0' + digit);
        value /= 10;
    }
    while (pos > 0) {
        std::fputc(buffer[--pos], out);
    }
}

void write_scaled_money_2(FILE* out, __int128 scaled_value) {
    const bool negative = scaled_value < 0;
    __int128 abs_value = negative ? -scaled_value : scaled_value;
    const __int128 cent_divisor = VALUE_SCALE / 100;
    const __int128 cents = (abs_value + cent_divisor / 2) / cent_divisor;
    const __int128 whole = cents / 100;
    const int frac = static_cast<int>(cents % 100);

    if (negative && cents != 0) {
        std::fputc('-', out);
    }
    write_i128(out, whole);
    std::fprintf(out, ".%02d", frac);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 3) {
        return 1;
    }

    GENDB_PHASE("total");

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    MmapColumn<uint64_t> num_uom_dict_offsets;
    MmapColumn<char> num_uom_dict_data;
    MmapColumn<uint16_t> num_uom_postings_values;
    MmapColumn<uint64_t> num_uom_postings_offsets;
    MmapColumn<uint32_t> num_uom_postings_rowids;

    MmapColumn<int16_t> sub_fy_postings_values;
    MmapColumn<uint64_t> sub_fy_postings_offsets;
    MmapColumn<uint32_t> sub_fy_postings_rowids;

    MmapColumn<uint32_t> sub_adsh_dense_lookup;
    MmapColumn<uint32_t> num_adsh;
    MmapColumn<double> num_value;
    MmapColumn<uint32_t> sub_name;
    MmapColumn<int32_t> sub_cik;

    MmapColumn<uint64_t> sub_name_dict_offsets;
    MmapColumn<char> sub_name_dict_data;

    {
        GENDB_PHASE("data_loading");
        num_uom_dict_offsets.open(gendb_dir + "/dicts/num_uom.offsets.bin");
        num_uom_dict_data.open(gendb_dir + "/dicts/num_uom.data.bin");
        num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
        num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
        num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

        sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
        sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
        sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");

        sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");
        sub_adsh_dense_lookup.advise_random();

        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");
        sub_cik.open(gendb_dir + "/sub/cik.bin");

        sub_name_dict_offsets.open(gendb_dir + "/dicts/sub_name.offsets.bin");
        sub_name_dict_data.open(gendb_dir + "/dicts/sub_name.data.bin");

        mmap_prefetch_all(num_uom_postings_rowids, num_adsh, num_value, sub_name, sub_cik, sub_adsh_dense_lookup);
    }

    uint16_t usd_code = 0;
    uint64_t usd_row_begin = 0;
    uint64_t usd_row_end = 0;
    uint64_t fy_row_begin = 0;
    uint64_t fy_row_end = 0;

    std::vector<int32_t> sub_rowid_to_outer_gid(sub_name.count, INVALID_GID);
    std::vector<int32_t> sub_rowid_to_inner_gid(sub_name.count, INVALID_GID);
    std::vector<OuterGroupInfo> outer_groups;
    size_t inner_group_count = 0;

    {
        GENDB_PHASE("dim_filter");

        if (!resolve_dict_code<uint16_t>(num_uom_dict_offsets, num_uom_dict_data, "USD", usd_code)) {
            std::fprintf(stderr, "Failed to resolve num.uom literal USD\n");
            return 1;
        }

        const bool has_usd = find_postings_slice<uint16_t>(
            num_uom_postings_values, num_uom_postings_offsets, usd_code, usd_row_begin, usd_row_end);
        const bool has_fy_2022 = find_postings_slice<int16_t>(
            sub_fy_postings_values, sub_fy_postings_offsets, static_cast<int16_t>(2022), fy_row_begin, fy_row_end);

        if (has_usd && has_fy_2022) {
            std::unordered_map<int32_t, int32_t> inner_gid_by_cik;
            inner_gid_by_cik.reserve(static_cast<size_t>((fy_row_end - fy_row_begin) * 2));

            std::unordered_map<uint64_t, int32_t> outer_gid_by_key;
            outer_gid_by_key.reserve(static_cast<size_t>((fy_row_end - fy_row_begin) * 2));

            outer_groups.reserve(static_cast<size_t>(fy_row_end - fy_row_begin));

            for (uint64_t idx = fy_row_begin; idx < fy_row_end; ++idx) {
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

                const uint64_t outer_key = make_outer_key(name_code, cik);
                auto outer_it = outer_gid_by_key.find(outer_key);
                int32_t outer_gid;
                if (outer_it == outer_gid_by_key.end()) {
                    outer_gid = static_cast<int32_t>(outer_groups.size());
                    outer_gid_by_key.emplace(outer_key, outer_gid);
                    outer_groups.push_back(OuterGroupInfo{name_code, cik});
                } else {
                    outer_gid = outer_it->second;
                }

                sub_rowid_to_inner_gid[sub_rowid] = inner_gid;
                sub_rowid_to_outer_gid[sub_rowid] = outer_gid;
            }

            inner_group_count = inner_gid_by_cik.size();
        }
    }

    const size_t outer_group_count = outer_groups.size();
    const int thread_count = std::max(1, omp_get_max_threads());

    std::vector<__int128> thread_local_inner_sums;
    std::vector<__int128> thread_local_outer_sums;
    std::vector<uint32_t> thread_local_inner_counts;
    std::vector<uint32_t> thread_local_outer_counts;

    {
        GENDB_PHASE("build_joins");
        thread_local_inner_sums.assign(static_cast<size_t>(thread_count) * inner_group_count, 0);
        thread_local_outer_sums.assign(static_cast<size_t>(thread_count) * outer_group_count, 0);
        thread_local_inner_counts.assign(static_cast<size_t>(thread_count) * inner_group_count, 0);
        thread_local_outer_counts.assign(static_cast<size_t>(thread_count) * outer_group_count, 0);
    }

    std::vector<__int128> inner_sums(inner_group_count, 0);
    std::vector<__int128> outer_sums(outer_group_count, 0);
    std::vector<uint32_t> inner_counts(inner_group_count, 0);
    std::vector<uint32_t> outer_counts(outer_group_count, 0);

    {
        GENDB_PHASE("main_scan");

        if (usd_row_end > usd_row_begin && inner_group_count > 0 && outer_group_count > 0) {
            const uint32_t* usd_rowids = num_uom_postings_rowids.data + usd_row_begin;
            const uint64_t usd_count = usd_row_end - usd_row_begin;
            const size_t lookup_size = sub_adsh_dense_lookup.count;

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                __int128* local_inner_sums = thread_local_inner_sums.data() + static_cast<size_t>(tid) * inner_group_count;
                __int128* local_outer_sums = thread_local_outer_sums.data() + static_cast<size_t>(tid) * outer_group_count;
                uint32_t* local_inner_counts = thread_local_inner_counts.data() + static_cast<size_t>(tid) * inner_group_count;
                uint32_t* local_outer_counts = thread_local_outer_counts.data() + static_cast<size_t>(tid) * outer_group_count;

                #pragma omp for schedule(static)
                for (uint64_t pos = 0; pos < usd_count; ++pos) {
                    const uint32_t num_rowid = usd_rowids[pos];
                    const double raw_value = num_value[num_rowid];
                    if (std::isnan(raw_value)) {
                        continue;
                    }

                    const uint32_t adsh_code = num_adsh[num_rowid];
                    if (static_cast<size_t>(adsh_code) >= lookup_size) {
                        continue;
                    }

                    const uint32_t sub_rowid = sub_adsh_dense_lookup[adsh_code];
                    if (sub_rowid == INVALID_ROWID) {
                        continue;
                    }

                    const int32_t outer_gid = sub_rowid_to_outer_gid[sub_rowid];
                    if (outer_gid == INVALID_GID) {
                        continue;
                    }

                    const int32_t inner_gid = sub_rowid_to_inner_gid[sub_rowid];
                    const __int128 value_scaled = double_to_scaled_decimal(raw_value);
                    local_outer_sums[outer_gid] += value_scaled;
                    local_inner_sums[inner_gid] += value_scaled;
                    local_outer_counts[outer_gid] += 1;
                    local_inner_counts[inner_gid] += 1;
                }
            }

            for (int tid = 0; tid < thread_count; ++tid) {
                const size_t inner_base = static_cast<size_t>(tid) * inner_group_count;
                for (size_t gid = 0; gid < inner_group_count; ++gid) {
                    inner_sums[gid] += thread_local_inner_sums[inner_base + gid];
                    inner_counts[gid] += thread_local_inner_counts[inner_base + gid];
                }

                const size_t outer_base = static_cast<size_t>(tid) * outer_group_count;
                for (size_t gid = 0; gid < outer_group_count; ++gid) {
                    outer_sums[gid] += thread_local_outer_sums[outer_base + gid];
                    outer_counts[gid] += thread_local_outer_counts[outer_base + gid];
                }
            }
        }
    }

    {
        GENDB_PHASE("output");

        std::vector<ResultRow> winners;

        uint64_t participating_inner_groups = 0;
        __int128 sum_of_inner_totals = 0;
        for (size_t gid = 0; gid < inner_group_count; ++gid) {
            if (inner_counts[gid] == 0) {
                continue;
            }
            sum_of_inner_totals += inner_sums[gid];
            ++participating_inner_groups;
        }

        if (participating_inner_groups > 0) {
            winners.reserve(outer_group_count);
            for (size_t gid = 0; gid < outer_group_count; ++gid) {
                if (outer_counts[gid] == 0) {
                    continue;
                }
                const __int128 total_scaled = outer_sums[gid];
                if (total_scaled * static_cast<__int128>(participating_inner_groups) > sum_of_inner_totals) {
                    winners.push_back(ResultRow{static_cast<uint32_t>(gid), total_scaled});
                }
            }

            const size_t limit = std::min<size_t>(100, winners.size());
            if (winners.size() > limit) {
                std::nth_element(
                    winners.begin(), winners.begin() + limit, winners.end(),
                    [](const ResultRow& left, const ResultRow& right) {
                        return left.total_scaled > right.total_scaled;
                    });
                winners.resize(limit);
            }
            std::sort(
                winners.begin(), winners.end(),
                [](const ResultRow& left, const ResultRow& right) {
                    return left.total_scaled > right.total_scaled;
                });
        }

        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            std::perror("fopen");
            return 1;
        }

        std::fprintf(out, "name,cik,total_value\n");
        for (const ResultRow& row : winners) {
            const OuterGroupInfo& group = outer_groups[row.gid];
            const std::string_view name = decode_dict_string(sub_name_dict_offsets, sub_name_dict_data, group.name_code);
            write_csv_escaped(out, name);
            std::fprintf(out, ",%d,", group.cik);
            write_scaled_money_2(out, row.total_scaled);
            std::fputc('\n', out);
        }
        std::fclose(out);
    }

    return 0;
}
