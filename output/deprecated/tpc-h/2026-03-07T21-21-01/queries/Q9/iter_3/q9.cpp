#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr uint64_t kMissingRow = std::numeric_limits<uint64_t>::max();
constexpr uint32_t kMissingSlot = std::numeric_limits<uint32_t>::max();
constexpr uint8_t kMissingNationCode = std::numeric_limits<uint8_t>::max();
constexpr int kPartsuppFanout = 4;
constexpr int kExpectedGroups = 256;
constexpr size_t kMorselRows = 1u << 20;

struct ResultRow {
    std::string nation;
    int year;
    double sum_profit;
};

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

inline void check_count(size_t expected, size_t actual, const char* name) {
    if (expected != actual) {
        fail(std::string("row count mismatch for ") + name);
    }
}

inline bool contains_green(const char* bytes, uint64_t begin, uint64_t end) {
    static constexpr char needle[] = "green";
    static constexpr uint64_t needle_len = sizeof(needle) - 1;
    if (end < begin || end - begin < needle_len) {
        return false;
    }
    const char* s = bytes + begin;
    const uint64_t limit = end - begin - needle_len;
    for (uint64_t i = 0; i <= limit; ++i) {
        if (s[i] == 'g' && std::memcmp(s + i, needle, needle_len) == 0) {
            return true;
        }
    }
    return false;
}

inline std::string decode_dict_entry(const char* bytes,
                                     const uint64_t* offsets,
                                     size_t offset_count,
                                     uint32_t code) {
    const size_t idx = static_cast<size_t>(code);
    if (idx + 1 >= offset_count) {
        fail("dictionary code out of range");
    }
    return std::string(bytes + offsets[idx], bytes + offsets[idx + 1]);
}

inline int64_t lookup_supplycost(const int32_t* suppkeys,
                                 const int64_t* supplycosts,
                                 uint32_t base,
                                 int32_t suppkey) {
    const size_t offset = static_cast<size_t>(base) * kPartsuppFanout;
    if (suppkeys[offset] == suppkey) return supplycosts[offset];
    if (suppkeys[offset + 1] == suppkey) return supplycosts[offset + 1];
    if (suppkeys[offset + 2] == suppkey) return supplycosts[offset + 2];
    if (suppkeys[offset + 3] == suppkey) return supplycosts[offset + 3];
    return -1;
}

inline void add_amount(CompactHashMap<int32_t, int64_t>& map, int32_t key, int64_t amount) {
    int64_t* existing = map.find(key);
    if (existing != nullptr) {
        *existing += amount;
    } else {
        map.insert(key, amount);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        init_date_tables();
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        const int max_threads = std::max(1, omp_get_max_threads());

        MmapColumn<int32_t> p_partkey;
        MmapColumn<uint64_t> p_name_offsets;
        MmapColumn<char> p_name_data;

        MmapColumn<int32_t> ps_partkey;
        MmapColumn<int32_t> ps_suppkey;
        MmapColumn<double> ps_supplycost;

        MmapColumn<uint64_t> supplier_pk_dense;
        MmapColumn<int32_t> s_nationkey;

        MmapColumn<uint64_t> nation_pk_dense;
        MmapColumn<uint32_t> n_name_code;
        MmapColumn<uint64_t> n_name_dict_offsets;
        MmapColumn<char> n_name_dict_data;

        MmapColumn<int32_t> o_orderkey;
        MmapColumn<int32_t> o_orderdate;

        MmapColumn<int32_t> l_partkey;
        MmapColumn<int32_t> l_suppkey;
        MmapColumn<int32_t> l_orderkey;
        MmapColumn<double> l_extendedprice;
        MmapColumn<double> l_discount;
        MmapColumn<double> l_quantity;

        std::vector<uint8_t> green_part_filter;
        std::vector<uint32_t> partkey_to_green_slot;
        std::vector<int32_t> green_suppkeys;
        std::vector<int64_t> green_supplycosts;
        std::vector<uint8_t> suppkey_to_nation_code;
        std::vector<std::string> nation_names;
        std::vector<CompactHashMap<int32_t, int64_t>> thread_aggs;

        size_t part_rows = 0;
        size_t partsupp_rows = 0;
        size_t supplier_rows = 0;
        size_t nation_rows = 0;
        size_t order_rows = 0;
        size_t lineitem_rows = 0;
        uint32_t green_part_count = 0;

        {
            GENDB_PHASE("total");

            {
                GENDB_PHASE("data_loading");

                p_partkey.open(gendb_dir + "/part/p_partkey.bin");
                p_name_offsets.open(gendb_dir + "/part/p_name.offsets.bin");
                p_name_data.open(gendb_dir + "/part/p_name.data.bin");

                ps_partkey.open(gendb_dir + "/partsupp/ps_partkey.bin");
                ps_suppkey.open(gendb_dir + "/partsupp/ps_suppkey.bin");
                ps_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

                supplier_pk_dense.open(gendb_dir + "/supplier/indexes/supplier_pk_dense.bin");
                s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

                nation_pk_dense.open(gendb_dir + "/nation/indexes/nation_pk_dense.bin");
                n_name_code.open(gendb_dir + "/nation/n_name.bin");
                n_name_dict_offsets.open(gendb_dir + "/nation/n_name.dict.offsets.bin");
                n_name_dict_data.open(gendb_dir + "/nation/n_name.dict.data.bin");

                o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
                o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");

                l_partkey.open(gendb_dir + "/lineitem/l_partkey.bin");
                l_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
                l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
                l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
                l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
                l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");

                part_rows = p_partkey.size();
                check_count(part_rows + 1, p_name_offsets.size(), "part/p_name.offsets.bin");

                partsupp_rows = ps_partkey.size();
                check_count(partsupp_rows, ps_suppkey.size(), "partsupp/ps_suppkey.bin");
                check_count(partsupp_rows, ps_supplycost.size(), "partsupp/ps_supplycost.bin");

                supplier_rows = s_nationkey.size();
                nation_rows = n_name_code.size();
                check_count(o_orderkey.size(), o_orderdate.size(), "orders/o_orderdate.bin");
                order_rows = o_orderkey.size();

                lineitem_rows = l_partkey.size();
                check_count(lineitem_rows, l_suppkey.size(), "lineitem/l_suppkey.bin");
                check_count(lineitem_rows, l_orderkey.size(), "lineitem/l_orderkey.bin");
                check_count(lineitem_rows, l_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(lineitem_rows, l_discount.size(), "lineitem/l_discount.bin");
                check_count(lineitem_rows, l_quantity.size(), "lineitem/l_quantity.bin");

                mmap_prefetch_all(p_partkey,
                                  p_name_offsets,
                                  p_name_data,
                                  ps_partkey,
                                  ps_suppkey,
                                  ps_supplycost,
                                  supplier_pk_dense,
                                  s_nationkey,
                                  nation_pk_dense,
                                  n_name_code,
                                  n_name_dict_offsets,
                                  n_name_dict_data,
                                  o_orderkey,
                                  o_orderdate,
                                  l_partkey,
                                  l_suppkey,
                                  l_orderkey,
                                  l_extendedprice,
                                  l_discount,
                                  l_quantity);

                supplier_pk_dense.advise_random();
                nation_pk_dense.advise_random();
            }

            {
                GENDB_PHASE("dim_filter");

                int32_t max_partkey = 0;
                for (size_t row = 0; row < part_rows; ++row) {
                    if (p_partkey[row] > max_partkey) {
                        max_partkey = p_partkey[row];
                    }
                }
                if (max_partkey < 0) {
                    fail("invalid max partkey");
                }

                green_part_filter.assign(static_cast<size_t>(max_partkey) + 1, 0);
                partkey_to_green_slot.assign(static_cast<size_t>(max_partkey) + 1, kMissingSlot);

                for (size_t row = 0; row < part_rows; ++row) {
                    const uint64_t begin = p_name_offsets[row];
                    const uint64_t end = p_name_offsets[row + 1];
                    if (!contains_green(p_name_data.data, begin, end)) {
                        continue;
                    }
                    const int32_t partkey = p_partkey[row];
                    if (partkey < 0) {
                        continue;
                    }
                    green_part_filter[static_cast<size_t>(partkey)] = 1;
                    partkey_to_green_slot[static_cast<size_t>(partkey)] = green_part_count++;
                }

                green_suppkeys.assign(static_cast<size_t>(green_part_count) * kPartsuppFanout, -1);
                green_supplycosts.assign(static_cast<size_t>(green_part_count) * kPartsuppFanout, 0);
            }

            {
                GENDB_PHASE("build_joins");

                suppkey_to_nation_code.assign(supplier_pk_dense.size(), kMissingNationCode);
                nation_names.assign(n_name_dict_offsets.size() > 0 ? n_name_dict_offsets.size() - 1 : 0, "");

                for (size_t nation_row = 0; nation_row < nation_rows; ++nation_row) {
                    const uint32_t code = n_name_code[nation_row];
                    if (code >= nation_names.size()) {
                        fail("nation name code exceeds dictionary size");
                    }
                    nation_names[code] = decode_dict_entry(n_name_dict_data.data,
                                                           n_name_dict_offsets.data,
                                                           n_name_dict_offsets.size(),
                                                           code);
                }

                for (uint32_t suppkey = 0; suppkey < supplier_pk_dense.size(); ++suppkey) {
                    const uint64_t supplier_row = supplier_pk_dense[suppkey];
                    if (supplier_row == kMissingRow || supplier_row >= supplier_rows) {
                        continue;
                    }
                    const int32_t nationkey = s_nationkey[static_cast<size_t>(supplier_row)];
                    if (nationkey < 0 || static_cast<size_t>(nationkey) >= nation_pk_dense.size()) {
                        continue;
                    }
                    const uint64_t nation_row = nation_pk_dense[static_cast<size_t>(nationkey)];
                    if (nation_row == kMissingRow || nation_row >= nation_rows) {
                        continue;
                    }
                    suppkey_to_nation_code[suppkey] = static_cast<uint8_t>(n_name_code[static_cast<size_t>(nation_row)]);
                }

                std::vector<uint8_t> slot_fill(green_part_count, 0);
                for (size_t row = 0; row < partsupp_rows; ++row) {
                    const int32_t partkey = ps_partkey[row];
                    if (partkey < 0 || static_cast<size_t>(partkey) >= partkey_to_green_slot.size()) {
                        continue;
                    }
                    const uint32_t green_slot = partkey_to_green_slot[static_cast<size_t>(partkey)];
                    if (green_slot == kMissingSlot) {
                        continue;
                    }
                    const uint8_t fill = slot_fill[green_slot];
                    if (fill >= kPartsuppFanout) {
                        fail("green partsupp fanout exceeded expected width 4");
                    }
                    const size_t offset = static_cast<size_t>(green_slot) * kPartsuppFanout + fill;
                    green_suppkeys[offset] = ps_suppkey[row];
                    green_supplycosts[offset] = static_cast<int64_t>(std::llround(ps_supplycost[row] * 100.0));
                    slot_fill[green_slot] = static_cast<uint8_t>(fill + 1);
                }

                thread_aggs.reserve(static_cast<size_t>(max_threads));
                for (int tid = 0; tid < max_threads; ++tid) {
                    thread_aggs.emplace_back(kExpectedGroups);
                }
            }

            {
                GENDB_PHASE("main_scan");

                const int32_t* __restrict__ line_part = l_partkey.data;
                const int32_t* __restrict__ line_supp = l_suppkey.data;
                const int32_t* __restrict__ line_order = l_orderkey.data;
                const double* __restrict__ line_extprice = l_extendedprice.data;
                const double* __restrict__ line_disc = l_discount.data;
                const double* __restrict__ line_qty = l_quantity.data;

                const uint32_t* __restrict__ part_to_slot = partkey_to_green_slot.data();
                const int32_t* __restrict__ green_slot_supp = green_suppkeys.data();
                const int64_t* __restrict__ green_slot_cost = green_supplycosts.data();
                const uint8_t* __restrict__ supp_to_nation = suppkey_to_nation_code.data();

                const int32_t* __restrict__ order_keys = o_orderkey.data;
                const int32_t* __restrict__ order_dates = o_orderdate.data;

                const size_t morsel_count = (lineitem_rows + kMorselRows - 1) / kMorselRows;

                #pragma omp parallel
                {
                    CompactHashMap<int32_t, int64_t>& local = thread_aggs[static_cast<size_t>(omp_get_thread_num())];

                    #pragma omp for schedule(dynamic, 1)
                    for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                        const size_t start = morsel * kMorselRows;
                        const size_t end = std::min(start + kMorselRows, lineitem_rows);
                        if (start >= end) {
                            continue;
                        }

                        const int32_t first_orderkey = line_order[start];
                        size_t order_cursor = static_cast<size_t>(
                            std::lower_bound(order_keys, order_keys + order_rows, first_orderkey) - order_keys);

                        for (size_t row = start; row < end; ++row) {
                            const int32_t partkey = line_part[row];
                            if (partkey < 0 || static_cast<size_t>(partkey) >= partkey_to_green_slot.size()) {
                                continue;
                            }
                            const uint32_t green_slot = part_to_slot[static_cast<size_t>(partkey)];
                            if (green_slot == kMissingSlot) {
                                continue;
                            }

                            const int32_t suppkey = line_supp[row];
                            if (suppkey < 0 || static_cast<size_t>(suppkey) >= suppkey_to_nation_code.size()) {
                                continue;
                            }
                            const uint8_t nation_code = supp_to_nation[static_cast<size_t>(suppkey)];
                            if (nation_code == kMissingNationCode) {
                                continue;
                            }

                            const int64_t supplycost_cents =
                                lookup_supplycost(green_slot_supp, green_slot_cost, green_slot, suppkey);
                            if (supplycost_cents < 0) {
                                continue;
                            }

                            const int32_t orderkey = line_order[row];
                            while (order_cursor < order_rows && order_keys[order_cursor] < orderkey) {
                                ++order_cursor;
                            }
                            if (order_cursor >= order_rows || order_keys[order_cursor] != orderkey) {
                                continue;
                            }

                            const int current_year = extract_year(order_dates[order_cursor]);
                            const int64_t extprice_cents =
                                static_cast<int64_t>(std::llround(line_extprice[row] * 100.0));
                            const int64_t discount_pct =
                                static_cast<int64_t>(std::llround(line_disc[row] * 100.0));
                            const int64_t quantity_hundredths =
                                static_cast<int64_t>(std::llround(line_qty[row] * 100.0));
                            const int32_t agg_key = (static_cast<int32_t>(nation_code) << 16) | current_year;
                            add_amount(local,
                                       agg_key,
                                       extprice_cents * (100 - discount_pct) -
                                           supplycost_cents * quantity_hundredths);
                        }
                    }
                }
            }

            {
                GENDB_PHASE("output");

                CompactHashMap<int32_t, int64_t> totals(kExpectedGroups);
                for (CompactHashMap<int32_t, int64_t>& local : thread_aggs) {
                    for (const auto& kv : local) {
                        add_amount(totals, kv.first, kv.second);
                    }
                }

                std::vector<ResultRow> rows;
                rows.reserve(totals.size());
                for (const auto& kv : totals) {
                    const int32_t nation_code = kv.first >> 16;
                    const int32_t year = kv.first & 0xFFFF;
                    if (nation_code < 0 || static_cast<size_t>(nation_code) >= nation_names.size()) {
                        fail("aggregated nation code out of range");
                    }
                    rows.push_back(ResultRow{
                        nation_names[static_cast<size_t>(nation_code)],
                        year,
                        static_cast<double>(kv.second) / 10000.0,
                    });
                }

                std::sort(rows.begin(), rows.end(), [](const ResultRow& lhs, const ResultRow& rhs) {
                    if (lhs.nation != rhs.nation) {
                        return lhs.nation < rhs.nation;
                    }
                    return lhs.year > rhs.year;
                });

                std::filesystem::create_directories(results_dir);
                const std::string output_path = results_dir + "/Q9.csv";
                FILE* out = std::fopen(output_path.c_str(), "w");
                if (!out) {
                    fail("failed to open output file");
                }

                std::fprintf(out, "nation,o_year,sum_profit\n");
                for (const ResultRow& row : rows) {
                    std::fprintf(out, "%s,%d,%.2f\n", row.nation.c_str(), row.year, row.sum_profit);
                }
                std::fclose(out);
            }
        }
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Q9 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
