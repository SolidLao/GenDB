#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr uint8_t kMissingByte = std::numeric_limits<uint8_t>::max();
constexpr int kNationCount = 25;
constexpr int kYearBase = 1992;
constexpr int kYearCount = 7;
constexpr int kGroupsPerThread = kNationCount * kYearCount;
constexpr int kPartsuppFanout = 4;
constexpr int kAggStride = 192;

struct ResultRow {
    std::string nation;
    int year;
    int64_t sum_profit_1e4;
};

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

inline void check_count(size_t expected, size_t actual, const char* name) {
    if (expected != actual) {
        fail(std::string("row count mismatch for ") + name);
    }
}

inline std::string decode_dict_entry(const char* bytes,
                                     const uint64_t* offsets,
                                     size_t offset_count,
                                     uint8_t code) {
    const size_t idx = static_cast<size_t>(code);
    if (idx + 1 >= offset_count) {
        fail("dictionary code out of range");
    }
    const uint64_t begin = offsets[idx];
    const uint64_t end = offsets[idx + 1];
    return std::string(bytes + begin, bytes + end);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        const int max_threads = std::max(1, omp_get_max_threads());
        omp_set_num_threads(max_threads);

        MmapColumn<int32_t> lineitem_partkey;
        MmapColumn<int32_t> lineitem_suppkey;
        MmapColumn<int32_t> lineitem_orderkey;
        MmapColumn<int64_t> lineitem_quantity;
        MmapColumn<int64_t> lineitem_extendedprice;
        MmapColumn<int64_t> lineitem_discount;

        MmapColumn<uint8_t> part_name_green_bitset;
        MmapColumn<uint8_t> supplier_nation_code;
        MmapColumn<uint8_t> order_year_idx;
        MmapColumn<int32_t> partsupp_dense_suppkey;
        MmapColumn<int32_t> partsupp_dense_supplycost;

        MmapColumn<uint64_t> nation_name_dict_offsets;
        MmapColumn<char> nation_name_dict_data;

        size_t lineitem_rows = 0;
        std::array<std::string, kNationCount> nation_names;
        std::vector<int64_t> thread_agg(static_cast<size_t>(max_threads) * kAggStride, 0);

        {
            GENDB_PHASE("total");

            {
                GENDB_PHASE("data_loading");

                lineitem_partkey.open(gendb_dir + "/lineitem/l_partkey.bin");
                lineitem_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
                lineitem_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
                lineitem_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
                lineitem_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
                lineitem_discount.open(gendb_dir + "/lineitem/l_discount.bin");

                part_name_green_bitset.open(gendb_dir + "/part/indexes/part_name_green_bitset.bin");
                supplier_nation_code.open(
                    gendb_dir + "/column_versions/supplier.s_suppkey.nation_code_u8/nation_code.bin");
                order_year_idx.open(
                    gendb_dir + "/column_versions/orders.o_orderkey.year_idx_u8/year_idx.bin");
                partsupp_dense_suppkey.open(
                    gendb_dir + "/column_versions/partsupp.ps_partkey.dense4_i32/suppkey.bin");
                partsupp_dense_supplycost.open(
                    gendb_dir + "/column_versions/partsupp.ps_partkey.dense4_i32/supplycost.bin");

                nation_name_dict_offsets.open(gendb_dir + "/nation/n_name.dict.offsets.bin");
                nation_name_dict_data.open(gendb_dir + "/nation/n_name.dict.data.bin");

                lineitem_rows = lineitem_partkey.size();
                check_count(lineitem_rows, lineitem_suppkey.size(), "lineitem/l_suppkey.bin");
                check_count(lineitem_rows, lineitem_orderkey.size(), "lineitem/l_orderkey.bin");
                check_count(lineitem_rows, lineitem_quantity.size(), "lineitem/l_quantity.bin");
                check_count(lineitem_rows, lineitem_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(lineitem_rows, lineitem_discount.size(), "lineitem/l_discount.bin");
                check_count(partsupp_dense_suppkey.size(),
                            partsupp_dense_supplycost.size(),
                            "column_versions/partsupp.ps_partkey.dense4_i32/supplycost.bin");
                if (nation_name_dict_offsets.size() != static_cast<size_t>(kNationCount + 1)) {
                    fail("unexpected nation dictionary size");
                }
                if ((partsupp_dense_suppkey.size() % kPartsuppFanout) != 0) {
                    fail("invalid dense4 partsupp suppkey payload length");
                }
            }

            {
                GENDB_PHASE("dim_filter");

                for (int nation_code = 0; nation_code < kNationCount; ++nation_code) {
                    nation_names[static_cast<size_t>(nation_code)] =
                        decode_dict_entry(nation_name_dict_data.data,
                                          nation_name_dict_offsets.data,
                                          nation_name_dict_offsets.size(),
                                          static_cast<uint8_t>(nation_code));
                }
            }

            {
                GENDB_PHASE("build_joins");

                part_name_green_bitset.advise_random();
                supplier_nation_code.advise_random();
                order_year_idx.advise_random();
                partsupp_dense_suppkey.advise_random();
                partsupp_dense_supplycost.advise_random();
                nation_name_dict_offsets.advise_random();
                nation_name_dict_data.advise_random();

                mmap_prefetch_all(lineitem_partkey,
                                  lineitem_suppkey,
                                  lineitem_orderkey,
                                  lineitem_quantity,
                                  lineitem_extendedprice,
                                  lineitem_discount,
                                  part_name_green_bitset,
                                  supplier_nation_code,
                                  order_year_idx,
                                  partsupp_dense_suppkey,
                                  partsupp_dense_supplycost,
                                  nation_name_dict_offsets,
                                  nation_name_dict_data);
            }

            {
                GENDB_PHASE("main_scan");

                const int32_t* __restrict__ l_partkey = lineitem_partkey.data;
                const int32_t* __restrict__ l_suppkey = lineitem_suppkey.data;
                const int32_t* __restrict__ l_orderkey = lineitem_orderkey.data;
                const int64_t* __restrict__ l_quantity = lineitem_quantity.data;
                const int64_t* __restrict__ l_extendedprice = lineitem_extendedprice.data;
                const int64_t* __restrict__ l_discount = lineitem_discount.data;

                const uint8_t* __restrict__ green_bitset = part_name_green_bitset.data;
                const uint8_t* __restrict__ supp_to_nation = supplier_nation_code.data;
                const uint8_t* __restrict__ order_to_year = order_year_idx.data;
                const int32_t* __restrict__ dense_ps_suppkey = partsupp_dense_suppkey.data;
                const int32_t* __restrict__ dense_ps_supplycost = partsupp_dense_supplycost.data;

                const size_t green_size = part_name_green_bitset.size();
                const size_t supp_to_nation_size = supplier_nation_code.size();
                const size_t order_to_year_size = order_year_idx.size();
                const size_t dense_slot_count = partsupp_dense_suppkey.size();

                #pragma omp parallel
                {
                    const int tid = omp_get_thread_num();
                    const int thread_count = omp_get_num_threads();
                    int64_t* __restrict__ local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;

                    const size_t start =
                        (lineitem_rows * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                    const size_t end =
                        (lineitem_rows * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);

                    for (size_t row = start; row < end; ++row) {
                        const int32_t partkey = l_partkey[row];
                        if (partkey < 0) {
                            continue;
                        }
                        const size_t part_idx = static_cast<size_t>(partkey);
                        if (part_idx >= green_size || green_bitset[part_idx] == 0) {
                            continue;
                        }

                        const int32_t suppkey = l_suppkey[row];
                        if (suppkey < 0) {
                            continue;
                        }
                        const size_t supp_idx = static_cast<size_t>(suppkey);
                        if (supp_idx >= supp_to_nation_size) {
                            continue;
                        }
                        const uint8_t nation_code = supp_to_nation[supp_idx];
                        if (nation_code == kMissingByte || nation_code >= static_cast<uint8_t>(kNationCount)) {
                            continue;
                        }

                        const size_t base = part_idx * kPartsuppFanout;
                        if (base + (kPartsuppFanout - 1) >= dense_slot_count) {
                            continue;
                        }

                        int32_t matched_supplycost = 0;
                        if (dense_ps_suppkey[base] == suppkey) {
                            matched_supplycost = dense_ps_supplycost[base];
                        } else if (dense_ps_suppkey[base + 1] == suppkey) {
                            matched_supplycost = dense_ps_supplycost[base + 1];
                        } else if (dense_ps_suppkey[base + 2] == suppkey) {
                            matched_supplycost = dense_ps_supplycost[base + 2];
                        } else if (dense_ps_suppkey[base + 3] == suppkey) {
                            matched_supplycost = dense_ps_supplycost[base + 3];
                        } else {
                            continue;
                        }

                        const int32_t orderkey = l_orderkey[row];
                        if (orderkey < 0) {
                            continue;
                        }
                        const size_t order_idx = static_cast<size_t>(orderkey);
                        if (order_idx >= order_to_year_size) {
                            continue;
                        }
                        const uint8_t year_idx = order_to_year[order_idx];
                        if (year_idx == kMissingByte || year_idx >= static_cast<uint8_t>(kYearCount)) {
                            continue;
                        }

                        const __int128 revenue_1e4 =
                            static_cast<__int128>(l_extendedprice[row]) *
                            static_cast<__int128>(100 - l_discount[row]);
                        const __int128 supply_1e4 =
                            static_cast<__int128>(matched_supplycost) * static_cast<__int128>(l_quantity[row]);
                        const int64_t amount_1e4 = static_cast<int64_t>(revenue_1e4 - supply_1e4);
                        local[static_cast<int>(nation_code) * kYearCount + static_cast<int>(year_idx)] +=
                            amount_1e4;
                    }
                }
            }

            {
                GENDB_PHASE("output");

                std::array<int64_t, kGroupsPerThread> totals{};
                for (int tid = 0; tid < max_threads; ++tid) {
                    const int64_t* local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;
                    for (int idx = 0; idx < kGroupsPerThread; ++idx) {
                        totals[static_cast<size_t>(idx)] += local[idx];
                    }
                }

                std::vector<ResultRow> rows;
                rows.reserve(kGroupsPerThread);
                for (int nation_code = 0; nation_code < kNationCount; ++nation_code) {
                    for (int year_idx = 0; year_idx < kYearCount; ++year_idx) {
                        const int64_t sum_profit_1e4 =
                            totals[static_cast<size_t>(nation_code * kYearCount + year_idx)];
                        if (sum_profit_1e4 == 0) {
                            continue;
                        }
                        rows.push_back(ResultRow{
                            nation_names[static_cast<size_t>(nation_code)],
                            kYearBase + year_idx,
                            sum_profit_1e4,
                        });
                    }
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
                    std::fprintf(out,
                                 "%s,%d,%.2f\n",
                                 row.nation.c_str(),
                                 row.year,
                                 static_cast<double>(row.sum_profit_1e4) / 10000.0);
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
