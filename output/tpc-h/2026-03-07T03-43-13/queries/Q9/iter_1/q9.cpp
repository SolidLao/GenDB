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

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr uint32_t kMissingRow = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kMissingOffset = std::numeric_limits<uint32_t>::max();
constexpr uint8_t kMissingNationCode = std::numeric_limits<uint8_t>::max();
constexpr int kNationCount = 25;
constexpr int kYearBase = 1992;
constexpr int kYearCount = 7;
constexpr int kGroupsPerThread = kNationCount * kYearCount;
constexpr int kAggStride = 256;
constexpr int kPartsuppFanout = 4;

struct ResultRow {
    std::string nation;
    int year;
    int64_t sum_profit_scaled;
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
        fail("nation dictionary code out of range");
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
        init_date_tables();
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

        MmapColumn<uint32_t> supplier_pk_dense;
        MmapColumn<int32_t> supplier_nationkey;

        MmapColumn<uint32_t> nation_pk_dense;
        MmapColumn<uint8_t> nation_name_codes;
        MmapColumn<uint64_t> nation_name_dict_offsets;
        MmapColumn<char> nation_name_dict_data;

        MmapColumn<int32_t> partsupp_partkey;
        MmapColumn<int32_t> partsupp_suppkey;
        MmapColumn<int64_t> partsupp_supplycost;

        MmapColumn<uint32_t> orders_pk_dense;
        MmapColumn<int32_t> orders_orderkey;
        MmapColumn<int32_t> orders_orderdate;

        size_t lineitem_rows = 0;
        size_t partsupp_rows = 0;

        std::vector<uint32_t> green_part_offset;
        std::vector<int32_t> green_ps_suppkey;
        std::vector<int64_t> green_ps_supplycost;
        std::vector<uint8_t> suppkey_nation_code;
        std::vector<std::string> nation_names(256);
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

                supplier_pk_dense.open(gendb_dir + "/supplier/indexes/supplier_pk_dense.bin");
                supplier_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

                nation_pk_dense.open(gendb_dir + "/nation/indexes/nation_pk_dense.bin");
                nation_name_codes.open(gendb_dir + "/nation/n_name.codes.bin");
                nation_name_dict_offsets.open(gendb_dir + "/nation/n_name.dict.offsets.bin");
                nation_name_dict_data.open(gendb_dir + "/nation/n_name.dict.data.bin");

                partsupp_partkey.open(gendb_dir + "/partsupp/ps_partkey.bin");
                partsupp_suppkey.open(gendb_dir + "/partsupp/ps_suppkey.bin");
                partsupp_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

                orders_pk_dense.open(gendb_dir + "/orders/indexes/orders_pk_dense.bin");
                orders_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
                orders_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");

                lineitem_rows = lineitem_partkey.size();
                check_count(lineitem_rows, lineitem_suppkey.size(), "lineitem/l_suppkey.bin");
                check_count(lineitem_rows, lineitem_orderkey.size(), "lineitem/l_orderkey.bin");
                check_count(lineitem_rows, lineitem_quantity.size(), "lineitem/l_quantity.bin");
                check_count(lineitem_rows, lineitem_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(lineitem_rows, lineitem_discount.size(), "lineitem/l_discount.bin");

                partsupp_rows = partsupp_partkey.size();
                check_count(partsupp_rows, partsupp_suppkey.size(), "partsupp/ps_suppkey.bin");
                check_count(partsupp_rows, partsupp_supplycost.size(), "partsupp/ps_supplycost.bin");

                check_count(supplier_nationkey.size(), 100000, "supplier/s_nationkey.bin");
                check_count(orders_orderkey.size(), orders_orderdate.size(), "orders/o_orderdate.bin");
                check_count(nation_name_codes.size(), 25, "nation/n_name.codes.bin");

                mmap_prefetch_all(lineitem_partkey,
                                  lineitem_suppkey,
                                  lineitem_orderkey,
                                  lineitem_quantity,
                                  lineitem_extendedprice,
                                  lineitem_discount,
                                  part_name_green_bitset,
                                  supplier_pk_dense,
                                  supplier_nationkey,
                                  nation_pk_dense,
                                  nation_name_codes,
                                  nation_name_dict_offsets,
                                  nation_name_dict_data,
                                  partsupp_partkey,
                                  partsupp_suppkey,
                                  partsupp_supplycost,
                                  orders_pk_dense,
                                  orders_orderkey,
                                  orders_orderdate);

                orders_pk_dense.advise_random();
                part_name_green_bitset.advise_random();
            }

            {
                GENDB_PHASE("dim_filter");

                green_part_offset.assign(part_name_green_bitset.count, kMissingOffset);
                uint32_t next_offset = 0;
                for (uint32_t partkey = 0; partkey < part_name_green_bitset.count; ++partkey) {
                    if (part_name_green_bitset[partkey] != 0) {
                        green_part_offset[partkey] = next_offset;
                        next_offset += kPartsuppFanout;
                    }
                }

                green_ps_suppkey.assign(static_cast<size_t>(next_offset), -1);
                green_ps_supplycost.assign(static_cast<size_t>(next_offset), 0);
            }

            {
                GENDB_PHASE("build_joins");

                const uint32_t* __restrict__ supplier_dense = supplier_pk_dense.data;
                const int32_t* __restrict__ supplier_nk = supplier_nationkey.data;
                const uint32_t* __restrict__ nation_dense = nation_pk_dense.data;
                const uint8_t* __restrict__ nation_codes = nation_name_codes.data;
                const int32_t* __restrict__ ps_partkey = partsupp_partkey.data;
                const int32_t* __restrict__ ps_suppkey = partsupp_suppkey.data;
                const int64_t* __restrict__ ps_supplycost = partsupp_supplycost.data;

                suppkey_nation_code.assign(supplier_pk_dense.count, kMissingNationCode);
                for (uint32_t suppkey = 0; suppkey < supplier_pk_dense.count; ++suppkey) {
                    const uint32_t supplier_row = supplier_dense[suppkey];
                    if (supplier_row == kMissingRow || supplier_row >= supplier_nationkey.count) {
                        continue;
                    }
                    const int32_t nationkey = supplier_nk[supplier_row];
                    if (nationkey < 0 || static_cast<uint32_t>(nationkey) >= nation_pk_dense.count) {
                        continue;
                    }
                    const uint32_t nation_row = nation_dense[static_cast<uint32_t>(nationkey)];
                    if (nation_row == kMissingRow || nation_row >= nation_name_codes.count) {
                        continue;
                    }
                    suppkey_nation_code[suppkey] = nation_codes[nation_row];
                }

                for (size_t row = 0; row < nation_name_codes.size(); ++row) {
                    const uint8_t code = nation_codes[row];
                    nation_names[code] = decode_dict_entry(nation_name_dict_data.data,
                                                           nation_name_dict_offsets.data,
                                                           nation_name_dict_offsets.count,
                                                           code);
                }

                int32_t current_partkey = std::numeric_limits<int32_t>::min();
                uint32_t current_base = kMissingOffset;
                int current_count = 0;
                for (size_t row = 0; row < partsupp_rows; ++row) {
                    const int32_t partkey = ps_partkey[row];
                    if (partkey != current_partkey) {
                        current_partkey = partkey;
                        current_count = 0;
                        if (partkey >= 0 && static_cast<uint32_t>(partkey) < green_part_offset.size()) {
                            current_base = green_part_offset[static_cast<uint32_t>(partkey)];
                        } else {
                            current_base = kMissingOffset;
                        }
                    }
                    if (current_base == kMissingOffset) {
                        continue;
                    }
                    if (current_count >= kPartsuppFanout) {
                        fail("green partsupp fanout exceeded fixed payload width");
                    }
                    const size_t slot = static_cast<size_t>(current_base) + static_cast<size_t>(current_count);
                    green_ps_suppkey[slot] = ps_suppkey[row];
                    green_ps_supplycost[slot] = ps_supplycost[row];
                    ++current_count;
                }
            }

            {
                GENDB_PHASE("main_scan");

                const int32_t* __restrict__ l_partkey = lineitem_partkey.data;
                const int32_t* __restrict__ l_suppkey = lineitem_suppkey.data;
                const int32_t* __restrict__ l_orderkey = lineitem_orderkey.data;
                const int64_t* __restrict__ l_quantity = lineitem_quantity.data;
                const int64_t* __restrict__ l_extendedprice = lineitem_extendedprice.data;
                const int64_t* __restrict__ l_discount = lineitem_discount.data;

                const uint32_t* __restrict__ part_offset = green_part_offset.data();
                const int32_t* __restrict__ green_suppkey = green_ps_suppkey.data();
                const int64_t* __restrict__ green_supplycost = green_ps_supplycost.data();
                const uint8_t* __restrict__ supp_to_nation = suppkey_nation_code.data();

                const uint32_t* __restrict__ orders_dense = orders_pk_dense.data;
                const int32_t* __restrict__ o_orderkey = orders_orderkey.data;
                const int32_t* __restrict__ o_orderdate = orders_orderdate.data;

                #pragma omp parallel
                {
                    const int tid = omp_get_thread_num();
                    const int thread_count = omp_get_num_threads();
                    int64_t* __restrict__ local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;

                    const size_t start = (lineitem_rows * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                    const size_t end = (lineitem_rows * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
                    if (start < end) {
                        int32_t current_orderkey = l_orderkey[start];
                        uint32_t current_order_row = kMissingRow;
                        int current_year_idx = -1;
                        if (current_orderkey >= 0 && static_cast<uint32_t>(current_orderkey) < orders_pk_dense.count) {
                            current_order_row = orders_dense[static_cast<uint32_t>(current_orderkey)];
                        }
                        if (current_order_row != kMissingRow && current_order_row < orders_orderkey.count) {
                            if (o_orderkey[current_order_row] == current_orderkey) {
                                current_year_idx = extract_year(o_orderdate[current_order_row]) - kYearBase;
                            } else {
                                current_order_row = kMissingRow;
                            }
                        }

                        for (size_t row = start; row < end; ++row) {
                        const int32_t partkey = l_partkey[row];
                        if (partkey < 0 || static_cast<uint32_t>(partkey) >= green_part_offset.size()) {
                            continue;
                        }
                        const uint32_t base = part_offset[static_cast<uint32_t>(partkey)];
                        if (base == kMissingOffset) {
                            continue;
                        }

                        const int32_t suppkey = l_suppkey[row];
                        if (suppkey < 0 || static_cast<uint32_t>(suppkey) >= suppkey_nation_code.size()) {
                            continue;
                        }
                        const uint8_t nation_code = supp_to_nation[static_cast<uint32_t>(suppkey)];
                        if (nation_code == kMissingNationCode || nation_code >= kNationCount) {
                            continue;
                        }

                        int64_t matched_supplycost = 0;
                        const size_t base_idx = static_cast<size_t>(base);
                        if (green_suppkey[base_idx] == suppkey) {
                            matched_supplycost = green_supplycost[base_idx];
                        } else if (green_suppkey[base_idx + 1] == suppkey) {
                            matched_supplycost = green_supplycost[base_idx + 1];
                        } else if (green_suppkey[base_idx + 2] == suppkey) {
                            matched_supplycost = green_supplycost[base_idx + 2];
                        } else if (green_suppkey[base_idx + 3] == suppkey) {
                            matched_supplycost = green_supplycost[base_idx + 3];
                        } else {
                            continue;
                        }

                        const int32_t orderkey = l_orderkey[row];
                        if (orderkey != current_orderkey) {
                            if (orderkey > current_orderkey && current_order_row != kMissingRow) {
                                do {
                                    ++current_order_row;
                                } while (current_order_row < orders_orderkey.count &&
                                         o_orderkey[current_order_row] < orderkey);
                                if (current_order_row >= orders_orderkey.count ||
                                    o_orderkey[current_order_row] != orderkey) {
                                    current_order_row = kMissingRow;
                                }
                            } else {
                                current_order_row = kMissingRow;
                            }

                            if (current_order_row == kMissingRow) {
                                if (orderkey < 0 || static_cast<uint32_t>(orderkey) >= orders_pk_dense.count) {
                                    current_orderkey = orderkey;
                                    current_year_idx = -1;
                                    continue;
                                }
                                current_order_row = orders_dense[static_cast<uint32_t>(orderkey)];
                                if (current_order_row == kMissingRow || current_order_row >= orders_orderkey.count ||
                                    o_orderkey[current_order_row] != orderkey) {
                                    current_orderkey = orderkey;
                                    current_year_idx = -1;
                                    current_order_row = kMissingRow;
                                    continue;
                                }
                            }

                            current_orderkey = orderkey;
                            current_year_idx = extract_year(o_orderdate[current_order_row]) - kYearBase;
                        }

                        if (static_cast<unsigned>(current_year_idx) >= static_cast<unsigned>(kYearCount)) {
                            continue;
                        }

                            const __int128 revenue_scaled =
                                static_cast<__int128>(l_extendedprice[row]) * static_cast<__int128>(100 - l_discount[row]);
                            const __int128 supply_scaled =
                                static_cast<__int128>(matched_supplycost) * static_cast<__int128>(l_quantity[row]);
                            const int64_t amount_scaled = static_cast<int64_t>(revenue_scaled - supply_scaled);
                            local[static_cast<int>(nation_code) * kYearCount + current_year_idx] += amount_scaled;
                        }
                    }
                }
            }

            {
                GENDB_PHASE("output");

                std::array<int64_t, kGroupsPerThread> totals{};
                for (int tid = 0; tid < max_threads; ++tid) {
                    const int64_t* local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;
                    for (int idx = 0; idx < kGroupsPerThread; ++idx) {
                        totals[idx] += local[idx];
                    }
                }

                std::vector<ResultRow> rows;
                rows.reserve(kGroupsPerThread);
                for (int nation_code = 0; nation_code < kNationCount; ++nation_code) {
                    for (int year_idx = 0; year_idx < kYearCount; ++year_idx) {
                        const int64_t sum_scaled = totals[nation_code * kYearCount + year_idx];
                        if (sum_scaled == 0) {
                            continue;
                        }
                        rows.push_back(ResultRow{
                            nation_names[static_cast<size_t>(nation_code)],
                            kYearBase + year_idx,
                            sum_scaled,
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
                                 "%s,%d,%.4f\n",
                                 row.nation.c_str(),
                                 row.year,
                                 static_cast<double>(row.sum_profit_scaled) / 10000.0);
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
