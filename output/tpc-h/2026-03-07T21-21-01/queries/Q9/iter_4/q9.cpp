#include <algorithm>
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
constexpr uint32_t kMissingOffset = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kMissingNationCode = std::numeric_limits<uint32_t>::max();
constexpr int kExpectedGroups = 256;

struct ResultRow {
    std::string nation;
    int32_t year;
    double sum_profit;
};

struct OrderAlignedPartition {
    size_t line_start;
    size_t line_end;
    size_t order_start;
    size_t order_end;
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

        MmapColumn<uint64_t> orders_pk_dense;
        MmapColumn<int32_t> o_orderkey;
        MmapColumn<int32_t> o_orderdate;

        MmapColumn<int32_t> l_partkey;
        MmapColumn<int32_t> l_suppkey;
        MmapColumn<int32_t> l_orderkey;
        MmapColumn<double> l_extendedprice;
        MmapColumn<double> l_discount;
        MmapColumn<double> l_quantity;

        std::vector<uint8_t> green_part_filter;
        std::vector<uint32_t> partkey_to_payload_offset;
        std::vector<uint32_t> partkey_to_payload_count;
        std::vector<uint32_t> payload_write_cursor;
        std::vector<int32_t> green_payload_suppkey;
        std::vector<int64_t> green_payload_supplycost_cents;
        std::vector<uint32_t> suppkey_to_nation_code;
        std::vector<std::string> nation_names;
        std::vector<OrderAlignedPartition> partitions;
        std::vector<CompactHashMap<int32_t, int64_t>> thread_aggs;

        size_t part_rows = 0;
        size_t partsupp_rows = 0;
        size_t supplier_rows = 0;
        size_t nation_rows = 0;
        size_t order_rows = 0;
        size_t lineitem_rows = 0;
        int32_t max_partkey = 0;

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

                orders_pk_dense.open(gendb_dir + "/orders/indexes/orders_pk_dense.bin");
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

                order_rows = o_orderkey.size();
                check_count(order_rows, o_orderdate.size(), "orders/o_orderdate.bin");

                lineitem_rows = l_partkey.size();
                check_count(lineitem_rows, l_suppkey.size(), "lineitem/l_suppkey.bin");
                check_count(lineitem_rows, l_orderkey.size(), "lineitem/l_orderkey.bin");
                check_count(lineitem_rows, l_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(lineitem_rows, l_discount.size(), "lineitem/l_discount.bin");
                check_count(lineitem_rows, l_quantity.size(), "lineitem/l_quantity.bin");

                if (part_rows == 0) {
                    fail("part table is empty");
                }
                max_partkey = p_partkey[part_rows - 1];
                if (max_partkey < 0) {
                    fail("invalid max partkey");
                }

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
                orders_pk_dense.advise_random();
            }

            {
                GENDB_PHASE("dim_filter");

                green_part_filter.assign(static_cast<size_t>(max_partkey) + 1, 0);
                partkey_to_payload_offset.assign(static_cast<size_t>(max_partkey) + 1, kMissingOffset);
                partkey_to_payload_count.assign(static_cast<size_t>(max_partkey) + 1, 0);

                uint64_t green_part_count = 0;
                #pragma omp parallel for schedule(static) reduction(+:green_part_count)
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
                    green_part_count += 1;
                }
            }

            {
                GENDB_PHASE("build_joins");

                nation_names.assign(n_name_dict_offsets.size() > 0 ? n_name_dict_offsets.size() - 1 : 0, "");
                for (size_t nation_row = 0; nation_row < nation_rows; ++nation_row) {
                    const uint32_t code = n_name_code[nation_row];
                    if (static_cast<size_t>(code) >= nation_names.size()) {
                        fail("nation name code exceeds dictionary size");
                    }
                    nation_names[static_cast<size_t>(code)] = decode_dict_entry(n_name_dict_data.data,
                                                                                n_name_dict_offsets.data,
                                                                                n_name_dict_offsets.size(),
                                                                                code);
                }

                suppkey_to_nation_code.assign(supplier_pk_dense.size(), kMissingNationCode);
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
                    suppkey_to_nation_code[suppkey] = n_name_code[static_cast<size_t>(nation_row)];
                }

                for (size_t row = 0; row < partsupp_rows; ++row) {
                    const int32_t partkey = ps_partkey[row];
                    if (partkey < 0 || static_cast<size_t>(partkey) >= green_part_filter.size()) {
                        continue;
                    }
                    if (green_part_filter[static_cast<size_t>(partkey)] == 0) {
                        continue;
                    }
                    partkey_to_payload_count[static_cast<size_t>(partkey)] += 1;
                }

                uint64_t payload_size = 0;
                for (size_t partkey = 0; partkey < partkey_to_payload_count.size(); ++partkey) {
                    const uint32_t count = partkey_to_payload_count[partkey];
                    if (count == 0) {
                        continue;
                    }
                    if (payload_size > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) - count) {
                        fail("green partsupp payload exceeds uint32_t address space");
                    }
                    partkey_to_payload_offset[partkey] = static_cast<uint32_t>(payload_size);
                    payload_size += count;
                }

                green_payload_suppkey.assign(static_cast<size_t>(payload_size), -1);
                green_payload_supplycost_cents.assign(static_cast<size_t>(payload_size), 0);
                payload_write_cursor = partkey_to_payload_offset;

                for (size_t row = 0; row < partsupp_rows; ++row) {
                    const int32_t partkey = ps_partkey[row];
                    if (partkey < 0 || static_cast<size_t>(partkey) >= green_part_filter.size()) {
                        continue;
                    }
                    const size_t partkey_idx = static_cast<size_t>(partkey);
                    if (green_part_filter[partkey_idx] == 0) {
                        continue;
                    }
                    const uint32_t offset = payload_write_cursor[partkey_idx]++;
                    green_payload_suppkey[static_cast<size_t>(offset)] = ps_suppkey[row];
                    green_payload_supplycost_cents[static_cast<size_t>(offset)] =
                        static_cast<int64_t>(std::llround(ps_supplycost[row] * 100.0));
                }

                std::vector<size_t> partition_starts(static_cast<size_t>(max_threads) + 1, 0);
                partition_starts[0] = 0;
                partition_starts[static_cast<size_t>(max_threads)] = lineitem_rows;
                const int32_t* __restrict__ line_order = l_orderkey.data;
                for (int tid = 1; tid < max_threads; ++tid) {
                    size_t start = (lineitem_rows * static_cast<size_t>(tid)) / static_cast<size_t>(max_threads);
                    if (start > 0 && start < lineitem_rows) {
                        const int32_t previous_orderkey = line_order[start - 1];
                        while (start < lineitem_rows && line_order[start] == previous_orderkey) {
                            ++start;
                        }
                    }
                    partition_starts[static_cast<size_t>(tid)] = start;
                }

                partitions.reserve(static_cast<size_t>(max_threads));
                for (int tid = 0; tid < max_threads; ++tid) {
                    const size_t line_start = partition_starts[static_cast<size_t>(tid)];
                    const size_t line_end = partition_starts[static_cast<size_t>(tid + 1)];
                    if (line_start >= line_end) {
                        partitions.push_back(OrderAlignedPartition{line_start, line_end, 0, 0});
                        continue;
                    }

                    const int32_t first_orderkey = line_order[line_start];
                    const int32_t last_orderkey = line_order[line_end - 1];
                    if (first_orderkey < 0 || last_orderkey < 0 ||
                        static_cast<size_t>(first_orderkey) >= orders_pk_dense.size() ||
                        static_cast<size_t>(last_orderkey) >= orders_pk_dense.size()) {
                        fail("lineitem orderkey out of dense index range");
                    }
                    const uint64_t order_start = orders_pk_dense[static_cast<size_t>(first_orderkey)];
                    const uint64_t order_last = orders_pk_dense[static_cast<size_t>(last_orderkey)];
                    if (order_start == kMissingRow || order_last == kMissingRow ||
                        order_start >= order_rows || order_last >= order_rows || order_start > order_last) {
                        fail("orders dense index lookup failed");
                    }
                    partitions.push_back(OrderAlignedPartition{
                        line_start,
                        line_end,
                        static_cast<size_t>(order_start),
                        static_cast<size_t>(order_last + 1),
                    });
                }

                thread_aggs.reserve(partitions.size());
                for (size_t i = 0; i < partitions.size(); ++i) {
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

                const uint8_t* __restrict__ green_filter = green_part_filter.data();
                const uint32_t* __restrict__ payload_offsets = partkey_to_payload_offset.data();
                const uint32_t* __restrict__ payload_counts = partkey_to_payload_count.data();
                const int32_t* __restrict__ payload_suppkeys = green_payload_suppkey.data();
                const int64_t* __restrict__ payload_supplycosts = green_payload_supplycost_cents.data();
                const uint32_t* __restrict__ supplier_nation = suppkey_to_nation_code.data();

                const int32_t* __restrict__ order_keys = o_orderkey.data;
                const int32_t* __restrict__ order_dates = o_orderdate.data;

                #pragma omp parallel for schedule(static, 1)
                for (int tid = 0; tid < static_cast<int>(partitions.size()); ++tid) {
                    const OrderAlignedPartition partition = partitions[static_cast<size_t>(tid)];
                    if (partition.line_start >= partition.line_end) {
                        continue;
                    }

                    CompactHashMap<int32_t, int64_t>& local = thread_aggs[static_cast<size_t>(tid)];
                    size_t order_row = partition.order_start;

                    for (size_t row = partition.line_start; row < partition.line_end; ++row) {
                        const int32_t partkey = line_part[row];
                        if (partkey < 0 || static_cast<size_t>(partkey) >= green_part_filter.size()) {
                            continue;
                        }
                        const size_t partkey_idx = static_cast<size_t>(partkey);
                        if (green_filter[partkey_idx] == 0) {
                            continue;
                        }

                        const uint32_t payload_count = payload_counts[partkey_idx];
                        if (payload_count == 0) {
                            continue;
                        }

                        const int32_t suppkey = line_supp[row];
                        if (suppkey < 0 || static_cast<size_t>(suppkey) >= suppkey_to_nation_code.size()) {
                            continue;
                        }
                        const uint32_t nation_code = supplier_nation[static_cast<size_t>(suppkey)];
                        if (nation_code == kMissingNationCode) {
                            continue;
                        }

                        const uint32_t payload_offset = payload_offsets[partkey_idx];
                        int64_t supplycost_cents = -1;
                        const size_t payload_begin = static_cast<size_t>(payload_offset);
                        const size_t payload_end = payload_begin + static_cast<size_t>(payload_count);
                        for (size_t i = payload_begin; i < payload_end; ++i) {
                            if (payload_suppkeys[i] == suppkey) {
                                supplycost_cents = payload_supplycosts[i];
                                break;
                            }
                        }
                        if (supplycost_cents < 0) {
                            continue;
                        }

                        const int32_t orderkey = line_order[row];
                        while (order_row < partition.order_end && order_keys[order_row] < orderkey) {
                            ++order_row;
                        }
                        if (order_row >= partition.order_end || order_keys[order_row] != orderkey) {
                            fail("sequential orders cursor lost alignment");
                        }

                        const int32_t extprice_cents =
                            static_cast<int32_t>(std::llround(line_extprice[row] * 100.0));
                        const int32_t discount_pct =
                            static_cast<int32_t>(std::llround(line_disc[row] * 100.0));
                        const int32_t quantity_hundredths =
                            static_cast<int32_t>(std::llround(line_qty[row] * 100.0));
                        const int32_t year = extract_year(order_dates[order_row]);
                        const int32_t agg_key = static_cast<int32_t>((nation_code << 16) | static_cast<uint32_t>(year));

                        add_amount(local,
                                   agg_key,
                                   static_cast<int64_t>(extprice_cents) * (100 - discount_pct) -
                                       supplycost_cents * quantity_hundredths);
                    }
                }
            }

            {
                GENDB_PHASE("output");

                CompactHashMap<int32_t, int64_t> totals(kExpectedGroups);
                for (const CompactHashMap<int32_t, int64_t>& local : thread_aggs) {
                    for (const auto& kv : local) {
                        add_amount(totals, kv.first, kv.second);
                    }
                }

                std::vector<ResultRow> rows;
                rows.reserve(totals.size());
                for (const auto& kv : totals) {
                    const uint32_t nation_code = static_cast<uint32_t>(kv.first >> 16);
                    const int32_t year = kv.first & 0xFFFF;
                    if (static_cast<size_t>(nation_code) >= nation_names.size()) {
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
