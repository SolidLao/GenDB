#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
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

constexpr uint64_t kMissingRow = std::numeric_limits<uint64_t>::max();
constexpr uint32_t kMissingOffset = std::numeric_limits<uint32_t>::max();
constexpr uint8_t kMissingNationRow = std::numeric_limits<uint8_t>::max();
constexpr int32_t kBaseYear = 1992;
constexpr int32_t kYearBucketCount = 7;

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

inline int compare_token_at(const uint64_t* offsets,
                            const char* data,
                            size_t token_idx,
                            const char* needle,
                            size_t needle_len) {
    const uint64_t begin = offsets[token_idx];
    const uint64_t end = offsets[token_idx + 1];
    const size_t token_len = static_cast<size_t>(end - begin);
    const size_t cmp_len = std::min(token_len, needle_len);
    const int cmp = std::memcmp(data + begin, needle, cmp_len);
    if (cmp != 0) {
        return cmp;
    }
    if (token_len < needle_len) {
        return -1;
    }
    if (token_len > needle_len) {
        return 1;
    }
    return 0;
}

inline bool find_token_index(const uint64_t* offsets,
                             size_t offset_count,
                             const char* data,
                             const char* needle,
                             size_t needle_len,
                             size_t& token_idx_out) {
    if (offset_count < 2) {
        return false;
    }
    size_t lo = 0;
    size_t hi = offset_count - 1;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        const int cmp = compare_token_at(offsets, data, mid, needle, needle_len);
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo + 1 >= offset_count) {
        return false;
    }
    if (compare_token_at(offsets, data, lo, needle, needle_len) != 0) {
        return false;
    }
    token_idx_out = lo;
    return true;
}

inline bool bitmap_contains(const uint64_t* bitmap, size_t key) {
    return (bitmap[key >> 6] >> (key & 63)) & 1ULL;
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

        MmapColumn<uint64_t> token_offsets;
        MmapColumn<char> token_data;
        MmapColumn<uint64_t> token_bitmaps;

        MmapColumn<uint64_t> partsupp_posting_offsets;
        MmapColumn<uint64_t> partsupp_posting_rowids;
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

        const uint64_t* green_bitmap = nullptr;
        size_t max_partkey_plus_one = 0;
        std::vector<int32_t> green_partkeys;
        std::vector<uint32_t> partkey_to_payload_offset;
        std::vector<uint32_t> partkey_to_payload_count;
        std::vector<int32_t> payload_suppkeys;
        std::vector<int32_t> payload_supplycost_cents;
        std::vector<uint8_t> supplier_to_nation_row;
        std::vector<std::string> nation_names_by_row;
        std::vector<OrderAlignedPartition> partitions;
        std::vector<std::vector<int64_t>> thread_aggs;

        size_t partsupp_rows = 0;
        size_t supplier_rows = 0;
        size_t nation_rows = 0;
        size_t order_rows = 0;
        size_t lineitem_rows = 0;

        {
            GENDB_PHASE("total");

            {
                GENDB_PHASE("data_loading");

                token_offsets.open(gendb_dir + "/column_versions/part.p_name.token_bitmap/token_offsets.bin");
                token_data.open(gendb_dir + "/column_versions/part.p_name.token_bitmap/token_data.bin");
                token_bitmaps.open(gendb_dir + "/column_versions/part.p_name.token_bitmap/bitmaps.bin");

                partsupp_posting_offsets.open(gendb_dir + "/partsupp/indexes/partsupp_part_postings.offsets.bin");
                partsupp_posting_rowids.open(gendb_dir + "/partsupp/indexes/partsupp_part_postings.rowids.bin");
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

                if (token_offsets.size() < 2) {
                    fail("token bitmap dictionary is empty");
                }
                const size_t token_count = token_offsets.size() - 1;
                if (token_count == 0) {
                    fail("token bitmap dictionary has no tokens");
                }

                if (partsupp_posting_offsets.size() < 2) {
                    fail("partsupp postings offsets are empty");
                }
                max_partkey_plus_one = partsupp_posting_offsets.size() - 1;
                if (max_partkey_plus_one == 0) {
                    fail("invalid partkey domain");
                }
                if (token_bitmaps.size() % token_count != 0) {
                    fail("token bitmap file size is not divisible by token count");
                }
                const size_t words_per_bitmap = token_bitmaps.size() / token_count;
                const size_t expected_words_per_bitmap = (max_partkey_plus_one + 63) / 64;
                if (words_per_bitmap != expected_words_per_bitmap) {
                    fail("token bitmap words_per_bitmap mismatch");
                }

                partsupp_rows = ps_suppkey.size();
                check_count(partsupp_rows, ps_supplycost.size(), "partsupp/ps_supplycost.bin");
                check_count(partsupp_rows, partsupp_posting_rowids.size(), "partsupp_part_postings.rowids.bin");

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

                mmap_prefetch_all(token_offsets,
                                  token_data,
                                  token_bitmaps,
                                  partsupp_posting_offsets,
                                  partsupp_posting_rowids,
                                  ps_suppkey,
                                  ps_supplycost,
                                  supplier_pk_dense,
                                  s_nationkey,
                                  nation_pk_dense,
                                  n_name_code,
                                  n_name_dict_offsets,
                                  n_name_dict_data,
                                  orders_pk_dense,
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

                size_t green_token_idx = 0;
                if (!find_token_index(token_offsets.data,
                                      token_offsets.size(),
                                      token_data.data,
                                      "green",
                                      5,
                                      green_token_idx)) {
                    fail("token bitmap dictionary is missing token 'green'");
                }

                const size_t words_per_bitmap = (max_partkey_plus_one + 63) / 64;
                green_bitmap = token_bitmaps.data + green_token_idx * words_per_bitmap;

                uint64_t green_part_count = 0;
                for (size_t word_idx = 0; word_idx < words_per_bitmap; ++word_idx) {
                    green_part_count += static_cast<uint64_t>(__builtin_popcountll(green_bitmap[word_idx]));
                }
                green_partkeys.reserve(static_cast<size_t>(green_part_count));

                for (size_t word_idx = 0; word_idx < words_per_bitmap; ++word_idx) {
                    uint64_t bits = green_bitmap[word_idx];
                    while (bits != 0) {
                        const unsigned bit = static_cast<unsigned>(__builtin_ctzll(bits));
                        const size_t partkey = (word_idx << 6) + bit;
                        if (partkey < max_partkey_plus_one) {
                            green_partkeys.push_back(static_cast<int32_t>(partkey));
                        }
                        bits &= (bits - 1);
                    }
                }
            }

            {
                GENDB_PHASE("build_joins");

                nation_names_by_row.assign(nation_rows, "");
                for (size_t nation_row = 0; nation_row < nation_rows; ++nation_row) {
                    const uint32_t code = n_name_code[nation_row];
                    nation_names_by_row[nation_row] = decode_dict_entry(n_name_dict_data.data,
                                                                        n_name_dict_offsets.data,
                                                                        n_name_dict_offsets.size(),
                                                                        code);
                }

                supplier_to_nation_row.assign(supplier_pk_dense.size(), kMissingNationRow);
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
                    if (nation_row > static_cast<uint64_t>(std::numeric_limits<uint8_t>::max() - 1)) {
                        fail("nation row exceeds uint8_t range");
                    }
                    supplier_to_nation_row[suppkey] = static_cast<uint8_t>(nation_row);
                }

                partkey_to_payload_offset.assign(max_partkey_plus_one, kMissingOffset);
                partkey_to_payload_count.assign(max_partkey_plus_one, 0);

                uint64_t payload_size = 0;
                for (int32_t partkey : green_partkeys) {
                    const size_t partkey_idx = static_cast<size_t>(partkey);
                    const uint64_t begin = partsupp_posting_offsets[partkey_idx];
                    const uint64_t end = partsupp_posting_offsets[partkey_idx + 1];
                    const uint64_t count = end - begin;
                    if (count == 0) {
                        continue;
                    }
                    if (count > std::numeric_limits<uint32_t>::max()) {
                        fail("posting slice exceeds uint32_t range");
                    }
                    if (payload_size > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) - count) {
                        fail("green partsupp payload exceeds uint32_t address space");
                    }
                    partkey_to_payload_offset[partkey_idx] = static_cast<uint32_t>(payload_size);
                    partkey_to_payload_count[partkey_idx] = static_cast<uint32_t>(count);
                    payload_size += count;
                }

                payload_suppkeys.resize(static_cast<size_t>(payload_size));
                payload_supplycost_cents.resize(static_cast<size_t>(payload_size));

                for (int32_t partkey : green_partkeys) {
                    const size_t partkey_idx = static_cast<size_t>(partkey);
                    const uint32_t count = partkey_to_payload_count[partkey_idx];
                    if (count == 0) {
                        continue;
                    }
                    const uint32_t payload_offset = partkey_to_payload_offset[partkey_idx];
                    const uint64_t begin = partsupp_posting_offsets[partkey_idx];
                    for (uint32_t i = 0; i < count; ++i) {
                        const uint64_t rowid = partsupp_posting_rowids[begin + i];
                        if (rowid >= partsupp_rows) {
                            fail("partsupp posting rowid out of range");
                        }
                        const size_t dst = static_cast<size_t>(payload_offset) + i;
                        payload_suppkeys[dst] = ps_suppkey[static_cast<size_t>(rowid)];
                        payload_supplycost_cents[dst] =
                            static_cast<int32_t>(std::llround(ps_supplycost[static_cast<size_t>(rowid)] * 100.0));
                    }
                }

                std::vector<size_t> partition_starts(static_cast<size_t>(max_threads) + 1, 0);
                partition_starts[0] = 0;
                partition_starts[static_cast<size_t>(max_threads)] = lineitem_rows;
                const int32_t* __restrict__ line_order = l_orderkey.data;
                for (int tid = 1; tid < max_threads; ++tid) {
                    size_t start =
                        (lineitem_rows * static_cast<size_t>(tid)) / static_cast<size_t>(max_threads);
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

                const size_t agg_cells = nation_rows * static_cast<size_t>(kYearBucketCount);
                thread_aggs.assign(partitions.size(), std::vector<int64_t>(agg_cells, 0));
            }

            {
                GENDB_PHASE("main_scan");

                const int32_t* __restrict__ line_part = l_partkey.data;
                const int32_t* __restrict__ line_supp = l_suppkey.data;
                const int32_t* __restrict__ line_order = l_orderkey.data;
                const double* __restrict__ line_extprice = l_extendedprice.data;
                const double* __restrict__ line_disc = l_discount.data;
                const double* __restrict__ line_qty = l_quantity.data;

                const uint32_t* __restrict__ payload_offsets = partkey_to_payload_offset.data();
                const uint32_t* __restrict__ payload_counts = partkey_to_payload_count.data();
                const int32_t* __restrict__ part_payload_suppkeys = payload_suppkeys.data();
                const int32_t* __restrict__ part_payload_supplycost_cents = payload_supplycost_cents.data();
                const uint8_t* __restrict__ supp_to_nation = supplier_to_nation_row.data();

                const int32_t* __restrict__ order_keys = o_orderkey.data;
                const int32_t* __restrict__ order_dates = o_orderdate.data;

                #pragma omp parallel for schedule(static, 1)
                for (int tid = 0; tid < static_cast<int>(partitions.size()); ++tid) {
                    const OrderAlignedPartition partition = partitions[static_cast<size_t>(tid)];
                    if (partition.line_start >= partition.line_end) {
                        continue;
                    }

                    std::vector<uint32_t> selected_rows;
                    std::vector<uint8_t> selected_nations;
                    std::vector<int32_t> selected_supplycost_cents;
                    const size_t reserve_guess =
                        (partition.line_end - partition.line_start) / 16;
                    selected_rows.reserve(reserve_guess);
                    selected_nations.reserve(reserve_guess);
                    selected_supplycost_cents.reserve(reserve_guess);

                    for (size_t row = partition.line_start; row < partition.line_end; ++row) {
                        const int32_t partkey = line_part[row];
                        if (partkey < 0) {
                            continue;
                        }
                        const size_t partkey_idx = static_cast<size_t>(partkey);
                        if (partkey_idx >= max_partkey_plus_one || !bitmap_contains(green_bitmap, partkey_idx)) {
                            continue;
                        }

                        const uint32_t count = payload_counts[partkey_idx];
                        if (count == 0) {
                            continue;
                        }

                        const int32_t suppkey = line_supp[row];
                        if (suppkey < 0 || static_cast<size_t>(suppkey) >= supplier_to_nation_row.size()) {
                            continue;
                        }

                        const uint8_t nation_row = supp_to_nation[static_cast<size_t>(suppkey)];
                        if (nation_row == kMissingNationRow) {
                            continue;
                        }

                        const uint32_t offset = payload_offsets[partkey_idx];
                        const size_t begin = static_cast<size_t>(offset);
                        const size_t end = begin + static_cast<size_t>(count);
                        int32_t supplycost_cents = -1;
                        for (size_t i = begin; i < end; ++i) {
                            if (part_payload_suppkeys[i] == suppkey) {
                                supplycost_cents = part_payload_supplycost_cents[i];
                                break;
                            }
                        }
                        if (supplycost_cents < 0) {
                            continue;
                        }

                        selected_rows.push_back(static_cast<uint32_t>(row));
                        selected_nations.push_back(nation_row);
                        selected_supplycost_cents.push_back(supplycost_cents);
                    }

                    std::vector<int64_t>& local_agg = thread_aggs[static_cast<size_t>(tid)];
                    size_t order_row = partition.order_start;

                    for (size_t i = 0; i < selected_rows.size(); ++i) {
                        const size_t row = selected_rows[i];
                        const int32_t orderkey = line_order[row];
                        while (order_row < partition.order_end && order_keys[order_row] < orderkey) {
                            ++order_row;
                        }
                        if (order_row >= partition.order_end || order_keys[order_row] != orderkey) {
                            fail("sequential orders cursor lost alignment");
                        }

                        const int32_t year = extract_year(order_dates[order_row]);
                        const int32_t year_idx = year - kBaseYear;
                        if (year_idx < 0 || year_idx >= kYearBucketCount) {
                            fail("o_orderdate year out of expected TPCH Q9 range");
                        }

                        const int32_t extprice_cents =
                            static_cast<int32_t>(std::llround(line_extprice[row] * 100.0));
                        const int32_t discount_pct =
                            static_cast<int32_t>(std::llround(line_disc[row] * 100.0));
                        const int32_t quantity_hundredths =
                            static_cast<int32_t>(std::llround(line_qty[row] * 100.0));

                        const int64_t amount =
                            static_cast<int64_t>(extprice_cents) * (100 - discount_pct) -
                            static_cast<int64_t>(selected_supplycost_cents[i]) * quantity_hundredths;

                        const size_t agg_idx =
                            static_cast<size_t>(selected_nations[i]) * static_cast<size_t>(kYearBucketCount) +
                            static_cast<size_t>(year_idx);
                        local_agg[agg_idx] += amount;
                    }
                }
            }

            {
                GENDB_PHASE("output");

                const size_t agg_cells = nation_rows * static_cast<size_t>(kYearBucketCount);
                std::vector<int64_t> totals(agg_cells, 0);
                for (const auto& local : thread_aggs) {
                    for (size_t i = 0; i < agg_cells; ++i) {
                        totals[i] += local[i];
                    }
                }

                std::vector<ResultRow> rows;
                rows.reserve(agg_cells);
                for (size_t nation_row = 0; nation_row < nation_rows; ++nation_row) {
                    for (int32_t year_idx = 0; year_idx < kYearBucketCount; ++year_idx) {
                        const int64_t amount =
                            totals[nation_row * static_cast<size_t>(kYearBucketCount) +
                                   static_cast<size_t>(year_idx)];
                        if (amount == 0) {
                            continue;
                        }
                        rows.push_back(ResultRow{
                            nation_names_by_row[nation_row],
                            kBaseYear + year_idx,
                            static_cast<double>(amount) / 10000.0,
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
