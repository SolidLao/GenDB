#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr uint32_t kMissingRow = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kPartsuppHashMul = 11400714819323198485ull;
constexpr int kNationCount = 25;
constexpr int kYearBase = 1992;
constexpr int kYearCount = 7;
constexpr int kAggStride = 256;
constexpr int kMorselRows = 1 << 16;

struct ResultRow {
    std::string nation;
    int year;
    int64_t sum_profit_scaled;
};

inline uint32_t probe_partsupp_row(const uint64_t* keys,
                                   const uint32_t* row_ids,
                                   uint64_t mask,
                                   int32_t partkey,
                                   int32_t suppkey) {
    const uint64_t packed =
        (static_cast<uint64_t>(static_cast<uint32_t>(partkey)) << 32) |
        static_cast<uint32_t>(suppkey);
    uint64_t slot = (packed * kPartsuppHashMul) & mask;
    while (true) {
        const uint64_t cur = keys[slot];
        if (cur == packed) {
            return row_ids[slot];
        }
        if (cur == 0) {
            return kMissingRow;
        }
        slot = (slot + 1) & mask;
    }
}

inline std::string decode_dict_entry(const char* bytes,
                                     const uint64_t* offsets,
                                     size_t code) {
    const uint64_t begin = offsets[code];
    const uint64_t end = offsets[code + 1];
    return std::string(bytes + begin, bytes + end);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        GENDB_PHASE("total");
        init_date_tables();

        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        const int threads = std::min(64, omp_get_max_threads());
        omp_set_num_threads(threads);

        MmapColumn<int32_t> lineitem_partkey;
        MmapColumn<int32_t> lineitem_suppkey;
        MmapColumn<int32_t> lineitem_orderkey;
        MmapColumn<int64_t> lineitem_quantity;
        MmapColumn<int64_t> lineitem_extendedprice;
        MmapColumn<int64_t> lineitem_discount;

        MmapColumn<uint8_t> part_name_green_bitset;

        MmapColumn<uint32_t> supplier_pk_dense;
        MmapColumn<int32_t> supplier_nationkey;

        MmapColumn<uint64_t> partsupp_pk_hash_keys;
        MmapColumn<uint32_t> partsupp_pk_hash_row_ids;
        MmapColumn<int64_t> partsupp_supplycost;

        MmapColumn<uint32_t> orders_pk_dense;
        MmapColumn<int32_t> orders_orderdate;

        MmapColumn<uint32_t> nation_pk_dense;
        MmapColumn<uint8_t> nation_name_codes;
        MmapColumn<uint64_t> nation_name_dict_offsets;
        MmapColumn<char> nation_name_dict_data;

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

            partsupp_pk_hash_keys.open(gendb_dir + "/partsupp/indexes/partsupp_pk_hash.keys.bin");
            partsupp_pk_hash_row_ids.open(gendb_dir + "/partsupp/indexes/partsupp_pk_hash.row_ids.bin");
            partsupp_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

            orders_pk_dense.open(gendb_dir + "/orders/indexes/orders_pk_dense.bin");
            orders_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");

            nation_pk_dense.open(gendb_dir + "/nation/indexes/nation_pk_dense.bin");
            nation_name_codes.open(gendb_dir + "/nation/n_name.codes.bin");
            nation_name_dict_offsets.open(gendb_dir + "/nation/n_name.dict.offsets.bin");
            nation_name_dict_data.open(gendb_dir + "/nation/n_name.dict.data.bin");

            mmap_prefetch_all(lineitem_partkey,
                              lineitem_suppkey,
                              lineitem_orderkey,
                              lineitem_quantity,
                              lineitem_extendedprice,
                              lineitem_discount,
                              part_name_green_bitset,
                              supplier_pk_dense,
                              supplier_nationkey,
                              partsupp_pk_hash_keys,
                              partsupp_pk_hash_row_ids,
                              partsupp_supplycost,
                              orders_pk_dense,
                              orders_orderdate,
                              nation_pk_dense,
                              nation_name_codes,
                              nation_name_dict_offsets,
                              nation_name_dict_data);

            supplier_pk_dense.advise_random();
            supplier_nationkey.advise_random();
            partsupp_pk_hash_keys.advise_random();
            partsupp_pk_hash_row_ids.advise_random();
            partsupp_supplycost.advise_random();
            orders_pk_dense.advise_random();
            orders_orderdate.advise_random();
            nation_pk_dense.advise_random();
            nation_name_codes.advise_random();
        }

        {
            GENDB_PHASE("dim_filter");
            if (part_name_green_bitset.count != 2000001) {
                throw std::runtime_error("unexpected part_name_green_bitset length");
            }
        }

        std::array<std::string, kNationCount> nation_names;
        {
            GENDB_PHASE("build_joins");
            if (partsupp_pk_hash_keys.count == 0 ||
                partsupp_pk_hash_keys.count != partsupp_pk_hash_row_ids.count) {
                throw std::runtime_error("invalid partsupp hash index");
            }
            if ((partsupp_pk_hash_keys.count & (partsupp_pk_hash_keys.count - 1)) != 0) {
                throw std::runtime_error("partsupp hash slot count must be power of two");
            }
            if (nation_name_dict_offsets.count < static_cast<size_t>(kNationCount + 1)) {
                throw std::runtime_error("invalid nation dictionary offsets");
            }
            for (int code = 0; code < kNationCount; ++code) {
                nation_names[code] = decode_dict_entry(
                    nation_name_dict_data.data,
                    nation_name_dict_offsets.data,
                    static_cast<size_t>(code));
            }
        }

        const int64_t row_count = static_cast<int64_t>(lineitem_partkey.count);
        if (lineitem_suppkey.count != static_cast<size_t>(row_count) ||
            lineitem_orderkey.count != static_cast<size_t>(row_count) ||
            lineitem_quantity.count != static_cast<size_t>(row_count) ||
            lineitem_extendedprice.count != static_cast<size_t>(row_count) ||
            lineitem_discount.count != static_cast<size_t>(row_count)) {
            throw std::runtime_error("lineitem column length mismatch");
        }

        const uint64_t partsupp_mask = partsupp_pk_hash_keys.count - 1;
        const int actual_threads = omp_get_max_threads();
        std::vector<int64_t> thread_agg(static_cast<size_t>(actual_threads) * kAggStride, 0);

        {
            GENDB_PHASE("main_scan");

            const int32_t* __restrict__ l_partkey = lineitem_partkey.data;
            const int32_t* __restrict__ l_suppkey = lineitem_suppkey.data;
            const int32_t* __restrict__ l_orderkey = lineitem_orderkey.data;
            const int64_t* __restrict__ l_quantity = lineitem_quantity.data;
            const int64_t* __restrict__ l_extendedprice = lineitem_extendedprice.data;
            const int64_t* __restrict__ l_discount = lineitem_discount.data;

            const uint8_t* __restrict__ green = part_name_green_bitset.data;
            const uint32_t* __restrict__ supplier_dense = supplier_pk_dense.data;
            const int32_t* __restrict__ supplier_nk = supplier_nationkey.data;

            const uint64_t* __restrict__ ps_keys = partsupp_pk_hash_keys.data;
            const uint32_t* __restrict__ ps_rows = partsupp_pk_hash_row_ids.data;
            const int64_t* __restrict__ ps_supplycost = partsupp_supplycost.data;

            const uint32_t* __restrict__ orders_dense = orders_pk_dense.data;
            const int32_t* __restrict__ o_orderdate = orders_orderdate.data;

            const uint32_t* __restrict__ nation_dense = nation_pk_dense.data;
            const uint8_t* __restrict__ nation_codes = nation_name_codes.data;

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                int64_t* __restrict__ local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;

                #pragma omp for schedule(dynamic, kMorselRows) nowait
                for (int64_t i = 0; i < row_count; ++i) {
                    const int32_t partkey = l_partkey[i];
                    if (static_cast<uint32_t>(partkey) >= part_name_green_bitset.count ||
                        green[partkey] == 0) {
                        continue;
                    }

                    const int32_t suppkey = l_suppkey[i];
                    if (static_cast<uint32_t>(suppkey) >= supplier_pk_dense.count) {
                        continue;
                    }
                    const uint32_t supplier_row = supplier_dense[suppkey];
                    if (supplier_row == kMissingRow || supplier_row >= supplier_nationkey.count) {
                        continue;
                    }

                    const uint32_t ps_row = probe_partsupp_row(ps_keys, ps_rows, partsupp_mask, partkey, suppkey);
                    if (ps_row == kMissingRow || ps_row >= partsupp_supplycost.count) {
                        continue;
                    }

                    const int32_t orderkey = l_orderkey[i];
                    if (static_cast<uint32_t>(orderkey) >= orders_pk_dense.count) {
                        continue;
                    }
                    const uint32_t order_row = orders_dense[orderkey];
                    if (order_row == kMissingRow || order_row >= orders_orderdate.count) {
                        continue;
                    }

                    const int32_t nationkey = supplier_nk[supplier_row];
                    if (static_cast<uint32_t>(nationkey) >= nation_pk_dense.count) {
                        continue;
                    }
                    const uint32_t nation_row = nation_dense[nationkey];
                    if (nation_row == kMissingRow || nation_row >= nation_name_codes.count) {
                        continue;
                    }

                    const int year = extract_year(o_orderdate[order_row]);
                    const int year_idx = year - kYearBase;
                    if (static_cast<unsigned>(year_idx) >= kYearCount) {
                        continue;
                    }

                    const uint8_t nation_code = nation_codes[nation_row];
                    if (nation_code >= kNationCount) {
                        continue;
                    }

                    const __int128 revenue_scaled =
                        static_cast<__int128>(l_extendedprice[i]) * (100 - l_discount[i]);
                    const __int128 supply_scaled =
                        static_cast<__int128>(ps_supplycost[ps_row]) * l_quantity[i];
                    const int64_t amount_scaled = static_cast<int64_t>(revenue_scaled - supply_scaled);
                    local[static_cast<int>(nation_code) * kYearCount + year_idx] += amount_scaled;
                }
            }
        }

        std::array<int64_t, kNationCount * kYearCount> totals{};
        for (int tid = 0; tid < actual_threads; ++tid) {
            const int64_t* local = thread_agg.data() + static_cast<size_t>(tid) * kAggStride;
            for (int idx = 0; idx < kNationCount * kYearCount; ++idx) {
                totals[idx] += local[idx];
            }
        }

        std::vector<ResultRow> rows;
        rows.reserve(kNationCount * kYearCount);
        for (int nation_code = 0; nation_code < kNationCount; ++nation_code) {
            for (int year_idx = 0; year_idx < kYearCount; ++year_idx) {
                const int64_t sum_scaled = totals[nation_code * kYearCount + year_idx];
                if (sum_scaled == 0) {
                    continue;
                }
                rows.push_back(ResultRow{
                    nation_names[nation_code],
                    kYearBase + year_idx,
                    sum_scaled,
                });
            }
        }

        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) {
                return a.nation < b.nation;
            }
            return a.year > b.year;
        });

        {
            GENDB_PHASE("output");
            mkdir(results_dir.c_str(), 0777);
            const std::string out_path = results_dir + "/Q9.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }
            std::fprintf(out, "nation,o_year,sum_profit\n");
            for (const ResultRow& row : rows) {
                std::fprintf(out, "%s,%d,%.2f\n",
                             row.nation.c_str(),
                             row.year,
                             static_cast<double>(row.sum_profit_scaled) / 10000.0);
            }
            std::fclose(out);
        }
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Q9 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
