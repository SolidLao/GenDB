#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint64_t kMissingRow = std::numeric_limits<uint64_t>::max();
constexpr const char kNeedle[] = "green";
constexpr size_t kNeedleLen = 5;
constexpr uint32_t kAggCapacity = 256;
constexpr uint32_t kEmptyKey = std::numeric_limits<uint32_t>::max();

template <typename T>
void ensure_same_rows(const char* name, const gendb::MmapColumn<T>& col, size_t rows) {
    if (col.size() != rows) {
        throw std::runtime_error(std::string("Row-count mismatch for ") + name);
    }
}

inline bool string_contains_green(const uint64_t* offsets, const char* data, size_t row) {
    const uint64_t begin = offsets[row];
    const uint64_t end = offsets[row + 1];
    const size_t len = static_cast<size_t>(end - begin);
    if (len < kNeedleLen) {
        return false;
    }
    const std::string_view text(data + begin, len);
    return text.find(kNeedle) != std::string_view::npos;
}

inline int32_t year_from_days_since_epoch(int32_t z) {
    z += 719468;
    const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    const uint32_t doe = static_cast<uint32_t>(z - era * 146097);
    const uint32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int32_t y = static_cast<int32_t>(yoe) + era * 400;
    const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const uint32_t mp = (5 * doy + 2) / 153;
    const int32_t m = static_cast<int32_t>(mp) + (mp < 10 ? 3 : -9);
    y += (m <= 2);
    return y;
}

struct AggEntry {
    uint32_t key = kEmptyKey;
    double value = 0.0;
};

struct SmallAggMap {
    std::array<AggEntry, kAggCapacity> entries{};
    size_t used = 0;

    void add(uint32_t key, double delta) {
        uint32_t slot = (key * 2654435761u) & (kAggCapacity - 1);
        while (true) {
            AggEntry& entry = entries[slot];
            if (entry.key == key) {
                entry.value += delta;
                return;
            }
            if (entry.key == kEmptyKey) {
                entry.key = key;
                entry.value = delta;
                ++used;
                return;
            }
            slot = (slot + 1) & (kAggCapacity - 1);
        }
    }
};

struct OutputRow {
    std::string nation;
    int32_t o_year;
    double sum_profit;
};

inline std::string decode_dict_string(const uint64_t* offsets, const char* data, uint32_t code) {
    const uint64_t begin = offsets[code];
    const uint64_t end = offsets[code + 1];
    return std::string(data + begin, data + end);
}

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

        gendb::MmapColumn<int32_t> p_partkey;
        gendb::MmapColumn<uint64_t> p_name_offsets;
        gendb::MmapColumn<char> p_name_data;

        gendb::MmapColumn<uint64_t> supplier_pk_dense;
        gendb::MmapColumn<int32_t> s_nationkey;
        gendb::MmapColumn<uint64_t> nation_pk_dense;
        gendb::MmapColumn<uint32_t> n_name_codes;
        gendb::MmapColumn<uint64_t> n_name_dict_offsets;
        gendb::MmapColumn<char> n_name_dict_data;

        gendb::MmapColumn<uint64_t> orders_pk_dense;
        gendb::MmapColumn<int32_t> o_orderdate;

        gendb::MmapColumn<uint64_t> partsupp_offsets;
        gendb::MmapColumn<uint64_t> partsupp_rowids;
        gendb::MmapColumn<int32_t> ps_suppkey;
        gendb::MmapColumn<double> ps_supplycost;

        gendb::MmapColumn<int32_t> l_suppkey;
        gendb::MmapColumn<int32_t> l_partkey;
        gendb::MmapColumn<int32_t> l_orderkey;
        gendb::MmapColumn<double> l_extendedprice;
        gendb::MmapColumn<double> l_discount;
        gendb::MmapColumn<double> l_quantity;

        size_t lineitem_rows = 0;

        {
            GENDB_PHASE("data_loading");

            const std::string part_dir = gendb_dir + "/part";
            p_partkey.open(part_dir + "/p_partkey.bin");
            p_name_offsets.open(part_dir + "/p_name.offsets.bin");
            p_name_data.open(part_dir + "/p_name.data.bin");

            const std::string supplier_dir = gendb_dir + "/supplier";
            supplier_pk_dense.open(supplier_dir + "/indexes/supplier_pk_dense.bin");
            s_nationkey.open(supplier_dir + "/s_nationkey.bin");

            const std::string nation_dir = gendb_dir + "/nation";
            nation_pk_dense.open(nation_dir + "/indexes/nation_pk_dense.bin");
            n_name_codes.open(nation_dir + "/n_name.bin");
            n_name_dict_offsets.open(nation_dir + "/n_name.dict.offsets.bin");
            n_name_dict_data.open(nation_dir + "/n_name.dict.data.bin");

            const std::string orders_dir = gendb_dir + "/orders";
            orders_pk_dense.open(orders_dir + "/indexes/orders_pk_dense.bin");
            o_orderdate.open(orders_dir + "/o_orderdate.bin");

            const std::string partsupp_dir = gendb_dir + "/partsupp";
            partsupp_offsets.open(partsupp_dir + "/indexes/partsupp_part_postings.offsets.bin");
            partsupp_rowids.open(partsupp_dir + "/indexes/partsupp_part_postings.rowids.bin");
            ps_suppkey.open(partsupp_dir + "/ps_suppkey.bin");
            ps_supplycost.open(partsupp_dir + "/ps_supplycost.bin");

            const std::string lineitem_dir = gendb_dir + "/lineitem";
            l_suppkey.open(lineitem_dir + "/l_suppkey.bin");
            l_partkey.open(lineitem_dir + "/l_partkey.bin");
            l_orderkey.open(lineitem_dir + "/l_orderkey.bin");
            l_extendedprice.open(lineitem_dir + "/l_extendedprice.bin");
            l_discount.open(lineitem_dir + "/l_discount.bin");
            l_quantity.open(lineitem_dir + "/l_quantity.bin");

            ensure_same_rows("p_name.offsets", p_name_offsets, p_partkey.size() + 1);
            ensure_same_rows("s_nationkey", s_nationkey, 100000);
            ensure_same_rows("n_name", n_name_codes, 25);
            ensure_same_rows("o_orderdate", o_orderdate, 15000000);
            ensure_same_rows("ps_suppkey", ps_suppkey, ps_supplycost.size());
            ensure_same_rows("partsupp_rowids", partsupp_rowids, ps_suppkey.size());

            lineitem_rows = l_partkey.size();
            ensure_same_rows("l_suppkey", l_suppkey, lineitem_rows);
            ensure_same_rows("l_orderkey", l_orderkey, lineitem_rows);
            ensure_same_rows("l_extendedprice", l_extendedprice, lineitem_rows);
            ensure_same_rows("l_discount", l_discount, lineitem_rows);
            ensure_same_rows("l_quantity", l_quantity, lineitem_rows);

            p_partkey.advise_sequential();
            p_name_offsets.advise_sequential();
            p_name_data.advise_sequential();
            l_suppkey.advise_sequential();
            l_partkey.advise_sequential();
            l_orderkey.advise_sequential();
            l_extendedprice.advise_sequential();
            l_discount.advise_sequential();
            l_quantity.advise_sequential();

            supplier_pk_dense.advise_random();
            nation_pk_dense.advise_random();
            orders_pk_dense.advise_random();
            partsupp_offsets.advise_random();
            partsupp_rowids.advise_random();
            ps_suppkey.advise_random();
            ps_supplycost.advise_random();

            gendb::mmap_prefetch_all(
                p_partkey, p_name_offsets, p_name_data,
                supplier_pk_dense, s_nationkey, nation_pk_dense, n_name_codes, n_name_dict_offsets, n_name_dict_data,
                orders_pk_dense, o_orderdate,
                partsupp_offsets, partsupp_rowids, ps_suppkey, ps_supplycost,
                l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, l_quantity
            );
        }

        std::vector<uint64_t> green_part_bits((2000001u + 63u) / 64u, 0);
        std::vector<uint32_t> supplier_to_nation_code;
        std::vector<int16_t> order_to_year;

        {
            GENDB_PHASE("dim_filter");

            const int32_t* __restrict partkeys = p_partkey.data;
            const uint64_t* __restrict name_offsets = p_name_offsets.data;
            const char* __restrict name_data = p_name_data.data;
            const size_t part_rows = p_partkey.size();

            #pragma omp parallel for schedule(static)
            for (size_t row = 0; row < part_rows; ++row) {
                if (!string_contains_green(name_offsets, name_data, row)) {
                    continue;
                }
                const uint32_t key = static_cast<uint32_t>(partkeys[row]);
                const size_t word = key >> 6;
                const uint64_t mask = 1ULL << (key & 63U);
                #pragma omp atomic update
                green_part_bits[word] |= mask;
            }
        }

        {
            GENDB_PHASE("build_joins");

            supplier_to_nation_code.assign(supplier_pk_dense.size(), 0);
            const uint64_t* __restrict supp_index = supplier_pk_dense.data;
            const int32_t* __restrict supplier_nation = s_nationkey.data;
            const uint64_t* __restrict nation_index = nation_pk_dense.data;
            const uint32_t* __restrict nation_codes = n_name_codes.data;

            #pragma omp parallel for schedule(static)
            for (size_t suppkey = 0; suppkey < supplier_pk_dense.size(); ++suppkey) {
                const uint64_t supplier_row = supp_index[suppkey];
                if (supplier_row == kMissingRow) {
                    continue;
                }
                const int32_t nation_key = supplier_nation[supplier_row];
                const uint64_t nation_row = nation_index[static_cast<size_t>(nation_key)];
                if (nation_row == kMissingRow) {
                    continue;
                }
                supplier_to_nation_code[suppkey] = nation_codes[nation_row];
            }

            order_to_year.assign(orders_pk_dense.size(), 0);
            const uint64_t* __restrict order_index = orders_pk_dense.data;
            const int32_t* __restrict order_dates = o_orderdate.data;

            #pragma omp parallel for schedule(static)
            for (size_t orderkey = 0; orderkey < orders_pk_dense.size(); ++orderkey) {
                const uint64_t order_row = order_index[orderkey];
                if (order_row == kMissingRow) {
                    continue;
                }
                order_to_year[orderkey] = static_cast<int16_t>(year_from_days_since_epoch(order_dates[order_row]));
            }
        }

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<SmallAggMap> thread_maps(static_cast<size_t>(max_threads));

        {
            GENDB_PHASE("main_scan");

            const uint64_t* __restrict green_bits = green_part_bits.data();
            const int32_t* __restrict line_supp = l_suppkey.data;
            const int32_t* __restrict line_part = l_partkey.data;
            const int32_t* __restrict line_order = l_orderkey.data;
            const double* __restrict line_price = l_extendedprice.data;
            const double* __restrict line_disc = l_discount.data;
            const double* __restrict line_qty = l_quantity.data;

            const uint32_t* __restrict supp_to_nation = supplier_to_nation_code.data();
            const int16_t* __restrict order_year = order_to_year.data();

            const uint64_t* __restrict ps_offsets = partsupp_offsets.data;
            const uint64_t* __restrict ps_rows = partsupp_rowids.data;
            const int32_t* __restrict ps_supp = ps_suppkey.data;
            const double* __restrict ps_cost = ps_supplycost.data;

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                SmallAggMap& local_map = thread_maps[static_cast<size_t>(tid)];

                #pragma omp for schedule(dynamic, 16384)
                for (size_t row = 0; row < lineitem_rows; ++row) {
                    const uint32_t partkey = static_cast<uint32_t>(line_part[row]);
                    if (((green_bits[partkey >> 6] >> (partkey & 63U)) & 1ULL) == 0) {
                        continue;
                    }

                    const uint32_t suppkey = static_cast<uint32_t>(line_supp[row]);
                    const uint32_t orderkey = static_cast<uint32_t>(line_order[row]);

                    double supply_cost = 0.0;
                    bool found = false;
                    const uint64_t begin = ps_offsets[partkey];
                    const uint64_t end = ps_offsets[partkey + 1];
                    for (uint64_t pos = begin; pos < end; ++pos) {
                        const uint64_t ps_row = ps_rows[pos];
                        if (static_cast<uint32_t>(ps_supp[ps_row]) == suppkey) {
                            supply_cost = ps_cost[ps_row];
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        continue;
                    }

                    const uint32_t nation_code = supp_to_nation[suppkey];
                    const int32_t year = order_year[orderkey];
                    const double amount =
                        line_price[row] * (1.0 - line_disc[row]) - supply_cost * line_qty[row];
                    const uint32_t agg_key =
                        (nation_code << 16) | static_cast<uint32_t>(year - 1900);
                    local_map.add(agg_key, amount);
                }
            }
        }

        SmallAggMap merged;
        std::vector<OutputRow> output_rows;

        {
            GENDB_PHASE("build_joins");

            for (const SmallAggMap& thread_map : thread_maps) {
                for (const AggEntry& entry : thread_map.entries) {
                    if (entry.key != kEmptyKey) {
                        merged.add(entry.key, entry.value);
                    }
                }
            }

            output_rows.reserve(merged.used);
            const uint64_t* __restrict dict_offsets = n_name_dict_offsets.data;
            const char* __restrict dict_data = n_name_dict_data.data;
            for (const AggEntry& entry : merged.entries) {
                if (entry.key == kEmptyKey) {
                    continue;
                }
                const uint32_t nation_code = entry.key >> 16;
                const int32_t year = static_cast<int32_t>(entry.key & 0xFFFFu) + 1900;
                output_rows.push_back(OutputRow{
                    decode_dict_string(dict_offsets, dict_data, nation_code),
                    year,
                    entry.value
                });
            }

            std::sort(output_rows.begin(), output_rows.end(), [](const OutputRow& a, const OutputRow& b) {
                if (a.nation != b.nation) {
                    return a.nation < b.nation;
                }
                return a.o_year > b.o_year;
            });
        }

        {
            GENDB_PHASE("output");
            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q9.csv";
            std::FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file");
            }

            std::fprintf(out, "nation,o_year,sum_profit\n");
            for (const OutputRow& row : output_rows) {
                std::fprintf(out, "%s,%d,%.4f\n", row.nation.c_str(), row.o_year, row.sum_profit);
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q9 failed: %s\n", e.what());
        return 1;
    }
}
