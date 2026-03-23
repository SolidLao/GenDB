#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr size_t kExpectedRows = 59986052;
constexpr size_t kMaxGroupsPerThread = 8;

struct AggState {
    uint16_t key = 0;
    bool used = false;
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    long double sum_disc_price = 0.0L;
    long double sum_charge = 0.0L;
    double sum_disc = 0.0;
    uint64_t count = 0;
};

struct FixedAggMap {
    AggState groups[kMaxGroupsPerThread];
    uint8_t size = 0;

    inline AggState& lookup_or_insert(uint16_t key) {
        for (uint8_t i = 0; i < size; ++i) {
            if (groups[i].key == key) {
                return groups[i];
            }
        }
        if (size >= kMaxGroupsPerThread) {
            throw std::runtime_error("FixedAggMap capacity exceeded");
        }
        AggState& slot = groups[size++];
        slot.key = key;
        slot.used = true;
        slot.sum_qty = 0.0;
        slot.sum_base_price = 0.0;
        slot.sum_disc_price = 0.0L;
        slot.sum_charge = 0.0L;
        slot.sum_disc = 0.0;
        slot.count = 0;
        return slot;
    }
};

inline uint16_t pack_key(uint8_t returnflag, uint8_t linestatus) {
    return static_cast<uint16_t>((static_cast<uint16_t>(returnflag) << 8) |
                                 static_cast<uint16_t>(linestatus));
}

inline char unpack_returnflag(uint16_t key) {
    return static_cast<char>(key >> 8);
}

inline char unpack_linestatus(uint16_t key) {
    return static_cast<char>(key & 0xFF);
}

inline void aggregate_row(AggState& agg,
                          double quantity,
                          double extendedprice,
                          double discount,
                          double tax) {
    const long double disc_price =
        static_cast<long double>(extendedprice) * (1.0L - static_cast<long double>(discount));

    agg.sum_qty += quantity;
    agg.sum_base_price += extendedprice;
    agg.sum_disc_price += disc_price;
    agg.sum_charge += disc_price * (1.0L + static_cast<long double>(tax));
    agg.sum_disc += discount;
    ++agg.count;
}

}  // namespace

int main(int argc, char** argv) {
    GENDB_PHASE("total");

    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        std::filesystem::create_directories(results_dir);

        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<uint8_t> l_returnflag;
        gendb::MmapColumn<uint8_t> l_linestatus;
        gendb::MmapColumn<double> l_quantity;
        gendb::MmapColumn<double> l_extendedprice;
        gendb::MmapColumn<double> l_discount;
        gendb::MmapColumn<double> l_tax;

        {
            GENDB_PHASE("data_loading");
            const std::string lineitem_dir = gendb_dir + "/lineitem";

            l_shipdate.open(lineitem_dir + "/l_shipdate.bin");
            l_returnflag.open(lineitem_dir + "/l_returnflag.bin");
            l_linestatus.open(lineitem_dir + "/l_linestatus.bin");
            l_quantity.open(lineitem_dir + "/l_quantity.bin");
            l_extendedprice.open(lineitem_dir + "/l_extendedprice.bin");
            l_discount.open(lineitem_dir + "/l_discount.bin");
            l_tax.open(lineitem_dir + "/l_tax.bin");

            gendb::mmap_prefetch_all(
                l_shipdate, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax);
        }

        if (l_shipdate.size() != kExpectedRows || l_returnflag.size() != kExpectedRows ||
            l_linestatus.size() != kExpectedRows || l_quantity.size() != kExpectedRows ||
            l_extendedprice.size() != kExpectedRows || l_discount.size() != kExpectedRows ||
            l_tax.size() != kExpectedRows) {
            throw std::runtime_error("Unexpected lineitem column size");
        }

        const size_t n_rows = l_shipdate.size();
        const int32_t cutoff =
            gendb::add_days(gendb::date_str_to_epoch_days("1998-12-01"), -90);

        {
            GENDB_PHASE("dim_filter");
        }

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<FixedAggMap> thread_maps(static_cast<size_t>(max_threads));

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel num_threads(max_threads)
            {
                const int tid = omp_get_thread_num();
                const int nthr = omp_get_num_threads();
                const size_t chunk = (n_rows + static_cast<size_t>(nthr) - 1) / static_cast<size_t>(nthr);
                const size_t begin = std::min(static_cast<size_t>(tid) * chunk, n_rows);
                const size_t end = std::min(begin + chunk, n_rows);
                FixedAggMap& local = thread_maps[static_cast<size_t>(tid)];

                const int32_t* __restrict__ shipdate = l_shipdate.data;
                const uint8_t* __restrict__ returnflag = l_returnflag.data;
                const uint8_t* __restrict__ linestatus = l_linestatus.data;
                const double* __restrict__ quantity = l_quantity.data;
                const double* __restrict__ extendedprice = l_extendedprice.data;
                const double* __restrict__ discount = l_discount.data;
                const double* __restrict__ tax = l_tax.data;

                for (size_t i = begin; i < end; ++i) {
                    if (shipdate[i] > cutoff) {
                        continue;
                    }

                    AggState& agg = local.lookup_or_insert(pack_key(returnflag[i], linestatus[i]));
                    aggregate_row(agg, quantity[i], extendedprice[i], discount[i], tax[i]);
                }
            }
        }

        FixedAggMap global_map;
        {
            GENDB_PHASE("build_joins");
            for (const FixedAggMap& local : thread_maps) {
                for (uint8_t i = 0; i < local.size; ++i) {
                    const AggState& src = local.groups[i];
                    AggState& dst = global_map.lookup_or_insert(src.key);
                    dst.sum_qty += src.sum_qty;
                    dst.sum_base_price += src.sum_base_price;
                    dst.sum_disc_price += src.sum_disc_price;
                    dst.sum_charge += src.sum_charge;
                    dst.sum_disc += src.sum_disc;
                    dst.count += src.count;
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<const AggState*> rows;
            rows.reserve(global_map.size);
            for (uint8_t i = 0; i < global_map.size; ++i) {
                if (global_map.groups[i].count != 0) {
                    rows.push_back(&global_map.groups[i]);
                }
            }

            std::sort(rows.begin(), rows.end(), [](const AggState* a, const AggState* b) {
                return a->key < b->key;
            });

            const std::string out_path = results_dir + "/Q1.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file");
            }

            std::fprintf(out,
                         "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

            for (const AggState* agg : rows) {
                const double count = static_cast<double>(agg->count);
                std::fprintf(out,
                             "%c,%c,%.2f,%.2f,%.4Lf,%.6Lf,%.2f,%.2f,%.2f,%llu\n",
                             unpack_returnflag(agg->key),
                             unpack_linestatus(agg->key),
                             agg->sum_qty,
                             agg->sum_base_price,
                             agg->sum_disc_price,
                             agg->sum_charge,
                             agg->sum_qty / count,
                             agg->sum_base_price / count,
                             agg->sum_disc / count,
                             static_cast<unsigned long long>(agg->count));
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
