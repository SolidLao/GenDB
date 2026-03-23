#include <algorithm>
#include <bitset>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <cmath>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr size_t kBlockSize = 131072;
constexpr size_t kNumRows = 59986052;
constexpr size_t kNumZones = 458;
constexpr size_t kNumKeys = 65536;

struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};

struct AggState {
    int64_t sum_qty_100 = 0;
    int64_t sum_base_price_100 = 0;
    long double sum_disc_price = 0.0L;
    long double sum_charge = 0.0L;
    int64_t sum_discount_100 = 0;
    uint64_t count = 0;
};

inline uint16_t make_key(uint8_t returnflag, uint8_t linestatus) {
    return static_cast<uint16_t>((static_cast<uint16_t>(returnflag) << 8) |
                                 static_cast<uint16_t>(linestatus));
}

inline void aggregate_row(AggState& agg,
                          double quantity,
                          double extendedprice,
                          double discount,
                          double tax) {
    const int64_t qty_100 = static_cast<int64_t>(std::llround(quantity * 100.0));
    const int64_t price_100 = static_cast<int64_t>(std::llround(extendedprice * 100.0));
    const int64_t discount_100 = static_cast<int64_t>(std::llround(discount * 100.0));
    const long double disc_price =
        static_cast<long double>(extendedprice) * (1.0L - static_cast<long double>(discount));

    agg.sum_qty_100 += qty_100;
    agg.sum_base_price_100 += price_100;
    agg.sum_disc_price += disc_price;
    agg.sum_charge += disc_price * (1.0L + static_cast<long double>(tax));
    agg.sum_discount_100 += discount_100;
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

        gendb::MmapColumn<uint8_t> l_returnflag;
        gendb::MmapColumn<uint8_t> l_linestatus;
        gendb::MmapColumn<double> l_quantity;
        gendb::MmapColumn<double> l_extendedprice;
        gendb::MmapColumn<double> l_discount;
        gendb::MmapColumn<double> l_tax;
        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<ZoneMap1I32> shipdate_zonemap;

        {
            GENDB_PHASE("data_loading");
            const std::string lineitem_dir = gendb_dir + "/lineitem";

            l_returnflag.open(lineitem_dir + "/l_returnflag.bin");
            l_linestatus.open(lineitem_dir + "/l_linestatus.bin");
            l_quantity.open(lineitem_dir + "/l_quantity.bin");
            l_extendedprice.open(lineitem_dir + "/l_extendedprice.bin");
            l_discount.open(lineitem_dir + "/l_discount.bin");
            l_tax.open(lineitem_dir + "/l_tax.bin");
            l_shipdate.open(lineitem_dir + "/l_shipdate.bin");
            shipdate_zonemap.open(lineitem_dir + "/indexes/lineitem_shipdate_zonemap.bin");

            gendb::mmap_prefetch_all(
                l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate);
        }

        if (l_shipdate.size() != kNumRows || l_returnflag.size() != kNumRows || l_linestatus.size() != kNumRows ||
            l_quantity.size() != kNumRows || l_extendedprice.size() != kNumRows || l_discount.size() != kNumRows ||
            l_tax.size() != kNumRows) {
            throw std::runtime_error("Unexpected lineitem column size");
        }
        if (shipdate_zonemap.size() != kNumZones) {
            throw std::runtime_error("Unexpected zonemap size");
        }

        const int32_t cutoff = gendb::add_days(gendb::date_str_to_epoch_days("1998-12-01"), -90);
        std::bitset<kNumZones> kept_blocks;
        std::bitset<kNumZones> full_blocks;
        std::vector<uint16_t> qualifying_blocks;
        qualifying_blocks.reserve(kNumZones);

        {
            GENDB_PHASE("dim_filter");
            for (size_t block = 0; block < kNumZones; ++block) {
                const ZoneMap1I32 zone = shipdate_zonemap[block];
                if (zone.min_value > cutoff) {
                    continue;
                }
                kept_blocks.set(block);
                qualifying_blocks.push_back(static_cast<uint16_t>(block));
                if (zone.max_value <= cutoff) {
                    full_blocks.set(block);
                }
            }
        }

        const int nthreads = std::max(1, omp_get_max_threads());
        std::vector<AggState> thread_aggs(static_cast<size_t>(nthreads) * kNumKeys);

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                AggState* local = thread_aggs.data() + static_cast<size_t>(tid) * kNumKeys;

#pragma omp for schedule(dynamic, 1)
                for (int64_t bi = 0; bi < static_cast<int64_t>(qualifying_blocks.size()); ++bi) {
                    const size_t block = qualifying_blocks[static_cast<size_t>(bi)];
                    const size_t start = block * kBlockSize;
                    const size_t end = std::min(start + kBlockSize, l_shipdate.size());

                    const uint8_t* returnflag = l_returnflag.data + start;
                    const uint8_t* linestatus = l_linestatus.data + start;
                    const double* quantity = l_quantity.data + start;
                    const double* extendedprice = l_extendedprice.data + start;
                    const double* discount = l_discount.data + start;
                    const double* tax = l_tax.data + start;

                    if (full_blocks.test(block)) {
                        for (size_t i = start; i < end; ++i) {
                            AggState& agg = local[make_key(returnflag[i - start], linestatus[i - start])];
                            aggregate_row(agg,
                                          quantity[i - start],
                                          extendedprice[i - start],
                                          discount[i - start],
                                          tax[i - start]);
                        }
                        continue;
                    }

                    const int32_t* shipdate = l_shipdate.data + start;
                    for (size_t i = 0, len = end - start; i < len; ++i) {
                        if (shipdate[i] > cutoff) {
                            continue;
                        }
                        AggState& agg = local[make_key(returnflag[i], linestatus[i])];
                        aggregate_row(agg, quantity[i], extendedprice[i], discount[i], tax[i]);
                    }
                }
            }
        }

        std::vector<AggState> global_aggs(kNumKeys);
        {
            GENDB_PHASE("build_joins");
            for (int tid = 0; tid < nthreads; ++tid) {
                const AggState* local = thread_aggs.data() + static_cast<size_t>(tid) * kNumKeys;
                for (size_t key = 0; key < kNumKeys; ++key) {
                    global_aggs[key].sum_qty_100 += local[key].sum_qty_100;
                    global_aggs[key].sum_base_price_100 += local[key].sum_base_price_100;
                    global_aggs[key].sum_disc_price += local[key].sum_disc_price;
                    global_aggs[key].sum_charge += local[key].sum_charge;
                    global_aggs[key].sum_discount_100 += local[key].sum_discount_100;
                    global_aggs[key].count += local[key].count;
                }
            }
        }

        {
            GENDB_PHASE("output");
            const std::string out_path = results_dir + "/Q1.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file");
            }

            std::fprintf(out,
                         "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

            for (size_t key = 0; key < kNumKeys; ++key) {
                const AggState& agg = global_aggs[key];
                if (agg.count == 0) {
                    continue;
                }

                const char returnflag = static_cast<char>(key >> 8);
                const char linestatus = static_cast<char>(key & 0xFF);
                const double count = static_cast<double>(agg.count);

                std::fprintf(out,
                             "%c,%c,%.2f,%.2f,%.2Lf,%.2Lf,%.2f,%.2f,%.2f,%llu\n",
                             returnflag,
                             linestatus,
                             static_cast<double>(agg.sum_qty_100) / 100.0,
                             static_cast<double>(agg.sum_base_price_100) / 100.0,
                             agg.sum_disc_price,
                             agg.sum_charge,
                             static_cast<double>(agg.sum_qty_100) / (count * 100.0),
                             static_cast<double>(agg.sum_base_price_100) / (count * 100.0),
                             static_cast<double>(agg.sum_discount_100) / (count * 100.0),
                             static_cast<unsigned long long>(agg.count));
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
