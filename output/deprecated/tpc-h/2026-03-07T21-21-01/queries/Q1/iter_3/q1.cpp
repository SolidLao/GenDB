#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
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
constexpr size_t kBlockSize = 131072;
constexpr uint8_t kEmptySlot = 0xFF;
constexpr uint8_t kMaxGroups = 8;

struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};

struct AggState {
    long double sum_qty = 0.0L;
    long double sum_base_price = 0.0L;
    long double sum_disc_price = 0.0L;
    long double sum_charge = 0.0L;
    long double sum_disc = 0.0L;
    uint64_t count = 0;
};

struct alignas(64) ThreadAgg {
    std::array<uint8_t, 65536> key_to_slot{};
    std::array<AggState, kMaxGroups> slots{};
    std::array<uint16_t, kMaxGroups> populated_keys{};
    uint8_t populated_count = 0;

    ThreadAgg() {
        std::memset(key_to_slot.data(), 0xFF, key_to_slot.size());
    }

    inline AggState& lookup_or_insert(uint16_t key) {
        uint8_t slot = key_to_slot[key];
        if (slot != kEmptySlot) {
            return slots[slot];
        }

        slot = populated_count;
        if (slot >= kMaxGroups) {
            throw std::runtime_error("ThreadAgg capacity exceeded");
        }
        key_to_slot[key] = slot;
        populated_keys[slot] = key;
        ++populated_count;
        slots[slot] = AggState{};
        return slots[slot];
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

    agg.sum_qty += static_cast<long double>(quantity);
    agg.sum_base_price += static_cast<long double>(extendedprice);
    agg.sum_disc_price += disc_price;
    agg.sum_charge += disc_price * (1.0L + static_cast<long double>(tax));
    agg.sum_disc += static_cast<long double>(discount);
    ++agg.count;
}

inline void scan_block_full(ThreadAgg& local,
                            size_t begin,
                            size_t end,
                            const uint8_t* __restrict__ returnflag,
                            const uint8_t* __restrict__ linestatus,
                            const double* __restrict__ quantity,
                            const double* __restrict__ extendedprice,
                            const double* __restrict__ discount,
                            const double* __restrict__ tax) {
    for (size_t i = begin; i < end; ++i) {
        AggState& agg = local.lookup_or_insert(pack_key(returnflag[i], linestatus[i]));
        aggregate_row(agg, quantity[i], extendedprice[i], discount[i], tax[i]);
    }
}

inline void scan_block_mixed(ThreadAgg& local,
                             size_t begin,
                             size_t end,
                             int32_t cutoff,
                             const int32_t* __restrict__ shipdate,
                             const uint8_t* __restrict__ returnflag,
                             const uint8_t* __restrict__ linestatus,
                             const double* __restrict__ quantity,
                             const double* __restrict__ extendedprice,
                             const double* __restrict__ discount,
                             const double* __restrict__ tax) {
    for (size_t i = begin; i < end; ++i) {
        if (shipdate[i] > cutoff) {
            continue;
        }
        AggState& agg = local.lookup_or_insert(pack_key(returnflag[i], linestatus[i]));
        aggregate_row(agg, quantity[i], extendedprice[i], discount[i], tax[i]);
    }
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
        gendb::MmapColumn<ZoneMap1I32> shipdate_zonemap;

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
            shipdate_zonemap.open(lineitem_dir + "/indexes/lineitem_shipdate_zonemap.bin");

            gendb::mmap_prefetch_all(l_shipdate,
                                     l_returnflag,
                                     l_linestatus,
                                     l_quantity,
                                     l_extendedprice,
                                     l_discount,
                                     l_tax,
                                     shipdate_zonemap);
        }

        if (l_shipdate.size() != kExpectedRows || l_returnflag.size() != kExpectedRows ||
            l_linestatus.size() != kExpectedRows || l_quantity.size() != kExpectedRows ||
            l_extendedprice.size() != kExpectedRows || l_discount.size() != kExpectedRows ||
            l_tax.size() != kExpectedRows) {
            throw std::runtime_error("Unexpected lineitem column size");
        }

        const size_t n_rows = l_shipdate.size();
        const size_t n_blocks = (n_rows + kBlockSize - 1) / kBlockSize;
        if (shipdate_zonemap.size() != n_blocks) {
            throw std::runtime_error("Unexpected shipdate zonemap size");
        }

        const int32_t cutoff =
            gendb::add_days(gendb::date_str_to_epoch_days("1998-12-01"), -90);

        std::vector<uint32_t> full_blocks;
        std::vector<uint32_t> mixed_blocks;
        full_blocks.reserve(n_blocks);
        mixed_blocks.reserve(n_blocks);

        {
            GENDB_PHASE("dim_filter");
            const ZoneMap1I32* __restrict__ zones = shipdate_zonemap.data;
            for (size_t block = 0; block < n_blocks; ++block) {
                if (zones[block].min_value > cutoff) {
                    continue;
                }
                if (zones[block].max_value <= cutoff) {
                    full_blocks.push_back(static_cast<uint32_t>(block));
                } else {
                    mixed_blocks.push_back(static_cast<uint32_t>(block));
                }
            }
        }

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<ThreadAgg> thread_aggs(static_cast<size_t>(max_threads));
        ThreadAgg global_agg;

        {
            GENDB_PHASE("main_scan");
            const int32_t* __restrict__ shipdate = l_shipdate.data;
            const uint8_t* __restrict__ returnflag = l_returnflag.data;
            const uint8_t* __restrict__ linestatus = l_linestatus.data;
            const double* __restrict__ quantity = l_quantity.data;
            const double* __restrict__ extendedprice = l_extendedprice.data;
            const double* __restrict__ discount = l_discount.data;
            const double* __restrict__ tax = l_tax.data;

#pragma omp parallel num_threads(max_threads)
            {
                ThreadAgg& local = thread_aggs[static_cast<size_t>(omp_get_thread_num())];

#pragma omp for schedule(dynamic, 1) nowait
                for (size_t idx = 0; idx < full_blocks.size(); ++idx) {
                    const size_t block = full_blocks[idx];
                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, n_rows);
                    scan_block_full(local,
                                    begin,
                                    end,
                                    returnflag,
                                    linestatus,
                                    quantity,
                                    extendedprice,
                                    discount,
                                    tax);
                }

#pragma omp for schedule(dynamic, 1)
                for (size_t idx = 0; idx < mixed_blocks.size(); ++idx) {
                    const size_t block = mixed_blocks[idx];
                    const size_t begin = block * kBlockSize;
                    const size_t end = std::min(begin + kBlockSize, n_rows);
                    scan_block_mixed(local,
                                     begin,
                                     end,
                                     cutoff,
                                     shipdate,
                                     returnflag,
                                     linestatus,
                                     quantity,
                                     extendedprice,
                                     discount,
                                     tax);
                }
            }
        }

        {
            GENDB_PHASE("build_joins");
            for (const ThreadAgg& local : thread_aggs) {
                for (uint8_t i = 0; i < local.populated_count; ++i) {
                    const uint16_t key = local.populated_keys[i];
                    const AggState& src = local.slots[i];
                    AggState& dst = global_agg.lookup_or_insert(key);
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
            std::vector<uint16_t> keys;
            keys.reserve(global_agg.populated_count);
            for (uint8_t i = 0; i < global_agg.populated_count; ++i) {
                if (global_agg.slots[i].count != 0) {
                    keys.push_back(global_agg.populated_keys[i]);
                }
            }

            std::sort(keys.begin(), keys.end());

            const std::string out_path = results_dir + "/Q1.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file");
            }

            std::fprintf(out,
                         "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

            for (uint16_t key : keys) {
                const AggState& agg = global_agg.slots[global_agg.key_to_slot[key]];
                const long double count = static_cast<long double>(agg.count);
                std::fprintf(out,
                             "%c,%c,%.2Lf,%.2Lf,%.4Lf,%.6Lf,%.2Lf,%.2Lf,%.2Lf,%llu\n",
                             unpack_returnflag(key),
                             unpack_linestatus(key),
                             agg.sum_qty,
                             agg.sum_base_price,
                             agg.sum_disc_price,
                             agg.sum_charge,
                             agg.sum_qty / count,
                             agg.sum_base_price / count,
                             agg.sum_disc / count,
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
