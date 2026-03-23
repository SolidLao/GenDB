#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kShipdateCutoff = 10471;
constexpr size_t kBlockSize = 100000;

struct AggSlot {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    __int128 sum_disc_price = 0;
    __int128 sum_charge = 0;
    int64_t sum_discount = 0;
    uint64_t count = 0;
};

struct HashEntry {
    uint16_t key = 0;
    bool used = false;
    AggSlot agg{};
};

struct CompactAggMap {
    static constexpr size_t kCapacity = 16;
    std::array<HashEntry, kCapacity> entries{};

    AggSlot& find_or_insert(uint16_t key) {
        size_t idx = (static_cast<uint32_t>(key) * 2654435761u) & (kCapacity - 1);
        for (size_t probe = 0; probe < kCapacity; ++probe) {
            HashEntry& entry = entries[(idx + probe) & (kCapacity - 1)];
            if (!entry.used) {
                entry.used = true;
                entry.key = key;
                entry.agg = AggSlot{};
                return entry.agg;
            }
            if (entry.key == key) {
                return entry.agg;
            }
        }
        throw std::runtime_error("CompactAggMap capacity exceeded");
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (const HashEntry& entry : entries) {
            if (entry.used) {
                fn(entry.key, entry.agg);
            }
        }
    }
};

struct alignas(64) LocalState {
    CompactAggMap map;
};

template <typename T>
gendb::MmapColumn<T> mmap_column(const std::string& gendb_dir, const char* relative_path) {
    return gendb::MmapColumn<T>(gendb_dir + "/" + relative_path);
}

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

inline void check_count(size_t expected, size_t actual, const char* name) {
    if (expected != actual) {
        fail(std::string("row count mismatch for ") + name);
    }
}

inline long double to_ld(__int128 value) {
    return static_cast<long double>(value);
}

inline void accumulate_row(
    CompactAggMap& map,
    uint8_t returnflag,
    uint8_t linestatus,
    int64_t quantity,
    int64_t extended_price,
    int64_t discount,
    int64_t tax) {

    const uint16_t key = static_cast<uint16_t>((static_cast<uint16_t>(returnflag) << 8) | linestatus);
    AggSlot& slot = map.find_or_insert(key);
    slot.sum_qty += quantity;
    slot.sum_base_price += extended_price;
    slot.sum_disc_price += static_cast<__int128>(extended_price) * static_cast<__int128>(100 - discount);
    slot.sum_charge += static_cast<__int128>(extended_price) * static_cast<__int128>(100 - discount) * static_cast<__int128>(100 + tax);
    slot.sum_discount += discount;
    ++slot.count;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        std::filesystem::create_directories(results_dir);

        {
            GENDB_PHASE("total");

            gendb::MmapColumn<uint8_t> l_returnflag;
            gendb::MmapColumn<uint8_t> l_linestatus;
            gendb::MmapColumn<int64_t> l_quantity;
            gendb::MmapColumn<int64_t> l_extendedprice;
            gendb::MmapColumn<int64_t> l_discount;
            gendb::MmapColumn<int64_t> l_tax;
            gendb::MmapColumn<int32_t> l_shipdate;
            gendb::MmapColumn<int32_t> shipdate_mins;
            gendb::MmapColumn<int32_t> shipdate_maxs;
            size_t row_count = 0;

            {
                GENDB_PHASE("data_loading");
                l_returnflag = mmap_column<uint8_t>(gendb_dir, "lineitem/l_returnflag.bin");
                l_linestatus = mmap_column<uint8_t>(gendb_dir, "lineitem/l_linestatus.bin");
                l_quantity = mmap_column<int64_t>(gendb_dir, "lineitem/l_quantity.bin");
                l_extendedprice = mmap_column<int64_t>(gendb_dir, "lineitem/l_extendedprice.bin");
                l_discount = mmap_column<int64_t>(gendb_dir, "lineitem/l_discount.bin");
                l_tax = mmap_column<int64_t>(gendb_dir, "lineitem/l_tax.bin");
                l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem/l_shipdate.bin");
                shipdate_mins = mmap_column<int32_t>(gendb_dir, "lineitem/indexes/lineitem_shipdate_zone_map.mins.bin");
                shipdate_maxs = mmap_column<int32_t>(gendb_dir, "lineitem/indexes/lineitem_shipdate_zone_map.maxs.bin");

                row_count = l_shipdate.size();
                check_count(row_count, l_returnflag.size(), "lineitem/l_returnflag.bin");
                check_count(row_count, l_linestatus.size(), "lineitem/l_linestatus.bin");
                check_count(row_count, l_quantity.size(), "lineitem/l_quantity.bin");
                check_count(row_count, l_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(row_count, l_discount.size(), "lineitem/l_discount.bin");
                check_count(row_count, l_tax.size(), "lineitem/l_tax.bin");
                if (shipdate_mins.size() != shipdate_maxs.size()) {
                    fail("zone map mins/maxs size mismatch");
                }
                const size_t expected_blocks = (row_count + kBlockSize - 1) / kBlockSize;
                check_count(expected_blocks, shipdate_mins.size(), "lineitem shipdate zone map");

                gendb::mmap_prefetch_all(
                    l_returnflag,
                    l_linestatus,
                    l_quantity,
                    l_extendedprice,
                    l_discount,
                    l_tax,
                    l_shipdate,
                    shipdate_mins,
                    shipdate_maxs);
            }

            std::vector<uint32_t> accept_blocks;
            std::vector<uint32_t> boundary_blocks;
            accept_blocks.reserve(shipdate_mins.size());
            boundary_blocks.reserve(shipdate_mins.size());

            {
                GENDB_PHASE("dim_filter");
                for (uint32_t block = 0; block < shipdate_mins.size(); ++block) {
                    if (shipdate_mins[block] > kShipdateCutoff) {
                        continue;
                    }
                    if (shipdate_maxs[block] <= kShipdateCutoff) {
                        accept_blocks.push_back(block);
                    } else {
                        boundary_blocks.push_back(block);
                    }
                }
            }

            const int num_threads = std::max(1, omp_get_num_procs());
            omp_set_num_threads(num_threads);
            std::vector<LocalState> locals(static_cast<size_t>(num_threads));

            {
                GENDB_PHASE("main_scan");

                const uint8_t* __restrict__ returnflag = l_returnflag.data;
                const uint8_t* __restrict__ linestatus = l_linestatus.data;
                const int64_t* __restrict__ quantity = l_quantity.data;
                const int64_t* __restrict__ extendedprice = l_extendedprice.data;
                const int64_t* __restrict__ discount = l_discount.data;
                const int64_t* __restrict__ tax = l_tax.data;
                const int32_t* __restrict__ shipdate = l_shipdate.data;

                std::atomic<size_t> next_accept{0};
                std::atomic<size_t> next_boundary{0};

                #pragma omp parallel
                {
                    LocalState& local = locals[static_cast<size_t>(omp_get_thread_num())];

                    while (true) {
                        const size_t morsel = next_accept.fetch_add(1, std::memory_order_relaxed);
                        if (morsel >= accept_blocks.size()) {
                            break;
                        }

                        const size_t block = accept_blocks[morsel];
                        const size_t begin = block * kBlockSize;
                        const size_t end = std::min(begin + kBlockSize, row_count);

                        for (size_t row = begin; row < end; ++row) {
                            accumulate_row(
                                local.map,
                                returnflag[row],
                                linestatus[row],
                                quantity[row],
                                extendedprice[row],
                                discount[row],
                                tax[row]);
                        }
                    }

                    while (true) {
                        const size_t morsel = next_boundary.fetch_add(1, std::memory_order_relaxed);
                        if (morsel >= boundary_blocks.size()) {
                            break;
                        }

                        const size_t block = boundary_blocks[morsel];
                        const size_t begin = block * kBlockSize;
                        const size_t end = std::min(begin + kBlockSize, row_count);

                        for (size_t row = begin; row < end; ++row) {
                            if (shipdate[row] > kShipdateCutoff) {
                                continue;
                            }

                            accumulate_row(
                                local.map,
                                returnflag[row],
                                linestatus[row],
                                quantity[row],
                                extendedprice[row],
                                discount[row],
                                tax[row]);
                        }
                    }
                }
            }

            CompactAggMap final_map;
            {
                GENDB_PHASE("build_joins");
                for (const LocalState& local : locals) {
                    local.map.for_each([&](uint16_t key, const AggSlot& src) {
                        AggSlot& dst = final_map.find_or_insert(key);
                        dst.sum_qty += src.sum_qty;
                        dst.sum_base_price += src.sum_base_price;
                        dst.sum_disc_price += src.sum_disc_price;
                        dst.sum_charge += src.sum_charge;
                        dst.sum_discount += src.sum_discount;
                        dst.count += src.count;
                    });
                }
            }

            {
                GENDB_PHASE("output");

                struct OutputRow {
                    uint16_t key;
                    AggSlot agg;
                };

                std::vector<OutputRow> rows;
                rows.reserve(8);
                final_map.for_each([&](uint16_t key, const AggSlot& agg) {
                    rows.push_back(OutputRow{key, agg});
                });
                std::sort(rows.begin(), rows.end(), [](const OutputRow& left, const OutputRow& right) {
                    return left.key < right.key;
                });

                const std::string output_path = results_dir + "/Q1.csv";
                FILE* out = std::fopen(output_path.c_str(), "w");
                if (!out) {
                    fail("failed to open output file");
                }

                std::fprintf(
                    out,
                    "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

                for (const OutputRow& row : rows) {
                    const long double count = static_cast<long double>(row.agg.count);
                    const long double sum_qty = static_cast<long double>(row.agg.sum_qty) / 100.0L;
                    const long double sum_base_price = static_cast<long double>(row.agg.sum_base_price) / 100.0L;
                    const long double sum_disc_price = to_ld(row.agg.sum_disc_price) / 10000.0L;
                    const long double sum_charge = to_ld(row.agg.sum_charge) / 1000000.0L;
                    const long double avg_qty = static_cast<long double>(row.agg.sum_qty) / (100.0L * count);
                    const long double avg_price = static_cast<long double>(row.agg.sum_base_price) / (100.0L * count);
                    const long double avg_disc = static_cast<long double>(row.agg.sum_discount) / (100.0L * count);

                    std::fprintf(
                        out,
                        "%c,%c,%.2Lf,%.2Lf,%.4Lf,%.6Lf,%.2Lf,%.2Lf,%.2Lf,%llu\n",
                        static_cast<char>(row.key >> 8),
                        static_cast<char>(row.key & 0xFFu),
                        sum_qty,
                        sum_base_price,
                        sum_disc_price,
                        sum_charge,
                        avg_qty,
                        avg_price,
                        avg_disc,
                        static_cast<unsigned long long>(row.agg.count));
                }

                std::fclose(out);
            }
        }
    } catch (const std::exception& error) {
        std::fprintf(stderr, "Error: %s\n", error.what());
        return 1;
    }

    return 0;
}
