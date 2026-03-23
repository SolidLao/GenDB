#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kShipdateCutoff = 10471;
constexpr size_t kBlockSize = 100000;
constexpr uint8_t kSkipBlock = 0;
constexpr uint8_t kAcceptBlock = 1;
constexpr uint8_t kCheckRows = 2;

struct AggSlot {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    __int128 sum_disc_price = 0;
    __int128 sum_charge = 0;
    int64_t sum_discount = 0;
    uint64_t count = 0;
};

struct LocalState {
    std::unique_ptr<AggSlot[]> slots;
    std::vector<uint16_t> touched;

    LocalState() : slots(std::make_unique<AggSlot[]>(65536)) {
        touched.reserve(16);
    }
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

            std::vector<uint8_t> block_modes(shipdate_mins.size(), kCheckRows);
            {
                GENDB_PHASE("dim_filter");
                for (size_t block = 0; block < shipdate_mins.size(); ++block) {
                    if (shipdate_mins[block] > kShipdateCutoff) {
                        block_modes[block] = kSkipBlock;
                    } else if (shipdate_maxs[block] <= kShipdateCutoff) {
                        block_modes[block] = kAcceptBlock;
                    } else {
                        block_modes[block] = kCheckRows;
                    }
                }
            }

            {
                GENDB_PHASE("build_joins");
            }

            const int num_threads = std::max(1, std::min(64, omp_get_num_procs()));
            omp_set_num_threads(num_threads);
            std::vector<LocalState> locals(static_cast<size_t>(num_threads));

            {
                GENDB_PHASE("main_scan");
                #pragma omp parallel
                {
                    const int thread_id = omp_get_thread_num();
                    LocalState& local = locals[static_cast<size_t>(thread_id)];

                    #pragma omp for schedule(static)
                    for (size_t block = 0; block < shipdate_mins.size(); ++block) {
                        const uint8_t mode = block_modes[block];
                        if (mode == kSkipBlock) {
                            continue;
                        }

                        const size_t begin = block * kBlockSize;
                        const size_t end = std::min(begin + kBlockSize, row_count);

                        if (mode == kAcceptBlock) {
                            for (size_t row = begin; row < end; ++row) {
                                const uint16_t key = static_cast<uint16_t>(
                                    (static_cast<uint16_t>(l_returnflag[row]) << 8) | l_linestatus[row]);
                                AggSlot& slot = local.slots[key];
                                if (slot.count == 0) {
                                    local.touched.push_back(key);
                                }

                                const int64_t quantity = l_quantity[row];
                                const int64_t extended_price = l_extendedprice[row];
                                const int64_t discount = l_discount[row];
                                const int64_t tax = l_tax[row];

                                slot.sum_qty += quantity;
                                slot.sum_base_price += extended_price;
                                slot.sum_disc_price += static_cast<__int128>(extended_price) * (100 - discount);
                                slot.sum_charge += static_cast<__int128>(extended_price) * (100 - discount) * (100 + tax);
                                slot.sum_discount += discount;
                                ++slot.count;
                            }
                        } else {
                            for (size_t row = begin; row < end; ++row) {
                                if (l_shipdate[row] > kShipdateCutoff) {
                                    continue;
                                }

                                const uint16_t key = static_cast<uint16_t>(
                                    (static_cast<uint16_t>(l_returnflag[row]) << 8) | l_linestatus[row]);
                                AggSlot& slot = local.slots[key];
                                if (slot.count == 0) {
                                    local.touched.push_back(key);
                                }

                                const int64_t quantity = l_quantity[row];
                                const int64_t extended_price = l_extendedprice[row];
                                const int64_t discount = l_discount[row];
                                const int64_t tax = l_tax[row];

                                slot.sum_qty += quantity;
                                slot.sum_base_price += extended_price;
                                slot.sum_disc_price += static_cast<__int128>(extended_price) * (100 - discount);
                                slot.sum_charge += static_cast<__int128>(extended_price) * (100 - discount) * (100 + tax);
                                slot.sum_discount += discount;
                                ++slot.count;
                            }
                        }
                    }
                }
            }

            std::array<AggSlot, 65536> final_slots{};
            std::vector<uint16_t> populated_keys;
            populated_keys.reserve(16);

            for (const LocalState& local : locals) {
                for (uint16_t key : local.touched) {
                    const AggSlot& src = local.slots[key];
                    AggSlot& dst = final_slots[key];
                    if (dst.count == 0) {
                        populated_keys.push_back(key);
                    }
                    dst.sum_qty += src.sum_qty;
                    dst.sum_base_price += src.sum_base_price;
                    dst.sum_disc_price += src.sum_disc_price;
                    dst.sum_charge += src.sum_charge;
                    dst.sum_discount += src.sum_discount;
                    dst.count += src.count;
                }
            }

            std::sort(populated_keys.begin(), populated_keys.end());

            {
                GENDB_PHASE("output");
                const std::string output_path = results_dir + "/Q1.csv";
                FILE* out = std::fopen(output_path.c_str(), "w");
                if (!out) {
                    fail("failed to open output file");
                }

                std::fprintf(
                    out,
                    "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

                for (uint16_t key : populated_keys) {
                    const AggSlot& slot = final_slots[key];
                    const long double count = static_cast<long double>(slot.count);
                    const long double sum_qty = static_cast<long double>(slot.sum_qty) / 100.0L;
                    const long double sum_base_price = static_cast<long double>(slot.sum_base_price) / 100.0L;
                    const long double sum_disc_price = to_ld(slot.sum_disc_price) / 10000.0L;
                    const long double sum_charge = to_ld(slot.sum_charge) / 1000000.0L;
                    const long double avg_qty = static_cast<long double>(slot.sum_qty) / (100.0L * count);
                    const long double avg_price = static_cast<long double>(slot.sum_base_price) / (100.0L * count);
                    const long double avg_disc = static_cast<long double>(slot.sum_discount) / (100.0L * count);

                    std::fprintf(
                        out,
                        "%c,%c,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%llu\n",
                        static_cast<char>(key >> 8),
                        static_cast<char>(key & 0xFFu),
                        sum_qty,
                        sum_base_price,
                        sum_disc_price,
                        sum_charge,
                        avg_qty,
                        avg_price,
                        avg_disc,
                        static_cast<unsigned long long>(slot.count));
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
