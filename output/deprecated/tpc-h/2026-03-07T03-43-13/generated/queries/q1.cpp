#include <array>
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
constexpr uint16_t kKeyAF = (static_cast<uint16_t>('A') << 8) | static_cast<uint16_t>('F');
constexpr uint16_t kKeyNF = (static_cast<uint16_t>('N') << 8) | static_cast<uint16_t>('F');
constexpr uint16_t kKeyNO = (static_cast<uint16_t>('N') << 8) | static_cast<uint16_t>('O');
constexpr uint16_t kKeyRF = (static_cast<uint16_t>('R') << 8) | static_cast<uint16_t>('F');
constexpr uint8_t kInvalidSlot = 0xFFu;

struct AggSlot {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    int64_t sum_disc_price = 0;
    int64_t sum_charge = 0;
    int64_t sum_discount = 0;
    int64_t count = 0;
};

struct alignas(64) ThreadLocalAgg {
    std::array<AggSlot, 4> slots{};
    bool saw_unexpected_key = false;
};

template <typename T>
gendb::MmapColumn<T> mmap_column(const std::string& gendb_dir, const char* relative_path) {
    return gendb::MmapColumn<T>(gendb_dir + "/" + relative_path);
}

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

void check_count(size_t expected, size_t actual, const char* name) {
    if (expected != actual) {
        fail(std::string("row count mismatch for ") + name);
    }
}

void merge_slots(AggSlot& dst, const AggSlot& src) {
    dst.sum_qty += src.sum_qty;
    dst.sum_base_price += src.sum_base_price;
    dst.sum_disc_price += src.sum_disc_price;
    dst.sum_charge += src.sum_charge;
    dst.sum_discount += src.sum_discount;
    dst.count += src.count;
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

        gendb::MmapColumn<uint8_t> l_returnflag;
        gendb::MmapColumn<uint8_t> l_linestatus;
        gendb::MmapColumn<int64_t> l_quantity;
        gendb::MmapColumn<int64_t> l_extendedprice;
        gendb::MmapColumn<int64_t> l_discount;
        gendb::MmapColumn<int64_t> l_tax;
        gendb::MmapColumn<int32_t> l_shipdate;

        std::array<AggSlot, 4> final_slots{};
        bool saw_unexpected_key = false;

        {
            GENDB_PHASE("total");

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

                row_count = l_shipdate.size();
                check_count(row_count, l_returnflag.size(), "lineitem/l_returnflag.bin");
                check_count(row_count, l_linestatus.size(), "lineitem/l_linestatus.bin");
                check_count(row_count, l_quantity.size(), "lineitem/l_quantity.bin");
                check_count(row_count, l_extendedprice.size(), "lineitem/l_extendedprice.bin");
                check_count(row_count, l_discount.size(), "lineitem/l_discount.bin");
                check_count(row_count, l_tax.size(), "lineitem/l_tax.bin");

                gendb::mmap_prefetch_all(
                    l_returnflag,
                    l_linestatus,
                    l_quantity,
                    l_extendedprice,
                    l_discount,
                    l_tax,
                    l_shipdate);
            }

            std::array<uint8_t, 65536> dense_key_lut{};
            {
                GENDB_PHASE("dim_filter");
                dense_key_lut.fill(kInvalidSlot);
                dense_key_lut[kKeyAF] = 0;
                dense_key_lut[kKeyNF] = 1;
                dense_key_lut[kKeyNO] = 2;
                dense_key_lut[kKeyRF] = 3;
            }

            const int num_threads = std::max(1, omp_get_num_procs());
            omp_set_num_threads(num_threads);
            std::vector<ThreadLocalAgg> partials(static_cast<size_t>(num_threads));

            {
                GENDB_PHASE("main_scan");

                const uint8_t* __restrict returnflag = l_returnflag.data;
                const uint8_t* __restrict linestatus = l_linestatus.data;
                const int64_t* __restrict quantity = l_quantity.data;
                const int64_t* __restrict extendedprice = l_extendedprice.data;
                const int64_t* __restrict discount = l_discount.data;
                const int64_t* __restrict tax = l_tax.data;
                const int32_t* __restrict shipdate = l_shipdate.data;
                const uint8_t* __restrict lut = dense_key_lut.data();

#pragma omp parallel num_threads(num_threads)
                {
                    const int tid = omp_get_thread_num();
                    ThreadLocalAgg& local = partials[static_cast<size_t>(tid)];

                    const size_t begin = (row_count * static_cast<size_t>(tid)) / static_cast<size_t>(num_threads);
                    const size_t end = (row_count * static_cast<size_t>(tid + 1)) / static_cast<size_t>(num_threads);

                    for (size_t row = begin; row < end; ++row) {
                        if (__builtin_expect(shipdate[row] > kShipdateCutoff, 0)) {
                            continue;
                        }

                        const uint16_t key = static_cast<uint16_t>(
                            (static_cast<uint16_t>(returnflag[row]) << 8) |
                            static_cast<uint16_t>(linestatus[row]));
                        const uint8_t slot_id = lut[key];
                        if (__builtin_expect(slot_id >= 4, 0)) {
                            local.saw_unexpected_key = true;
                            continue;
                        }

                        AggSlot& slot = local.slots[slot_id];
                        const int64_t qty = quantity[row];
                        const int64_t price = extendedprice[row];
                        const int64_t disc = discount[row];
                        const int64_t tx = tax[row];
                        const int64_t disc_factor = 100 - disc;
                        const int64_t discounted_price = price * disc_factor;

                        slot.sum_qty += qty;
                        slot.sum_base_price += price;
                        slot.sum_disc_price += discounted_price;
                        slot.sum_charge += discounted_price * (100 + tx);
                        slot.sum_discount += disc;
                        ++slot.count;
                    }
                }
            }

            {
                GENDB_PHASE("build_joins");
                for (const ThreadLocalAgg& partial : partials) {
                    saw_unexpected_key = saw_unexpected_key || partial.saw_unexpected_key;
                    for (size_t slot_id = 0; slot_id < final_slots.size(); ++slot_id) {
                        merge_slots(final_slots[slot_id], partial.slots[slot_id]);
                    }
                }
            }

            if (saw_unexpected_key) {
                fail("encountered unexpected (l_returnflag, l_linestatus) combination");
            }

            {
                GENDB_PHASE("output");
                const std::string output_path = results_dir + "/Q1.csv";
                FILE* out = std::fopen(output_path.c_str(), "w");
                if (out == nullptr) {
                    fail("failed to open output file");
                }

                std::fprintf(
                    out,
                    "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

                constexpr std::array<uint16_t, 4> ordered_keys = {kKeyAF, kKeyNF, kKeyNO, kKeyRF};
                for (size_t slot_id = 0; slot_id < ordered_keys.size(); ++slot_id) {
                    const AggSlot& slot = final_slots[slot_id];
                    if (slot.count == 0) {
                        continue;
                    }

                    const long double count = static_cast<long double>(slot.count);
                    const long double sum_qty = static_cast<long double>(slot.sum_qty) / 100.0L;
                    const long double sum_base_price = static_cast<long double>(slot.sum_base_price) / 100.0L;
                    const long double sum_disc_price = static_cast<long double>(slot.sum_disc_price) / 10000.0L;
                    const long double sum_charge = static_cast<long double>(slot.sum_charge) / 1000000.0L;
                    const long double avg_qty = static_cast<long double>(slot.sum_qty) / (100.0L * count);
                    const long double avg_price = static_cast<long double>(slot.sum_base_price) / (100.0L * count);
                    const long double avg_disc = static_cast<long double>(slot.sum_discount) / (100.0L * count);
                    const uint16_t key = ordered_keys[slot_id];

                    std::fprintf(
                        out,
                        "%c,%c,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%lld\n",
                        static_cast<char>(key >> 8),
                        static_cast<char>(key & 0xFFu),
                        sum_qty,
                        sum_base_price,
                        sum_disc_price,
                        sum_charge,
                        avg_qty,
                        avg_price,
                        avg_disc,
                        static_cast<long long>(slot.count));
                }

                std::fclose(out);
            }
        }

        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "Error: %s\n", error.what());
        return 1;
    }
}
