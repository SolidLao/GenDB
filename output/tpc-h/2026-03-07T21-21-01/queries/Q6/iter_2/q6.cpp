#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

template <typename T>
void require_count(const char* name, const gendb::MmapColumn<T>& col, size_t expected) {
    if (col.size() != expected) {
        throw std::runtime_error(std::string("unexpected element count for ") + name);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    constexpr int32_t kDayBase = 8036;
    constexpr int32_t kShipLo = 8766;
    constexpr int32_t kShipHi = 9131;
    constexpr size_t kOffsetLo = static_cast<size_t>(kShipLo - kDayBase);
    constexpr size_t kOffsetHi = static_cast<size_t>(kShipHi - kDayBase);
    constexpr uint8_t kDiscountLo = 5;
    constexpr uint8_t kDiscountHi = 7;
    constexpr uint8_t kQuantityHi = 24;

    try {
        GENDB_PHASE("total");

        gendb::MmapColumn<uint64_t> offsets;
        gendb::MmapColumn<uint8_t> discount_pct;
        gendb::MmapColumn<uint8_t> quantity_units;
        gendb::MmapColumn<uint32_t> extendedprice_cents;

        uint64_t scan_begin = 0;
        uint64_t scan_end = 0;

        {
            GENDB_PHASE("data_loading");

            const std::string cover_dir =
                gendb_dir + "/column_versions/lineitem.q6_shipdate_cover";
            offsets.open(cover_dir + "/offsets.bin");
            discount_pct.open(cover_dir + "/discount_pct.bin");
            quantity_units.open(cover_dir + "/quantity_units.bin");
            extendedprice_cents.open(cover_dir + "/extendedprice_cents.bin");

            constexpr size_t kExpectedOffsets = 2527;
            require_count("offsets", offsets, kExpectedOffsets);

            const size_t row_count = discount_pct.size();
            require_count("quantity_units", quantity_units, row_count);
            require_count("extendedprice_cents", extendedprice_cents, row_count);

            if (offsets[kExpectedOffsets - 1] != row_count) {
                throw std::runtime_error("offset terminal value does not match payload row count");
            }

            scan_begin = offsets[kOffsetLo];
            scan_end = offsets[kOffsetHi];
            if (scan_begin > scan_end || scan_end > row_count) {
                throw std::runtime_error("invalid shipdate slice boundaries");
            }

            discount_pct.advise_sequential();
            quantity_units.advise_sequential();
            extendedprice_cents.advise_sequential();

            if (scan_end > scan_begin) {
                ::madvise(const_cast<uint8_t*>(discount_pct.data + scan_begin),
                          static_cast<size_t>(scan_end - scan_begin) * sizeof(uint8_t),
                          MADV_WILLNEED);
                ::madvise(const_cast<uint8_t*>(quantity_units.data + scan_begin),
                          static_cast<size_t>(scan_end - scan_begin) * sizeof(uint8_t),
                          MADV_WILLNEED);
                ::madvise(const_cast<uint32_t*>(extendedprice_cents.data + scan_begin),
                          static_cast<size_t>(scan_end - scan_begin) * sizeof(uint32_t),
                          MADV_WILLNEED);
            }
        }

        {
            GENDB_PHASE("dim_filter");
        }

        {
            GENDB_PHASE("build_joins");
        }

        int64_t revenue_1e4 = 0;

        {
            GENDB_PHASE("main_scan");

            const uint8_t* __restrict disc = discount_pct.data;
            const uint8_t* __restrict qty = quantity_units.data;
            const uint32_t* __restrict ext = extendedprice_cents.data;
            const int64_t begin = static_cast<int64_t>(scan_begin);
            const int64_t end = static_cast<int64_t>(scan_end);

            #pragma omp parallel for schedule(static) reduction(+:revenue_1e4)
            for (int64_t i = begin; i < end; ++i) {
                const uint8_t d = disc[i];
                if (d < kDiscountLo || d > kDiscountHi) {
                    continue;
                }
                if (qty[i] >= kQuantityHi) {
                    continue;
                }
                revenue_1e4 += static_cast<int64_t>(ext[i]) * static_cast<int64_t>(d);
            }
        }

        {
            GENDB_PHASE("output");

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q6.csv";
            std::FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }

            const int64_t revenue_cents = revenue_1e4 / 100;
            const int64_t whole = revenue_cents / 100;
            const int64_t frac = std::llabs(revenue_cents % 100);

            std::fprintf(out, "revenue\n");
            std::fprintf(out, "%lld.%02lld\n",
                         static_cast<long long>(whole),
                         static_cast<long long>(frac));
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q6 failed: %s\n", e.what());
        return 1;
    }
}
