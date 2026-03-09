#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

struct ZoneMap3 {
    int32_t shipdate_min;
    int32_t shipdate_max;
    double discount_min;
    double discount_max;
    double quantity_min;
    double quantity_max;
};

template <typename T>
void ensure_same_rows(const char* name, const gendb::MmapColumn<T>& col, size_t rows) {
    if (col.size() != rows) {
        throw std::runtime_error(std::string("Row-count mismatch for ") + name);
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

    constexpr int32_t kShipdateLo = 8766;
    constexpr int32_t kShipdateHi = 9131;
    constexpr double kDiscountLo = 0.05;
    constexpr double kDiscountHi = 0.07;
    constexpr double kQuantityHi = 24.0;
    constexpr size_t kBlockSize = 131072;

    try {
        GENDB_PHASE("total");

        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<double> l_discount;
        gendb::MmapColumn<double> l_quantity;
        gendb::MmapColumn<double> l_extendedprice;
        gendb::MmapColumn<ZoneMap3> q6_zonemap;
        size_t row_count = 0;

        {
            GENDB_PHASE("data_loading");

            const std::string lineitem_dir = gendb_dir + "/lineitem";
            l_shipdate.open(lineitem_dir + "/l_shipdate.bin");
            l_discount.open(lineitem_dir + "/l_discount.bin");
            l_quantity.open(lineitem_dir + "/l_quantity.bin");
            l_extendedprice.open(lineitem_dir + "/l_extendedprice.bin");
            q6_zonemap.open(lineitem_dir + "/indexes/lineitem_q6_zonemap.bin");

            row_count = l_shipdate.size();
            ensure_same_rows("l_discount", l_discount, row_count);
            ensure_same_rows("l_quantity", l_quantity, row_count);
            ensure_same_rows("l_extendedprice", l_extendedprice, row_count);

            const size_t expected_blocks = (row_count + kBlockSize - 1) / kBlockSize;
            if (q6_zonemap.size() != expected_blocks) {
                throw std::runtime_error("Unexpected Q6 zone-map entry count");
            }

            l_shipdate.advise_sequential();
            l_discount.advise_sequential();
            l_quantity.advise_sequential();
            l_extendedprice.advise_sequential();
            q6_zonemap.advise_sequential();
        }

        std::vector<uint32_t> qualifying_blocks;
        qualifying_blocks.reserve(q6_zonemap.size());

        {
            GENDB_PHASE("dim_filter");

            const ZoneMap3* zones = q6_zonemap.data;
            const size_t zone_count = q6_zonemap.size();
            for (size_t block = 0; block < zone_count; ++block) {
                const ZoneMap3& z = zones[block];
                if (z.shipdate_max < kShipdateLo || z.shipdate_min >= kShipdateHi) {
                    continue;
                }
                if (z.discount_max < kDiscountLo || z.discount_min > kDiscountHi) {
                    continue;
                }
                if (z.quantity_min >= kQuantityHi) {
                    continue;
                }
                qualifying_blocks.push_back(static_cast<uint32_t>(block));
            }
        }

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<double> thread_sums(static_cast<size_t>(max_threads), 0.0);

        {
            GENDB_PHASE("build_joins");
        }

        {
            GENDB_PHASE("main_scan");

            const int32_t* __restrict shipdate = l_shipdate.data;
            const double* __restrict discount = l_discount.data;
            const double* __restrict quantity = l_quantity.data;
            const double* __restrict extendedprice = l_extendedprice.data;
            const uint32_t* __restrict blocks = qualifying_blocks.data();
            const size_t block_count = qualifying_blocks.size();

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                double local_sum = 0.0;

                #pragma omp for schedule(dynamic, 8) nowait
                for (size_t i = 0; i < block_count; ++i) {
                    const size_t block = blocks[i];
                    const size_t row_begin = block * kBlockSize;
                    const size_t row_end = std::min(row_begin + kBlockSize, row_count);

                    for (size_t row = row_begin; row < row_end; ++row) {
                        const int32_t ship = shipdate[row];
                        const double disc = discount[row];
                        const double qty = quantity[row];
                        if (ship >= kShipdateLo && ship < kShipdateHi &&
                            disc >= kDiscountLo && disc <= kDiscountHi &&
                            qty < kQuantityHi) {
                            local_sum += extendedprice[row] * disc;
                        }
                    }
                }

                thread_sums[static_cast<size_t>(tid)] = local_sum;
            }
        }

        double revenue = 0.0;
        {
            GENDB_PHASE("build_joins");
            for (double partial : thread_sums) {
                revenue += partial;
            }
        }

        {
            GENDB_PHASE("output");
            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q6.csv";
            std::FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file");
            }
            std::fprintf(out, "revenue\n");
            std::fprintf(out, "%.4f\n", revenue);
            std::fclose(out);
            std::printf("revenue: %.2f\n", revenue);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q6 failed: %s\n", e.what());
        return 1;
    }
}
