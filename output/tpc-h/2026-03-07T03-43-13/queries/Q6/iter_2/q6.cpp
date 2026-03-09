#include <algorithm>
#include <bitset>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kBlockRows = 100000;
constexpr size_t kExpectedBlocks = 600;

constexpr int32_t kShipdateLo = 8766;
constexpr int32_t kShipdateHi = 9131;
constexpr int64_t kDiscountLo = 5;
constexpr int64_t kDiscountHi = 7;
constexpr int64_t kQuantityHi = 2400;
constexpr int32_t kShipdateSpan = kShipdateHi - kShipdateLo;
constexpr int64_t kDiscountSpan = kDiscountHi - kDiscountLo;

struct alignas(64) PaddedSum {
    int64_t value;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

inline bool shipdate_pass(const int32_t shipdate) {
    return static_cast<uint32_t>(shipdate - kShipdateLo) < static_cast<uint32_t>(kShipdateSpan);
}

inline bool discount_pass(const int64_t discount) {
    return static_cast<uint64_t>(discount - kDiscountLo) <= static_cast<uint64_t>(kDiscountSpan);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::filesystem::path gendb_dir = argv[1];
        const std::filesystem::path results_dir = argv[2];
        std::filesystem::create_directories(results_dir);

        GENDB_PHASE("total");

        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<int64_t> l_discount;
        gendb::MmapColumn<int64_t> l_quantity;
        gendb::MmapColumn<int64_t> l_extendedprice;

        gendb::MmapColumn<int32_t> shipdate_mins;
        gendb::MmapColumn<int32_t> shipdate_maxs;
        gendb::MmapColumn<int64_t> discount_mins;
        gendb::MmapColumn<int64_t> discount_maxs;
        gendb::MmapColumn<int64_t> quantity_mins;
        gendb::MmapColumn<int64_t> quantity_maxs;

        {
            GENDB_PHASE("data_loading");
            const std::filesystem::path lineitem_dir = gendb_dir / "lineitem";
            const std::filesystem::path index_dir = lineitem_dir / "indexes";

            l_shipdate.open((lineitem_dir / "l_shipdate.bin").string());
            l_discount.open((lineitem_dir / "l_discount.bin").string());
            l_quantity.open((lineitem_dir / "l_quantity.bin").string());
            l_extendedprice.open((lineitem_dir / "l_extendedprice.bin").string());

            shipdate_mins.open((index_dir / "lineitem_shipdate_zone_map.mins.bin").string());
            shipdate_maxs.open((index_dir / "lineitem_shipdate_zone_map.maxs.bin").string());
            discount_mins.open((index_dir / "lineitem_discount_zone_map.mins.bin").string());
            discount_maxs.open((index_dir / "lineitem_discount_zone_map.maxs.bin").string());
            quantity_mins.open((index_dir / "lineitem_quantity_zone_map.mins.bin").string());
            quantity_maxs.open((index_dir / "lineitem_quantity_zone_map.maxs.bin").string());

            gendb::mmap_prefetch_all(
                l_shipdate,
                l_discount,
                l_quantity,
                l_extendedprice,
                shipdate_mins,
                shipdate_maxs,
                discount_mins,
                discount_maxs,
                quantity_mins,
                quantity_maxs);
        }

        const size_t n_rows = l_shipdate.size();
        if (l_discount.size() != n_rows || l_quantity.size() != n_rows ||
            l_extendedprice.size() != n_rows) {
            fail("lineitem column length mismatch");
        }

        if (shipdate_mins.size() != shipdate_maxs.size() ||
            discount_mins.size() != discount_maxs.size() ||
            quantity_mins.size() != quantity_maxs.size() ||
            shipdate_mins.size() != discount_mins.size() ||
            shipdate_mins.size() != quantity_mins.size()) {
            fail("zone map length mismatch");
        }

        const size_t zone_blocks = shipdate_mins.size();
        if (zone_blocks != kExpectedBlocks) {
            fail("unexpected zone map block count");
        }

        const size_t expected_blocks = (n_rows + kBlockRows - 1) / kBlockRows;
        if (expected_blocks != zone_blocks) {
            fail("unexpected row count for 100000-row blocks");
        }

        std::bitset<kExpectedBlocks> candidate_blocks;
        bool any_candidate_block = false;
        bool full_density = true;
        {
            GENDB_PHASE("dim_filter");
            for (size_t block = 0; block < zone_blocks; ++block) {
                const bool ship_overlap =
                    !(shipdate_maxs[block] < kShipdateLo || shipdate_mins[block] >= kShipdateHi);
                const bool discount_overlap =
                    !(discount_maxs[block] < kDiscountLo || discount_mins[block] > kDiscountHi);
                const bool quantity_overlap = quantity_mins[block] < kQuantityHi;
                const bool candidate = ship_overlap && discount_overlap && quantity_overlap;
                candidate_blocks.set(block, candidate);
                any_candidate_block = any_candidate_block || candidate;
                full_density = full_density && candidate;
            }
        }

        {
            GENDB_PHASE("build_joins");
        }

        int64_t revenue_scaled4 = 0;
        {
            GENDB_PHASE("main_scan");
            if (any_candidate_block) {
                const int thread_count = std::max(1, std::min(omp_get_max_threads(), 32));
                std::vector<PaddedSum> partial_sums(static_cast<size_t>(thread_count));
                for (PaddedSum& slot : partial_sums) {
                    slot.value = 0;
                }

                const int32_t* __restrict shipdate = l_shipdate.data;
                const int64_t* __restrict discount = l_discount.data;
                const int64_t* __restrict quantity = l_quantity.data;
                const int64_t* __restrict extendedprice = l_extendedprice.data;

#pragma omp parallel num_threads(thread_count)
                {
                    const int tid = omp_get_thread_num();
                    int64_t local_sum = 0;

                    if (full_density) {
                        const size_t row_begin =
                            (n_rows * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                        const size_t row_end =
                            (n_rows * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);

                        for (size_t row = row_begin; row < row_end; ++row) {
                            const int32_t ship = shipdate[row];
                            if (!shipdate_pass(ship)) {
                                continue;
                            }

                            const int64_t disc = discount[row];
                            if (!discount_pass(disc)) {
                                continue;
                            }

                            if (quantity[row] >= kQuantityHi) {
                                continue;
                            }

                            local_sum += extendedprice[row] * disc;
                        }
                    } else {
                        const size_t block_begin =
                            (zone_blocks * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                        const size_t block_end =
                            (zone_blocks * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);

                        for (size_t block = block_begin; block < block_end; ++block) {
                            if (!candidate_blocks.test(block)) {
                                continue;
                            }

                            const size_t row_begin = block * static_cast<size_t>(kBlockRows);
                            const size_t row_end = std::min(row_begin + static_cast<size_t>(kBlockRows), n_rows);

                            for (size_t row = row_begin; row < row_end; ++row) {
                                const int32_t ship = shipdate[row];
                                if (!shipdate_pass(ship)) {
                                    continue;
                                }

                                const int64_t disc = discount[row];
                                if (!discount_pass(disc)) {
                                    continue;
                                }

                                if (quantity[row] >= kQuantityHi) {
                                    continue;
                                }

                                local_sum += extendedprice[row] * disc;
                            }
                        }
                    }

                    partial_sums[static_cast<size_t>(tid)].value = local_sum;
                }

                for (const PaddedSum& slot : partial_sums) {
                    revenue_scaled4 += slot.value;
                }
            }
        }

        {
            GENDB_PHASE("output");
            const double revenue = static_cast<double>(revenue_scaled4) / 10000.0;
            FILE* out = std::fopen((results_dir / "Q6.csv").c_str(), "w");
            if (out == nullptr) {
                fail("failed to open results file");
            }
            std::fprintf(out, "revenue\n%.2f\n", revenue);
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        fail(std::string("Q6 failed: ") + e.what());
    }
}
