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

constexpr uint32_t kCandidateReservePct = 17;
constexpr uint32_t kSurvivorReservePct = 3;

struct alignas(64) ThreadState {
    std::vector<uint32_t> candidate_rows;
    std::vector<uint32_t> survivor_rows;
    std::vector<int64_t> survivor_discounts;
    int64_t revenue_scaled4 = 0;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

inline bool shipdate_pass(const int32_t shipdate) {
    return static_cast<uint32_t>(shipdate - kShipdateLo) <
           static_cast<uint32_t>(kShipdateHi - kShipdateLo);
}

inline bool discount_pass(const int64_t discount) {
    return static_cast<uint64_t>(discount - kDiscountLo) <=
           static_cast<uint64_t>(kDiscountHi - kDiscountLo);
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
        size_t candidate_block_count = 0;
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
                candidate_block_count += static_cast<size_t>(candidate);
            }
        }

        {
            GENDB_PHASE("build_joins");
        }

        int64_t revenue_scaled4 = 0;
        {
            GENDB_PHASE("main_scan");
            if (candidate_block_count != 0) {
                const int thread_count = omp_get_max_threads() > 0 ? omp_get_max_threads() : 1;
                std::vector<ThreadState> states(static_cast<size_t>(thread_count));

                const int32_t* __restrict shipdate = l_shipdate.data;
                const int64_t* __restrict discount = l_discount.data;
                const int64_t* __restrict quantity = l_quantity.data;
                const int64_t* __restrict extendedprice = l_extendedprice.data;

#pragma omp parallel num_threads(thread_count)
                {
                    const int tid = omp_get_thread_num();
                    ThreadState& state = states[static_cast<size_t>(tid)];

                    const size_t row_begin =
                        (n_rows * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                    const size_t row_end =
                        (n_rows * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
                    const size_t partition_rows = row_end - row_begin;

                    state.candidate_rows.reserve((partition_rows * kCandidateReservePct) / 100 + 1024);
                    state.survivor_rows.reserve((partition_rows * kSurvivorReservePct) / 100 + 256);
                    state.survivor_discounts.reserve((partition_rows * kSurvivorReservePct) / 100 + 256);

                    for (size_t row = row_begin; row < row_end; ++row) {
                        if (shipdate_pass(shipdate[row])) {
                            state.candidate_rows.push_back(static_cast<uint32_t>(row));
                        }
                    }

                    for (const uint32_t row_id : state.candidate_rows) {
                        const int64_t disc = discount[row_id];
                        if (!discount_pass(disc)) {
                            continue;
                        }
                        if (quantity[row_id] >= kQuantityHi) {
                            continue;
                        }
                        state.survivor_rows.push_back(row_id);
                        state.survivor_discounts.push_back(disc);
                    }

                    state.candidate_rows.clear();

                    int64_t local_sum = 0;
                    const size_t survivor_count = state.survivor_rows.size();
                    for (size_t idx = 0; idx < survivor_count; ++idx) {
                        local_sum += extendedprice[state.survivor_rows[idx]] * state.survivor_discounts[idx];
                    }
                    state.revenue_scaled4 = local_sum;
                }

                for (const ThreadState& state : states) {
                    revenue_scaled4 += state.revenue_scaled4;
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
