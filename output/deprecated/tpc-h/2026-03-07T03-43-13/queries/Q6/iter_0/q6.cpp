#include <algorithm>
#include <bitset>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

struct CandidateBlock {
    uint32_t block_id;
    bool shipdate_all_pass;
    bool discount_all_pass;
    bool quantity_all_pass;
};

struct alignas(64) PaddedSum {
    int64_t value;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE_MS("total", total_ms);

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
        const std::string lineitem_dir = gendb_dir + "/lineitem";
        const std::string index_dir = lineitem_dir + "/indexes";

        l_shipdate.open(lineitem_dir + "/l_shipdate.bin");
        l_discount.open(lineitem_dir + "/l_discount.bin");
        l_quantity.open(lineitem_dir + "/l_quantity.bin");
        l_extendedprice.open(lineitem_dir + "/l_extendedprice.bin");

        shipdate_mins.open(index_dir + "/lineitem_shipdate_zone_map.mins.bin");
        shipdate_maxs.open(index_dir + "/lineitem_shipdate_zone_map.maxs.bin");
        discount_mins.open(index_dir + "/lineitem_discount_zone_map.mins.bin");
        discount_maxs.open(index_dir + "/lineitem_discount_zone_map.maxs.bin");
        quantity_mins.open(index_dir + "/lineitem_quantity_zone_map.mins.bin");
        quantity_maxs.open(index_dir + "/lineitem_quantity_zone_map.maxs.bin");
    }

    const uint64_t n_rows = l_shipdate.size();
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

    if (shipdate_mins.size() != kExpectedBlocks) {
        fail("unexpected zone map block count");
    }

    const uint64_t expected_blocks = (n_rows + kBlockRows - 1) / kBlockRows;
    if (expected_blocks != kExpectedBlocks) {
        fail("unexpected row count for 100000-row blocks");
    }

    std::bitset<kExpectedBlocks> candidate_mask;
    std::vector<uint8_t> shipdate_all_pass(kExpectedBlocks, 0);
    std::vector<uint8_t> discount_all_pass(kExpectedBlocks, 0);
    std::vector<uint8_t> quantity_all_pass(kExpectedBlocks, 0);

    {
        GENDB_PHASE("dim_filter");
        for (size_t block = 0; block < kExpectedBlocks; ++block) {
            const bool ship_overlap =
                !(shipdate_maxs[block] < kShipdateLo || shipdate_mins[block] >= kShipdateHi);
            if (!ship_overlap) {
                continue;
            }

            const bool discount_overlap =
                !(discount_maxs[block] < kDiscountLo || discount_mins[block] > kDiscountHi);
            if (!discount_overlap) {
                continue;
            }

            const bool quantity_overlap = quantity_mins[block] < kQuantityHi;
            if (!quantity_overlap) {
                continue;
            }

            candidate_mask.set(block);
            shipdate_all_pass[block] =
                (shipdate_mins[block] >= kShipdateLo && shipdate_maxs[block] < kShipdateHi) ? 1 : 0;
            discount_all_pass[block] =
                (discount_mins[block] >= kDiscountLo && discount_maxs[block] <= kDiscountHi) ? 1 : 0;
            quantity_all_pass[block] = (quantity_maxs[block] < kQuantityHi) ? 1 : 0;
        }
    }

    std::vector<CandidateBlock> candidate_blocks;
    {
        GENDB_PHASE("build_joins");
        candidate_blocks.reserve(kExpectedBlocks);
        for (size_t block = 0; block < kExpectedBlocks; ++block) {
            if (!candidate_mask.test(block)) {
                continue;
            }
            candidate_blocks.push_back(CandidateBlock{
                static_cast<uint32_t>(block),
                shipdate_all_pass[block] != 0,
                discount_all_pass[block] != 0,
                quantity_all_pass[block] != 0,
            });
        }
    }

    int64_t revenue_scaled4 = 0;
    {
        GENDB_PHASE("main_scan");
        if (!candidate_blocks.empty()) {
            const int thread_count = std::max(1, omp_get_max_threads());
            std::vector<PaddedSum> partial_sums(static_cast<size_t>(thread_count));
            for (PaddedSum& slot : partial_sums) {
                slot.value = 0;
            }

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                int64_t local_sum = 0;

#pragma omp for schedule(dynamic, 1)
                for (size_t block_index = 0; block_index < candidate_blocks.size(); ++block_index) {
                    const CandidateBlock block = candidate_blocks[block_index];
                    const uint64_t begin = static_cast<uint64_t>(block.block_id) * kBlockRows;
                    const uint64_t end = std::min(begin + static_cast<uint64_t>(kBlockRows), n_rows);

                    if (block.shipdate_all_pass && block.discount_all_pass && block.quantity_all_pass) {
                        for (uint64_t row = begin; row < end; ++row) {
                            local_sum += l_extendedprice[row] * l_discount[row];
                        }
                        continue;
                    }

                    if (block.shipdate_all_pass && block.discount_all_pass) {
                        for (uint64_t row = begin; row < end; ++row) {
                            const int64_t quantity = l_quantity[row];
                            if (!block.quantity_all_pass && quantity >= kQuantityHi) {
                                continue;
                            }
                            local_sum += l_extendedprice[row] * l_discount[row];
                        }
                        continue;
                    }

                    if (block.shipdate_all_pass && block.quantity_all_pass) {
                        for (uint64_t row = begin; row < end; ++row) {
                            const int64_t discount = l_discount[row];
                            if (!block.discount_all_pass &&
                                (discount < kDiscountLo || discount > kDiscountHi)) {
                                continue;
                            }
                            local_sum += l_extendedprice[row] * discount;
                        }
                        continue;
                    }

                    if (block.discount_all_pass && block.quantity_all_pass) {
                        for (uint64_t row = begin; row < end; ++row) {
                            const int32_t shipdate = l_shipdate[row];
                            if (!block.shipdate_all_pass &&
                                (shipdate < kShipdateLo || shipdate >= kShipdateHi)) {
                                continue;
                            }
                            local_sum += l_extendedprice[row] * l_discount[row];
                        }
                        continue;
                    }

                    for (uint64_t row = begin; row < end; ++row) {
                        const int32_t shipdate = l_shipdate[row];
                        if (!block.shipdate_all_pass &&
                            (shipdate < kShipdateLo || shipdate >= kShipdateHi)) {
                            continue;
                        }

                        const int64_t discount = l_discount[row];
                        if (!block.discount_all_pass &&
                            (discount < kDiscountLo || discount > kDiscountHi)) {
                            continue;
                        }

                        const int64_t quantity = l_quantity[row];
                        if (!block.quantity_all_pass && quantity >= kQuantityHi) {
                            continue;
                        }

                        local_sum += l_extendedprice[row] * discount;
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
        const std::string output_path = results_dir + "/Q6.csv";
        std::FILE* out = std::fopen(output_path.c_str(), "w");
        if (out == nullptr) {
            fail("failed to open output file");
        }
        std::fprintf(out, "revenue\n%.2f\n", revenue);
        std::fclose(out);
    }

    (void)total_ms;
    return 0;
}
