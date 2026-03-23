#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr int32_t kShipdateLo = 8766;
constexpr int32_t kShipdateHi = 9131;
constexpr int64_t kDiscountLo = 5;
constexpr int64_t kDiscountHi = 7;
constexpr int64_t kQuantityHi = 2400;
constexpr uint64_t kBlockRows = 100000;

struct alignas(64) ThreadBuffers {
    std::vector<uint32_t> row_ids;
    std::vector<uint8_t> discounts;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

template <typename T>
void require_same_count(const MmapColumn<T>& column, size_t expected, const char* name) {
    if (column.count != expected) {
        fail(std::string("Column size mismatch for ") + name);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE("total");

    MmapColumn<int32_t> shipdate;
    MmapColumn<int64_t> discount;
    MmapColumn<int64_t> quantity;
    MmapColumn<int64_t> extendedprice;

    MmapColumn<int32_t> shipdate_mins;
    MmapColumn<int32_t> shipdate_maxs;
    MmapColumn<int64_t> discount_mins;
    MmapColumn<int64_t> discount_maxs;
    MmapColumn<int64_t> quantity_mins;
    MmapColumn<int64_t> quantity_maxs;

    {
        GENDB_PHASE("data_loading");
        shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        discount.open(gendb_dir + "/lineitem/l_discount.bin");
        quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        shipdate_mins.open(gendb_dir + "/lineitem/indexes/lineitem_shipdate_zone_map.mins.bin");
        shipdate_maxs.open(gendb_dir + "/lineitem/indexes/lineitem_shipdate_zone_map.maxs.bin");
        discount_mins.open(gendb_dir + "/lineitem/indexes/lineitem_discount_zone_map.mins.bin");
        discount_maxs.open(gendb_dir + "/lineitem/indexes/lineitem_discount_zone_map.maxs.bin");
        quantity_mins.open(gendb_dir + "/lineitem/indexes/lineitem_quantity_zone_map.mins.bin");
        quantity_maxs.open(gendb_dir + "/lineitem/indexes/lineitem_quantity_zone_map.maxs.bin");

        mmap_prefetch_all(shipdate, discount, quantity, extendedprice);
    }

    const size_t n_rows = shipdate.count;
    require_same_count(discount, n_rows, "l_discount");
    require_same_count(quantity, n_rows, "l_quantity");
    require_same_count(extendedprice, n_rows, "l_extendedprice");

    if (shipdate_mins.count != shipdate_maxs.count ||
        discount_mins.count != discount_maxs.count ||
        quantity_mins.count != quantity_maxs.count) {
        fail("Zone map min/max count mismatch");
    }

    const size_t zone_blocks = shipdate_mins.count;
    if (zone_blocks == 0 || discount_mins.count != zone_blocks || quantity_mins.count != zone_blocks) {
        fail("Zone map block count mismatch");
    }

    const size_t expected_blocks = (n_rows + kBlockRows - 1) / kBlockRows;
    if (zone_blocks != expected_blocks) {
        fail("Unexpected zone map block count for lineitem");
    }

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
            any_candidate_block = any_candidate_block || candidate;
            full_density = full_density && candidate;
        }
    }

    const size_t word_count = (n_rows + 63) / 64;
    std::vector<uint64_t> shipdate_bitmap(word_count, 0);
    const int thread_count = std::max(1, omp_get_max_threads());
    std::vector<ThreadBuffers> thread_buffers(static_cast<size_t>(thread_count));
    int64_t revenue_scaled4 = 0;

    if (any_candidate_block) {
        {
            GENDB_PHASE("dim_filter");
#pragma omp parallel for schedule(static)
            for (size_t word_index = 0; word_index < word_count; ++word_index) {
                const size_t base = word_index * 64;
                const size_t end = std::min(base + static_cast<size_t>(64), n_rows);

                uint64_t mask = 0;
                for (size_t row = base; row < end; ++row) {
                    const int32_t value = shipdate[row];
                    mask |= static_cast<uint64_t>(value >= kShipdateLo && value < kShipdateHi)
                            << (row - base);
                }
                shipdate_bitmap[word_index] = mask;
            }
        }

        {
            GENDB_PHASE("build_joins");
            const size_t reserve_rows =
                std::max<size_t>(1024, ((n_rows / static_cast<size_t>(thread_count)) * 3) / 100);
            for (ThreadBuffers& buffers : thread_buffers) {
                buffers.row_ids.reserve(reserve_rows);
                buffers.discounts.reserve(reserve_rows);
            }

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                ThreadBuffers& buffers = thread_buffers[static_cast<size_t>(tid)];
                const size_t word_begin = (word_count * static_cast<size_t>(tid)) /
                                          static_cast<size_t>(thread_count);
                const size_t word_end = (word_count * static_cast<size_t>(tid + 1)) /
                                        static_cast<size_t>(thread_count);

                for (size_t word_index = word_begin; word_index < word_end; ++word_index) {
                    uint64_t mask = shipdate_bitmap[word_index];
                    if (mask == 0) {
                        continue;
                    }

                    const uint32_t row_base = static_cast<uint32_t>(word_index * 64);
                    uint64_t keep_mask = 0;
                    while (mask != 0) {
                        const unsigned bit = static_cast<unsigned>(__builtin_ctzll(mask));
                        const uint32_t row_id = row_base + bit;
                        if (row_id >= n_rows) {
                            break;
                        }

                        const int64_t d = discount[row_id];
                        const int64_t q = quantity[row_id];
                        if (d >= kDiscountLo && d <= kDiscountHi && q < kQuantityHi) {
                            keep_mask |= (uint64_t{1} << bit);
                            buffers.row_ids.push_back(row_id);
                            buffers.discounts.push_back(static_cast<uint8_t>(d));
                        }
                        mask &= (mask - 1);
                    }
                    shipdate_bitmap[word_index] = keep_mask;
                }
            }
        }

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel reduction(+:revenue_scaled4) num_threads(thread_count)
            {
                const ThreadBuffers& buffers = thread_buffers[static_cast<size_t>(omp_get_thread_num())];
                int64_t local_sum = 0;
                const size_t count = buffers.row_ids.size();
                for (size_t i = 0; i < count; ++i) {
                    local_sum += extendedprice[buffers.row_ids[i]] *
                                 static_cast<int64_t>(buffers.discounts[i]);
                }
                revenue_scaled4 += local_sum;
            }
        }
    }

    {
        GENDB_PHASE("output");
        const double revenue = static_cast<double>(revenue_scaled4) / 10000.0;
        FILE* out = std::fopen((results_dir + "/Q6.csv").c_str(), "w");
        if (out == nullptr) {
            fail("Failed to open results file");
        }
        std::fprintf(out, "revenue\n%.4f\n", revenue);
        std::fclose(out);
    }

    (void)full_density;
    return 0;
}
