#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kDateCutoff = 9204;
constexpr uint32_t kBlockRows = 100000;
constexpr size_t kTopK = 10;
constexpr const char* kSegment = "BUILDING";

enum BlockState : uint8_t {
    kSkip = 0,
    kAccept = 1,
    kFallback = 2,
};

struct ResultRow {
    int32_t orderkey;
    int64_t revenue_scaled4;
    int32_t orderdate;
    int32_t shippriority;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

inline bool result_better(const ResultRow& left, const ResultRow& right) {
    if (left.revenue_scaled4 != right.revenue_scaled4) {
        return left.revenue_scaled4 > right.revenue_scaled4;
    }
    if (left.orderdate != right.orderdate) {
        return left.orderdate < right.orderdate;
    }
    if (left.orderkey != right.orderkey) {
        return left.orderkey < right.orderkey;
    }
    return left.shippriority < right.shippriority;
}

inline void topk_consider(std::vector<ResultRow>& topk, const ResultRow& row) {
    if (topk.size() < kTopK) {
        topk.push_back(row);
        return;
    }

    size_t worst_index = 0;
    for (size_t i = 1; i < topk.size(); ++i) {
        if (result_better(topk[worst_index], topk[i])) {
            worst_index = i;
        }
    }
    if (result_better(row, topk[worst_index])) {
        topk[worst_index] = row;
    }
}

inline void bitset_set(std::vector<uint64_t>& bits, uint32_t index) {
    bits[static_cast<size_t>(index >> 6)] |= (uint64_t{1} << (index & 63u));
}

inline bool bitset_test(const uint64_t* bits, uint32_t index) {
    return (bits[static_cast<size_t>(index >> 6)] & (uint64_t{1} << (index & 63u))) != 0;
}

uint8_t resolve_segment_code(const gendb::MmapColumn<char>& dict_data,
                             const gendb::MmapColumn<uint64_t>& dict_offsets,
                             const char* target) {
    const size_t target_len = std::strlen(target);
    if (dict_offsets.size() < 2) {
        fail("customer c_mktsegment dictionary offsets too small");
    }

    for (size_t code = 0; code + 1 < dict_offsets.size(); ++code) {
        const uint64_t begin = dict_offsets[code];
        const uint64_t end = dict_offsets[code + 1];
        if (end < begin || end > dict_data.size()) {
            fail("customer c_mktsegment dictionary offsets out of range");
        }
        const size_t len = static_cast<size_t>(end - begin);
        if (len == target_len && std::memcmp(dict_data.data + begin, target, target_len) == 0) {
            return static_cast<uint8_t>(code);
        }
    }

    fail("BUILDING segment not found in customer dictionary");
}

inline int64_t accumulate_all_rows(uint32_t begin,
                                   uint32_t end,
                                   const int64_t* extendedprice,
                                   const int64_t* discount) {
    int64_t revenue = 0;
    for (uint32_t row = begin; row < end; ++row) {
        revenue += extendedprice[row] * (100 - discount[row]);
    }
    return revenue;
}

inline int64_t accumulate_filtered_rows(uint32_t begin,
                                        uint32_t end,
                                        const int32_t* shipdate,
                                        const int64_t* extendedprice,
                                        const int64_t* discount) {
    int64_t revenue = 0;
    for (uint32_t row = begin; row < end; ++row) {
        if (shipdate[row] > kDateCutoff) {
            revenue += extendedprice[row] * (100 - discount[row]);
        }
    }
    return revenue;
}

inline int64_t accumulate_run_revenue(uint32_t row_start,
                                      uint32_t row_count,
                                      const uint8_t* shipdate_block_states,
                                      size_t shipdate_block_count,
                                      const int32_t* shipdate,
                                      const int64_t* extendedprice,
                                      const int64_t* discount,
                                      size_t lineitem_row_count) {
    const uint64_t run_end64 = static_cast<uint64_t>(row_start) + static_cast<uint64_t>(row_count);
    if (run_end64 > lineitem_row_count) {
        fail("lineitem run exceeds lineitem row count");
    }

    const uint32_t row_end = static_cast<uint32_t>(run_end64);
    int64_t revenue = 0;
    uint32_t cursor = row_start;
    while (cursor < row_end) {
        const size_t block = static_cast<size_t>(cursor / kBlockRows);
        if (block >= shipdate_block_count) {
            fail("lineitem shipdate block index out of range");
        }
        const uint32_t block_end = std::min<uint32_t>(
            row_end,
            static_cast<uint32_t>((block + 1) * static_cast<size_t>(kBlockRows)));
        const uint8_t state = shipdate_block_states[block];
        if (state == kAccept) {
            revenue += accumulate_all_rows(cursor, block_end, extendedprice, discount);
        } else if (state == kFallback) {
            revenue += accumulate_filtered_rows(cursor, block_end, shipdate, extendedprice, discount);
        }
        cursor = block_end;
    }
    return revenue;
}

void write_scaled4(std::FILE* out, int64_t value) {
    if (value < 0) {
        std::fputc('-', out);
        value = -value;
    }
    const long long whole = static_cast<long long>(value / 10000);
    const long long frac = static_cast<long long>(value % 10000);
    std::fprintf(out, "%lld.%04lld", whole, frac);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::filesystem::path gendb_dir = argv[1];
    const std::filesystem::path results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    try {
        GENDB_PHASE("total");

        gendb::MmapColumn<char> c_mktsegment_dict_data;
        gendb::MmapColumn<uint64_t> c_mktsegment_dict_offsets;
        gendb::MmapColumn<uint64_t> customer_mktsegment_offsets;
        gendb::MmapColumn<uint32_t> customer_mktsegment_row_ids;
        gendb::MmapColumn<int32_t> c_custkey;

        gendb::MmapColumn<int32_t> o_custkey;
        gendb::MmapColumn<int32_t> o_orderdate;
        gendb::MmapColumn<int32_t> o_orderkey;
        gendb::MmapColumn<int32_t> o_shippriority;
        gendb::MmapColumn<int32_t> orders_orderdate_mins;
        gendb::MmapColumn<int32_t> orders_orderdate_maxs;

        gendb::MmapColumn<uint32_t> lineitem_group_row_starts;
        gendb::MmapColumn<uint32_t> lineitem_group_row_counts;
        gendb::MmapColumn<int32_t> lineitem_shipdate_mins;
        gendb::MmapColumn<int32_t> lineitem_shipdate_maxs;
        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<int64_t> l_extendedprice;
        gendb::MmapColumn<int64_t> l_discount;

        {
            GENDB_PHASE("data_loading");
            const std::filesystem::path customer_dir = gendb_dir / "customer";
            const std::filesystem::path customer_index_dir = customer_dir / "indexes";
            const std::filesystem::path orders_dir = gendb_dir / "orders";
            const std::filesystem::path orders_index_dir = orders_dir / "indexes";
            const std::filesystem::path lineitem_dir = gendb_dir / "lineitem";
            const std::filesystem::path lineitem_index_dir = lineitem_dir / "indexes";

            c_mktsegment_dict_data.open((customer_dir / "c_mktsegment.dict.data.bin").string());
            c_mktsegment_dict_offsets.open((customer_dir / "c_mktsegment.dict.offsets.bin").string());
            customer_mktsegment_offsets.open((customer_index_dir / "customer_mktsegment_postings.offsets.bin").string());
            customer_mktsegment_row_ids.open((customer_index_dir / "customer_mktsegment_postings.row_ids.bin").string());
            c_custkey.open((customer_dir / "c_custkey.bin").string());

            o_custkey.open((orders_dir / "o_custkey.bin").string());
            o_orderdate.open((orders_dir / "o_orderdate.bin").string());
            o_orderkey.open((orders_dir / "o_orderkey.bin").string());
            o_shippriority.open((orders_dir / "o_shippriority.bin").string());
            orders_orderdate_mins.open((orders_index_dir / "orders_orderdate_zone_map.mins.bin").string());
            orders_orderdate_maxs.open((orders_index_dir / "orders_orderdate_zone_map.maxs.bin").string());

            lineitem_group_row_starts.open((lineitem_index_dir / "lineitem_orderkey_groups.row_starts.bin").string());
            lineitem_group_row_counts.open((lineitem_index_dir / "lineitem_orderkey_groups.row_counts.bin").string());
            lineitem_shipdate_mins.open((lineitem_index_dir / "lineitem_shipdate_zone_map.mins.bin").string());
            lineitem_shipdate_maxs.open((lineitem_index_dir / "lineitem_shipdate_zone_map.maxs.bin").string());
            l_shipdate.open((lineitem_dir / "l_shipdate.bin").string());
            l_extendedprice.open((lineitem_dir / "l_extendedprice.bin").string());
            l_discount.open((lineitem_dir / "l_discount.bin").string());

            c_custkey.advise_sequential();
            customer_mktsegment_row_ids.advise_sequential();
            o_custkey.advise_sequential();
            o_orderdate.advise_sequential();
            o_orderkey.advise_sequential();
            o_shippriority.advise_sequential();
            lineitem_group_row_starts.advise_sequential();
            lineitem_group_row_counts.advise_sequential();
            lineitem_shipdate_mins.advise_sequential();
            lineitem_shipdate_maxs.advise_sequential();
            l_shipdate.advise_sequential();
            l_extendedprice.advise_sequential();
            l_discount.advise_sequential();

            gendb::mmap_prefetch_all(c_custkey,
                                     customer_mktsegment_row_ids,
                                     o_custkey,
                                     o_orderdate,
                                     o_orderkey,
                                     o_shippriority,
                                     lineitem_group_row_starts,
                                     lineitem_group_row_counts,
                                     lineitem_shipdate_mins,
                                     lineitem_shipdate_maxs,
                                     l_shipdate,
                                     l_extendedprice,
                                     l_discount);
        }

        const size_t customer_count = c_custkey.size();
        const size_t order_count = std::min(
            std::min(o_custkey.size(), o_orderdate.size()),
            std::min(std::min(o_orderkey.size(), o_shippriority.size()),
                     std::min(lineitem_group_row_starts.size(), lineitem_group_row_counts.size())));
        const size_t lineitem_row_count = std::min(l_shipdate.size(), std::min(l_extendedprice.size(), l_discount.size()));

        if (customer_count == 0) {
            fail("customer table is empty");
        }
        if (order_count == 0) {
            fail("orders inputs are empty");
        }
        if (orders_orderdate_mins.size() != orders_orderdate_maxs.size()) {
            fail("orders orderdate zone map size mismatch");
        }
        if (lineitem_shipdate_mins.size() != lineitem_shipdate_maxs.size()) {
            fail("lineitem shipdate zone map size mismatch");
        }
        if (static_cast<uint64_t>(lineitem_group_row_starts[order_count - 1]) +
                static_cast<uint64_t>(lineitem_group_row_counts[order_count - 1]) >
            lineitem_row_count) {
            fail("lineitem orderkey groups exceed lineitem rows");
        }

        std::vector<uint64_t> customer_membership_bits;
        {
            GENDB_PHASE("dim_filter");
            const uint8_t building_code = resolve_segment_code(c_mktsegment_dict_data,
                                                               c_mktsegment_dict_offsets,
                                                               kSegment);
            if (static_cast<size_t>(building_code) + 1 >= customer_mktsegment_offsets.size()) {
                fail("BUILDING code exceeds customer_mktsegment_postings offsets");
            }

            const uint64_t begin = customer_mktsegment_offsets[building_code];
            const uint64_t end = customer_mktsegment_offsets[static_cast<size_t>(building_code) + 1];
            if (end < begin || end > customer_mktsegment_row_ids.size()) {
                fail("customer_mktsegment_postings range out of bounds");
            }

            const uint32_t max_custkey = static_cast<uint32_t>(c_custkey[customer_count - 1]);
            customer_membership_bits.assign(static_cast<size_t>(max_custkey >> 6) + 1, 0);
            for (uint64_t pos = begin; pos < end; ++pos) {
                const uint32_t customer_row = customer_mktsegment_row_ids[pos];
                if (customer_row >= customer_count) {
                    fail("customer_mktsegment_postings row id out of bounds");
                }
                const uint32_t custkey = static_cast<uint32_t>(c_custkey[customer_row]);
                bitset_set(customer_membership_bits, custkey);
            }
        }

        const int num_threads = std::max(1, omp_get_num_procs());
        std::vector<uint64_t> qualifying_order_bits((order_count + 63) >> 6, 0);
        std::vector<uint8_t> lineitem_shipdate_block_states;
        {
            GENDB_PHASE("build_joins");
            const uint64_t* customer_bits_ptr = customer_membership_bits.data();
            const size_t customer_bit_capacity = customer_membership_bits.size() << 6;
            const size_t order_blocks = orders_orderdate_mins.size();
            const size_t order_bit_words = qualifying_order_bits.size();
            std::vector<uint64_t> local_order_bits(order_bit_words * static_cast<size_t>(num_threads), 0);

#pragma omp parallel num_threads(num_threads)
            {
                const int tid = omp_get_thread_num();
                uint64_t* local_bits = local_order_bits.data() + static_cast<size_t>(tid) * order_bit_words;

#pragma omp for schedule(static)
                for (size_t block = 0; block < order_blocks; ++block) {
                    const size_t block_start = block * static_cast<size_t>(kBlockRows);
                    if (block_start >= order_count) {
                        continue;
                    }
                    const size_t block_end = std::min(block_start + static_cast<size_t>(kBlockRows), order_count);

                    if (orders_orderdate_mins[block] >= kDateCutoff) {
                        continue;
                    }

                    if (orders_orderdate_maxs[block] < kDateCutoff) {
                        for (size_t row = block_start; row < block_end; ++row) {
                            const uint32_t custkey = static_cast<uint32_t>(o_custkey[row]);
                            if (custkey < customer_bit_capacity && bitset_test(customer_bits_ptr, custkey)) {
                                local_bits[row >> 6] |= (uint64_t{1} << (row & 63u));
                            }
                        }
                    } else {
                        for (size_t row = block_start; row < block_end; ++row) {
                            if (o_orderdate[row] < kDateCutoff) {
                                const uint32_t custkey = static_cast<uint32_t>(o_custkey[row]);
                                if (custkey < customer_bit_capacity && bitset_test(customer_bits_ptr, custkey)) {
                                    local_bits[row >> 6] |= (uint64_t{1} << (row & 63u));
                                }
                            }
                        }
                    }
                }
            }

#pragma omp parallel for schedule(static) num_threads(num_threads)
            for (size_t word = 0; word < order_bit_words; ++word) {
                uint64_t merged = 0;
                for (int tid = 0; tid < num_threads; ++tid) {
                    merged |= local_order_bits[static_cast<size_t>(tid) * order_bit_words + word];
                }
                qualifying_order_bits[word] = merged;
            }

            lineitem_shipdate_block_states.resize(lineitem_shipdate_mins.size(), kFallback);
#pragma omp parallel for schedule(static) num_threads(num_threads)
            for (size_t block = 0; block < lineitem_shipdate_block_states.size(); ++block) {
                if (lineitem_shipdate_maxs[block] <= kDateCutoff) {
                    lineitem_shipdate_block_states[block] = kSkip;
                } else if (lineitem_shipdate_mins[block] > kDateCutoff) {
                    lineitem_shipdate_block_states[block] = kAccept;
                } else {
                    lineitem_shipdate_block_states[block] = kFallback;
                }
            }
        }

        omp_set_num_threads(num_threads);
        std::vector<std::vector<ResultRow>> thread_topk(static_cast<size_t>(num_threads));
        {
            GENDB_PHASE("main_scan");
            const uint64_t* __restrict__ order_bits_ptr = qualifying_order_bits.data();
            const uint32_t* __restrict__ group_row_starts_ptr = lineitem_group_row_starts.data;
            const uint32_t* __restrict__ group_row_counts_ptr = lineitem_group_row_counts.data;
            const uint8_t* __restrict__ shipdate_block_states_ptr = lineitem_shipdate_block_states.data();
            const int32_t* __restrict__ shipdate_ptr = l_shipdate.data;
            const int64_t* __restrict__ extendedprice_ptr = l_extendedprice.data;
            const int64_t* __restrict__ discount_ptr = l_discount.data;
            const int32_t* __restrict__ orderkey_ptr = o_orderkey.data;
            const int32_t* __restrict__ orderdate_ptr = o_orderdate.data;
            const int32_t* __restrict__ shippriority_ptr = o_shippriority.data;

#pragma omp parallel num_threads(num_threads)
            {
                const int tid = omp_get_thread_num();
                std::vector<ResultRow>& local_topk = thread_topk[static_cast<size_t>(tid)];
                local_topk.reserve(kTopK);

                const size_t begin = (order_count * static_cast<size_t>(tid)) / static_cast<size_t>(num_threads);
                const size_t end = (order_count * static_cast<size_t>(tid + 1)) / static_cast<size_t>(num_threads);

                for (size_t order_row = begin; order_row < end; ++order_row) {
                    const uint32_t row32 = static_cast<uint32_t>(order_row);
                    const uint32_t row_start = group_row_starts_ptr[order_row];
                    const uint32_t row_count = group_row_counts_ptr[order_row];
                    if (!bitset_test(order_bits_ptr, row32)) {
                        continue;
                    }

                    const int64_t revenue = accumulate_run_revenue(row_start,
                                                                   row_count,
                                                                   shipdate_block_states_ptr,
                                                                   lineitem_shipdate_block_states.size(),
                                                                   shipdate_ptr,
                                                                   extendedprice_ptr,
                                                                   discount_ptr,
                                                                   lineitem_row_count);
                    if (revenue <= 0) {
                        continue;
                    }

                    topk_consider(local_topk, ResultRow{
                        orderkey_ptr[order_row],
                        revenue,
                        orderdate_ptr[order_row],
                        shippriority_ptr[order_row],
                    });
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> final_topk;
            final_topk.reserve(kTopK);
            for (const auto& local_topk : thread_topk) {
                for (const ResultRow& row : local_topk) {
                    topk_consider(final_topk, row);
                }
            }
            std::sort(final_topk.begin(), final_topk.end(), result_better);
            if (final_topk.size() > kTopK) {
                final_topk.resize(kTopK);
            }

            gendb::init_date_tables();
            const std::filesystem::path output_path = results_dir / "Q3.csv";
            std::FILE* out = std::fopen(output_path.string().c_str(), "w");
            if (!out) {
                fail("failed to open output file");
            }

            std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char date_buf[11];
            for (const ResultRow& row : final_topk) {
                gendb::epoch_days_to_date_str(row.orderdate, date_buf);
                std::fprintf(out, "%d,", row.orderkey);
                write_scaled4(out, row.revenue_scaled4);
                std::fprintf(out, ",%s,%d\n", date_buf, row.shippriority);
            }
            std::fclose(out);
        }
    } catch (const std::exception& e) {
        fail(std::string("Unhandled exception: ") + e.what());
    }

    return 0;
}
