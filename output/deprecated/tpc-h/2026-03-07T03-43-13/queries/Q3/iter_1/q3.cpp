#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
constexpr uint32_t kOrderBlockRows = 100000;
constexpr size_t kTopK = 10;
constexpr const char* kSegment = "BUILDING";

enum BlockState : uint8_t {
    kSkip = 0,
    kAccept = 1,
    kFallback = 2,
};

struct CandidateRow {
    int32_t orderkey;
    int64_t revenue_scaled4;
    int32_t orderdate;
    int32_t shippriority;
};

[[noreturn]] void fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    std::exit(1);
}

bool better_candidate(const CandidateRow& lhs, const CandidateRow& rhs) {
    if (lhs.revenue_scaled4 != rhs.revenue_scaled4) {
        return lhs.revenue_scaled4 > rhs.revenue_scaled4;
    }
    if (lhs.orderdate != rhs.orderdate) {
        return lhs.orderdate < rhs.orderdate;
    }
    if (lhs.orderkey != rhs.orderkey) {
        return lhs.orderkey < rhs.orderkey;
    }
    return lhs.shippriority < rhs.shippriority;
}

struct HeapBetterCmp {
    bool operator()(const CandidateRow& lhs, const CandidateRow& rhs) const {
        return better_candidate(lhs, rhs);
    }
};

void topk_push(std::vector<CandidateRow>& heap, const CandidateRow& row) {
    if (heap.size() < kTopK) {
        heap.push_back(row);
        std::push_heap(heap.begin(), heap.end(), HeapBetterCmp{});
        return;
    }
    if (better_candidate(row, heap.front())) {
        std::pop_heap(heap.begin(), heap.end(), HeapBetterCmp{});
        heap.back() = row;
        std::push_heap(heap.begin(), heap.end(), HeapBetterCmp{});
    }
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

uint8_t resolve_segment_code(const gendb::MmapColumn<char>& dict_data,
                             const gendb::MmapColumn<uint64_t>& dict_offsets,
                             const std::string& target) {
    if (dict_offsets.size() < 2) {
        fail("customer segment dictionary offsets too small");
    }

    for (size_t code = 0; code + 1 < dict_offsets.size(); ++code) {
        const uint64_t begin = dict_offsets[code];
        const uint64_t end = dict_offsets[code + 1];
        if (end < begin || end > dict_data.size()) {
            fail("customer segment dictionary offsets out of range");
        }
        const size_t len = static_cast<size_t>(end - begin);
        if (len == target.size() &&
            std::char_traits<char>::compare(dict_data.data + begin, target.data(), len) == 0) {
            return static_cast<uint8_t>(code);
        }
    }

    fail("failed to resolve BUILDING code from customer segment dictionary");
}

inline void set_membership_bit(std::vector<uint64_t>& bits, uint32_t key) {
    bits[key >> 6] |= (uint64_t{1} << (key & 63u));
}

inline bool test_membership_bit(const uint64_t* bits, size_t word_count, uint32_t key) {
    const size_t word_index = static_cast<size_t>(key >> 6);
    if (word_index >= word_count) {
        return false;
    }
    return (bits[word_index] & (uint64_t{1} << (key & 63u))) != 0;
}

inline int64_t aggregate_revenue(uint32_t row_start,
                                 uint32_t row_count,
                                 const int32_t* l_shipdate,
                                 const int64_t* l_extendedprice,
                                 const int64_t* l_discount) {
    int64_t revenue = 0;
    const uint32_t row_end = row_start + row_count;
    for (uint32_t row = row_start; row < row_end; ++row) {
        if (l_shipdate[row] > kDateCutoff) {
            revenue += l_extendedprice[row] * (100 - l_discount[row]);
        }
    }
    return revenue;
}

inline void process_order_row(size_t order_row,
                              int32_t orderdate,
                              const uint64_t* customer_bits,
                              size_t customer_bit_words,
                              const int32_t* o_custkey,
                              const int32_t* o_orderkey,
                              const int32_t* o_shippriority,
                              const uint32_t* lineitem_group_row_starts,
                              const uint32_t* lineitem_group_row_counts,
                              size_t lineitem_group_count,
                              const int32_t* l_shipdate,
                              const int64_t* l_extendedprice,
                              const int64_t* l_discount,
                              size_t lineitem_row_count,
                              std::vector<CandidateRow>& thread_topk) {
    const uint32_t custkey = static_cast<uint32_t>(o_custkey[order_row]);
    if (!test_membership_bit(customer_bits, customer_bit_words, custkey)) {
        return;
    }

    if (order_row >= lineitem_group_count) {
        return;
    }

    const uint32_t row_start = lineitem_group_row_starts[order_row];
    const uint32_t row_count = lineitem_group_row_counts[order_row];
    const uint64_t row_end = static_cast<uint64_t>(row_start) + static_cast<uint64_t>(row_count);
    if (row_end > lineitem_row_count) {
        return;
    }

    const int64_t revenue = aggregate_revenue(row_start, row_count, l_shipdate, l_extendedprice, l_discount);
    if (revenue <= 0) {
        return;
    }

    topk_push(thread_topk, CandidateRow{
        o_orderkey[order_row],
        revenue,
        orderdate,
        o_shippriority[order_row],
    });
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
            l_shipdate.open((lineitem_dir / "l_shipdate.bin").string());
            l_extendedprice.open((lineitem_dir / "l_extendedprice.bin").string());
            l_discount.open((lineitem_dir / "l_discount.bin").string());

            o_custkey.advise_sequential();
            o_orderdate.advise_sequential();
            o_orderkey.advise_sequential();
            o_shippriority.advise_sequential();
            lineitem_group_row_starts.advise_sequential();
            lineitem_group_row_counts.advise_sequential();
            l_shipdate.advise_sequential();
            l_extendedprice.advise_sequential();
            l_discount.advise_sequential();
            gendb::mmap_prefetch_all(o_custkey,
                                     o_orderdate,
                                     o_orderkey,
                                     o_shippriority,
                                     lineitem_group_row_starts,
                                     lineitem_group_row_counts,
                                     l_shipdate,
                                     l_extendedprice,
                                     l_discount);
        }

        const size_t order_count = std::min(
            std::min(o_custkey.size(), o_orderdate.size()),
            std::min(std::min(o_orderkey.size(), o_shippriority.size()),
                     std::min(lineitem_group_row_starts.size(), lineitem_group_row_counts.size())));
        if (order_count == 0) {
            fail("orders scan inputs are empty");
        }
        if (orders_orderdate_mins.size() != orders_orderdate_maxs.size()) {
            fail("orders orderdate zone-map arrays length mismatch");
        }
        if (c_custkey.empty()) {
            fail("customer c_custkey is empty");
        }

        uint8_t building_code = 0;
        std::vector<uint64_t> customer_membership_bits;
        {
            GENDB_PHASE("dim_filter");
            building_code = resolve_segment_code(c_mktsegment_dict_data, c_mktsegment_dict_offsets, kSegment);
            if (static_cast<size_t>(building_code) + 1 >= customer_mktsegment_offsets.size()) {
                fail("resolved BUILDING code exceeds customer_mktsegment_postings offsets");
            }

            const uint64_t posting_begin = customer_mktsegment_offsets[building_code];
            const uint64_t posting_end = customer_mktsegment_offsets[static_cast<size_t>(building_code) + 1];
            if (posting_end < posting_begin || posting_end > customer_mktsegment_row_ids.size()) {
                fail("customer_mktsegment_postings row id range out of bounds");
            }

            const uint32_t max_custkey = static_cast<uint32_t>(c_custkey[c_custkey.size() - 1]);
            customer_membership_bits.assign(static_cast<size_t>(max_custkey >> 6) + 1, 0);

            for (uint64_t pos = posting_begin; pos < posting_end; ++pos) {
                const uint32_t customer_row = customer_mktsegment_row_ids[pos];
                if (customer_row >= c_custkey.size()) {
                    fail("customer_mktsegment_postings row id exceeds c_custkey length");
                }
                const uint32_t custkey = static_cast<uint32_t>(c_custkey[customer_row]);
                set_membership_bit(customer_membership_bits, custkey);
            }
        }

        std::vector<uint8_t> order_block_states;
        {
            GENDB_PHASE("build_joins");
            order_block_states.resize(orders_orderdate_mins.size(), kFallback);
            for (size_t block = 0; block < order_block_states.size(); ++block) {
                if (orders_orderdate_mins[block] >= kDateCutoff) {
                    order_block_states[block] = kSkip;
                } else if (orders_orderdate_maxs[block] < kDateCutoff) {
                    order_block_states[block] = kAccept;
                } else {
                    order_block_states[block] = kFallback;
                }
            }
        }

        const int thread_count = std::max(1, omp_get_max_threads());
        std::vector<std::vector<CandidateRow>> local_topk(static_cast<size_t>(thread_count));
        {
            GENDB_PHASE("main_scan");
            const uint64_t* customer_bits_ptr = customer_membership_bits.data();
            const size_t customer_bit_words = customer_membership_bits.size();
            const int32_t* o_custkey_ptr = o_custkey.data;
            const int32_t* o_orderdate_ptr = o_orderdate.data;
            const int32_t* o_orderkey_ptr = o_orderkey.data;
            const int32_t* o_shippriority_ptr = o_shippriority.data;
            const uint32_t* lineitem_group_row_starts_ptr = lineitem_group_row_starts.data;
            const uint32_t* lineitem_group_row_counts_ptr = lineitem_group_row_counts.data;
            const size_t lineitem_group_count = std::min(lineitem_group_row_starts.size(), lineitem_group_row_counts.size());
            const int32_t* l_shipdate_ptr = l_shipdate.data;
            const int64_t* l_extendedprice_ptr = l_extendedprice.data;
            const int64_t* l_discount_ptr = l_discount.data;
            const size_t lineitem_row_count = std::min(std::min(l_shipdate.size(), l_extendedprice.size()), l_discount.size());

#pragma omp parallel num_threads(thread_count)
            {
                auto& thread_heap = local_topk[static_cast<size_t>(omp_get_thread_num())];
                thread_heap.reserve(kTopK);

#pragma omp for schedule(dynamic, 1)
                for (size_t block = 0; block < order_block_states.size(); ++block) {
                    const uint8_t block_state = order_block_states[block];
                    if (block_state == kSkip) {
                        continue;
                    }

                    const size_t block_start = block * static_cast<size_t>(kOrderBlockRows);
                    const size_t block_end = std::min(block_start + static_cast<size_t>(kOrderBlockRows), order_count);
                    if (block_start >= block_end) {
                        continue;
                    }

                    if (block_state == kAccept) {
                        for (size_t order_row = block_start; order_row < block_end; ++order_row) {
                            process_order_row(order_row,
                                              o_orderdate_ptr[order_row],
                                              customer_bits_ptr,
                                              customer_bit_words,
                                              o_custkey_ptr,
                                              o_orderkey_ptr,
                                              o_shippriority_ptr,
                                              lineitem_group_row_starts_ptr,
                                              lineitem_group_row_counts_ptr,
                                              lineitem_group_count,
                                              l_shipdate_ptr,
                                              l_extendedprice_ptr,
                                              l_discount_ptr,
                                              lineitem_row_count,
                                              thread_heap);
                        }
                    } else {
                        for (size_t order_row = block_start; order_row < block_end; ++order_row) {
                            const int32_t orderdate = o_orderdate_ptr[order_row];
                            if (orderdate >= kDateCutoff) {
                                continue;
                            }
                            process_order_row(order_row,
                                              orderdate,
                                              customer_bits_ptr,
                                              customer_bit_words,
                                              o_custkey_ptr,
                                              o_orderkey_ptr,
                                              o_shippriority_ptr,
                                              lineitem_group_row_starts_ptr,
                                              lineitem_group_row_counts_ptr,
                                              lineitem_group_count,
                                              l_shipdate_ptr,
                                              l_extendedprice_ptr,
                                              l_discount_ptr,
                                              lineitem_row_count,
                                              thread_heap);
                        }
                    }
                }
            }
        }

        std::vector<CandidateRow> topk_rows;
        topk_rows.reserve(kTopK);
        for (const auto& thread_heap : local_topk) {
            for (const CandidateRow& row : thread_heap) {
                topk_push(topk_rows, row);
            }
        }
        std::sort(topk_rows.begin(), topk_rows.end(), better_candidate);
        if (topk_rows.size() > kTopK) {
            topk_rows.resize(kTopK);
        }

        {
            GENDB_PHASE("output");
            gendb::init_date_tables();

            const std::filesystem::path output_path = results_dir / "Q3.csv";
            std::FILE* out = std::fopen(output_path.c_str(), "w");
            if (out == nullptr) {
                fail("failed to open output file");
            }

            std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char date_buf[11];
            for (const CandidateRow& row : topk_rows) {
                gendb::epoch_days_to_date_str(row.orderdate, date_buf);
                std::fprintf(out, "%d,", row.orderkey);
                write_scaled4(out, row.revenue_scaled4);
                std::fprintf(out, ",%s,%d\n", date_buf, row.shippriority);
            }
            std::fclose(out);
        }

        (void)building_code;
        return 0;
    } catch (const std::exception& ex) {
        fail(ex.what());
    }
}
