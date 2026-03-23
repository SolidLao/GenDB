#include <algorithm>
#include <cstdint>
#include <cstdio>
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
constexpr size_t kScanMorselWords = 2048;
constexpr const char* kSegment = "BUILDING";

enum BlockState : uint8_t {
    kSkip = 0,
    kAccept = 1,
    kFallback = 2,
};

struct CandidateRow {
    int32_t orderkey = 0;
    int64_t revenue_scaled4 = 0;
    int32_t orderdate = 0;
    int32_t shippriority = 0;
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

inline void set_bit(std::vector<uint64_t>& bits, uint32_t row_id) {
    bits[static_cast<size_t>(row_id >> 6)] |= (uint64_t{1} << (row_id & 63u));
}

inline int64_t aggregate_revenue(uint32_t row_start,
                                 uint32_t row_count,
                                 const int32_t* l_shipdate,
                                 const int64_t* l_extendedprice,
                                 const int64_t* l_discount) {
    int64_t revenue_scaled4 = 0;
    const uint32_t row_end = row_start + row_count;
    for (uint32_t row = row_start; row < row_end; ++row) {
        if (__builtin_expect(l_shipdate[row] > kDateCutoff, 1)) {
            revenue_scaled4 += l_extendedprice[row] * (100 - l_discount[row]);
        }
    }
    return revenue_scaled4;
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

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        std::filesystem::create_directories(results_dir);

        gendb::init_date_tables();
        const int num_threads = std::max(1, std::min(32, omp_get_num_procs()));
        omp_set_num_threads(num_threads);
#pragma omp parallel num_threads(num_threads)
        {
        }

        GENDB_PHASE("total");

        gendb::MmapColumn<int32_t> c_custkey;
        gendb::MmapColumn<char> c_mktsegment_dict_data;
        gendb::MmapColumn<uint64_t> c_mktsegment_dict_offsets;
        gendb::MmapColumn<uint64_t> customer_mktsegment_offsets;
        gendb::MmapColumn<uint32_t> customer_mktsegment_row_ids;

        gendb::MmapColumn<uint64_t> orders_custkey_offsets;
        gendb::MmapColumn<uint32_t> orders_custkey_row_ids;
        gendb::MmapColumn<int32_t> orders_orderdate_zone_mins;
        gendb::MmapColumn<int32_t> orders_orderdate_zone_maxs;
        gendb::MmapColumn<int32_t> o_orderdate;
        gendb::MmapColumn<int32_t> o_orderkey;
        gendb::MmapColumn<int32_t> o_shippriority;

        gendb::MmapColumn<uint32_t> lineitem_group_row_starts;
        gendb::MmapColumn<uint32_t> lineitem_group_row_counts;
        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<int64_t> l_extendedprice;
        gendb::MmapColumn<int64_t> l_discount;

        size_t customer_count = 0;
        size_t order_count = 0;
        size_t lineitem_group_count = 0;
        size_t lineitem_row_count = 0;
        bool dense_customer_keys = false;

        {
            GENDB_PHASE("data_loading");

            c_custkey = mmap_column<int32_t>(gendb_dir, "customer/c_custkey.bin");
            c_mktsegment_dict_data = mmap_column<char>(gendb_dir, "customer/c_mktsegment.dict.data.bin");
            c_mktsegment_dict_offsets = mmap_column<uint64_t>(gendb_dir, "customer/c_mktsegment.dict.offsets.bin");
            customer_mktsegment_offsets = mmap_column<uint64_t>(gendb_dir, "customer/indexes/customer_mktsegment_postings.offsets.bin");
            customer_mktsegment_row_ids = mmap_column<uint32_t>(gendb_dir, "customer/indexes/customer_mktsegment_postings.row_ids.bin");

            orders_custkey_offsets = mmap_column<uint64_t>(gendb_dir, "orders/indexes/orders_custkey_postings.offsets.bin");
            orders_custkey_row_ids = mmap_column<uint32_t>(gendb_dir, "orders/indexes/orders_custkey_postings.row_ids.bin");
            orders_orderdate_zone_mins = mmap_column<int32_t>(gendb_dir, "orders/indexes/orders_orderdate_zone_map.mins.bin");
            orders_orderdate_zone_maxs = mmap_column<int32_t>(gendb_dir, "orders/indexes/orders_orderdate_zone_map.maxs.bin");
            o_orderdate = mmap_column<int32_t>(gendb_dir, "orders/o_orderdate.bin");
            o_orderkey = mmap_column<int32_t>(gendb_dir, "orders/o_orderkey.bin");
            o_shippriority = mmap_column<int32_t>(gendb_dir, "orders/o_shippriority.bin");

            lineitem_group_row_starts = mmap_column<uint32_t>(gendb_dir, "lineitem/indexes/lineitem_orderkey_groups.row_starts.bin");
            lineitem_group_row_counts = mmap_column<uint32_t>(gendb_dir, "lineitem/indexes/lineitem_orderkey_groups.row_counts.bin");
            l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem/l_shipdate.bin");
            l_extendedprice = mmap_column<int64_t>(gendb_dir, "lineitem/l_extendedprice.bin");
            l_discount = mmap_column<int64_t>(gendb_dir, "lineitem/l_discount.bin");

            customer_count = c_custkey.size();
            order_count = o_orderdate.size();
            lineitem_group_count = lineitem_group_row_starts.size();
            lineitem_row_count = l_shipdate.size();

            check_count(order_count, o_orderkey.size(), "orders/o_orderkey.bin");
            check_count(order_count, o_shippriority.size(), "orders/o_shippriority.bin");
            check_count(lineitem_group_count, lineitem_group_row_counts.size(), "lineitem_orderkey_groups.row_counts.bin");
            check_count(lineitem_row_count, l_extendedprice.size(), "lineitem/l_extendedprice.bin");
            check_count(lineitem_row_count, l_discount.size(), "lineitem/l_discount.bin");

            if (lineitem_group_count < order_count) {
                fail("lineitem orderkey groups do not cover all order rows");
            }
            if (orders_orderdate_zone_mins.size() != orders_orderdate_zone_maxs.size()) {
                fail("orders orderdate zone-map arrays length mismatch");
            }
            if (customer_mktsegment_offsets.size() < 2) {
                fail("customer_mktsegment_postings offsets too small");
            }
            if (orders_custkey_offsets.empty()) {
                fail("orders_custkey_postings offsets is empty");
            }
            if (orders_custkey_row_ids.size() != order_count) {
                fail("orders_custkey_postings row_ids length mismatch");
            }
            if (customer_mktsegment_row_ids.size() != customer_count) {
                fail("customer_mktsegment_postings row_ids length mismatch");
            }
            if (lineitem_group_count > 0) {
                const uint64_t last_group_end = static_cast<uint64_t>(lineitem_group_row_starts[lineitem_group_count - 1]) +
                                                static_cast<uint64_t>(lineitem_group_row_counts[lineitem_group_count - 1]);
                if (last_group_end > lineitem_row_count) {
                    fail("lineitem orderkey groups exceed lineitem row count");
                }
            }

            dense_customer_keys =
                (customer_count > 0) &&
                (c_custkey[0] == 1) &&
                (c_custkey[customer_count - 1] == static_cast<int32_t>(customer_count));

            c_custkey.advise_sequential();
            customer_mktsegment_row_ids.advise_sequential();
            orders_custkey_offsets.advise_random();
            orders_custkey_row_ids.advise_random();
            o_orderdate.advise_random();
            o_orderkey.advise_random();
            o_shippriority.advise_random();
            lineitem_group_row_starts.advise_sequential();
            lineitem_group_row_counts.advise_sequential();
            l_shipdate.advise_sequential();
            l_extendedprice.advise_sequential();
            l_discount.advise_sequential();

            gendb::mmap_prefetch_all(
                c_custkey,
                c_mktsegment_dict_data,
                c_mktsegment_dict_offsets,
                customer_mktsegment_offsets,
                customer_mktsegment_row_ids,
                orders_custkey_offsets,
                orders_custkey_row_ids,
                orders_orderdate_zone_mins,
                orders_orderdate_zone_maxs,
                o_orderdate,
                o_orderkey,
                o_shippriority,
                lineitem_group_row_starts,
                lineitem_group_row_counts,
                l_shipdate,
                l_extendedprice,
                l_discount);
        }

        uint64_t customer_posting_begin = 0;
        uint64_t customer_posting_end = 0;
        {
            GENDB_PHASE("dim_filter");

            const uint8_t building_code = resolve_segment_code(c_mktsegment_dict_data, c_mktsegment_dict_offsets, kSegment);
            if (static_cast<size_t>(building_code) + 1 >= customer_mktsegment_offsets.size()) {
                fail("resolved BUILDING code exceeds customer_mktsegment_postings offsets");
            }

            customer_posting_begin = customer_mktsegment_offsets[building_code];
            customer_posting_end = customer_mktsegment_offsets[static_cast<size_t>(building_code) + 1];
            if (customer_posting_end < customer_posting_begin || customer_posting_end > customer_mktsegment_row_ids.size()) {
                fail("customer_mktsegment_postings row id range out of bounds");
            }
        }

        std::vector<uint8_t> order_block_states;
        std::vector<uint64_t> qualifying_order_bits;
        {
            GENDB_PHASE("build_joins");

            order_block_states.resize(orders_orderdate_zone_mins.size(), kFallback);
            for (size_t block = 0; block < order_block_states.size(); ++block) {
                if (orders_orderdate_zone_mins[block] >= kDateCutoff) {
                    order_block_states[block] = kSkip;
                } else if (orders_orderdate_zone_maxs[block] < kDateCutoff) {
                    order_block_states[block] = kAccept;
                } else {
                    order_block_states[block] = kFallback;
                }
            }

            const size_t qualifying_word_count = (order_count + 63u) >> 6u;
            const uint32_t* __restrict__ qualifying_customer_rows = customer_mktsegment_row_ids.data;
            const uint64_t* __restrict__ orders_offsets = orders_custkey_offsets.data;
            const uint32_t* __restrict__ orders_rows = orders_custkey_row_ids.data;
            const int32_t* __restrict__ order_dates = o_orderdate.data;
            const int32_t* __restrict__ customer_keys = c_custkey.data;

            std::vector<std::vector<uint32_t>> local_selected_rows(static_cast<size_t>(num_threads));
            const size_t expected_selected_orders = 1600000;
            for (std::vector<uint32_t>& rows : local_selected_rows) {
                rows.reserve((expected_selected_orders / static_cast<size_t>(num_threads)) + 1024);
            }

#pragma omp parallel num_threads(num_threads)
            {
                std::vector<uint32_t>& local_rows = local_selected_rows[static_cast<size_t>(omp_get_thread_num())];

#pragma omp for schedule(static)
                for (uint64_t pos = customer_posting_begin; pos < customer_posting_end; ++pos) {
                    const uint32_t customer_row = qualifying_customer_rows[pos];
                    if (customer_row >= customer_count) {
                        continue;
                    }

                    const size_t custkey = dense_customer_keys
                                               ? (static_cast<size_t>(customer_row) + 1u)
                                               : static_cast<size_t>(customer_keys[customer_row]);
                    if (custkey == 0 || custkey >= orders_custkey_offsets.size()) {
                        continue;
                    }

                    const uint64_t orders_begin = orders_offsets[custkey];
                    const uint64_t orders_end = (custkey + 1 < orders_custkey_offsets.size())
                                                    ? orders_offsets[custkey + 1]
                                                    : orders_custkey_row_ids.size();
                    if (orders_end < orders_begin || orders_end > orders_custkey_row_ids.size()) {
                        continue;
                    }

                    for (uint64_t order_pos = orders_begin; order_pos < orders_end; ++order_pos) {
                        const uint32_t order_row = orders_rows[order_pos];
                        if (order_row >= order_count) {
                            continue;
                        }

                        const size_t block = static_cast<size_t>(order_row / kOrderBlockRows);
                        const uint8_t state = order_block_states[block];
                        if (state == kAccept || (state == kFallback && order_dates[order_row] < kDateCutoff)) {
                            local_rows.push_back(order_row);
                        }
                    }
                }
            }

            qualifying_order_bits.assign(qualifying_word_count, 0);
            for (const std::vector<uint32_t>& local_rows : local_selected_rows) {
                for (const uint32_t order_row : local_rows) {
                    set_bit(qualifying_order_bits, order_row);
                }
            }
        }
        std::vector<std::vector<CandidateRow>> local_topk(static_cast<size_t>(num_threads));

        {
            GENDB_PHASE("main_scan");

            const uint64_t* __restrict__ qualifying_bits = qualifying_order_bits.data();
            const size_t qualifying_word_count = qualifying_order_bits.size();
            const uint32_t* __restrict__ group_row_starts = lineitem_group_row_starts.data;
            const uint32_t* __restrict__ group_row_counts = lineitem_group_row_counts.data;
            const int32_t* __restrict__ shipdates = l_shipdate.data;
            const int64_t* __restrict__ extendedprices = l_extendedprice.data;
            const int64_t* __restrict__ discounts = l_discount.data;
            const int32_t* __restrict__ orderkeys = o_orderkey.data;
            const int32_t* __restrict__ orderdates = o_orderdate.data;
            const int32_t* __restrict__ shippriorities = o_shippriority.data;

            const size_t morsel_count = (qualifying_word_count + kScanMorselWords - 1) / kScanMorselWords;

#pragma omp parallel num_threads(num_threads)
            {
                std::vector<CandidateRow>& thread_heap = local_topk[static_cast<size_t>(omp_get_thread_num())];
                thread_heap.reserve(kTopK);

#pragma omp for schedule(static)
                for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                    const size_t word_begin = morsel * kScanMorselWords;
                    const size_t word_end = std::min(word_begin + kScanMorselWords, qualifying_word_count);

                    for (size_t word_index = word_begin; word_index < word_end; ++word_index) {
                        uint64_t bits = qualifying_bits[word_index];
                        while (bits != 0) {
                            const unsigned bit_index = static_cast<unsigned>(__builtin_ctzll(bits));
                            const size_t order_row = (word_index << 6u) + static_cast<size_t>(bit_index);
                            bits &= (bits - 1);

                            if (order_row >= order_count) {
                                continue;
                            }

                            const uint32_t row_start = group_row_starts[order_row];
                            const uint32_t row_count = group_row_counts[order_row];
                            const int64_t revenue_scaled4 = aggregate_revenue(
                                row_start,
                                row_count,
                                shipdates,
                                extendedprices,
                                discounts);
                            if (revenue_scaled4 <= 0) {
                                continue;
                            }

                            topk_push(thread_heap, CandidateRow{
                                orderkeys[order_row],
                                revenue_scaled4,
                                orderdates[order_row],
                                shippriorities[order_row],
                            });
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<CandidateRow> final_topk;
            final_topk.reserve(static_cast<size_t>(num_threads) * kTopK);
            for (const std::vector<CandidateRow>& thread_heap : local_topk) {
                for (const CandidateRow& row : thread_heap) {
                    topk_push(final_topk, row);
                }
            }

            std::sort(final_topk.begin(), final_topk.end(), better_candidate);
            if (final_topk.size() > kTopK) {
                final_topk.resize(kTopK);
            }

            const std::string output_path = results_dir + "/Q3.csv";
            std::FILE* out = std::fopen(output_path.c_str(), "w");
            if (!out) {
                fail("failed to open output file");
            }

            std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            for (const CandidateRow& row : final_topk) {
                char date_buf[11];
                gendb::epoch_days_to_date_str(row.orderdate, date_buf);
                date_buf[10] = '\0';

                std::fprintf(out, "%d,", row.orderkey);
                write_scaled4(out, row.revenue_scaled4);
                std::fprintf(out, ",%s,%d\n", date_buf, row.shippriority);
            }

            std::fclose(out);
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 1;
    }

    return 0;
}
