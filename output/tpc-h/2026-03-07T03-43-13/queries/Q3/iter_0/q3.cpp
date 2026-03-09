#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kDateCutoff = 9204;
constexpr uint32_t kOrderBlockRows = 100000;
constexpr size_t kExpectedOrderBlocks = 150;
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
    const int64_t whole = value / 10000;
    const int64_t frac = value % 10000;
    std::fprintf(out, "%lld.%04lld", static_cast<long long>(whole), static_cast<long long>(frac));
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

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::filesystem::path gendb_dir = argv[1];
    const std::filesystem::path results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE_MS("total", total_ms);

    gendb::MmapColumn<char> c_mktsegment_dict_data;
    gendb::MmapColumn<uint64_t> c_mktsegment_dict_offsets;
    gendb::MmapColumn<uint64_t> customer_mktsegment_offsets;
    gendb::MmapColumn<uint32_t> customer_mktsegment_row_ids;
    gendb::MmapColumn<int32_t> c_custkey;

    gendb::MmapColumn<uint64_t> orders_custkey_offsets;
    gendb::MmapColumn<uint32_t> orders_custkey_row_ids;
    gendb::MmapColumn<int32_t> o_orderkey;
    gendb::MmapColumn<int32_t> o_orderdate;
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

        orders_custkey_offsets.open((orders_index_dir / "orders_custkey_postings.offsets.bin").string());
        orders_custkey_row_ids.open((orders_index_dir / "orders_custkey_postings.row_ids.bin").string());
        o_orderkey.open((orders_dir / "o_orderkey.bin").string());
        o_orderdate.open((orders_dir / "o_orderdate.bin").string());
        o_shippriority.open((orders_dir / "o_shippriority.bin").string());
        orders_orderdate_mins.open((orders_index_dir / "orders_orderdate_zone_map.mins.bin").string());
        orders_orderdate_maxs.open((orders_index_dir / "orders_orderdate_zone_map.maxs.bin").string());

        lineitem_group_row_starts.open((lineitem_index_dir / "lineitem_orderkey_groups.row_starts.bin").string());
        lineitem_group_row_counts.open((lineitem_index_dir / "lineitem_orderkey_groups.row_counts.bin").string());
        l_shipdate.open((lineitem_dir / "l_shipdate.bin").string());
        l_extendedprice.open((lineitem_dir / "l_extendedprice.bin").string());
        l_discount.open((lineitem_dir / "l_discount.bin").string());

        c_custkey.advise_random();
        orders_custkey_offsets.advise_random();
        o_orderkey.advise_random();
        o_orderdate.advise_random();
        o_shippriority.advise_random();
        lineitem_group_row_starts.advise_random();
        lineitem_group_row_counts.advise_random();
    }

    if (o_orderkey.size() != o_orderdate.size() || o_orderkey.size() != o_shippriority.size()) {
        fail("orders column length mismatch");
    }
    if (lineitem_group_row_starts.size() != o_orderkey.size() ||
        lineitem_group_row_counts.size() != o_orderkey.size()) {
        fail("lineitem orderkey groups do not align with orders rows");
    }
    if (l_shipdate.size() != l_extendedprice.size() || l_shipdate.size() != l_discount.size()) {
        fail("lineitem column length mismatch");
    }
    if (orders_orderdate_mins.size() != orders_orderdate_maxs.size()) {
        fail("orders orderdate zone map length mismatch");
    }
    if (orders_orderdate_mins.size() != kExpectedOrderBlocks) {
        fail("unexpected orders orderdate zone map block count");
    }

    std::vector<int32_t> qualifying_custkeys;
    uint8_t building_code = 0;
    {
        GENDB_PHASE("dim_filter");
        building_code = resolve_segment_code(c_mktsegment_dict_data, c_mktsegment_dict_offsets, kSegment);
        if (static_cast<size_t>(building_code) + 1 >= customer_mktsegment_offsets.size()) {
            fail("resolved BUILDING code outside postings offsets");
        }

        const uint64_t begin = customer_mktsegment_offsets[building_code];
        const uint64_t end = customer_mktsegment_offsets[static_cast<size_t>(building_code) + 1];
        if (end < begin || end > customer_mktsegment_row_ids.size()) {
            fail("customer segment postings range out of bounds");
        }

        qualifying_custkeys.resize(static_cast<size_t>(end - begin));
#pragma omp parallel for schedule(static)
        for (uint64_t i = 0; i < end - begin; ++i) {
            const uint32_t customer_row = customer_mktsegment_row_ids[begin + i];
            qualifying_custkeys[static_cast<size_t>(i)] = c_custkey[customer_row];
        }
    }

    std::vector<uint8_t> order_block_states;
    std::vector<int32_t> qualifying_orderkeys;
    std::vector<int32_t> qualifying_orderdates;
    std::vector<int32_t> qualifying_shippriorities;
    std::vector<uint32_t> qualifying_line_starts;
    std::vector<uint32_t> qualifying_line_counts;

    {
        GENDB_PHASE("build_joins");
        order_block_states.resize(orders_orderdate_mins.size());
        for (size_t block = 0; block < orders_orderdate_mins.size(); ++block) {
            if (orders_orderdate_mins[block] >= kDateCutoff) {
                order_block_states[block] = kSkip;
            } else if (orders_orderdate_maxs[block] < kDateCutoff) {
                order_block_states[block] = kAccept;
            } else {
                order_block_states[block] = kFallback;
            }
        }

        const int thread_count = std::max(1, omp_get_max_threads());
        std::vector<std::vector<int32_t>> local_orderkeys(static_cast<size_t>(thread_count));
        std::vector<std::vector<int32_t>> local_orderdates(static_cast<size_t>(thread_count));
        std::vector<std::vector<int32_t>> local_shippriorities(static_cast<size_t>(thread_count));
        std::vector<std::vector<uint32_t>> local_line_starts(static_cast<size_t>(thread_count));
        std::vector<std::vector<uint32_t>> local_line_counts(static_cast<size_t>(thread_count));

#pragma omp parallel num_threads(thread_count)
        {
            const int tid = omp_get_thread_num();
            auto& orderkeys = local_orderkeys[static_cast<size_t>(tid)];
            auto& orderdates = local_orderdates[static_cast<size_t>(tid)];
            auto& shippriorities = local_shippriorities[static_cast<size_t>(tid)];
            auto& line_starts = local_line_starts[static_cast<size_t>(tid)];
            auto& line_counts = local_line_counts[static_cast<size_t>(tid)];

            orderkeys.reserve(32768);
            orderdates.reserve(32768);
            shippriorities.reserve(32768);
            line_starts.reserve(32768);
            line_counts.reserve(32768);

#pragma omp for schedule(dynamic, 4096)
            for (size_t i = 0; i < qualifying_custkeys.size(); ++i) {
                const int32_t custkey = qualifying_custkeys[i];
                if (custkey <= 0 || static_cast<size_t>(custkey) + 1 >= orders_custkey_offsets.size()) {
                    continue;
                }

                const uint64_t begin = orders_custkey_offsets[static_cast<size_t>(custkey)];
                const uint64_t end = orders_custkey_offsets[static_cast<size_t>(custkey) + 1];
                for (uint64_t pos = begin; pos < end; ++pos) {
                    const uint32_t order_row = orders_custkey_row_ids[pos];
                    const uint8_t block_state = order_block_states[order_row / kOrderBlockRows];
                    if (block_state == kSkip) {
                        continue;
                    }
                    const int32_t orderdate = o_orderdate[order_row];
                    if (block_state == kFallback && orderdate >= kDateCutoff) {
                        continue;
                    }

                    orderkeys.push_back(o_orderkey[order_row]);
                    orderdates.push_back(orderdate);
                    shippriorities.push_back(o_shippriority[order_row]);
                    line_starts.push_back(lineitem_group_row_starts[order_row]);
                    line_counts.push_back(lineitem_group_row_counts[order_row]);
                }
            }
        }

        size_t total_qualifying_orders = 0;
        for (int tid = 0; tid < thread_count; ++tid) {
            total_qualifying_orders += local_orderkeys[static_cast<size_t>(tid)].size();
        }

        qualifying_orderkeys.reserve(total_qualifying_orders);
        qualifying_orderdates.reserve(total_qualifying_orders);
        qualifying_shippriorities.reserve(total_qualifying_orders);
        qualifying_line_starts.reserve(total_qualifying_orders);
        qualifying_line_counts.reserve(total_qualifying_orders);

        for (int tid = 0; tid < thread_count; ++tid) {
            auto& src_orderkeys = local_orderkeys[static_cast<size_t>(tid)];
            auto& src_orderdates = local_orderdates[static_cast<size_t>(tid)];
            auto& src_shippriorities = local_shippriorities[static_cast<size_t>(tid)];
            auto& src_line_starts = local_line_starts[static_cast<size_t>(tid)];
            auto& src_line_counts = local_line_counts[static_cast<size_t>(tid)];

            qualifying_orderkeys.insert(qualifying_orderkeys.end(), src_orderkeys.begin(), src_orderkeys.end());
            qualifying_orderdates.insert(qualifying_orderdates.end(), src_orderdates.begin(), src_orderdates.end());
            qualifying_shippriorities.insert(qualifying_shippriorities.end(), src_shippriorities.begin(), src_shippriorities.end());
            qualifying_line_starts.insert(qualifying_line_starts.end(), src_line_starts.begin(), src_line_starts.end());
            qualifying_line_counts.insert(qualifying_line_counts.end(), src_line_counts.begin(), src_line_counts.end());
        }
    }

    if (qualifying_orderkeys.size() != qualifying_orderdates.size() ||
        qualifying_orderkeys.size() != qualifying_shippriorities.size() ||
        qualifying_orderkeys.size() != qualifying_line_starts.size() ||
        qualifying_orderkeys.size() != qualifying_line_counts.size()) {
        fail("qualifying order payload arrays length mismatch");
    }

    std::vector<int64_t> revenues_scaled4(qualifying_orderkeys.size(), 0);
    std::vector<CandidateRow> topk_rows;
    {
        GENDB_PHASE("main_scan");
        const int thread_count = std::max(1, omp_get_max_threads());
        std::vector<std::vector<CandidateRow>> local_topk(static_cast<size_t>(thread_count));

#pragma omp parallel num_threads(thread_count)
        {
            const int tid = omp_get_thread_num();
            auto& thread_topk = local_topk[static_cast<size_t>(tid)];
            thread_topk.reserve(kTopK);

#pragma omp for schedule(dynamic, 4096)
            for (size_t slot = 0; slot < qualifying_orderkeys.size(); ++slot) {
                const uint32_t row_start = qualifying_line_starts[slot];
                const uint32_t row_count = qualifying_line_counts[slot];
                const uint32_t row_end = row_start + row_count;
                if (row_end > l_shipdate.size()) {
                    continue;
                }

                int64_t revenue = 0;
                for (uint32_t row = row_start; row < row_end; ++row) {
                    if (l_shipdate[row] > kDateCutoff) {
                        revenue += l_extendedprice[row] * (100 - l_discount[row]);
                    }
                }
                revenues_scaled4[slot] = revenue;
                if (revenue > 0) {
                    topk_push(thread_topk, CandidateRow{
                        qualifying_orderkeys[slot],
                        revenue,
                        qualifying_orderdates[slot],
                        qualifying_shippriorities[slot],
                    });
                }
            }
        }

        for (const auto& thread_topk : local_topk) {
            for (const CandidateRow& row : thread_topk) {
                topk_push(topk_rows, row);
            }
        }

        std::sort(topk_rows.begin(), topk_rows.end(), better_candidate);
        if (topk_rows.size() > kTopK) {
            topk_rows.resize(kTopK);
        }
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

    (void)revenues_scaled4;
    (void)building_code;
    (void)total_ms;
    return 0;
}
