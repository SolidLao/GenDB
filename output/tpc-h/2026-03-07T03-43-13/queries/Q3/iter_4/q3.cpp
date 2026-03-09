#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
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

inline bool better_candidate(const CandidateRow& lhs, const CandidateRow& rhs) {
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

size_t worst_index(const std::vector<CandidateRow>& rows) {
    size_t worst = 0;
    for (size_t i = 1; i < rows.size(); ++i) {
        if (better_candidate(rows[worst], rows[i])) {
            worst = i;
        }
    }
    return worst;
}

void topk_insert(std::vector<CandidateRow>& rows, const CandidateRow& candidate) {
    if (rows.size() < kTopK) {
        rows.push_back(candidate);
        return;
    }
    const size_t worst = worst_index(rows);
    if (better_candidate(candidate, rows[worst])) {
        rows[worst] = candidate;
    }
}

bool may_enter_topk(const std::vector<CandidateRow>& rows, int64_t revenue_scaled4) {
    if (revenue_scaled4 <= 0) {
        return false;
    }
    if (rows.size() < kTopK) {
        return true;
    }
    return revenue_scaled4 >= rows[worst_index(rows)].revenue_scaled4;
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
    return word_index < word_count &&
           (bits[word_index] & (uint64_t{1} << (key & 63u))) != 0;
}

inline int64_t aggregate_revenue(uint32_t row_begin,
                                 uint32_t row_end,
                                 const int32_t* l_shipdate,
                                 const uint32_t* l_extendedprice_u32,
                                 const uint8_t* l_discount_u8) {
    int64_t revenue_scaled4 = 0;
    for (uint32_t row = row_begin; row < row_end; ++row) {
        if (l_shipdate[row] > kDateCutoff) {
            revenue_scaled4 += static_cast<int64_t>(l_extendedprice_u32[row]) *
                               static_cast<int64_t>(100u - l_discount_u8[row]);
        }
    }
    return revenue_scaled4;
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
        gendb::MmapColumn<int32_t> l_shipdate;
        gendb::MmapColumn<uint32_t> l_extendedprice_u32;
        gendb::MmapColumn<uint8_t> l_discount_u8;

        {
            GENDB_PHASE("data_loading");
            const std::filesystem::path customer_dir = gendb_dir / "customer";
            const std::filesystem::path customer_index_dir = customer_dir / "indexes";
            const std::filesystem::path orders_dir = gendb_dir / "orders";
            const std::filesystem::path orders_index_dir = orders_dir / "indexes";
            const std::filesystem::path lineitem_index_dir = gendb_dir / "lineitem" / "indexes";

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
            l_shipdate.open((gendb_dir / "lineitem" / "l_shipdate.bin").string());
            l_extendedprice_u32.open((gendb_dir / "column_versions" / "lineitem.l_extendedprice.u32" / "values.bin").string());
            l_discount_u8.open((gendb_dir / "column_versions" / "lineitem.l_discount.u8" / "values.bin").string());

            c_custkey.advise_sequential();
            customer_mktsegment_row_ids.advise_sequential();
            o_custkey.advise_sequential();
            o_orderdate.advise_sequential();
            orders_orderdate_mins.advise_sequential();
            orders_orderdate_maxs.advise_sequential();
            lineitem_group_row_starts.advise_sequential();
            l_shipdate.advise_sequential();
            l_extendedprice_u32.advise_sequential();
            l_discount_u8.advise_sequential();

            o_orderkey.advise_random();
            o_shippriority.advise_random();

            gendb::mmap_prefetch_all(c_custkey,
                                     customer_mktsegment_row_ids,
                                     o_custkey,
                                     o_orderdate,
                                     orders_orderdate_mins,
                                     orders_orderdate_maxs,
                                     lineitem_group_row_starts,
                                     l_shipdate,
                                     l_extendedprice_u32,
                                     l_discount_u8);
        }

        const size_t order_count = std::min(
            std::min(o_custkey.size(), o_orderdate.size()),
            std::min(o_orderkey.size(), o_shippriority.size()));
        if (order_count == 0) {
            fail("orders inputs are empty");
        }
        if (lineitem_group_row_starts.size() < order_count) {
            fail("lineitem_orderkey_groups.row_starts shorter than orders");
        }
        if (orders_orderdate_mins.size() != orders_orderdate_maxs.size()) {
            fail("orders orderdate zone-map arrays length mismatch");
        }
        if (c_custkey.empty()) {
            fail("customer c_custkey is empty");
        }

        const size_t lineitem_row_count = std::min(
            l_shipdate.size(), std::min(l_extendedprice_u32.size(), l_discount_u8.size()));
        if (lineitem_row_count == 0) {
            fail("lineitem inputs are empty");
        }

        std::vector<uint64_t> customer_membership_bits;
        {
            GENDB_PHASE("dim_filter");
            const uint8_t building_code =
                resolve_segment_code(c_mktsegment_dict_data, c_mktsegment_dict_offsets, kSegment);
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
            const uint32_t* row_starts_ptr = lineitem_group_row_starts.data;
            const int32_t* l_shipdate_ptr = l_shipdate.data;
            const uint32_t* l_extendedprice_ptr = l_extendedprice_u32.data;
            const uint8_t* l_discount_ptr = l_discount_u8.data;

#pragma omp parallel num_threads(thread_count)
            {
                std::vector<CandidateRow>& thread_rows =
                    local_topk[static_cast<size_t>(omp_get_thread_num())];
                thread_rows.reserve(kTopK);

#pragma omp for schedule(dynamic, 1)
                for (size_t block = 0; block < order_block_states.size(); ++block) {
                    const uint8_t block_state = order_block_states[block];
                    if (block_state == kSkip) {
                        continue;
                    }

                    const size_t block_start = block * static_cast<size_t>(kOrderBlockRows);
                    const size_t block_end = std::min(block_start + static_cast<size_t>(kOrderBlockRows), order_count);
                    for (size_t order_row = block_start; order_row < block_end; ++order_row) {
                        const uint32_t custkey = static_cast<uint32_t>(o_custkey_ptr[order_row]);
                        if (!test_membership_bit(customer_bits_ptr, customer_bit_words, custkey)) {
                            continue;
                        }

                        int32_t orderdate = 0;
                        if (block_state == kFallback) {
                            orderdate = o_orderdate_ptr[order_row];
                            if (orderdate >= kDateCutoff) {
                                continue;
                            }
                        }

                        const uint32_t row_begin = row_starts_ptr[order_row];
                        const uint32_t row_end = (order_row + 1 < order_count)
                                                     ? row_starts_ptr[order_row + 1]
                                                     : static_cast<uint32_t>(lineitem_row_count);
                        if (row_end <= row_begin) {
                            continue;
                        }

                        const int64_t revenue_scaled4 = aggregate_revenue(
                            row_begin,
                            row_end,
                            l_shipdate_ptr,
                            l_extendedprice_ptr,
                            l_discount_ptr);
                        if (!may_enter_topk(thread_rows, revenue_scaled4)) {
                            continue;
                        }

                        if (block_state == kAccept) {
                            orderdate = o_orderdate_ptr[order_row];
                        }

                        const CandidateRow candidate{
                            o_orderkey_ptr[order_row],
                            revenue_scaled4,
                            orderdate,
                            o_shippriority_ptr[order_row],
                        };
                        topk_insert(thread_rows, candidate);
                    }
                }
            }
        }

        std::vector<CandidateRow> topk_rows;
        topk_rows.reserve(kTopK);
        for (const auto& thread_rows : local_topk) {
            for (const CandidateRow& row : thread_rows) {
                topk_insert(topk_rows, row);
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

        return 0;
    } catch (const std::exception& ex) {
        fail(ex.what());
    }
}
