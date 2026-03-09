#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <omp.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int kTopK = 10;

struct ResultRow {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

static inline bool result_better(const ResultRow& lhs, const ResultRow& rhs) {
    if (lhs.revenue != rhs.revenue) {
        return lhs.revenue > rhs.revenue;
    }
    if (lhs.orderdate != rhs.orderdate) {
        return lhs.orderdate < rhs.orderdate;
    }
    if (lhs.shippriority != rhs.shippriority) {
        return lhs.shippriority < rhs.shippriority;
    }
    return lhs.orderkey < rhs.orderkey;
}

struct TopK {
    std::array<ResultRow, kTopK> rows {};
    int count = 0;
    int worst = 0;

    inline void recompute_worst() {
        worst = 0;
        for (int i = 1; i < count; ++i) {
            if (result_better(rows[worst], rows[i])) {
                worst = i;
            }
        }
    }

    inline void push(const ResultRow& row) {
        if (count < kTopK) {
            rows[count++] = row;
            if (count == kTopK) {
                recompute_worst();
            }
            return;
        }
        if (result_better(row, rows[worst])) {
            rows[worst] = row;
            recompute_worst();
        }
    }
};

static int32_t find_segment_code(const gendb::MmapColumn<uint64_t>& dict_offsets,
                                 const gendb::MmapColumn<char>& dict_data,
                                 const char* needle) {
    const size_t needle_len = std::strlen(needle);
    for (size_t code = 0; code + 1 < dict_offsets.size(); ++code) {
        const uint64_t begin = dict_offsets[code];
        const uint64_t end = dict_offsets[code + 1];
        if (end >= begin && (end - begin) == needle_len &&
            std::memcmp(dict_data.data + begin, needle, needle_len) == 0) {
            return static_cast<int32_t>(code);
        }
    }
    return -1;
}

static void ensure_results_dir(const std::string& dir) {
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("mkdir failed for " + dir);
    }
}

static int plan_thread_count() {
    const unsigned hw = std::thread::hardware_concurrency();
    const int hw_threads = hw == 0 ? omp_get_max_threads() : static_cast<int>(hw);
    return std::max(1, std::min(hw_threads, omp_get_max_threads()));
}

}  // namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    gendb::init_date_tables();
    const int32_t cutoff_date = gendb::date_str_to_epoch_days("1995-03-15");
    const int thread_count = plan_thread_count();

    gendb::MmapColumn<int32_t> c_custkey;
    gendb::MmapColumn<uint64_t> c_dict_offsets;
    gendb::MmapColumn<char> c_dict_data;
    gendb::MmapColumn<uint64_t> c_seg_offsets;
    gendb::MmapColumn<uint64_t> c_seg_rowids;

    gendb::MmapColumn<int32_t> o_orderkey;
    gendb::MmapColumn<int32_t> o_orderdate;
    gendb::MmapColumn<int32_t> o_shippriority;
    gendb::MmapColumn<uint64_t> o_cust_offsets;
    gendb::MmapColumn<uint64_t> o_cust_rowids;

    gendb::MmapColumn<int32_t> l_shipdate;
    gendb::MmapColumn<double> l_extendedprice;
    gendb::MmapColumn<double> l_discount;
    gendb::MmapColumn<uint64_t> l_order_offsets;
    gendb::MmapColumn<uint64_t> l_order_rowids;

    std::vector<int32_t> building_custkeys;
    std::vector<uint32_t> qualifying_order_rowids;
    std::vector<ResultRow> merged_rows;

    {
        GENDB_PHASE("data_loading");

        c_custkey.open(gendb_dir + "/customer/c_custkey.bin");
        c_dict_offsets.open(gendb_dir + "/customer/c_mktsegment.dict.offsets.bin");
        c_dict_data.open(gendb_dir + "/customer/c_mktsegment.dict.data.bin");
        c_seg_offsets.open(gendb_dir + "/customer/indexes/customer_segment_postings.offsets.bin");
        c_seg_rowids.open(gendb_dir + "/customer/indexes/customer_segment_postings.rowids.bin");

        o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
        o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        o_shippriority.open(gendb_dir + "/orders/o_shippriority.bin");
        o_cust_offsets.open(gendb_dir + "/orders/indexes/orders_cust_postings.offsets.bin");
        o_cust_rowids.open(gendb_dir + "/orders/indexes/orders_cust_postings.rowids.bin");

        l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        l_order_offsets.open(gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin");
        l_order_rowids.open(gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin");

        c_custkey.advise_random();
        c_seg_offsets.advise_random();
        c_seg_rowids.advise_random();
        o_orderkey.advise_random();
        o_orderdate.advise_random();
        o_shippriority.advise_random();
        o_cust_offsets.advise_random();
        o_cust_rowids.advise_random();
        l_shipdate.advise_random();
        l_extendedprice.advise_random();
        l_discount.advise_random();
        l_order_offsets.advise_random();
        l_order_rowids.advise_random();

        gendb::mmap_prefetch_all(c_custkey,
                                 c_dict_offsets,
                                 c_dict_data,
                                 c_seg_offsets,
                                 c_seg_rowids,
                                 o_orderkey,
                                 o_orderdate,
                                 o_shippriority,
                                 o_cust_offsets,
                                 o_cust_rowids,
                                 l_shipdate,
                                 l_extendedprice,
                                 l_discount,
                                 l_order_offsets,
                                 l_order_rowids);
    }

    {
        GENDB_PHASE("dim_filter");

        const int32_t building_code = find_segment_code(c_dict_offsets, c_dict_data, "BUILDING");
        if (building_code < 0) {
            throw std::runtime_error("BUILDING not found in customer segment dictionary");
        }
        if (static_cast<size_t>(building_code + 1) >= c_seg_offsets.size()) {
            throw std::runtime_error("BUILDING code outside customer segment postings");
        }

        const uint64_t begin = c_seg_offsets[static_cast<size_t>(building_code)];
        const uint64_t end = c_seg_offsets[static_cast<size_t>(building_code) + 1];
        if (end < begin || end > c_seg_rowids.size()) {
            throw std::runtime_error("invalid BUILDING postings slice");
        }

        building_custkeys.reserve(static_cast<size_t>(end - begin));
        for (uint64_t pos = begin; pos < end; ++pos) {
            const uint64_t customer_rowid = c_seg_rowids[pos];
            if (customer_rowid >= c_custkey.size()) {
                continue;
            }
            const int32_t custkey = c_custkey[customer_rowid];
            if (custkey > 0) {
                building_custkeys.push_back(custkey);
            }
        }
    }

    {
        GENDB_PHASE("build_joins");

        std::vector<std::vector<uint32_t>> local_orders(static_cast<size_t>(thread_count));

        #pragma omp parallel num_threads(thread_count)
        {
            const int tid = omp_get_thread_num();
            const size_t total = building_custkeys.size();
            const size_t begin =
                (total * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
            const size_t end =
                (total * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
            std::vector<uint32_t>& out = local_orders[static_cast<size_t>(tid)];

            if (begin < end) {
                out.reserve((end - begin) * 5);
            }

            for (size_t i = begin; i < end; ++i) {
                const int32_t custkey = building_custkeys[i];
                if (custkey < 0) {
                    continue;
                }

                const size_t cust_slot = static_cast<size_t>(custkey);
                if (cust_slot + 1 >= o_cust_offsets.size()) {
                    continue;
                }

                const uint64_t posting_begin = o_cust_offsets[cust_slot];
                const uint64_t posting_end = o_cust_offsets[cust_slot + 1];
                for (uint64_t pos = posting_begin; pos < posting_end; ++pos) {
                    const uint64_t order_rowid = o_cust_rowids[pos];
                    if (order_rowid >= o_orderdate.size()) {
                        continue;
                    }

                    const int32_t orderdate = o_orderdate[order_rowid];
                    if (orderdate >= cutoff_date) {
                        continue;
                    }

                    out.push_back(static_cast<uint32_t>(order_rowid));
                }
            }
        }

        size_t total_orders = 0;
        for (const auto& local : local_orders) {
            total_orders += local.size();
        }

        qualifying_order_rowids.reserve(total_orders);
        for (auto& local : local_orders) {
            qualifying_order_rowids.insert(
                qualifying_order_rowids.end(), local.begin(), local.end());
        }

        std::sort(qualifying_order_rowids.begin(), qualifying_order_rowids.end());
    }

    {
        GENDB_PHASE("main_scan");

        std::vector<TopK> local_topk(static_cast<size_t>(thread_count));

        #pragma omp parallel num_threads(thread_count)
        {
            const int tid = omp_get_thread_num();
            const size_t total = qualifying_order_rowids.size();
            const size_t begin =
                (total * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
            const size_t end =
                (total * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
            TopK& topk = local_topk[static_cast<size_t>(tid)];

            for (size_t i = begin; i < end; ++i) {
                const uint64_t order_rowid = qualifying_order_rowids[i];
                if (order_rowid >= o_orderkey.size()) {
                    continue;
                }

                const int32_t orderkey = o_orderkey[order_rowid];
                if (orderkey <= 0) {
                    continue;
                }
                const int32_t orderdate = o_orderdate[order_rowid];
                const int32_t shippriority = o_shippriority[order_rowid];
                const size_t order_slot = static_cast<size_t>(orderkey);
                if (order_slot + 1 >= l_order_offsets.size()) {
                    continue;
                }

                const uint64_t line_begin = l_order_offsets[order_slot];
                const uint64_t line_end = l_order_offsets[order_slot + 1];
                double revenue = 0.0;

                for (uint64_t pos = line_begin; pos < line_end; ++pos) {
                    const uint64_t line_rowid = l_order_rowids[pos];
                    if (line_rowid >= l_shipdate.size()) {
                        continue;
                    }
                    if (l_shipdate[line_rowid] <= cutoff_date) {
                        continue;
                    }
                    revenue += l_extendedprice[line_rowid] * (1.0 - l_discount[line_rowid]);
                }

                if (revenue > 0.0) {
                    topk.push(ResultRow{
                        orderkey,
                        revenue,
                        orderdate,
                        shippriority,
                    });
                }
            }
        }

        merged_rows.reserve(static_cast<size_t>(thread_count) * kTopK);
        for (const TopK& topk : local_topk) {
            for (int i = 0; i < topk.count; ++i) {
                merged_rows.push_back(topk.rows[i]);
            }
        }
    }

    {
        GENDB_PHASE("output");

        const size_t limit = std::min(static_cast<size_t>(kTopK), merged_rows.size());
        if (limit > 0) {
            std::partial_sort(merged_rows.begin(),
                              merged_rows.begin() + static_cast<std::ptrdiff_t>(limit),
                              merged_rows.end(),
                              result_better);
        }

        ensure_results_dir(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            throw std::runtime_error("fopen failed for " + out_path);
        }

        std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[16];
        for (size_t i = 0; i < limit; ++i) {
            gendb::epoch_days_to_date_str(merged_rows[i].orderdate, date_buf);
            std::fprintf(out,
                         "%d,%.2f,%s,%d\n",
                         merged_rows[i].orderkey,
                         merged_rows[i].revenue,
                         date_buf,
                         merged_rows[i].shippriority);
        }
        std::fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        run_q3(argv[1], argv[2]);
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "%s\n", ex.what());
        return 1;
    }
    return 0;
}
#endif
