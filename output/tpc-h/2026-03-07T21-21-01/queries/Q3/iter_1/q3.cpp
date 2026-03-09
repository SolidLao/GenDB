#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <omp.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

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
    std::array<ResultRow, 10> rows {};
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
        if (count < 10) {
            rows[count++] = row;
            if (count == 10) {
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
        std::perror(("mkdir: " + dir).c_str());
    }
}

}  // namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    static constexpr char kCutoffDate[] = "1995-03-15";
    const int32_t cutoff_date = gendb::date_str_to_epoch_days(kCutoffDate);

    int32_t building_code = -1;
    const uint64_t* building_rowids = nullptr;
    size_t building_count = 0;
    const int32_t* customer_custkey = nullptr;

    const int32_t* orders_orderkey = nullptr;
    const int32_t* orders_orderdate = nullptr;
    const int32_t* orders_shippriority = nullptr;
    const uint64_t* orders_cust_offsets = nullptr;
    const uint64_t* orders_cust_rowids = nullptr;
    size_t orders_cust_offsets_count = 0;

    const int32_t* lineitem_shipdate = nullptr;
    const double* lineitem_extendedprice = nullptr;
    const double* lineitem_discount = nullptr;
    const uint64_t* lineitem_order_offsets = nullptr;
    const uint64_t* lineitem_order_rowids = nullptr;
    size_t lineitem_order_offsets_count = 0;

    std::vector<ResultRow> merged;

    {
        GENDB_PHASE("data_loading");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<uint64_t> c_dict_offsets(
            gendb_dir + "/customer/c_mktsegment.dict.offsets.bin");
        gendb::MmapColumn<char> c_dict_data(gendb_dir + "/customer/c_mktsegment.dict.data.bin");
        gendb::MmapColumn<uint64_t> c_seg_offsets(
            gendb_dir + "/customer/indexes/customer_segment_postings.offsets.bin");
        gendb::MmapColumn<uint64_t> c_seg_rowids(
            gendb_dir + "/customer/indexes/customer_segment_postings.rowids.bin");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");
        gendb::MmapColumn<uint64_t> o_cust_offsets(
            gendb_dir + "/orders/indexes/orders_cust_postings.offsets.bin");
        gendb::MmapColumn<uint64_t> o_cust_rowids(
            gendb_dir + "/orders/indexes/orders_cust_postings.rowids.bin");

        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
        gendb::MmapColumn<double> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<double> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<uint64_t> l_order_offsets(
            gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin");
        gendb::MmapColumn<uint64_t> l_order_rowids(
            gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin");

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

        {
            GENDB_PHASE("dim_filter");

            building_code = find_segment_code(c_dict_offsets, c_dict_data, "BUILDING");
            if (building_code < 0) {
                std::fprintf(stderr, "BUILDING not found in customer segment dictionary\n");
                return;
            }
            if (static_cast<size_t>(building_code + 1) >= c_seg_offsets.size()) {
                std::fprintf(stderr, "BUILDING code outside customer segment postings\n");
                return;
            }

            const uint64_t begin = c_seg_offsets[static_cast<size_t>(building_code)];
            const uint64_t end = c_seg_offsets[static_cast<size_t>(building_code) + 1];
            if (end < begin || end > c_seg_rowids.size()) {
                std::fprintf(stderr, "invalid BUILDING postings slice\n");
                return;
            }

            building_rowids = c_seg_rowids.data + begin;
            building_count = static_cast<size_t>(end - begin);
            customer_custkey = c_custkey.data;

            orders_orderkey = o_orderkey.data;
            orders_orderdate = o_orderdate.data;
            orders_shippriority = o_shippriority.data;
            orders_cust_offsets = o_cust_offsets.data;
            orders_cust_rowids = o_cust_rowids.data;
            orders_cust_offsets_count = o_cust_offsets.size();

            lineitem_shipdate = l_shipdate.data;
            lineitem_extendedprice = l_extendedprice.data;
            lineitem_discount = l_discount.data;
            lineitem_order_offsets = l_order_offsets.data;
            lineitem_order_rowids = l_order_rowids.data;
            lineitem_order_offsets_count = l_order_offsets.size();
        }

        {
            GENDB_PHASE("build_joins");
        }

        {
            GENDB_PHASE("main_scan");

            const int thread_count = omp_get_max_threads();
            std::vector<TopK> local_topk(static_cast<size_t>(thread_count));

            #pragma omp parallel
            {
                TopK& topk = local_topk[static_cast<size_t>(omp_get_thread_num())];

                #pragma omp for schedule(dynamic, 1024)
                for (size_t i = 0; i < building_count; ++i) {
                    const uint64_t customer_rowid = building_rowids[i];
                    const int32_t custkey = customer_custkey[customer_rowid];
                    if (custkey <= 0) {
                        continue;
                    }

                    const size_t cust_slot = static_cast<size_t>(custkey);
                    if (cust_slot + 1 >= orders_cust_offsets_count) {
                        continue;
                    }

                    const uint64_t order_begin = orders_cust_offsets[cust_slot];
                    const uint64_t order_end = orders_cust_offsets[cust_slot + 1];
                    for (uint64_t order_pos = order_begin; order_pos < order_end; ++order_pos) {
                        const uint64_t order_rowid = orders_cust_rowids[order_pos];
                        const int32_t orderdate = orders_orderdate[order_rowid];
                        if (orderdate >= cutoff_date) {
                            continue;
                        }

                        const int32_t orderkey = orders_orderkey[order_rowid];
                        if (orderkey <= 0) {
                            continue;
                        }

                        const size_t order_slot = static_cast<size_t>(orderkey);
                        if (order_slot + 1 >= lineitem_order_offsets_count) {
                            continue;
                        }

                        const uint64_t line_begin = lineitem_order_offsets[order_slot];
                        const uint64_t line_end = lineitem_order_offsets[order_slot + 1];

                        double revenue = 0.0;
                        for (uint64_t line_pos = line_begin; line_pos < line_end; ++line_pos) {
                            const uint64_t line_rowid = lineitem_order_rowids[line_pos];
                            if (lineitem_shipdate[line_rowid] <= cutoff_date) {
                                continue;
                            }
                            revenue += lineitem_extendedprice[line_rowid] *
                                       (1.0 - lineitem_discount[line_rowid]);
                        }

                        if (revenue > 0.0) {
                            topk.push(ResultRow{orderkey,
                                                revenue,
                                                orderdate,
                                                orders_shippriority[order_rowid]});
                        }
                    }
                }
            }

            merged.reserve(static_cast<size_t>(thread_count) * 10);
            for (const TopK& topk : local_topk) {
                for (int i = 0; i < topk.count; ++i) {
                    merged.push_back(topk.rows[i]);
                }
            }
        }
    }

    {
        GENDB_PHASE("output");

        const size_t limit = std::min<size_t>(10, merged.size());
        if (limit > 0) {
            std::partial_sort(merged.begin(),
                              merged.begin() + static_cast<std::ptrdiff_t>(limit),
                              merged.end(),
                              result_better);
        }

        ensure_results_dir(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            std::perror(("fopen: " + out_path).c_str());
            return;
        }

        gendb::init_date_tables();
        std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[16];
        for (size_t i = 0; i < limit; ++i) {
            gendb::epoch_days_to_date_str(merged[i].orderdate, date_buf);
            std::fprintf(out,
                         "%d,%.2f,%s,%d\n",
                         merged[i].orderkey,
                         merged[i].revenue,
                         date_buf,
                         merged[i].shippriority);
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
    run_q3(argv[1], argv[2]);
    return 0;
}
#endif
