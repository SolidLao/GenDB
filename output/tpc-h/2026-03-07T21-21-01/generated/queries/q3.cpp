#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
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

namespace {

constexpr uint32_t kOrdersBlockSize = 131072;

struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};

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

static inline bool bitset_test(const std::vector<uint64_t>& bits, uint32_t key) {
    return (bits[key >> 6] >> (key & 63U)) & 1ULL;
}

static inline void bitset_set(std::vector<uint64_t>& bits, uint32_t key) {
    bits[key >> 6] |= (1ULL << (key & 63U));
}

static void ensure_results_dir(const std::string& dir) {
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        std::perror(("mkdir: " + dir).c_str());
    }
}

}  // namespace

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    static constexpr char kCutoffDate[] = "1995-03-15";
    const int32_t cutoff_date = gendb::date_str_to_epoch_days(kCutoffDate);

    gendb::MmapColumn<int32_t> c_custkey;
    gendb::MmapColumn<uint64_t> c_dict_offsets;
    gendb::MmapColumn<char> c_dict_data;
    gendb::MmapColumn<uint64_t> c_seg_offsets;
    gendb::MmapColumn<uint64_t> c_seg_rowids;

    gendb::MmapColumn<int32_t> o_custkey;
    gendb::MmapColumn<int32_t> o_orderkey;
    gendb::MmapColumn<int32_t> o_orderdate;
    gendb::MmapColumn<int32_t> o_shippriority;
    gendb::MmapColumn<ZoneMap1I32> o_orderdate_zonemap;

    gendb::MmapColumn<int32_t> l_shipdate;
    gendb::MmapColumn<double> l_extendedprice;
    gendb::MmapColumn<double> l_discount;
    gendb::MmapColumn<uint64_t> l_order_offsets;
    gendb::MmapColumn<uint64_t> l_order_rowids;

    std::vector<uint64_t> customer_key_bitset;
    std::vector<ResultRow> merged;

    {
        GENDB_PHASE("data_loading");

        c_custkey.open(gendb_dir + "/customer/c_custkey.bin");
        c_dict_offsets.open(gendb_dir + "/customer/c_mktsegment.dict.offsets.bin");
        c_dict_data.open(gendb_dir + "/customer/c_mktsegment.dict.data.bin");
        c_seg_offsets.open(gendb_dir + "/customer/indexes/customer_segment_postings.offsets.bin");
        c_seg_rowids.open(gendb_dir + "/customer/indexes/customer_segment_postings.rowids.bin");

        o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
        o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        o_shippriority.open(gendb_dir + "/orders/o_shippriority.bin");
        o_orderdate_zonemap.open(gendb_dir + "/orders/indexes/orders_orderdate_zonemap.bin");

        l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        l_order_offsets.open(gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin");
        l_order_rowids.open(gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin");

        o_custkey.advise_sequential();
        o_orderkey.advise_sequential();
        o_orderdate.advise_sequential();
        o_shippriority.advise_sequential();
        o_orderdate_zonemap.advise_sequential();
        l_order_offsets.advise_random();
        l_order_rowids.advise_random();
        l_shipdate.advise_random();
        l_extendedprice.advise_random();
        l_discount.advise_random();
    }

    {
        GENDB_PHASE("dim_filter");

        const int32_t building_code = find_segment_code(c_dict_offsets, c_dict_data, "BUILDING");
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

        const uint32_t max_customer_key = static_cast<uint32_t>(o_custkey.size());
        customer_key_bitset.assign((static_cast<size_t>(max_customer_key) + 64U) >> 6, 0ULL);

        for (uint64_t pos = begin; pos < end; ++pos) {
            const uint64_t customer_rowid = c_seg_rowids[pos];
            if (customer_rowid >= c_custkey.size()) {
                continue;
            }
            const int32_t custkey = c_custkey[customer_rowid];
            if (custkey > 0 && static_cast<uint32_t>(custkey) <= max_customer_key) {
                bitset_set(customer_key_bitset, static_cast<uint32_t>(custkey));
            }
        }
    }

    {
        GENDB_PHASE("build_joins");
    }

    {
        GENDB_PHASE("main_scan");

        const size_t orders_count = o_orderdate.size();
        const size_t orderkey_slots = l_order_offsets.size() > 0 ? l_order_offsets.size() - 1 : 0;
        const int thread_count = omp_get_max_threads();
        std::vector<TopK> local_topk(static_cast<size_t>(thread_count));

        #pragma omp parallel
        {
            TopK& topk = local_topk[static_cast<size_t>(omp_get_thread_num())];

            #pragma omp for schedule(dynamic, 1)
            for (size_t block = 0; block < o_orderdate_zonemap.size(); ++block) {
                const ZoneMap1I32 zone = o_orderdate_zonemap[block];
                if (zone.min_value >= cutoff_date) {
                    continue;
                }

                const size_t block_begin = block * static_cast<size_t>(kOrdersBlockSize);
                if (block_begin >= orders_count) {
                    continue;
                }
                const size_t block_end =
                    std::min(block_begin + static_cast<size_t>(kOrdersBlockSize), orders_count);
                const bool full_qualifying = zone.max_value < cutoff_date;

                for (size_t order_rowid = block_begin; order_rowid < block_end; ++order_rowid) {
                    if (!full_qualifying && o_orderdate[order_rowid] >= cutoff_date) {
                        continue;
                    }

                    const int32_t custkey = o_custkey[order_rowid];
                    if (custkey <= 0 ||
                        static_cast<uint32_t>(custkey) > static_cast<uint32_t>(o_custkey.size()) ||
                        !bitset_test(customer_key_bitset, static_cast<uint32_t>(custkey))) {
                        continue;
                    }

                    const int32_t orderkey = o_orderkey[order_rowid];
                    if (orderkey <= 0 || static_cast<size_t>(orderkey) >= orderkey_slots) {
                        continue;
                    }

                    const uint64_t line_begin = l_order_offsets[static_cast<size_t>(orderkey)];
                    const uint64_t line_end = l_order_offsets[static_cast<size_t>(orderkey) + 1];
                    double revenue = 0.0;

                    for (uint64_t line_pos = line_begin; line_pos < line_end; ++line_pos) {
                        const uint64_t line_rowid = l_order_rowids[line_pos];
                        if (l_shipdate[line_rowid] <= cutoff_date) {
                            continue;
                        }
                        revenue += l_extendedprice[line_rowid] * (1.0 - l_discount[line_rowid]);
                    }

                    if (revenue > 0.0) {
                        topk.push(ResultRow{
                            orderkey,
                            revenue,
                            o_orderdate[order_rowid],
                            o_shippriority[order_rowid],
                        });
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
