#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <stdexcept>
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

constexpr uint32_t kOrdersBlockSize = 131072;
constexpr int kTopK = 10;

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

struct Partition {
    uint64_t row_begin;
    uint64_t row_end;
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

static inline bool bitset_test(const std::vector<uint64_t>& bits, uint32_t key) {
    return ((bits[key >> 6] >> (key & 63U)) & 1ULL) != 0ULL;
}

static inline void bitset_set(std::vector<uint64_t>& bits, uint32_t key) {
    bits[key >> 6] |= (1ULL << (key & 63U));
}

static void ensure_results_dir(const std::string& dir) {
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        std::perror(("mkdir: " + dir).c_str());
        throw std::runtime_error("failed to create results directory");
    }
}

static std::vector<Partition> build_partitions(const gendb::MmapColumn<uint64_t>& offsets,
                                               uint64_t row_count,
                                               int partition_count) {
    std::vector<Partition> partitions(static_cast<size_t>(partition_count));
    const uint64_t* begin = offsets.data;
    const uint64_t* end = offsets.data + offsets.size();

    for (int t = 0; t < partition_count; ++t) {
        const uint64_t target_begin =
            (row_count * static_cast<uint64_t>(t)) / static_cast<uint64_t>(partition_count);
        const uint64_t target_end =
            (row_count * static_cast<uint64_t>(t + 1)) / static_cast<uint64_t>(partition_count);

        const uint64_t row_begin =
            *std::lower_bound(begin, end, target_begin);
        const uint64_t row_end = (t + 1 == partition_count)
                                     ? row_count
                                     : *std::lower_bound(begin, end, target_end);
        partitions[static_cast<size_t>(t)] = Partition{row_begin, row_end};
    }
    return partitions;
}

}  // namespace

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

    gendb::MmapColumn<int32_t> l_orderkey;
    gendb::MmapColumn<int32_t> l_shipdate;
    gendb::MmapColumn<double> l_extendedprice;
    gendb::MmapColumn<double> l_discount;
    gendb::MmapColumn<uint64_t> l_order_offsets;

    std::vector<uint64_t> customer_key_bitset;
    std::vector<uint64_t> qualifying_order_bitset;
    std::vector<int32_t> qualifying_order_dates;
    std::vector<int32_t> qualifying_order_shippriority;
    std::vector<ResultRow> merged_rows;

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

        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        l_order_offsets.open(gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin");

        c_custkey.advise_random();
        c_seg_offsets.advise_random();
        c_seg_rowids.advise_random();
        o_custkey.advise_sequential();
        o_orderkey.advise_sequential();
        o_orderdate.advise_sequential();
        o_shippriority.advise_sequential();
        o_orderdate_zonemap.advise_sequential();
        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();
        l_order_offsets.advise_random();
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

        customer_key_bitset.assign((c_custkey.size() + 64ULL) >> 6, 0ULL);
        for (uint64_t pos = begin; pos < end; ++pos) {
            const uint64_t customer_rowid = c_seg_rowids[pos];
            if (customer_rowid >= c_custkey.size()) {
                continue;
            }
            const int32_t custkey = c_custkey[customer_rowid];
            if (custkey > 0 && static_cast<size_t>(custkey) <= c_custkey.size()) {
                bitset_set(customer_key_bitset, static_cast<uint32_t>(custkey));
            }
        }
    }

    {
        GENDB_PHASE("build_joins");

        const size_t order_slots = l_order_offsets.size() == 0 ? 0 : (l_order_offsets.size() - 1);
        qualifying_order_bitset.assign((order_slots + 64ULL) >> 6, 0ULL);
        qualifying_order_dates.assign(order_slots, 0);
        qualifying_order_shippriority.assign(order_slots, 0);

        const size_t orders_count = o_orderkey.size();

        #pragma omp parallel for schedule(static)
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

            for (size_t row = block_begin; row < block_end; ++row) {
                if (!full_qualifying && o_orderdate[row] >= cutoff_date) {
                    continue;
                }

                const int32_t custkey = o_custkey[row];
                if (custkey <= 0 ||
                    static_cast<size_t>(custkey) > c_custkey.size() ||
                    !bitset_test(customer_key_bitset, static_cast<uint32_t>(custkey))) {
                    continue;
                }

                const int32_t orderkey = o_orderkey[row];
                if (orderkey <= 0 || static_cast<size_t>(orderkey) >= order_slots) {
                    continue;
                }

                const size_t slot = static_cast<size_t>(orderkey);
                bitset_set(qualifying_order_bitset, static_cast<uint32_t>(orderkey));
                qualifying_order_dates[slot] = o_orderdate[row];
                qualifying_order_shippriority[slot] = o_shippriority[row];
            }
        }
    }

    {
        GENDB_PHASE("main_scan");

        const int thread_count = omp_get_max_threads();
        const uint64_t lineitem_rows = static_cast<uint64_t>(l_orderkey.size());
        const size_t order_slots = l_order_offsets.size() == 0 ? 0 : (l_order_offsets.size() - 1);
        const std::vector<Partition> partitions =
            build_partitions(l_order_offsets, lineitem_rows, thread_count);
        std::vector<TopK> local_topk(static_cast<size_t>(thread_count));

        #pragma omp parallel for schedule(static, 1)
        for (int part = 0; part < thread_count; ++part) {
            const Partition partition = partitions[static_cast<size_t>(part)];
            if (partition.row_begin >= partition.row_end) {
                continue;
            }

            TopK topk;
            uint64_t row = partition.row_begin;
            int32_t current_orderkey = l_orderkey[row];
            bool current_qualifies =
                current_orderkey > 0 &&
                static_cast<size_t>(current_orderkey) < order_slots &&
                bitset_test(qualifying_order_bitset, static_cast<uint32_t>(current_orderkey));
            int32_t current_orderdate =
                current_qualifies ? qualifying_order_dates[static_cast<size_t>(current_orderkey)] : 0;
            int32_t current_shippriority =
                current_qualifies ? qualifying_order_shippriority[static_cast<size_t>(current_orderkey)] : 0;
            double current_revenue = 0.0;

            for (; row < partition.row_end; ++row) {
                const int32_t orderkey = l_orderkey[row];
                if (orderkey != current_orderkey) {
                    if (current_qualifies && current_revenue > 0.0) {
                        topk.push(ResultRow{
                            current_orderkey,
                            current_revenue,
                            current_orderdate,
                            current_shippriority,
                        });
                    }

                    current_orderkey = orderkey;
                    current_qualifies =
                        current_orderkey > 0 &&
                        static_cast<size_t>(current_orderkey) < order_slots &&
                        bitset_test(qualifying_order_bitset, static_cast<uint32_t>(current_orderkey));
                    current_orderdate = current_qualifies
                                            ? qualifying_order_dates[static_cast<size_t>(current_orderkey)]
                                            : 0;
                    current_shippriority = current_qualifies
                                               ? qualifying_order_shippriority[static_cast<size_t>(current_orderkey)]
                                               : 0;
                    current_revenue = 0.0;
                }

                if (current_qualifies && l_shipdate[row] > cutoff_date) {
                    current_revenue += l_extendedprice[row] * (1.0 - l_discount[row]);
                }
            }

            if (current_qualifies && current_revenue > 0.0) {
                topk.push(ResultRow{
                    current_orderkey,
                    current_revenue,
                    current_orderdate,
                    current_shippriority,
                });
            }

            local_topk[static_cast<size_t>(part)] = topk;
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

        const size_t limit = std::min<size_t>(kTopK, merged_rows.size());
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
            std::perror(("fopen: " + out_path).c_str());
            throw std::runtime_error("failed to open output file");
        }

        gendb::init_date_tables();
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
