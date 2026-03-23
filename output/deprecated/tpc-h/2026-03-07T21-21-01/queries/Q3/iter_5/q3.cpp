#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
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
constexpr uint64_t kTargetLineitemsPerPartition = 524288;

struct ResultRow {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrderInfo {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrderkeyPartition {
    uint32_t orderkey_begin;
    uint32_t orderkey_end;
    uint64_t li_row_begin;
    uint64_t li_row_end;
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

static std::vector<OrderkeyPartition> build_orderkey_partitions(
    const gendb::MmapColumn<uint64_t>& offsets,
    int thread_count) {
    const uint32_t orderkey_slots =
        offsets.size() == 0 ? 0U : static_cast<uint32_t>(offsets.size() - 1);
    const uint64_t total_lineitems = offsets.size() == 0 ? 0ULL : offsets[offsets.size() - 1];
    if (orderkey_slots == 0) {
        return {};
    }

    const uint64_t target_from_rows =
        (total_lineitems + kTargetLineitemsPerPartition - 1) / kTargetLineitemsPerPartition;
    uint32_t partition_count = static_cast<uint32_t>(
        std::max<uint64_t>(static_cast<uint64_t>(thread_count) * 2ULL, target_from_rows));
    partition_count = std::max<uint32_t>(1U, std::min<uint32_t>(partition_count, orderkey_slots));

    std::vector<OrderkeyPartition> partitions;
    partitions.reserve(partition_count);

    uint32_t key_begin = 0;
    const uint64_t* offsets_begin = offsets.data;
    const uint64_t* offsets_end = offsets.data + offsets.size();
    for (uint32_t part = 1; part < partition_count; ++part) {
        const uint64_t target =
            (total_lineitems * static_cast<uint64_t>(part)) / static_cast<uint64_t>(partition_count);
        uint32_t key_end = static_cast<uint32_t>(
            std::lower_bound(offsets_begin + key_begin, offsets_end, target) - offsets_begin);
        if (key_end > orderkey_slots) {
            key_end = orderkey_slots;
        }
        partitions.push_back(OrderkeyPartition{
            key_begin, key_end, offsets[key_begin], offsets[key_end]});
        key_begin = key_end;
    }

    partitions.push_back(OrderkeyPartition{
        key_begin, orderkey_slots, offsets[key_begin], offsets[orderkey_slots]});
    return partitions;
}

static size_t find_partition_for_key(const std::vector<OrderkeyPartition>& partitions,
                                     uint32_t orderkey) {
    size_t lo = 0;
    size_t hi = partitions.size();
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1U);
        if (orderkey < partitions[mid].orderkey_end) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
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
    std::vector<OrderkeyPartition> partitions;
    std::vector<std::vector<OrderInfo>> partition_orders;
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
        l_order_offsets.advise_random();
        l_order_rowids.advise_sequential();
        l_shipdate.advise_random();
        l_extendedprice.advise_random();
        l_discount.advise_random();

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
            throw std::runtime_error("BUILDING code outside customer_segment_postings");
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

        partitions = build_orderkey_partitions(l_order_offsets, thread_count);
        partition_orders.resize(partitions.size());

        std::vector<std::vector<OrderInfo>> local_orders(static_cast<size_t>(thread_count));

        #pragma omp parallel num_threads(thread_count)
        {
            const int tid = omp_get_thread_num();
            const size_t total = building_custkeys.size();
            const size_t begin =
                (total * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
            const size_t end =
                (total * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
            std::vector<OrderInfo>& out = local_orders[static_cast<size_t>(tid)];

            if (begin < end) {
                out.reserve((end - begin) * 6);
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
                    if (order_rowid >= o_orderkey.size()) {
                        continue;
                    }

                    const int32_t orderdate = o_orderdate[order_rowid];
                    if (orderdate >= cutoff_date) {
                        continue;
                    }

                    const int32_t orderkey = o_orderkey[order_rowid];
                    if (orderkey <= 0) {
                        continue;
                    }

                    const size_t key_slot = static_cast<size_t>(orderkey);
                    if (key_slot + 1 >= l_order_offsets.size()) {
                        continue;
                    }

                    out.push_back(OrderInfo{orderkey, orderdate, o_shippriority[order_rowid]});
                }
            }
        }

        size_t total_qualifying_orders = 0;
        for (const auto& local : local_orders) {
            total_qualifying_orders += local.size();
        }

        std::vector<uint32_t> partition_counts(partitions.size(), 0U);
        for (const auto& local : local_orders) {
            for (const OrderInfo& order : local) {
                const size_t pid =
                    find_partition_for_key(partitions, static_cast<uint32_t>(order.orderkey));
                if (pid < partitions.size()) {
                    ++partition_counts[pid];
                }
            }
        }

        for (size_t pid = 0; pid < partitions.size(); ++pid) {
            partition_orders[pid].reserve(partition_counts[pid]);
        }

        for (auto& local : local_orders) {
            for (const OrderInfo& order : local) {
                const size_t pid =
                    find_partition_for_key(partitions, static_cast<uint32_t>(order.orderkey));
                if (pid < partitions.size()) {
                    partition_orders[pid].push_back(order);
                }
            }
            std::vector<OrderInfo>().swap(local);
        }

        (void)total_qualifying_orders;
    }

    {
        GENDB_PHASE("main_scan");

        std::vector<TopK> thread_topk(static_cast<size_t>(thread_count));

        #pragma omp parallel for schedule(dynamic, 1) num_threads(thread_count)
        for (size_t pid = 0; pid < partition_orders.size(); ++pid) {
            const int tid = omp_get_thread_num();
            TopK& local_topk = thread_topk[static_cast<size_t>(tid)];
            std::vector<OrderInfo>& orders = partition_orders[pid];
            if (orders.empty()) {
                continue;
            }

            std::sort(orders.begin(), orders.end(), [](const OrderInfo& lhs, const OrderInfo& rhs) {
                return lhs.orderkey < rhs.orderkey;
            });

            const OrderkeyPartition part = partitions[pid];
            uint64_t postings_cursor = part.li_row_begin;
            (void)postings_cursor;

            for (const OrderInfo& order : orders) {
                const uint32_t orderkey = static_cast<uint32_t>(order.orderkey);
                const uint64_t begin = l_order_offsets[orderkey];
                const uint64_t end = l_order_offsets[orderkey + 1];
                if (end <= begin) {
                    continue;
                }

                double revenue = 0.0;
                for (uint64_t pos = begin; pos < end; ++pos) {
                    const uint64_t lineitem_rowid = l_order_rowids[pos];
                    if (lineitem_rowid >= l_shipdate.size()) {
                        continue;
                    }
                    if (l_shipdate[lineitem_rowid] <= cutoff_date) {
                        continue;
                    }
                    revenue += l_extendedprice[lineitem_rowid] *
                               (1.0 - l_discount[lineitem_rowid]);
                }

                if (revenue > 0.0) {
                    local_topk.push(ResultRow{
                        order.orderkey, revenue, order.orderdate, order.shippriority});
                }
            }
        }

        merged_rows.reserve(static_cast<size_t>(thread_count) * kTopK);
        for (const TopK& local_topk : thread_topk) {
            for (int i = 0; i < local_topk.count; ++i) {
                merged_rows.push_back(local_topk.rows[i]);
            }
        }
    }

    {
        GENDB_PHASE("output");

        std::sort(merged_rows.begin(), merged_rows.end(), result_better);
        if (merged_rows.size() > static_cast<size_t>(kTopK)) {
            merged_rows.resize(static_cast<size_t>(kTopK));
        }

        ensure_results_dir(results_dir);
        const std::string output_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(output_path.c_str(), "w");
        if (out == nullptr) {
            throw std::runtime_error("failed to open output file");
        }

        std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[11];
        for (const ResultRow& row : merged_rows) {
            gendb::epoch_days_to_date_str(row.orderdate, date_buf);
            std::fprintf(out, "%d,%.2f,%s,%d\n",
                         row.orderkey,
                         row.revenue,
                         date_buf,
                         row.shippriority);
        }
        std::fclose(out);
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        run_q3(argv[1], argv[2]);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "q3 failed: %s\n", e.what());
        return 1;
    }
    return 0;
}
