#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "date_utils.h"
#include "timing_utils.h"

namespace {

struct MmapRegion {
    void* ptr = nullptr;
    size_t sz = 0;

    bool valid() const { return ptr != nullptr && ptr != MAP_FAILED; }

    template <typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(ptr);
    }

    void unmap() {
        if (valid()) {
            munmap(ptr, sz);
            ptr = nullptr;
            sz = 0;
        }
    }
};

static MmapRegion do_mmap(const std::string& path, bool sequential) {
    MmapRegion region;
    const int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::perror(("open: " + path).c_str());
        return region;
    }

    struct stat st {};
    if (fstat(fd, &st) != 0) {
        std::perror(("fstat: " + path).c_str());
        close(fd);
        return region;
    }

    region.sz = static_cast<size_t>(st.st_size);
    region.ptr = mmap(nullptr, region.sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (region.ptr == MAP_FAILED) {
        std::perror(("mmap: " + path).c_str());
        region.ptr = nullptr;
        region.sz = 0;
        close(fd);
        return region;
    }

    if (sequential) {
        madvise(region.ptr, region.sz, MADV_SEQUENTIAL);
        posix_fadvise(fd, 0, static_cast<off_t>(region.sz), POSIX_FADV_SEQUENTIAL);
    } else {
        madvise(region.ptr, region.sz, MADV_RANDOM);
    }
    close(fd);
    return region;
}

struct ResultRow {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

static bool result_better(const ResultRow& a, const ResultRow& b) {
    if (a.revenue != b.revenue) {
        return a.revenue > b.revenue;
    }
    if (a.orderdate != b.orderdate) {
        return a.orderdate < b.orderdate;
    }
    if (a.shippriority != b.shippriority) {
        return a.shippriority < b.shippriority;
    }
    return a.orderkey < b.orderkey;
}

struct TopK {
    std::array<ResultRow, 10> rows {};
    int count = 0;
    int worst = 0;

    void recompute_worst() {
        worst = 0;
        for (int i = 1; i < count; ++i) {
            if (result_better(rows[worst], rows[i])) {
                worst = i;
            }
        }
    }

    void push(const ResultRow& row) {
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

static int32_t find_segment_code(const uint64_t* offsets,
                                 size_t offset_count,
                                 const char* data) {
    for (size_t code = 0; code + 1 < offset_count; ++code) {
        const uint64_t begin = offsets[code];
        const uint64_t end = offsets[code + 1];
        const uint64_t len = end - begin;
        if (len == 8 && std::memcmp(data + begin, "BUILDING", 8) == 0) {
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

    static constexpr char kDateStr[] = "1995-03-15";
    const int32_t cutoff_date = gendb::date_str_to_epoch_days(kDateStr);

    std::vector<MmapRegion> regions;
    regions.reserve(16);

    const int32_t* customer_custkey = nullptr;
    const uint64_t* customer_segment_offsets = nullptr;
    const uint64_t* customer_segment_rowids = nullptr;
    size_t customer_segment_offsets_count = 0;
    size_t customer_segment_rowids_count = 0;
    const uint64_t* customer_dict_offsets = nullptr;
    size_t customer_dict_offsets_count = 0;
    const char* customer_dict_data = nullptr;

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

    {
        GENDB_PHASE("data_loading");

        auto c_custkey_region = do_mmap(gendb_dir + "/customer/c_custkey.bin", true);
        auto c_dict_offsets_region =
            do_mmap(gendb_dir + "/customer/c_mktsegment.dict.offsets.bin", true);
        auto c_dict_data_region =
            do_mmap(gendb_dir + "/customer/c_mktsegment.dict.data.bin", true);
        auto c_seg_offsets_region =
            do_mmap(gendb_dir + "/customer/indexes/customer_segment_postings.offsets.bin", true);
        auto c_seg_rowids_region =
            do_mmap(gendb_dir + "/customer/indexes/customer_segment_postings.rowids.bin", true);

        auto o_orderkey_region = do_mmap(gendb_dir + "/orders/o_orderkey.bin", false);
        auto o_orderdate_region = do_mmap(gendb_dir + "/orders/o_orderdate.bin", false);
        auto o_shippriority_region = do_mmap(gendb_dir + "/orders/o_shippriority.bin", false);
        auto o_cust_offsets_region =
            do_mmap(gendb_dir + "/orders/indexes/orders_cust_postings.offsets.bin", false);
        auto o_cust_rowids_region =
            do_mmap(gendb_dir + "/orders/indexes/orders_cust_postings.rowids.bin", false);

        auto l_shipdate_region = do_mmap(gendb_dir + "/lineitem/l_shipdate.bin", false);
        auto l_extendedprice_region = do_mmap(gendb_dir + "/lineitem/l_extendedprice.bin", false);
        auto l_discount_region = do_mmap(gendb_dir + "/lineitem/l_discount.bin", false);
        auto l_order_offsets_region =
            do_mmap(gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin", false);
        auto l_order_rowids_region =
            do_mmap(gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin", false);

        if (!c_custkey_region.valid() || !c_dict_offsets_region.valid() ||
            !c_dict_data_region.valid() || !c_seg_offsets_region.valid() ||
            !c_seg_rowids_region.valid() || !o_orderkey_region.valid() ||
            !o_orderdate_region.valid() || !o_shippriority_region.valid() ||
            !o_cust_offsets_region.valid() || !o_cust_rowids_region.valid() ||
            !l_shipdate_region.valid() || !l_extendedprice_region.valid() ||
            !l_discount_region.valid() || !l_order_offsets_region.valid() ||
            !l_order_rowids_region.valid()) {
            std::fprintf(stderr, "failed to mmap one or more Q3 inputs\n");
            return;
        }

        customer_custkey = c_custkey_region.as<int32_t>();
        customer_dict_offsets = c_dict_offsets_region.as<uint64_t>();
        customer_dict_offsets_count = c_dict_offsets_region.sz / sizeof(uint64_t);
        customer_dict_data = c_dict_data_region.as<char>();
        customer_segment_offsets = c_seg_offsets_region.as<uint64_t>();
        customer_segment_offsets_count = c_seg_offsets_region.sz / sizeof(uint64_t);
        customer_segment_rowids = c_seg_rowids_region.as<uint64_t>();
        customer_segment_rowids_count = c_seg_rowids_region.sz / sizeof(uint64_t);

        orders_orderkey = o_orderkey_region.as<int32_t>();
        orders_orderdate = o_orderdate_region.as<int32_t>();
        orders_shippriority = o_shippriority_region.as<int32_t>();
        orders_cust_offsets = o_cust_offsets_region.as<uint64_t>();
        orders_cust_rowids = o_cust_rowids_region.as<uint64_t>();
        orders_cust_offsets_count = o_cust_offsets_region.sz / sizeof(uint64_t);

        lineitem_shipdate = l_shipdate_region.as<int32_t>();
        lineitem_extendedprice = l_extendedprice_region.as<double>();
        lineitem_discount = l_discount_region.as<double>();
        lineitem_order_offsets = l_order_offsets_region.as<uint64_t>();
        lineitem_order_rowids = l_order_rowids_region.as<uint64_t>();
        lineitem_order_offsets_count = l_order_offsets_region.sz / sizeof(uint64_t);

        regions.push_back(c_custkey_region);
        regions.push_back(c_dict_offsets_region);
        regions.push_back(c_dict_data_region);
        regions.push_back(c_seg_offsets_region);
        regions.push_back(c_seg_rowids_region);
        regions.push_back(o_orderkey_region);
        regions.push_back(o_orderdate_region);
        regions.push_back(o_shippriority_region);
        regions.push_back(o_cust_offsets_region);
        regions.push_back(o_cust_rowids_region);
        regions.push_back(l_shipdate_region);
        regions.push_back(l_extendedprice_region);
        regions.push_back(l_discount_region);
        regions.push_back(l_order_offsets_region);
        regions.push_back(l_order_rowids_region);
    }

    std::vector<uint64_t> building_customer_rowids;
    std::vector<int32_t> building_customer_keys;

    {
        GENDB_PHASE("dim_filter");

        const int32_t building_code =
            find_segment_code(customer_dict_offsets, customer_dict_offsets_count, customer_dict_data);
        if (building_code < 0) {
            std::fprintf(stderr, "BUILDING not found in customer segment dictionary\n");
            return;
        }
        if (static_cast<size_t>(building_code + 1) >= customer_segment_offsets_count) {
            std::fprintf(stderr, "BUILDING code outside customer segment postings\n");
            return;
        }

        const uint64_t begin = customer_segment_offsets[building_code];
        const uint64_t end = customer_segment_offsets[building_code + 1];
        if (end < begin || end > customer_segment_rowids_count) {
            std::fprintf(stderr, "invalid BUILDING postings slice\n");
            return;
        }

        const size_t building_count = static_cast<size_t>(end - begin);
        building_customer_rowids.resize(building_count);
        building_customer_keys.resize(building_count);

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < building_count; ++i) {
            const uint64_t rowid = customer_segment_rowids[begin + i];
            building_customer_rowids[i] = rowid;
            building_customer_keys[i] = customer_custkey[rowid];
        }
    }

    std::vector<int32_t> qualifying_orderkeys;
    std::vector<int32_t> qualifying_orderdates;
    std::vector<int32_t> qualifying_shippriorities;
    std::vector<double> qualifying_revenue;

    {
        GENDB_PHASE("build_joins");

        const int thread_count = omp_get_max_threads();
        std::vector<std::vector<int32_t>> local_orderkeys(thread_count);
        std::vector<std::vector<int32_t>> local_orderdates(thread_count);
        std::vector<std::vector<int32_t>> local_shippriorities(thread_count);

        const size_t expected_per_thread =
            (building_customer_keys.size() / static_cast<size_t>(thread_count)) * 6 + 1024;
        for (int t = 0; t < thread_count; ++t) {
            local_orderkeys[t].reserve(expected_per_thread);
            local_orderdates[t].reserve(expected_per_thread);
            local_shippriorities[t].reserve(expected_per_thread);
        }

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            auto& out_orderkeys = local_orderkeys[tid];
            auto& out_orderdates = local_orderdates[tid];
            auto& out_shippriorities = local_shippriorities[tid];

            #pragma omp for schedule(dynamic, 1024)
            for (size_t i = 0; i < building_customer_keys.size(); ++i) {
                const int32_t custkey = building_customer_keys[i];
                if (custkey <= 0) {
                    continue;
                }

                const size_t key = static_cast<size_t>(custkey);
                if (key + 1 >= orders_cust_offsets_count) {
                    continue;
                }

                const uint64_t begin = orders_cust_offsets[key];
                const uint64_t end = orders_cust_offsets[key + 1];
                for (uint64_t pos = begin; pos < end; ++pos) {
                    const uint64_t order_rowid = orders_cust_rowids[pos];
                    const int32_t orderdate = orders_orderdate[order_rowid];
                    if (orderdate >= cutoff_date) {
                        continue;
                    }
                    out_orderkeys.push_back(orders_orderkey[order_rowid]);
                    out_orderdates.push_back(orderdate);
                    out_shippriorities.push_back(orders_shippriority[order_rowid]);
                }
            }
        }

        std::vector<size_t> offsets(static_cast<size_t>(thread_count) + 1, 0);
        for (int t = 0; t < thread_count; ++t) {
            offsets[t + 1] = offsets[t] + local_orderkeys[t].size();
        }

        const size_t qualifying_count = offsets.back();
        qualifying_orderkeys.resize(qualifying_count);
        qualifying_orderdates.resize(qualifying_count);
        qualifying_shippriorities.resize(qualifying_count);
        qualifying_revenue.assign(qualifying_count, 0.0);

        #pragma omp parallel for schedule(static)
        for (int t = 0; t < thread_count; ++t) {
            const size_t out_begin = offsets[t];
            const size_t n = local_orderkeys[t].size();
            if (n == 0) {
                continue;
            }
            std::memcpy(qualifying_orderkeys.data() + out_begin,
                        local_orderkeys[t].data(),
                        n * sizeof(int32_t));
            std::memcpy(qualifying_orderdates.data() + out_begin,
                        local_orderdates[t].data(),
                        n * sizeof(int32_t));
            std::memcpy(qualifying_shippriorities.data() + out_begin,
                        local_shippriorities[t].data(),
                        n * sizeof(int32_t));
        }
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 512)
        for (size_t i = 0; i < qualifying_orderkeys.size(); ++i) {
            const int32_t orderkey = qualifying_orderkeys[i];
            if (orderkey <= 0) {
                continue;
            }

            const size_t key = static_cast<size_t>(orderkey);
            if (key + 1 >= lineitem_order_offsets_count) {
                continue;
            }

            const uint64_t begin = lineitem_order_offsets[key];
            const uint64_t end = lineitem_order_offsets[key + 1];
            double revenue = 0.0;
            for (uint64_t pos = begin; pos < end; ++pos) {
                const uint64_t lineitem_rowid = lineitem_order_rowids[pos];
                if (lineitem_shipdate[lineitem_rowid] <= cutoff_date) {
                    continue;
                }
                revenue += lineitem_extendedprice[lineitem_rowid] *
                           (1.0 - lineitem_discount[lineitem_rowid]);
            }
            qualifying_revenue[i] = revenue;
        }
    }

    {
        GENDB_PHASE("output");

        const int thread_count = omp_get_max_threads();
        std::vector<TopK> local_topk(static_cast<size_t>(thread_count));

        #pragma omp parallel
        {
            TopK& topk = local_topk[static_cast<size_t>(omp_get_thread_num())];
            #pragma omp for schedule(static)
            for (size_t i = 0; i < qualifying_orderkeys.size(); ++i) {
                if (qualifying_revenue[i] <= 0.0) {
                    continue;
                }
                topk.push(ResultRow{qualifying_orderkeys[i],
                                    qualifying_revenue[i],
                                    qualifying_orderdates[i],
                                    qualifying_shippriorities[i]});
            }
        }

        std::vector<ResultRow> merged;
        merged.reserve(static_cast<size_t>(thread_count) * 10);
        for (const TopK& topk : local_topk) {
            for (int i = 0; i < topk.count; ++i) {
                merged.push_back(topk.rows[i]);
            }
        }

        const size_t limit = std::min<size_t>(10, merged.size());
        std::partial_sort(merged.begin(),
                          merged.begin() + static_cast<std::ptrdiff_t>(limit),
                          merged.end(),
                          result_better);

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

    for (auto& region : regions) {
        region.unmap();
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
