#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// Lightweight mmap wrapper
struct MMap {
    void* ptr = nullptr;
    size_t sz = 0;
    int fd = -1;
    void open(const std::string& path, bool populate = true, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st); sz = st.st_size;
        int flags = MAP_PRIVATE;
        if (populate) flags |= MAP_POPULATE;
        ptr = mmap(nullptr, sz, PROT_READ, flags, fd, 0);
        madvise(ptr, sz, advice);
    }
    ~MMap() { if (ptr && ptr != MAP_FAILED) munmap(ptr, sz); if (fd >= 0) ::close(fd); }
    template<typename T> const T* as() const { return (const T*)ptr; }
    template<typename T> size_t count() const { return sz / sizeof(T); }
};

struct QualOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
    double revenue;
};

int main(int argc, char* argv[]) {
    gendb::init_date_tables();

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int32_t o_orderdate_upper = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", 9204);
    int32_t l_shipdate_lower = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9204);
    int64_t limit_val = gendb::parse_int_arg(argc, argv, "--limit", 10);
    std::string c_mktsegment_eq = gendb::parse_string_arg(argc, argv, "--c_mktsegment_eq", "BUILDING");

    GENDB_PHASE("total");

    // === Data Loading ===
    MMap mf_c_seg, mf_c_ck;
    MMap mf_o_ck, mf_o_date, mf_o_ok, mf_o_sp;
    MMap mf_l_sd, mf_l_ep, mf_l_disc;
    MMap mf_li_idx;

    {
        GENDB_PHASE("data_loading");
        // Customer + Orders: sequential scan, use MAP_POPULATE
        mf_c_seg.open(gendb_dir + "/customer/c_mktsegment.bin");
        mf_c_ck.open(gendb_dir + "/customer/c_custkey.bin");
        mf_o_ck.open(gendb_dir + "/orders/o_custkey.bin");
        mf_o_date.open(gendb_dir + "/orders/o_orderdate.bin");
        mf_o_ok.open(gendb_dir + "/orders/o_orderkey.bin");
        mf_o_sp.open(gendb_dir + "/orders/o_shippriority.bin");
        // Lineitem: accessed via index (random), no MAP_POPULATE
        mf_l_sd.open(gendb_dir + "/lineitem/l_shipdate.bin", false, MADV_RANDOM);
        mf_l_ep.open(gendb_dir + "/lineitem/l_extendedprice.bin", false, MADV_RANDOM);
        mf_l_disc.open(gendb_dir + "/lineitem/l_discount.bin", false, MADV_RANDOM);
        mf_li_idx.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin", true, MADV_RANDOM);
    }

    const int8_t* c_mktsegment = mf_c_seg.as<int8_t>();
    const int32_t* c_custkey = mf_c_ck.as<int32_t>();
    size_t num_customers = mf_c_ck.count<int32_t>();

    const int32_t* o_custkey = mf_o_ck.as<int32_t>();
    const int32_t* o_orderdate = mf_o_date.as<int32_t>();
    const int32_t* o_orderkey = mf_o_ok.as<int32_t>();
    const int32_t* o_shippriority = mf_o_sp.as<int32_t>();
    size_t num_orders = mf_o_ck.count<int32_t>();

    const int32_t* l_shipdate = mf_l_sd.as<int32_t>();
    const double* l_extendedprice = mf_l_ep.as<double>();
    const double* l_discount = mf_l_disc.as<double>();

    uint64_t li_idx_entries = *(const uint64_t*)mf_li_idx.ptr;
    const uint32_t* li_idx = (const uint32_t*)((const char*)mf_li_idx.ptr + 8);

    // === Build Customer Bitmap (187KB, fits L1/L2) ===
    constexpr int32_t BITMAP_SIZE = 1500001;
    std::vector<uint64_t> cust_bitmap((BITMAP_SIZE + 63) / 64, 0);

    int8_t building_code = -1;
    {
        GENDB_PHASE("build_customer_bitmap");
        std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.bin", std::ios::binary);
        uint32_t dict_count; df.read((char*)&dict_count, 4);
        for (uint32_t d = 0; d < dict_count; d++) {
            uint16_t len; df.read((char*)&len, 2);
            std::string val(len, '\0'); df.read(val.data(), len);
            if (val == c_mktsegment_eq) building_code = (int8_t)d;
        }

        if (building_code >= 0) {
            for (size_t i = 0; i < num_customers; i++) {
                if (c_mktsegment[i] == building_code) {
                    int32_t ck = c_custkey[i];
                    cust_bitmap[ck >> 6] |= (1ULL << (ck & 63));
                }
            }
        }
    }

    // === Scan Orders: Collect Compact Qualifying Orders ===
    // Orders table is sorted by o_orderkey, static partitioning preserves sort order
    // per thread → we can merge instead of sort
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<QualOrder>> thread_results(nthreads);

    {
        GENDB_PHASE("scan_orders");
        size_t est = 4000000 / nthreads + 1024;
        for (auto& v : thread_results) v.reserve(est);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = thread_results[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < num_orders; i++) {
                if (o_orderdate[i] >= o_orderdate_upper) continue;
                int32_t ck = o_custkey[i];
                if (!(cust_bitmap[ck >> 6] & (1ULL << (ck & 63)))) continue;
                local.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i], 0.0});
            }
        }
    }

    // === Merge: just concatenate (orders sorted by o_orderkey + static partitioning
    //     = each thread's chunk covers a contiguous orderkey range, already sorted) ===
    std::vector<QualOrder> qual_orders;
    {
        GENDB_PHASE("merge_sort");
        size_t total_qual = 0;
        for (auto& v : thread_results) total_qual += v.size();
        qual_orders.reserve(total_qual);
        for (int t = 0; t < nthreads; t++) {
            qual_orders.insert(qual_orders.end(),
                thread_results[t].begin(), thread_results[t].end());
            thread_results[t].clear();
            thread_results[t].shrink_to_fit();
        }
        thread_results.clear();
        thread_results.shrink_to_fit();
    }

    // === Probe Lineitem + Aggregate Revenue (Fused) ===
    {
        GENDB_PHASE("main_scan");
        int64_t n_qual = (int64_t)qual_orders.size();

        #pragma omp parallel for schedule(dynamic, 4096)
        for (int64_t qi = 0; qi < n_qual; qi++) {
            int32_t ok = qual_orders[qi].orderkey;
            if (ok < 0 || (uint64_t)ok >= li_idx_entries) continue;

            uint32_t start = li_idx[ok * 2];
            uint32_t count = li_idx[ok * 2 + 1];
            if (count == 0) continue;

            double rev = 0.0;
            bool any = false;
            for (uint32_t r = start, end = start + count; r < end; r++) {
                if (l_shipdate[r] > l_shipdate_lower) {
                    rev += l_extendedprice[r] * (1.0 - l_discount[r]);
                    any = true;
                }
            }
            if (any) {
                qual_orders[qi].revenue = rev;
            }
        }
    }

    // === Top-K ===
    {
        GENDB_PHASE("topk");
        auto end_it = std::remove_if(qual_orders.begin(), qual_orders.end(),
            [](const QualOrder& q) { return q.revenue == 0.0; });
        qual_orders.erase(end_it, qual_orders.end());

        size_t k = std::min((size_t)limit_val, qual_orders.size());
        if (k > 0) {
            std::partial_sort(qual_orders.begin(), qual_orders.begin() + k, qual_orders.end(),
                [](const QualOrder& a, const QualOrder& b) {
                    if (a.revenue != b.revenue) return a.revenue > b.revenue;
                    return a.orderdate < b.orderdate;
                });
        }
        qual_orders.resize(k);
    }

    // === Output ===
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char datebuf[11];
        for (auto& q : qual_orders) {
            gendb::epoch_days_to_date_str(q.orderdate, datebuf);
            fprintf(fp, "%d,%.2f,%s,%d\n", q.orderkey, q.revenue, datebuf, q.shippriority);
        }
        fclose(fp);
    }

    return 0;
}
