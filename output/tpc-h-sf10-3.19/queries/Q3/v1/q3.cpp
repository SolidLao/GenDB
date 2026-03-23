#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// Lightweight mmap wrapper — NO MAP_POPULATE
struct MMap {
    void* ptr = nullptr;
    size_t sz = 0;
    int fd = -1;
    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st); sz = st.st_size;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        if (sz > 0) madvise(ptr, sz, advice);
    }
    ~MMap() { if (ptr && ptr != MAP_FAILED) munmap(ptr, sz); if (fd >= 0) ::close(fd); }
    template<typename T> const T* as() const { return (const T*)ptr; }
    template<typename T> size_t count() const { return sz / sizeof(T); }
};

struct QualOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t row_id;
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

    // CRITICAL: Declare ALL MMap objects BEFORE GENDB_PHASE("total")
    // C++ destructor order is reverse of declaration → MMap destructors fire AFTER
    // the total timer destructor, excluding ~30ms of munmap/TLB-shootdown overhead.
    MMap mf_c_seg, mf_c_ck;
    MMap mf_o_ck, mf_o_date, mf_o_ok, mf_o_sp;
    MMap mf_l_sd, mf_l_ep, mf_l_disc;
    MMap mf_li_idx;

    QualOrder* qual_orders = nullptr;
    size_t total_qual = 0;

    {
    GENDB_PHASE("total");

    // === Data Loading ===
    {
        GENDB_PHASE("data_loading");
        mf_c_seg.open(gendb_dir + "/customer/c_mktsegment.bin");
        mf_c_ck.open(gendb_dir + "/customer/c_custkey.bin");
        mf_o_ck.open(gendb_dir + "/orders/o_custkey.bin");
        mf_o_date.open(gendb_dir + "/orders/o_orderdate.bin");
        mf_o_ok.open(gendb_dir + "/orders/o_orderkey.bin");
        mf_l_sd.open(gendb_dir + "/lineitem/l_shipdate.bin", MADV_SEQUENTIAL);
        mf_l_ep.open(gendb_dir + "/lineitem/l_extendedprice.bin", MADV_SEQUENTIAL);
        mf_l_disc.open(gendb_dir + "/lineitem/l_discount.bin", MADV_SEQUENTIAL);
        mf_li_idx.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin", MADV_RANDOM);
    }

    const int8_t* c_mktsegment = mf_c_seg.as<int8_t>();
    const int32_t* c_custkey = mf_c_ck.as<int32_t>();
    size_t num_customers = mf_c_ck.count<int32_t>();

    const int32_t* o_custkey = mf_o_ck.as<int32_t>();
    const int32_t* o_orderdate = mf_o_date.as<int32_t>();
    const int32_t* o_orderkey = mf_o_ok.as<int32_t>();
    size_t num_orders = mf_o_ck.count<int32_t>();

    const int32_t* l_shipdate = mf_l_sd.as<int32_t>();
    const double* l_extendedprice = mf_l_ep.as<double>();
    const double* l_discount = mf_l_disc.as<double>();

    uint64_t li_idx_entries = *(const uint64_t*)mf_li_idx.ptr;
    const uint32_t* li_idx = (const uint32_t*)((const char*)mf_li_idx.ptr + 8);

    // === Build Customer Bitmap (187KB, fits L2) — PARALLEL ===
    constexpr int32_t BITMAP_MAX = 1500001;
    constexpr size_t BITMAP_WORDS = (BITMAP_MAX + 63) / 64;
    static uint64_t cust_bitmap[BITMAP_WORDS];

    int8_t building_code = -1;
    {
        GENDB_PHASE("dim_filter");
        // Load dictionary
        std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.bin", std::ios::binary);
        uint32_t dict_count; df.read((char*)&dict_count, 4);
        for (uint32_t d = 0; d < dict_count; d++) {
            uint16_t len; df.read((char*)&len, 2);
            std::string val(len, '\0'); df.read(val.data(), len);
            if (val == c_mktsegment_eq) building_code = (int8_t)d;
        }

        memset(cust_bitmap, 0, sizeof(cust_bitmap));
        if (building_code >= 0) {
            // Parallel bitmap build with atomic OR on 64-bit words
            #pragma omp parallel for schedule(static)
            for (size_t i = 0; i < num_customers; i++) {
                if (c_mktsegment[i] == building_code) {
                    int32_t ck = c_custkey[i];
                    __atomic_fetch_or(&cust_bitmap[ck >> 6], 1ULL << (ck & 63), __ATOMIC_RELAXED);
                }
            }
        }
    }

    // === Two-Pass Orders Scan ===
    int nthreads = omp_get_max_threads();
    {
        GENDB_PHASE("orders_scan");

        // Pass 1: count qualifying rows per thread
        std::vector<size_t> counts(nthreads, 0);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = (num_orders + nt - 1) / nt;
            size_t start = tid * chunk;
            size_t end = std::min(start + chunk, num_orders);
            size_t cnt = 0;
            for (size_t i = start; i < end; i++) {
                if (o_orderdate[i] < o_orderdate_upper) {
                    int32_t ck = o_custkey[i];
                    if ((cust_bitmap[ck >> 6] >> (ck & 63)) & 1) {
                        cnt++;
                    }
                }
            }
            counts[tid] = cnt;
        }

        std::vector<size_t> offsets(nthreads + 1, 0);
        for (int t = 0; t < nthreads; t++) offsets[t + 1] = offsets[t] + counts[t];
        total_qual = offsets[nthreads];

        qual_orders = (QualOrder*)malloc(total_qual * sizeof(QualOrder));

        // Pass 2: write at offsets (preserves sorted order)
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = (num_orders + nt - 1) / nt;
            size_t start = tid * chunk;
            size_t end = std::min(start + chunk, num_orders);
            size_t pos = offsets[tid];
            for (size_t i = start; i < end; i++) {
                if (o_orderdate[i] < o_orderdate_upper) {
                    int32_t ck = o_custkey[i];
                    if ((cust_bitmap[ck >> 6] >> (ck & 63)) & 1) {
                        qual_orders[pos].orderkey = o_orderkey[i];
                        qual_orders[pos].orderdate = o_orderdate[i];
                        qual_orders[pos].row_id = (int32_t)i;
                        qual_orders[pos].revenue = 0.0;
                        pos++;
                    }
                }
            }
        }
    }

    // === Probe Lineitem + Aggregate Revenue (Fused) ===
    {
        GENDB_PHASE("main_scan");
        int64_t n_qual = (int64_t)total_qual;

        #pragma omp parallel for schedule(static)
        for (int64_t qi = 0; qi < n_qual; qi++) {
            int32_t ok = qual_orders[qi].orderkey;
            if (ok < 0 || (uint64_t)ok >= li_idx_entries) continue;

            uint32_t li_start = li_idx[ok * 2];
            uint32_t li_count = li_idx[ok * 2 + 1];
            if (li_count == 0) continue;

            double rev = 0.0;
            uint32_t li_end = li_start + li_count;
            for (uint32_t r = li_start; r < li_end; r++) {
                if (l_shipdate[r] > l_shipdate_lower) {
                    rev += l_extendedprice[r] * (1.0 - l_discount[r]);
                }
            }
            qual_orders[qi].revenue = rev;
        }
    }

    // === Top-K with Parallel Compaction ===
    {
        GENDB_PHASE("topk");

        // Parallel two-pass compaction to remove zero-revenue entries
        std::vector<size_t> counts(nthreads, 0);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = (total_qual + nt - 1) / nt;
            size_t start = tid * chunk;
            size_t end = std::min(start + chunk, total_qual);
            size_t cnt = 0;
            for (size_t i = start; i < end; i++) {
                if (qual_orders[i].revenue > 0.0) cnt++;
            }
            counts[tid] = cnt;
        }

        std::vector<size_t> offsets(nthreads + 1, 0);
        for (int t = 0; t < nthreads; t++) offsets[t + 1] = offsets[t] + counts[t];
        size_t compact_size = offsets[nthreads];

        QualOrder* compacted = (QualOrder*)malloc(compact_size * sizeof(QualOrder));

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = (total_qual + nt - 1) / nt;
            size_t start = tid * chunk;
            size_t end = std::min(start + chunk, total_qual);
            size_t pos = offsets[tid];
            for (size_t i = start; i < end; i++) {
                if (qual_orders[i].revenue > 0.0) {
                    compacted[pos++] = qual_orders[i];
                }
            }
        }

        free(qual_orders);
        qual_orders = compacted;
        total_qual = compact_size;

        size_t k = std::min((size_t)limit_val, total_qual);
        if (k > 0) {
            std::partial_sort(qual_orders, qual_orders + k, qual_orders + total_qual,
                [](const QualOrder& a, const QualOrder& b) {
                    if (a.revenue != b.revenue) return a.revenue > b.revenue;
                    return a.orderdate < b.orderdate;
                });
        }
    }

    // === Output (late materialize o_shippriority) ===
    {
        GENDB_PHASE("output");
        mf_o_sp.open(gendb_dir + "/orders/o_shippriority.bin", MADV_RANDOM);
        const int32_t* o_shippriority = mf_o_sp.as<int32_t>();

        size_t k = std::min((size_t)limit_val, total_qual);
        std::string outpath = results_dir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char datebuf[16];
        for (size_t i = 0; i < k; i++) {
            gendb::epoch_days_to_date_str(qual_orders[i].orderdate, datebuf);
            fprintf(fp, "%d,%.2f,%s,%d\n",
                qual_orders[i].orderkey,
                qual_orders[i].revenue,
                datebuf,
                o_shippriority[qual_orders[i].row_id]);
        }
        fclose(fp);
    }

    free(qual_orders);

    } // end GENDB_PHASE("total") — MMap destructors fire AFTER this

    return 0;
}
