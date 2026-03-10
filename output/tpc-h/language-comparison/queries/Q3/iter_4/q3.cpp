// Q3: Shipping Priority — GenDB Generated Code (iter 4)
// Optimizations vs iter 2:
// 1. No separate dim_filter phase — inline customer check via customer_custkey_lookup + c_mktsegment
// 2. Late materialization of o_shippriority (only for final 10 rows)
// 3. No madvise calls on lineitem columns (allow hardware prefetch)
// 4. init_date_tables() deferred to output phase
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

static constexpr int32_t DATE_CUT = 9204; // 1995-03-15
static constexpr int TOPK = 10;

struct LiIdx { uint32_t start, count; };

struct ResultRow {
    double revenue;
    int32_t orderkey;
    int32_t orderdate;
    int32_t order_row_idx; // late materialize: look up o_shippriority only for final 10
};

// Simple mmap helper — NO madvise calls per plan
struct RawMmap {
    const char* ptr;
    size_t len;
    RawMmap() : ptr(nullptr), len(0) {}
    void open(const char* path) {
        int fd = ::open(path, O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path); exit(1); }
        struct stat st;
        fstat(fd, &st);
        len = st.st_size;
        void* p = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (p == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path); exit(1); }
        ptr = (const char*)p;
    }
    ~RawMmap() { if (ptr && len) munmap((void*)ptr, len); }
};

// Min-heap: worst result on top (lowest revenue, or highest date on tie)
static inline bool heap_cmp(const ResultRow& a, const ResultRow& b) {
    if (a.revenue != b.revenue) return a.revenue > b.revenue;
    return a.orderdate < b.orderdate;
}

static inline void heap_insert(ResultRow* heap, int& n, const ResultRow& r) {
    if (n < TOPK) {
        heap[n++] = r;
        if (n == TOPK) std::make_heap(heap, heap + TOPK, heap_cmp);
    } else {
        if (r.revenue > heap[0].revenue ||
            (r.revenue == heap[0].revenue && r.orderdate < heap[0].orderdate)) {
            std::pop_heap(heap, heap + TOPK, heap_cmp);
            heap[TOPK - 1] = r;
            std::push_heap(heap, heap + TOPK, heap_cmp);
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gdir(argv[1]);
    std::string rdir(argv[2]);

    GENDB_PHASE("total");

    // ==== Load BUILDING dict code ====
    uint8_t building_code = 255;
    {
        FILE* df = fopen((gdir + "/customer/c_mktsegment_dict.bin").c_str(), "r");
        if (!df) { fprintf(stderr, "Cannot open dict\n"); return 1; }
        char line[256];
        while (fgets(line, sizeof(line), df)) {
            int code; char name[64];
            if (sscanf(line, "%d|%63s", &code, name) == 2 && strcmp(name, "BUILDING") == 0) {
                building_code = (uint8_t)code; break;
            }
        }
        fclose(df);
    }

    // ==== Data Loading — all mmap, no madvise ====
    RawMmap c_mktsegment_mm, cust_lookup_mm;
    RawMmap o_orderdate_mm, o_custkey_mm, o_orderkey_mm, o_shipprio_mm;
    RawMmap l_shipdate_mm, l_extprice_mm, l_discount_mm;
    RawMmap li_idx_mm;

    {
        GENDB_PHASE("data_loading");
        c_mktsegment_mm.open((gdir + "/customer/c_mktsegment.bin").c_str());
        cust_lookup_mm.open((gdir + "/indexes/customer_custkey_lookup.bin").c_str());
        o_orderdate_mm.open((gdir + "/orders/o_orderdate.bin").c_str());
        o_custkey_mm.open((gdir + "/orders/o_custkey.bin").c_str());
        o_orderkey_mm.open((gdir + "/orders/o_orderkey.bin").c_str());
        o_shipprio_mm.open((gdir + "/orders/o_shippriority.bin").c_str());
        li_idx_mm.open((gdir + "/indexes/lineitem_orderkey_index.bin").c_str());
        l_shipdate_mm.open((gdir + "/lineitem/l_shipdate.bin").c_str());
        l_extprice_mm.open((gdir + "/lineitem/l_extendedprice.bin").c_str());
        l_discount_mm.open((gdir + "/lineitem/l_discount.bin").c_str());
    }

    // Setup pointers
    const uint8_t* c_mktsegment = (const uint8_t*)c_mktsegment_mm.ptr;
    const size_t n_orders = o_orderdate_mm.len / sizeof(int32_t);

    const int32_t* o_orderdate = (const int32_t*)o_orderdate_mm.ptr;
    const int32_t* o_custkey   = (const int32_t*)o_custkey_mm.ptr;
    const int32_t* o_orderkey  = (const int32_t*)o_orderkey_mm.ptr;
    const int32_t* o_shipprio  = (const int32_t*)o_shipprio_mm.ptr;

    // Customer custkey lookup: [uint32_t max_custkey][int32_t lookup[max_custkey+1]]
    uint32_t cust_max = *(const uint32_t*)cust_lookup_mm.ptr;
    const int32_t* cust_lookup = (const int32_t*)(cust_lookup_mm.ptr + 4);

    // Lineitem orderkey index
    uint32_t li_max = *(const uint32_t*)li_idx_mm.ptr;
    const LiIdx* li_idx = (const LiIdx*)(li_idx_mm.ptr + 4);

    const int32_t* l_shipdate  = (const int32_t*)l_shipdate_mm.ptr;
    const double*  l_extprice  = (const double*)l_extprice_mm.ptr;
    const double*  l_discount  = (const double*)l_discount_mm.ptr;

    // ==== Parallel Scan Orders + Inline Customer Check + Probe Lineitem ====
    int nth = omp_get_max_threads();
    ResultRow* all_heaps = new ResultRow[nth * TOPK];
    int* heap_sizes = new int[nth]();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            ResultRow my_heap[TOPK];
            int my_n = 0;

            #pragma omp for schedule(dynamic, 100000) nowait
            for (size_t i = 0; i < n_orders; i++) {
                // Filter: o_orderdate < 9204
                if (o_orderdate[i] >= DATE_CUT) continue;

                // Inline customer check via lookup index (no bitset phase)
                int32_t ck = o_custkey[i];
                if ((uint32_t)ck > cust_max) continue;
                int32_t cust_row = cust_lookup[ck];
                if (cust_row < 0) continue;
                if (c_mktsegment[cust_row] != building_code) continue;

                // Probe lineitem index
                int32_t ok = o_orderkey[i];
                if ((uint32_t)ok > li_max) continue;
                const LiIdx& le = li_idx[ok];
                if (le.count == 0) continue;

                // Sum revenue for qualifying lineitem rows (l_shipdate > 9204)
                double rev = 0.0;
                uint32_t js = le.start;
                uint32_t je = js + le.count;
                for (uint32_t j = js; j < je; j++) {
                    if (l_shipdate[j] > DATE_CUT) {
                        rev += l_extprice[j] * (1.0 - l_discount[j]);
                    }
                }
                if (rev <= 0.0) continue;

                ResultRow r;
                r.revenue = rev;
                r.orderkey = ok;
                r.orderdate = o_orderdate[i];
                r.order_row_idx = (int32_t)i;
                heap_insert(my_heap, my_n, r);
            }

            // Copy to shared storage
            for (int k = 0; k < my_n; k++)
                all_heaps[tid * TOPK + k] = my_heap[k];
            heap_sizes[tid] = my_n;
        }
    }

    // ==== Merge & Output ====
    {
        GENDB_PHASE("output");

        init_date_tables(); // deferred

        ResultRow final_heap[TOPK];
        int final_n = 0;
        for (int t = 0; t < nth; t++) {
            for (int k = 0; k < heap_sizes[t]; k++)
                heap_insert(final_heap, final_n, all_heaps[t * TOPK + k]);
        }

        std::sort(final_heap, final_heap + final_n, [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });

        std::string outpath = rdir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { fprintf(stderr, "Cannot open %s\n", outpath.c_str()); return 1; }
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char db[16];
        for (int i = 0; i < final_n; i++) {
            epoch_days_to_date_str(final_heap[i].orderdate, db);
            int32_t shipprio = o_shipprio[final_heap[i].order_row_idx];
            fprintf(fp, "%d,%.4f,%s,%d\n",
                    final_heap[i].orderkey,
                    final_heap[i].revenue,
                    db,
                    shipprio);
        }
        fclose(fp);
    }

    delete[] all_heaps;
    delete[] heap_sizes;

    return 0;
}
