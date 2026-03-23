// Q3: Shipping Priority — GenDB Generated Code (iter 2)
// Optimization: No per-thread hash tables. o_orderkey is PK so each qualifying order
// produces exactly one group. Compute revenue inline, feed directly into per-thread top-10 heap.
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "mmap_utils.h"
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
    int32_t shippriority;
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
        // Compare with heap top (min element)
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
    init_date_tables();

    // ==== Data Loading ====
    MmapColumn<uint8_t>  c_mktsegment;
    MmapColumn<int32_t>  o_orderkey_col, o_custkey_col, o_orderdate_col, o_shippriority_col;
    MmapColumn<int32_t>  l_shipdate_col;
    MmapColumn<double>   l_extendedprice_col, l_discount_col;
    MmapColumn<char>     li_raw;

    {
        GENDB_PHASE("data_loading");
        c_mktsegment.open(gdir + "/customer/c_mktsegment.bin");
        o_orderdate_col.open(gdir + "/orders/o_orderdate.bin");
        o_custkey_col.open(gdir + "/orders/o_custkey.bin");
        o_orderkey_col.open(gdir + "/orders/o_orderkey.bin");
        o_shippriority_col.open(gdir + "/orders/o_shippriority.bin");
        l_shipdate_col.open(gdir + "/lineitem/l_shipdate.bin");
        l_extendedprice_col.open(gdir + "/lineitem/l_extendedprice.bin");
        l_discount_col.open(gdir + "/lineitem/l_discount.bin");
        li_raw.open(gdir + "/indexes/lineitem_orderkey_index.bin");
        // Random access for lineitem columns (probed per-order via index)
        l_shipdate_col.advise_random();
        l_extendedprice_col.advise_random();
        l_discount_col.advise_random();
    }

    const int32_t* o_orderdate  = o_orderdate_col.data;
    const int32_t* o_custkey    = o_custkey_col.data;
    const int32_t* o_orderkey   = o_orderkey_col.data;
    const int32_t* o_shipprio   = o_shippriority_col.data;
    const int32_t* l_shipdate   = l_shipdate_col.data;
    const double*  l_extprice   = l_extendedprice_col.data;
    const double*  l_discount   = l_discount_col.data;

    // Lineitem orderkey index
    const uint32_t li_max = *(const uint32_t*)li_raw.data;
    const LiIdx* li_idx = (const LiIdx*)(li_raw.data + 4);

    size_t n_cust   = c_mktsegment.size();
    size_t n_orders = o_orderkey_col.size();

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

    // ==== Build Customer BUILDING Bitset ====
    // Custkeys are dense 1..1500000
    constexpr int32_t MAX_CUSTKEY = 1500000;
    constexpr size_t BSET_WORDS = (MAX_CUSTKEY + 64) / 64;
    std::vector<uint64_t> cust_bset(BSET_WORDS, 0);
    {
        GENDB_PHASE("dim_filter");
        const uint8_t* seg = c_mktsegment.data;
        for (size_t i = 0; i < n_cust; i++) {
            if (seg[i] == building_code) {
                // custkey = i + 1 (dense, sorted)
                int32_t ck = (int32_t)(i + 1);
                cust_bset[ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // ==== Parallel Scan Orders + Probe Lineitem (no hash tables) ====
    int nth = omp_get_max_threads();
    // Per-thread top-10 heaps — stack allocated inside parallel region
    std::vector<ResultRow> all_heaps(nth * TOPK);
    std::vector<int> heap_sizes(nth, 0);

    {
        GENDB_PHASE("main_scan");

        const uint64_t* bset = cust_bset.data();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            ResultRow my_heap[TOPK];
            int my_n = 0;

            #pragma omp for schedule(dynamic, 100000) nowait
            for (size_t i = 0; i < n_orders; i++) {
                // Filter: o_orderdate < 9204
                if (o_orderdate[i] >= DATE_CUT) continue;

                // Check customer bitset
                int32_t ck = o_custkey[i];
                if ((uint32_t)ck > (uint32_t)MAX_CUSTKEY) continue;
                if (!(bset[ck >> 6] & (1ULL << (ck & 63)))) continue;

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
                r.shippriority = o_shipprio[i];
                heap_insert(my_heap, my_n, r);
            }

            // Copy thread-local heap to shared storage
            for (int k = 0; k < my_n; k++)
                all_heaps[tid * TOPK + k] = my_heap[k];
            heap_sizes[tid] = my_n;
        }
    }

    // ==== Merge & Output ====
    {
        GENDB_PHASE("output");

        // Merge all thread heaps
        ResultRow final_heap[TOPK];
        int final_n = 0;
        for (int t = 0; t < nth; t++) {
            for (int k = 0; k < heap_sizes[t]; k++) {
                heap_insert(final_heap, final_n, all_heaps[t * TOPK + k]);
            }
        }

        // Sort final top-10: revenue DESC, orderdate ASC
        std::sort(final_heap, final_heap + final_n, [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });

        // Write CSV
        std::string outpath = rdir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { fprintf(stderr, "Cannot open %s\n", outpath.c_str()); return 1; }
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char db[16];
        for (int i = 0; i < final_n; i++) {
            epoch_days_to_date_str(final_heap[i].orderdate, db);
            fprintf(fp, "%d,%.4f,%s,%d\n",
                    final_heap[i].orderkey,
                    final_heap[i].revenue,
                    db,
                    final_heap[i].shippriority);
        }
        fclose(fp);
    }

    return 0;
}
