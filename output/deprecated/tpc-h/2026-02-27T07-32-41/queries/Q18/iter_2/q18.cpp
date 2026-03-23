// Q18 iter_2: uint16_t qty_sum (30MB physical → fits in L3!) with parallel atomic adds
// Parallel qty_sum scan + parallel orders scan
// Usage: ./q18 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"
using namespace gendb;

static const int32_t BASE_DATE = 8035;
struct Name26 { char s[26]; };

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");
    gendb::init_date_tables();

    std::string gd = argv[1];
    std::string rd = argv[2];

    const int MAX_OKEY = 60000001;

    // Step 1: Parallel lineitem scan, atomic uint16_t qty accumulation
    // uint16_t: 15M active entries × 2B = 30MB physical → FITS IN L3 (44MB per socket)!
    // This is the key difference from iter_1 which used uint32_t (60MB physical, didn't fit)
    uint16_t* qty_sum = (uint16_t*)calloc(MAX_OKEY, sizeof(uint16_t)); // 120MB virtual, 30MB physical

    {
        MmapColumn<int32_t> orderkey(gd+"/lineitem/l_orderkey.bin");
        MmapColumn<uint8_t> quantity(gd+"/lineitem/l_quantity.bin");
        mmap_prefetch_all(orderkey, quantity);

        size_t n = orderkey.count;
        const int32_t* ok = orderkey.data;
        const uint8_t* qt = quantity.data;

        // Parallel atomic uint16_t adds: LOCK ADD WORD PTR on x86 (lock-free)
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) {
            int32_t key = ok[i];
            if (key > 0 && key < MAX_OKEY) {
                __atomic_fetch_add(&qty_sum[key], (uint16_t)qt[i], __ATOMIC_RELAXED);
            }
        }
    }

    // Step 2: Parallel scan of qty_sum to find qualifying orderkeys (sum > 300)
    // Sequential scan of 120MB (uint16_t × 60M) — parallel across threads
    int nth = omp_get_max_threads();
    std::vector<std::vector<int32_t>> tq(nth);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local = tq[tid];
        local.reserve(10);

        #pragma omp for schedule(static) nowait
        for (int32_t i = 1; i < MAX_OKEY; i++) {
            if (qty_sum[i] > 300) local.push_back(i);
        }
    }

    // Merge qualifying lists
    std::vector<int32_t> qualifying;
    qualifying.reserve(200);
    for (auto& v : tq)
        qualifying.insert(qualifying.end(), v.begin(), v.end());
    std::sort(qualifying.begin(), qualifying.end()); // small sort

    // Build a dense bitset of qualifying orderkeys for fast order-scan lookup
    // qualifying is only ~100-200 entries, use binary search in orders scan
    // OR: build a small hash set. Use bsearch or linear scan (tiny set).

    // Step 3: Scan orders, collect qualifying order records
    struct OrderRec {
        int32_t  orderkey;
        int32_t  custkey;
        uint16_t orderdate;
        int32_t  totalprice;
        uint16_t sum_qty;
    };
    std::vector<OrderRec> order_recs;
    order_recs.reserve(qualifying.size() + 10);

    {
        MmapColumn<int32_t>  okey(gd+"/orders/o_orderkey.bin");
        MmapColumn<int32_t>  ckey(gd+"/orders/o_custkey.bin");
        MmapColumn<uint16_t> odat(gd+"/orders/o_orderdate.bin");
        MmapColumn<int32_t>  otp (gd+"/orders/o_totalprice.bin");
        mmap_prefetch_all(okey, ckey, odat, otp);

        size_t n = okey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t ok = okey.data[i];
            if (ok > 0 && ok < MAX_OKEY && qty_sum[ok] > 300) {
                order_recs.push_back({ok, ckey.data[i], odat.data[i],
                                      otp.data[i], qty_sum[ok]});
            }
        }
    }
    free(qty_sum);

    // Step 4: Load customer names (dense indexed by custkey-1)
    MmapColumn<Name26> cname(gd+"/customer/c_name.bin");

    // Step 5: Sort by totalprice DESC, orderdate ASC, limit 100
    std::sort(order_recs.begin(), order_recs.end(), [](const OrderRec& a, const OrderRec& b){
        if (a.totalprice != b.totalprice) return a.totalprice > b.totalprice;
        return a.orderdate < b.orderdate;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((rd+"/Q18.csv").c_str(), "w");
        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
        int limit = std::min((int)order_recs.size(), 100);
        char datebuf[16];
        for (int i = 0; i < limit; i++) {
            auto& rec = order_recs[i];
            int32_t ck = rec.custkey;
            const char* name = (ck >= 1 && ck-1 < (int32_t)cname.count)
                               ? cname.data[ck-1].s : "";
            gendb::epoch_days_to_date_str(BASE_DATE + rec.orderdate, datebuf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    name, ck, rec.orderkey, datebuf,
                    rec.totalprice / 100.0,
                    (double)rec.sum_qty);
        }
        fclose(f);
    }
    return 0;
}
