// Q18 iter_1: OpenMP parallel qty accumulation (atomic) + parallel qualifying scan
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

    // Step 1: Parallel lineitem scan, atomic qty accumulation
    const int MAX_OKEY = 60000001;
    // Use uint32_t to avoid uint16_t atomic issues on some platforms
    uint32_t* qty_sum = (uint32_t*)calloc(MAX_OKEY, sizeof(uint32_t)); // 240MB virtual

    {
        MmapColumn<int32_t> orderkey(gd+"/lineitem/l_orderkey.bin");
        MmapColumn<uint8_t> quantity(gd+"/lineitem/l_quantity.bin");
        mmap_prefetch_all(orderkey, quantity);

        size_t n = orderkey.count;
        const int32_t* ok = orderkey.data;
        const uint8_t* qt = quantity.data;

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) {
            int32_t key = ok[i];
            if (key > 0 && key < MAX_OKEY) {
                __atomic_fetch_add(&qty_sum[key], (uint32_t)qt[i], __ATOMIC_RELAXED);
            }
        }
    }

    // Step 2: Parallel scan of qty_sum to find qualifying orderkeys
    // Split into per-thread qualifying lists, then merge
    int nth = omp_get_max_threads();
    std::vector<std::vector<int32_t>> tqualifying(nth);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local = tqualifying[tid];
        local.reserve(10);

        #pragma omp for schedule(static) nowait
        for (int32_t i = 1; i < MAX_OKEY; i++) {
            if (qty_sum[i] > 300) local.push_back(i);
        }
    }

    // Merge qualifying lists
    std::vector<int32_t> qualifying;
    qualifying.reserve(200);
    for (auto& v : tqualifying)
        qualifying.insert(qualifying.end(), v.begin(), v.end());
    std::sort(qualifying.begin(), qualifying.end());

    // Step 3: Scan orders, collect qualifying order records
    struct OrderRec {
        int32_t  orderkey;
        int32_t  custkey;
        uint16_t orderdate;
        int32_t  totalprice;
        uint32_t sum_qty;
    };
    std::vector<OrderRec> order_recs;
    order_recs.reserve(qualifying.size());

    {
        MmapColumn<int32_t>  okey(gd+"/orders/o_orderkey.bin");
        MmapColumn<int32_t>  ckey(gd+"/orders/o_custkey.bin");
        MmapColumn<uint16_t> odat(gd+"/orders/o_orderdate.bin");
        MmapColumn<int32_t>  otp (gd+"/orders/o_totalprice.bin");

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

    // Step 4: Load customer names
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
