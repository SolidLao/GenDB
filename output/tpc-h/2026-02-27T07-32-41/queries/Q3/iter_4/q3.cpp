// Q3 iter_4: Use secondary lineitem index (sorted by orderkey) for revenue accumulation
// Key insight: qualifying orders sorted by okey → SEQUENTIAL scan of secondary index!
// No random scatter, each order's rows are adjacent in secondary index.
// Usage: ./q3 <gendb_dir> <results_dir>

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

static const uint16_t ODATE_CUT = 1169;  // o_orderdate < 1995-03-15
static const uint16_t SDATE_CUT = 1169;  // l_shipdate > 1995-03-15
static const int32_t  BASE_DATE = 8035;

struct QualInfo {
    int32_t  orderkey;
    uint16_t orderdate;
    uint8_t  shippriority;
};

struct Result {
    int32_t  orderkey;
    double   revenue;
    uint16_t orderdate;
    int32_t  shippriority;
};

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");
    gendb::init_date_tables();

    std::string g = argv[1];
    std::string r = argv[2];

    const int MAX_CUST = 1500001;
    const int MAX_OKEY = 60000001;

    // Step 1: Build is_building bitset (1.5MB → fits in L2)
    uint8_t* is_building = (uint8_t*)calloc(MAX_CUST, 1);
    {
        MmapColumn<int32_t> ck(g+"/customer/c_custkey.bin");
        MmapColumn<uint8_t> ms(g+"/customer/c_mktseg.bin");
        mmap_prefetch_all(ck, ms);
        size_t n = ck.count;
        for (size_t i = 0; i < n; i++)
            if (ms.data[i] == 'B') is_building[ck.data[i]] = 1;
    }

    // Step 2: PARALLEL orders scan — build compact QualInfo vector (sorted by orderkey)
    int nth = omp_get_max_threads();
    std::vector<std::vector<QualInfo>> tq(nth);
    for (int t = 0; t < nth; t++) tq[t].reserve(30000);

    {
        MmapColumn<int32_t>  okey(g+"/orders/o_orderkey.bin");
        MmapColumn<int32_t>  ckey(g+"/orders/o_custkey.bin");
        MmapColumn<uint16_t> odat(g+"/orders/o_orderdate.bin");
        MmapColumn<uint8_t>  osp (g+"/orders/o_shippriority.bin");
        mmap_prefetch_all(okey, ckey, odat, osp);

        size_t n = okey.count;
        const int32_t*  OK  = okey.data;
        const int32_t*  CK  = ckey.data;
        const uint16_t* OD  = odat.data;
        const uint8_t*  OSP = osp.data;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = tq[tid];

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; i++) {
                int32_t ck = CK[i];
                if (OD[i] < ODATE_CUT && ck < MAX_CUST && is_building[ck]) {
                    int32_t key = OK[i];
                    if (key > 0 && key < MAX_OKEY)
                        local.push_back({key, OD[i], OSP[i]});
                }
            }
        }
    }
    free(is_building);

    // Merge thread-local qualifying lists
    std::vector<QualInfo> qualifying_orders;
    qualifying_orders.reserve(1600000);
    for (auto& v : tq)
        qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());

    // CRITICAL: Sort qualifying orders by orderkey
    // This makes the secondary index access SEQUENTIAL!
    std::sort(qualifying_orders.begin(), qualifying_orders.end(),
              [](const QualInfo& a, const QualInfo& b){ return a.orderkey < b.orderkey; });

    size_t nq = qualifying_orders.size();

    // Step 3: Use secondary index (lineitem sorted by orderkey) for revenue accumulation
    // With qualifying orders sorted by okey, we scan the secondary index nearly sequentially.
    // For each qualifying order, directly access its rows using okey_offsets.
    // Filter by l_shipdate > SDATE_CUT.
    {
        MmapColumn<uint32_t> okey_offsets(g+"/lineitem_by_okey/okey_offsets.bin");
        MmapColumn<int32_t>  l_ep(g+"/lineitem_by_okey/l_extprice.bin");
        MmapColumn<uint8_t>  l_dc(g+"/lineitem_by_okey/l_discount.bin");
        MmapColumn<uint16_t> l_sd(g+"/lineitem_by_okey/l_shipdate.bin");
        mmap_prefetch_all(l_ep, l_dc, l_sd);

        const uint32_t* OFF = okey_offsets.data;
        const int32_t*  EP  = l_ep.data;
        const uint8_t*  DC  = l_dc.data;
        const uint16_t* SD  = l_sd.data;
        size_t n_li = l_ep.count;

        // Revenue accumulation — parallel over qualifying orders
        // Each qualifying order has ~4 rows; access is sequential within each order's rows.
        // Since qualifying orders are sorted by okey and rows are sorted by okey,
        // consecutive qualifying orders access consecutive parts of the secondary index.
        // → Excellent cache utilization!
        std::vector<int64_t> rev(nq, 0LL);

        #pragma omp parallel for schedule(dynamic, 4096)
        for (size_t qi = 0; qi < nq; qi++) {
            int32_t k = qualifying_orders[qi].orderkey;
            size_t row_start = (k >= MAX_OKEY) ? n_li : (size_t)OFF[k];
            size_t row_end   = (k+1 >= MAX_OKEY) ? n_li : (size_t)OFF[k+1];
            if (row_end > n_li) row_end = n_li;

            int64_t rv = 0;
            for (size_t i = row_start; i < row_end; i++) {
                if (SD[i] > SDATE_CUT)
                    rv += (int64_t)EP[i] * (100 - DC[i]);
            }
            rev[qi] = rv;
        }

        // Collect results
        std::vector<Result> results;
        results.reserve(qualifying_orders.size());
        for (size_t i = 0; i < nq; i++) {
            if (rev[i] > 0) {
                results.push_back({qualifying_orders[i].orderkey,
                                   (double)rev[i] / 10000.0,
                                   qualifying_orders[i].orderdate,
                                   qualifying_orders[i].shippriority});
            }
        }

        // Sort by revenue DESC, orderdate ASC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b){
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });

        {
            GENDB_PHASE("output");
            FILE* f = fopen((r+"/Q3.csv").c_str(), "w");
            fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            int limit = std::min((int)results.size(), 10);
            char datebuf[16];
            for (int i = 0; i < limit; i++) {
                auto& res = results[i];
                gendb::epoch_days_to_date_str(BASE_DATE + res.orderdate, datebuf);
                fprintf(f, "%d,%.4f,%s,%d\n",
                        res.orderkey, res.revenue, datebuf, res.shippriority);
            }
            fclose(f);
        }
    }
    return 0;
}
