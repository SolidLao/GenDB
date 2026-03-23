// Q3 iter_2: Full parallelism — parallel orders scan (atomic bitset), compact qualifying vector
// No large dense arrays (ord_date/ord_sp). Use 7.5MB bitset + compact QualInfo vector.
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
static const int      BASE_DATE = 8035;

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

    // Step 1: Build is_building bitset (1.5MB, fits in L1/L2 after warmup)
    uint8_t* is_building = (uint8_t*)calloc(MAX_CUST, 1);
    {
        MmapColumn<int32_t> ck(g+"/customer/c_custkey.bin");
        MmapColumn<uint8_t> ms(g+"/customer/c_mktseg.bin");
        mmap_prefetch_all(ck, ms);
        size_t n = ck.count;
        // Customer ids are 1..MAX_CUST, sequential in file → L1 friendly
        for (size_t i = 0; i < n; i++)
            if (ms.data[i] == 'B') is_building[ck.data[i]] = 1;
    }

    // Allocations: only the 7.5MB bitset (ord_ok_bits) + demand-paged rev[]
    // No large 120MB ord_date or 60MB ord_sp arrays!
    uint8_t* ord_ok_bits = (uint8_t*)calloc((MAX_OKEY+7)/8, 1); // 7.5MB — fits in L3
    int64_t* rev         = (int64_t*)calloc(MAX_OKEY, sizeof(int64_t)); // 480MB virtual, ~11MB physical

    // Step 2: PARALLEL orders scan — build ord_ok bitset + compact QualInfo vector
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
                    if (key > 0 && key < MAX_OKEY) {
                        // Atomic bit-set (lock-free LOCK OR BYTE on x86)
                        __atomic_fetch_or(&ord_ok_bits[key>>3],
                                          (uint8_t)(1u<<(key&7)), __ATOMIC_RELAXED);
                        local.push_back({key, OD[i], OSP[i]});
                    }
                }
            }
        }
    }
    free(is_building);

    // Merge thread-local qualifying lists
    std::vector<QualInfo> qualifying_orders;
    qualifying_orders.reserve(1500000);
    for (auto& v : tq)
        qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());

    // Step 3: Parallel lineitem scan from SDATE_CUT, atomic revenue accumulation
    {
        MmapColumn<uint16_t> sd(g+"/lineitem/l_shipdate.bin");
        MmapColumn<int32_t>  ok(g+"/lineitem/l_orderkey.bin");
        MmapColumn<int32_t>  ep(g+"/lineitem/l_extprice.bin");
        MmapColumn<uint8_t>  dc(g+"/lineitem/l_discount.bin");
        mmap_prefetch_all(sd, ok, ep, dc);

        size_t n = sd.count;
        const uint16_t* S = sd.data;
        const int32_t*  K = ok.data;
        const int32_t*  E = ep.data;
        const uint8_t*  D = dc.data;

        // Binary search for first row above SDATE_CUT
        size_t start = 0;
        { size_t lo=0, hi=n;
          while(lo<hi){size_t m=(lo+hi)>>1; if(S[m]<=SDATE_CUT)lo=m+1;else hi=m;}
          start=lo; }

        // Parallel scan: bitset check (7.5MB, L3-cached) + atomic add to rev[]
        #pragma omp parallel for schedule(static)
        for (size_t i = start; i < n; i++) {
            int32_t key = K[i];
            if (key > 0 && key < MAX_OKEY) {
                if ((ord_ok_bits[key>>3] >> (key&7)) & 1u) {
                    int64_t rv = (int64_t)E[i] * (100 - D[i]);
                    __atomic_fetch_add(&rev[key], rv, __ATOMIC_RELAXED);
                }
            }
        }
    }
    free(ord_ok_bits);

    // Step 4: Collect results from compact qualifying_orders vector
    std::vector<Result> results;
    results.reserve(120000); // ~114K orders have revenue > 0
    for (const auto& q : qualifying_orders) {
        int64_t r_val = rev[q.orderkey];
        if (r_val > 0) {
            results.push_back({q.orderkey, (double)r_val / 10000.0,
                               q.orderdate, q.shippriority});
        }
    }
    free(rev);

    // Sort by revenue DESC, orderdate ASC (need stable tie-breaking)
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
    return 0;
}
