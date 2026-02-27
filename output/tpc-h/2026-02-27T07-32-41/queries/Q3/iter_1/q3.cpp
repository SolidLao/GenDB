// Q3 iter_1: OpenMP parallel with dense arrays + atomic revenue accumulation
// Dense ord_ok (60MB bitset->7.5MB), dense ord_date/ord_sp, int64 atomic rev[]
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

// 1995-03-15: epoch=9204, stored=9204-8035=1169
static const uint16_t ODATE_CUT = 1169;
static const uint16_t SDATE_CUT = 1169;
static const int BASE_DATE = 8035;

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

    // Step 1: Load customer, build bitset of BUILDING custkeys
    const int MAX_CUST = 1500001;
    uint8_t* is_building = (uint8_t*)calloc(MAX_CUST, 1);
    {
        MmapColumn<int32_t> ck(g+"/customer/c_custkey.bin");
        MmapColumn<uint8_t> ms(g+"/customer/c_mktseg.bin");
        size_t n = ck.count;
        for (size_t i = 0; i < n; i++)
            if (ms.data[i] == 'B') is_building[ck.data[i]] = 1;
    }

    // Step 2: Scan orders, build dense arrays + qualifying_keys list
    // Use dense arrays for O(1) lookup without hash probe
    const int MAX_OKEY = 60000001;

    // ord_ok: 7.5MB bitset (fits entirely in L3 cache!)
    uint8_t*  ord_ok   = (uint8_t*)calloc((MAX_OKEY+7)/8, 1);  // 7.5MB bitset
    uint16_t* ord_date = (uint16_t*)calloc(MAX_OKEY, sizeof(uint16_t)); // 120MB
    uint8_t*  ord_sp   = (uint8_t*)calloc(MAX_OKEY, 1);                 // 60MB
    // Revenue accumulator: int64_t, demand-paged (only qualifying entries touched)
    int64_t*  rev      = (int64_t*)calloc(MAX_OKEY, sizeof(int64_t));   // 480MB virtual, ~11MB physical

    std::vector<int32_t> qualifying_keys;
    qualifying_keys.reserve(1500000);

    {
        MmapColumn<int32_t>  okey(g+"/orders/o_orderkey.bin");
        MmapColumn<int32_t>  ckey(g+"/orders/o_custkey.bin");
        MmapColumn<uint16_t> odat(g+"/orders/o_orderdate.bin");
        MmapColumn<uint8_t>  osp (g+"/orders/o_shippriority.bin");
        size_t n = okey.count;
        // Sequential scan to build bitset (avoid atomic bit-set race conditions)
        for (size_t i = 0; i < n; i++) {
            int32_t ck = ckey.data[i];
            if (odat.data[i] < ODATE_CUT && ck < MAX_CUST && is_building[ck]) {
                int32_t key = okey.data[i];
                if (key > 0 && key < MAX_OKEY) {
                    // Set bit in bitset
                    ord_ok[key >> 3] |= (uint8_t)(1u << (key & 7));
                    ord_date[key] = odat.data[i];
                    ord_sp[key]   = osp.data[i];
                    qualifying_keys.push_back(key);
                }
            }
        }
    }
    free(is_building);

    // Step 3: Parallel lineitem scan from SDATE_CUT, accumulate revenue
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

        // Binary search for start position
        size_t start = 0;
        { size_t lo=0,hi=n;
          while(lo<hi){size_t m=(lo+hi)>>1; if(S[m]<=SDATE_CUT)lo=m+1;else hi=m;}
          start=lo; }

        // Parallel scan: atomic add to rev[]
        #pragma omp parallel for schedule(static)
        for (size_t i = start; i < n; i++) {
            int32_t key = K[i];
            if (key > 0 && key < MAX_OKEY) {
                // Check bitset (7.5MB, fits in L3)
                if ((ord_ok[key >> 3] >> (key & 7)) & 1u) {
                    int64_t rv = (int64_t)E[i] * (100 - D[i]);
                    __atomic_fetch_add(&rev[key], rv, __ATOMIC_RELAXED);
                }
            }
        }
    }

    // Step 4: Collect results from qualifying_keys list
    std::vector<Result> results;
    results.reserve(qualifying_keys.size());
    for (int32_t key : qualifying_keys) {
        if (rev[key] > 0) {
            results.push_back({key, (double)rev[key] / 10000.0, ord_date[key], ord_sp[key]});
        }
    }

    free(ord_ok);
    free(ord_date);
    free(ord_sp);
    free(rev);

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
    return 0;
}
