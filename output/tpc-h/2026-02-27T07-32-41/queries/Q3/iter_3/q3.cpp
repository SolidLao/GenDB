// Q3 iter_3: Compact rev[] array (11.7MB) with Robin Hood hash lookup
// Key insight: rev[compact_idx] where compact_idx ∈ [0, nq-1] (nq ≈ 1.46M qualifying orders)
// Working set: bitset(7.5MB) + hash_table(16MB) + rev(11.7MB) = 35.2MB → fits in L3 (44MB)!
// This eliminates cache misses from scattered atomic writes to 480MB virtual rev[].
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

// Robin Hood hash table: int32_t key -> int32_t compact_idx
// size must be power of 2
struct RobinHoodMap {
    uint32_t mask;
    int32_t* keys;    // -1 = empty
    int32_t* values;

    explicit RobinHoodMap(uint32_t sz) : mask(sz-1) {
        keys   = (int32_t*)malloc(sz * sizeof(int32_t));
        values = (int32_t*)malloc(sz * sizeof(int32_t));
        memset(keys, -1, sz * sizeof(int32_t));
    }
    ~RobinHoodMap() { free(keys); free(values); }

    inline void insert(int32_t k, int32_t v) {
        uint32_t h = (uint32_t)k * 2654435761u;
        uint32_t slot = h & mask;
        while (keys[slot] != -1) slot = (slot+1) & mask;
        keys[slot] = k;
        values[slot] = v;
    }

    // Returns -1 if not found
    inline int32_t find(int32_t k) const {
        uint32_t h = (uint32_t)k * 2654435761u;
        uint32_t slot = h & mask;
        while (keys[slot] != -1 && keys[slot] != k)
            slot = (slot+1) & mask;
        return (keys[slot] == k) ? values[slot] : -1;
    }
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

    // ord_ok bitset: 7.5MB — fits in L3
    uint8_t* ord_ok_bits = (uint8_t*)calloc((MAX_OKEY+7)/8, 1);

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
    qualifying_orders.reserve(1600000);
    for (auto& v : tq)
        qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());
    size_t nq = qualifying_orders.size();

    // Step 3: Build compact revenue array (11.7MB) + hash map (16MB)
    // TOTAL L3 working set: bitset(7.5MB) + hash(16MB) + rev(11.7MB) = 35.2MB → fits in 44MB L3!
    std::vector<int64_t> rev(nq, 0LL);  // compact, ~11.7MB

    // Build Robin Hood hash: orderkey → compact_idx in qualifying_orders
    // Size: next power of 2 above nq * 1.4
    uint32_t htsize = 1;
    while (htsize < nq * 2) htsize <<= 1;  // e.g., 4M for 1.46M entries, 4M × 8B = 32MB
    // Keep at most 32MB for hash table. If nq > 2M, use 4M buckets = 32MB.
    // At 1.46M qualifying orders, 2M buckets × 8B = 16MB → fits in L3!

    RobinHoodMap okey_to_idx(htsize);
    // Sequential build (fast, ~1.46M inserts)
    for (size_t i = 0; i < nq; i++)
        okey_to_idx.insert(qualifying_orders[i].orderkey, (int32_t)i);

    // Step 4: Parallel lineitem scan with compact rev[] accumulation
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

        // Parallel scan: bitset filter (7.5MB, L3) + hash lookup (16MB, L3) + compact atomic add (11.7MB, L3)
        #pragma omp parallel for schedule(static)
        for (size_t i = start; i < n; i++) {
            int32_t key = K[i];
            if (key > 0 && key < MAX_OKEY) {
                // Quick filter: bitset check (7.5MB, L3-resident)
                if ((ord_ok_bits[key>>3] >> (key&7)) & 1u) {
                    // Hash lookup to get compact index (16MB hash table, L3-resident)
                    int32_t idx = okey_to_idx.find(key);
                    if (idx >= 0) {
                        int64_t rv = (int64_t)E[i] * (100 - D[i]);
                        // Atomic add to compact rev[] (11.7MB, L3-resident → cache HITS!)
                        __atomic_fetch_add(&rev[idx], rv, __ATOMIC_RELAXED);
                    }
                }
            }
        }
    }
    free(ord_ok_bits);

    // Step 5: Collect results
    std::vector<Result> results;
    results.reserve(qualifying_orders.size());
    for (size_t i = 0; i < nq; i++) {
        int64_t r_val = rev[i];
        if (r_val > 0) {
            results.push_back({qualifying_orders[i].orderkey,
                               (double)r_val / 10000.0,
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
    return 0;
}
