// Q9 iter_4: Use secondary lineitem index (sorted by orderkey) for cache-friendly order_year lookup
// Key insight: lineitem_by_okey has consecutive rows with same orderkey → order_year lookups
// are near-sequential (same/adjacent cache lines), converting L3 misses to L3 hits!
// Also: ps_map lookups are partially reused since same order's rows share suppkey.
// Usage: ./q9 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <string>
#include <omp.h>
#include "mmap_utils.h"
#include "hash_utils.h"
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

    // Step 1: Build is_green bitset (2MB → fits in L2)
    const int MAX_PART = 2100000;
    uint8_t* is_green = (uint8_t*)calloc(MAX_PART, 1);
    {
        MmapColumn<int32_t> pk(gd+"/part/p_partkey.bin");
        MmapColumn<uint8_t> hg(gd+"/part/p_has_green.bin");
        size_t n = pk.count;
        for (size_t i = 0; i < n; i++)
            if (hg.data[i]) is_green[pk.data[i]] = 1;
    }

    // Step 2: Load partsupp → ps_map (~4MB → fits in L3)
    CompactHashMapPair<int32_t> ps_map(500000);
    {
        MmapColumn<int32_t> pk(gd+"/partsupp/ps_partkey.bin");
        MmapColumn<int32_t> sk(gd+"/partsupp/ps_suppkey.bin");
        MmapColumn<int32_t> sc(gd+"/partsupp/ps_supplycost.bin");
        size_t n = pk.count;
        for (size_t i = 0; i < n; i++) {
            int32_t p = pk.data[i];
            if (p < MAX_PART && is_green[p])
                ps_map.insert({p, sk.data[i]}, sc.data[i]);
        }
    }

    // Step 3: Build supp_nat dense array (110KB → fits in L1)
    const int MAX_SUPP = 110000;
    uint8_t* supp_nat = new uint8_t[MAX_SUPP]();
    {
        MmapColumn<int32_t> sk(gd+"/supplier/s_suppkey.bin");
        MmapColumn<uint8_t> nk(gd+"/supplier/s_nationkey.bin");
        size_t n = sk.count;
        for (size_t i = 0; i < n; i++)
            supp_nat[sk.data[i]] = nk.data[i];
    }

    // Step 4: Load nation names
    char nation_names[25][26] = {};
    {
        MmapColumn<uint8_t> nk(gd+"/nation/n_nationkey.bin");
        MmapColumn<Name26>  nn(gd+"/nation/n_name.bin");
        size_t n = nk.count;
        for (size_t i = 0; i < n; i++) {
            int k = nk.data[i];
            if (k < 25) strncpy(nation_names[k], nn.data[i].s, 25);
        }
    }

    // Step 5: Build order_year dense array (60MB)
    // With sorted-by-okey lineitem, consecutive rows with same orderkey reuse the same
    // order_year cache line → dramatically fewer cache misses than shipdate-sorted lineitem!
    const int MAX_OKEY = 60000001;
    uint8_t* order_year = (uint8_t*)calloc(MAX_OKEY, 1);
    {
        MmapColumn<int32_t>  okey(gd+"/orders/o_orderkey.bin");
        MmapColumn<uint16_t> odat(gd+"/orders/o_orderdate.bin");
        size_t n = okey.count;
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) {
            int32_t ok = okey.data[i];
            if (ok < MAX_OKEY) {
                int32_t epoch = BASE_DATE + (int32_t)odat.data[i];
                order_year[ok] = (uint8_t)(gendb::extract_year(epoch) - 1992);
            }
        }
    }

    // Step 6: Parallel lineitem scan using SECONDARY INDEX (sorted by orderkey)
    // Advantages:
    // 1. order_year[orderkey] access: consecutive rows with same okey → SEQUENTIAL cache access!
    //    Hardware prefetcher can predict next order_year entry (only advances by ~4 cache positions per 4 rows)
    // 2. is_green[partkey]: still random, but fits in L3 (2MB)
    // 3. ps_map: random, fits in L3 (4MB)
    // Total L3 working set: order_year(60MB) + is_green(2MB) + ps_map(4MB) = 66MB
    // Note: order_year doesn't fit in L3 (60MB > 44MB per socket), BUT with sequential
    // orderkey access, hardware prefetcher loads it ahead of time → latency hidden!
    int nth = omp_get_max_threads();
    const int PROFIT_STRIDE = 256;
    std::vector<double> tprofit((size_t)nth * PROFIT_STRIDE, 0.0);

    {
        MmapColumn<int32_t> ok(gd+"/lineitem_by_okey/l_orderkey.bin");
        MmapColumn<int32_t> pk(gd+"/lineitem_by_okey/l_partkey.bin");
        MmapColumn<int32_t> sk(gd+"/lineitem_by_okey/l_suppkey.bin");
        MmapColumn<uint8_t> qt(gd+"/lineitem_by_okey/l_quantity.bin");
        MmapColumn<int32_t> ep(gd+"/lineitem_by_okey/l_extprice.bin");
        MmapColumn<uint8_t> dc(gd+"/lineitem_by_okey/l_discount.bin");
        mmap_prefetch_all(ok, pk, sk, qt, ep, dc);

        size_t n = ok.count;
        const int32_t* K  = ok.data;
        const int32_t* PK = pk.data;
        const int32_t* SK = sk.data;
        const uint8_t* QT = qt.data;
        const int32_t* EP = ep.data;
        const uint8_t* DC = dc.data;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_profit = &tprofit[(size_t)tid * PROFIT_STRIDE];

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; i++) {
                int32_t p = PK[i];
                if (p >= MAX_PART || !is_green[p]) continue;

                int32_t s = SK[i];
                int32_t* sc = ps_map.find({p, s});
                if (!sc) continue;

                int32_t o = K[i];
                if (o >= MAX_OKEY) continue;

                uint8_t nat = supp_nat[s];
                // order_year lookup: with okey-sorted lineitem, consecutive rows with same okey
                // access the SAME byte in order_year → hardware prefetcher keeps it warm!
                uint8_t yr  = order_year[o];
                if (nat >= 25 || yr >= 10) continue;

                double amount = ((double)EP[i] * (100 - DC[i]) - (double)(*sc) * QT[i] * 100.0) / 10000.0;
                my_profit[nat * 10 + yr] += amount;
            }
        }
    }

    free(is_green);
    delete[] supp_nat;
    free(order_year);

    // Reduce thread-local profit arrays
    double profit[25][10] = {};
    for (int t = 0; t < nth; t++) {
        double* tp = &tprofit[(size_t)t * PROFIT_STRIDE];
        for (int n = 0; n < 25; n++)
            for (int y = 0; y < 10; y++)
                profit[n][y] += tp[n*10+y];
    }

    // Collect non-zero results
    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    for (int n = 0; n < 25; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < 10; y++) {
            if (profit[n][y] != 0.0) {
                Row rw;
                strncpy(rw.nation, nation_names[n], 25); rw.nation[25]='\0';
                rw.year = 1992 + y;
                rw.sum_profit = profit[n][y];
                rows.push_back(rw);
            }
        }
    }

    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
        int c = strcmp(a.nation, b.nation);
        if (c != 0) return c < 0;
        return a.year > b.year;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((rd+"/Q9.csv").c_str(), "w");
        fprintf(f, "nation,o_year,sum_profit\n");
        for (auto& rw : rows)
            fprintf(f, "%s,%d,%.4f\n", rw.nation, rw.year, rw.sum_profit);
        fclose(f);
    }
    return 0;
}
