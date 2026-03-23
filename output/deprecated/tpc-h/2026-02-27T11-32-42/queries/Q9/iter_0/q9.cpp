// Q9 — Product Type Profit Measure
// SELECT nation, o_year, SUM(amount) AS sum_profit
// FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
//              l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
//       FROM part, supplier, lineitem, partsupp, orders, nation
//       WHERE s_suppkey=l_suppkey AND ps_suppkey=l_suppkey AND ps_partkey=l_partkey
//         AND p_partkey=l_partkey AND o_orderkey=l_orderkey AND s_nationkey=n_nationkey
//         AND p_name LIKE '%green%') profit
// GROUP BY nation, o_year ORDER BY nation, o_year DESC

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"
#include "date_utils.h"

using namespace std;

template<typename T>
static T* mmap_col(const char* dir, const char* name, size_t& count) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return (T*)p;
}

// Open-addressing hash map: int64 key (packed partkey+suppkey) → double supplycost
struct PSMap {
    static constexpr int64_t EMPTY = INT64_MIN;
    int cap;
    vector<int64_t> keys;
    vector<double> vals;

    PSMap() : cap(0) {}
    PSMap(int n) {
        cap = 1;
        while (cap < n*2) cap <<= 1;
        keys.assign(cap, EMPTY);
        vals.resize(cap, 0.0);
    }
    void insert(int32_t pk, int32_t sk, double v) {
        int64_t k = ((int64_t)pk << 20) | (int64_t)sk;
        int h = (int)(((uint64_t)k * 6364136223846793005ULL) >> 32) & (cap-1);
        while (keys[h] != EMPTY) h = (h+1)&(cap-1);
        keys[h] = k; vals[h] = v;
    }
    bool get(int32_t pk, int32_t sk, double& v) const {
        int64_t k = ((int64_t)pk << 20) | (int64_t)sk;
        int h = (int)(((uint64_t)k * 6364136223846793005ULL) >> 32) & (cap-1);
        while (keys[h] != EMPTY) {
            if (keys[h] == k) { v = vals[h]; return true; }
            h = (h+1)&(cap-1);
        }
        return false;
    }
};

// Hash map: int32 orderkey → int16 year
struct OrderYearMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys;
    vector<int16_t> vals;

    OrderYearMap(int n) {
        cap = 1;
        while (cap < n*2) cap <<= 1;
        keys.assign(cap, EMPTY);
        vals.resize(cap, 0);
    }
    void insert(int32_t k, int16_t v) {
        int h = (int)((unsigned)k * 2654435761u) & (cap-1);
        while (keys[h] != EMPTY) h = (h+1)&(cap-1);
        keys[h] = k; vals[h] = v;
    }
    bool get(int32_t k, int16_t& v) const {
        int h = (int)((unsigned)k * 2654435761u) & (cap-1);
        while (keys[h] != EMPTY) {
            if (keys[h] == k) { v = vals[h]; return true; }
            h = (h+1)&(cap-1);
        }
        return false;
    }
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr,"Usage: q9 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        // Load all columns
        size_t Nl, Nps, No;
        size_t tmp;
        // lineitem
        int32_t* li_okey  = mmap_col<int32_t>(gendb_dir, "li_orderkey.bin", Nl);
        int32_t* li_pkey  = mmap_col<int32_t>(gendb_dir, "li_partkey.bin",  tmp);
        int32_t* li_skey  = mmap_col<int32_t>(gendb_dir, "li_suppkey.bin",  tmp);
        double*  li_ep    = mmap_col<double> (gendb_dir, "li_extprice.bin", tmp);
        int8_t*  li_disc  = mmap_col<int8_t> (gendb_dir, "li_discount.bin", tmp);
        int8_t*  li_qty   = mmap_col<int8_t> (gendb_dir, "li_qty.bin",      tmp);
        // partsupp
        int32_t* ps_pk    = mmap_col<int32_t>(gendb_dir, "ps_partkey.bin",    Nps);
        int32_t* ps_sk    = mmap_col<int32_t>(gendb_dir, "ps_suppkey.bin",    tmp);
        double*  ps_cost  = mmap_col<double> (gendb_dir, "ps_supplycost.bin", tmp);
        // orders
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
        // part green filter
        size_t   pg_sz;
        uint8_t* part_green_arr = mmap_col<uint8_t>(gendb_dir, "part_green_arr.bin", pg_sz);
        // supplier nationkey
        size_t   sn_sz;
        int8_t*  sup_nat_arr = mmap_col<int8_t>(gendb_dir, "sup_nat_arr.bin", sn_sz);
        // nation names: 25 rows × 26 chars
        size_t Nnat;
        char*  nat_names = (char*)mmap_col<char>(gendb_dir, "nat_names.bin", Nnat);

        // Build orders year map
        OrderYearMap omap((int)No);
        for (size_t i = 0; i < No; ++i) {
            int32_t yr = gendb::extract_year(ord_odat[i]);
            omap.insert(ord_okey[i], (int16_t)yr);
        }

        // Build partsupp map: (partkey, suppkey) → supplycost
        PSMap psmap((int)Nps);
        for (size_t i = 0; i < Nps; ++i)
            psmap.insert(ps_pk[i], ps_sk[i], ps_cost[i]);

        // Aggregate: (nation_idx, year) → sum_profit
        // nation_idx 0..24, year range ~1992-1998 (7 values) → 25*10=250 slots
        // Use (nation*20 + (year-1990)) as index
        const int NYEARS = 20; // 1990-2009
        const int NNATIONS = 25;
        // Thread-local aggregation
        int NT = min((int)thread::hardware_concurrency(), 32);
        vector<vector<double>> local_profit(NT, vector<double>(NNATIONS * NYEARS, 0.0));
        size_t chunk = (Nl + NT - 1) / NT;

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk;
            size_t end   = min(start + chunk, Nl);
            threads.emplace_back([&, t, start, end]() {
                auto& prof = local_profit[t];
                for (size_t i = start; i < end; ++i) {
                    int32_t pk = li_pkey[i];
                    if ((size_t)pk >= pg_sz || !part_green_arr[pk]) continue;
                    int32_t sk = li_skey[i];
                    double sc;
                    if (!psmap.get(pk, sk, sc)) continue;
                    int32_t ok = li_okey[i];
                    int16_t yr;
                    if (!omap.get(ok, yr)) continue;
                    if ((size_t)sk >= sn_sz) continue;
                    int8_t nat = sup_nat_arr[sk];
                    if (nat < 0 || nat >= NNATIONS) continue;
                    int yidx = yr - 1990;
                    if (yidx < 0 || yidx >= NYEARS) continue;
                    double ep = li_ep[i];
                    double d  = li_disc[i] * 0.01;
                    double q  = (double)li_qty[i];
                    double amount = ep * (1.0 - d) - sc * q;
                    prof[nat * NYEARS + yidx] += amount;
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge
        vector<double> total_profit(NNATIONS * NYEARS, 0.0);
        for (auto& lp : local_profit)
            for (int i = 0; i < NNATIONS * NYEARS; ++i)
                total_profit[i] += lp[i];

        // Build result rows (nation, year, sum_profit) for non-zero entries
        struct Row { const char* nation; int year; double profit; };
        vector<Row> rows;
        for (int n = 0; n < NNATIONS; ++n) {
            for (int y = 0; y < NYEARS; ++y) {
                double p = total_profit[n * NYEARS + y];
                if (p != 0.0)
                    rows.push_back({nat_names + n*26, 1990+y, p});
            }
        }
        // Sort: nation ASC, year DESC
        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
            int c = strcmp(a.nation, b.nation);
            if (c != 0) return c < 0;
            return a.year > b.year;
        });

        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q9.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "nation,o_year,sum_profit\n");
            for (auto& row : rows)
                fprintf(out, "%s,%d,%.4f\n", row.nation, row.year, row.profit);
            fclose(out);
        }
    }
    return 0;
}
