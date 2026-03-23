// Q3 — Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey
//   AND l_orderkey=o_orderkey
//   AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate LIMIT 10

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

// Fast open-addressing hash map: int32 key → int32/struct value
struct OrderInfo {
    int32_t orderdate;
    int32_t shippriority;
};

// Hash map: int32 key → OrderInfo
// Use open addressing with linear probing
struct OrderMap {
    static constexpr int EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys;
    vector<OrderInfo> vals;

    OrderMap(int n) {
        cap = 1;
        while (cap < n*2) cap <<= 1;
        keys.assign(cap, EMPTY);
        vals.resize(cap);
    }
    void insert(int32_t k, OrderInfo v) {
        int h = (int)((unsigned)k * 2654435761u) & (cap-1);
        while (keys[h] != EMPTY) { h = (h+1)&(cap-1); }
        keys[h] = k; vals[h] = v;
    }
    int find(int32_t k) const {
        int h = (int)((unsigned)k * 2654435761u) & (cap-1);
        while (keys[h] != EMPTY && keys[h] != k) h = (h+1)&(cap-1);
        return keys[h]==k ? h : -1;
    }
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr,"Usage: q3 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        // Load customer columns
        size_t Nc, Nck;
        int32_t* cust_custkey = mmap_col<int32_t>(gendb_dir, "cust_custkey.bin",    Nc);
        uint8_t* cust_isbld   = mmap_col<uint8_t>(gendb_dir, "cust_isbuilding.bin", Nck);

        // Load orders columns
        size_t No;
        size_t Nok, Ncc, Nod, Ntp, Nsp;
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",    No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",     Nok);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin",   Ncc);
        int32_t* ord_shpr = mmap_col<int32_t>(gendb_dir, "ord_shippriority.bin",Nod);

        // Load lineitem columns
        size_t Nl;
        size_t Nls, Nle, Nld, Nlok;
        int32_t* li_shd  = mmap_col<int32_t>(gendb_dir, "li_shipdate.bin",  Nl);
        int32_t* li_okey = mmap_col<int32_t>(gendb_dir, "li_orderkey.bin",  Nle);
        double*  li_ep   = mmap_col<double> (gendb_dir, "li_extprice.bin",  Nld);
        int8_t*  li_disc = mmap_col<int8_t> (gendb_dir, "li_discount.bin",  Nlok);

        int32_t cut_date = gendb::date_str_to_epoch_days("1995-03-15");

        // Step 1: Build set of BUILDING customer custkeys
        // cust_custkey is dense 1..1500000, use bitset
        int32_t max_custkey = 0;
        for (size_t i = 0; i < Nc; ++i)
            if (cust_custkey[i] > max_custkey) max_custkey = cust_custkey[i];
        vector<uint8_t> is_building(max_custkey+1, 0);
        for (size_t i = 0; i < Nc; ++i)
            if (cust_isbld[i]) is_building[cust_custkey[i]] = 1;

        // Step 2: Filter orders: orderdate < cut_date AND custkey in building set
        // Build hash map: orderkey -> (orderdate, shippriority)
        // First count qualifying orders
        int qual_orders = 0;
        for (size_t i = 0; i < No; ++i)
            if (ord_odat[i] < cut_date && (int)ord_ckey[i] <= max_custkey && is_building[ord_ckey[i]])
                ++qual_orders;

        OrderMap omap(qual_orders + 16);
        for (size_t i = 0; i < No; ++i) {
            if (ord_odat[i] < cut_date && (int)ord_ckey[i] <= max_custkey && is_building[ord_ckey[i]]) {
                omap.insert(ord_okey[i], {ord_odat[i], ord_shpr[i]});
            }
        }

        // Step 3: Scan lineitem (sorted by shipdate) — find range where shipdate > cut_date
        size_t li_start;
        {
            size_t l=0, r=Nl;
            while (l<r) { size_t m=(l+r)/2; if (li_shd[m]<=cut_date) l=m+1; else r=m; }
            li_start = l;
        }

        // Aggregate revenue per orderkey
        // Use thread-local maps then merge
        int NT = min((int)thread::hardware_concurrency(), 32);
        vector<unordered_map<int32_t,double>> local_rev(NT);
        size_t chunk = (Nl - li_start + NT - 1) / NT;

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = li_start + t * chunk;
            size_t end   = min(start + chunk, Nl);
            threads.emplace_back([&, t, start, end]() {
                auto& rev = local_rev[t];
                rev.reserve(qual_orders / NT + 1000);
                for (size_t i = start; i < end; ++i) {
                    int32_t ok = li_okey[i];
                    int slot = omap.find(ok);
                    if (slot >= 0) {
                        double contrib = li_ep[i] * (1.0 - li_disc[i] * 0.01);
                        rev[ok] += contrib;
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge
        unordered_map<int32_t,double> rev;
        rev.reserve(qual_orders * 2);
        for (auto& lm : local_rev)
            for (auto& [k,v] : lm)
                rev[k] += v;

        // Build result: for each orderkey in rev, get orderdate+shippriority
        struct Row {
            int32_t orderkey;
            double  revenue;
            int32_t orderdate;
            int32_t shippriority;
        };
        vector<Row> rows;
        rows.reserve(rev.size());
        for (auto& [ok, r] : rev) {
            int slot = omap.find(ok);
            if (slot >= 0)
                rows.push_back({ok, r, omap.vals[slot].orderdate, omap.vals[slot].shippriority});
        }

        // Sort: revenue DESC, orderdate ASC
        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });
        if ((int)rows.size() > 10) rows.resize(10);

        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q3.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char datebuf[16];
            for (auto& row : rows) {
                gendb::epoch_days_to_date_str(row.orderdate, datebuf);
                fprintf(out, "%d,%.4f,%s,%d\n",
                        row.orderkey, row.revenue, datebuf, row.shippriority);
            }
            fclose(out);
        }
    }
    return 0;
}
