// Q3 iter_1 — Bitset-accelerated filter + partitioned thread-local hash maps.
// Each thread owns a hash partition so its local revenue map stays in L3 cache.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
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

// Compact open-addressing map: int32 key → double value
struct RevMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    int32_t* keys;
    double*  vals;
    RevMap() : cap(0), keys(nullptr), vals(nullptr) {}
    RevMap(int n) {
        cap = 1; while (cap < n*2) cap <<= 1;
        keys = new int32_t[cap];
        vals = new double[cap]();
        for (int i = 0; i < cap; ++i) keys[i] = EMPTY;
    }
    ~RevMap() { delete[] keys; delete[] vals; }
    void add(int32_t k, double v) {
        int h = ((unsigned)k * 2654435761u) & (cap-1);
        while (keys[h] != EMPTY && keys[h] != k) h = (h+1) & (cap-1);
        if (keys[h] == EMPTY) keys[h] = k;
        vals[h] += v;
    }
    // Iterate: call fn(key, value) for each non-empty slot
    template<typename F> void each(F fn) const {
        for (int i = 0; i < cap; ++i)
            if (keys[i] != EMPTY) fn(keys[i], vals[i]);
    }
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: q3 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        // Load customer
        size_t Nc, tmp;
        int32_t* cust_custkey = mmap_col<int32_t>(gendb_dir, "cust_custkey.bin",    Nc);
        uint8_t* cust_isbld   = mmap_col<uint8_t>(gendb_dir, "cust_isbuilding.bin", tmp);

        // Load orders
        size_t No;
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",    No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",     tmp);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin",   tmp);
        int32_t* ord_shpr = mmap_col<int32_t>(gendb_dir, "ord_shippriority.bin",tmp);

        // Load lineitem (sorted by shipdate)
        size_t Nl;
        int32_t* li_shd  = mmap_col<int32_t>(gendb_dir, "li_shipdate.bin",  Nl);
        int32_t* li_okey = mmap_col<int32_t>(gendb_dir, "li_orderkey.bin",  tmp);
        double*  li_ep   = mmap_col<double> (gendb_dir, "li_extprice.bin",  tmp);
        int8_t*  li_disc = mmap_col<int8_t> (gendb_dir, "li_discount.bin",  tmp);

        int32_t cut_date = gendb::date_str_to_epoch_days("1995-03-15");

        // ── Step 1: BUILDING customer bitset (tiny, fits in L1) ──────────────
        int32_t max_ck = 0;
        for (size_t i = 0; i < Nc; ++i) if (cust_custkey[i] > max_ck) max_ck = cust_custkey[i];
        vector<uint8_t> is_bld(max_ck + 1, 0);
        for (size_t i = 0; i < Nc; ++i)
            if (cust_isbld[i]) is_bld[cust_custkey[i]] = 1;

        // ── Step 2: Scan orders → qualifying bitset + compact order data ─────
        // Find max orderkey
        int64_t max_ok_64 = 0;
        {
            char path[512]; snprintf(path, sizeof(path), "%s/ord_maxkey.bin", gendb_dir);
            FILE* f = fopen(path, "rb");
            if (f) { fread(&max_ok_64, 8, 1, f); fclose(f); }
            else {
                for (size_t i = 0; i < No; ++i) if (ord_okey[i] > max_ok_64) max_ok_64 = ord_okey[i];
            }
        }
        int32_t max_ok = (int32_t)max_ok_64;

        // Build qualifying-orderkey bitset
        size_t bits_n = (max_ok / 64) + 2;
        vector<uint64_t> qual_bits(bits_n, 0);
        // Build compact order lookup: orderkey → (orderdate, shippriority)
        // Use a flat open-addressing map
        // Count qualifying orders first
        int n_qual = 0;
        for (size_t i = 0; i < No; ++i)
            if (ord_odat[i] < cut_date && ord_ckey[i] <= max_ck && is_bld[ord_ckey[i]])
                ++n_qual;

        // Build RevMap-like structure for order metadata
        struct OrderMeta { int32_t orderdate; int32_t shippriority; };
        int mcap = 1; while (mcap < n_qual*2) mcap <<= 1;
        vector<int32_t>    meta_keys(mcap, INT32_MIN);
        vector<OrderMeta>  meta_vals(mcap);

        for (size_t i = 0; i < No; ++i) {
            if (ord_odat[i] < cut_date && ord_ckey[i] <= max_ck && is_bld[ord_ckey[i]]) {
                int32_t ok = ord_okey[i];
                // Set bit in bitset
                qual_bits[ok >> 6] |= (1ULL << (ok & 63));
                // Insert into meta map
                int h = ((unsigned)ok * 2654435761u) & (mcap-1);
                while (meta_keys[h] != INT32_MIN && meta_keys[h] != ok) h = (h+1) & (mcap-1);
                meta_keys[h] = ok;
                meta_vals[h] = {ord_odat[i], ord_shpr[i]};
            }
        }

        // ── Step 3: Binary search for lineitem shipdate > cut_date ───────────
        size_t li_start;
        {
            size_t l=0, r=Nl;
            while (l<r) { size_t m=(l+r)/2; if (li_shd[m]<=cut_date) l=m+1; else r=m; }
            li_start = l;
        }
        size_t li_count = Nl - li_start;

        // ── Step 4: Parallel revenue aggregation with partitioned hash maps ──
        // Use NT partitions. Each thread t handles orderkeys where ok % NT == t.
        // This keeps each thread's local map small (fits in L3).
        int NT = min((int)thread::hardware_concurrency(), 32);
        int map_cap_per_thread = 1;
        while (map_cap_per_thread < (n_qual / NT + 16) * 2) map_cap_per_thread <<= 1;

        vector<RevMap> rev_maps(NT, RevMap(0));
        for (int t = 0; t < NT; ++t) new (&rev_maps[t]) RevMap(n_qual / NT + 16);

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            threads.emplace_back([&, t]() {
                auto& rev = rev_maps[t];
                for (size_t i = li_start; i < Nl; ++i) {
                    int32_t ok = li_okey[i];
                    // Partition filter: this thread handles ok % NT == t
                    if (((unsigned)ok % (unsigned)NT) != (unsigned)t) continue;
                    // Bitset check
                    if (!(qual_bits[ok >> 6] & (1ULL << (ok & 63)))) continue;
                    double contrib = li_ep[i] * (1.0 - li_disc[i] * 0.01);
                    rev.add(ok, contrib);
                }
            });
        }
        for (auto& th : threads) th.join();

        // ── Step 5: Collect results, look up metadata, sort top 10 ───────────
        struct Row {
            int32_t orderkey;
            double  revenue;
            int32_t orderdate;
            int32_t shippriority;
        };
        vector<Row> rows;
        rows.reserve(n_qual);
        for (int t = 0; t < NT; ++t) {
            rev_maps[t].each([&](int32_t ok, double rev) {
                // Look up metadata
                int h = ((unsigned)ok * 2654435761u) & (mcap-1);
                while (meta_keys[h] != INT32_MIN && meta_keys[h] != ok) h = (h+1) & (mcap-1);
                if (meta_keys[h] == ok)
                    rows.push_back({ok, rev, meta_vals[h].orderdate, meta_vals[h].shippriority});
            });
        }

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
