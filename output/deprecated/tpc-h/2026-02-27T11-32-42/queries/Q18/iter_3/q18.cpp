// Q18 iter_3 — Large Volume Customer Orders
// Key optimization over iter_2: NO 240MB dense array.
// Each thread handles an exclusive orderkey range of li2 (sorted by orderkey).
// Sequential group-by scan within range → only simple counter needed.
// No hash maps, no large allocations → tiny working set per thread.
//
// Algorithm:
//   1. Read max_okey from ord_maxkey.bin
//   2. NT threads, each handles orderkeys in [ok_lo, ok_hi)
//      - Binary search li2_okey for pos [pos_lo, pos_hi)
//      - Sequential group-by scan: accumulate sum_qty per orderkey
//      - If sum_qty > 300: add to thread-local qualifying vector
//   3. Merge qualifying vectors from all threads
//   4. Build small OrdIdxMap for qualifying orderkeys
//   5. Scan orders to get custkey, orderdate, totalprice for qualifying orderkeys
//   6. Sort by totalprice DESC, orderdate ASC; output top 100

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
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

// Compact hash map: int32_t key → int32_t value
struct OrdIdxMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys;
    vector<int32_t> idxs;

    void init(int n) {
        cap = 1;
        while (cap < n * 2) cap <<= 1;
        keys.assign(cap, EMPTY);
        idxs.assign(cap, -1);
    }
    void insert(int32_t k, int32_t idx) {
        int h = (int)((uint32_t)(k * 2654435761u)) & (cap - 1);
        while (keys[h] != EMPTY) h = (h + 1) & (cap - 1);
        keys[h] = k; idxs[h] = idx;
    }
    int32_t find(int32_t k) const {
        int h = (int)((uint32_t)(k * 2654435761u)) & (cap - 1);
        while (keys[h] != EMPTY && keys[h] != k) h = (h + 1) & (cap - 1);
        return keys[h] == k ? idxs[h] : -1;
    }
};

// Lower bound on sorted int32 array
static size_t lower_bound_i32(const int32_t* arr, size_t n, int32_t val) {
    size_t l = 0, r = n;
    while (l < r) {
        size_t m = (l + r) >> 1;
        if (arr[m] < val) l = m + 1; else r = m;
    }
    return l;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: q18 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();
    double output_ms = 0;

    {
        GENDB_PHASE("total");

        // ── Load columns ──────────────────────────────────────────────────────
        size_t N, No, tmp;
        int32_t* li2_okey = mmap_col<int32_t>(gendb_dir, "li2_orderkey.bin", N);
        int8_t*  li2_qty  = mmap_col<int8_t> (gendb_dir, "li2_qty.bin",      tmp);

        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",   tmp);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
        double*  ord_tp   = mmap_col<double> (gendb_dir, "ord_totalprice.bin",tmp);

        // Read max orderkey
        size_t mk_cnt;
        int64_t* maxok_ptr = mmap_col<int64_t>(gendb_dir, "ord_maxkey.bin", mk_cnt);
        int32_t max_okey = (int32_t)maxok_ptr[0];

        // ── Step 1: Parallel sequential group-by scan of li2 ─────────────────
        // Each thread owns orderkeys in [ok_lo, ok_hi), no overlaps → no sync needed.
        int NT = min((int)thread::hardware_concurrency(), 64);

        // Thread-local qualifying pairs: {orderkey, sum_qty}
        vector<vector<pair<int32_t,int32_t>>> local_qual(NT);

        {
            vector<thread> thr;
            for (int t = 0; t < NT; ++t) {
                thr.emplace_back([&, t]() {
                    int64_t ok_lo = (int64_t)t * (max_okey + 1) / NT;
                    int64_t ok_hi = (int64_t)(t + 1) * (max_okey + 1) / NT;

                    // Binary search for position range in li2_okey
                    size_t pos_lo = lower_bound_i32(li2_okey, N, (int32_t)ok_lo);
                    size_t pos_hi = lower_bound_i32(li2_okey + pos_lo, N - pos_lo, (int32_t)ok_hi) + pos_lo;

                    // Sequential group-by scan: trivial since sorted by orderkey
                    auto& qual = local_qual[t];
                    int32_t cur_okey = -1, cur_qty = 0;
                    for (size_t i = pos_lo; i < pos_hi; ++i) {
                        int32_t ok  = li2_okey[i];
                        int32_t qty = (int32_t)(uint8_t)li2_qty[i];
                        if (ok != cur_okey) {
                            if (cur_qty > 300) qual.push_back({cur_okey, cur_qty});
                            cur_okey = ok;
                            cur_qty  = 0;
                        }
                        cur_qty += qty;
                    }
                    // Flush last group
                    if (cur_qty > 300) qual.push_back({cur_okey, cur_qty});
                });
            }
            for (auto& th : thr) th.join();
        }

        // ── Step 2: Merge qualifying orderkeys from all threads ───────────────
        vector<pair<int32_t,int32_t>> qual_pairs; // {orderkey, sum_qty}
        qual_pairs.reserve(512);
        for (auto& lq : local_qual)
            for (auto& p : lq) qual_pairs.push_back(p);

        // ── Step 3: Build OrdIdxMap for qualifying orderkeys ──────────────────
        OrdIdxMap omap;
        omap.init((int)qual_pairs.size() + 16);
        for (int i = 0; i < (int)qual_pairs.size(); ++i)
            omap.insert(qual_pairs[i].first, i);

        // ── Step 4: Scan orders for qualifying orderkeys ───────────────────────
        struct OrderData { int32_t custkey, orderdate; double totalprice; };
        vector<OrderData> ord_data(qual_pairs.size());

        for (size_t i = 0; i < No; ++i) {
            int32_t idx = omap.find(ord_okey[i]);
            if (idx >= 0)
                ord_data[idx] = {ord_ckey[i], ord_odat[i], ord_tp[i]};
        }

        // ── Step 5: Build result rows ─────────────────────────────────────────
        struct Row {
            int32_t custkey, orderkey, orderdate;
            double  totalprice;
            int32_t sum_qty_val;
        };
        vector<Row> rows;
        rows.reserve(qual_pairs.size());
        for (int i = 0; i < (int)qual_pairs.size(); ++i) {
            rows.push_back({ord_data[i].custkey, qual_pairs[i].first, ord_data[i].orderdate,
                            ord_data[i].totalprice, qual_pairs[i].second});
        }

        // Sort: totalprice DESC, orderdate ASC
        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.totalprice != b.totalprice) return a.totalprice > b.totalprice;
            return a.orderdate < b.orderdate;
        });
        if ((int)rows.size() > 100) rows.resize(100);

        // ── Output ────────────────────────────────────────────────────────────
        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q18.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
            char datebuf[16];
            for (auto& row : rows) {
                gendb::epoch_days_to_date_str(row.orderdate, datebuf);
                fprintf(out, "Customer#%09d,%d,%d,%s,%.2f,%.2f\n",
                        row.custkey, row.custkey, row.orderkey, datebuf,
                        row.totalprice, (double)row.sum_qty_val);
            }
            fclose(out);
        }
    }
    return 0;
}
