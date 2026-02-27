// Q18 iter_2 — Large Volume Customer Orders
// Key optimization over iter_1: parallel group-by scan of li2 (sorted by orderkey).
//   Divide orderkey space into NT ranges; each thread binary-searches for its
//   range boundaries in li2 and does a sequential scan → no hash maps needed.
//   The sum_qty values per thread are written to exclusive sections of a dense
//   int32_t array (no false sharing). After parallel phase, single scan finds
//   qualifying orderkeys (sum_qty > 300), then join with orders + customer.

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

// Simple hash map: int32_t key → int (index into arrays)
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

        // ── Step 1: Parallel group-by scan of li2 ─────────────────────────────
        // Dense sum_qty array indexed by orderkey.
        // Each thread handles an exclusive range of orderkey values → no false sharing.
        vector<int32_t> sum_qty(max_okey + 1, 0);

        int NT = min((int)thread::hardware_concurrency(), 64);
        {
            vector<thread> thr;
            for (int t = 0; t < NT; ++t) {
                thr.emplace_back([&, t]() {
                    // This thread owns orderkeys in [ok_lo, ok_hi)
                    int64_t ok_lo = (int64_t)t * (max_okey + 1) / NT;
                    int64_t ok_hi = (int64_t)(t + 1) * (max_okey + 1) / NT;

                    // Binary search li2_okey for first position with okey >= ok_lo
                    size_t pos_lo, pos_hi;
                    {
                        size_t l = 0, r = N;
                        while (l < r) {
                            size_t m = (l + r) / 2;
                            if (li2_okey[m] < (int32_t)ok_lo) l = m + 1; else r = m;
                        }
                        pos_lo = l;
                    }
                    {
                        size_t l = pos_lo, r = N;
                        while (l < r) {
                            size_t m = (l + r) / 2;
                            if (li2_okey[m] < (int32_t)ok_hi) l = m + 1; else r = m;
                        }
                        pos_hi = l;
                    }

                    // Sequential scan: group by orderkey, accumulate into sum_qty
                    // (This thread is the sole writer for orderkeys in [ok_lo, ok_hi))
                    for (size_t i = pos_lo; i < pos_hi; ++i) {
                        sum_qty[li2_okey[i]] += (int32_t)(uint8_t)li2_qty[i];
                    }
                });
            }
            for (auto& th : thr) th.join();
        }

        // ── Step 2: Collect qualifying orderkeys (sum_qty > 300) ─────────────
        vector<int32_t> qual_okeys;
        qual_okeys.reserve(256);
        for (int32_t ok = 1; ok <= max_okey; ++ok)
            if (sum_qty[ok] > 300) qual_okeys.push_back(ok);

        // ── Step 3: Build OrdIdxMap for qualifying orderkeys ──────────────────
        OrdIdxMap omap;
        omap.init((int)qual_okeys.size() + 16);
        // Temporarily map okey → index in qual_okeys
        for (int i = 0; i < (int)qual_okeys.size(); ++i)
            omap.insert(qual_okeys[i], i);

        // ── Step 4: Scan orders for qualifying orderkeys ───────────────────────
        struct OrderData { int32_t custkey, orderdate; double totalprice; };
        vector<OrderData> ord_data(qual_okeys.size());

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
        rows.reserve(qual_okeys.size());
        for (int i = 0; i < (int)qual_okeys.size(); ++i) {
            int32_t ok = qual_okeys[i];
            rows.push_back({ord_data[i].custkey, ok, ord_data[i].orderdate,
                            ord_data[i].totalprice, sum_qty[ok]});
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
