// Q18 iter_4 — Large Volume Customer Orders
// Key optimizations over iter_3:
//   1. Position-based li2 partitioning (no binary search → no DRAM miss chains)
//   2. Parallel orders scan (64 threads) distributes 15K page faults
//   3. Two-phase li2: parallel group-by with boundary group merging
// Expected: ~6-10ms total

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
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

struct OrdIdxMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys;
    vector<int32_t> idxs;
    void init(int n) {
        cap = 1; while (cap < n * 2) cap <<= 1;
        keys.assign(cap, EMPTY); idxs.assign(cap, -1);
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

struct ChunkResult {
    int32_t head_okey, head_qty;
    int32_t tail_okey, tail_qty;
    vector<pair<int32_t,int32_t>> complete;
    bool head_only;
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr,"Usage: q18 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];
    gendb::init_date_tables();
    double output_ms = 0;
    {
        GENDB_PHASE("total");

        size_t N, No, tmp;
        int32_t* li2_okey = mmap_col<int32_t>(gendb_dir, "li2_orderkey.bin", N);
        int8_t*  li2_qty  = mmap_col<int8_t> (gendb_dir, "li2_qty.bin",      tmp);
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",   tmp);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
        double*  ord_tp   = mmap_col<double> (gendb_dir, "ord_totalprice.bin",tmp);

        int NT = min((int)thread::hardware_concurrency(), 64);

        // ── Phase 1: Parallel position-based group-by of li2 ─────────────────
        vector<ChunkResult> chunks(NT);
        {
            vector<thread> thr;
            for (int t = 0; t < NT; ++t) {
                size_t s = (size_t)t * N / NT;
                size_t e = (size_t)(t+1) * N / NT;
                thr.emplace_back([&,t,s,e](){
                    ChunkResult& cr = chunks[t];
                    cr.head_only = false;
                    if (s >= e) {
                        cr.head_okey = cr.tail_okey = -1;
                        cr.head_qty  = cr.tail_qty  = 0;
                        cr.head_only = true; return;
                    }
                    cr.head_okey = li2_okey[s];
                    cr.head_qty  = 0;
                    int32_t cur_okey = cr.head_okey, cur_qty = 0;
                    for (size_t i = s; i < e; ++i) {
                        int32_t ok  = li2_okey[i];
                        int32_t qty = (int32_t)(uint8_t)li2_qty[i];
                        if (ok != cur_okey) {
                            if (cur_okey == cr.head_okey)
                                cr.head_qty = cur_qty;
                            else if (cur_qty > 300)
                                cr.complete.push_back({cur_okey, cur_qty});
                            cur_okey = ok; cur_qty = 0;
                        }
                        cur_qty += qty;
                    }
                    cr.tail_okey = cur_okey;
                    cr.tail_qty  = cur_qty;
                    if (cr.head_okey == cr.tail_okey) {
                        cr.head_only = true;
                        cr.head_qty = 0;
                    }
                });
            }
            for (auto& th : thr) th.join();
        }

        // ── Phase 2: Merge boundary groups sequentially ───────────────────────
        vector<pair<int32_t,int32_t>> qual_pairs;
        qual_pairs.reserve(1024);
        for (int t = 0; t < NT; ++t)
            for (auto& p : chunks[t].complete) qual_pairs.push_back(p);

        int32_t bnd_okey = -1, bnd_qty = 0;
        for (int t = 0; t < NT; ++t) {
            ChunkResult& cr = chunks[t];
            if (cr.head_okey == -1) continue;
            if (cr.head_only) {
                if (cr.head_okey == bnd_okey) {
                    bnd_qty += cr.tail_qty;
                } else {
                    if (bnd_okey >= 0 && bnd_qty > 300) qual_pairs.push_back({bnd_okey, bnd_qty});
                    bnd_okey = cr.head_okey; bnd_qty = cr.tail_qty;
                }
            } else {
                if (cr.head_okey == bnd_okey) {
                    bnd_qty += cr.head_qty;
                    if (bnd_qty > 300) qual_pairs.push_back({bnd_okey, bnd_qty});
                } else {
                    if (bnd_okey >= 0 && bnd_qty > 300) qual_pairs.push_back({bnd_okey, bnd_qty});
                    if (cr.head_qty > 300) qual_pairs.push_back({cr.head_okey, cr.head_qty});
                }
                bnd_okey = cr.tail_okey; bnd_qty = cr.tail_qty;
            }
        }
        if (bnd_okey >= 0 && bnd_qty > 300) qual_pairs.push_back({bnd_okey, bnd_qty});

        // ── Phase 3: Build qualifying orderkey map ────────────────────────────
        OrdIdxMap omap;
        omap.init((int)qual_pairs.size() + 16);
        for (int i = 0; i < (int)qual_pairs.size(); ++i)
            omap.insert(qual_pairs[i].first, i);

        // ── Phase 4: Parallel orders scan ─────────────────────────────────────
        struct OrderData { int32_t custkey, orderdate; double totalprice; };
        vector<OrderData> ord_data(qual_pairs.size());
        {
            vector<thread> thr;
            size_t chunk_ord = (No + NT - 1) / NT;
            for (int t = 0; t < NT; ++t) {
                size_t s = t * chunk_ord;
                size_t e = min(s + chunk_ord, No);
                thr.emplace_back([&,s,e](){
                    for (size_t i = s; i < e; ++i) {
                        int32_t idx = omap.find(ord_okey[i]);
                        if (idx >= 0)
                            ord_data[idx] = {ord_ckey[i], ord_odat[i], ord_tp[i]};
                    }
                });
            }
            for (auto& th : thr) th.join();
        }

        // ── Phase 5: Build and sort result rows ───────────────────────────────
        struct Row { int32_t custkey, orderkey, orderdate; double totalprice; int32_t sum_qty_val; };
        vector<Row> rows;
        rows.reserve(qual_pairs.size());
        for (int i = 0; i < (int)qual_pairs.size(); ++i)
            rows.push_back({ord_data[i].custkey, qual_pairs[i].first, ord_data[i].orderdate,
                            ord_data[i].totalprice, qual_pairs[i].second});

        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
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
