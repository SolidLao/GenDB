#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <string>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

static constexpr int32_t EMPTY_KEY = INT32_MIN;

static inline uint32_t hash32(int32_t key) {
    return (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32);
}

// ============================================================
// LocalMap: per-thread aggregation hash map for Phase 1a
// 2M slots x 8 bytes = 16MB each
// ============================================================
struct LocalMap {
    static constexpr uint32_t CAP  = 1u << 19; // 524288 (512K) -- ~234K unique keys/thread → load 0.45
    static constexpr uint32_t MASK = CAP - 1;
    int32_t keys[CAP];
    double  vals[CAP]; // double sum of l_quantity per Column Reference spec

    void init() {
        std::fill(keys, keys + CAP, EMPTY_KEY); // correct sentinel init (memset(0x80) gives 0x80808080 ≠ INT32_MIN)
        memset(vals, 0, sizeof(vals));
    }

    inline void insert(int32_t key, double qty) {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        keys[s]  = key;
        vals[s] += qty;
    }
};

// ============================================================
// MergeMap: per-partition merge hash map for Phase 1b
// 1M slots x 8 bytes = 8MB each (fits in L3 per thread)
// Holds at most ~15M/64 = 234K unique keys -> load factor ~0.22
// ============================================================
struct MergeMap {
    static constexpr uint32_t CAP  = 1u << 19; // 524288 (512K) -- 15M/64=234K per partition → load 0.45
    static constexpr uint32_t MASK = CAP - 1;
    int32_t keys[CAP];
    double  vals[CAP]; // double sum of l_quantity per Column Reference spec

    void init() {
        std::fill(keys, keys + CAP, EMPTY_KEY); // fix: 0x80808080 ≠ INT32_MIN
        memset(vals, 0, sizeof(vals));
    }

    inline void insert(int32_t key, double qty) {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        keys[s]  = key;
        vals[s] += qty;
    }
};

// ============================================================
// QualSet: small flat hash set for qualifying orderkeys (~100 max)
// ============================================================
struct QualSet {
    static constexpr uint32_t CAP  = 4096;
    static constexpr uint32_t MASK = CAP - 1;
    int32_t keys[CAP];

    void init() { std::fill(keys, keys + CAP, EMPTY_KEY); }

    void insert(int32_t key) {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        keys[s] = key;
    }

    bool contains(int32_t key) const {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        return keys[s] == key;
    }
};

// ============================================================
// Index entry types
// ============================================================
struct UHEntry { int32_t key; uint32_t row_idx; };              // unique hash
struct MHEntry { int32_t key; uint32_t offset; uint32_t count; }; // multi-value hash

// ============================================================
// SumQtyMap: tiny hash map for ~57 qualifying orderkey → sum_qty
// 4096 slots × 12B = 48KB, fits in L1 cache
// ============================================================
struct SumQtyMap {
    static constexpr uint32_t CAP  = 4096;
    static constexpr uint32_t MASK = CAP - 1;
    int32_t keys[CAP];
    double  vals[CAP];
    void init() {
        std::fill(keys, keys + CAP, EMPTY_KEY);
        memset(vals, 0, sizeof(vals));
    }
    void set_key(int32_t key) {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        keys[s] = key;
    }
    inline void add(int32_t key, double qty) {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        vals[s] += qty;
    }
    double get(int32_t key) const {
        uint32_t s = hash32(key) & MASK;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
        return (keys[s] == key) ? vals[s] : 0.0;
    }
};

// ============================================================
// mmap helper -- returns raw pointer and sets out_size
// ============================================================
static const void* safe_mmap(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    posix_fadvise(fd, 0, (off_t)out_size, POSIX_FADV_SEQUENTIAL);
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0); // no MAP_POPULATE: avoid blocking HDD reads
    close(fd);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    return p;
}

// ============================================================
// Result row
// ============================================================
struct ResultRow {
    std::string c_name;
    int32_t c_custkey, o_orderkey, o_orderdate;
    double  o_totalprice, sum_qty;
};

void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ================================================================
    // Phase 1: Subquery -- full lineitem scan + parallel hash aggregation
    //   GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300
    //
    // Strategy:
    //   Phase 1a: 64 threads each scan ~937K rows into their own 16MB LocalMap
    //   Phase 1b: 64 partition threads, pid handles keys where (hash32(key)&63)==pid
    //             Each scans all n_threads LocalMaps and merges into a tiny 8MB MergeMap
    //             No cross-thread writes, no locks, no contention
    // ================================================================
    QualSet qual_set;
    qual_set.init();

    {
        GENDB_PHASE("subquery_lineitem_scan");

        size_t li_ok_sz, li_qty_sz;
        const int32_t* l_orderkey = (const int32_t*)safe_mmap(gendb_dir + "/lineitem/l_orderkey.bin", li_ok_sz);
        const double*  l_quantity  = (const double*)safe_mmap(gendb_dir + "/lineitem/l_quantity.bin",  li_qty_sz);
        int64_t n_li = (int64_t)(li_ok_sz / sizeof(int32_t));

        int n_threads = omp_get_max_threads();
        static constexpr int N_PARTS = 64; // must be power-of-2

        LocalMap** lmaps = new LocalMap*[n_threads];

        // Phase 1a: parallel init + scan -- each thread owns its LocalMap exclusively
        #pragma omp parallel num_threads(n_threads)
        {
            int tid   = omp_get_thread_num();
            int nthr  = omp_get_num_threads();

            // Heap-allocate and init this thread's map
            lmaps[tid] = (LocalMap*)aligned_alloc(64, sizeof(LocalMap));
            lmaps[tid]->init();

            int64_t chunk = (n_li + nthr - 1) / nthr;
            int64_t start = (int64_t)tid * chunk;
            int64_t end   = std::min(start + chunk, n_li);

            LocalMap* lm = lmaps[tid];
            for (int64_t j = start; j < end; j++) {
                lm->insert(l_orderkey[j], l_quantity[j]);
            }
        }

        // Phase 1b: partitioned merge -- N_PARTS threads, no sharing
        std::vector<std::vector<int32_t>> part_quals(N_PARTS);

        #pragma omp parallel for schedule(static, 1) num_threads(N_PARTS)
        for (int pid = 0; pid < N_PARTS; pid++) {
            MergeMap* mm = (MergeMap*)aligned_alloc(64, sizeof(MergeMap));
            mm->init();

            const uint32_t part_mask = (uint32_t)(N_PARTS - 1);

            for (int t = 0; t < n_threads; t++) {
                LocalMap* lm = lmaps[t];
                for (uint32_t s = 0; s < LocalMap::CAP; s++) {
                    int32_t key = lm->keys[s];
                    if (key == EMPTY_KEY) continue;
                    if ((hash32(key) & part_mask) != (uint32_t)pid) continue;
                    mm->insert(key, lm->vals[s]);
                }
            }

            // Collect qualifying keys from this partition
            for (uint32_t s = 0; s < MergeMap::CAP; s++) {
                if (mm->keys[s] != EMPTY_KEY && mm->vals[s] > 300.0) {
                    part_quals[pid].push_back(mm->keys[s]);
                }
            }

            free(mm);
        }

        // Build qualifying set (serial -- tiny)
        for (int pid = 0; pid < N_PARTS; pid++) {
            for (int32_t key : part_quals[pid]) {
                qual_set.insert(key);
            }
        }

        // Cleanup
        for (int t = 0; t < n_threads; t++) free(lmaps[t]);
        delete[] lmaps;

        munmap((void*)l_orderkey, li_ok_sz);
        munmap((void*)l_quantity,  li_qty_sz);
    }

    // ================================================================
    // Phase 2: Sequential scan of orders -- filter by qualifying orderkey set
    // ================================================================
    struct OrderInfo { int32_t orderkey, custkey, orderdate; double totalprice; };
    std::vector<OrderInfo> qualifying_orders;
    qualifying_orders.reserve(256);

    {
        GENDB_PHASE("orders_scan");

        size_t ok_sz, ck_sz, od_sz, tp_sz;
        const int32_t* o_orderkey   = (const int32_t*)safe_mmap(gendb_dir + "/orders/o_orderkey.bin",   ok_sz);
        const int32_t* o_custkey    = (const int32_t*)safe_mmap(gendb_dir + "/orders/o_custkey.bin",    ck_sz);
        const int32_t* o_orderdate  = (const int32_t*)safe_mmap(gendb_dir + "/orders/o_orderdate.bin",  od_sz);
        const double*  o_totalprice = (const double*)safe_mmap(gendb_dir + "/orders/o_totalprice.bin",  tp_sz);
        int64_t n_orders = (int64_t)(ok_sz / sizeof(int32_t));

        for (int64_t i = 0; i < n_orders; i++) {
            if (qual_set.contains(o_orderkey[i])) {
                qualifying_orders.push_back({o_orderkey[i], o_custkey[i], o_orderdate[i], o_totalprice[i]});
            }
        }

        munmap((void*)o_orderkey,   ok_sz);
        munmap((void*)o_custkey,    ck_sz);
        munmap((void*)o_orderdate,  od_sz);
        munmap((void*)o_totalprice, tp_sz);
    }

    // ================================================================
    // Phase 3: Second lineitem scan -- compute sum_qty for qualifying orders
    //   Avoids loading 613MB lineitem_orderkey_hash.bin.
    //   l_orderkey + l_quantity are in page cache from Phase 1; only ~57 rows match.
    //   Parallel scan: 64 threads each read 1/64 of 240MB l_orderkey sequentially.
    // ================================================================

    SumQtyMap sqmap;
    sqmap.init();
    for (const auto& oi : qualifying_orders) sqmap.set_key(oi.orderkey);

    {
        GENDB_PHASE("lineitem_sum_qty_scan");

        size_t li_ok2_sz, li_qty2_sz;
        const int32_t* l_ok2  = (const int32_t*)safe_mmap(gendb_dir + "/lineitem/l_orderkey.bin",  li_ok2_sz);
        const double*  l_qty2 = (const double*) safe_mmap(gendb_dir + "/lineitem/l_quantity.bin",   li_qty2_sz);
        int64_t n_li2 = (int64_t)(li_ok2_sz / sizeof(int32_t));

        int n_thr2 = omp_get_max_threads();
        // Thread-local SumQtyMaps: 48KB each × 64 = 3MB total, fits in L2 per thread
        SumQtyMap* tsq = new SumQtyMap[n_thr2];

        #pragma omp parallel num_threads(n_thr2)
        {
            int tid  = omp_get_thread_num();
            int nthr = omp_get_num_threads();
            tsq[tid].init();
            for (const auto& oi : qualifying_orders) tsq[tid].set_key(oi.orderkey);

            int64_t chunk = (n_li2 + nthr - 1) / nthr;
            int64_t s0    = (int64_t)tid * chunk;
            int64_t e0    = std::min(s0 + chunk, n_li2);
            for (int64_t j = s0; j < e0; j++) {
                int32_t key = l_ok2[j];
                if (qual_set.contains(key))
                    tsq[tid].add(key, l_qty2[j]);
            }
        }

        // Serial merge (tiny: ~57 × 64 = 3648 operations)
        for (int t = 0; t < n_thr2; t++) {
            for (uint32_t s = 0; s < SumQtyMap::CAP; s++) {
                if (tsq[t].keys[s] != EMPTY_KEY)
                    sqmap.add(tsq[t].keys[s], tsq[t].vals[s]);
            }
        }
        delete[] tsq;

        munmap((void*)l_ok2,  li_ok2_sz);
        munmap((void*)l_qty2, li_qty2_sz);
    }

    // ================================================================
    // Phase 4: Customer hash index lookup for qualifying orders
    // ================================================================
    std::vector<ResultRow> results;
    results.reserve(qualifying_orders.size());

    {
        GENDB_PHASE("customer_lookup");

        // --- Customer unique hash index ---
        size_t cust_idx_sz;
        const char* cust_base = (const char*)safe_mmap(gendb_dir + "/indexes/customer_custkey_hash.bin", cust_idx_sz);
        uint32_t cust_cap     = *(const uint32_t*)cust_base;
        uint32_t cust_mask    = cust_cap - 1;
        const UHEntry* cust_ht = (const UHEntry*)(cust_base + 4);

        // --- Customer columns ---
        size_t c_ck_sz, c_no_sz, c_nd_sz;
        const int32_t*  c_custkey_col = (const int32_t*)safe_mmap(gendb_dir + "/customer/c_custkey.bin",       c_ck_sz);
        const uint32_t* c_name_off    = (const uint32_t*)safe_mmap(gendb_dir + "/customer/c_name_offsets.bin", c_no_sz);
        const char*     c_name_data   = (const char*)safe_mmap(gendb_dir + "/customer/c_name_data.bin",        c_nd_sz);

        for (const auto& oi : qualifying_orders) {
            // Customer lookup via hash index
            uint32_t cs = hash32(oi.custkey) & cust_mask;
            while (cust_ht[cs].key != EMPTY_KEY && cust_ht[cs].key != oi.custkey)
                cs = (cs + 1) & cust_mask;
            if (cust_ht[cs].key != oi.custkey) continue;
            uint32_t cust_row = cust_ht[cs].row_idx;

            int32_t c_custkey = c_custkey_col[cust_row];
            std::string c_name(c_name_data + c_name_off[cust_row],
                               c_name_off[cust_row + 1] - c_name_off[cust_row]);

            double sum_qty = sqmap.get(oi.orderkey);
            results.push_back({c_name, c_custkey, oi.orderkey, oi.orderdate, oi.totalprice, sum_qty});
        }

        munmap((void*)cust_base,      cust_idx_sz);
        munmap((void*)c_custkey_col,  c_ck_sz);
        munmap((void*)c_name_off,     c_no_sz);
        munmap((void*)c_name_data,    c_nd_sz);
    }

    // ================================================================
    // Phase 5: Sort top-100 and write CSV output
    //   ORDER BY o_totalprice DESC, o_orderdate ASC LIMIT 100
    // ================================================================
    {
        GENDB_PHASE("sort_output");

        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        };
        size_t top_k = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + top_k, results.end(), cmp);
        results.resize(top_k);

        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); return; }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
        for (const auto& r : results) {
            char date_buf[16]; gendb::epoch_days_to_date_str(r.o_orderdate, date_buf); std::string date_str(date_buf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                r.c_name.c_str(), r.c_custkey, r.o_orderkey,
                date_str.c_str(), r.o_totalprice, r.sum_qty);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
