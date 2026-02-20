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
// DynMap: dynamically-sized per-thread aggregation hash map
// Capacity computed at runtime based on actual thread count,
// preventing infinite-loop overflow for any OMP configuration.
// ============================================================
struct DynMap {
    int32_t* keys = nullptr;
    double*  vals = nullptr;
    uint32_t cap  = 0;
    uint32_t mask = 0;

    void init(uint32_t capacity) {
        cap  = capacity;
        mask = capacity - 1;
        keys = (int32_t*)aligned_alloc(64, (size_t)capacity * sizeof(int32_t));
        vals = (double*)  aligned_alloc(64, (size_t)capacity * sizeof(double));
        if (!keys || !vals) { perror("DynMap aligned_alloc"); exit(1); }
        memset(keys, 0x80, (size_t)capacity * sizeof(int32_t)); // INT32_MIN
        memset(vals, 0,    (size_t)capacity * sizeof(double));
    }

    void destroy() { free(keys); free(vals); keys = nullptr; vals = nullptr; }

    inline void insert(int32_t key, double qty) {
        uint32_t s = hash32(key) & mask;
        while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & mask;
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
    static constexpr uint32_t CAP  = 1u << 20; // 1048576
    static constexpr uint32_t MASK = CAP - 1;
    int32_t keys[CAP];
    double  vals[CAP]; // double sum of l_quantity per Column Reference spec

    void init() {
        memset(keys, 0x80, sizeof(keys));
        memset(vals, 0,    sizeof(vals));
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

    void init() { memset(keys, 0x80, sizeof(keys)); }

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
// mmap helper -- returns raw pointer and sets out_size
// ============================================================
static const void* safe_mmap(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    posix_fadvise(fd, 0, (off_t)out_size, POSIX_FADV_SEQUENTIAL);
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    return p;
}

// mmap for index/random-access files: NO MAP_POPULATE (only ~57 probes needed)
static const void* safe_mmap_idx(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    posix_fadvise(fd, 0, (off_t)out_size, POSIX_FADV_RANDOM);
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0); // lazy fault
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

        // -------------------------------------------------------
        // Pre-probe: determine ACTUAL thread count OMP will spawn.
        // Critical: LocalMap must be sized for actual threads to
        // prevent overflow infinite-loop when OMP_NUM_THREADS < 64.
        // -------------------------------------------------------
        int actual_threads = 1;
        #pragma omp parallel num_threads(n_threads)
        {
            #pragma omp single
            actual_threads = omp_get_num_threads();
        }

        // Compute safe per-thread LocalMap capacity:
        //   max unique keys per thread ≤ max_chunk_rows (conservative upper bound)
        //   maintain load factor < 50%  →  cap > 2 * max_chunk_rows
        int64_t max_chunk_rows = (n_li + actual_threads - 1) / actual_threads;
        uint32_t lm_cap = 1u;
        while (lm_cap <= (uint32_t)(max_chunk_rows * 2)) lm_cap <<= 1;
        lm_cap = std::max(lm_cap, 1u << 19); // minimum 512K slots

        DynMap* lmaps = new DynMap[actual_threads];

        // Phase 1a: parallel init + scan
        #pragma omp parallel num_threads(actual_threads)
        {
            int tid  = omp_get_thread_num();
            int nthr = omp_get_num_threads();

            lmaps[tid].init(lm_cap);

            int64_t chunk = (n_li + nthr - 1) / nthr;
            int64_t start = (int64_t)tid * chunk;
            int64_t end   = std::min(start + chunk, n_li);

            DynMap& lm = lmaps[tid];
            for (int64_t j = start; j < end; j++) {
                lm.insert(l_orderkey[j], l_quantity[j]);
            }
        }

        // Phase 1b: partitioned merge -- N_PARTS threads, no sharing
        std::vector<std::vector<int32_t>> part_quals(N_PARTS);

        #pragma omp parallel for schedule(static, 1) num_threads(N_PARTS)
        for (int pid = 0; pid < N_PARTS; pid++) {
            MergeMap* mm = (MergeMap*)aligned_alloc(64, sizeof(MergeMap));
            mm->init();

            const uint32_t part_mask = (uint32_t)(N_PARTS - 1);

            for (int t = 0; t < actual_threads; t++) {
                const int32_t* lkeys = lmaps[t].keys;
                const double*  lvals = lmaps[t].vals;
                const uint32_t  lcap = lmaps[t].cap;
                for (uint32_t s = 0; s < lcap; s++) {
                    int32_t key = lkeys[s];
                    if (key == EMPTY_KEY) continue;
                    if ((hash32(key) & part_mask) != (uint32_t)pid) continue;
                    mm->insert(key, lvals[s]);
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
        for (int t = 0; t < actual_threads; t++) lmaps[t].destroy();
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
    // Phase 3+4: Customer + lineitem index point lookups
    //   Per qualifying order:
    //     - probe customer_custkey_hash -> get c_name, c_custkey
    //     - probe lineitem_orderkey_hash -> sum l_quantity over matched rows
    // ================================================================
    std::vector<ResultRow> results;
    results.reserve(qualifying_orders.size());

    {
        GENDB_PHASE("index_lookups");

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

        // --- Lineitem multi-value hash index (613MB, random access -- NO MAP_POPULATE) ---
        // Layout: [uint32_t cap][uint32_t n_pos][cap x MHEntry(12B)][n_pos x uint32_t positions]
        size_t li_idx_sz;
        const char* li_base = (const char*)safe_mmap_idx(gendb_dir + "/indexes/lineitem_orderkey_hash.bin", li_idx_sz);
        uint32_t li_cap  = *(const uint32_t*)(li_base + 0);
        uint32_t li_mask = li_cap - 1;
        // n_pos at offset 4 -- not needed explicitly
        const MHEntry*  li_ht  = (const MHEntry*)(li_base + 8);
        const uint32_t* li_pos = (const uint32_t*)(li_base + 8 + (size_t)li_cap * sizeof(MHEntry));

        // --- Lineitem l_quantity (480MB, random access for ~57 orders -- NO MAP_POPULATE) ---
        size_t li_qty_sz;
        const double* l_quantity = (const double*)safe_mmap_idx(gendb_dir + "/lineitem/l_quantity.bin", li_qty_sz);

        for (const auto& oi : qualifying_orders) {
            // Customer lookup
            uint32_t cs = hash32(oi.custkey) & cust_mask;
            while (cust_ht[cs].key != EMPTY_KEY && cust_ht[cs].key != oi.custkey)
                cs = (cs + 1) & cust_mask;
            if (cust_ht[cs].key != oi.custkey) continue; // should not happen in valid data
            uint32_t cust_row = cust_ht[cs].row_idx;

            int32_t c_custkey = c_custkey_col[cust_row];
            std::string c_name(c_name_data + c_name_off[cust_row],
                               c_name_off[cust_row + 1] - c_name_off[cust_row]);

            // Lineitem index lookup -> SUM(l_quantity)
            uint32_t ls = hash32(oi.orderkey) & li_mask;
            while (li_ht[ls].key != EMPTY_KEY && li_ht[ls].key != oi.orderkey)
                ls = (ls + 1) & li_mask;

            double sum_qty = 0.0;
            if (li_ht[ls].key == oi.orderkey) {
                uint32_t off = li_ht[ls].offset;
                uint32_t cnt = li_ht[ls].count;
                for (uint32_t k = 0; k < cnt; k++) {
                    sum_qty += l_quantity[li_pos[off + k]];
                }
            }

            results.push_back({c_name, c_custkey, oi.orderkey, oi.orderdate, oi.totalprice, sum_qty});
        }

        munmap((void*)cust_base,      cust_idx_sz);
        munmap((void*)c_custkey_col,  c_ck_sz);
        munmap((void*)c_name_off,     c_no_sz);
        munmap((void*)c_name_data,    c_nd_sz);
        munmap((void*)li_base,        li_idx_sz);
        munmap((void*)l_quantity,     li_qty_sz);
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
