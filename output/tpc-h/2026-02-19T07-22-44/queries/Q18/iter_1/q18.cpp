/*
 * Q18: Large Volume Customer
 *
 * Strategy:
 *  Phase 1: Parallel scan of lineitem (l_orderkey + l_quantity) using 32 thread-local
 *           open-addressing hash maps → sequential merge into global map →
 *           filter sum > 300 → qualifying_set (~10K entries, 16K OA hash set) +
 *           qualifying_sums compact map (reused for main-query SUM output).
 *
 *  Phase 2: Sequential scan of orders (avoids random HDD access from index).
 *           On HDD, 300MB sequential scan beats 10K random probes into 268MB index.
 *           Filter by qualifying_set (fits in L2 cache). Collect ~10K order rows.
 *
 *  Phase 3: Direct array lookup for c_name: row = o_custkey - 1 (dense 1-based key).
 *
 *  Phase 4: Sort 10K rows by (o_totalprice DESC, o_orderdate ASC), emit top 100.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Open-addressing hash map: int32_t key (sentinel=0) → double accumulator
// TPC-H orderkeys are 1-based, so 0 is a safe empty sentinel.
// ---------------------------------------------------------------------------
struct OAMap {
    struct Entry { int32_t key; int32_t _pad; double val; }; // 16 bytes
    Entry* data;
    uint32_t cap;
    uint32_t mask;

    OAMap() : data(nullptr), cap(0), mask(0) {}

    void init(uint32_t c) {
        cap = c; mask = c - 1;
        // calloc gives zero-initialized memory; key=0 means EMPTY
        data = static_cast<Entry*>(calloc(c, sizeof(Entry)));
        if (!data) { perror("calloc OAMap"); exit(1); }
    }

    void destroy() { if (data) { free(data); data = nullptr; } }

    inline void add(int32_t key, double val) {
        uint32_t h = static_cast<uint32_t>(
            (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 0x9E3779B97F4A7C15ULL) >> 32
        ) & mask;
        while (data[h].key != 0 && data[h].key != key) h = (h + 1) & mask;
        if (data[h].key == 0) data[h].key = key;
        data[h].val += val;
    }

    inline double* find(int32_t key) const {
        uint32_t h = static_cast<uint32_t>(
            (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 0x9E3779B97F4A7C15ULL) >> 32
        ) & mask;
        while (data[h].key != 0 && data[h].key != key) h = (h + 1) & mask;
        return (data[h].key == key) ? &data[h].val : nullptr;
    }
};

// ---------------------------------------------------------------------------
// Small open-addressing hash set: int32_t (sentinel=0) for qualifying keys
// ---------------------------------------------------------------------------
struct OASet {
    int32_t* keys;
    uint32_t cap;
    uint32_t mask;

    OASet() : keys(nullptr), cap(0), mask(0) {}

    void init(uint32_t c) {
        cap = c; mask = c - 1;
        keys = static_cast<int32_t*>(calloc(c, sizeof(int32_t)));
        if (!keys) { perror("calloc OASet"); exit(1); }
    }

    void destroy() { if (keys) { free(keys); keys = nullptr; } }

    inline void insert(int32_t key) {
        uint32_t h = static_cast<uint32_t>(
            (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 0x9E3779B97F4A7C15ULL) >> 32
        ) & mask;
        while (keys[h] != 0 && keys[h] != key) h = (h + 1) & mask;
        keys[h] = key;
    }

    inline bool contains(int32_t key) const {
        uint32_t h = static_cast<uint32_t>(
            (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 0x9E3779B97F4A7C15ULL) >> 32
        ) & mask;
        while (keys[h] != 0 && keys[h] != key) h = (h + 1) & mask;
        return keys[h] == key;
    }
};

// ---------------------------------------------------------------------------
// Helper: mmap a binary file read-only
// ---------------------------------------------------------------------------
static const void* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    out_sz = static_cast<size_t>(st.st_size);
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, out_sz, MADV_SEQUENTIAL);
    return p;
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    char     c_name[27];   // null-terminated (26 bytes + sentinel)
    int32_t  c_custkey;
    int32_t  o_orderkey;
    int32_t  o_orderdate;
    double   o_totalprice;
    double   sum_qty;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================
    // Phase 1: Parallel lineitem scan → SUM(l_quantity) per l_orderkey
    // =========================================================
    OASet  qual_set;
    OAMap  qual_sums;   // compact: only entries with sum > 300

    {
        GENDB_PHASE("subquery_precompute");

        size_t lkey_sz, lqty_sz;
        const int32_t* l_orderkey = static_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/lineitem/l_orderkey.bin", lkey_sz));
        const double*  l_quantity = static_cast<const double*>(
            mmap_ro(gendb_dir + "/lineitem/l_quantity.bin", lqty_sz));
        const size_t n_lineitem = lkey_sz / sizeof(int32_t);

        // Thread-local maps: 2M capacity each (handles ~1.4M distinct keys @ 70% load)
        // Per thread: 2M × 16 bytes = 32MB; 32 threads = 1 GB total
        constexpr int      NTHREADS  = 32;
        constexpr uint32_t LOCAL_CAP = 1u << 21; // 2,097,152

        // Allocate per-thread maps
        OAMap local_maps[NTHREADS];
        for (int t = 0; t < NTHREADS; t++) local_maps[t].init(LOCAL_CAP);

        // Parallel scan with static morsel distribution
        #pragma omp parallel for schedule(static) num_threads(NTHREADS)
        for (int t = 0; t < NTHREADS; t++) {
            OAMap& lm = local_maps[t];
            const size_t start = static_cast<size_t>(t)     * n_lineitem / NTHREADS;
            const size_t end   = static_cast<size_t>(t + 1) * n_lineitem / NTHREADS;
            for (size_t i = start; i < end; i++) {
                lm.add(l_orderkey[i], l_quantity[i]);
            }
        }

        // Merge thread-local maps into a global map (32M capacity for all ~15M distinct keys)
        // 32M × 16 bytes = 512MB; calloc avoids upfront physical allocation
        OAMap global_agg;
        global_agg.init(1u << 25); // 33,554,432 entries

        for (int t = 0; t < NTHREADS; t++) {
            const OAMap& lm = local_maps[t];
            for (uint32_t b = 0; b < lm.cap; b++) {
                if (lm.data[b].key != 0) {
                    global_agg.add(lm.data[b].key, lm.data[b].val);
                }
            }
            local_maps[t].destroy();
        }

        // Filter: collect qualifying orderkeys (sum > 300) into compact structures
        // ~10K entries → 16K capacity is sufficient at 62% load
        qual_set.init(1u << 14);   // 16,384 slots
        qual_sums.init(1u << 14);  // 16,384 slots

        for (uint32_t b = 0; b < global_agg.cap; b++) {
            if (global_agg.data[b].key != 0 && global_agg.data[b].val > 300.0) {
                qual_set.insert(global_agg.data[b].key);
                qual_sums.add(global_agg.data[b].key, global_agg.data[b].val);
            }
        }

        global_agg.destroy();
        munmap(const_cast<int32_t*>(l_orderkey), lkey_sz);
        munmap(const_cast<double*>(l_quantity),   lqty_sz);
    }

    // =========================================================
    // Phase 2: Sequential orders scan + customer direct lookup
    // =========================================================
    std::vector<ResultRow> results;
    results.reserve(16384);

    {
        GENDB_PHASE("orders_scan");

        size_t okey_sz, ocust_sz, oprice_sz, odate_sz;
        const int32_t* o_orderkey  = static_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderkey.bin", okey_sz));
        const int32_t* o_custkey   = static_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_custkey.bin", ocust_sz));
        const double*  o_totalprice = static_cast<const double*>(
            mmap_ro(gendb_dir + "/orders/o_totalprice.bin", oprice_sz));
        const int32_t* o_orderdate = static_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderdate.bin", odate_sz));

        // c_name: 26 bytes/row fixed-width, row = custkey - 1
        size_t cname_sz;
        const char* c_name = static_cast<const char*>(
            mmap_ro(gendb_dir + "/customer/c_name.bin", cname_sz));

        const size_t n_orders = okey_sz / sizeof(int32_t);

        for (size_t i = 0; i < n_orders; i++) {
            const int32_t ok = o_orderkey[i];
            if (!qual_set.contains(ok)) continue;

            const int32_t ck = o_custkey[i];
            const char* name_ptr = c_name + static_cast<size_t>(ck - 1) * 26;

            ResultRow row;
            memcpy(row.c_name, name_ptr, 26);
            row.c_name[26] = '\0';
            row.c_custkey   = ck;
            row.o_orderkey  = ok;
            row.o_orderdate = o_orderdate[i];
            row.o_totalprice = o_totalprice[i];

            // SUM(l_quantity) reused from subquery phase (no second lineitem scan)
            double* sq = qual_sums.find(ok);
            row.sum_qty = sq ? *sq : 0.0;

            results.push_back(row);
        }

        munmap(const_cast<int32_t*>(o_orderkey),   okey_sz);
        munmap(const_cast<int32_t*>(o_custkey),    ocust_sz);
        munmap(const_cast<double*>(o_totalprice),  oprice_sz);
        munmap(const_cast<int32_t*>(o_orderdate),  odate_sz);
        munmap(const_cast<char*>(c_name),          cname_sz);
    }

    // =========================================================
    // Phase 3: Sort + Top-100
    // =========================================================
    {
        GENDB_PHASE("sort_topk");

        // ORDER BY o_totalprice DESC, o_orderdate ASC
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        });

        if (results.size() > 100) results.resize(100);
    }

    // =========================================================
    // Phase 4: Output CSV
    // =========================================================
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[11];
        for (const ResultRow& row : results) {
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                row.c_name, row.c_custkey, row.o_orderkey,
                date_buf, row.o_totalprice, row.sum_qty);
        }

        fclose(f);
    }

    qual_set.destroy();
    qual_sums.destroy();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
