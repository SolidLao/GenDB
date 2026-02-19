/**
 * Q18: Large Volume Customer
 *
 * Key optimizations:
 * 1. Single lineitem pass: flat-array aggregation SUM(l_quantity) per l_orderkey
 *    using a 60M-entry int64_t array indexed directly by orderkey (no hash map, no merge).
 *    Atomic fetch_add allows lock-free parallel accumulation.
 * 2. Qualifying set (~1K entries) loaded into a tiny linear-probe hash map for O(1) probes.
 * 3. c_name derived at output time: TPC-H invariant → "Customer#" + 9-digit zero-padded custkey.
 *    No customer table scan needed.
 */

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Tiny linear-probe hash map: int32_t key → int64_t value (for qualifying set ~1K)
// Sentinel: key == 0 means empty (TPC-H orderkeys are >= 1)
// ---------------------------------------------------------------------------
struct QualMap {
    struct Slot { int32_t key; int64_t val; };
    std::vector<Slot> data;
    uint32_t mask;

    void init(size_t n) {
        size_t sz = 4096;
        while (sz < n * 4) sz <<= 1;
        data.assign(sz, {0, 0LL});
        mask = static_cast<uint32_t>(sz - 1);
    }

    void insert(int32_t key, int64_t val) {
        uint32_t h = (static_cast<uint32_t>(key) * 2654435769u) & mask;
        while (data[h].key != 0) h = (h + 1) & mask;
        data[h] = {key, val};
    }

    inline int64_t* lookup(int32_t key) {
        uint32_t h = (static_cast<uint32_t>(key) * 2654435769u) & mask;
        while (true) {
            if (data[h].key == key) return &data[h].val;
            if (data[h].key == 0)   return nullptr;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static const void* mmap_ro(const std::string& path, size_t& out_bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_bytes = static_cast<size_t>(st.st_size);
    void* p = mmap(nullptr, out_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, out_bytes, MADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t o_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;  // scaled ×100
    int64_t sum_qty;       // scaled ×100
};

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int NTHREADS = omp_get_max_threads();

    // -----------------------------------------------------------------------
    // Phase 1: Lineitem scan → flat-array SUM(l_quantity) per l_orderkey
    //
    // At TPC-H SF10: 15M orders, max orderkey ≤ 4 × 1,500,000 × 10 = 60,000,000.
    // Allocate flat array of 60M+1 int64_t (480 MB) indexed directly by l_orderkey.
    // Each cell accumulates the scaled l_quantity for that orderkey.
    // Parallel threads use atomic fetch_add (relaxed) — lock-free, low contention
    // since each key has on average only ~4 rows.
    // -----------------------------------------------------------------------
    static const size_t MAX_ORDERKEY = 60'000'001;

    std::vector<std::pair<int32_t, int64_t>> qualifying;  // {orderkey, sum_qty_scaled}

    {
        GENDB_PHASE("lineitem_scan_aggregate");

        // Allocate flat aggregation array via mmap (zero-initialized, lazy pages)
        size_t arr_bytes = MAX_ORDERKEY * sizeof(int64_t);
        int64_t* qty_arr = static_cast<int64_t*>(
            mmap(nullptr, arr_bytes, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        if (qty_arr == MAP_FAILED) { perror("mmap qty_arr"); exit(1); }
        // Hint: access pattern is random during scan, sequential during filter
        madvise(qty_arr, arr_bytes, MADV_RANDOM);

        // Load lineitem columns
        size_t sz_ok, sz_qty;
        const int32_t* l_orderkey = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/lineitem/l_orderkey.bin", sz_ok));
        const int64_t* l_quantity = reinterpret_cast<const int64_t*>(
            mmap_ro(gendb_dir + "/lineitem/l_quantity.bin", sz_qty));
        const size_t n_rows = sz_ok / sizeof(int32_t);

        // Parallel accumulation: atomic fetch_add per orderkey
        #pragma omp parallel for schedule(static) num_threads(NTHREADS)
        for (size_t i = 0; i < n_rows; i++) {
            __atomic_fetch_add(&qty_arr[l_orderkey[i]], l_quantity[i], __ATOMIC_RELAXED);
        }

        munmap(const_cast<int32_t*>(l_orderkey), sz_ok);
        munmap(const_cast<int64_t*>(l_quantity), sz_qty);

        // Sequential scan of flat array to find qualifying orderkeys (sum > 30000)
        // HAVING SUM(l_quantity) > 300 with scale_factor=100 → stored sum > 30000
        madvise(qty_arr, arr_bytes, MADV_SEQUENTIAL);

        {
            GENDB_PHASE("extract_qualifying");
            // Parallel scan with thread-local collection, then merge (tiny output ~1K)
            std::vector<std::vector<std::pair<int32_t, int64_t>>> thread_qual(NTHREADS);

            #pragma omp parallel for schedule(static) num_threads(NTHREADS)
            for (size_t k = 1; k < MAX_ORDERKEY; k++) {
                int64_t v = qty_arr[k];
                if (__builtin_expect(v > 30000LL, 0)) {
                    thread_qual[omp_get_thread_num()].emplace_back(
                        static_cast<int32_t>(k), v);
                }
            }
            for (auto& tq : thread_qual)
                for (auto& kv : tq) qualifying.push_back(kv);
        }

        munmap(qty_arr, arr_bytes);
    }

    // Build tiny O(1) hash map for qualifying orderkey lookup
    QualMap qual_map;
    qual_map.init(qualifying.size() + 1);
    for (auto& [k, v] : qualifying) qual_map.insert(k, v);

    // -----------------------------------------------------------------------
    // Phase 2: Orders scan → parallel semi-join probe against qualifying set
    // -----------------------------------------------------------------------
    std::vector<ResultRow> qual_orders;

    {
        GENDB_PHASE("orders_scan_semijoin");

        size_t sz_ok, sz_ck, sz_od, sz_tp;
        const int32_t* o_orderkey  = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderkey.bin",  sz_ok));
        const int32_t* o_custkey   = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_custkey.bin",   sz_ck));
        const int32_t* o_orderdate = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderdate.bin", sz_od));
        const int64_t* o_totalprice = reinterpret_cast<const int64_t*>(
            mmap_ro(gendb_dir + "/orders/o_totalprice.bin", sz_tp));
        const size_t n_orders = sz_ok / sizeof(int32_t);

        std::vector<std::vector<ResultRow>> thread_results(NTHREADS);
        for (auto& tr : thread_results) tr.reserve(32);

        #pragma omp parallel for schedule(static) num_threads(NTHREADS)
        for (size_t i = 0; i < n_orders; i++) {
            int32_t ok = o_orderkey[i];
            int64_t* sq = qual_map.lookup(ok);
            if (sq) {
                thread_results[omp_get_thread_num()].push_back(
                    {o_custkey[i], ok, o_orderdate[i], o_totalprice[i], *sq});
            }
        }

        for (auto& tr : thread_results)
            for (auto& r : tr) qual_orders.push_back(r);

        munmap(const_cast<int32_t*>(o_orderkey),   sz_ok);
        munmap(const_cast<int32_t*>(o_custkey),    sz_ck);
        munmap(const_cast<int32_t*>(o_orderdate),  sz_od);
        munmap(const_cast<int64_t*>(o_totalprice), sz_tp);
    }

    // -----------------------------------------------------------------------
    // Phase 3: Sort by o_totalprice DESC, o_orderdate ASC; take top 100
    // ~1K rows — partial_sort or std::sort both trivial at this scale
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("sort_topk");
        std::sort(qual_orders.begin(), qual_orders.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
        if (qual_orders.size() > 100) qual_orders.resize(100);
    }

    // -----------------------------------------------------------------------
    // Phase 4: Output CSV
    // c_name derived: TPC-H invariant → "Customer#" + 9-digit zero-padded custkey
    // o_totalprice and sum_qty: divide by 100 (scale_factor=100)
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[12];
        for (const auto& r : qual_orders) {
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(f, "Customer#%09d,%d,%d,%s,%.2f,%.2f\n",
                r.o_custkey,
                r.o_custkey,
                r.o_orderkey,
                date_buf,
                static_cast<double>(r.o_totalprice) / 100.0,
                static_cast<double>(r.sum_qty) / 100.0);
        }

        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
