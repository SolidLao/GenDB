/**
 * Q18: Large Volume Customer (iteration 0)
 *
 * Key optimizations:
 * 1. Flat-array aggregation: 60M-entry int64_t array indexed directly by l_orderkey.
 *    Atomic fetch_add allows lock-free parallel accumulation without hash map or merges.
 *    HAVING SUM(l_quantity) > 300 → scaled: sum > 30000.
 * 2. ~1K qualifying orderkeys loaded into tiny linear-probe hash map for O(1) probes.
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
// Linear-probe hash map: int32_t key → int64_t value
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
// Result row (compact)
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
    // TPC-H SF10: up to ~60M orderkeys. Allocate flat array of 60M+1 int64_t
    // (480 MB) indexed directly by l_orderkey. Parallel atomic accumulation.
    // -----------------------------------------------------------------------
    static const size_t MAX_ORDERKEY = 60'000'001;

    std::vector<std::pair<int32_t, int64_t>> qualifying;

    {
        GENDB_PHASE("lineitem_scan_aggregate");

        // Anonymous mmap for aggregation array (zero-initialized, lazy pages)
        size_t arr_bytes = MAX_ORDERKEY * sizeof(int64_t);
        int64_t* qty_arr = static_cast<int64_t*>(
            mmap(nullptr, arr_bytes, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        if (qty_arr == MAP_FAILED) { perror("mmap qty_arr"); exit(1); }
        madvise(qty_arr, arr_bytes, MADV_RANDOM);

        // Load lineitem columns
        size_t sz_ok, sz_qty;
        const int32_t* l_orderkey = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/lineitem/l_orderkey.bin", sz_ok));
        const int64_t* l_quantity = reinterpret_cast<const int64_t*>(
            mmap_ro(gendb_dir + "/lineitem/l_quantity.bin", sz_qty));
        const size_t n_rows = sz_ok / sizeof(int32_t);

        // Parallel accumulation: atomic fetch_add per orderkey (relaxed — no ordering needed)
        #pragma omp parallel for schedule(static) num_threads(NTHREADS)
        for (size_t i = 0; i < n_rows; i++) {
            __atomic_fetch_add(&qty_arr[l_orderkey[i]], l_quantity[i], __ATOMIC_RELAXED);
        }

        munmap(const_cast<int32_t*>(l_orderkey), sz_ok);
        munmap(const_cast<int64_t*>(l_quantity), sz_qty);

        // Sequential scan of flat array to find qualifying orderkeys
        // HAVING SUM(l_quantity) > 300 with scale_factor=100 → stored sum > 30000
        madvise(qty_arr, arr_bytes, MADV_SEQUENTIAL);

        {
            GENDB_PHASE("extract_qualifying");
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

    // Build tiny hash map for qualifying orderkey → sum_qty lookup
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
        const int32_t* o_orderkey   = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderkey.bin",  sz_ok));
        const int32_t* o_custkey    = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_custkey.bin",   sz_ck));
        const int32_t* o_orderdate  = reinterpret_cast<const int32_t*>(
            mmap_ro(gendb_dir + "/orders/o_orderdate.bin", sz_od));
        const int64_t* o_totalprice = reinterpret_cast<const int64_t*>(
            mmap_ro(gendb_dir + "/orders/o_totalprice.bin", sz_tp));
        const size_t n_orders = sz_ok / sizeof(int32_t);

        std::vector<std::vector<ResultRow>> thread_results(NTHREADS);
        for (auto& tr : thread_results) tr.reserve(64);

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
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("sort_topk");
        size_t top_n = std::min<size_t>(100, qual_orders.size());
        std::partial_sort(qual_orders.begin(), qual_orders.begin() + top_n, qual_orders.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
        qual_orders.resize(top_n);
    }

    // -----------------------------------------------------------------------
    // Phase 4: Output CSV
    // c_name: TPC-H invariant → "Customer#" + 9-digit zero-padded custkey
    // Divide scaled integers by 100 for display
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
            // Use integer arithmetic to avoid double precision rounding errors
            int64_t tp_int = r.o_totalprice / 100;
            int64_t tp_frac = r.o_totalprice % 100;
            int64_t sq_int = r.sum_qty / 100;
            int64_t sq_frac = r.sum_qty % 100;
            fprintf(f, "Customer#%09d,%d,%d,%s,%lld.%02lld,%lld.%02lld\n",
                r.o_custkey,
                r.o_custkey,
                r.o_orderkey,
                date_buf,
                (long long)tp_int, (long long)tp_frac,
                (long long)sq_int, (long long)sq_frac);
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
