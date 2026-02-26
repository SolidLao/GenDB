// Q3: Shipping Priority
// Strategy: index_nested_loop via pre-built sorted FK indexes (morsel-driven parallelism)
//   1. Resolve BUILDING code from c_mktsegment.dict
//   2. dim_filter: inline per-thread during main_scan (no separate bitmap needed)
//   3. main_scan (morsel-driven over ~1.5M customer rows, 64 morsels):
//      a. Per qualifying customer (c_mktsegment == BUILDING):
//         binary-search orders_custkey_sorted → iterate matching orders
//         filter o_orderdate < 9204 inline
//      b. Per qualifying order:
//         binary-search lineitem_orderkey_sorted → iterate matching lineitems
//         filter l_shipdate > 9204 inline, accumulate revenue
//      c. Push (orderkey, revenue, odate, ospri) into thread-local result vector
//   4. Merge thread-local vectors → partial_sort top-10 → write CSV

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_CONST   = 9204;    // DATE '1995-03-15'
static constexpr int64_t MORSEL_SIZE  = 23438;   // ~64 morsels over 1.5M rows

// ─────────────────────────────────────────────────────────────────────────────
// FK-sorted index pair {key, row_id}
// ─────────────────────────────────────────────────────────────────────────────
struct Pair { int32_t key; int32_t row_id; };
static_assert(sizeof(Pair) == 8, "Pair must be 8 bytes");

// Hand-rolled binary search: first index where pairs[i].key >= target
inline size_t lb_pairs(const Pair* __restrict__ pairs, size_t n, int32_t target) {
    size_t lo = 0, hi = n;
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if (pairs[mid].key < target) lo = mid + 1;
        else                          hi = mid;
    }
    return lo;
}

// ─────────────────────────────────────────────────────────────────────────────
// mmap helper
// ─────────────────────────────────────────────────────────────────────────────
struct MmapRegion {
    const void* ptr = nullptr;
    size_t      sz  = 0;
    bool valid() const { return ptr != nullptr && ptr != MAP_FAILED; }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    void unmap() {
        if (valid()) { munmap(const_cast<void*>(ptr), sz); ptr = nullptr; }
    }
};

static MmapRegion do_mmap(const std::string& path, bool sequential) {
    MmapRegion r;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); return r; }
    struct stat st;
    fstat(fd, &st);
    r.sz  = (size_t)st.st_size;
    r.ptr = mmap(nullptr, r.sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (r.ptr == MAP_FAILED) { perror("mmap"); r.ptr = nullptr; close(fd); return r; }
    if (sequential) {
        madvise(const_cast<void*>(r.ptr), r.sz, MADV_SEQUENTIAL);
        posix_fadvise(fd, 0, (off_t)r.sz, POSIX_FADV_SEQUENTIAL);
    } else {
        madvise(const_cast<void*>(r.ptr), r.sz, MADV_WILLNEED);
    }
    close(fd);
    return r;
}

// ─────────────────────────────────────────────────────────────────────────────
// Result type
// ─────────────────────────────────────────────────────────────────────────────
struct Result {
    int32_t l_orderkey;
    double  revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ─────────────────────────────────────────────────────────────────────────────
// Main query
// ─────────────────────────────────────────────────────────────────────────────
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Phase 0: resolve BUILDING dict code ───────────────────────────────────
    int8_t BUILDING_CODE = -1;
    {
        const std::string dict_path = gendb_dir + "/customer/c_mktsegment.dict";
        FILE* f = fopen(dict_path.c_str(), "r");
        if (!f) { perror(("fopen: " + dict_path).c_str()); return; }
        char line[64];
        int8_t code = 0;
        while (fgets(line, sizeof(line), f)) {
            int len = (int)strlen(line);
            while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) line[--len] = '\0';
            if (strcmp(line, "BUILDING") == 0) { BUILDING_CODE = code; break; }
            code++;
        }
        fclose(f);
        if (BUILDING_CODE < 0) {
            fprintf(stderr, "BUILDING not found in dict: %s\n", dict_path.c_str());
            return;
        }
    }

    // ── data_loading ──────────────────────────────────────────────────────────
    // Customer columns (sequential scan)
    const int8_t*  c_mkt   = nullptr;
    const int32_t* c_ckey  = nullptr;
    uint64_t       n_cust  = 0;

    // Orders payload columns (random access via row_id from index)
    const int32_t* o_odate  = nullptr;
    const int32_t* o_okey   = nullptr;
    const int32_t* o_ospri  = nullptr;

    // Lineitem payload columns (random access via row_id from index)
    const int32_t* l_ship  = nullptr;
    const double*  l_ep    = nullptr;
    const double*  l_disc  = nullptr;

    // Pre-built FK sorted indexes
    const Pair* ord_pairs  = nullptr;   // orders_custkey_sorted  sorted by o_custkey
    uint64_t    ord_npairs = 0;
    const Pair* li_pairs   = nullptr;   // lineitem_orderkey_sorted sorted by l_orderkey
    uint64_t    li_npairs  = 0;

    std::vector<MmapRegion> regions;
    regions.reserve(12);

    {
        GENDB_PHASE("data_loading");

        // Customer
        auto f_cmkt  = do_mmap(gendb_dir + "/customer/c_mktsegment.bin", true);
        auto f_cckey = do_mmap(gendb_dir + "/customer/c_custkey.bin",    true);
        if (!f_cmkt.valid() || !f_cckey.valid()) {
            fprintf(stderr, "Failed to load customer columns\n"); return;
        }
        c_mkt  = f_cmkt.as<int8_t>();
        c_ckey = f_cckey.as<int32_t>();
        n_cust = f_cckey.sz / sizeof(int32_t);
        regions.push_back(f_cmkt); regions.push_back(f_cckey);

        // Orders payload columns (random access)
        auto f_oodate = do_mmap(gendb_dir + "/orders/o_orderdate.bin",    false);
        auto f_ookey  = do_mmap(gendb_dir + "/orders/o_orderkey.bin",     false);
        auto f_oospri = do_mmap(gendb_dir + "/orders/o_shippriority.bin", false);
        if (!f_oodate.valid() || !f_ookey.valid() || !f_oospri.valid()) {
            fprintf(stderr, "Failed to load orders columns\n"); return;
        }
        o_odate = f_oodate.as<int32_t>();
        o_okey  = f_ookey.as<int32_t>();
        o_ospri = f_oospri.as<int32_t>();
        regions.push_back(f_oodate); regions.push_back(f_ookey); regions.push_back(f_oospri);

        // Lineitem payload columns (random access)
        auto f_lship = do_mmap(gendb_dir + "/lineitem/l_shipdate.bin",      false);
        auto f_lep   = do_mmap(gendb_dir + "/lineitem/l_extendedprice.bin", false);
        auto f_ldisc = do_mmap(gendb_dir + "/lineitem/l_discount.bin",      false);
        if (!f_lship.valid() || !f_lep.valid() || !f_ldisc.valid()) {
            fprintf(stderr, "Failed to load lineitem columns\n"); return;
        }
        l_ship = f_lship.as<int32_t>();
        l_ep   = f_lep.as<double>();
        l_disc = f_ldisc.as<double>();
        regions.push_back(f_lship); regions.push_back(f_lep); regions.push_back(f_ldisc);

        // FK sorted index: orders_custkey_sorted  (sorted by o_custkey)
        // Layout: uint64_t num_pairs | {int32 key; int32 row_id}[num_pairs]
        auto f_ocs = do_mmap(gendb_dir + "/indexes/orders_custkey_sorted.bin", false);
        if (!f_ocs.valid()) { fprintf(stderr, "Failed to load orders_custkey_sorted\n"); return; }
        ord_npairs = *f_ocs.as<uint64_t>();
        ord_pairs  = reinterpret_cast<const Pair*>(f_ocs.as<uint8_t>() + sizeof(uint64_t));
        regions.push_back(f_ocs);

        // FK sorted index: lineitem_orderkey_sorted (sorted by l_orderkey)
        // Layout: uint64_t num_pairs | {int32 key; int32 row_id}[num_pairs]
        auto f_los = do_mmap(gendb_dir + "/indexes/lineitem_orderkey_sorted.bin", false);
        if (!f_los.valid()) { fprintf(stderr, "Failed to load lineitem_orderkey_sorted\n"); return; }
        li_npairs = *f_los.as<uint64_t>();
        li_pairs  = reinterpret_cast<const Pair*>(f_los.as<uint8_t>() + sizeof(uint64_t));
        regions.push_back(f_los);
    }

    // ── dim_filter: handled inline in main_scan (no bitmap needed) ────────────
    {
        GENDB_PHASE("dim_filter");
        // Customer filter (c_mktsegment == BUILDING_CODE) is applied per-row
        // inline during the morsel scan below — no separate structure required.
    }

    // ── build_joins: indexes already mmap'd during data_loading ───────────────
    {
        GENDB_PHASE("build_joins");
        // Both FK sorted indexes are already accessible via ord_pairs / li_pairs.
        // No runtime build step needed — they are pre-built on disk.
    }

    // ── main_scan: morsel-driven parallel loop over customer rows ─────────────
    const int nthreads = omp_get_max_threads();

    // Thread-local result vectors: since each customer belongs to exactly one
    // thread's morsel, and orders/lineitems are addressed through that customer,
    // there is NO overlap between thread-local results. Safe to concatenate.
    std::vector<std::vector<Result>> tl_results(nthreads);
    for (int t = 0; t < nthreads; t++)
        tl_results[t].reserve(32768);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            std::vector<Result>& local = tl_results[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (int64_t morsel_start = 0; morsel_start < (int64_t)n_cust; morsel_start += MORSEL_SIZE) {
                const int64_t morsel_end = std::min(morsel_start + MORSEL_SIZE, (int64_t)n_cust);

                for (int64_t ci = morsel_start; ci < morsel_end; ci++) {
                    // Filter: c_mktsegment == BUILDING
                    if (c_mkt[ci] != BUILDING_CODE) continue;

                    const int32_t custkey = c_ckey[ci];

                    // Binary search in orders_custkey_sorted for first row with key == custkey
                    const size_t ord_lo = lb_pairs(ord_pairs, ord_npairs, custkey);

                    for (size_t op = ord_lo;
                         op < ord_npairs && ord_pairs[op].key == custkey;
                         op++) {

                        const int32_t ord_row = ord_pairs[op].row_id;

                        // Filter: o_orderdate < DATE '1995-03-15'
                        const int32_t odate = o_odate[ord_row];
                        if (odate >= DATE_CONST) continue;

                        const int32_t orderkey = o_okey[ord_row];
                        const int32_t ospri    = o_ospri[ord_row];

                        // Binary search in lineitem_orderkey_sorted for first row with key == orderkey
                        const size_t li_lo = lb_pairs(li_pairs, li_npairs, orderkey);

                        double revenue = 0.0;
                        for (size_t lp = li_lo;
                             lp < li_npairs && li_pairs[lp].key == orderkey;
                             lp++) {

                            const int32_t li_row = li_pairs[lp].row_id;

                            // Filter: l_shipdate > DATE '1995-03-15'
                            if (l_ship[li_row] <= DATE_CONST) continue;

                            // Accumulate: SUM(l_extendedprice * (1 - l_discount))
                            revenue += l_ep[li_row] * (1.0 - l_disc[li_row]);
                        }

                        // Only emit if at least one lineitem row qualified (inner join)
                        if (revenue > 0.0) {
                            local.push_back({orderkey, revenue, odate, ospri});
                        }
                    }
                }
            }
        }
        // Implicit OpenMP barrier — all thread-local results are complete
    }

    // ── output: merge, partial_sort top-10, write CSV ─────────────────────────
    {
        GENDB_PHASE("output");

        // Merge thread-local result vectors into one
        std::vector<Result> all_results;
        {
            size_t total = 0;
            for (int t = 0; t < nthreads; t++) total += tl_results[t].size();
            all_results.reserve(total);
            for (int t = 0; t < nthreads; t++) {
                for (auto& r : tl_results[t]) all_results.push_back(r);
                tl_results[t].clear();
                tl_results[t].shrink_to_fit();
            }
        }

        // partial_sort top-10: revenue DESC, o_orderdate ASC, l_orderkey ASC
        const size_t limit = std::min((size_t)10, all_results.size());
        std::partial_sort(
            all_results.begin(),
            all_results.begin() + (std::ptrdiff_t)limit,
            all_results.end(),
            [](const Result& a, const Result& b) -> bool {
                if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
                if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
                return a.l_orderkey < b.l_orderkey;
            });

        // Write CSV
        gendb::init_date_tables();
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(("fopen: " + out_path).c_str()); }
        else {
            fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char date_buf[16];
            for (size_t i = 0; i < limit; i++) {
                const Result& r = all_results[i];
                gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
                fprintf(fp, "%d,%.2f,%s,%d\n",
                        r.l_orderkey, r.revenue, date_buf, r.o_shippriority);
            }
            fclose(fp);
        }
    }

    // Cleanup
    for (auto& m : regions) m.unmap();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
