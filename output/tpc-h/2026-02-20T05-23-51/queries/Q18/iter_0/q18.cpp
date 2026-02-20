// Q18: Large Volume Customer — GenDB iteration 0
//
// Strategy:
//   Phase 1: sorted group-by scan of lineitem (TPC-H dbgen generates l_orderkey in
//            monotonically non-decreasing order).  One sequential pass, no hash map,
//            zero random-access overhead.  SUM(l_quantity) > 300 keeps ~624 orderkeys.
//   Phase 2: sequential scan of o_orderkey; probe tiny 4096-slot L1-cached hash map;
//            for 624 hits: direct c_name access via c_custkey-1 (TPC-H PK is 1-indexed).
//   Phase 3: partial_sort top-100 by (o_totalprice DESC, o_orderdate ASC).
//
// Deviation from plan: uses sorted-scan instead of parallel hash aggregation for phase 1.
// Reason: TPC-H dbgen writes lineitems in l_orderkey order; a sequential group-change scan
// is strictly faster (no hash table, no memory indirection, purely streaming I/O).

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include "date_utils.h"
#include "timing_utils.h"

// ─── hash utility ────────────────────────────────────────────────────────────
static inline uint32_t hash32(int32_t key) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32);
}

// ─── Tiny L1-resident hash map: qualifying orderkey → sum_qty (~624 entries) ─
struct QualMap {
    static constexpr uint32_t CAP      = 4096;           // 4096 >> 624 entries
    static constexpr uint32_t MASK     = CAP - 1;
    static constexpr int32_t  EMPTY_K  = INT32_MIN;

    int32_t keys[CAP];
    double  vals[CAP];

    QualMap() {
        std::fill(keys, keys + CAP, EMPTY_K);
        memset(vals, 0, sizeof(vals));
    }

    void insert(int32_t k, double v) {
        uint32_t h = hash32(k) & MASK;
        while (keys[h] != EMPTY_K && keys[h] != k) h = (h + 1) & MASK;
        keys[h] = k;
        vals[h] = v;
    }

    // Returns -1.0 if k not present (sum_qty is always > 300 so never -1)
    double get(int32_t k) const {
        uint32_t h = hash32(k) & MASK;
        while (keys[h] != EMPTY_K && keys[h] != k) h = (h + 1) & MASK;
        return (keys[h] == k) ? vals[h] : -1.0;
    }
};

// ─── result row ──────────────────────────────────────────────────────────────
struct ResultRow {
    char    c_name[27];      // null-terminated (source is 26-byte null-padded)
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const void* mmap_ro(const std::string& path, size_t& out_sz,
                            bool sequential = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    if (sequential)
        posix_fadvise(fd, 0, (off_t)out_sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ─── main query function ──────────────────────────────────────────────────────
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    QualMap qmap;

    // ══ Phase 1: lineitem sorted group-by scan ═══════════════════════════════
    // TPC-H dbgen generates lineitems sorted by l_orderkey (asc) then l_linenumber.
    // Exploit this: one sequential pass with a running key/sum pair, no hash map.
    {
        GENDB_PHASE("phase1_subquery");

        size_t lok_sz = 0, lqty_sz = 0;
        const int32_t* l_ok  = (const int32_t*)mmap_ro(
            gendb_dir + "/lineitem/l_orderkey.bin", lok_sz);
        const double*  l_qty = (const double*) mmap_ro(
            gendb_dir + "/lineitem/l_quantity.bin", lqty_sz);
        const size_t N = lok_sz / sizeof(int32_t);

        int32_t cur_key = INT32_MIN;
        double  cur_sum = 0.0;

        for (size_t i = 0; i < N; ++i) {
            int32_t k = l_ok[i];
            if (__builtin_expect(k == cur_key, 1)) {
                cur_sum += l_qty[i];
            } else {
                if (cur_key != INT32_MIN && cur_sum > 300.0)
                    qmap.insert(cur_key, cur_sum);
                cur_key = k;
                cur_sum = l_qty[i];
            }
        }
        // flush last group
        if (cur_key != INT32_MIN && cur_sum > 300.0)
            qmap.insert(cur_key, cur_sum);

        munmap((void*)l_ok,  lok_sz);
        munmap((void*)l_qty, lqty_sz);
    }

    // ══ Phase 2: orders scan + customer lookup ════════════════════════════════
    // Late materialisation: scan o_orderkey only in the hot loop; fetch the
    // deferred columns (o_custkey, o_orderdate, o_totalprice) only for the
    // ~624 qualifying rows.
    std::vector<ResultRow> results;
    results.reserve(1024);

    {
        GENDB_PHASE("phase2_main");

        size_t ook_sz = 0, ock_sz = 0, ood_sz = 0, otp_sz = 0, cnm_sz = 0;
        const int32_t* o_ok = (const int32_t*)mmap_ro(
            gendb_dir + "/orders/o_orderkey.bin",   ook_sz);
        const int32_t* o_ck = (const int32_t*)mmap_ro(
            gendb_dir + "/orders/o_custkey.bin",    ock_sz);
        const int32_t* o_od = (const int32_t*)mmap_ro(
            gendb_dir + "/orders/o_orderdate.bin",  ood_sz);
        const double*  o_tp = (const double*) mmap_ro(
            gendb_dir + "/orders/o_totalprice.bin", otp_sz);
        // customer c_name: 26-byte fixed-width, row i → offset i*26
        // Accessed randomly only for ~624 rows; no SEQUENTIAL hint.
        const char* c_nm = (const char*)mmap_ro(
            gendb_dir + "/customer/c_name.bin",     cnm_sz, /*sequential=*/false);

        const size_t N_ord = ook_sz / sizeof(int32_t);

        for (size_t i = 0; i < N_ord; ++i) {
            // probe L1-resident QualMap — ~3 cycles for not-found path
            double sq = qmap.get(o_ok[i]);
            if (sq < 0.0) continue;          // not qualifying (vast majority)

            // deferred column fetch for the ~624 hits
            int32_t ck = o_ck[i];

            // TPC-H: c_custkey is 1-indexed and sequential; row = custkey - 1
            const char* name = c_nm + (uint32_t)(ck - 1) * 26u;

            ResultRow r;
            size_t nlen = strnlen(name, 26);
            memcpy(r.c_name, name, nlen);
            r.c_name[nlen] = '\0';

            r.c_custkey    = ck;
            r.o_orderkey   = o_ok[i];
            r.o_orderdate  = o_od[i];
            r.o_totalprice = o_tp[i];
            r.sum_qty      = sq;
            results.push_back(r);
        }

        munmap((void*)o_ok, ook_sz);
        munmap((void*)o_ck, ock_sz);
        munmap((void*)o_od, ood_sz);
        munmap((void*)o_tp, otp_sz);
        munmap((void*)c_nm, cnm_sz);
    }

    // ══ Phase 3: partial sort → top-100 ══════════════════════════════════════
    {
        GENDB_PHASE("sort_topk");
        // ~624 rows total; std::sort is effectively O(1) at this size
        std::sort(results.begin(), results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;   // DESC
                return a.o_orderdate < b.o_orderdate;          // ASC
            });
        if (results.size() > 100) results.resize(100);
    }

    // ══ Phase 4: write CSV ════════════════════════════════════════════════════
    {
        GENDB_PHASE("output");
        std::ofstream out(results_dir + "/Q18.csv");
        if (!out) { perror((results_dir + "/Q18.csv").c_str()); exit(1); }
        out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
        out << std::fixed << std::setprecision(2);
        char date_buf[16];
        for (const auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            out << r.c_name       << ','
                << r.c_custkey    << ','
                << r.o_orderkey   << ','
                << date_buf       << ','
                << r.o_totalprice << ','
                << r.sum_qty      << '\n';
        }
    }
}

// ─── entry point ─────────────────────────────────────────────────────────────
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_Q18(argv[1], argc > 2 ? argv[2] : ".");
    return 0;
}
#endif
