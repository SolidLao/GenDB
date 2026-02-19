// Q18: Large Volume Customer
//
// BOTTLENECK DIAGNOSIS (iter 3):
//   The partitioned aggregation had each of 64 threads scan ALL 60M lineitem rows
//   but process only 1/64 of them (key-hash filter). This caused 64 × 720MB = 46GB
//   of DRAM reads, explaining the ~1055ms dim_filter time.
//
// ARCHITECTURAL FIX (iter 4):
//   Phase 1 — Direct flat-array aggregation (no hash table at all):
//     At SF10, l_orderkey ∈ [1, 60,000,000]. Allocate a 240MB int32 array indexed
//     by orderkey value. 64 threads scan DISJOINT row ranges (morsel-driven).
//     Each thread calls __atomic_fetch_add(&qtysums[okey], qty_scaled, RELAXED).
//     Total memory reads: 720MB (once). No merge step — shared array serves all threads.
//     madvise(MADV_HUGEPAGE) to reduce TLB pressure on the 240MB random-access array.
//   Phase 2 — Sequential scan of 240MB array → collect qualifying keys (sum > 30000).
//   Phase 3 — Index point lookups for qualifying orderkeys (same as before, 11ms).
//   Phase 4 — Top-K sort + CSV output (trivial).
//
// Key insight from query-planning.md Principle 5:
//   "Known value domains → direct array lookup. value[key] is faster than any hash probe."
//   The 60M-slot direct array enables O(1) updates with no hash overhead and no merge.

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>

#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// mmap helper (no MAP_POPULATE — parallel threads fault their own pages)
// ---------------------------------------------------------------------------
static void* do_mmap(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

// ---------------------------------------------------------------------------
// Hash index slot (matches on-disk layout)
// ---------------------------------------------------------------------------
struct HashSlot { int32_t key; uint32_t row_pos; };

static inline uint32_t idx_lookup(const HashSlot* slots, uint32_t cap,
                                   int32_t key, int shift) {
    uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> shift) & (cap - 1);
    for (;;) {
        if (slots[h].key == key) return slots[h].row_pos;
        if (slots[h].key == INT32_MIN) return UINT32_MAX;
        h = (h + 1) & (cap - 1);
    }
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================================
    // Phase 1: Parallel morsel scan → direct flat-array aggregation
    //   qtysums[orderkey] accumulates SUM(l_quantity * 100) for that orderkey.
    //   Scaled by 100 (int32) to use lock-free __atomic_fetch_add instead of CAS.
    //   TPC-H l_quantity ∈ {1.00..50.00} (integers stored as double).
    //   Threshold: 300.0 * 100 = 30000.
    // =========================================================================

    std::vector<std::pair<int32_t, double>> qualifying;

    {
        GENDB_PHASE("dim_filter");

        // --- Load lineitem columns (sequential mmap, parallel page faults) ---
        size_t sz_lok, sz_lqty;
        const int32_t* l_orderkey = (const int32_t*)do_mmap(
            gendb_dir + "/lineitem/l_orderkey.bin", sz_lok);
        const double*  l_quantity = (const double*)do_mmap(
            gendb_dir + "/lineitem/l_quantity.bin", sz_lqty);
        const size_t nrows = sz_lok / sizeof(int32_t);

        // --- Allocate direct accumulator array ---
        // At SF10: l_orderkey ∈ [1, 60,000,000]. Array size = 60,000,001 × 4B = ~240MB.
        // mmap anonymous → zero-initialized by OS (no explicit memset needed).
        // MADV_HUGEPAGE: use 2MB transparent huge pages to reduce TLB pressure.
        // With 60K regular 4KB pages vs 120 huge 2MB pages, the TLB miss rate drops
        // dramatically for random accesses to this 240MB array.
        const int32_t MAX_OKEY = 60000001;
        const size_t arr_bytes = (size_t)MAX_OKEY * sizeof(int32_t);
        int32_t* qtysums = (int32_t*)mmap(nullptr, arr_bytes,
                                           PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (qtysums == MAP_FAILED) { perror("mmap qtysums"); exit(1); }
        madvise(qtysums, arr_bytes, MADV_HUGEPAGE); // reduce TLB pressure for random writes

        // --- Parallel morsel scan ---
        // Each thread processes a DISJOINT range of lineitem rows.
        // No key-partitioning filter — every row in the range is processed.
        // Total memory reads: 720MB (l_orderkey + l_quantity), once across all threads.
        const int hw = (int)std::thread::hardware_concurrency();
        const int nthreads = (hw > 0 && hw <= 64) ? hw : 64;
        const size_t chunk = (nrows + nthreads - 1) / nthreads;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t start = (size_t)t * chunk;
                const size_t end   = std::min(start + chunk, nrows);
                for (size_t i = start; i < end; i++) {
                    // l_quantity is always an integer value (1..50) stored as double.
                    // Multiply by 100 for exact int32 representation. Threshold becomes 30000.
                    const int32_t qty_int = (int32_t)(l_quantity[i] * 100.0 + 0.5);
                    __atomic_fetch_add(&qtysums[l_orderkey[i]], qty_int, __ATOMIC_RELAXED);
                }
            });
        }
        for (auto& th : threads) th.join();

        // --- Sequential scan: collect qualifying orderkeys ---
        // qtysums is 240MB sequential → ~5ms at 50GB/s DRAM bandwidth.
        for (int32_t k = 1; k < MAX_OKEY; k++) {
            const int32_t s = qtysums[k]; // No atomic needed after join (all writes complete)
            if (s > 30000) {              // 300.0 * 100 = 30000
                qualifying.emplace_back(k, (double)s / 100.0);
            }
        }

        munmap(qtysums, arr_bytes);
        munmap((void*)l_orderkey, sz_lok);
        munmap((void*)l_quantity, sz_lqty);
    }

    // =========================================================================
    // Phase 2: Index point lookups for qualifying orderkeys.
    //   o_orderkey_hash  → orders row_pos → o_custkey, o_orderdate, o_totalprice
    //   c_custkey_hash   → customer row_pos → c_name (varstring)
    //   sum_qty reused from Phase 1 (no second lineitem scan needed).
    // =========================================================================

    std::vector<ResultRow> results;
    results.reserve(qualifying.size() + 4);

    {
        GENDB_PHASE("build_joins");

        // --- Orders hash index: cap=2^25=33554432, shift=64-25=39 ---
        size_t sz_ohi;
        const uint8_t* ohi_raw = (const uint8_t*)do_mmap(
            gendb_dir + "/orders/indexes/o_orderkey_hash.bin", sz_ohi);
        const uint32_t ohi_cap = *(const uint32_t*)(ohi_raw + 0);
        const HashSlot* ohi    = (const HashSlot*)(ohi_raw + 8);
        const int ohi_shift    = 64 - 25;

        // --- Customer hash index: cap=2^22=4194304, shift=64-22=42 ---
        size_t sz_chi;
        const uint8_t* chi_raw = (const uint8_t*)do_mmap(
            gendb_dir + "/customer/indexes/c_custkey_hash.bin", sz_chi);
        const uint32_t chi_cap = *(const uint32_t*)(chi_raw + 0);
        const HashSlot* chi    = (const HashSlot*)(chi_raw + 8);
        const int chi_shift    = 64 - 22;

        // --- Orders columns ---
        size_t sz_oc, sz_od, sz_op;
        const int32_t* o_custkey    = (const int32_t*)do_mmap(gendb_dir + "/orders/o_custkey.bin",    sz_oc);
        const int32_t* o_orderdate  = (const int32_t*)do_mmap(gendb_dir + "/orders/o_orderdate.bin",  sz_od);
        const double*  o_totalprice = (const double*)do_mmap(gendb_dir  + "/orders/o_totalprice.bin", sz_op);

        // --- Customer name (varstring, late materialization) ---
        size_t sz_cno, sz_cnd;
        const uint32_t* c_name_off = (const uint32_t*)do_mmap(
            gendb_dir + "/customer/c_name_offsets.bin", sz_cno);
        const char* c_name_dat = (const char*)do_mmap(
            gendb_dir + "/customer/c_name_data.bin", sz_cnd);

        for (auto& [okey, sqty] : qualifying) {
            const uint32_t orow = idx_lookup(ohi, ohi_cap, okey, ohi_shift);
            if (orow == UINT32_MAX) continue;

            const int32_t custkey    = o_custkey[orow];
            const int32_t orderdate  = o_orderdate[orow];
            const double  totalprice = o_totalprice[orow];

            const uint32_t crow = idx_lookup(chi, chi_cap, custkey, chi_shift);
            if (crow == UINT32_MAX) continue;

            const uint32_t ns = c_name_off[crow];
            const uint32_t ne = c_name_off[crow + 1];

            ResultRow r;
            r.c_name.assign(c_name_dat + ns, ne - ns);
            r.c_custkey    = custkey;
            r.o_orderkey   = okey;
            r.o_orderdate  = orderdate;
            r.o_totalprice = totalprice;
            r.sum_qty      = sqty;
            results.push_back(std::move(r));
        }
    }

    // =========================================================================
    // Phase 3: Sort by o_totalprice DESC, o_orderdate ASC; take top 100
    // =========================================================================
    {
        GENDB_PHASE("sort_topk");
        const int LIMIT = 100;
        if ((int)results.size() > LIMIT) {
            std::partial_sort(results.begin(), results.begin() + LIMIT, results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.o_totalprice != b.o_totalprice)
                        return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
            results.resize(LIMIT);
        } else {
            std::sort(results.begin(), results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.o_totalprice != b.o_totalprice)
                        return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
        }
    }

    // =========================================================================
    // Phase 4: Write CSV output
    // =========================================================================
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q18.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); exit(1); }

        fprintf(fout, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char dbuf[16];
        for (const auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, dbuf);
            fprintf(fout, "%s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name.c_str(),
                    r.c_custkey,
                    r.o_orderkey,
                    dbuf,
                    r.o_totalprice,
                    r.sum_qty);
        }
        fclose(fout);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
