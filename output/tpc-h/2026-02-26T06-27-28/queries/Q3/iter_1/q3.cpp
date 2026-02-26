// Q3: Shipping Priority — iter_1 optimized
//
// Strategy: hash-join + sequential scans + lineitem start_row optimization
//   Root-cause of 208ms (iter_0): index-nested-loop with 2 large FK indexes loaded via
//   MAP_POPULATE + 1.47M binary searches into 480MB cache-exceeding index.
//
// New approach:
//   1. dim_filter: parallel customer scan → bitmap (187.5KB, L2-resident)
//      cust_bm[custkey] = 1 for BUILDING customers
//   2. build_joins: sequential orders scan (half skipped via zone map: blocks 73-149 all fail)
//      probe cust_bm[o_custkey] O(1) → insert qualifying orders into 4M-slot OrdAgg hash map
//      OrdAgg is column-layout: oa_keys[4M]=16MB + oa_odate/ospri/revenue = total ~80MB
//      Probe during main_scan touches only oa_keys (16MB, LLC-resident at 44MB)
//   3. main_scan: use zone map + binary search on l_shipdate.bin to find start_row
//      → scan only l_orderkey/l_extendedprice/l_discount from start_row to end
//      → NO l_shipdate check in main loop (all rows >= start_row have l_shipdate > 9204)
//      → saves ~50% of lineitem reads (~600MB+ of data)
//      → revenue accumulated via CAS atomic_add per slot
//   4. output: parallel top-10 per thread → merge 640 candidates → CSV
//
// Key data: l_shipdate zonemap block 276 is first qualifying block (row ~27.6M);
//           orders zonemap block 73+ all fail (min >= 9204), skip 50% of orders table.

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <climits>
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
static constexpr int32_t  DATE_CONST   = 9204;        // DATE '1995-03-15'
static constexpr int32_t  ORD_SENTINEL = 0;            // o_orderkey always > 0 in TPC-H
static constexpr uint32_t ORD_CAP      = 4194304u;     // 4M = 2^22
static constexpr uint64_t ORD_MASK     = ORD_CAP - 1u;
static constexpr uint64_t BLOCK_SIZE   = 100000ull;    // rows per block

// ─────────────────────────────────────────────────────────────────────────────
// Zone-map entry (matches index file format: uint64_t n_blocks + ZmEntry[])
// ─────────────────────────────────────────────────────────────────────────────
struct ZmEntry { int32_t min_v; int32_t max_v; };

// ─────────────────────────────────────────────────────────────────────────────
// Customer bitmap: dense keys [1..1500000] → 187.5 KB — fits in L2
// Atomic byte-OR ensures no bit is lost under parallel writes.
// ─────────────────────────────────────────────────────────────────────────────
inline void bm_set_atomic(uint8_t* bm, int32_t k) {
    if (__builtin_expect(k > 0, 1)) {
        const uint32_t uk = (uint32_t)k;
        __atomic_or_fetch(&bm[uk >> 3], (uint8_t)(1u << (uk & 7u)), __ATOMIC_RELAXED);
    }
}
inline bool bm_test(const uint8_t* bm, int32_t k) {
    return __builtin_expect(k > 0, 1) && ((bm[(uint32_t)k >> 3] >> ((uint32_t)k & 7u)) & 1u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Hash: Knuth multiplicative — fast, good dispersion for int32 keys
// ─────────────────────────────────────────────────────────────────────────────
inline uint64_t hash_knuth(int32_t key) {
    return ((uint32_t)key * 2654435761u) & ORD_MASK;
}

// ─────────────────────────────────────────────────────────────────────────────
// Atomic double CAS: *ptr += val  (no hardware 64-bit fadd on x86)
// ─────────────────────────────────────────────────────────────────────────────
inline void atomic_add_double(double* ptr, double val) {
    uint64_t old_bits, new_bits;
    do {
        old_bits = __atomic_load_n(reinterpret_cast<uint64_t*>(ptr), __ATOMIC_RELAXED);
        double old_val, new_val;
        memcpy(&old_val, &old_bits, sizeof(double));
        new_val = old_val + val;
        memcpy(&new_bits, &new_val, sizeof(double));
    } while (!__atomic_compare_exchange_n(
                 reinterpret_cast<uint64_t*>(ptr), &old_bits, new_bits,
                 false, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
}

// ─────────────────────────────────────────────────────────────────────────────
// OrdAgg insert: o_orderkey is a PK → exactly one thread inserts each key.
// Lazy-init: oa_revenue[h] = 0.0 here → no separate 32MB memset.
// ─────────────────────────────────────────────────────────────────────────────
inline void oa_insert(int32_t* __restrict__ oa_keys,
                      int32_t* __restrict__ oa_odate,
                      int32_t* __restrict__ oa_ospri,
                      double*  __restrict__ oa_revenue,
                      int32_t ok, int32_t odate, int32_t ospri) {
    uint64_t h = hash_knuth(ok);
    while (true) {
        int32_t expected = ORD_SENTINEL;
        if (__atomic_compare_exchange_n(&oa_keys[h], &expected, ok,
                                        false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
            oa_odate[h]   = odate;
            oa_ospri[h]   = ospri;
            oa_revenue[h] = 0.0;
            return;
        }
        h = (h + 1) & ORD_MASK;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OrdAgg probe: touches only oa_keys (16MB, LLC-resident at 44MB LLC).
// Returns slot index or UINT64_MAX if absent.
// Miss is the common case (~75% of qualifying lineitems), so no __builtin_expect here.
// ─────────────────────────────────────────────────────────────────────────────
inline uint64_t oa_probe(const int32_t* __restrict__ oa_keys, int32_t ok) {
    uint64_t h = hash_knuth(ok);
    while (true) {
        const int32_t hk = __atomic_load_n(&oa_keys[h], __ATOMIC_ACQUIRE);
        if (hk == ORD_SENTINEL) return UINT64_MAX;
        if (hk == ok)           return h;
        h = (h + 1) & ORD_MASK;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// mmap helper — NO MAP_POPULATE: page faults amortized into parallel phases
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
    r.ptr = mmap(nullptr, r.sz, PROT_READ, MAP_PRIVATE, fd, 0);
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
// Main query
// ─────────────────────────────────────────────────────────────────────────────
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Phase 0: resolve BUILDING dict code at runtime ────────────────────────
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
        if (BUILDING_CODE < 0) { fprintf(stderr, "BUILDING not found in dict\n"); return; }
    }

    // ── data pointers ─────────────────────────────────────────────────────────
    const int8_t*  c_mkt   = nullptr;
    const int32_t* c_ckey  = nullptr;
    uint64_t       n_cust  = 0;

    const int32_t* o_okey  = nullptr;
    const int32_t* o_odate = nullptr;
    const int32_t* o_cust  = nullptr;
    const int32_t* o_ospri = nullptr;
    uint64_t       n_orows = 0;

    // Orders zone map (format: uint64_t n_blocks, ZmEntry[])
    const ZmEntry* o_zm      = nullptr;
    uint64_t       o_nblocks = 0;

    const int32_t* l_okey  = nullptr;
    const double*  l_ep    = nullptr;
    const double*  l_disc  = nullptr;
    uint64_t       n_lrows = 0;

    // Lineitem shipdate (needed only for binary search to find start_row)
    const int32_t* l_ship  = nullptr;

    // Lineitem zone map (format: uint64_t n_blocks, ZmEntry[])
    const ZmEntry* l_zm      = nullptr;
    uint64_t       l_nblocks = 0;

    std::vector<MmapRegion> regions;
    regions.reserve(16);
    MmapRegion f_lship_region;   // kept separate so we can unmap after binary search

    {
        GENDB_PHASE("data_loading");

        // Customer (small, sequential scan in dim_filter)
        auto f_cmkt  = do_mmap(gendb_dir + "/customer/c_mktsegment.bin", true);
        auto f_cckey = do_mmap(gendb_dir + "/customer/c_custkey.bin",    true);
        if (!f_cmkt.valid() || !f_cckey.valid()) {
            fprintf(stderr, "Failed to load customer columns\n"); return;
        }
        c_mkt  = f_cmkt.as<int8_t>();
        c_ckey = f_cckey.as<int32_t>();
        n_cust = f_cckey.sz / sizeof(int32_t);
        regions.push_back(f_cmkt); regions.push_back(f_cckey);

        // Orders columns (sequential scan in build_joins)
        auto f_ookey  = do_mmap(gendb_dir + "/orders/o_orderkey.bin",     true);
        auto f_oodate = do_mmap(gendb_dir + "/orders/o_orderdate.bin",    true);
        auto f_ocust  = do_mmap(gendb_dir + "/orders/o_custkey.bin",      true);
        auto f_oospri = do_mmap(gendb_dir + "/orders/o_shippriority.bin", true);
        if (!f_ookey.valid() || !f_oodate.valid() || !f_ocust.valid() || !f_oospri.valid()) {
            fprintf(stderr, "Failed to load orders columns\n"); return;
        }
        o_okey  = f_ookey.as<int32_t>();
        o_odate = f_oodate.as<int32_t>();
        o_cust  = f_ocust.as<int32_t>();
        o_ospri = f_oospri.as<int32_t>();
        n_orows = f_ookey.sz / sizeof(int32_t);
        regions.push_back(f_ookey);  regions.push_back(f_oodate);
        regions.push_back(f_ocust);  regions.push_back(f_oospri);

        // Orders zone map: format = uint64_t n_blocks, ZmEntry[n_blocks]
        auto f_ozm = do_mmap(gendb_dir + "/indexes/orders_orderdate_zonemap.bin", false);
        if (f_ozm.valid()) {
            o_nblocks = *f_ozm.as<uint64_t>();
            o_zm      = reinterpret_cast<const ZmEntry*>(f_ozm.as<uint8_t>() + sizeof(uint64_t));
            regions.push_back(f_ozm);
        }

        // Lineitem payload columns (sequential scan from start_row in main_scan)
        auto f_lokey = do_mmap(gendb_dir + "/lineitem/l_orderkey.bin",      true);
        auto f_lep   = do_mmap(gendb_dir + "/lineitem/l_extendedprice.bin", true);
        auto f_ldisc = do_mmap(gendb_dir + "/lineitem/l_discount.bin",      true);
        if (!f_lokey.valid() || !f_lep.valid() || !f_ldisc.valid()) {
            fprintf(stderr, "Failed to load lineitem payload columns\n"); return;
        }
        l_okey = f_lokey.as<int32_t>();
        l_ep   = f_lep.as<double>();
        l_disc = f_ldisc.as<double>();
        n_lrows = f_lokey.sz / sizeof(int32_t);
        regions.push_back(f_lokey); regions.push_back(f_lep); regions.push_back(f_ldisc);

        // l_shipdate: mmap for binary search to find start_row; NOT used in main scan
        f_lship_region = do_mmap(gendb_dir + "/lineitem/l_shipdate.bin", false);
        if (!f_lship_region.valid()) {
            fprintf(stderr, "Failed to load l_shipdate for binary search\n"); return;
        }
        l_ship = f_lship_region.as<int32_t>();

        // Lineitem zone map: format = uint64_t n_blocks, ZmEntry[n_blocks]
        auto f_lzm = do_mmap(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", false);
        if (f_lzm.valid()) {
            l_nblocks = *f_lzm.as<uint64_t>();
            l_zm      = reinterpret_cast<const ZmEntry*>(f_lzm.as<uint8_t>() + sizeof(uint64_t));
            regions.push_back(f_lzm);
        }
    }

    // ── dim_filter: parallel customer scan → build custkey bitmap ─────────────
    // bitmap size = ceil(1500001 / 8) = 187,501 bytes ≈ 187.5KB, fits in L2
    static constexpr int32_t CUST_MAX   = 1500001;
    static constexpr size_t  CUST_BM_SZ = (CUST_MAX + 7) / 8;
    uint8_t* cust_bm = new uint8_t[CUST_BM_SZ]();

    const int nthreads = omp_get_max_threads();

    {
        GENDB_PHASE("dim_filter");
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (uint64_t i = 0; i < n_cust; i++) {
            if (c_mkt[i] == BUILDING_CODE)
                bm_set_atomic(cust_bm, c_ckey[i]);
        }
    }

    // ── OrdAgg hash map (column-layout, 4M slots open-addressing) ─────────────
    // oa_keys:    16MB  — probe-only array, LLC-resident (44MB LLC)
    // oa_odate:   16MB  — written at insert, read at output
    // oa_ospri:   16MB  — written at insert, read at output
    // oa_revenue: 32MB  — lazy-init to 0.0 at insert, CAS-updated in main_scan
    int32_t* oa_keys    = new int32_t[ORD_CAP];
    int32_t* oa_odate   = new int32_t[ORD_CAP];
    int32_t* oa_ospri   = new int32_t[ORD_CAP];
    double*  oa_revenue = new double [ORD_CAP];

    // ── build_joins: parallel init oa_keys + sequential orders scan → insert ──
    {
        GENDB_PHASE("build_joins");

        // Parallel first-touch init of oa_keys to sentinel (16MB, spreads across NUMA)
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (uint32_t i = 0; i < ORD_CAP; i++) {
            oa_keys[i] = ORD_SENTINEL;
        }

        // Sequential orders scan with zone-map skip.
        // Orders are sorted by o_orderdate ascending; blocks with min_v >= DATE_CONST
        // have all rows failing o_orderdate < 9204 → skip entirely.
        // For zone map: n_blocks=150, blocks 73-149 all have min_v >= 9204 → skip 50%.
        const uint64_t o_nblks = (o_zm && o_nblocks > 0)
                                 ? o_nblocks
                                 : ((n_orows + BLOCK_SIZE - 1) / BLOCK_SIZE);

        #pragma omp parallel for schedule(dynamic, 2) num_threads(nthreads)
        for (uint64_t b = 0; b < o_nblks; b++) {
            // Zone-map skip: all dates in block >= 9204 → none satisfy o_orderdate < 9204
            if (o_zm && o_zm[b].min_v >= DATE_CONST) continue;

            const uint64_t rs = b * BLOCK_SIZE;
            const uint64_t re = std::min(rs + BLOCK_SIZE, n_orows);

            for (uint64_t i = rs; i < re; i++) {
                if (o_odate[i] >= DATE_CONST) continue;           // o_orderdate < 9204
                if (!bm_test(cust_bm, o_cust[i])) continue;       // c_mktsegment = BUILDING
                oa_insert(oa_keys, oa_odate, oa_ospri, oa_revenue,
                          o_okey[i], o_odate[i], o_ospri[i]);
            }
        }
        // OMP barrier: all inserts + lazy revenue inits complete before main_scan
    }

    // ── Find start_row: zone map → first qualifying block → binary search ─────
    // All lineitem rows from start_row onward have l_shipdate > 9204 (guaranteed
    // by sorted order). Main scan does NOT check l_shipdate → saves 120MB reads.
    uint64_t start_row = 0;
    {
        // Step 1: zone map → find first block where max_v > 9204
        uint64_t first_qual_block = 0;
        if (l_zm && l_nblocks > 0) {
            for (uint64_t b = 0; b < l_nblocks; b++) {
                if (l_zm[b].max_v > DATE_CONST) {
                    first_qual_block = b;
                    break;
                }
                first_qual_block = b + 1;  // all blocks fail
            }
        }

        // Step 2: binary search within [first_qual_block*BLOCK_SIZE .. n_lrows)
        // for first row where l_shipdate > 9204
        int64_t lo = (int64_t)(first_qual_block * BLOCK_SIZE);
        int64_t hi = (int64_t)n_lrows;
        while (lo < hi) {
            const int64_t mid = lo + ((hi - lo) >> 1);
            if (l_ship[mid] <= DATE_CONST) lo = mid + 1;
            else                            hi = mid;
        }
        start_row = (uint64_t)lo;

        // Release l_shipdate: not needed in main scan
        // Advise kernel to free pages; unmap after main query
        madvise(const_cast<int32_t*>(l_ship), f_lship_region.sz, MADV_DONTNEED);
        l_ship = nullptr;
    }

    // ── main_scan: parallel lineitem scan from start_row → probe → atomic add ─
    // Critical path: 3 columns × ~32M rows = ~640MB sequential reads
    // No l_shipdate check: all rows [start_row..n_lrows) have l_shipdate > 9204
    {
        GENDB_PHASE("main_scan");

        const uint64_t n_scan_rows = n_lrows - start_row;
        // Compute number of morsels of BLOCK_SIZE starting from start_row
        const uint64_t n_morsels = (n_scan_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

        #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (uint64_t m = 0; m < n_morsels; m++) {
            const uint64_t rs = start_row + m * BLOCK_SIZE;
            const uint64_t re = std::min(rs + BLOCK_SIZE, n_lrows);

            for (uint64_t i = rs; i < re; i++) {
                // No l_shipdate check: guaranteed l_ship[i] > DATE_CONST for all i >= start_row
                const int32_t  lok  = l_okey[i];
                const uint64_t slot = oa_probe(oa_keys, lok);
                if (slot == UINT64_MAX) continue;   // miss: ~75% of rows

                // Revenue formula (anchored): l_extendedprice * (1 - l_discount)
                const double contrib = l_ep[i] * (1.0 - l_disc[i]);
                atomic_add_double(&oa_revenue[slot], contrib);
            }
        }
        // OMP barrier: all revenue accumulations complete
    }

    // ── output: parallel top-10 reduction → merge → CSV ──────────────────────
    {
        GENDB_PHASE("output");

        struct Result {
            int32_t key;
            int32_t odate;
            int32_t ospri;
            double  rev;
        };

        auto is_better = [](const Result& a, const Result& b) -> bool {
            if (a.rev   != b.rev)   return a.rev   > b.rev;    // revenue DESC
            if (a.odate != b.odate) return a.odate < b.odate;  // o_orderdate ASC
            return a.key < b.key;                               // l_orderkey ASC
        };

        struct TopK {
            Result items[10];
            int    count     = 0;
            int    worst_idx = 0;

            void update_worst(const decltype(is_better)& cmp) {
                worst_idx = 0;
                for (int j = 1; j < count; j++)
                    if (cmp(items[worst_idx], items[j])) worst_idx = j;
            }
            void try_add(const Result& r, const decltype(is_better)& cmp) {
                if (count < 10) {
                    items[count++] = r;
                    if (count == 10) update_worst(cmp);
                } else if (cmp(r, items[worst_idx])) {
                    items[worst_idx] = r;
                    update_worst(cmp);
                }
            }
        };

        std::vector<TopK> topk_arr(nthreads);

        // Each thread scans its static partition of oa_keys (16MB / nthreads per thread)
        #pragma omp parallel num_threads(nthreads)
        {
            TopK& tk = topk_arr[omp_get_thread_num()];
            #pragma omp for schedule(static) nowait
            for (uint32_t i = 0; i < ORD_CAP; i++) {
                if (oa_keys[i] == ORD_SENTINEL) continue;
                if (oa_revenue[i] <= 0.0) continue;    // no matching lineitem
                Result r{oa_keys[i], oa_odate[i], oa_ospri[i], oa_revenue[i]};
                tk.try_add(r, is_better);
            }
        }

        // Free OrdAgg arrays
        delete[] oa_keys;    oa_keys    = nullptr;
        delete[] oa_odate;   oa_odate   = nullptr;
        delete[] oa_ospri;   oa_ospri   = nullptr;
        delete[] oa_revenue; oa_revenue = nullptr;

        // Merge: at most nthreads*10 candidates
        std::vector<Result> merged;
        merged.reserve(nthreads * 10);
        for (int t = 0; t < nthreads; t++)
            for (int j = 0; j < topk_arr[t].count; j++)
                merged.push_back(topk_arr[t].items[j]);

        const size_t limit = std::min((size_t)10, merged.size());
        std::partial_sort(merged.begin(), merged.begin() + (std::ptrdiff_t)limit, merged.end(),
                          [&](const Result& a, const Result& b) { return is_better(a, b); });

        // Write CSV
        gendb::init_date_tables();
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(("fopen: " + out_path).c_str()); }
        else {
            fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char date_buf[16];
            for (size_t i = 0; i < limit; i++) {
                const Result& r = merged[i];
                gendb::epoch_days_to_date_str(r.odate, date_buf);
                fprintf(fp, "%d,%.2f,%s,%d\n", r.key, r.rev, date_buf, r.ospri);
            }
            fclose(fp);
        }
    }

    // Cleanup
    delete[] cust_bm;
    if (oa_keys)    { delete[] oa_keys;    }
    if (oa_odate)   { delete[] oa_odate;   }
    if (oa_ospri)   { delete[] oa_ospri;   }
    if (oa_revenue) { delete[] oa_revenue; }
    f_lship_region.unmap();
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
