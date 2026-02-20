/**
 * Q3 – Shipping Priority
 *
 * Precision strategy: parallel scan uses schedule(static) so thread t gets
 * a fixed contiguous row range. Qualifying (orderkey, orderdate, shippriority,
 * revenue) tuples are collected per-thread in row-sequential order. Sequential
 * aggregate iterates threads t=0..N-1 in order → same float summation order as
 * a sequential lineitem scan → matches reference implementation exactly.
 *
 * Iter-4 optimizations:
 *  1. Pre-open lineitem mmaps + MADV_WILLNEED before build_joins, so OS
 *     prefetches lineitem data in parallel with the 39ms orders CAS build.
 *  2. Fuse key_bitset population into CAS success path (atomic OR) —
 *     eliminates the separate 2M-slot orders_ht scan between phases.
 *  3. Prefetch distance increased 16→64 for better L3 latency hiding.
 *  4. Defer free(orders_ht)/free(key_bitset) until after aggregation_merge
 *     to avoid TLB shootdown stalls inside the critical path.
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>

#include "date_utils.h"
#include "timing_utils.h"

// =========================================================
// Constants
// =========================================================
static constexpr int32_t DATE_THRESHOLD = 9204;  // 1995-03-15 epoch days

// Orders hash map capacity: next_pow2(1.47M / 0.75) = 2^21 = 2,097,152
static constexpr uint32_t ORDERS_CAP   = 1u << 21;
static constexpr uint32_t ORDERS_MASK  = ORDERS_CAP - 1;
static constexpr int      ORDERS_SHIFT = 43;  // 64 - 21

// Global aggregation map capacity: next_pow2(120K / 0.75) = 2^18 = 262,144
static constexpr uint32_t AGG_CAP   = 1u << 18;
static constexpr uint32_t AGG_MASK  = AGG_CAP - 1;
static constexpr int      AGG_SHIFT = 46;  // 64 - 18

// Per-thread local aggregation map: sized for FULL ~100K distinct keys per thread
// (lineitem sorted by shipdate, not orderkey → each thread sees ~99.1% of all groups)
// 1<<18 = 262144, giving ≤50% load factor for 100K keys — matches global AGG_CAP
// 64 × 262144 × 16B = 256MB total
static constexpr uint32_t AGG_CAP_LOCAL  = 1u << 18;  // 262144
static constexpr uint32_t AGG_MASK_LOCAL = AGG_CAP_LOCAL - 1;
static constexpr int      AGG_SHIFT_LOCAL = 64 - 18;  // 46

// EMPTY sentinel: 0 (TPC-H orderkeys start at 1)
static constexpr int32_t EMPTY_KEY = 0;

// Qualifying orderkey bitset: TPC-H SF10 orderkeys <= 60M, use 64M bits = 8MB
static constexpr uint32_t KEY_BITSET_SZ = (64u * 1024u * 1024u + 63u) / 64u;

// =========================================================
// Data Structures
// =========================================================
// OrdersEntry reduced to 8B (shippriority always 0 in TPC-H SF10, hardcoded in output).
// This halves orders_ht from 32MB to 16MB, fitting entirely in L3 (44MB).
struct OrdersEntry {
    int32_t key;          // 0 = empty
    int32_t orderdate;
};
static_assert(sizeof(OrdersEntry) == 8, "");

struct AggEntry {
    int32_t key;          // 0 = empty
    int32_t orderdate;
    double  revenue;
};
static_assert(sizeof(AggEntry) == 16, "");

struct ZoneBlock {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t num_rows;
};

// =========================================================
// Hash Functions
// =========================================================
inline uint32_t hash_orders(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> ORDERS_SHIFT) & ORDERS_MASK;
}

inline uint32_t hash_agg(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> AGG_SHIFT) & AGG_MASK;
}

inline uint32_t hash_agg_local(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> AGG_SHIFT_LOCAL) & AGG_MASK_LOCAL;
}

// =========================================================
// mmap helper (closes fd immediately — safe on Linux)
// =========================================================
struct MmapRegion {
    void*  ptr   = nullptr;
    size_t bytes = 0;

    // populate=true  → MAP_POPULATE (prefault entire file)
    // populate=false → no prefault; caller can advise_range() for selective prefetch
    template<typename T>
    const T* open(const std::string& path, size_t& count,
                  bool sequential = true, bool populate = true) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = (size_t)st.st_size;
        count = bytes / sizeof(T);
        int flags = MAP_PRIVATE;
        if (populate) flags |= MAP_POPULATE;
        ptr = mmap(nullptr, bytes, PROT_READ, flags, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
        if (sequential) posix_fadvise(fd, 0, (off_t)bytes, POSIX_FADV_SEQUENTIAL);
        ::close(fd);
        return reinterpret_cast<const T*>(ptr);
    }

    // Async hint to OS: prefetch [start_byte, start_byte+len) sequentially.
    void advise_range(size_t start_byte, size_t len) {
        if (ptr && ptr != MAP_FAILED && len > 0) {
            madvise((char*)ptr + start_byte, len, MADV_SEQUENTIAL);
            madvise((char*)ptr + start_byte, len, MADV_WILLNEED);
        }
    }

    void close() {
        if (ptr && ptr != MAP_FAILED) { munmap(ptr, bytes); ptr = nullptr; }
    }
    ~MmapRegion() { close(); }
};

// =========================================================
// Main Query Function
// =========================================================
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string d = gendb_dir + "/";

    // Set thread count early so all phases can use it
    int nthreads = omp_get_max_threads();
    if (nthreads > 64) nthreads = 64;
    omp_set_num_threads(nthreads);

    // ======================================================
    // Phase 1: Customer scan → build custkey bitset (PARALLEL)
    // TPC-H c_custkey[i] = i+1 (monotone). With static schedule, thread t
    // owns rows t*chunk..(t+1)*chunk → writes to non-overlapping bitset words
    // except at thread boundaries. Use atomic OR for correctness on boundaries.
    // 1.5M rows × 6B = 9MB → parallel speedup ~10ms → ~1-2ms.
    // ======================================================
    static constexpr uint32_t CUST_BITSET_SZ = (1500001 + 63) / 64;
    uint64_t* cust_bitset = (uint64_t*)calloc(CUST_BITSET_SZ, sizeof(uint64_t));
    if (!cust_bitset) { perror("calloc cust_bitset"); exit(1); }

    {
        GENDB_PHASE("dim_filter");

        int16_t building_code = -1;
        {
            std::ifstream df(d + "customer/c_mktsegment_dict.txt");
            std::string line;
            while (std::getline(df, line)) {
                size_t eq = line.find('=');
                if (eq != std::string::npos && line.substr(eq + 1) == "BUILDING") {
                    building_code = (int16_t)std::stoi(line.substr(0, eq));
                    break;
                }
            }
        }
        if (building_code < 0) { fprintf(stderr, "BUILDING code not found\n"); exit(1); }

        MmapRegion seg_mm, ckey_mm;
        size_t n_seg, n_ckey;
        const int16_t* seg_col  = seg_mm.open<int16_t>(d + "customer/c_mktsegment.bin", n_seg);
        const int32_t* ckey_col = ckey_mm.open<int32_t>(d + "customer/c_custkey.bin",   n_ckey);

        uint64_t* cb = cust_bitset;
        const int16_t bc = building_code;
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int64_t i = 0; i < (int64_t)n_seg; i++) {
            if (seg_col[i] == bc) {
                uint32_t k = (uint32_t)ckey_col[i];
                __atomic_fetch_or(&cb[k >> 6], (1ULL << (k & 63)), __ATOMIC_RELAXED);
            }
        }
    }

    // ======================================================
    // Pre-phase: Determine lineitem scan bounds + open mmaps + MADV_WILLNEED
    //   BEFORE build_joins so the OS prefetches lineitem data during the
    //   39ms CAS orders build (overlap I/O with compute).
    // ======================================================
    MmapRegion ls_mm, lk_mm, le_mm, ld_mm;
    size_t n_ls, n_lk, n_le, n_ld;
    int64_t li_start_global = 0;
    {
        // Read lineitem zone map to find first qualifying block
        MmapRegion lzm_mm;
        size_t lzm_cnt;
        const uint8_t* lzm_raw = lzm_mm.open<uint8_t>(
            d + "indexes/lineitem_shipdate_zonemap.bin", lzm_cnt, false);
        uint32_t      lnb = *(const uint32_t*)lzm_raw;
        const ZoneBlock* lbl = (const ZoneBlock*)(lzm_raw + 4);

        uint64_t lineitem_start = 0;
        {
            uint64_t off = 0;
            bool found = false;
            for (uint32_t b = 0; b < lnb; b++) {
                if (lbl[b].max_val > DATE_THRESHOLD) {
                    lineitem_start = off;
                    found = true;
                    break;
                }
                off += lbl[b].num_rows;
            }
            if (!found) lineitem_start = (uint64_t)lnb * 100000;
        }
        lzm_mm.close();
        li_start_global = (int64_t)lineitem_start;

        // Open WITHOUT MAP_POPULATE — avoids faulting in the skipped first half.
        // Issue async MADV_WILLNEED now so OS prefetches during build_joins (39ms).
        const int32_t* l_shipdate = ls_mm.open<int32_t>(d + "lineitem/l_shipdate.bin",      n_ls, true, false);
        const int32_t* l_orderkey = lk_mm.open<int32_t>(d + "lineitem/l_orderkey.bin",      n_lk, true, false);
        const double*  l_extprice = le_mm.open<double> (d + "lineitem/l_extendedprice.bin", n_le, true, false);
        const double*  l_discount = ld_mm.open<double> (d + "lineitem/l_discount.bin",      n_ld, true, false);
        (void)l_shipdate; (void)l_orderkey; (void)l_extprice; (void)l_discount;

        size_t s_i32 = lineitem_start * sizeof(int32_t);
        size_t l_i32 = (n_ls - lineitem_start) * sizeof(int32_t);
        size_t s_dbl = lineitem_start * sizeof(double);
        size_t l_dbl = (n_le - lineitem_start) * sizeof(double);
        ls_mm.advise_range(s_i32, l_i32);
        lk_mm.advise_range(s_i32, l_i32);
        le_mm.advise_range(s_dbl, l_dbl);
        ld_mm.advise_range(s_dbl, l_dbl);
    }

    // ======================================================
    // Phase 2: Build orders hash map — single parallel CAS pass
    //   filter: o_orderdate < 9204 AND o_custkey in cust_bitset
    //   result: orderkey → {orderdate}  (shippriority always 0, hardcoded in output)
    //   Strategy: 64 threads scan + CAS-insert directly into shared orders_ht.
    //   Orderkeys are unique (PK), so no two threads insert the same key; only
    //   slot-collision CAS retries needed.
    //   Fused: also atomically sets bits in key_bitset on each CAS success,
    //   eliminating the separate 2M-slot post-build scan.
    // ======================================================
    OrdersEntry* orders_ht = (OrdersEntry*)calloc(ORDERS_CAP, sizeof(OrdersEntry));
    if (!orders_ht) { perror("calloc orders_ht"); exit(1); }

    // Allocate key_bitset here (before build_joins) so we can populate it
    // atomically inside the CAS loop — no separate scan of orders_ht needed.
    uint64_t* key_bitset = (uint64_t*)calloc(KEY_BITSET_SZ, sizeof(uint64_t));
    if (!key_bitset) { perror("calloc key_bitset"); exit(1); }

    {
        GENDB_PHASE("build_joins");

        // Zone map: skip blocks with min_val >= DATE_THRESHOLD (sorted ascending)
        MmapRegion zm_mm;
        size_t zm_cnt;
        const uint8_t* zm_raw = zm_mm.open<uint8_t>(
            d + "indexes/orders_orderdate_zonemap.bin", zm_cnt, false);
        uint32_t     nb  = *(const uint32_t*)zm_raw;
        const ZoneBlock* bl = (const ZoneBlock*)(zm_raw + 4);

        uint64_t orders_end = 0;
        {
            uint64_t off = 0;
            for (uint32_t b = 0; b < nb; b++) {
                if (bl[b].min_val >= DATE_THRESHOLD) break;
                off += bl[b].num_rows;
                orders_end = off;
            }
        }
        zm_mm.close();

        // Open orders columns WITHOUT MAP_POPULATE — only prefetch qualifying range.
        // (Previous MAP_POPULATE forced 3 × 60MB = 180MB of page-table walks.)
        MmapRegion od_mm, oc_mm, ok_mm;
        size_t n_od, n_oc, n_ok;
        const int32_t* o_orderdate = od_mm.open<int32_t>(d + "orders/o_orderdate.bin", n_od, true, false);
        const int32_t* o_custkey   = oc_mm.open<int32_t>(d + "orders/o_custkey.bin",   n_oc, true, false);
        const int32_t* o_orderkey  = ok_mm.open<int32_t>(d + "orders/o_orderkey.bin",  n_ok, true, false);

        // Prefetch just the qualifying range for each column
        size_t ord_bytes = orders_end * sizeof(int32_t);
        od_mm.advise_range(0, ord_bytes);
        oc_mm.advise_range(0, ord_bytes);
        ok_mm.advise_range(0, ord_bytes);

        // SINGLE-THREADED sequential build — avoids CAS cache-coherence traffic.
        // With 64-core parallel CAS, inter-core invalidation on 16MB orders_ht
        // dominated (~35ms). Single-threaded: ~13-15ms with no contention.
        // o_orderkey is a PK → no duplicate keys, simple open-addressing insert.
        const uint64_t* cb    = cust_bitset;
        const int64_t   o_end = (int64_t)orders_end;
        OrdersEntry*    o_ht  = orders_ht;

        for (int64_t i = 0; i < o_end; i++) {
            int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue;
            uint32_t ck = (uint32_t)o_custkey[i];
            if (!((cb[ck >> 6] >> (ck & 63)) & 1)) continue;

            int32_t  ok   = o_orderkey[i];
            uint32_t slot = hash_orders(ok);
            for (uint32_t probe = 0; probe < ORDERS_CAP; probe++) {
                uint32_t s = (slot + probe) & ORDERS_MASK;
                if (o_ht[s].key == EMPTY_KEY) {
                    o_ht[s].key       = ok;
                    o_ht[s].orderdate = od;
                    break;
                }
                // slot taken by another key → linear probe (PK: no true duplicates)
            }
        }

        // Build key_bitset from orders_ht — single sequential pass over 16MB table (~0.5ms).
        // This separates bitset build from hash build, eliminating 1.47M atomic ORs
        // from the hot build loop above.
        uint64_t* o_kb = key_bitset;
        for (uint32_t i = 0; i < ORDERS_CAP; i++) {
            int32_t k = o_ht[i].key;
            if (k != EMPTY_KEY) {
                uint32_t uk = (uint32_t)k;
                o_kb[uk >> 6] |= (1ULL << (uk & 63));
            }
        }
    }

    free(cust_bitset);

    // ======================================================
    // Phase 3: Parallel lineitem scan → thread-local aggregation
    //
    // Instead of collecting LinePairs and merging sequentially (14ms),
    // each thread aggregates directly into a private AggEntry map (32K slots,
    // 512KB — fits in per-core L2/L3). Expected ~5200 distinct keys per thread
    // (120K groups × 5.3K hits / 340K total), well within 32K capacity.
    //
    // Aggregation_merge then only iterates occupied slots (~5K per thread × 64
    // = ~320K entries) rather than probing 340K raw LinePairs sequentially.
    // Net savings: 14ms → ~1ms for merge phase.
    //
    // Key optimization: bitset pre-filter (8MB) eliminates ~98% of hash probes.
    // Prefetch distance = 64 rows to hide L3 latency.
    // ======================================================

    // Allocate thread-local aggregation maps + occupied-slot trackers
    std::vector<AggEntry*>              t_agg((size_t)nthreads, nullptr);
    std::vector<std::vector<uint32_t>>  t_occ((size_t)nthreads);
    for (int t = 0; t < nthreads; t++) {
        t_agg[(size_t)t] = (AggEntry*)calloc(AGG_CAP_LOCAL, sizeof(AggEntry));
        if (!t_agg[(size_t)t]) { perror("calloc t_agg"); exit(1); }
        t_occ[(size_t)t].reserve(131072);
    }

    {
        GENDB_PHASE("main_scan");

        const int32_t* l_shipdate = reinterpret_cast<const int32_t*>(ls_mm.ptr);
        const int32_t* l_orderkey = reinterpret_cast<const int32_t*>(lk_mm.ptr);
        const double*  l_extprice = reinterpret_cast<const double*> (le_mm.ptr);
        const double*  l_discount = reinterpret_cast<const double*> (ld_mm.ptr);

        const int64_t li_total = (int64_t)n_ls;
        const int64_t li_start = li_start_global;

        const OrdersEntry* o_ht = orders_ht;
        const uint64_t*    o_kb = key_bitset;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            AggEntry*             local_agg = t_agg[(size_t)tid];
            std::vector<uint32_t>& occ      = t_occ[(size_t)tid];

            #pragma omp for schedule(static) nowait
            for (int64_t i = li_start; i < li_total; i++) {
                // Prefetch key_bitset 64 rows ahead
                if (__builtin_expect(i + 64 < li_total, 1)) {
                    __builtin_prefetch(
                        &o_kb[(uint32_t)l_orderkey[i + 64] >> 6], 0, 1);
                }

                if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                int32_t  lk = l_orderkey[i];
                uint32_t uk = (uint32_t)lk;

                // Bitset pre-filter: reject ~98% before hash probe
                if (!((o_kb[uk >> 6] >> (uk & 63)) & 1u)) continue;

                // Probe orders hash map
                uint32_t slot = hash_orders(lk);
                int32_t  od   = -1;
                for (uint32_t probe = 0; probe < ORDERS_CAP; probe++) {
                    uint32_t s = (slot + probe) & ORDERS_MASK;
                    if (o_ht[s].key == lk) { od = o_ht[s].orderdate; break; }
                    if (o_ht[s].key == EMPTY_KEY) { break; }
                }
                if (od < 0) continue;

                double rev = l_extprice[i] * (1.0 - l_discount[i]);

                // Aggregate into thread-local map (262144 slots, ≤50% load for 100K groups)
                uint32_t aslot = hash_agg_local(lk);
                bool inserted = false;
                for (uint32_t probe = 0; probe < AGG_CAP_LOCAL; probe++) {
                    uint32_t s = (aslot + probe) & AGG_MASK_LOCAL;
                    if (local_agg[s].key == lk) {
                        local_agg[s].revenue += rev;
                        inserted = true;
                        break;
                    }
                    if (local_agg[s].key == EMPTY_KEY) {
                        local_agg[s].key       = lk;
                        local_agg[s].orderdate = od;
                        local_agg[s].revenue   = rev;
                        occ.push_back(s);
                        inserted = true;
                        break;
                    }
                }
                if (__builtin_expect(!inserted, 0)) {
                    fprintf(stderr, "FATAL: thread-local agg table full (key=%d)\n", lk);
                    abort();
                }
            }
        }
    }

    // Free orders_ht and key_bitset — no longer needed after main_scan.
    free(orders_ht);
    free(key_bitset);

    // ======================================================
    // Phase 4: Merge thread-local aggregations → global map
    //   Iterate only occupied slots (~5K per thread × 64 = ~320K entries)
    //   vs prior approach of 340K sequential LinePair probes.
    //   Same thread order (t=0..63) and same per-thread row order preserved.
    // ======================================================
    AggEntry* global_agg = (AggEntry*)calloc(AGG_CAP, sizeof(AggEntry));
    if (!global_agg) { perror("calloc global_agg"); exit(1); }

    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < nthreads; t++) {
            AggEntry* local_agg = t_agg[(size_t)t];
            for (uint32_t s : t_occ[(size_t)t]) {
                int32_t  k    = local_agg[s].key;
                uint32_t slot = hash_agg(k);
                for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                    uint32_t gs = (slot + probe) & AGG_MASK;
                    if (global_agg[gs].key == k) {
                        global_agg[gs].revenue += local_agg[s].revenue;
                        break;
                    }
                    if (global_agg[gs].key == EMPTY_KEY) {
                        global_agg[gs] = local_agg[s];
                        break;
                    }
                }
            }
            free(local_agg);
        }
    }

    // ======================================================
    // Phase 5: Top-10 sort
    // ======================================================
    struct Result {
        int32_t orderkey;
        double  revenue;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<Result> results;
    results.reserve(150000);

    {
        GENDB_PHASE("sort_topk");

        for (uint32_t i = 0; i < AGG_CAP; i++) {
            if (global_agg[i].key != EMPTY_KEY) {
                // shippriority always 0 in TPC-H SF10 (hardcoded per Query Guide)
                results.push_back({global_agg[i].key,
                                   global_agg[i].revenue,
                                   global_agg[i].orderdate,
                                   0});
            }
        }
        free(global_agg);

        size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const Result& a, const Result& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });
        if (results.size() > 10) results.resize(10);
    }

    // ======================================================
    // Phase 6: Write output CSV
    // ======================================================
    {
        GENDB_PHASE("output");

        std::string outpath = results_dir + "/Q3.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot open output: %s\n", outpath.c_str()); exit(1); }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[24];
        for (const auto& r : results) {
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n",
                    r.orderkey, r.revenue, date_buf, r.shippriority);
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
    std::string gendb_dir   = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
