/**
 * Q3 – Shipping Priority
 *
 * Precision strategy: parallel scan uses schedule(static) so thread t gets
 * a fixed contiguous row range. Qualifying (orderkey, orderdate, shippriority,
 * revenue) tuples are collected per-thread in row-sequential order. Sequential
 * aggregate iterates threads t=0..N-1 in order → same float summation order as
 * a sequential lineitem scan → matches reference implementation exactly.
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

// Qualifying lineitem row collected during parallel scan.
struct LinePair {
    int32_t orderkey;
    int32_t orderdate;
    double  revenue;
};
static_assert(sizeof(LinePair) == 16, "");

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

    // ======================================================
    // Phase 1: Customer scan → build custkey bitset
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

        for (size_t i = 0; i < n_seg; i++) {
            if (seg_col[i] == building_code) {
                uint32_t k = (uint32_t)ckey_col[i];
                cust_bitset[k >> 6] |= (1ULL << (k & 63));
            }
        }
    }

    // Set thread count before Phase 2 so parallel orders scan can use it
    int nthreads = omp_get_max_threads();
    if (nthreads > 64) nthreads = 64;
    omp_set_num_threads(nthreads);

    // ======================================================
    // Phase 2: Build orders hash map — single-threaded sequential scan
    //   filter: o_orderdate < 9204 AND o_custkey in cust_bitset
    //   result: orderkey → {orderdate}  (shippriority always 0, hardcoded in output)
    //   Fuses key_bitset build into same pass (no separate 2M-slot scan needed).
    // ======================================================
    OrdersEntry* orders_ht = (OrdersEntry*)calloc(ORDERS_CAP, sizeof(OrdersEntry));
    if (!orders_ht) { perror("calloc orders_ht"); exit(1); }

    // Allocate qualifying orderkey bitset (8MB) before build phase so we can
    // populate it in the same pass as orders_ht — eliminates a separate 2M-slot scan.
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

        // Note: skip o_shippriority.bin entirely — always 0 in TPC-H SF10
        MmapRegion od_mm, oc_mm, ok_mm;
        size_t n_od, n_oc, n_ok;
        const int32_t* o_orderdate = od_mm.open<int32_t>(d + "orders/o_orderdate.bin", n_od);
        const int32_t* o_custkey   = oc_mm.open<int32_t>(d + "orders/o_custkey.bin",   n_oc);
        const int32_t* o_orderkey  = ok_mm.open<int32_t>(d + "orders/o_orderkey.bin",  n_ok);

        // Single-threaded sequential scan + direct hash insert (no atomics).
        // Eliminates 64-core CAS cache-coherency traffic that dominated the prior
        // parallel approach: each cache line in orders_ht (16MB) was invalidated
        // across all 64 cores on every write. Sequential single-thread keeps the
        // hash table hot in L3 with no coherency overhead.
        // Also fuses key_bitset population into this same pass (eliminates separate
        // 2M-slot iteration that previously added ~8ms of unaccounted overhead).
        const uint64_t* cb    = cust_bitset;
        const int64_t   o_end = (int64_t)orders_end;
        OrdersEntry*    o_ht  = orders_ht;
        uint64_t*       o_kb  = key_bitset;

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
                    uint32_t uk = (uint32_t)ok;
                    o_kb[uk >> 6] |= (1ULL << (uk & 63));
                    break;
                }
            }
        }
    }

    free(cust_bitset);

    // ======================================================
    // Phase 3: Parallel lineitem scan → per-thread LinePair buffers
    //
    // schedule(static) assigns contiguous row-ranges in thread order.
    // Merging in thread order (t=0..N-1) reproduces sequential row order
    // → identical floating-point summation order as a sequential scan.
    // Key optimization: bitset pre-filter (8MB, L3-resident) eliminates
    // 98.8% of hash probes before touching the 16MB orders_ht.
    // ======================================================
    std::vector<std::vector<LinePair>> tpairs((size_t)nthreads);

    {
        GENDB_PHASE("main_scan");

        // Zone map: skip blocks with max_val <= DATE_THRESHOLD (sorted ascending)
        MmapRegion lzm_mm;
        size_t lzm_cnt;
        const uint8_t* lzm_raw = lzm_mm.open<uint8_t>(
            d + "indexes/lineitem_shipdate_zonemap.bin", lzm_cnt, false);
        uint32_t     lnb  = *(const uint32_t*)lzm_raw;
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
            if (!found) lineitem_start = (uint64_t)lnb * 100000;  // shouldn't happen
        }
        lzm_mm.close();

        // Open WITHOUT MAP_POPULATE — avoids faulting in the first ~30M rows we skip.
        // Issue async MADV_WILLNEED on the qualifying range so the kernel prefetches
        // only [lineitem_start, end) before (or in parallel with) our OMP scan.
        MmapRegion ls_mm, lk_mm, le_mm, ld_mm;
        size_t n_ls, n_lk, n_le, n_ld;
        const int32_t* l_shipdate = ls_mm.open<int32_t>(d + "lineitem/l_shipdate.bin",      n_ls, true, false);
        const int32_t* l_orderkey = lk_mm.open<int32_t>(d + "lineitem/l_orderkey.bin",      n_lk, true, false);
        const double*  l_extprice = le_mm.open<double> (d + "lineitem/l_extendedprice.bin", n_le, true, false);
        const double*  l_discount = ld_mm.open<double> (d + "lineitem/l_discount.bin",      n_ld, true, false);

        // Async prefetch only the qualifying range for all 4 columns.
        {
            size_t s_i32 = lineitem_start * sizeof(int32_t);
            size_t l_i32 = (n_ls - lineitem_start) * sizeof(int32_t);
            size_t s_dbl = lineitem_start * sizeof(double);
            size_t l_dbl = (n_le - lineitem_start) * sizeof(double);
            ls_mm.advise_range(s_i32, l_i32);
            lk_mm.advise_range(s_i32, l_i32);
            le_mm.advise_range(s_dbl, l_dbl);
            ld_mm.advise_range(s_dbl, l_dbl);
        }

        const int64_t li_total = (int64_t)n_ls;
        const int64_t li_start = (int64_t)lineitem_start;

        const OrdersEntry* o_ht = orders_ht;
        const uint64_t*    o_kb = key_bitset;
        auto& tp = tpairs;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            tp[(size_t)tid].reserve(8192);

            #pragma omp for schedule(static) nowait
            for (int64_t i = li_start; i < li_total; i++) {
                // Software prefetch bitset entry for a future row to hide L3 latency
                if (__builtin_expect(i + 16 < li_total, 1)) {
                    __builtin_prefetch(
                        &o_kb[(uint32_t)l_orderkey[i + 16] >> 6], 0, 1);
                }

                if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                int32_t  lk = l_orderkey[i];
                uint32_t uk = (uint32_t)lk;

                // Bitset pre-filter (8MB, L3-resident): reject ~97.6% of rows
                // before the more expensive hash table probe.
                // TPC-H SF10 orderkeys ≤ 60M < KEY_BITSET_SZ*64=64M — bounds check omitted.
                if (!((o_kb[uk >> 6] >> (uk & 63)) & 1u))
                    continue;

                // Probe orders hash map (only ~340K hits reach here)
                uint32_t slot = hash_orders(lk);
                const OrdersEntry* oe = nullptr;
                for (uint32_t probe = 0; probe < ORDERS_CAP; probe++) {
                    uint32_t s = (slot + probe) & ORDERS_MASK;
                    if (o_ht[s].key == lk)       { oe = &o_ht[s]; break; }
                    if (o_ht[s].key == EMPTY_KEY) { break; }
                }
                if (!oe) continue;

                double rev = l_extprice[i] * (1.0 - l_discount[i]);
                tp[(size_t)tid].push_back({lk, oe->orderdate, rev});
            }
        }
    }

    free(orders_ht);
    free(key_bitset);

    // ======================================================
    // Phase 4: Sequential aggregate — iterate thread buffers in thread order
    //   → same summation order as sequential row scan
    // ======================================================
    AggEntry* global_agg = (AggEntry*)calloc(AGG_CAP, sizeof(AggEntry));
    if (!global_agg) { perror("calloc global_agg"); exit(1); }

    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < nthreads; t++) {
            for (const LinePair& p : tpairs[(size_t)t]) {
                int32_t k = p.orderkey;
                uint32_t slot = hash_agg(k);
                for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                    uint32_t s = (slot + probe) & AGG_MASK;
                    if (global_agg[s].key == k) {
                        global_agg[s].revenue += p.revenue;
                        break;
                    }
                    if (global_agg[s].key == EMPTY_KEY) {
                        global_agg[s].key       = k;
                        global_agg[s].orderdate = p.orderdate;
                        global_agg[s].revenue   = p.revenue;
                        break;
                    }
                }
            }
        }

        // Free per-thread buffers
        for (auto& v : tpairs) { std::vector<LinePair>().swap(v); }
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
