// Q3: Shipping Priority
// Filters: c_mktsegment='BUILDING', o_orderdate<9204, l_shipdate>9204
// Joins: customer ⋈ orders ⋈ lineitem
// Agg: SUM(l_extendedprice*(1-l_discount)) GROUP BY l_orderkey, o_orderdate, o_shippriority
// Output: top-10 by revenue DESC, o_orderdate ASC

#include <cstdint>
#include <cstdlib>
#include <climits>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <queue>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ── Utilities ──────────────────────────────────────────────────────────────────

static inline uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

static inline uint32_t hash32(int32_t key) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32);
}

// mmap_col: eager (MAP_POPULATE) for small files scanned fully
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_elems) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    n_elems = sz / sizeof(T);
    if (sz == 0) { close(fd); return nullptr; }
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(ptr);
}

// mmap_col_lazy: no MAP_POPULATE — use selective madvise for zone-map filtered columns
template<typename T>
static const T* mmap_col_lazy(const std::string& path, size_t& n_elems) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    n_elems = sz / sizeof(T);
    if (sz == 0) { close(fd); return nullptr; }
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(ptr);
}

// ── Customer custkey direct-address bitmap ─────────────────────────────────────
// TPC-H c_custkey is 1..n_customer (dense sequential), so a flat bool array
// gives O(1) insert/lookup with zero hash overhead. Fits in L2 cache (1.5MB).

// ── ConcurrentOrderMap: combines OrderMap + aggregation in one structure ────────
// Keys are atomic for lock-free parallel insert during build_joins.
// Revenue array is atomic for concurrent accumulation during main_scan.
// One hash lookup per lineitem row instead of two (eliminates SharedAggMap).
// Total size: 2M slots × (4+4+4+8) = 40MB — fits in 44MB L3 cache.

struct ConcurrentOrderMap {
    std::atomic<int32_t>*  keys;      // C20: sentinel INT32_MIN
    int32_t*               odates;    // written once on key claim (build_joins)
    int32_t*               shippriors;
    std::atomic<uint64_t>* revs;      // atomic double revenue (main_scan)
    uint32_t cap, mask;

    explicit ConcurrentOrderMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        keys       = new std::atomic<int32_t>[cap];
        odates     = new int32_t[cap]();
        shippriors = new int32_t[cap]();
        revs       = new std::atomic<uint64_t>[cap];
        // C20: init via store loop, never memset
        for (uint32_t i = 0; i < cap; i++) {
            keys[i].store(INT32_MIN, std::memory_order_relaxed);
            revs[i].store(0ULL, std::memory_order_relaxed);
        }
    }

    ~ConcurrentOrderMap() {
        delete[] keys; delete[] odates; delete[] shippriors; delete[] revs;
    }

    // CAS-based concurrent insert (build_joins phase).
    // o_orderkey is unique in orders table → no logical key conflicts.
    void insert(int32_t key, int32_t odate, int32_t shippr) {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            int32_t cur = keys[s].load(std::memory_order_acquire);
            if (cur == key) return;  // already inserted by another thread
            if (cur == INT32_MIN) {
                int32_t expected = INT32_MIN;
                if (keys[s].compare_exchange_strong(expected, key,
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    odates[s]     = odate;
                    shippriors[s] = shippr;
                    return;
                }
                if (expected == key) return;
            }
        }
    }

    // Like insert(), but returns the claimed slot index (so caller can track it),
    // or UINT32_MAX if the key already existed (inserted by another thread).
    uint32_t insert_get_slot(int32_t key, int32_t odate, int32_t shippr) {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            int32_t cur = keys[s].load(std::memory_order_acquire);
            if (cur == key) return UINT32_MAX;  // already present
            if (cur == INT32_MIN) {
                int32_t expected = INT32_MIN;
                if (keys[s].compare_exchange_strong(expected, key,
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    odates[s]     = odate;
                    shippriors[s] = shippr;
                    return s;  // winner: return slot for tracking
                }
                if (expected == key) return UINT32_MAX;
            }
        }
        return UINT32_MAX;
    }

    // Probe-only lookup — returns slot index or UINT32_MAX (main_scan phase).
    // Called after build_joins is complete → keys are stable, relaxed loads OK.
    uint32_t findSlot(int32_t key) const {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            int32_t cur = keys[s].load(std::memory_order_relaxed);
            if (cur == INT32_MIN) return UINT32_MAX;
            if (cur == key) return s;
        }
        return UINT32_MAX;
    }

    // Atomic double-precision add via CAS loop. memcpy avoids aliasing UB.
    static void atomic_add_double(std::atomic<uint64_t>* ptr, double delta) {
        uint64_t old_bits = ptr->load(std::memory_order_relaxed);
        for (;;) {
            double old_val; memcpy(&old_val, &old_bits, 8);
            double new_val = old_val + delta;
            uint64_t new_bits; memcpy(&new_bits, &new_val, 8);
            if (ptr->compare_exchange_weak(old_bits, new_bits,
                    std::memory_order_relaxed, std::memory_order_relaxed))
                break;
        }
    }
};

// ── Top-K entry ────────────────────────────────────────────────────────────────

struct TopKEntry {
    int32_t l_orderkey;
    double  revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Heap comparator: returns true if a is BETTER quality than b.
// "Better" = higher revenue; on tie, smaller orderdate (ASC).
// With std::priority_queue this puts the WORST entry at top() for easy eviction.
struct HeapCmp {
    bool operator()(const TopKEntry& a, const TopKEntry& b) const {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    }
};

// ── Main query ─────────────────────────────────────────────────────────────────

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // C11

    const int32_t DATE_THRESHOLD = 9204;  // 1995-03-15 epoch days
    const uint32_t BLOCK_SIZE    = 100000;

    // ── Data loading ─────────────────────────────────────────────────────────
    // Strategy: load zone maps first → compute qualifying block ranges →
    // lazy mmap large columns (no MAP_POPULATE) → selective MADV_WILLNEED
    // on qualifying byte ranges only. Saves ~45% lineitem + ~51% orders PTE faults.

    size_t n_customer = 0, n_orders = 0, n_lineitem = 0, tmp = 0;

    const uint8_t*  c_mktsegment  = nullptr;
    const int32_t*  c_custkey     = nullptr;
    const int32_t*  o_custkey     = nullptr;
    const int32_t*  o_orderdate   = nullptr;
    const int32_t*  o_orderkey    = nullptr;
    const int32_t*  o_shippriority= nullptr;
    const int32_t*  l_orderkey    = nullptr;
    const int32_t*  l_shipdate    = nullptr;
    const double*   l_extprice    = nullptr;
    const double*   l_discount    = nullptr;

    uint32_t orders_zm_nblocks = 0, li_zm_nblocks = 0;
    const int32_t* orders_zm = nullptr;
    const int32_t* li_zm     = nullptr;
    uint32_t end_block   = 0;  // orders: blocks [0, end_block) qualify
    uint32_t first_block = 0;  // lineitem: blocks [first_block, li_zm_nblocks) qualify

    {
        GENDB_PHASE("data_loading");

        // Step 1: Load zone maps first (tiny, MAP_POPULATE ok)
        {
            int fd = open((gendb_dir + "/indexes/orders_orderdate_zonemap.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            auto* raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
            close(fd);
            orders_zm_nblocks = raw[0];
            orders_zm = reinterpret_cast<const int32_t*>(raw + 1);
        }
        {
            int fd = open((gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            auto* raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
            close(fd);
            li_zm_nblocks = raw[0];
            li_zm = reinterpret_cast<const int32_t*>(raw + 1);
        }

        // Step 2: Compute qualifying ranges from zone maps
        // Orders sorted by o_orderdate → find last qualifying block
        for (uint32_t b = 0; b < orders_zm_nblocks; b++) {
            if (orders_zm[b * 2] >= DATE_THRESHOLD) break;
            end_block = b + 1;
        }
        // Lineitem sorted by l_shipdate → find first qualifying block
        first_block = li_zm_nblocks;
        for (uint32_t b = 0; b < li_zm_nblocks; b++) {
            if (li_zm[b * 2 + 1] > DATE_THRESHOLD) { first_block = b; break; }
        }

        // Step 3: Customer columns — small (9MB total), MAP_POPULATE all rows
        c_mktsegment = mmap_col<uint8_t>(gendb_dir + "/customer/c_mktsegment.bin", n_customer);
        c_custkey    = mmap_col<int32_t>(gendb_dir + "/customer/c_custkey.bin",    tmp);

        // Step 4: Orders + lineitem — lazy mmap (no MAP_POPULATE)
        o_custkey      = mmap_col_lazy<int32_t>(gendb_dir + "/orders/o_custkey.bin",          n_orders);
        o_orderdate    = mmap_col_lazy<int32_t>(gendb_dir + "/orders/o_orderdate.bin",        tmp);
        o_orderkey     = mmap_col_lazy<int32_t>(gendb_dir + "/orders/o_orderkey.bin",         tmp);
        o_shippriority = mmap_col_lazy<int32_t>(gendb_dir + "/orders/o_shippriority.bin",     tmp);
        l_orderkey     = mmap_col_lazy<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",       n_lineitem);
        l_shipdate     = mmap_col_lazy<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",       tmp);
        l_extprice     = mmap_col_lazy<double>  (gendb_dir + "/lineitem/l_extendedprice.bin", tmp);
        l_discount     = mmap_col_lazy<double>  (gendb_dir + "/lineitem/l_discount.bin",      tmp);

        // Step 5: Selective MADV_WILLNEED — qualifying ranges only
        // Orders: blocks [0, end_block) → contiguous from byte 0
        {
            size_t ord_rows = std::min((size_t)end_block * BLOCK_SIZE, n_orders);
            size_t ord_bytes_i32 = ord_rows * sizeof(int32_t);
            madvise((void*)o_custkey,      ord_bytes_i32, MADV_WILLNEED);
            madvise((void*)o_orderdate,    ord_bytes_i32, MADV_WILLNEED);
            madvise((void*)o_orderkey,     ord_bytes_i32, MADV_WILLNEED);
            madvise((void*)o_shippriority, ord_bytes_i32, MADV_WILLNEED);
        }
        // Lineitem: blocks [first_block, end) → contiguous suffix
        {
            size_t li_start = (size_t)first_block * BLOCK_SIZE;
            size_t li_valid = n_lineitem - li_start;
            madvise((void*)(l_orderkey + li_start), li_valid * sizeof(int32_t), MADV_WILLNEED);
            madvise((void*)(l_shipdate + li_start), li_valid * sizeof(int32_t), MADV_WILLNEED);
            madvise((void*)(l_extprice + li_start), li_valid * sizeof(double),  MADV_WILLNEED);
            madvise((void*)(l_discount + li_start), li_valid * sizeof(double),  MADV_WILLNEED);
        }
    }

    // ── Dim filter: scan customer, build custkey bitmap ────────────────────────
    // Direct-address bitmap: custkey_bitmap[k] = 1 iff customer k qualifies.
    // TPC-H custkeys are 1..n_customer → bitmap size = n_customer+1 = 1.5MB.
    // Fits entirely in L2 cache; O(1) insert and lookup with zero hash cost.
    // Parallel direct writes: each custkey is unique (PK) → no race conditions.
    std::vector<int8_t> custkey_bitmap(n_customer + 1, 0);

    {
        GENDB_PHASE("dim_filter");

        // C2: load dict, find BUILDING code at runtime
        uint8_t building_code = 255;
        {
            std::ifstream f(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            uint8_t code = 0;
            while (std::getline(f, line)) {
                if (line == "BUILDING") { building_code = code; break; }
                code++;
            }
        }

        // Parallel direct bitmap write: no thread-local vectors, no serial merge.
        // c_custkey is a primary key → each index written by at most one thread.
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_customer; i++) {
            if (c_mktsegment[i] == building_code) {
                custkey_bitmap[c_custkey[i]] = 1;
            }
        }
    }

    // ── Build joins: parallel CAS insert into ConcurrentOrderMap ─────────────
    // C9: next_pow2(735000 * 2) = 2097152
    // Row-level static scheduling: 7.35M rows / 64 threads = ~115K per thread.
    // Custkey probe uses direct bitmap (O(1), 1.5MB in L2) instead of hash set.
    // Output uses linear sweep of order_map → no tl_slots tracking needed.
    const uint32_t ORDER_CAP = next_pow2(735000u * 2u);  // 2097152
    ConcurrentOrderMap order_map(ORDER_CAP);

    {
        GENDB_PHASE("build_joins");

        // end_rows: all orders in qualifying blocks (sorted by o_orderdate → prefix)
        const uint32_t end_rows = std::min((uint32_t)n_orders, end_block * BLOCK_SIZE);

        #pragma omp parallel for schedule(static)
        for (uint32_t r = 0; r < end_rows; r++) {
            if (o_orderdate[r] >= DATE_THRESHOLD) continue;  // boundary rows
            if (!custkey_bitmap[o_custkey[r]]) continue;
            order_map.insert(o_orderkey[r], o_orderdate[r], o_shippriority[r]);
        }
    }

    // ── Main scan: lineitem → findSlot → atomic revenue add ──────────────────
    // Row-level static scheduling: 33M rows / 64 threads = ~515K per thread.
    // Explicit parallel block lets us compute per-thread [r_start, r_end) for
    // bounded software prefetch of hash table slots (hides ~10-15ns L3 latency).
    {
        GENDB_PHASE("main_scan");

        const size_t li_start = (size_t)first_block * BLOCK_SIZE;
        const size_t li_total = n_lineitem - li_start;
        constexpr int PFDIST  = 32;  // prefetch distance in rows

        #pragma omp parallel
        {
            int tid      = omp_get_thread_num();
            int nthreads = omp_get_num_threads();

            // Compute this thread's row range
            size_t chunk   = (li_total + (size_t)nthreads - 1) / (size_t)nthreads;
            size_t r_start = li_start + (size_t)tid * chunk;
            size_t r_end   = std::min(n_lineitem, r_start + chunk);

            for (size_t r = r_start; r < r_end; r++) {
                // Software prefetch: hide L3 latency for both keys and revs probes
                if (r + PFDIST < r_end) {
                    uint32_t ph = hash32(l_orderkey[r + PFDIST]) & order_map.mask;
                    __builtin_prefetch(order_map.keys + ph, 0, 1);
                    __builtin_prefetch(order_map.revs + ph, 1, 1);
                }
                if (l_shipdate[r] <= DATE_THRESHOLD) continue;
                uint32_t slot = order_map.findSlot(l_orderkey[r]);
                if (slot == UINT32_MAX) continue;
                // revenue_formula: l_extendedprice*(1-l_discount) — DO NOT MODIFY
                double rev = l_extprice[r] * (1.0 - l_discount[r]);
                ConcurrentOrderMap::atomic_add_double(&order_map.revs[slot], rev);
            }
        }
    }

    // ── Output: top-10 by revenue DESC, o_orderdate ASC ──────────────────────
    {
        GENDB_PHASE("output");

        std::priority_queue<TopKEntry, std::vector<TopKEntry>, HeapCmp> heap;
        HeapCmp hcmp;

        // Linear sweep over order_map — sequential access pattern lets hardware
        // prefetcher stride through keys (8MB), odates (8MB), shippriors (8MB),
        // revs (16MB) with perfect stride-1 locality. Avoids 735K random accesses
        // from tl_slots scatter which thrash cache lines across all 4 arrays.
        for (uint32_t i = 0; i < order_map.cap; i++) {
            int32_t key = order_map.keys[i].load(std::memory_order_relaxed);
            if (key == INT32_MIN) continue;  // empty slot

            uint64_t rev_bits = order_map.revs[i].load(std::memory_order_relaxed);
            double rev; memcpy(&rev, &rev_bits, 8);
            if (rev == 0.0) continue;  // order had no qualifying lineitems

            TopKEntry e{ key, rev, order_map.odates[i], order_map.shippriors[i] };

            if ((int)heap.size() < 10) {
                heap.push(e);
            } else if (hcmp(e, heap.top())) {
                heap.pop();
                heap.push(e);
            }
        }

        // Extract and sort: revenue DESC, o_orderdate ASC
        std::vector<TopKEntry> results;
        results.reserve(heap.size());
        while (!heap.empty()) {
            results.push_back(heap.top());
            heap.pop();
        }
        std::sort(results.begin(), results.end(), [](const TopKEntry& a, const TopKEntry& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q3.csv";
        std::ofstream out(outpath);
        if (!out) { std::cerr << "Cannot open output: " << outpath << "\n"; exit(1); }
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed;
        out.precision(2);
        char datebuf[32];
        for (auto& e : results) {
            gendb::epoch_days_to_date_str(e.o_orderdate, datebuf);
            out << e.l_orderkey << ","
                << e.revenue << ","
                << datebuf << ","
                << e.o_shippriority << "\n";
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
