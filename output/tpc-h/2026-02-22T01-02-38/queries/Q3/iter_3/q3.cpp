// Q3: Shipping Priority — GenDB iteration 0
// Strategy:
//   Phase 1: customer scan → custkey hash set
//   Phase 2: orders zone-map scan + custkey probe → orderkey hash map + bloom filter
//   Phase 3: lineitem zone-map scan + bloom + hash probe → per-thread hit vectors
//   Phase 4: Kahan-summation hash aggregation + top-10 min-heap

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

static constexpr int32_t DATE_THRESHOLD = 9204; // 1995-03-15
static constexpr int32_t EMPTY_KEY      = -1;   // 0xFFFFFFFF — safe sentinel (all TPC-H keys > 0)
                                                  // Allows fast memset(0xFF) initialization

// ---------------------------------------------------------------------------
// Hash function
// ---------------------------------------------------------------------------
inline uint32_t hash32(int32_t k) {
    return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> 32);
}

// ---------------------------------------------------------------------------
// Customer bitmap: 1 bit per possible custkey (1..1,500,000)
// 187,501 bytes — fits in L2 cache; direct bit test, no hash/probe needed
// ---------------------------------------------------------------------------
static constexpr uint32_t CUST_BITMAP_BYTES = (1500001u + 7u) / 8u; // 187501 bytes

inline void bitmap_set(uint8_t* bm, int32_t k) {
    __atomic_or_fetch(&bm[(uint32_t)k >> 3], (uint8_t)(1u << ((uint32_t)k & 7u)), __ATOMIC_RELAXED);
}
inline bool bitmap_test(const uint8_t* bm, int32_t k) {
    return (k > 0 && (uint32_t)k < 1500001u) &&
           (bm[(uint32_t)k >> 3] & (1u << ((uint32_t)k & 7u)));
}

// ---------------------------------------------------------------------------
// Open-addressing hash map: orderkey -> (orderdate, shippriority)
// (~735K entries, cap=2097152)
// ---------------------------------------------------------------------------
struct OrdEntry {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdMap {
    OrdEntry* slots = nullptr;
    uint32_t  mask  = 0;

    void init(uint32_t cap) {
        slots = new OrdEntry[cap];
        // memset(0xFF) sets all int32 fields to -1 == EMPTY_KEY; doubles become NaN (checked after key test)
        memset(slots, 0xFF, (size_t)cap * sizeof(OrdEntry));
        mask = cap - 1;
    }

    void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (slots[s].key == EMPTY_KEY) { slots[s] = {k, od, sp}; return; }
            if (slots[s].key == k) return;
        }
    }

    const OrdEntry* find(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (slots[s].key == EMPTY_KEY) return nullptr;
            if (slots[s].key == k) return &slots[s];
        }
        return nullptr;
    }

    ~OrdMap() { delete[] slots; }
};

// ---------------------------------------------------------------------------
// Bloom filter (3 hashes, 1MB = 8M bits, ~1% FP for 735K keys)
// ---------------------------------------------------------------------------
struct Bloom {
    uint8_t* data = nullptr;
    uint32_t mask = 0; // bit-index mask

    void init(uint32_t nbytes) {
        data = new uint8_t[nbytes]();
        mask = nbytes * 8u - 1u;
    }

    void add(int32_t k) {
        uint32_t h1 = hash32(k);
        uint32_t h2 = (uint32_t)(((uint64_t)(uint32_t)k * 0xC4CEB9FE1A85EC53ULL) >> 32);
        uint32_t h3 = h1 ^ (h2 * 0x9E3779B9u);
        uint32_t b1 = h1 & mask, b2 = h2 & mask, b3 = h3 & mask;
        data[b1 >> 3] |= 1u << (b1 & 7);
        data[b2 >> 3] |= 1u << (b2 & 7);
        data[b3 >> 3] |= 1u << (b3 & 7);
    }

    bool test(int32_t k) const {
        uint32_t h1 = hash32(k);
        uint32_t h2 = (uint32_t)(((uint64_t)(uint32_t)k * 0xC4CEB9FE1A85EC53ULL) >> 32);
        uint32_t h3 = h1 ^ (h2 * 0x9E3779B9u);
        uint32_t b1 = h1 & mask, b2 = h2 & mask, b3 = h3 & mask;
        return (data[b1 >> 3] & (1u << (b1 & 7))) &&
               (data[b2 >> 3] & (1u << (b2 & 7))) &&
               (data[b3 >> 3] & (1u << (b3 & 7)));
    }

    ~Bloom() { delete[] data; }
};

// ---------------------------------------------------------------------------
// Thread-local per-partition aggregation entry (no Kahan — merged later)
// Sentinel: orderkey == -1 (EMPTY_KEY). Layout: 24 bytes for cache efficiency.
// ---------------------------------------------------------------------------
static constexpr int P           = 32;           // partitions
static constexpr int SCAN_THREADS = 64;          // lineitem scan threads
static constexpr int THAGG_SLOTS  = 2048;        // slots per (thread,partition) map; sized for ~1024 keys at 50% load with static scheduling
                                                  // ~358 distinct keys → 35% load, good for linear probe
                                                  // 24KB per map, 32 maps per thread = 768KB/thread (L2/L3)

struct ThrAggEntry {
    int32_t orderkey;      // 4 bytes, EMPTY_KEY (-1) if empty
    int32_t orderdate;     // 4 bytes
    int32_t shippriority;  // 4 bytes
    int32_t _pad;          // 4 bytes → 16 bytes header
    double  revenue;       // 8 bytes → 24 bytes total
};
static_assert(sizeof(ThrAggEntry) == 24, "ThrAggEntry size mismatch");

// ---------------------------------------------------------------------------
// Global aggregation hash map with Kahan summation for precision
// key = orderkey, value = (revenue_sum, kahan_comp, orderdate, shippriority)
// ---------------------------------------------------------------------------
struct AggEntry {
    double  revenue;       // 8 bytes, Kahan running sum
    double  comp;          // 8 bytes, Kahan compensation
    int32_t orderkey;      // 4 bytes, EMPTY_KEY if empty
    int32_t orderdate;     // 4 bytes
    int32_t shippriority;  // 4 bytes
    int32_t _pad;          // 4 bytes → 32 bytes total
};

struct AggMap {
    AggEntry* slots = nullptr;
    uint32_t  mask  = 0;

    void init(uint32_t cap) {
        slots = new AggEntry[cap];
        // memset(0xFF): orderkey=-1=EMPTY_KEY; revenue/comp=NaN (never read until key confirmed valid)
        memset(slots, 0xFF, (size_t)cap * sizeof(AggEntry));
        mask = cap - 1;
    }

    // Kahan summation update
    void update(int32_t k, int32_t od, int32_t sp, double rev) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (slots[s].orderkey == EMPTY_KEY) {
                slots[s] = {rev, 0.0, k, od, sp, 0};
                return;
            }
            if (slots[s].orderkey == k) {
                // Kahan summation
                double y = rev - slots[s].comp;
                double t = slots[s].revenue + y;
                slots[s].comp    = (t - slots[s].revenue) - y;
                slots[s].revenue = t;
                return;
            }
        }
    }

    ~AggMap() { delete[] slots; }
};

// ---------------------------------------------------------------------------
// Zone map helpers
// ---------------------------------------------------------------------------
struct ZMBlock { int32_t mn, mx; };

// First block where block_max > threshold (l_shipdate > threshold on sorted col)
static uint32_t zm_first_gt(const ZMBlock* blocks, uint32_t nb, int32_t thr) {
    uint32_t lo = 0, hi = nb;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (blocks[mid].mx <= thr) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

// First block where block_min >= threshold; all blocks [0, result) are candidates
// (for o_orderdate < threshold on sorted col, scan [0, result))
static uint32_t zm_last_lt_end(const ZMBlock* blocks, uint32_t nb, int32_t thr) {
    uint32_t lo = 0, hi = nb;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (blocks[mid].mn < thr) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static std::pair<const void*, size_t> mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); return {nullptr, 0}; }
    struct stat st; fstat(fd, &st);
    void* p = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return {p, (size_t)st.st_size};
}

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string dir = gendb_dir + "/";

    // Column pointers
    const int32_t* c_custkey_col   = nullptr; size_t c_nrows = 0;
    const uint8_t* c_mktseg_col    = nullptr;
    const int32_t* o_orderkey_col  = nullptr; size_t o_nrows = 0;
    const int32_t* o_custkey_col   = nullptr;
    const int32_t* o_orderdate_col = nullptr;
    const int32_t* o_shippr_col    = nullptr;
    const int32_t* l_orderkey_col  = nullptr; size_t l_nrows = 0;
    const int32_t* l_shipdate_col  = nullptr;
    const double*  l_extprice_col  = nullptr;
    const double*  l_discount_col  = nullptr;

    const ZMBlock* o_zm_blocks = nullptr; uint32_t o_zm_nb = 0;
    const ZMBlock* l_zm_blocks = nullptr; uint32_t l_zm_nb = 0;

    uint32_t orders_end_block     = 0;
    uint32_t lineitem_start_block = 0;

    // ---- Phase 0: Data Loading ----
    {
        GENDB_PHASE("data_loading");

        // Load zone maps first (tiny)
        {
            auto [p, sz] = mmap_file(dir + "indexes/orders_orderdate_zonemap.bin");
            o_zm_nb     = *reinterpret_cast<const uint32_t*>(p);
            o_zm_blocks = reinterpret_cast<const ZMBlock*>((const uint8_t*)p + 4);
        }
        {
            auto [p, sz] = mmap_file(dir + "indexes/lineitem_shipdate_zonemap.bin");
            l_zm_nb     = *reinterpret_cast<const uint32_t*>(p);
            l_zm_blocks = reinterpret_cast<const ZMBlock*>((const uint8_t*)p + 4);
        }

        // Determine qualifying block ranges before issuing I/O
        orders_end_block     = zm_last_lt_end(o_zm_blocks, o_zm_nb, DATE_THRESHOLD);
        lineitem_start_block = zm_first_gt(l_zm_blocks, l_zm_nb, DATE_THRESHOLD);

        // Load all columns in parallel
        #pragma omp parallel sections num_threads(8)
        {
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "customer/c_custkey.bin");
              c_custkey_col = (const int32_t*)p; c_nrows = sz / 4; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "customer/c_mktsegment.bin");
              c_mktseg_col = (const uint8_t*)p; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "orders/o_orderkey.bin");
              o_orderkey_col = (const int32_t*)p; o_nrows = sz / 4; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "orders/o_custkey.bin");
              o_custkey_col = (const int32_t*)p; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "orders/o_orderdate.bin");
              o_orderdate_col = (const int32_t*)p; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "orders/o_shippriority.bin");
              o_shippr_col = (const int32_t*)p; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "lineitem/l_orderkey.bin");
              l_orderkey_col = (const int32_t*)p; l_nrows = sz / 4; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "lineitem/l_shipdate.bin");
              l_shipdate_col = (const int32_t*)p; }
        }
        #pragma omp parallel sections num_threads(2)
        {
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "lineitem/l_extendedprice.bin");
              l_extprice_col = (const double*)p; }
            #pragma omp section
            { auto [p, sz] = mmap_file(dir + "lineitem/l_discount.bin");
              l_discount_col = (const double*)p; }
        }

        // Selective madvise: qualifying lineitem block ranges
        size_t li_start = (size_t)lineitem_start_block * 100000;
        size_t li_len_i = (l_nrows - li_start) * sizeof(int32_t);
        size_t li_len_d = (l_nrows - li_start) * sizeof(double);
        #pragma omp parallel sections num_threads(4)
        {
            #pragma omp section
            madvise((void*)(l_orderkey_col + li_start), li_len_i, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)(l_shipdate_col + li_start), li_len_i, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)(l_extprice_col + li_start), li_len_d, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)(l_discount_col + li_start), li_len_d, MADV_WILLNEED);
        }

        // Selective madvise: qualifying orders block ranges
        size_t o_end = std::min((size_t)orders_end_block * 100000, o_nrows);
        size_t o_len = o_end * sizeof(int32_t);
        #pragma omp parallel sections num_threads(4)
        {
            #pragma omp section
            madvise((void*)o_orderkey_col, o_len, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)o_custkey_col, o_len, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)o_orderdate_col, o_len, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)o_shippr_col, o_len, MADV_WILLNEED);
        }
    }

    // Load c_mktsegment dictionary (never hardcode codes)
    uint8_t building_code = 255;
    {
        std::ifstream f(dir + "customer/c_mktsegment_dict.txt");
        std::string line; uint8_t code = 0;
        while (std::getline(f, line)) {
            if (line == "BUILDING") { building_code = code; break; }
            code++;
        }
    }

    // ---- Phase 1: Filter customers → custkey bitmap (direct bit-test, 188KB, L2-resident) ----
    uint8_t* cust_bitmap = nullptr;
    {
        GENDB_PHASE("dim_filter");
        cust_bitmap = new uint8_t[CUST_BITMAP_BYTES](); // zero-initialized

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < c_nrows; i++) {
            if (c_mktseg_col[i] != building_code) continue;
            bitmap_set(cust_bitmap, c_custkey_col[i]);
        }
    }

    // ---- Phase 2: Orders zone-map scan → orderkey hash map + bloom filter ----
    OrdMap ord_map;
    Bloom  bloom;
    {
        GENDB_PHASE("build_joins");

        struct OrdTuple { int32_t ok, od, sp; };
        int nt = omp_get_max_threads();
        std::vector<std::vector<OrdTuple>> tlocal(nt);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& loc = tlocal[tid];
            loc.reserve(16384);

            #pragma omp for schedule(dynamic, 2)
            for (uint32_t b = 0; b < orders_end_block; b++) {
                if (o_zm_blocks[b].mn >= DATE_THRESHOLD) continue;

                uint32_t rs = b * 100000u;
                uint32_t re = std::min(rs + 100000u, (uint32_t)o_nrows);
                for (uint32_t r = rs; r < re; r++) {
                    if (o_orderdate_col[r] >= DATE_THRESHOLD) continue;
                    if (!bitmap_test(cust_bitmap, o_custkey_col[r])) continue;
                    loc.push_back({o_orderkey_col[r], o_orderdate_col[r], o_shippr_col[r]});
                }
            }
        }

        // OrdMap init: memset(0xFF) → all keys = -1 = EMPTY_KEY; fast SIMD path
        ord_map.init(2097152); // 2M slots for ~735K keys
        bloom.init(1048576);   // 1MB = 8M bits

        // Parallel CAS insertion — o_orderkey is unique per order row, no key duplicates
        #pragma omp parallel for schedule(dynamic, 1) num_threads(64)
        for (int t = 0; t < nt; t++) {
            for (auto& tup : tlocal[t]) {
                // Bloom: atomic bit-OR (false positives are harmless)
                {
                    uint32_t h1 = hash32(tup.ok);
                    uint32_t h2 = (uint32_t)(((uint64_t)(uint32_t)tup.ok * 0xC4CEB9FE1A85EC53ULL) >> 32);
                    uint32_t h3 = h1 ^ (h2 * 0x9E3779B9u);
                    uint32_t b1 = h1 & bloom.mask, b2 = h2 & bloom.mask, b3 = h3 & bloom.mask;
                    __atomic_or_fetch(&bloom.data[b1 >> 3], (uint8_t)(1u << (b1 & 7)), __ATOMIC_RELAXED);
                    __atomic_or_fetch(&bloom.data[b2 >> 3], (uint8_t)(1u << (b2 & 7)), __ATOMIC_RELAXED);
                    __atomic_or_fetch(&bloom.data[b3 >> 3], (uint8_t)(1u << (b3 & 7)), __ATOMIC_RELAXED);
                }
                // OrdMap: CAS-based probe-and-insert (unique keys → one winner per slot)
                {
                    uint32_t h = hash32(tup.ok) & ord_map.mask;
                    for (uint32_t p = 0; ; p++) {
                        uint32_t s = (h + p) & ord_map.mask;
                        int32_t exp = EMPTY_KEY;
                        if (__atomic_compare_exchange_n(&ord_map.slots[s].key, &exp, tup.ok,
                                false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
                            ord_map.slots[s].orderdate    = tup.od;
                            ord_map.slots[s].shippriority = tup.sp;
                            break;
                        }
                        if (__atomic_load_n(&ord_map.slots[s].key, __ATOMIC_ACQUIRE) == tup.ok) break;
                    }
                }
            }
        }
    }

    // ---- Phase 3: Lineitem zone-map scan → direct thread-local per-partition aggregation ----
    // ThrAgg layout: thr_agg_slots[(p * SCAN_THREADS + tid) * THAGG_SLOTS .. + THAGG_SLOTS]
    // Each (partition, thread) has a 1024-slot ThrAggEntry map = 24KB → fits in L2 cache.
    // No hit vectors, no push_back — direct aggregation into thread-private L2-resident maps.
    // Layout contiguous per partition: for merge, partition p scans 64*1024 = 65536 contiguous slots.
    const size_t thr_agg_total = (size_t)P * SCAN_THREADS * THAGG_SLOTS;
    ThrAggEntry* thr_agg_slots = new ThrAggEntry[thr_agg_total];
    // memset(0xFF): orderkey = -1 = sentinel; revenue = NaN (only read after key confirmation)
    memset(thr_agg_slots, 0xFF, thr_agg_total * sizeof(ThrAggEntry));

    {
        GENDB_PHASE("main_scan");

        const int32_t* li_ok   = l_orderkey_col;
        const int32_t* li_sd   = l_shipdate_col;
        const double*  li_ep   = l_extprice_col;
        const double*  li_disc = l_discount_col;
        const uint32_t li_nb   = l_zm_nb;
        const uint32_t li_nr   = (uint32_t)l_nrows;

        #pragma omp parallel num_threads(SCAN_THREADS)
        {
            int tid = omp_get_thread_num();

            #pragma omp for schedule(static)
            for (uint32_t b = lineitem_start_block; b < li_nb; b++) {
                uint32_t rs = b * 100000u;
                uint32_t re = std::min(rs + 100000u, li_nr);

                for (uint32_t r = rs; r < re; r++) {
                    if (li_sd[r] <= DATE_THRESHOLD) continue;

                    int32_t ok = li_ok[r];
                    // Cache hash32(ok) — reused for bloom, partition, and ThrAgg probe
                    uint32_t hok = hash32(ok);

                    // Bloom pre-filter (1MB, L1/L2-resident → ~1ns per test)
                    {
                        uint32_t h2 = (uint32_t)(((uint64_t)(uint32_t)ok * 0xC4CEB9FE1A85EC53ULL) >> 32);
                        uint32_t h3 = hok ^ (h2 * 0x9E3779B9u);
                        uint32_t b1 = hok & bloom.mask, b2 = h2 & bloom.mask, b3 = h3 & bloom.mask;
                        if (!((bloom.data[b1 >> 3] & (1u << (b1 & 7))) &&
                              (bloom.data[b2 >> 3] & (1u << (b2 & 7))) &&
                              (bloom.data[b3 >> 3] & (1u << (b3 & 7))))) continue;
                    }

                    const OrdEntry* oe = ord_map.find(ok);
                    if (!oe) continue;

                    double rev = li_ep[r] * (1.0 - li_disc[r]);

                    // Direct aggregation into thread-local per-partition ThrAgg map (L2-resident)
                    int part = (int)(hok & (uint32_t)(P - 1));
                    ThrAggEntry* tmap = thr_agg_slots + ((size_t)part * SCAN_THREADS + tid) * THAGG_SLOTS;
                    uint32_t th = hok & (uint32_t)(THAGG_SLOTS - 1);
                    bool inserted = false;
                    for (uint32_t tp = 0; tp < (uint32_t)THAGG_SLOTS; tp++) {
                        uint32_t ts = (th + tp) & (uint32_t)(THAGG_SLOTS - 1);
                        if (tmap[ts].orderkey == EMPTY_KEY) {
                            tmap[ts] = {ok, oe->orderdate, oe->shippriority, 0, rev};
                            inserted = true;
                            break;
                        }
                        if (tmap[ts].orderkey == ok) {
                            tmap[ts].revenue += rev;
                            inserted = true;
                            break;
                        }
                    }
                    if (!inserted) {
                        // Thread-local map overflow: should not happen with static scheduling
                        // and THAGG_SLOTS sized for 2x expected keys. Abort to surface the issue.
                        fprintf(stderr, "[ThrAgg] FATAL: thread-local partition map overflow (tid=%d, part=%d). Increase THAGG_SLOTS.\n", tid, (int)(hok & (uint32_t)(P-1)));
                        std::abort();
                    }
                }
            }
        }
    }

    // ---- Phase 4: Merge ThrAgg maps (per-partition) → final AggMaps → top-10 ----
    // Each partition p: 64 threads × 1024 slots = 65536 entries to scan (mostly empty).
    // Final per-partition AggMap: 128K slots, ~23K keys → 17.5% load, 4MB → L3-resident.
    // Kahan summation in AggMap.update() handles precision for thread-sum merging.
    {
        GENDB_PHASE("aggregation_merge");

        AggMap agg_parts[P];

        #pragma omp parallel for schedule(static) num_threads(P)
        for (int p = 0; p < P; p++) {
            agg_parts[p].init(131072); // memset(0xFF) → fast SIMD init
            // Merge 64 thread-local maps for partition p (contiguous in memory)
            const ThrAggEntry* pbase = thr_agg_slots + (size_t)p * SCAN_THREADS * THAGG_SLOTS;
            for (int t = 0; t < SCAN_THREADS; t++) {
                const ThrAggEntry* tmap = pbase + (size_t)t * THAGG_SLOTS;
                for (int s = 0; s < THAGG_SLOTS; s++) {
                    if (tmap[s].orderkey == EMPTY_KEY) continue;
                    agg_parts[p].update(tmap[s].orderkey, tmap[s].orderdate,
                                        tmap[s].shippriority, tmap[s].revenue);
                }
            }
        }
        delete[] thr_agg_slots;

        // Top-10 min-heap across all 32 partitions
        struct Result {
            int32_t orderkey, orderdate, shippriority;
            double  revenue;
        };

        // comp returns true if a is "better" (higher revenue, or equal rev + earlier date)
        // → we want front of heap = worst of the current top-10
        auto comp = [](const Result& a, const Result& b) -> bool {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        };

        std::vector<Result> heap;
        heap.reserve(11);

        for (int p = 0; p < P; p++) {
            for (uint32_t i = 0; i <= agg_parts[p].mask; i++) {
                if (agg_parts[p].slots[i].orderkey == EMPTY_KEY) continue;
                auto& e = agg_parts[p].slots[i];
                Result r = {e.orderkey, e.orderdate, e.shippriority, e.revenue};

                if ((int)heap.size() < 10) {
                    heap.push_back(r);
                    if ((int)heap.size() == 10)
                        std::make_heap(heap.begin(), heap.end(), comp);
                } else {
                    const Result& worst = heap.front();
                    bool better = (r.revenue > worst.revenue) ||
                                  (r.revenue == worst.revenue && r.orderdate < worst.orderdate);
                    if (better) {
                        std::pop_heap(heap.begin(), heap.end(), comp);
                        heap.back() = r;
                        std::push_heap(heap.begin(), heap.end(), comp);
                    }
                }
            }
        }

        // Sort: revenue DESC, orderdate ASC
        std::sort(heap.begin(), heap.end(), [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });

        // ---- Phase 5: Output ----
        {
            GENDB_PHASE("output");
            std::string outpath = results_dir + "/Q3.csv";
            FILE* fp = fopen(outpath.c_str(), "w");
            if (!fp) { perror(("fopen: " + outpath).c_str()); return; }
            fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char datebuf[12];
            for (auto& r : heap) {
                gendb::epoch_days_to_date_str(r.orderdate, datebuf);
                fprintf(fp, "%d,%.2f,%s,%d\n",
                        r.orderkey, r.revenue, datebuf, r.shippriority);
            }
            fclose(fp);
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
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
