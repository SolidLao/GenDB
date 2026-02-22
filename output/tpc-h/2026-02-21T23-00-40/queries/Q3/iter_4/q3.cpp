// Q3: Shipping Priority — GenDB iteration 0
// Strategy: customer hash set → orders parallel scan → bloom + orders hash map → lineitem parallel agg → top-10

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <tuple>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ===========================================================
// Inline data structures
// ===========================================================

// Open-addressing hash set for int32_t customer keys
// EMPTY=0: TPC-H custkeys are 1-based, so 0 is a safe sentinel → calloc works
struct CustKeySet {
    static constexpr int32_t EMPTY = 0;
    uint32_t cap, mask;
    int32_t* keys;

    explicit CustKeySet(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        keys = (int32_t*)calloc(cap, sizeof(int32_t)); // zero-pages from OS = EMPTY
    }
    ~CustKeySet() { free(keys); }

    inline void insert(int32_t k) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (keys[h] == EMPTY || keys[h] == k) { keys[h] = k; return; }
        }
    }

    // CAS-based concurrent insert — safe for parallel build (custkeys unique per TPC-H)
    inline void insert_concurrent(int32_t k) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            int32_t cur = __atomic_load_n(&keys[h], __ATOMIC_RELAXED);
            if (cur == k) return;
            if (cur == EMPTY) {
                int32_t expected = EMPTY;
                if (__atomic_compare_exchange_n(&keys[h], &expected, k,
                                                false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
                    return;
                if (expected == k) return;
            }
        }
    }

    inline bool contains(int32_t k) const {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (keys[h] == k)     return true;
            if (keys[h] == EMPTY) return false;
        }
        return false;
    }
};

// Open-addressing hash map: orderkey → {orderdate, shippriority}
// EMPTY=0: TPC-H orderkeys are 1-based → calloc works for initialization
struct OrderEntry {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersMap {
    static constexpr int32_t EMPTY = 0;
    uint32_t cap, mask;
    OrderEntry* entries;

    explicit OrdersMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        entries = (OrderEntry*)calloc(cap, sizeof(OrderEntry)); // zero = EMPTY
    }
    ~OrdersMap() { free(entries); }

    inline void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == EMPTY || entries[h].key == k) {
                entries[h] = {k, od, sp};
                return;
            }
        }
    }

    // Lock-free concurrent insert using CAS — safe because:
    // (a) TPC-H orderkeys are unique (primary key), so no two threads insert same key
    // (b) Reads only happen after the parallel build phase (OMP barrier ensures visibility)
    inline void insert_concurrent(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        int32_t expected = EMPTY;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (__atomic_compare_exchange_n(&entries[h].key, &expected, k,
                                            false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                // Slot claimed: write payload (safe, reader is in later OMP phase)
                entries[h].orderdate   = od;
                entries[h].shippriority = sp;
                return;
            }
            if (expected == k) return; // another thread inserted same key (shouldn't happen with PK)
            expected = EMPTY;
        }
    }

    inline const OrderEntry* find(int32_t k) const {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == k)     return &entries[h];
            if (entries[h].key == EMPTY) return nullptr;
        }
        return nullptr;
    }
};

// Thread-local aggregation map: orderkey → {orderdate, shippriority, revenue}
// EMPTY=0: TPC-H orderkeys are 1-based → calloc works for initialization
struct AggEntry {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
    double  revenue;
};

struct AggMap {
    static constexpr int32_t EMPTY = 0;
    uint32_t cap, mask;
    AggEntry* entries;

    explicit AggMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        entries = (AggEntry*)calloc(cap, sizeof(AggEntry)); // zero = EMPTY; OS zero-pages = fast
    }
    ~AggMap() { free(entries); }

    inline void accumulate(int32_t k, int32_t od, int32_t sp, double rev) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == k) { entries[h].revenue += rev; return; }
            if (entries[h].key == EMPTY) { entries[h] = {k, od, sp, rev}; return; }
        }
    }
};

// Bloom filter: 2MB bitvector (16M bits), 3 hash functions — heap-allocated
// Targets ~1.46M keys → FP rate ~1.4%
struct BloomFilter {
    static constexpr uint32_t BITS = (1u << 24); // 16M bits
    static constexpr uint32_t MASK = BITS - 1;
    uint64_t* data;

    BloomFilter() {
        data = (uint64_t*)calloc(BITS / 64, sizeof(uint64_t)); // zero-init
    }
    ~BloomFilter() { free(data); }

    inline void insert(int32_t k) {
        uint32_t u = (uint32_t)k;
        uint32_t h1 = u * 2654435761u;
        uint32_t h2 = u * 2246822519u;
        uint32_t h3 = u * 3266489917u;
        data[(h1 & MASK) >> 6] |= 1ull << (h1 & 63);
        data[(h2 & MASK) >> 6] |= 1ull << (h2 & 63);
        data[(h3 & MASK) >> 6] |= 1ull << (h3 & 63);
    }

    // Concurrent insert using atomic OR — safe for parallel build
    inline void insert_concurrent(int32_t k) {
        uint32_t u = (uint32_t)k;
        uint32_t h1 = u * 2654435761u;
        uint32_t h2 = u * 2246822519u;
        uint32_t h3 = u * 3266489917u;
        __atomic_fetch_or(&data[(h1 & MASK) >> 6], 1ull << (h1 & 63), __ATOMIC_RELAXED);
        __atomic_fetch_or(&data[(h2 & MASK) >> 6], 1ull << (h2 & 63), __ATOMIC_RELAXED);
        __atomic_fetch_or(&data[(h3 & MASK) >> 6], 1ull << (h3 & 63), __ATOMIC_RELAXED);
    }

    inline bool test(int32_t k) const {
        uint32_t u = (uint32_t)k;
        uint32_t h1 = u * 2654435761u;
        uint32_t h2 = u * 2246822519u;
        uint32_t h3 = u * 3266489917u;
        return ((data[(h1 & MASK) >> 6] >> (h1 & 63)) & 1) &&
               ((data[(h2 & MASK) >> 6] >> (h2 & 63)) & 1) &&
               ((data[(h3 & MASK) >> 6] >> (h3 & 63)) & 1);
    }
};

// Zone map entry layout (as in the binary files)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t start_row;
};

// ===========================================================
// Helper: mmap a file read-only
// ===========================================================
// mmap without MAP_POPULATE: returns immediately (no upfront page faults).
// Rely on selective madvise(MADV_WILLNEED) to prefetch only qualifying ranges.
static void* mmap_open(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    sz = (size_t)st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0); // No MAP_POPULATE
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    posix_fadvise(fd, 0, (off_t)sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ===========================================================
// Main query
// ===========================================================
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    constexpr int32_t DATE_THRESH = 9204; // 1995-03-15 epoch days

    const std::string cdir = gendb_dir + "/customer";
    const std::string odir = gendb_dir + "/orders";
    const std::string ldir = gendb_dir + "/lineitem";

    // ---- Phase 0: mmap all columns + selective madvise prefetch ----
    size_t sz_cseg, sz_ckey;
    size_t sz_ocky, sz_odat, sz_okey, sz_ospr;
    size_t sz_lshp, sz_loky, sz_lprc, sz_ldsc, sz_dlut;
    size_t sz_ozm, sz_lzm;

    const uint8_t*  c_seg  = nullptr;
    const int32_t*  c_key  = nullptr;
    const int32_t*  o_cky  = nullptr;
    const int32_t*  o_dat  = nullptr;
    const int32_t*  o_key  = nullptr;
    const int32_t*  o_spr  = nullptr;
    const int32_t*  l_shp  = nullptr;
    const int32_t*  l_oky  = nullptr;
    const double*   l_prc  = nullptr;
    const uint8_t*  l_dsc  = nullptr;
    const double*   d_lut  = nullptr;
    const char*     ozm_raw = nullptr;
    const char*     lzm_raw = nullptr;

    {
        GENDB_PHASE("data_loading");

        // Step 1: Load zone maps first (tiny files, sequential — needed for boundaries)
        ozm_raw = (const char*)mmap_open(odir + "/indexes/orderdate_zonemap.bin", sz_ozm);
        lzm_raw = (const char*)mmap_open(ldir + "/indexes/shipdate_zonemap.bin",  sz_lzm);

        // Immediately fault in zone map pages (tiny, few pages total)
        madvise((void*)ozm_raw, sz_ozm, MADV_WILLNEED);
        madvise((void*)lzm_raw, sz_lzm, MADV_WILLNEED);

        // Step 2: Parse zone maps and compute prefetch boundaries
        uint32_t o_nblk_dl = *(const uint32_t*)ozm_raw;
        const ZoneMapEntry* o_zm_dl = (const ZoneMapEntry*)(ozm_raw + sizeof(uint32_t));
        uint32_t l_nblk_dl = *(const uint32_t*)lzm_raw;
        const ZoneMapEntry* l_zm_dl = (const ZoneMapEntry*)(lzm_raw + sizeof(uint32_t));

        uint32_t o_end_blk_dl = o_nblk_dl;
        for (uint32_t b = 0; b < o_nblk_dl; b++) {
            if (o_zm_dl[b].min_val >= DATE_THRESH) { o_end_blk_dl = b; break; }
        }
        uint32_t l_st_blk_dl = l_nblk_dl;
        for (uint32_t b = 0; b < l_nblk_dl; b++) {
            if (l_zm_dl[b].max_val > DATE_THRESH) { l_st_blk_dl = b; break; }
        }

        // Precompute madvise byte ranges (needed inside parallel sections)
        // These will be used after mmap in each section
        // We store them as locals; sections capture by reference
        const size_t o_end_row_dl = (o_end_blk_dl < o_nblk_dl)
            ? o_zm_dl[o_end_blk_dl].start_row
            : (size_t)(15000000);
        const size_t ord_byte_len_dl = o_end_row_dl * sizeof(int32_t);

        const size_t l_start_row_dl = (l_st_blk_dl < l_nblk_dl)
            ? l_zm_dl[l_st_blk_dl].start_row
            : (size_t)(59986052);
        const size_t li_i32_off_dl = l_start_row_dl * sizeof(int32_t);
        const size_t li_dbl_off_dl = l_start_row_dl * sizeof(double);

        // Step 3: Parallel mmap + selective madvise across column groups.
        // Without MAP_POPULATE, mmap returns instantly; madvise issues async prefetch.
        // By the time we access data (dim_filter/build_joins/main_scan), pages are ready.
        #pragma omp parallel sections num_threads(5)
        {
            #pragma omp section
            {
                // Customer columns (7.5MB total) — fully prefetch
                c_seg = (const uint8_t*) mmap_open(cdir + "/c_mktsegment.bin",   sz_cseg);
                c_key = (const int32_t*) mmap_open(cdir + "/c_custkey.bin",      sz_ckey);
                madvise((void*)c_seg, sz_cseg, MADV_WILLNEED);
                madvise((void*)c_key, sz_ckey, MADV_WILLNEED);
            }
            #pragma omp section
            {
                // Orders cols 1+2: custkey + orderdate (120MB; prefetch qualifying range)
                o_cky = (const int32_t*) mmap_open(odir + "/o_custkey.bin",    sz_ocky);
                o_dat = (const int32_t*) mmap_open(odir + "/o_orderdate.bin",  sz_odat);
                if (ord_byte_len_dl > 0) {
                    madvise((void*)o_cky, ord_byte_len_dl, MADV_WILLNEED);
                    madvise((void*)o_dat, ord_byte_len_dl, MADV_WILLNEED);
                }
            }
            #pragma omp section
            {
                // Orders cols 3+4: orderkey + shippriority (120MB; selective prefetch)
                o_key = (const int32_t*) mmap_open(odir + "/o_orderkey.bin",     sz_okey);
                o_spr = (const int32_t*) mmap_open(odir + "/o_shippriority.bin", sz_ospr);
                if (ord_byte_len_dl > 0) {
                    madvise((void*)o_key, ord_byte_len_dl, MADV_WILLNEED);
                    madvise((void*)o_spr, ord_byte_len_dl, MADV_WILLNEED);
                }
            }
            #pragma omp section
            {
                // Lineitem int32 columns: shipdate + orderkey (480MB; selective prefetch)
                l_shp = (const int32_t*) mmap_open(ldir + "/l_shipdate.bin",  sz_lshp);
                l_oky = (const int32_t*) mmap_open(ldir + "/l_orderkey.bin",  sz_loky);
                const size_t li_i32_len_dl = sz_lshp - li_i32_off_dl;
                if (li_i32_len_dl > 0) {
                    madvise((void*)((const char*)l_shp + li_i32_off_dl), li_i32_len_dl, MADV_WILLNEED);
                    madvise((void*)((const char*)l_oky + li_i32_off_dl), li_i32_len_dl, MADV_WILLNEED);
                }
            }
            #pragma omp section
            {
                // Lineitem payload: extendedprice (double) + discount (uint8) + LUT
                l_prc = (const double*)  mmap_open(ldir + "/l_extendedprice.bin",   sz_lprc);
                l_dsc = (const uint8_t*) mmap_open(ldir + "/l_discount.bin",        sz_ldsc);
                d_lut = (const double*)  mmap_open(ldir + "/l_discount_lookup.bin", sz_dlut);
                const size_t li_dbl_len_dl = sz_lprc - li_dbl_off_dl;
                const size_t li_u8_off_dl  = l_start_row_dl;
                const size_t li_u8_len_dl  = sz_ldsc - li_u8_off_dl;
                if (li_dbl_len_dl > 0) {
                    madvise((void*)((const char*)l_prc + li_dbl_off_dl), li_dbl_len_dl, MADV_WILLNEED);
                    madvise((void*)((const char*)l_dsc + li_u8_off_dl),  li_u8_len_dl,  MADV_WILLNEED);
                }
                madvise((void*)d_lut, sz_dlut, MADV_WILLNEED);
            }
        }
    }

    // Re-parse zone map pointers (needed after data_loading scope)
    uint32_t o_nblk = *(const uint32_t*)ozm_raw;
    const ZoneMapEntry* o_zm = (const ZoneMapEntry*)(ozm_raw + sizeof(uint32_t));
    uint32_t l_nblk = *(const uint32_t*)lzm_raw;
    const ZoneMapEntry* l_zm = (const ZoneMapEntry*)(lzm_raw + sizeof(uint32_t));

    const size_t n_cust = sz_cseg;                          // uint8_t → 1 byte each
    const size_t n_ord  = sz_odat / sizeof(int32_t);
    const size_t n_li   = sz_lshp / sizeof(int32_t);

    // Compute scan boundaries
    uint32_t o_end_blk = o_nblk;
    for (uint32_t b = 0; b < o_nblk; b++) {
        if (o_zm[b].min_val >= DATE_THRESH) { o_end_blk = b; break; }
    }
    uint32_t o_end_row = (o_end_blk < o_nblk) ? o_zm[o_end_blk].start_row : (uint32_t)n_ord;

    uint32_t l_st_blk = l_nblk;
    for (uint32_t b = 0; b < l_nblk; b++) {
        if (l_zm[b].max_val > DATE_THRESH) { l_st_blk = b; break; }
    }
    uint32_t l_start_row = (l_st_blk < l_nblk) ? l_zm[l_st_blk].start_row : (uint32_t)n_li;

    int nthreads = omp_get_max_threads();

    // ---- Phase 1: Customer filter → build custkey hash set ----
    CustKeySet cust_set(524288); // 512K slots; load factor ~58% for 301K keys

    {
        GENDB_PHASE("dim_filter");

        // Load dictionary to find BUILDING code
        uint8_t bldg_code = 255;
        {
            std::ifstream df(cdir + "/c_mktsegment_dict.txt");
            std::string line;
            uint8_t code = 0;
            while (std::getline(df, line)) {
                if (line == "BUILDING") { bldg_code = code; break; }
                ++code;
            }
        }

        // Parallel scan: each thread inserts into shared set via CAS
        // CustKeySet is 2MB (L3-resident); CAS contention low for 315K unique keys
        #pragma omp parallel for num_threads(nthreads) schedule(static)
        for (int64_t i = 0; i < (int64_t)n_cust; i++) {
            if (c_seg[i] == bldg_code) {
                cust_set.insert_concurrent(c_key[i]);
            }
        }
    }

    // ---- Phase 2: Parallel orders scan + concurrent orders hash map + bloom filter build ----
    // Pre-allocate with known capacity (calloc = O(1) virtual alloc, no upfront zeroing cost)
    // Reduce ord_map capacity: 2M slots @ 73% load for 1.46M keys → 24MB fits in L3 (44MB)
    // vs old 4M = 48MB DRAM. Enables L3-resident probes in both build_joins and main_scan.
    constexpr uint32_t OM_CAP = 4194304u; // 4M, power of 2 — ~35% load factor for ~1.46M qualifying orders
    OrdersMap* ord_map = new OrdersMap(OM_CAP);
    BloomFilter* bloom = new BloomFilter();

    {
        GENDB_PHASE("build_joins");

        // Single parallel pass: scan orders, probe customer set, insert into shared hash map
        // + bloom filter concurrently.  CAS-based insert is safe because orderkeys are unique
        // (primary key), so two threads never insert the same key.
        // OMP barrier at end of parallel region provides memory fence for later readers.
        #pragma omp parallel num_threads(nthreads)
        {
            #pragma omp for schedule(dynamic, 16384)
            for (int64_t i = 0; i < (int64_t)o_end_row; i++) {
                if (o_dat[i] < DATE_THRESH && cust_set.contains(o_cky[i])) {
                    int32_t ok = o_key[i];
                    ord_map->insert_concurrent(ok, o_dat[i], o_spr[i]);
                    bloom->insert_concurrent(ok);
                }
            }
        }
    }

    // ---- Phase 3: Parallel lineitem scan + thread-local aggregation ----
    // KEY OPTIMIZATION: Use 8 threads instead of 64.
    //   With 64 threads: 64 × 4MB AggMaps = 256MB DRAM → random writes thrash DRAM.
    //   With 8 threads: 8 × 4MB AggMaps = 32MB + 24MB ord_map + 2MB bloom = 58MB ≈ L3 (44MB).
    //   AggMap accumulate() calls become L3 hits → ~5ns vs ~100ns DRAM → huge speedup.
    //   Also: aggregation_merge collects 8×77K=616K entries vs 64×77K=4.9M → 8× faster merge.
    // AGG_CAP: 131072 slots → covers ~77K distinct groups at ~59% load factor.
    constexpr uint32_t AGG_CAP = 131072u;
    const int scan_threads = std::min(nthreads, 8);
    std::vector<AggMap*> thr_agg(scan_threads, nullptr);
    // Parallel AggMap allocation: each thread faults its own pages concurrently
    #pragma omp parallel for num_threads(scan_threads) schedule(static, 1)
    for (int t = 0; t < scan_threads; t++) thr_agg[t] = new AggMap(AGG_CAP);

    {
        GENDB_PHASE("main_scan");

        const OrdersMap& om   = *ord_map;
        const BloomFilter& bf = *bloom;

        // Use scan_threads (8) so AggMaps + ord_map + bloom fit in L3 cache.
        // Static scheduling: contiguous chunks per thread → better HW prefetch, no OMP queue overhead.
        #pragma omp parallel num_threads(scan_threads)
        {
            int tid = omp_get_thread_num();
            AggMap& lagg = *thr_agg[tid];

            #pragma omp for schedule(static)
            for (int64_t i = (int64_t)l_start_row; i < (int64_t)n_li; i++) {
                if (l_shp[i] <= DATE_THRESH) continue;          // date filter
                int32_t ok = l_oky[i];
                if (!bf.test(ok)) continue;                      // bloom pre-filter
                const OrderEntry* oe = om.find(ok);
                if (!oe) continue;                               // hash map probe
                double rev = l_prc[i] * (1.0 - d_lut[l_dsc[i]]);
                lagg.accumulate(ok, oe->orderdate, oe->shippriority, rev);
            }
        }
    }

    delete ord_map; ord_map = nullptr;
    delete bloom;   bloom   = nullptr;

    // ---- Merge thread-local agg maps into final map ----
    // Strategy: first collect all non-empty entries from all thread-local maps into a flat
    // vector (parallel), then merge into a single final map (sequential over only real entries).
    // This avoids iterating over mostly-empty slots sequentially for every thread.

    // Step A: parallel collection of all populated entries
    std::vector<std::vector<AggEntry>> thr_collected(scan_threads);
    {
        GENDB_PHASE("aggregation_merge");

        // Each thread collects non-empty entries from its own thread-local map.
        // With scan_threads=8 and AGG_CAP=131072: 8 × 4MB = 32MB total scanned in parallel.
        // Total collected entries: 8 × ~77K = ~616K (vs 64 × 77K = 4.9M with 64 threads).
        // Sequential merge of 616K entries is 8× faster than previous 4.9M entries.
        #pragma omp parallel num_threads(scan_threads)
        {
            int tid = omp_get_thread_num();
            auto& col = thr_collected[tid];
            col.reserve(AGG_CAP / 2); // pre-reserve for ~77K entries per thread
            AggMap& am = *thr_agg[tid];
            for (uint32_t i = 0; i < am.cap; i++) {
                if (am.entries[i].key != AggMap::EMPTY) {
                    col.push_back(am.entries[i]);
                }
            }
        }

        // Free thread-local agg maps
        for (int t = 0; t < scan_threads; t++) {
            delete thr_agg[t];
            thr_agg[t] = nullptr;
        }
    }

    // Final cap: 262144 (256K) for up to ~77K groups at ~29% load factor
    constexpr uint32_t FINAL_CAP = 262144u;
    AggMap final_agg(FINAL_CAP);

    {
        // Step B: merge collected entries into final map (sequential, only real entries)
        for (int t = 0; t < scan_threads; t++) {
            for (auto& e : thr_collected[t]) {
                final_agg.accumulate(e.key, e.orderdate, e.shippriority, e.revenue);
            }
        }
        thr_collected.clear();
    }

    // ---- Top-10 by revenue DESC, o_orderdate ASC ----
    struct ResultRow {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
        double  revenue;
    };

    std::vector<ResultRow> rows;
    rows.reserve(100000);

    {
        GENDB_PHASE("sort_topk");

        for (uint32_t i = 0; i < final_agg.cap; i++) {
            if (final_agg.entries[i].key != AggMap::EMPTY) {
                auto& e = final_agg.entries[i];
                rows.push_back({e.key, e.orderdate, e.shippriority, e.revenue});
            }
        }

        size_t topk = std::min((size_t)10, rows.size());
        std::partial_sort(rows.begin(), rows.begin() + topk, rows.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });
        rows.resize(topk);
    }

    // ---- Output CSV ----
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); exit(1); }

        char date_buf[16];
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (auto& r : rows) {
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n",
                r.orderkey,
                r.revenue,
                date_buf,
                r.shippriority);
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
