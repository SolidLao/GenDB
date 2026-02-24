// Q3: Shipping Priority — GenDB iteration 5
// Strategy: customer parallel scan → orders thread-local collect+sequential build → bloom + orders hash map (8B/entry, L3-fit) → lineitem static-scheduled parallel agg → top-10

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
// Packing: 8 bytes/entry → 4M slots = 32MB (fits in L3=44MB, vs 48MB for 12B entry)
// packed = (uint16_t(shippriority) << 16) | uint16_t(orderdate - BASE_OD)
// BASE_OD = 8035 (1992-01-01 epoch days); TPC-H o_orderdate range: 1992..1998 = 0..2556 offset
static constexpr int32_t BASE_OD = 8035; // epoch days for 1992-01-01
struct OrderEntry {
    int32_t key;
    int32_t packed; // high16: shippriority, low16: (orderdate - BASE_OD)
};
static_assert(sizeof(OrderEntry) == 8, "OrderEntry must be 8 bytes");

struct OrdersMap {
    static constexpr int32_t EMPTY = 0;
    uint32_t cap, mask;
    OrderEntry* entries;

    explicit OrdersMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        entries = (OrderEntry*)calloc(cap, sizeof(OrderEntry)); // zero = EMPTY
    }
    ~OrdersMap() { free(entries); }

    static inline int32_t pack_od_sp(int32_t od, int32_t sp) {
        return (int32_t)(((uint32_t)(uint16_t)sp << 16) | (uint16_t)(od - BASE_OD));
    }
    static inline int32_t unpack_od(int32_t packed) {
        return BASE_OD + (int32_t)(uint16_t)(packed & 0xFFFF);
    }
    static inline int32_t unpack_sp(int32_t packed) {
        return (int32_t)(uint16_t)((uint32_t)packed >> 16);
    }

    inline void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == EMPTY || entries[h].key == k) {
                entries[h] = {k, pack_od_sp(od, sp)};
                return;
            }
        }
    }

    // Returns true if found; fills od_out and sp_out with decoded values
    inline bool find(int32_t k, int32_t& od_out, int32_t& sp_out) const {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == k) {
                od_out = unpack_od(entries[h].packed);
                sp_out = unpack_sp(entries[h].packed);
                return true;
            }
            if (entries[h].key == EMPTY) return false;
        }
        return false;
    }
};

// Thread-local aggregation map: orderkey → {orderdate, shippriority, revenue}
// EMPTY=0: TPC-H orderkeys are 1-based → calloc works for initialization
// Packing: 16 bytes/entry → 16K slots = 256KB/thread (L2-resident); 64 threads = 16MB total
struct AggEntry {
    int32_t  key;
    uint16_t od_off;      // orderdate - BASE_OD
    uint16_t shippriority;// always 0 in TPC-H; stored for correctness
    double   revenue;     // 8 bytes — naturally aligned at offset 8
};
static_assert(sizeof(AggEntry) == 16, "AggEntry must be 16 bytes");

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
            if (entries[h].key == EMPTY) {
                entries[h] = {k, (uint16_t)(od - BASE_OD), (uint16_t)sp, rev};
                return;
            }
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

        // Parallel scan: each thread collects matching custkeys into thread-local vector
        // Sequential merge into shared hash set (no synchronization needed in merge phase)
        std::vector<std::vector<int32_t>> thr_cust(nthreads);
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& lv = thr_cust[tid];
            lv.reserve(8192);
            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n_cust; i++) {
                if (c_seg[i] == bldg_code) {
                    lv.push_back(c_key[i]);
                }
            }
        }
        for (int t = 0; t < nthreads; t++) {
            for (int32_t k : thr_cust[t]) cust_set.insert(k);
        }
    }

    // ---- Phase 2: Parallel orders scan + orders hash map + bloom filter build ----
    // OM_CAP = 4M slots × 8B = 32MB (fits in L3=44MB) for ~1.46M qualifying orders → ~36% load
    constexpr uint32_t OM_CAP = 4194304u; // 4M, power of 2
    OrdersMap* ord_map = new OrdersMap(OM_CAP);
    BloomFilter* bloom = new BloomFilter();

    {
        GENDB_PHASE("build_joins");

        // Thread-local collect: each thread gathers qualifying (orderkey, orderdate, shippriority)
        // tuples from its static partition. Avoids CAS cache-line ping-pong across 64 threads.
        // Sequential insert after parallel phase: no contention, unique orderkeys.
        struct OrdTuple { int32_t key, dat, spr; };
        std::vector<std::vector<OrdTuple>> thr_ords(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& lv = thr_ords[tid];
            lv.reserve(32768);
            #pragma omp for schedule(static) nowait
            for (int64_t i = 0; i < (int64_t)o_end_row; i++) {
                if (o_dat[i] < DATE_THRESH && cust_set.contains(o_cky[i])) {
                    lv.push_back({o_key[i], o_dat[i], o_spr[i]});
                }
            }
        }

        // Sequential insert + bloom build (no contention; unique PKs)
        for (int t = 0; t < nthreads; t++) {
            for (auto& e : thr_ords[t]) {
                ord_map->insert(e.key, e.dat, e.spr);
                bloom->insert(e.key);
            }
        }
    }

    // ---- Phase 3: Parallel lineitem scan + thread-local aggregation ----
    // AGG_CAP: 16384 slots. Each thread processes ~308K/64 ≈ 4.8K qualifying rows
    // covering ~4.6K distinct orderkey groups → load factor ~28% at 16K slots.
    // 16K × 16B = 256KB per thread → fits in L2 cache (fully hot during scan).
    // AggMap uses EMPTY=0 + calloc: OS provides zero-pages with no upfront cost.
    constexpr uint32_t AGG_CAP = 16384u;
    std::vector<AggMap*> thr_agg(nthreads, nullptr);
    // Parallel AggMap allocation: each thread faults its own pages, avoiding sequential bottleneck
    #pragma omp parallel for num_threads(nthreads) schedule(static, 1)
    for (int t = 0; t < nthreads; t++) thr_agg[t] = new AggMap(AGG_CAP);

    {
        GENDB_PHASE("main_scan");

        const OrdersMap& om   = *ord_map;
        const BloomFilter& bf = *bloom;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            AggMap& lagg = *thr_agg[tid];

            // schedule(static): each thread owns a contiguous range of lineitem rows.
            // Enables HW prefetcher to work per-stream (vs dynamic which fragments streams).
            #pragma omp for schedule(static) nowait
            for (int64_t i = (int64_t)l_start_row; i < (int64_t)n_li; i++) {
                if (l_shp[i] <= DATE_THRESH) continue;          // date filter
                int32_t ok = l_oky[i];
                if (!bf.test(ok)) continue;                      // bloom pre-filter
                int32_t od, sp;
                if (!om.find(ok, od, sp)) continue;              // hash map probe (8B entries, L3-fit)
                double rev = l_prc[i] * (1.0 - d_lut[l_dsc[i]]);
                lagg.accumulate(ok, od, sp, rev);
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
    // AGG_CAP=16K → only ~4.6K real entries per thread → collect vectors are tiny
    std::vector<std::vector<AggEntry>> thr_collected(nthreads);
    {
        GENDB_PHASE("aggregation_merge");

        // Each thread collects non-empty entries from its own thread-local map.
        // With AGG_CAP=16384 slots per thread, this is 16384 × nthreads total slots
        // scanned in parallel — very fast (~256KB per thread, fully L2-resident).
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& col = thr_collected[tid];
            col.reserve(8192);
            AggMap& am = *thr_agg[tid];
            for (uint32_t i = 0; i < am.cap; i++) {
                if (am.entries[i].key != AggMap::EMPTY) {
                    col.push_back(am.entries[i]);
                }
            }
        }

        // Free thread-local agg maps
        for (int t = 0; t < nthreads; t++) {
            delete thr_agg[t];
            thr_agg[t] = nullptr;
        }
    }

    // Final cap: 131072 (128K) for up to ~77K groups at ~59% load factor
    // 128K × 16B = 2MB → fits comfortably in L3 for sequential merge
    constexpr uint32_t FINAL_CAP = 131072u;
    AggMap final_agg(FINAL_CAP);

    {
        // Step B: merge collected entries into final map (sequential, only real entries)
        // ~64 threads × ~4.6K entries = ~295K accumulate calls into 2MB map → L3 hot → ~3ms
        for (int t = 0; t < nthreads; t++) {
            for (auto& e : thr_collected[t]) {
                final_agg.accumulate(e.key, BASE_OD + (int32_t)e.od_off, (int32_t)e.shippriority, e.revenue);
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
                rows.push_back({e.key, BASE_OD + (int32_t)e.od_off, (int32_t)e.shippriority, e.revenue});
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
