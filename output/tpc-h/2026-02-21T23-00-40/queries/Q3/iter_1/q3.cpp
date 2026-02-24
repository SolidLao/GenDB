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
struct CustKeySet {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t cap, mask;
    int32_t* keys;

    explicit CustKeySet(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        keys = new int32_t[cap];
        std::fill(keys, keys + cap, EMPTY);
    }
    ~CustKeySet() { delete[] keys; }

    inline void insert(int32_t k) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (keys[h] == EMPTY || keys[h] == k) { keys[h] = k; return; }
        }
    }

    inline bool contains(int32_t k) const {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (keys[h] == k)    return true;
            if (keys[h] == EMPTY) return false;
        }
        return false;
    }
};

// Open-addressing hash map: orderkey → {orderdate, shippriority}
struct OrderEntry {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t cap, mask;
    OrderEntry* entries;

    explicit OrdersMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        entries = new OrderEntry[cap];
        for (uint32_t i = 0; i < cap; i++) entries[i].key = EMPTY;
    }
    ~OrdersMap() { delete[] entries; }

    inline void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == EMPTY || entries[h].key == k) {
                entries[h] = {k, od, sp};
                return;
            }
        }
    }

    inline const OrderEntry* find(int32_t k) const {
        uint32_t h = ((uint32_t)k * 2654435761u) & mask;
        for (uint32_t i = 0; i < cap; i++, h = (h + 1) & mask) {
            if (entries[h].key == k)    return &entries[h];
            if (entries[h].key == EMPTY) return nullptr;
        }
        return nullptr;
    }
};

// Thread-local aggregation map: orderkey → {orderdate, shippriority, revenue}
struct AggEntry {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
    double  revenue;
};

struct AggMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t cap, mask;
    AggEntry* entries;

    explicit AggMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        entries = new AggEntry[cap];
        for (uint32_t i = 0; i < cap; i++) entries[i].key = EMPTY;
    }
    ~AggMap() { delete[] entries; }

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
        data = new uint64_t[BITS / 64]();
    }
    ~BloomFilter() { delete[] data; }

    inline void insert(int32_t k) {
        uint32_t u = (uint32_t)k;
        uint32_t h1 = u * 2654435761u;
        uint32_t h2 = u * 2246822519u;
        uint32_t h3 = u * 3266489917u;
        data[(h1 & MASK) >> 6] |= 1ull << (h1 & 63);
        data[(h2 & MASK) >> 6] |= 1ull << (h2 & 63);
        data[(h3 & MASK) >> 6] |= 1ull << (h3 & 63);
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
static void* mmap_open(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    sz = (size_t)st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
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
    // Pre-built persistent hash indexes (avoid runtime construction)
    static constexpr uint32_t CUST_IDX_CAP  = 4194304u;
    static constexpr uint32_t CUST_IDX_MASK = CUST_IDX_CAP - 1;
    static constexpr uint32_t ORD_IDX_CAP   = 33554432u;
    static constexpr uint32_t ORD_IDX_MASK  = ORD_IDX_CAP - 1;

    size_t sz_cseg, sz_ckey;
    size_t sz_ocky, sz_odat, sz_okey, sz_ospr;
    size_t sz_lshp, sz_loky, sz_lprc, sz_ldsc, sz_dlut;
    size_t sz_ozm, sz_lzm;
    size_t sz_cust_idx, sz_ord_idx;

    const uint8_t*  c_seg  = nullptr;
    const int32_t*  c_key  = nullptr;
    const int32_t*  cust_idx = nullptr; // custkey_hash.bin: int32_t slots, empty=INT32_MIN
    const OrderEntry* ord_idx = nullptr; // orderkey_hash.bin: OrderEntry slots (key,odate,spr)
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

        // Load zone maps first (small, needed for selective prefetch)
        ozm_raw = (const char*)mmap_open(odir + "/indexes/orderdate_zonemap.bin", sz_ozm);
        lzm_raw = (const char*)mmap_open(ldir + "/indexes/shipdate_zonemap.bin",  sz_lzm);

        // Parse zone maps
        uint32_t o_nblk = *(const uint32_t*)ozm_raw;
        const ZoneMapEntry* o_zm = (const ZoneMapEntry*)(ozm_raw + sizeof(uint32_t));
        uint32_t l_nblk = *(const uint32_t*)lzm_raw;
        const ZoneMapEntry* l_zm = (const ZoneMapEntry*)(lzm_raw + sizeof(uint32_t));

        // Find orders scan boundary: first block where min_val >= DATE_THRESH
        // (orders sorted ascending by orderdate)
        uint32_t o_end_blk = o_nblk;
        for (uint32_t b = 0; b < o_nblk; b++) {
            if (o_zm[b].min_val >= DATE_THRESH) { o_end_blk = b; break; }
        }

        // Find lineitem scan start: first block where max_val > DATE_THRESH
        uint32_t l_st_blk = l_nblk;
        for (uint32_t b = 0; b < l_nblk; b++) {
            if (l_zm[b].max_val > DATE_THRESH) { l_st_blk = b; break; }
        }

        // mmap pre-built persistent hash indexes first (small reads, avoid runtime builds)
        cust_idx = (const int32_t*)  mmap_open(cdir + "/indexes/custkey_hash.bin",   sz_cust_idx);
        ord_idx  = (const OrderEntry*)mmap_open(odir + "/indexes/orderkey_hash.bin",  sz_ord_idx);

        // mmap all columns
        c_seg  = (const uint8_t*) mmap_open(cdir + "/c_mktsegment.bin",      sz_cseg);
        c_key  = (const int32_t*) mmap_open(cdir + "/c_custkey.bin",         sz_ckey);
        o_cky  = (const int32_t*) mmap_open(odir + "/o_custkey.bin",         sz_ocky);
        o_dat  = (const int32_t*) mmap_open(odir + "/o_orderdate.bin",       sz_odat);
        o_key  = (const int32_t*) mmap_open(odir + "/o_orderkey.bin",        sz_okey);
        o_spr  = (const int32_t*) mmap_open(odir + "/o_shippriority.bin",    sz_ospr);
        l_shp  = (const int32_t*) mmap_open(ldir + "/l_shipdate.bin",        sz_lshp);
        l_oky  = (const int32_t*) mmap_open(ldir + "/l_orderkey.bin",        sz_loky);
        l_prc  = (const double*)  mmap_open(ldir + "/l_extendedprice.bin",   sz_lprc);
        l_dsc  = (const uint8_t*) mmap_open(ldir + "/l_discount.bin",        sz_ldsc);
        d_lut  = (const double*)  mmap_open(ldir + "/l_discount_lookup.bin", sz_dlut);

        // Selective madvise: parallel across column groups
        // Orders: prefetch only rows [0, o_end_row)
        uint32_t o_end_row = (o_end_blk < o_nblk) ? o_zm[o_end_blk].start_row
                                                    : (uint32_t)(sz_odat / sizeof(int32_t));
        size_t ord_byte_len = (size_t)o_end_row * sizeof(int32_t);

        // Lineitem: prefetch from l_start_row onward
        uint32_t l_start_row = (l_st_blk < l_nblk) ? l_zm[l_st_blk].start_row
                                                     : (uint32_t)(sz_lshp / sizeof(int32_t));
        size_t li_i32_off = (size_t)l_start_row * sizeof(int32_t);
        size_t li_i32_len = sz_lshp - li_i32_off;
        size_t li_dbl_off = (size_t)l_start_row * sizeof(double);
        size_t li_dbl_len = sz_lprc - li_dbl_off;
        size_t li_u8_off  = (size_t)l_start_row;
        size_t li_u8_len  = sz_ldsc - li_u8_off;

        #pragma omp parallel sections
        {
            #pragma omp section
            {
                madvise((void*)c_seg, sz_cseg, MADV_WILLNEED);
                madvise((void*)c_key, sz_ckey, MADV_WILLNEED);
            }
            #pragma omp section
            {
                if (ord_byte_len > 0) {
                    madvise((void*)o_cky, ord_byte_len, MADV_WILLNEED);
                    madvise((void*)o_dat, ord_byte_len, MADV_WILLNEED);
                    madvise((void*)o_key, ord_byte_len, MADV_WILLNEED);
                    madvise((void*)o_spr, ord_byte_len, MADV_WILLNEED);
                }
            }
            #pragma omp section
            {
                if (li_i32_len > 0) {
                    madvise((void*)((const char*)l_shp + li_i32_off), li_i32_len, MADV_WILLNEED);
                    madvise((void*)((const char*)l_oky + li_i32_off), li_i32_len, MADV_WILLNEED);
                    madvise((void*)((const char*)l_prc + li_dbl_off), li_dbl_len, MADV_WILLNEED);
                    madvise((void*)((const char*)l_dsc + li_u8_off),  li_u8_len,  MADV_WILLNEED);
                }
            }
            #pragma omp section
            {
                madvise((void*)d_lut, sz_dlut, MADV_WILLNEED);
            }
        }
    }

    // Re-parse zone map pointers (needed after data_loading scope)
    uint32_t o_nblk = *(const uint32_t*)ozm_raw;
    const ZoneMapEntry* o_zm = (const ZoneMapEntry*)(ozm_raw + sizeof(uint32_t));
    uint32_t l_nblk = *(const uint32_t*)lzm_raw;
    const ZoneMapEntry* l_zm = (const ZoneMapEntry*)(lzm_raw + sizeof(uint32_t));

    (void)sz_cseg; (void)sz_ckey; // customer columns still mmapped for madvise prefetch
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

    // ---- Phase 1: Customer filter — replaced by pre-built custkey_hash.bin ----
    // cust_idx is a pre-built hash set of BUILDING custkeys; probe directly, no runtime build.
    auto cust_contains = [&](int32_t k) -> bool {
        uint32_t h = ((uint32_t)k * 2654435761u) & CUST_IDX_MASK;
        for (uint32_t i = 0; i < CUST_IDX_CAP; i++, h = (h + 1) & CUST_IDX_MASK) {
            if (cust_idx[h] == k)        return true;
            if (cust_idx[h] == INT32_MIN) return false;
        }
        return false;
    };

    {
        GENDB_PHASE("dim_filter");
        // No-op: pre-built index replaces runtime CustKeySet construction.
    }

    // ---- Phase 2: Parallel orders scan — collect qualifying (okey, odate, shippriority) ----
    int nthreads = omp_get_max_threads();
    using OrdTuple = std::tuple<int32_t, int32_t, int32_t>; // (orderkey, orderdate, shippriority)
    std::vector<std::vector<OrdTuple>> thr_orders(nthreads);

    {
        GENDB_PHASE("build_joins");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& loc = thr_orders[tid];
            loc.reserve(32768);

            #pragma omp for schedule(dynamic, 16384)
            for (int64_t i = 0; i < (int64_t)o_end_row; i++) {
                if (o_dat[i] < DATE_THRESH && cust_contains(o_cky[i])) {
                    loc.emplace_back(o_key[i], o_dat[i], o_spr[i]);
                }
            }
        }
    }

    // Build a compact qualifying-orderkey hash set (int32_t only) from thr_orders.
    // ord_idx (orderkey_hash.bin) provides {orderdate, shippriority} via direct probe;
    // no runtime OrdersMap needed — eliminates building a 24MB+ structure at query time.
    size_t total_ord = 0;
    for (auto& v : thr_orders) total_ord += v.size();

    // Qualifying key set: next power-of-2 >= total_ord*2 for <=50% load
    uint32_t qks_cap = 8388608u;
    while (qks_cap < (uint32_t)(total_ord * 2)) qks_cap <<= 1;
    const uint32_t qks_mask = qks_cap - 1;
    int32_t* qual_keys = new int32_t[qks_cap];
    std::fill(qual_keys, qual_keys + qks_cap, INT32_MIN);

    {
        GENDB_PHASE("subquery_precompute");
        for (auto& v : thr_orders) {
            for (auto& [ok, od, sp] : v) {
                uint32_t h = ((uint32_t)ok * 2654435761u) & qks_mask;
                while (qual_keys[h] != INT32_MIN && qual_keys[h] != ok)
                    h = (h + 1) & qks_mask;
                qual_keys[h] = ok;
            }
        }
        // Free thread-local order buffers
        thr_orders.clear();
        std::vector<std::vector<OrdTuple>>().swap(thr_orders);
    }

    // ---- Phase 3: Parallel lineitem scan + thread-local aggregation ----
    // AGG_CAP: 8388608 slots → next power-of-2 >= 2×3.5M distinct qualifying orderkeys
    // <=50% load factor prevents silent entry drops in accumulate()
    constexpr uint32_t AGG_CAP = 8388608u;
    std::vector<AggMap*> thr_agg(nthreads);
    {
        GENDB_PHASE("agg_init");
        #pragma omp parallel for num_threads(nthreads) schedule(static)
        for (int t = 0; t < nthreads; t++) thr_agg[t] = new AggMap(AGG_CAP);
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            AggMap& lagg = *thr_agg[tid];

            #pragma omp for schedule(dynamic, 16384)
            for (int64_t i = (int64_t)l_start_row; i < (int64_t)n_li; i++) {
                if (l_shp[i] <= DATE_THRESH) continue;           // shipdate filter
                int32_t ok = l_oky[i];
                // Fast membership check in compact qualifying-key set
                uint32_t hq = ((uint32_t)ok * 2654435761u) & qks_mask;
                while (qual_keys[hq] != INT32_MIN && qual_keys[hq] != ok)
                    hq = (hq + 1) & qks_mask;
                if (qual_keys[hq] != ok) continue;               // not a qualifying order
                // Probe pre-built orderkey_hash.bin for {orderdate, shippriority}
                uint32_t ho = ((uint32_t)ok * 2654435761u) & ORD_IDX_MASK;
                while (ord_idx[ho].key != ok && ord_idx[ho].key != INT32_MIN)
                    ho = (ho + 1) & ORD_IDX_MASK;
                if (ord_idx[ho].key != ok) continue;             // safety guard
                const OrderEntry& oe = ord_idx[ho];
                double rev = l_prc[i] * (1.0 - d_lut[l_dsc[i]]);
                lagg.accumulate(ok, oe.orderdate, oe.shippriority, rev);
            }
        }
    }

    delete[] qual_keys; qual_keys = nullptr;

    // ---- Merge thread-local agg maps into final map ----
    // Final cap: 8388608 → matches AGG_CAP; accommodates full ~3.5M qualifying orderkeys
    constexpr uint32_t FINAL_CAP = 8388608u;
    AggMap final_agg(FINAL_CAP);

    {
        GENDB_PHASE("aggregation_merge");
        // Sequential merge: nthreads × 8388608 entries per thread-local map.
        // Each AggEntry is 20 bytes; scan skips EMPTY slots quickly.
        for (int t = 0; t < nthreads; t++) {
            AggMap& am = *thr_agg[t];
            for (uint32_t i = 0; i < AGG_CAP; i++) {
                if (am.entries[i].key != AggMap::EMPTY) {
                    auto& e = am.entries[i];
                    final_agg.accumulate(e.key, e.orderdate, e.shippriority, e.revenue);
                }
            }
            delete thr_agg[t];
            thr_agg[t] = nullptr;
        }
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
