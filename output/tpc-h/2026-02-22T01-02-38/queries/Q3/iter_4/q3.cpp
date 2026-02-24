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
static constexpr int32_t EMPTY_KEY      = INT32_MIN;

// ---------------------------------------------------------------------------
// Hash function
// ---------------------------------------------------------------------------
inline uint32_t hash32(int32_t k) {
    return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> 32);
}

// ---------------------------------------------------------------------------
// Open-addressing hash set for int32 custkeys (~311K, cap=524288)
// ---------------------------------------------------------------------------
struct CustSet {
    int32_t* keys = nullptr;
    uint32_t mask = 0;

    void init(uint32_t cap) {
        keys = new int32_t[cap];
        std::fill(keys, keys + cap, EMPTY_KEY);
        mask = cap - 1;
    }

    void insert(int32_t k) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (keys[s] == EMPTY_KEY || keys[s] == k) { keys[s] = k; return; }
        }
    }

    bool has(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (keys[s] == EMPTY_KEY) return false;
            if (keys[s] == k) return true;
        }
        return false;
    }

    ~CustSet() { delete[] keys; }
};

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
        for (uint32_t i = 0; i < cap; i++) slots[i].key = EMPTY_KEY;
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
// Hit record collected during lineitem scan (per-thread vectors)
// orderdate/shippriority omitted — looked up from OrdMap at output
// ---------------------------------------------------------------------------
struct Hit {
    double  revenue;       // 8 bytes
    int32_t orderkey;      // 4 bytes
    int32_t _pad;          // 4 bytes → 16 bytes total
};

// ---------------------------------------------------------------------------
// Global aggregation hash map — 16B entries (no Kahan, no date/priority)
// orderdate/shippriority looked up from OrdMap at top-10 extraction
// ---------------------------------------------------------------------------
struct AggEntry {
    int32_t orderkey;      // 4 bytes, EMPTY_KEY if empty
    int32_t _pad;          // 4 bytes → 16 bytes total
    double  revenue;       // 8 bytes, running sum
};

struct AggMap {
    AggEntry* slots = nullptr;
    uint32_t  mask  = 0;

    void init(uint32_t cap) {
        slots = new AggEntry[cap];
        for (uint32_t i = 0; i < cap; i++) slots[i].orderkey = EMPTY_KEY;
        mask = cap - 1;
    }

    void update(int32_t k, double rev) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p <= mask; p++) {
            uint32_t s = (h + p) & mask;
            if (slots[s].orderkey == EMPTY_KEY) {
                slots[s].orderkey = k;
                slots[s].revenue  = rev;
                return;
            }
            if (slots[s].orderkey == k) {
                slots[s].revenue += rev;
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

        // Selective madvise: customer columns (small, needed first — 7.5MB total)
        madvise((void*)c_mktseg_col,  c_nrows * sizeof(uint8_t),  MADV_WILLNEED);
        madvise((void*)c_custkey_col, c_nrows * sizeof(int32_t),  MADV_WILLNEED);

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

    // ---- Phase 1: Filter customers → custkey hash set (parallel CAS insert) ----
    CustSet cust_set;
    {
        GENDB_PHASE("dim_filter");
        cust_set.init(524288); // 512K for ~311K keys — each c_custkey is unique

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < c_nrows; i++) {
            if (c_mktseg_col[i] != building_code) continue;
            int32_t k = c_custkey_col[i];
            uint32_t h = hash32(k) & cust_set.mask;
            for (uint32_t p = 0; ; p++) {
                uint32_t s = (h + p) & cust_set.mask;
                int32_t exp = EMPTY_KEY;
                if (__atomic_compare_exchange_n(&cust_set.keys[s], &exp, k,
                        false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) break;
                if (__atomic_load_n(&cust_set.keys[s], __ATOMIC_RELAXED) == k) break;
            }
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
                    if (!cust_set.has(o_custkey_col[r])) continue;
                    loc.push_back({o_orderkey_col[r], o_orderdate_col[r], o_shippr_col[r]});
                }
            }
        }

        ord_map.init(2097152); // 2M slots for ~735K keys
        bloom.init(1048576);   // 1MB = 8M bits

        // Serial insertion — eliminates CAS contention; ~735K inserts at ~3ns each ≈ 2ms
        for (int t = 0; t < nt; t++) {
            for (auto& tup : tlocal[t]) {
                bloom.add(tup.ok);
                ord_map.insert(tup.ok, tup.od, tup.sp);
            }
            tlocal[t].clear();
            tlocal[t].shrink_to_fit();
        }
    }

    // ---- Phase 3: Lineitem zone-map scan → partitioned hit dispatch ----
    // P=32 partitions keyed by hash(orderkey) & 31.  Each partition holds hits from all threads.
    // Partition p's AggMap has ~735K/32 ≈ 23K keys → 4MB per map → fits in L3 cache.
    static constexpr int P = 32;
    static constexpr int SCAN_THREADS = 64;
    // pth_hits[p * SCAN_THREADS + t] = hits for partition p produced by thread t
    std::vector<std::vector<Hit>> pth_hits((size_t)P * SCAN_THREADS);

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
            // Reserve per-partition buffers for this thread
            for (int p = 0; p < P; p++)
                pth_hits[(size_t)p * SCAN_THREADS + tid].reserve(2048);

            #pragma omp for schedule(dynamic, 1)
            for (uint32_t b = lineitem_start_block; b < li_nb; b++) {
                uint32_t rs = b * 100000u;
                uint32_t re = std::min(rs + 100000u, li_nr);

                for (uint32_t r = rs; r < re; r++) {
                    if (li_sd[r] <= DATE_THRESHOLD) continue;

                    int32_t ok = li_ok[r];
                    if (!bloom.test(ok)) continue;

                    if (!ord_map.find(ok)) continue;

                    double rev = li_ep[r] * (1.0 - li_disc[r]);
                    int part = (int)(hash32(ok) & 31u);
                    pth_hits[(size_t)part * SCAN_THREADS + tid].push_back({rev, ok, 0});
                }
            }
        }
    }

    // ---- Phase 4: Parallel partitioned Kahan aggregation + top-10 ----
    {
        GENDB_PHASE("aggregation_merge");

        // 32 AggMaps — 16B entries, 64K slots each → 32×64K×16B=32MB total (fits in 44MB L3)
        AggMap agg_parts[P];

        #pragma omp parallel for schedule(static) num_threads(P)
        for (int p = 0; p < P; p++) {
            agg_parts[p].init(65536); // 64K slots; ~23K keys → 35% load
            for (int t = 0; t < SCAN_THREADS; t++) {
                auto& vec = pth_hits[(size_t)p * SCAN_THREADS + t];
                for (auto& h : vec)
                    agg_parts[p].update(h.orderkey, h.revenue);
                vec.clear();
                vec.shrink_to_fit();
            }
        }

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
                const OrdEntry* oe = ord_map.find(e.orderkey);
                Result r = {e.orderkey, oe->orderdate, oe->shippriority, e.revenue};

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
