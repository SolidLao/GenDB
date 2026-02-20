// Q3: Shipping Priority — iter_4
// Key fixes vs iter_3:
//   1. No MAP_POPULATE — critical: avoided 300s HDD timeout (MAP_POPULATE forces
//      synchronous page loading; on cold HDD ~288s for 1.44 GB lineitem at 5 MB/s).
//      Instead: posix_fadvise(SEQUENTIAL) + madvise(MADV_SEQUENTIAL) for OS read-ahead.
//   2. CustBitset (188 KB, L2): replaces hash set for custkey membership test.
//   3. OrdBitset (~7.5 MB, L3): replaces 48 MB OrdMap probe in the inner lineitem loop.
//      48 MB > 44 MB L3 → every probe was a cache miss; bitset eliminates this.
//   4. Sequential lineitem scan (not index-driven) — correct for HDD.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <climits>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

static const int32_t EMPTY_KEY   = INT32_MIN;
static const int32_t DATE_CUTOFF = 9204; // epoch days for 1995-03-15

static inline uint32_t hash32(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> 32);
}

// ── mmap helpers: no MAP_POPULATE ────────────────────────────────────────────
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); n = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* pm = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (pm == MAP_FAILED) { n = 0; return nullptr; }
    madvise(pm, st.st_size, MADV_SEQUENTIAL);
    n = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(pm);
}

static const uint8_t* mmap_raw(const std::string& path, size_t& bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); bytes = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    bytes = st.st_size;
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* pm = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (pm == MAP_FAILED) { bytes = 0; return nullptr; }
    return reinterpret_cast<const uint8_t*>(pm);
}

// ── CustBitset: custkeys 1..1,500,000 → 188 KB (fits in L2 cache) ────────────
struct CustBitset {
    static const uint32_t MAX_CK = 1500002u;
    static const uint32_t WORDS  = (MAX_CK + 63) / 64;
    uint64_t* bits;
    CustBitset() : bits(new uint64_t[WORDS]()) {}
    ~CustBitset() { delete[] bits; }
    void set(int32_t k) {
        if (k > 0 && (uint32_t)k < MAX_CK) bits[(uint32_t)k >> 6] |= (1ULL << ((uint32_t)k & 63));
    }
    bool test(int32_t k) const {
        uint32_t u = (uint32_t)k;
        return u < MAX_CK && ((bits[u >> 6] >> (u & 63)) & 1);
    }
};

// ── OrdSlot: qualifying order metadata ────────────────────────────────────────
struct OrdSlot { int32_t key, orderdate, shippriority; };

// ── OrdMap: hash map orderkey → {orderdate, shippriority}
//    ~1.47M entries, cap = 4M → 37% load. Used post-aggregation for metadata lookup.
struct OrdMap {
    uint32_t cap, mask;
    OrdSlot* slots;
    OrdMap(uint32_t c) : cap(c), mask(c - 1), slots(new OrdSlot[c]) {
        for (uint32_t i = 0; i < c; ++i) slots[i].key = EMPTY_KEY;
    }
    ~OrdMap() { delete[] slots; }
    void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & mask;
        while (slots[h].key != EMPTY_KEY && slots[h].key != k) h = (h + 1) & mask;
        if (slots[h].key == EMPTY_KEY) slots[h] = {k, od, sp};
    }
    const OrdSlot* find(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        while (slots[h].key != EMPTY_KEY && slots[h].key != k) h = (h + 1) & mask;
        return slots[h].key == k ? &slots[h] : nullptr;
    }
};

// ── OrdBitset: qualifying orderkeys → ~7.5 MB for SF10, fits in L3 (44 MB)
//    Replaces OrdMap.find() in the inner lineitem loop — eliminates L3 cache misses.
struct OrdBitset {
    uint32_t  max_key = 0;
    uint32_t  words   = 0;
    uint64_t* bits    = nullptr;
    ~OrdBitset() { delete[] bits; }

    void build(const OrdMap& m) {
        // Find max qualifying orderkey
        for (uint32_t i = 0; i < m.cap; ++i)
            if (m.slots[i].key != EMPTY_KEY && (uint32_t)m.slots[i].key > max_key)
                max_key = (uint32_t)m.slots[i].key;
        words = (max_key >> 6) + 2;
        bits  = new uint64_t[words]();
        for (uint32_t i = 0; i < m.cap; ++i)
            if (m.slots[i].key != EMPTY_KEY) {
                uint32_t u = (uint32_t)m.slots[i].key;
                bits[u >> 6] |= (1ULL << (u & 63));
            }
    }

    inline bool test(int32_t k) const {
        uint32_t u = (uint32_t)k;
        if (u > max_key) return false;
        return (bits[u >> 6] >> (u & 63)) & 1;
    }
};

// ── AggMap: thread-local aggregation, inline slots → L1/L2 cached
//    ~3000 groups, 8192 slots → 37% load, 8192 × 12 = 96 KB per thread
struct AggSlot { int32_t key; double revenue; };
struct AggMap {
    static const uint32_t CAP = 1u << 13; // 8192
    AggSlot slots[CAP];
    AggMap() {
        for (uint32_t i = 0; i < CAP; ++i) { slots[i].key = EMPTY_KEY; slots[i].revenue = 0.0; }
    }
    void add(int32_t k, double rev) {
        uint32_t h = hash32(k) & (CAP - 1);
        while (slots[h].key != EMPTY_KEY && slots[h].key != k) h = (h + 1) & (CAP - 1);
        if (slots[h].key == EMPTY_KEY) { slots[h].key = k; slots[h].revenue  = rev; }
        else                            {                    slots[h].revenue += rev; }
    }
};

struct ZMBlock { int32_t min_val, max_val; uint32_t block_size; };

// ── Main query ────────────────────────────────────────────────────────────────
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================================
    // Phase 1: Scan customer → BUILDING custkey bitset (188 KB, L2 cached)
    // =========================================================================
    CustBitset* cust_bits = nullptr;
    {
        GENDB_PHASE("phase1_customer");

        // Determine BUILDING code from dict at runtime
        int8_t building_code = 1; // per Query Guide: code 1 = "BUILDING"
        {
            std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line; int8_t code = 0;
            while (std::getline(df, line)) {
                if (line == "BUILDING") { building_code = code; break; }
                ++code;
            }
        }

        size_t n_seg = 0, n_ck = 0;
        const auto* seg  = mmap_col<int8_t> (gendb_dir + "/customer/c_mktsegment.bin", n_seg);
        const auto* ckey = mmap_col<int32_t>(gendb_dir + "/customer/c_custkey.bin",    n_ck);
        size_t n = std::min(n_seg, n_ck);

        cust_bits = new CustBitset();
        for (size_t i = 0; i < n; ++i)
            if (seg[i] == building_code) cust_bits->set(ckey[i]);

        munmap((void*)seg,  n_seg * sizeof(int8_t));
        munmap((void*)ckey, n_ck  * sizeof(int32_t));
    }

    // =========================================================================
    // Phase 2: Parallel orders scan with zone map → OrdMap + OrdBitset
    //   Filter: o_orderdate < 9204 AND o_custkey ∈ cust_bits
    // =========================================================================
    OrdMap*    ord_map  = new OrdMap(1u << 22); // 4M slots, 37% load for 1.47M orders
    OrdBitset* ord_bits = new OrdBitset();
    {
        GENDB_PHASE("phase2_orders");

        // Load orderdate zone map for block pruning
        size_t zm_bytes = 0;
        const uint8_t* zm_raw = mmap_raw(gendb_dir + "/indexes/orders_orderdate_zonemap.bin", zm_bytes);
        uint32_t num_blocks = 0;
        const ZMBlock* zm = nullptr;
        if (zm_raw && zm_bytes > 4) {
            num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
            zm         = reinterpret_cast<const ZMBlock*>(zm_raw + sizeof(uint32_t));
        }

        size_t n_rows = 0, tmp = 0;
        const auto* o_custkey      = mmap_col<int32_t>(gendb_dir + "/orders/o_custkey.bin",      n_rows);
        const auto* o_orderdate    = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin",    tmp);
        const auto* o_orderkey     = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin",     tmp);
        const auto* o_shippriority = mmap_col<int32_t>(gendb_dir + "/orders/o_shippriority.bin", tmp);

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<OrdSlot>> tvecs(nthreads);
        for (auto& v : tvecs) v.reserve(32768);

        static const size_t BLOCK_SZ = 100000;

        if (zm && num_blocks > 0) {
            #pragma omp parallel for schedule(dynamic, 2) num_threads(nthreads)
            for (uint32_t b = 0; b < num_blocks; ++b) {
                if (zm[b].min_val >= DATE_CUTOFF) continue; // entire block: dates ≥ cutoff
                int tid = omp_get_thread_num();
                auto& lv = tvecs[tid];
                size_t rs = (size_t)b * BLOCK_SZ;
                size_t re = std::min(rs + BLOCK_SZ, n_rows);
                for (size_t i = rs; i < re; ++i) {
                    if (o_orderdate[i] < DATE_CUTOFF && cust_bits->test(o_custkey[i]))
                        lv.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                }
            }
        } else {
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n_rows; ++i) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_bits->test(o_custkey[i]))
                    tvecs[omp_get_thread_num()].push_back(
                        {o_orderkey[i], o_orderdate[i], o_shippriority[i]});
            }
        }

        // Sequential merge into OrdMap (~1.47M inserts)
        for (int t = 0; t < nthreads; ++t)
            for (const auto& s : tvecs[t])
                ord_map->insert(s.key, s.orderdate, s.shippriority);

        // Build orderkey bitset from OrdMap (used for fast inner-loop filter)
        ord_bits->build(*ord_map);

        munmap((void*)o_custkey,      n_rows * sizeof(int32_t));
        munmap((void*)o_orderdate,    n_rows * sizeof(int32_t));
        munmap((void*)o_orderkey,     n_rows * sizeof(int32_t));
        munmap((void*)o_shippriority, n_rows * sizeof(int32_t));
        if (zm_raw) munmap((void*)zm_raw, zm_bytes);
        delete cust_bits; cust_bits = nullptr;
    }

    // =========================================================================
    // Phase 3: Parallel sequential lineitem scan → thread-local revenue aggregation
    //   Key: ord_bits->test() hits L3 cache (7.5 MB bitset vs 48 MB hash map).
    //   Sequential scan essential for HDD; no MAP_POPULATE avoids 300s timeout.
    // =========================================================================
    int nthreads = omp_get_max_threads();
    std::vector<AggMap> taggs(nthreads);

    {
        GENDB_PHASE("phase3_lineitem");

        // Shipdate zone map for lineitem block pruning
        size_t lzm_bytes = 0;
        const uint8_t* lzm_raw = mmap_raw(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", lzm_bytes);
        uint32_t num_lzm = 0;
        const ZMBlock* lzm = nullptr;
        if (lzm_raw && lzm_bytes > 4) {
            num_lzm = *reinterpret_cast<const uint32_t*>(lzm_raw);
            lzm     = reinterpret_cast<const ZMBlock*>(lzm_raw + sizeof(uint32_t));
        }

        size_t n_li = 0, tmp = 0;
        const auto* l_orderkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",      n_li);
        const auto* l_shipdate = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",      tmp);
        const auto* l_extprice = mmap_col<double>  (gendb_dir + "/lineitem/l_extendedprice.bin", tmp);
        const auto* l_discount = mmap_col<double>  (gendb_dir + "/lineitem/l_discount.bin",     tmp);
        static const size_t LB = 100000;

        const OrdBitset& ob = *ord_bits;

        if (lzm && num_lzm > 0) {
            // Collect active blocks (skip where all shipdates ≤ DATE_CUTOFF)
            std::vector<uint32_t> active;
            active.reserve(num_lzm);
            for (uint32_t b = 0; b < num_lzm; ++b)
                if (lzm[b].max_val > DATE_CUTOFF) active.push_back(b);

            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (int bi = 0; bi < (int)active.size(); ++bi) {
                uint32_t b = active[bi];
                int tid = omp_get_thread_num();
                AggMap& la = taggs[tid];
                size_t rs = (size_t)b * LB;
                size_t re = std::min(rs + LB, n_li);
                bool all_qualify = (lzm[b].min_val > DATE_CUTOFF); // skip per-row date check

                for (size_t i = rs; i < re; ++i) {
                    if (!all_qualify && l_shipdate[i] <= DATE_CUTOFF) continue;
                    int32_t lk = l_orderkey[i];
                    if (!ob.test(lk)) continue;   // bitset: L3 cached, no cache miss storm
                    double rev = l_extprice[i] * (1.0 - l_discount[i]);
                    la.add(lk, rev);
                }
            }
        } else {
            // Fallback: no zone map — full sequential scan
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n_li; ++i) {
                if (l_shipdate[i] <= DATE_CUTOFF) continue;
                int32_t lk = l_orderkey[i];
                if (!ob.test(lk)) continue;
                double rev = l_extprice[i] * (1.0 - l_discount[i]);
                taggs[omp_get_thread_num()].add(lk, rev);
            }
        }

        munmap((void*)l_orderkey, n_li * sizeof(int32_t));
        munmap((void*)l_shipdate, n_li * sizeof(int32_t));
        munmap((void*)l_extprice, n_li * sizeof(double));
        munmap((void*)l_discount, n_li * sizeof(double));
        if (lzm_raw) munmap((void*)lzm_raw, lzm_bytes);
    }

    // =========================================================================
    // Phase 4: Merge thread-local agg maps → global → top-10 → CSV
    // =========================================================================
    {
        GENDB_PHASE("phase4_output");

        // Global agg: same 8192 slots (3000 groups → 37% load)
        AggMap global_agg;
        for (int t = 0; t < nthreads; ++t)
            for (uint32_t h = 0; h < AggMap::CAP; ++h)
                if (taggs[t].slots[h].key != EMPTY_KEY)
                    global_agg.add(taggs[t].slots[h].key, taggs[t].slots[h].revenue);

        struct Row { int32_t orderkey; double revenue; int32_t orderdate, shippriority; };
        std::vector<Row> rows;
        rows.reserve(4096);
        for (uint32_t h = 0; h < AggMap::CAP; ++h) {
            if (global_agg.slots[h].key == EMPTY_KEY) continue;
            int32_t ok = global_agg.slots[h].key;
            const OrdSlot* os = ord_map->find(ok);
            if (!os) continue;
            rows.push_back({ok, global_agg.slots[h].revenue, os->orderdate, os->shippriority});
        }

        auto cmp = [](const Row& a, const Row& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        };
        size_t top_k = std::min<size_t>(10, rows.size());
        std::partial_sort(rows.begin(), rows.begin() + top_k, rows.end(), cmp);

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); goto cleanup; }
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (size_t i = 0; i < top_k; ++i) {
            const Row& r = rows[i];
            char date_buf[16];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, date_buf, r.shippriority);
        }
        fclose(f);
    }

cleanup:
    delete ord_map;
    delete ord_bits;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    run_q3(argv[1], argc > 2 ? argv[2] : ".");
    return 0;
}
#endif
