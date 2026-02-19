#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ===========================================================================
// Zone map block layout: [uint32_t num_blocks] [int32_t min, int32_t max, uint32_t nrows] * num_blocks
// ===========================================================================
struct ZoneMapBlock {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_nrows;
};

// ===========================================================================
// Orders hash map: o_orderkey -> {o_orderdate, o_shippriority}
// Open addressing, linear probing, power-of-2 capacity
// ===========================================================================
struct OrderEntry {
    int32_t orderkey;      // INT32_MIN = empty sentinel
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersHashMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t  capacity;
    uint32_t  mask;
    OrderEntry* entries;

    void init(uint32_t cap) {
        capacity = cap;
        mask     = cap - 1;
        entries  = new OrderEntry[cap];
        for (uint32_t i = 0; i < cap; i++) entries[i].orderkey = EMPTY;
    }
    void destroy() { delete[] entries; }

    static inline uint32_t hash_key(int32_t key) {
        uint32_t x = (uint32_t)key;
        x ^= x >> 16;
        x *= 0x45d9f3bU;
        x ^= x >> 16;
        x *= 0x45d9f3bU;
        x ^= x >> 16;
        return x;
    }

    void insert(int32_t key, int32_t date, int32_t prio) {
        uint32_t h = hash_key(key) & mask;
        while (entries[h].orderkey != EMPTY) h = (h + 1) & mask;
        entries[h] = {key, date, prio};
    }

    inline const OrderEntry* find(int32_t key) const {
        uint32_t h = hash_key(key) & mask;
        while (true) {
            const OrderEntry& e = entries[h];
            if (__builtin_expect(e.orderkey == EMPTY, 0)) return nullptr;
            if (e.orderkey == key) return &e;
            h = (h + 1) & mask;
        }
    }
};

// ===========================================================================
// Bloom filter on qualifying o_orderkeys (built after orders scan)
// Uses double hashing: pos_i = (h1 + i*h2) & mask_bits
// 16M bits (2MB), 7 hash probes -> ~1% FP rate for 1.5M keys
// ===========================================================================
struct BloomFilter {
    static constexpr uint64_t BITS = 16ULL * 1024 * 1024;   // 16M bits
    static constexpr uint64_t MASK = BITS - 1;
    static constexpr uint32_t NHASHES = 7;
    uint64_t* data;                                           // BITS/64 words

    void init() {
        data = new uint64_t[BITS / 64]();
    }
    void destroy() { delete[] data; }

    static inline void hash2(int32_t key, uint64_t& h1, uint64_t& h2) {
        uint64_t k = (uint64_t)(uint32_t)key;
        h1 = k * 11400714819323198485ULL;
        h2 = k * 14029467366897019727ULL;
        h1 ^= h1 >> 33;
        h2 ^= h2 >> 33;
    }

    inline void set(int32_t key) {
        uint64_t h1, h2;
        hash2(key, h1, h2);
        for (uint32_t i = 0; i < NHASHES; i++) {
            uint64_t pos = (h1 + (uint64_t)i * h2) & MASK;
            data[pos >> 6] |= (1ULL << (pos & 63));
        }
    }

    inline bool test(int32_t key) const {
        uint64_t h1, h2;
        hash2(key, h1, h2);
        for (uint32_t i = 0; i < NHASHES; i++) {
            uint64_t pos = (h1 + (uint64_t)i * h2) & MASK;
            if (!(data[pos >> 6] & (1ULL << (pos & 63)))) return false;
        }
        return true;
    }
};

// ===========================================================================
// Aggregation hash map: l_orderkey -> {revenue, orderdate, shippriority}
// Open addressing, linear probing, power-of-2 capacity
// ===========================================================================
struct AggEntry {
    int32_t orderkey;      // INT32_MIN = empty
    int32_t orderdate;
    int32_t shippriority;
    double  revenue;
};

struct AggHashMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t  capacity;
    uint32_t  mask;
    AggEntry* entries;

    void init(uint32_t cap) {
        capacity = cap;
        mask     = cap - 1;
        entries  = new AggEntry[cap];
        for (uint32_t i = 0; i < cap; i++) entries[i].orderkey = EMPTY;
    }
    void destroy() { delete[] entries; }

    static inline uint32_t hash_key(int32_t key) {
        uint32_t x = (uint32_t)key;
        x ^= x >> 16;
        x *= 0x45d9f3bU;
        x ^= x >> 16;
        x *= 0x45d9f3bU;
        x ^= x >> 16;
        return x;
    }

    inline void accumulate(int32_t key, int32_t date, int32_t prio, double rev) {
        uint32_t h = hash_key(key) & mask;
        while (true) {
            AggEntry& e = entries[h];
            if (e.orderkey == EMPTY) {
                e = {key, date, prio, rev};
                return;
            }
            if (e.orderkey == key) {
                e.revenue += rev;
                return;
            }
            h = (h + 1) & mask;
        }
    }

    void merge_from(const AggHashMap& other) {
        for (uint32_t i = 0; i < other.capacity; i++) {
            const AggEntry& e = other.entries[i];
            if (e.orderkey != EMPTY) {
                accumulate(e.orderkey, e.orderdate, e.shippriority, e.revenue);
            }
        }
    }
};

// ===========================================================================
// Helper: mmap a typed column file
// ===========================================================================
template<typename T>
static const T* mmap_col(const std::string& path, size_t& nrows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    size_t sz = (size_t)st.st_size;
    nrows = sz / sizeof(T);
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return reinterpret_cast<const T*>(p);
}

static const void* mmap_raw(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    sz = (size_t)st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

// ===========================================================================
// Q3: Shipping Priority
// ===========================================================================
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // 1995-03-15 = epoch day 9204
    static constexpr int32_t DATE_THRESHOLD = 9204;

    // -----------------------------------------------------------------------
    // Phase 1: Customer scan — build custkey bitset (filter: c_mktsegment = 'BUILDING' = 1)
    // Bitset covers keys 0..1,500,000 → 187 KB → fits in L2
    // -----------------------------------------------------------------------
    static constexpr uint32_t CUST_MAX = 1500001;
    static constexpr uint32_t CUST_WORDS = (CUST_MAX + 63) / 64;
    uint64_t* cust_bitset = new uint64_t[CUST_WORDS]();

    {
        GENDB_PHASE("dim_filter");

        size_t n_cust;
        const int8_t*  c_mktsegment = mmap_col<int8_t> (gendb_dir + "/customer/c_mktsegment.bin", n_cust);
        size_t n_tmp;
        const int32_t* c_custkey    = mmap_col<int32_t>(gendb_dir + "/customer/c_custkey.bin",    n_tmp);

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> tlocal(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lv = tlocal[tid];
            lv.reserve(8192);

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_cust; i++) {
                if (c_mktsegment[i] == 1) {  // BUILDING
                    lv.push_back(c_custkey[i]);
                }
            }
        }

        // Sequential bitset population (avoids atomic ops)
        for (int t = 0; t < nthreads; t++) {
            for (int32_t ck : tlocal[t]) {
                uint32_t u = (uint32_t)ck;
                cust_bitset[u >> 6] |= (1ULL << (u & 63));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Orders scan (zone-map pruned) — build hash map + bloom filter
    // Filter: o_orderdate < 9204 AND o_custkey in cust_bitset
    // -----------------------------------------------------------------------
    // orders hash map: capacity = next power of 2 >= 2 * 1.5M = 3M → 4M (2^22)
    OrdersHashMap orders_map;
    orders_map.init(1u << 22);   // 4,194,304 slots × 12 bytes = ~50MB

    BloomFilter bloom;
    bloom.init();

    {
        GENDB_PHASE("build_joins");

        size_t n_ord;
        const int32_t* o_orderkey     = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin",     n_ord);
        size_t n_tmp;
        const int32_t* o_custkey      = mmap_col<int32_t>(gendb_dir + "/orders/o_custkey.bin",      n_tmp);
        const int32_t* o_orderdate    = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin",    n_tmp);
        const int32_t* o_shippriority = mmap_col<int32_t>(gendb_dir + "/orders/o_shippriority.bin", n_tmp);

        // Load zone map
        size_t zm_sz;
        const void* zm_raw = mmap_raw(gendb_dir + "/indexes/orders_orderdate_zonemap.bin", zm_sz);
        const uint32_t    num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapBlock* zm       = reinterpret_cast<const ZoneMapBlock*>(
            reinterpret_cast<const uint8_t*>(zm_raw) + sizeof(uint32_t));

        // Precompute row offsets (cumulative sum of block_nrows)
        std::vector<uint32_t> row_off(num_blocks + 1);
        row_off[0] = 0;
        for (uint32_t b = 0; b < num_blocks; b++)
            row_off[b+1] = row_off[b] + zm[b].block_nrows;

        // Parallel scan — collect qualifying rows thread-locally
        int nthreads = omp_get_max_threads();
        struct QualOrd { int32_t okey, odate, oprio; };
        std::vector<std::vector<QualOrd>> tlocal(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lv = tlocal[tid];
            lv.reserve(32768);

            #pragma omp for schedule(dynamic, 2)
            for (uint32_t b = 0; b < num_blocks; b++) {
                // Zone-map skip: orders sorted by o_orderdate
                // If entire block has min >= threshold, no row qualifies
                if (zm[b].min_val >= DATE_THRESHOLD) continue;

                uint32_t rbase = row_off[b];
                uint32_t nrows = zm[b].block_nrows;

                for (uint32_t r = 0; r < nrows; r++) {
                    uint32_t idx = rbase + r;
                    int32_t odate = o_orderdate[idx];
                    if (odate >= DATE_THRESHOLD) continue;
                    int32_t ock = o_custkey[idx];
                    uint32_t u = (uint32_t)ock;
                    if (!(cust_bitset[u >> 6] & (1ULL << (u & 63)))) continue;
                    lv.push_back({o_orderkey[idx], odate, o_shippriority[idx]});
                }
            }
        }

        // Sequential hash map + bloom filter build
        for (int t = 0; t < nthreads; t++) {
            for (const auto& q : tlocal[t]) {
                orders_map.insert(q.okey, q.odate, q.oprio);
                bloom.set(q.okey);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Lineitem scan (zone-map pruned) — bloom pre-filter + hash probe + aggregate
    // Filter: l_shipdate > 9204
    // -----------------------------------------------------------------------
    int nthreads = omp_get_max_threads();
    std::vector<AggHashMap> tagg(nthreads);
    for (int t = 0; t < nthreads; t++)
        tagg[t].init(8192);   // ~3K groups expected, 8192 = next power of 2

    {
        GENDB_PHASE("main_scan");

        size_t n_li;
        const int32_t* l_orderkey      = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",      n_li);
        size_t n_tmp;
        const double*  l_extendedprice = mmap_col<double> (gendb_dir + "/lineitem/l_extendedprice.bin", n_tmp);
        const double*  l_discount      = mmap_col<double> (gendb_dir + "/lineitem/l_discount.bin",      n_tmp);
        const int32_t* l_shipdate      = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",      n_tmp);

        // Load zone map
        size_t zm_sz;
        const void* zm_raw = mmap_raw(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zm_sz);
        const uint32_t    num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapBlock* zm       = reinterpret_cast<const ZoneMapBlock*>(
            reinterpret_cast<const uint8_t*>(zm_raw) + sizeof(uint32_t));

        // Precompute row offsets
        std::vector<uint32_t> row_off(num_blocks + 1);
        row_off[0] = 0;
        for (uint32_t b = 0; b < num_blocks; b++)
            row_off[b+1] = row_off[b] + zm[b].block_nrows;

        // Capture references for lambda
        const OrdersHashMap& om  = orders_map;
        const BloomFilter&   bf  = bloom;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggHashMap& lagg = tagg[tid];

            #pragma omp for schedule(dynamic, 3)
            for (uint32_t b = 0; b < num_blocks; b++) {
                // Zone-map skip: lineitem sorted by l_shipdate
                // If entire block has max <= threshold, no row has shipdate > threshold
                if (zm[b].max_val <= DATE_THRESHOLD) continue;

                uint32_t rbase = row_off[b];
                uint32_t nrows = zm[b].block_nrows;

                for (uint32_t r = 0; r < nrows; r++) {
                    uint32_t idx = rbase + r;
                    if (l_shipdate[idx] <= DATE_THRESHOLD) continue;
                    int32_t lkey = l_orderkey[idx];
                    // Bloom pre-filter (eliminates ~90% of non-matching keys)
                    if (!bf.test(lkey)) continue;
                    // Hash map probe
                    const OrderEntry* oe = om.find(lkey);
                    if (!oe) continue;
                    // Accumulate revenue = l_extendedprice * (1 - l_discount)
                    double rev = l_extendedprice[idx] * (1.0 - l_discount[idx]);
                    lagg.accumulate(lkey, oe->orderdate, oe->shippriority, rev);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Merge thread-local aggregation maps
    // -----------------------------------------------------------------------
    AggHashMap global_agg;
    global_agg.init(65536);  // generous: 64K slots for ~3K groups

    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; t++) {
            global_agg.merge_from(tagg[t]);
            tagg[t].destroy();
        }
    }

    // -----------------------------------------------------------------------
    // Phase 5: Top-10 sort and CSV output
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Collect all non-empty aggregation entries
        std::vector<AggEntry> results;
        results.reserve(4096);
        for (uint32_t i = 0; i < global_agg.capacity; i++) {
            if (global_agg.entries[i].orderkey != AggHashMap::EMPTY)
                results.push_back(global_agg.entries[i]);
        }

        // Partial sort: top-10 by revenue DESC, o_orderdate ASC
        size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const AggEntry& a, const AggEntry& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        // Write CSV output
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (size_t i = 0; i < k; i++) {
            const AggEntry& e = results[i];
            gendb::epoch_days_to_date_str(e.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", e.orderkey, e.revenue, date_buf, e.shippriority);
        }
        fclose(f);
    }

    // Cleanup
    delete[] cust_bitset;
    orders_map.destroy();
    bloom.destroy();
    global_agg.destroy();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
