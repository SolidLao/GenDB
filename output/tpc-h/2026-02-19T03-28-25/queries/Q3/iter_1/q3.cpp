// Q3: Shipping Priority — GenDB hand-tuned implementation, iteration 0
//
// Strategy:
//   1. Load customer c_mktsegment dict → find BUILDING code → scan c_mktsegment +
//      c_custkey, build bitset[c_custkey] (188KB, L2-resident).
//   2. Scan all 15M orders rows in parallel, filter o_orderdate < 9247 AND
//      bitset[o_custkey]; collect qualifying (orderkey, orderdate, shippriority)
//      tuples in thread-local vectors, then sequentially build a single
//      open-addressing hash table + bloom filter (2MB, 16M bits, k=7).
//      Note: Zone maps show every block spans the full date range → no pruning.
//   3. Scan all 60M lineitem rows in parallel (64 threads, morsel=100K),
//      filter l_shipdate > 9247, check bloom filter, probe orders hash table,
//      accumulate revenue in thread-local aggregation hash tables.
//   4. Merge thread-local agg tables into a global agg table sequentially.
//   5. Min-heap of size 10 to extract top-10 by (revenue DESC, o_orderdate ASC).
//   6. Write results CSV.
//
// Deviation from plan: Zone maps provide zero block-skipping (all blocks span
// full date range for both orders and lineitem); full scans are performed instead.

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <queue>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ============================================================
// Constants
// Note: Query Guide states 9247 for 1995-03-15, but the actual epoch value
// (days since 1970-01-01) is 9204. We compute at runtime for correctness.
// ============================================================
static int32_t DATE_THRESHOLD;  // set in run_Q3 via date_str_to_epoch_days("1995-03-15")

// ============================================================
// Customer Bitset — 1,500,001 bits, ~188KB, fits in L2
// ============================================================
static constexpr uint32_t CUST_BITSET_WORDS = (1500002 + 63) / 64;
static uint64_t cust_bitset[CUST_BITSET_WORDS];

inline void bitset_set(uint32_t idx) {
    cust_bitset[idx >> 6] |= (1ULL << (idx & 63));
}
inline bool bitset_test(uint32_t idx) {
    return (cust_bitset[idx >> 6] >> (idx & 63)) & 1ULL;
}

// ============================================================
// Orders Hash Table — open-addressing, 2M slots, 24MB, fits in L3
// ============================================================
static constexpr uint32_t OHT_BITS = 22;  // 4M slots for safety with ~1.35M entries
static constexpr uint32_t OHT_SIZE = 1u << OHT_BITS;
static constexpr uint32_t OHT_MASK = OHT_SIZE - 1u;

struct OrderSlot {
    int32_t orderkey;     // 0 = empty sentinel
    int32_t orderdate;
    int32_t shippriority;
};

static OrderSlot* orders_ht;

inline void oht_insert(int32_t key, int32_t date, int32_t prio) {
    uint32_t slot = ((uint32_t)key * 2654435761u) & OHT_MASK;
    while (__builtin_expect(orders_ht[slot].orderkey != 0, 0))
        slot = (slot + 1) & OHT_MASK;
    orders_ht[slot] = {key, date, prio};
}

inline const OrderSlot* oht_find(int32_t key) {
    uint32_t slot = ((uint32_t)key * 2654435761u) & OHT_MASK;
    while (true) {
        const OrderSlot& s = orders_ht[slot];
        if (s.orderkey == key) return &s;
        if (s.orderkey == 0) return nullptr;
        slot = (slot + 1) & OHT_MASK;
    }
}

// ============================================================
// Bloom Filter — 16M bits = 2MB, k=7, ~1.8% FP rate for 1.35M keys
// Eliminates ~98% of non-matching lineitem → hash-table probes
// ============================================================
static constexpr uint32_t BLOOM_BITS = 1u << 24;  // 16M bits
static constexpr uint32_t BLOOM_MASK = BLOOM_BITS - 1u;
static uint8_t bloom_data[BLOOM_BITS / 8];  // 2MB

inline void bloom_insert(int32_t key) {
    uint32_t h = (uint32_t)key * 2654435761u;
    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < 7; i++) {
        uint32_t bit = h & BLOOM_MASK;
        bloom_data[bit >> 3] |= (uint8_t)(1u << (bit & 7));
        h += delta;
    }
}

inline bool bloom_test(int32_t key) {
    uint32_t h = (uint32_t)key * 2654435761u;
    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < 7; i++) {
        uint32_t bit = h & BLOOM_MASK;
        if (!(bloom_data[bit >> 3] & (uint8_t)(1u << (bit & 7)))) return false;
        h += delta;
    }
    return true;
}

// ============================================================
// Per-Thread Aggregation Hash Table — 2^18 = 262144 slots, ~6.3MB each
// ============================================================
static constexpr uint32_t AGG_BITS = 18;       // 262144 slots per thread
static constexpr uint32_t AGG_SIZE = 1u << AGG_BITS;
static constexpr uint32_t AGG_MASK = AGG_SIZE - 1u;

struct AggSlot {
    int32_t orderkey;     // 0 = empty
    int32_t orderdate;
    int32_t shippriority;
    int64_t revenue;      // accumulated: sum(ep * (100 - disc)), scaled by 10000
};

inline void agg_update(AggSlot* ht, int32_t key, int32_t date, int32_t prio, int64_t rev) {
    uint32_t slot = ((uint32_t)key * 2654435761u) & AGG_MASK;
    while (ht[slot].orderkey != 0 && ht[slot].orderkey != key)
        slot = (slot + 1) & AGG_MASK;
    if (ht[slot].orderkey == 0) {
        ht[slot] = {key, date, prio, rev};
    } else {
        ht[slot].revenue += rev;
    }
}

// ============================================================
// Global merge aggregation table (same layout as per-thread)
// ============================================================
static AggSlot* global_agg;

inline void global_agg_merge(int32_t key, int32_t date, int32_t prio, int64_t rev) {
    uint32_t slot = ((uint32_t)key * 2654435761u) & AGG_MASK;
    while (global_agg[slot].orderkey != 0 && global_agg[slot].orderkey != key)
        slot = (slot + 1) & AGG_MASK;
    if (global_agg[slot].orderkey == 0) {
        global_agg[slot] = {key, date, prio, rev};
    } else {
        global_agg[slot].revenue += rev;
    }
}

// ============================================================
// mmap helper
// ============================================================
template<typename T>
struct MmapCol {
    const T* data = nullptr;
    size_t n = 0;
    int fd = -1;
    size_t bytes = 0;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st;
        fstat(fd, &st);
        bytes = (size_t)st.st_size;
        n = bytes / sizeof(T);
        void* p = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
        if (p == MAP_FAILED) { perror("mmap"); exit(1); }
        madvise(p, bytes, MADV_SEQUENTIAL);
        data = reinterpret_cast<const T*>(p);
    }

    void close() {
        if (data) { munmap(const_cast<T*>(data), bytes); data = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
};

// ============================================================
// Main Query Function
// ============================================================
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();
    DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");  // = 9204

    const std::string base = gendb_dir + "/";

    // ==================================================================
    // Phase 1: Load customer segment dict, build customer bitset
    // ==================================================================
    {
        GENDB_PHASE("dim_filter");

        // Find BUILDING code in dictionary (line index = code)
        int16_t building_code = -1;
        {
            std::ifstream f(base + "customer/c_mktsegment_dict.txt");
            if (!f) { fprintf(stderr, "Cannot open c_mktsegment_dict.txt\n"); exit(1); }
            std::string line;
            int16_t code = 0;
            while (std::getline(f, line)) {
                // Trim trailing whitespace
                while (!line.empty() && (line.back() == '\r' || line.back() == '\n' || line.back() == ' '))
                    line.pop_back();
                if (line == "BUILDING") { building_code = code; break; }
                code++;
            }
        }
        if (building_code < 0) { fprintf(stderr, "BUILDING not found in dict\n"); exit(1); }

        memset(cust_bitset, 0, sizeof(cust_bitset));

        MmapCol<int16_t> c_seg; c_seg.open(base + "customer/c_mktsegment.bin");
        MmapCol<int32_t> c_key; c_key.open(base + "customer/c_custkey.bin");

        size_t nc = c_seg.n;
        for (size_t i = 0; i < nc; i++) {
            if (c_seg.data[i] == building_code) {
                bitset_set((uint32_t)c_key.data[i]);
            }
        }

        c_seg.close();
        c_key.close();
    }

    // ==================================================================
    // Phase 2: Parallel orders scan → thread-local lists → build hash map + bloom
    // ==================================================================
    int num_threads = omp_get_max_threads();

    {
        GENDB_PHASE("build_joins");

        MmapCol<int32_t> o_date; o_date.open(base + "orders/o_orderdate.bin");
        MmapCol<int32_t> o_cust; o_cust.open(base + "orders/o_custkey.bin");
        MmapCol<int32_t> o_okey; o_okey.open(base + "orders/o_orderkey.bin");
        MmapCol<int32_t> o_prio; o_prio.open(base + "orders/o_shippriority.bin");

        size_t n = o_date.n;

        // Thread-local qualifying order lists
        struct QOrder { int32_t orderkey, orderdate, shippriority; };
        std::vector<std::vector<QOrder>> thread_orders(num_threads);
        for (auto& v : thread_orders) v.reserve(25000);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = thread_orders[tid];
            #pragma omp for schedule(static, 100000)
            for (size_t i = 0; i < n; i++) {
                if (o_date.data[i] < DATE_THRESHOLD
                    && bitset_test((uint32_t)o_cust.data[i])) {
                    local.push_back({o_okey.data[i], o_date.data[i], o_prio.data[i]});
                }
            }
        }

        o_date.close(); o_cust.close(); o_okey.close(); o_prio.close();

        // Count total qualifying orders
        size_t total_orders = 0;
        for (auto& v : thread_orders) total_orders += v.size();

        // Check load factor — OHT_SIZE = 4M slots, warn if >75%
        // (Silently continue even if high — correctness first)

        // Allocate orders hash table (zero-initialized = empty)
        orders_ht = new (std::nothrow) OrderSlot[OHT_SIZE]();
        if (!orders_ht) { fprintf(stderr, "OOM: orders_ht\n"); exit(1); }

        memset(bloom_data, 0, sizeof(bloom_data));

        // Sequential build — avoids concurrency complexity
        for (auto& local : thread_orders) {
            for (auto& q : local) {
                oht_insert(q.orderkey, q.orderdate, q.shippriority);
                bloom_insert(q.orderkey);
            }
        }
    }

    // ==================================================================
    // Phase 3: Parallel lineitem scan with thread-local aggregation
    // ==================================================================

    // Allocate per-thread aggregation maps
    std::vector<AggSlot*> agg_maps(num_threads, nullptr);
    for (int t = 0; t < num_threads; t++) {
        agg_maps[t] = new (std::nothrow) AggSlot[AGG_SIZE]();
        if (!agg_maps[t]) { fprintf(stderr, "OOM: agg_maps[%d]\n", t); exit(1); }
    }

    {
        GENDB_PHASE("main_scan");

        MmapCol<int32_t> l_ship; l_ship.open(base + "lineitem/l_shipdate.bin");
        MmapCol<int32_t> l_okey; l_okey.open(base + "lineitem/l_orderkey.bin");
        MmapCol<int64_t> l_eprc; l_eprc.open(base + "lineitem/l_extendedprice.bin");
        MmapCol<int64_t> l_disc; l_disc.open(base + "lineitem/l_discount.bin");

        size_t n = l_ship.n;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggSlot* local_agg = agg_maps[tid];

            #pragma omp for schedule(static, 100000)
            for (size_t i = 0; i < n; i++) {
                if (l_ship.data[i] <= DATE_THRESHOLD) continue;

                int32_t okey = l_okey.data[i];

                // Bloom filter check — eliminates ~98% of non-matching probes
                if (!bloom_test(okey)) continue;

                // Hash table probe
                const OrderSlot* os = oht_find(okey);
                if (!os) continue;

                // Compute revenue (scaled: ep_raw * (100 - disc_raw), units of SQL * 10000)
                int64_t ep   = l_eprc.data[i];
                int64_t disc = l_disc.data[i];
                int64_t rev  = ep * (100LL - disc);

                agg_update(local_agg, okey, os->orderdate, os->shippriority, rev);
            }
        }

        l_ship.close(); l_okey.close(); l_eprc.close(); l_disc.close();
    }

    // ==================================================================
    // Phase 4: Merge thread-local agg maps → global agg table
    // ==================================================================
    {
        GENDB_PHASE("aggregation_merge");

        // Allocate global aggregation table (same size as per-thread)
        global_agg = new (std::nothrow) AggSlot[AGG_SIZE]();
        if (!global_agg) { fprintf(stderr, "OOM: global_agg\n"); exit(1); }

        for (int t = 0; t < num_threads; t++) {
            AggSlot* la = agg_maps[t];
            for (uint32_t s = 0; s < AGG_SIZE; s++) {
                if (la[s].orderkey != 0) {
                    global_agg_merge(la[s].orderkey, la[s].orderdate,
                                     la[s].shippriority, la[s].revenue);
                }
            }
            delete[] agg_maps[t];
            agg_maps[t] = nullptr;
        }
    }

    // ==================================================================
    // Phase 5: Top-10 using min-heap
    // ==================================================================
    struct ResultRow {
        int32_t orderkey;
        int64_t revenue;   // scaled by 10000
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<ResultRow> top10;

    {
        GENDB_PHASE("sort_topk");

        // Min-heap: top element = worst among current top-10
        // Order: revenue DESC, orderdate ASC
        // Heap comparator: returns true if a < b in priority
        //   (so b should be higher in max-heap = a is popped before b)
        // We want min-heap: smallest (worst) at top, so comparator makes
        // std::priority_queue act as min-heap by negating:
        //   cmp(a, b) = true means "a has LESS priority than b" = a stays at bottom
        // For std::priority_queue (max-heap by default):
        //   top = element e where cmp(e, x) = false for all x = MAXIMUM priority element
        // We want MINIMUM priority (worst) at top → invert comparator:
        //   cmp(a, b) = true means a > b in priority (a is BETTER, goes below the worse element)
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            // a > b means a has higher priority → a should be below b in min-heap
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;  // smaller orderdate = higher priority
        };

        using MinHeap = std::priority_queue<ResultRow, std::vector<ResultRow>, decltype(cmp)>;
        MinHeap heap(cmp);

        for (uint32_t s = 0; s < AGG_SIZE; s++) {
            if (global_agg[s].orderkey == 0) continue;
            ResultRow row{global_agg[s].orderkey, global_agg[s].revenue,
                          global_agg[s].orderdate, global_agg[s].shippriority};

            if ((int)heap.size() < 10) {
                heap.push(row);
            } else {
                const ResultRow& worst = heap.top();
                bool better = (row.revenue > worst.revenue) ||
                              (row.revenue == worst.revenue && row.orderdate < worst.orderdate);
                if (better) {
                    heap.pop();
                    heap.push(row);
                }
            }
        }

        // Extract and sort
        while (!heap.empty()) {
            top10.push_back(heap.top());
            heap.pop();
        }
        std::sort(top10.begin(), top10.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        });
    }

    // ==================================================================
    // Phase 6: Output CSV
    // ==================================================================
    {
        GENDB_PHASE("output");

        // Ensure results directory exists
        std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); exit(1); }

        fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[16];
        for (const auto& row : top10) {
            // Revenue: row.revenue is sum(ep * (100 - disc)), divide by 10000 for SQL value
            // Display with 4 decimal places to match ground truth format
            int64_t rev_int  = row.revenue / 10000;
            int64_t rev_frac = row.revenue % 10000;
            if (rev_frac < 0) { rev_int--; rev_frac += 10000; }

            gendb::epoch_days_to_date_str(row.orderdate, date_buf);

            fprintf(out, "%d,%lld.%04lld,%s,%d\n",
                    row.orderkey,
                    (long long)rev_int,
                    (long long)rev_frac,
                    date_buf,
                    row.shippriority);
        }

        fclose(out);
    }

    // Cleanup
    delete[] orders_ht; orders_ht = nullptr;
    delete[] global_agg; global_agg = nullptr;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
