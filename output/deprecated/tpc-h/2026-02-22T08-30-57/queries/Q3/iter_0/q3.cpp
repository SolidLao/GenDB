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

// ── Customer custkey hash set ──────────────────────────────────────────────────

struct CustKeySet {
    std::vector<int32_t> tbl;
    uint32_t cap, mask;

    explicit CustKeySet(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        tbl.resize(cap);
        std::fill(tbl.begin(), tbl.end(), INT32_MIN);  // C20
    }

    void insert(int32_t key) {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            if (tbl[s] == INT32_MIN) { tbl[s] = key; return; }
            if (tbl[s] == key) return;
        }
    }

    bool contains(int32_t key) const {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            if (tbl[s] == INT32_MIN) return false;
            if (tbl[s] == key) return true;
        }
        return false;
    }
};

// ── Orders orderkey hash map ───────────────────────────────────────────────────

struct OrderPayload {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct OrderMap {
    std::vector<int32_t>      keys;
    std::vector<OrderPayload> vals;
    uint32_t cap, mask;

    explicit OrderMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        keys.resize(cap);
        vals.resize(cap);
        std::fill(keys.begin(), keys.end(), INT32_MIN);  // C20
    }

    void insert(int32_t key, int32_t odate, int32_t shippr) {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            if (keys[s] == INT32_MIN) {
                keys[s] = key;
                vals[s] = {odate, shippr};
                return;
            }
            if (keys[s] == key) return;  // duplicate
        }
    }

    const OrderPayload* find(int32_t key) const {
        uint32_t h = hash32(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            if (keys[s] == INT32_MIN) return nullptr;
            if (keys[s] == key) return &vals[s];
        }
        return nullptr;
    }
};

// ── Aggregation hash map ───────────────────────────────────────────────────────

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

static inline uint32_t hash_agg(const AggKey& k) {
    uint32_t h = (uint32_t)k.l_orderkey * 2654435761u;
    h ^= (uint32_t)k.o_orderdate * 40503u;
    h ^= (uint32_t)k.o_shippriority * 12345u;
    return h;
}

static inline bool agg_eq(const AggKey& a, const AggKey& b) {
    return a.l_orderkey == b.l_orderkey &&
           a.o_orderdate == b.o_orderdate &&
           a.o_shippriority == b.o_shippriority;
}

struct AggMap {
    std::vector<AggKey> keys;
    std::vector<double> vals;
    uint32_t cap, mask;

    explicit AggMap(uint32_t capacity) : cap(capacity), mask(capacity - 1) {
        keys.resize(cap);
        vals.resize(cap, 0.0);
        for (auto& k : keys) k.l_orderkey = INT32_MIN;  // C20
    }

    void accumulate(const AggKey& key, double rev) {
        uint32_t h = hash_agg(key) & mask;
        for (uint32_t p = 0; p < cap; p++) {  // C24
            uint32_t s = (h + p) & mask;
            if (keys[s].l_orderkey == INT32_MIN) {
                keys[s] = key;
                vals[s] = rev;
                return;
            }
            if (agg_eq(keys[s], key)) {
                vals[s] += rev;
                return;
            }
        }
    }

    void merge_into(AggMap& dst) const {
        for (uint32_t i = 0; i < cap; i++) {
            if (keys[i].l_orderkey != INT32_MIN) {
                dst.accumulate(keys[i], vals[i]);
            }
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
    const int32_t* orders_zm  = nullptr;  // pairs: [min, max] per block
    const int32_t* li_zm      = nullptr;

    {
        GENDB_PHASE("data_loading");

        c_mktsegment   = mmap_col<uint8_t> (gendb_dir + "/customer/c_mktsegment.bin",   n_customer);
        c_custkey      = mmap_col<int32_t> (gendb_dir + "/customer/c_custkey.bin",       tmp);
        o_custkey      = mmap_col<int32_t> (gendb_dir + "/orders/o_custkey.bin",         n_orders);
        o_orderdate    = mmap_col<int32_t> (gendb_dir + "/orders/o_orderdate.bin",       tmp);
        o_orderkey     = mmap_col<int32_t> (gendb_dir + "/orders/o_orderkey.bin",        tmp);
        o_shippriority = mmap_col<int32_t> (gendb_dir + "/orders/o_shippriority.bin",    tmp);
        l_orderkey     = mmap_col<int32_t> (gendb_dir + "/lineitem/l_orderkey.bin",      n_lineitem);
        l_shipdate     = mmap_col<int32_t> (gendb_dir + "/lineitem/l_shipdate.bin",      tmp);
        l_extprice     = mmap_col<double>  (gendb_dir + "/lineitem/l_extendedprice.bin", tmp);
        l_discount     = mmap_col<double>  (gendb_dir + "/lineitem/l_discount.bin",      tmp);

        // Orders zone map
        {
            int fd = open((gendb_dir + "/indexes/orders_orderdate_zonemap.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            auto* raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
            close(fd);
            orders_zm_nblocks = raw[0];
            orders_zm = reinterpret_cast<const int32_t*>(raw + 1);
        }
        // Lineitem zone map
        {
            int fd = open((gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            auto* raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
            close(fd);
            li_zm_nblocks = raw[0];
            li_zm = reinterpret_cast<const int32_t*>(raw + 1);
        }
    }

    // ── Dim filter: scan customer, build custkey hashset ──────────────────────
    // C9: next_pow2(300000 * 2) = 1048576
    CustKeySet custkey_set(next_pow2(300000u * 2u));

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

        for (size_t i = 0; i < n_customer; i++) {
            if (c_mktsegment[i] == building_code) {
                custkey_set.insert(c_custkey[i]);
            }
        }
    }

    // ── Build joins: orders scan → custkey semi-join → orderkey hashmap ───────
    // C9: next_pow2(735000 * 2) = 2097152
    const uint32_t ORDER_CAP = next_pow2(735000u * 2u);  // 2097152
    OrderMap order_map(ORDER_CAP);

    {
        GENDB_PHASE("build_joins");

        // Zone-map pruning for orders: skip block if block_min >= DATE_THRESHOLD
        // Orders sorted by o_orderdate → find last qualifying block
        uint32_t end_block = 0;
        for (uint32_t b = 0; b < orders_zm_nblocks; b++) {
            int32_t bmin = orders_zm[b * 2];
            if (bmin >= DATE_THRESHOLD) break;
            end_block = b + 1;
        }

        // Parallel scan with thread-local collection
        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::tuple<int32_t, int32_t, int32_t>>> local_orders(nthreads);

        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (uint32_t b = 0; b < end_block; b++) {
            // Extra check within block (boundary block may have mixed values)
            int32_t bmin = orders_zm[b * 2];
            if (bmin >= DATE_THRESHOLD) continue;

            int tid = omp_get_thread_num();
            uint32_t row_start = b * BLOCK_SIZE;
            uint32_t row_end   = std::min((uint32_t)n_orders, row_start + BLOCK_SIZE);

            for (uint32_t r = row_start; r < row_end; r++) {
                if (o_orderdate[r] < DATE_THRESHOLD &&
                    custkey_set.contains(o_custkey[r])) {
                    local_orders[tid].emplace_back(
                        o_orderkey[r], o_orderdate[r], o_shippriority[r]);
                }
            }
        }

        // Sequential merge into global hashmap
        for (int t = 0; t < nthreads; t++) {
            for (auto& [ok, od, sp] : local_orders[t]) {
                order_map.insert(ok, od, sp);
            }
        }
    }

    // ── Main scan: lineitem → join orders → hash aggregate ───────────────────
    // C9: thread-local maps sized for full group cardinality, not per-thread
    const uint32_t AGG_CAP = next_pow2(735000u * 2u);  // 2097152

    AggMap global_agg(AGG_CAP);

    {
        GENDB_PHASE("main_scan");

        // Zone-map pruning for lineitem: skip block if block_max <= DATE_THRESHOLD
        // Lineitem sorted by l_shipdate → find first qualifying block
        uint32_t first_block = li_zm_nblocks;
        for (uint32_t b = 0; b < li_zm_nblocks; b++) {
            int32_t bmax = li_zm[b * 2 + 1];
            if (bmax > DATE_THRESHOLD) { first_block = b; break; }
        }

        int nthreads = omp_get_max_threads();

        // Thread-local aggregation maps (C9: each sized for full cardinality)
        std::vector<AggMap*> local_aggs(nthreads, nullptr);
        for (int t = 0; t < nthreads; t++) {
            local_aggs[t] = new AggMap(AGG_CAP);
        }

        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (uint32_t b = first_block; b < li_zm_nblocks; b++) {
            int32_t bmax = li_zm[b * 2 + 1];
            if (bmax <= DATE_THRESHOLD) continue;  // extra safety

            int tid = omp_get_thread_num();
            AggMap& local_agg = *local_aggs[tid];

            uint32_t row_start = b * BLOCK_SIZE;
            uint32_t row_end   = std::min((uint32_t)n_lineitem, row_start + BLOCK_SIZE);

            for (uint32_t r = row_start; r < row_end; r++) {
                if (l_shipdate[r] <= DATE_THRESHOLD) continue;  // l_shipdate > 9204

                int32_t lk = l_orderkey[r];
                const OrderPayload* op = order_map.find(lk);
                if (!op) continue;

                double rev = l_extprice[r] * (1.0 - l_discount[r]);
                // C15: key includes ALL 3 GROUP BY columns
                AggKey key{ lk, op->o_orderdate, op->o_shippriority };
                local_agg.accumulate(key, rev);
            }
        }

        // Sequential merge of thread-local maps into global
        {
            GENDB_PHASE("aggregation_merge");
            for (int t = 0; t < nthreads; t++) {
                local_aggs[t]->merge_into(global_agg);
                delete local_aggs[t];
                local_aggs[t] = nullptr;
            }
        }
    }

    // ── Output: top-10 by revenue DESC, o_orderdate ASC ──────────────────────
    {
        GENDB_PHASE("output");

        // Min-heap of size 10.
        // HeapCmp(a,b) = true if a is BETTER than b → top() = worst entry (evicted first).
        std::priority_queue<TopKEntry, std::vector<TopKEntry>, HeapCmp> heap;

        HeapCmp hcmp;
        for (uint32_t i = 0; i < global_agg.cap; i++) {
            if (global_agg.keys[i].l_orderkey == INT32_MIN) continue;

            TopKEntry e{
                global_agg.keys[i].l_orderkey,
                global_agg.vals[i],
                global_agg.keys[i].o_orderdate,
                global_agg.keys[i].o_shippriority
            };

            if ((int)heap.size() < 10) {
                heap.push(e);
            } else if (hcmp(e, heap.top())) {
                // e is better than current worst → replace
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
