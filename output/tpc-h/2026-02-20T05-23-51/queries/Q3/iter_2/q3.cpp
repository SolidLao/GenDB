#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <vector>
#include <algorithm>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"
#include "date_utils.h"

static const char* BASE = "/home/jl4492/GenDB/benchmarks/tpc-h/sf10.gendb";
static const int32_t DATE_CUTOFF = 9204; // epoch days for 1995-03-15

// ──────────────────────────────────────────────
// mmap helper
// ──────────────────────────────────────────────
template<typename T>
static const T* mmap_file(const char* path, size_t& n_out, int advice = POSIX_FADV_SEQUENTIAL) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); n_out = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, advice);
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    close(fd);
    n_out = st.st_size / sizeof(T);
    return p;
}

// hash32: same as in index spec
static inline uint32_t hash32(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> 32);
}

// ──────────────────────────────────────────────
// Phase 1: Customer hash set (BUILDING custkeys)
// ──────────────────────────────────────────────
struct CustSet {
    static const int32_t EMPTY = INT32_MIN;
    uint32_t cap;
    uint32_t mask;
    int32_t* slots;

    CustSet(uint32_t c) : cap(c), mask(c - 1) {
        slots = new int32_t[c];
        std::fill(slots, slots + c, EMPTY);
    }
    ~CustSet() { delete[] slots; }

    void insert(int32_t k) {
        uint32_t h = hash32(k) & mask;
        while (slots[h] != EMPTY && slots[h] != k) h = (h + 1) & mask;
        slots[h] = k;
    }
    bool contains(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        while (slots[h] != EMPTY && slots[h] != k) h = (h + 1) & mask;
        return slots[h] == k;
    }
};

// ──────────────────────────────────────────────
// Phase 2: Order info map: orderkey → {orderdate, shippriority}
// ──────────────────────────────────────────────
struct OrdSlot {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdMap {
    static const int32_t EMPTY = INT32_MIN;
    uint32_t cap;
    uint32_t mask;
    OrdSlot* slots;

    OrdMap(uint32_t c) : cap(c), mask(c - 1) {
        slots = new OrdSlot[c];
        for (uint32_t i = 0; i < c; i++) slots[i].key = EMPTY;
    }
    ~OrdMap() { delete[] slots; }

    void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & mask;
        while (slots[h].key != EMPTY && slots[h].key != k) h = (h + 1) & mask;
        if (slots[h].key == EMPTY) { slots[h] = {k, od, sp}; }
    }

    const OrdSlot* find(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        while (slots[h].key != EMPTY && slots[h].key != k) h = (h + 1) & mask;
        return (slots[h].key == k) ? &slots[h] : nullptr;
    }
};

// ──────────────────────────────────────────────
// Phase 3: Revenue aggregation map (thread-local)
// ──────────────────────────────────────────────
struct AggSlot {
    int32_t key;          // l_orderkey (INT32_MIN = empty)
    double  revenue;
    int32_t orderdate;
    int32_t shippriority;
};

struct AggMap {
    static const int32_t EMPTY = INT32_MIN;
    uint32_t cap;
    uint32_t mask;
    AggSlot* slots;

    AggMap() : cap(0), mask(0), slots(nullptr) {}

    void init(uint32_t c) {
        cap = c; mask = c - 1;
        slots = new AggSlot[c];
        for (uint32_t i = 0; i < c; i++) slots[i].key = EMPTY;
    }
    ~AggMap() { delete[] slots; }

    void accumulate(int32_t k, double rev, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & mask;
        while (slots[h].key != EMPTY && slots[h].key != k) h = (h + 1) & mask;
        if (slots[h].key == EMPTY) {
            slots[h] = {k, rev, od, sp};
        } else {
            slots[h].revenue += rev;
        }
    }
};

// Zone map block descriptor
struct ZMBlock { int32_t min_val, max_val; uint32_t block_size; };

int main() {
    gendb::init_date_tables();
    char path[512];

    // ─────────────────────────────────────────
    // Phase 1: Build BUILDING customer hash set
    // ─────────────────────────────────────────
    CustSet* cust_set = nullptr;
    {
        GENDB_PHASE("phase1_customer");

        size_t n_seg, n_ck;
        snprintf(path, sizeof(path), "%s/customer/c_mktsegment.bin", BASE);
        const int8_t* seg = mmap_file<int8_t>(path, n_seg);

        snprintf(path, sizeof(path), "%s/customer/c_custkey.bin", BASE);
        const int32_t* custkey = mmap_file<int32_t>(path, n_ck);

        // ~330K BUILDING customers; 1<<20 slots (1M) = 33% load factor
        cust_set = new CustSet(1u << 20);
        size_t n = std::min(n_seg, n_ck);
        for (size_t i = 0; i < n; i++) {
            if (seg[i] == 1) cust_set->insert(custkey[i]);
        }
    }

    // ─────────────────────────────────────────
    // Phase 2: Scan orders → build OrdMap
    //          Filter: o_orderdate < 9204 AND o_custkey IN cust_set
    // ─────────────────────────────────────────
    // OrdMap: ~1.47M qualifying orders; 1<<22 (4M slots) = 37% load, 48MB
    OrdMap* ord_map = new OrdMap(1u << 22);
    {
        GENDB_PHASE("phase2_orders");

        size_t n_od, n_ok, n_ck2, n_sp;
        snprintf(path, sizeof(path), "%s/orders/o_orderdate.bin", BASE);
        const int32_t* o_orderdate = mmap_file<int32_t>(path, n_od);

        snprintf(path, sizeof(path), "%s/orders/o_orderkey.bin", BASE);
        const int32_t* o_orderkey = mmap_file<int32_t>(path, n_ok);

        snprintf(path, sizeof(path), "%s/orders/o_custkey.bin", BASE);
        const int32_t* o_custkey = mmap_file<int32_t>(path, n_ck2);

        snprintf(path, sizeof(path), "%s/orders/o_shippriority.bin", BASE);
        const int32_t* o_shippriority = mmap_file<int32_t>(path, n_sp);

        // Load orders orderdate zone map for block skipping
        size_t n_zm_raw;
        snprintf(path, sizeof(path), "%s/indexes/orders_orderdate_zonemap.bin", BASE);
        const uint32_t* zm_raw = mmap_file<uint32_t>(path, n_zm_raw);
        uint32_t num_zm_blocks = zm_raw[0];
        const ZMBlock* zm = reinterpret_cast<const ZMBlock*>(zm_raw + 1);

        size_t n_rows = n_od;
        size_t block_size = 100000;

        for (uint32_t b = 0; b < num_zm_blocks; b++) {
            // Skip block if all orderdate values >= DATE_CUTOFF (none qualify o_orderdate < 9204)
            if (zm[b].min_val >= DATE_CUTOFF) continue;

            size_t row_start = (size_t)b * block_size;
            size_t row_end = std::min(row_start + block_size, n_rows);

            for (size_t i = row_start; i < row_end; i++) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set->contains(o_custkey[i])) {
                    ord_map->insert(o_orderkey[i], o_orderdate[i], o_shippriority[i]);
                }
            }
        }
    }
    delete cust_set;

    // ─────────────────────────────────────────
    // Phase 3: Parallel sequential lineitem scan
    //          Filter l_shipdate > 9204, probe OrdMap, aggregate revenue
    // ─────────────────────────────────────────
    int nthreads = omp_get_max_threads();
    std::vector<AggMap> taggs(nthreads);
    // Thread-local: at most ~937K rows per thread, ~68% pass shipdate, fraction match orders
    // Use 1<<18 = 256K slots per thread (safe for tens of thousands of distinct orderkeys)
    for (int t = 0; t < nthreads; t++) taggs[t].init(1u << 18);

    {
        GENDB_PHASE("phase3_lineitem");

        size_t n_lk, n_ls, n_ep, n_ld;
        snprintf(path, sizeof(path), "%s/lineitem/l_orderkey.bin", BASE);
        const int32_t* l_orderkey = mmap_file<int32_t>(path, n_lk);

        snprintf(path, sizeof(path), "%s/lineitem/l_shipdate.bin", BASE);
        const int32_t* l_shipdate = mmap_file<int32_t>(path, n_ls);

        snprintf(path, sizeof(path), "%s/lineitem/l_extendedprice.bin", BASE);
        const double* l_extprice = mmap_file<double>(path, n_ep);

        snprintf(path, sizeof(path), "%s/lineitem/l_discount.bin", BASE);
        const double* l_discount = mmap_file<double>(path, n_ld);

        // Load lineitem shipdate zone map
        size_t n_lzm_raw;
        snprintf(path, sizeof(path), "%s/indexes/lineitem_shipdate_zonemap.bin", BASE);
        const uint32_t* lzm_raw = mmap_file<uint32_t>(path, n_lzm_raw);
        uint32_t num_lzm_blocks = lzm_raw[0];
        const ZMBlock* lzm = reinterpret_cast<const ZMBlock*>(lzm_raw + 1);

        size_t n_rows = n_lk;
        size_t block_size = 100000;

        // Collect qualifying block ranges for parallel work distribution
        std::vector<uint32_t> active_blocks;
        active_blocks.reserve(num_lzm_blocks);
        for (uint32_t b = 0; b < num_lzm_blocks; b++) {
            // Skip block if all shipdates <= 9204 (none qualify l_shipdate > 9204)
            if (lzm[b].max_val <= DATE_CUTOFF) continue;
            active_blocks.push_back(b);
        }

        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (int bi = 0; bi < (int)active_blocks.size(); bi++) {
            uint32_t b = active_blocks[bi];
            int tid = omp_get_thread_num();
            AggMap& local = taggs[tid];

            size_t row_start = (size_t)b * block_size;
            size_t row_end = std::min(row_start + block_size, n_rows);

            // If entire block qualifies on shipdate, skip per-row date check
            bool all_qualify_date = (lzm[b].min_val > DATE_CUTOFF);

            for (size_t i = row_start; i < row_end; i++) {
                if (!all_qualify_date && l_shipdate[i] <= DATE_CUTOFF) continue;
                int32_t lk = l_orderkey[i];
                const OrdSlot* os = ord_map->find(lk);
                if (!os) continue;
                double rev = l_extprice[i] * (1.0 - l_discount[i]);
                local.accumulate(lk, rev, os->orderdate, os->shippriority);
            }
        }
    }

    // ─────────────────────────────────────────
    // Phase 4: Merge thread-local agg maps → global, then top-10
    // ─────────────────────────────────────────
    {
        GENDB_PHASE("phase4_merge_output");

        // Global agg map: 1<<22 slots (4M) for up to 1.47M groups
        AggMap global_agg;
        global_agg.init(1u << 22);

        for (int t = 0; t < nthreads; t++) {
            AggMap& tm = taggs[t];
            for (uint32_t i = 0; i < tm.cap; i++) {
                if (tm.slots[i].key == AggMap::EMPTY) continue;
                auto& s = tm.slots[i];
                global_agg.accumulate(s.key, s.revenue, s.orderdate, s.shippriority);
            }
        }

        // Collect all non-empty entries
        struct Row {
            int32_t orderkey;
            double  revenue;
            int32_t orderdate;
            int32_t shippriority;
        };
        std::vector<Row> rows;
        rows.reserve(4096);
        for (uint32_t i = 0; i < global_agg.cap; i++) {
            auto& s = global_agg.slots[i];
            if (s.key == AggMap::EMPTY) continue;
            rows.push_back({s.key, s.revenue, s.orderdate, s.shippriority});
        }

        // Partial sort: top-10 by revenue DESC, o_orderdate ASC
        auto cmp = [](const Row& a, const Row& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        };
        size_t top = std::min<size_t>(10, rows.size());
        std::partial_sort(rows.begin(), rows.begin() + top, rows.end(), cmp);

        // Output
        for (size_t i = 0; i < top; i++) {
            const Row& r = rows[i];
            char date_buf[20];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            printf("%d,%.2f,%s,%d\n",
                   r.orderkey, r.revenue, date_buf, r.shippriority);
        }
    }

    return 0;
}
