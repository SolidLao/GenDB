// Q3: Shipping Priority
// Strategy: sequential scan for lineitem (HDD: random I/O via index = 300s timeout)
//   Phase 1: sequential scan customer → BUILDING custkey hash set (~301K entries)
//   Phase 2: parallel scan orders with orderdate zone map → qualifying order map
//   Phase 3: parallel sequential scan lineitem with shipdate zone map → aggregate
//   Phase 4: merge thread-local agg maps → top-10 heap → CSV

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

static const int32_t EMPTY_KEY  = INT32_MIN;
static const int32_t DATE_CUTOFF = 9204; // epoch days: 1995-03-15

static inline uint32_t hash32(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> 32);
}

// ── mmap helpers ──────────────────────────────────────────────────────────────

template<typename T>
static const T* mmap_seq(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); n_out = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    close(fd);
    n_out = st.st_size / sizeof(T);
    return p;
}

static const uint8_t* mmap_raw(const std::string& path, size_t& bytes_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); bytes_out = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    bytes_out = st.st_size;
    auto* p = reinterpret_cast<const uint8_t*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ── Data structures ───────────────────────────────────────────────────────────

// Phase 1: Open-addressing hash set for BUILDING custkeys (~301K entries)
struct CustSet {
    static const uint32_t CAP = 1u << 20; // 1M slots → ~30% load
    int32_t* slots;

    CustSet() : slots(new int32_t[CAP]) {
        std::fill(slots, slots + CAP, EMPTY_KEY);
    }
    ~CustSet() { delete[] slots; }

    void insert(int32_t k) {
        uint32_t h = hash32(k) & (CAP - 1);
        while (slots[h] != EMPTY_KEY && slots[h] != k) h = (h + 1) & (CAP - 1);
        slots[h] = k;
    }
    bool contains(int32_t k) const {
        uint32_t h = hash32(k) & (CAP - 1);
        while (slots[h] != EMPTY_KEY && slots[h] != k) h = (h + 1) & (CAP - 1);
        return slots[h] == k;
    }
};

// Phase 2: Qualifying order info map: orderkey → {orderdate, shippriority}
// ~1.47M qualifying orders; use 2^21=2M slots → ~70% load (acceptable)
struct OrdSlot {
    int32_t key;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdMap {
    uint32_t cap;
    uint32_t mask;
    OrdSlot* slots;

    explicit OrdMap(uint32_t c) : cap(c), mask(c - 1), slots(new OrdSlot[c]) {
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
        return (slots[h].key == k) ? &slots[h] : nullptr;
    }
};

// Phase 3: Thread-local aggregation map: orderkey → revenue
// ~3000 distinct groups expected; 1<<13=8192 slots → 37% load, fits in L1 cache
struct AggSlot {
    int32_t key;
    double  revenue;
};

struct AggMap {
    static const uint32_t CAP = 1u << 13; // 8192
    AggSlot slots[CAP]; // inline array — stays in L1/L2 cache

    AggMap() {
        for (uint32_t i = 0; i < CAP; ++i) slots[i].key = EMPTY_KEY;
    }

    void add(int32_t k, double rev) {
        uint32_t h = hash32(k) & (CAP - 1);
        while (slots[h].key != EMPTY_KEY && slots[h].key != k) h = (h + 1) & (CAP - 1);
        if (slots[h].key == EMPTY_KEY) {
            slots[h].key     = k;
            slots[h].revenue = rev;
        } else {
            slots[h].revenue += rev;
        }
    }
};

struct ZMBlock { int32_t min_val, max_val; uint32_t block_size; };

// ── Main query function ───────────────────────────────────────────────────────

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================
    // Phase 1: Scan customer → build BUILDING custkey set
    // =========================================================
    CustSet* cust_set = nullptr;
    {
        GENDB_PHASE("phase1_customer");
        size_t n_seg = 0, n_ck = 0;
        const auto* seg  = mmap_seq<int8_t> (gendb_dir + "/customer/c_mktsegment.bin", n_seg);
        const auto* ckey = mmap_seq<int32_t>(gendb_dir + "/customer/c_custkey.bin",    n_ck);
        size_t n = std::min(n_seg, n_ck);

        // Load c_mktsegment dictionary at runtime to find BUILDING code
        int8_t building_code = -1;
        {
            std::ifstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            int8_t code = 0;
            while (std::getline(dict_file, line)) {
                if (line == "BUILDING") { building_code = code; break; }
                ++code;
            }
        }

        cust_set = new CustSet();
        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code) cust_set->insert(ckey[i]);
        }

        munmap((void*)seg,  n_seg * sizeof(int8_t));
        munmap((void*)ckey, n_ck  * sizeof(int32_t));
    }

    // =========================================================
    // Phase 2: Parallel scan orders + zone map → build OrdMap
    //          Filter: o_orderdate < 9204 AND o_custkey IN cust_set
    // =========================================================
    // Use 2^22 = 4M slots; qualifying ~1.47M orders → ~37% load
    OrdMap* ord_map = new OrdMap(1u << 22);
    {
        GENDB_PHASE("phase2_orders");

        // Load zone map for orderdate block pruning
        size_t zm_bytes = 0;
        const uint8_t* zm_raw = mmap_raw(gendb_dir + "/indexes/orders_orderdate_zonemap.bin", zm_bytes);
        uint32_t num_zm_blocks = 0;
        const ZMBlock* zm = nullptr;
        if (zm_raw && zm_bytes > 4) {
            num_zm_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
            zm = reinterpret_cast<const ZMBlock*>(zm_raw + sizeof(uint32_t));
        }

        size_t n_od = 0, n_ok = 0, n_ck2 = 0, n_sp = 0;
        const auto* o_orderdate    = mmap_seq<int32_t>(gendb_dir + "/orders/o_orderdate.bin",    n_od);
        const auto* o_orderkey     = mmap_seq<int32_t>(gendb_dir + "/orders/o_orderkey.bin",     n_ok);
        const auto* o_custkey      = mmap_seq<int32_t>(gendb_dir + "/orders/o_custkey.bin",      n_ck2);
        const auto* o_shippriority = mmap_seq<int32_t>(gendb_dir + "/orders/o_shippriority.bin", n_sp);
        size_t n_rows = n_od;
        static const size_t BLOCK_SZ = 100000;

        int nthreads = omp_get_max_threads();
        // Thread-local vectors to avoid lock contention
        std::vector<std::vector<OrdSlot>> tvecs(nthreads);
        for (auto& v : tvecs) v.reserve(32768);

        if (zm && num_zm_blocks > 0) {
            #pragma omp parallel for schedule(dynamic, 2) num_threads(nthreads)
            for (uint32_t b = 0; b < num_zm_blocks; ++b) {
                if (zm[b].min_val >= DATE_CUTOFF) continue; // entire block has orderdate >= cutoff
                int tid = omp_get_thread_num();
                auto& lv = tvecs[tid];
                size_t rs = (size_t)b * BLOCK_SZ;
                size_t re = std::min(rs + BLOCK_SZ, n_rows);
                for (size_t i = rs; i < re; ++i) {
                    if (o_orderdate[i] < DATE_CUTOFF && cust_set->contains(o_custkey[i])) {
                        lv.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                    }
                }
            }
        } else {
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n_rows; ++i) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set->contains(o_custkey[i])) {
                    tvecs[omp_get_thread_num()].push_back(
                        {o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                }
            }
        }

        // Merge thread-local results into ord_map (sequential - ~1.47M inserts)
        for (int t = 0; t < nthreads; ++t) {
            for (auto& s : tvecs[t]) {
                ord_map->insert(s.key, s.orderdate, s.shippriority);
            }
        }

        munmap((void*)o_orderdate,    n_od  * sizeof(int32_t));
        munmap((void*)o_orderkey,     n_ok  * sizeof(int32_t));
        munmap((void*)o_custkey,      n_ck2 * sizeof(int32_t));
        munmap((void*)o_shippriority, n_sp  * sizeof(int32_t));
        if (zm_raw) munmap((void*)zm_raw, zm_bytes);
        delete cust_set; cust_set = nullptr;
    }

    // =========================================================
    // Phase 3: Parallel sequential lineitem scan + zone map
    //          Filter: l_shipdate > 9204, probe ord_map, aggregate revenue
    //          Sequential scan is critical on HDD — random I/O via index = timeout
    // =========================================================
    int nthreads = omp_get_max_threads();
    // Thread-local agg maps: inline 8192-slot arrays (~3000 groups, 37% load, fits L1)
    std::vector<AggMap> taggs(nthreads);

    {
        GENDB_PHASE("phase3_lineitem");

        // Load lineitem shipdate zone map for block pruning
        size_t lzm_bytes = 0;
        const uint8_t* lzm_raw = mmap_raw(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", lzm_bytes);
        uint32_t num_lzm_blocks = 0;
        const ZMBlock* lzm = nullptr;
        if (lzm_raw && lzm_bytes > 4) {
            num_lzm_blocks = *reinterpret_cast<const uint32_t*>(lzm_raw);
            lzm = reinterpret_cast<const ZMBlock*>(lzm_raw + sizeof(uint32_t));
        }

        size_t n_lk = 0, n_ls = 0, n_ep = 0, n_ld = 0;
        const auto* l_orderkey = mmap_seq<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",     n_lk);
        const auto* l_shipdate = mmap_seq<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",     n_ls);
        const auto* l_extprice = mmap_seq<double>  (gendb_dir + "/lineitem/l_extendedprice.bin", n_ep);
        const auto* l_discount = mmap_seq<double>  (gendb_dir + "/lineitem/l_discount.bin",    n_ld);
        size_t n_rows = n_lk;
        static const size_t LBLOCK_SZ = 100000;

        if (lzm && num_lzm_blocks > 0) {
            // Collect active blocks (skip where all shipdates <= DATE_CUTOFF)
            std::vector<uint32_t> active;
            active.reserve(num_lzm_blocks);
            for (uint32_t b = 0; b < num_lzm_blocks; ++b) {
                if (lzm[b].max_val > DATE_CUTOFF) active.push_back(b);
            }

            #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
            for (int bi = 0; bi < (int)active.size(); ++bi) {
                uint32_t b = active[bi];
                int tid = omp_get_thread_num();
                AggMap& la = taggs[tid];
                size_t rs = (size_t)b * LBLOCK_SZ;
                size_t re = std::min(rs + LBLOCK_SZ, n_rows);

                // If entire block has shipdate > cutoff, skip per-row date check
                bool all_qualify = (lzm[b].min_val > DATE_CUTOFF);

                for (size_t i = rs; i < re; ++i) {
                    if (!all_qualify && l_shipdate[i] <= DATE_CUTOFF) continue;
                    int32_t lk = l_orderkey[i];
                    if (ord_map->find(lk) == nullptr) continue;
                    double rev = l_extprice[i] * (1.0 - l_discount[i]);
                    la.add(lk, rev);
                }
            }
        } else {
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n_rows; ++i) {
                if (l_shipdate[i] <= DATE_CUTOFF) continue;
                int32_t lk = l_orderkey[i];
                if (ord_map->find(lk) == nullptr) continue;
                double rev = l_extprice[i] * (1.0 - l_discount[i]);
                taggs[omp_get_thread_num()].add(lk, rev);
            }
        }

        munmap((void*)l_orderkey, n_lk * sizeof(int32_t));
        munmap((void*)l_shipdate, n_ls * sizeof(int32_t));
        munmap((void*)l_extprice, n_ep * sizeof(double));
        munmap((void*)l_discount, n_ld * sizeof(double));
        if (lzm_raw) munmap((void*)lzm_raw, lzm_bytes);
    }

    // =========================================================
    // Phase 4: Merge thread-local agg maps → global → top-10 → CSV
    // =========================================================
    {
        GENDB_PHASE("phase4_output");

        // Global agg map: same size as thread-local (3000 groups → 8192 slots)
        AggMap global_agg;
        for (int t = 0; t < nthreads; ++t) {
            for (uint32_t h = 0; h < AggMap::CAP; ++h) {
                if (taggs[t].slots[h].key == EMPTY_KEY) continue;
                global_agg.add(taggs[t].slots[h].key, taggs[t].slots[h].revenue);
            }
        }

        struct Row {
            int32_t orderkey;
            double  revenue;
            int32_t orderdate;
            int32_t shippriority;
        };
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
        if (!f) { perror(out_path.c_str()); delete ord_map; return; }
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (size_t i = 0; i < top_k; ++i) {
            const Row& r = rows[i];
            char date_buf[16];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, date_buf, r.shippriority);
        }
        fclose(f);
    }

    delete ord_map;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
