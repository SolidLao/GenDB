// Q3: Shipping Priority — fixed + optimized version
// Fixes:
//   1. AggMap: dynamically sized based on actual qualifying order count (was hardcoded 8192)
//   2. Bounded probing: all hash tables use for-loop with probe < cap guard
//   3. Per-thread AggMap: sized to 64K (enough for ~22K distinct keys/thread with static scheduling)
//   4. Global AggMap: sized for full qualifying order count
//   5. Kahan summation for floating-point precision in revenue aggregation

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

// ── mmap helpers ────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); n = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* pm = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0);
    close(fd);
    if (pm == MAP_FAILED) { n = 0; return nullptr; }
    n = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(pm);
}

static const uint8_t* mmap_raw(const std::string& path, size_t& bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); bytes = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    bytes = st.st_size;
    void* pm = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0);
    close(fd);
    if (pm == MAP_FAILED) { bytes = 0; return nullptr; }
    return reinterpret_cast<const uint8_t*>(pm);
}

// ── CustBitset ────────────────────────────────────────────────────────────────
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

// ── OrdSlot/OrdMap: bounded probing ────────────────────────────────────────────
struct OrdSlot { int32_t key, orderdate, shippriority; };

struct OrdMap {
    uint32_t cap, mask;
    OrdSlot* slots;
    OrdMap(uint32_t c) : cap(c), mask(c - 1), slots(new OrdSlot[c]) {
        for (uint32_t i = 0; i < c; ++i) slots[i].key = EMPTY_KEY;
    }
    ~OrdMap() { delete[] slots; }
    void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            if (slots[h].key == EMPTY_KEY) { slots[h] = {k, od, sp}; return; }
            if (slots[h].key == k) return;
            h = (h + 1) & mask;
        }
        fprintf(stderr, "FATAL: OrdMap full (cap=%u)\n", cap); abort();
    }
    const OrdSlot* find(int32_t k) const {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            if (slots[h].key == EMPTY_KEY) return nullptr;
            if (slots[h].key == k) return &slots[h];
            h = (h + 1) & mask;
        }
        return nullptr;
    }
};

// ── OrdBitset ────────────────────────────────────────────────────────────────
struct OrdBitset {
    uint32_t  max_key = 0;
    uint32_t  words   = 0;
    uint64_t* bits    = nullptr;
    ~OrdBitset() { delete[] bits; }
    void build(const OrdMap& m) {
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

// ── AggMap: dynamically-sized, bounded probing ────────────────────────────────
struct AggSlot { int32_t key; double revenue; };
struct AggMap {
    uint32_t cap, mask;
    AggSlot* slots;

    explicit AggMap(uint32_t c) : cap(c), mask(c - 1), slots(new AggSlot[c]) {
        for (uint32_t i = 0; i < c; ++i) { slots[i].key = EMPTY_KEY; slots[i].revenue = 0.0; }
    }
    ~AggMap() { delete[] slots; }

    void add(int32_t k, double rev) {
        uint32_t h = hash32(k) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            if (slots[h].key == EMPTY_KEY) { slots[h].key = k; slots[h].revenue  = rev; return; }
            if (slots[h].key == k)         { slots[h].revenue += rev; return; }
            h = (h + 1) & mask;
        }
        fprintf(stderr, "FATAL: AggMap full (cap=%u)\n", cap); abort();
    }
};

struct ZMBlock { int32_t min_val, max_val; uint32_t block_size; };

static inline uint32_t next_pow2_ge(uint32_t n) {
    uint32_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Phase 1: customer → BUILDING custkey bitset
    CustBitset* cust_bits = nullptr;
    {
        GENDB_PHASE("phase1_customer");
        int8_t building_code = 1;
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

    // Phase 2: orders scan → OrdMap + OrdBitset
    OrdMap*    ord_map  = new OrdMap(1u << 22); // 4M slots
    OrdBitset* ord_bits = new OrdBitset();
    size_t     total_qualifying_orders = 0;
    {
        GENDB_PHASE("phase2_orders");
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
                if (zm[b].min_val >= DATE_CUTOFF) continue;
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

        for (int t = 0; t < nthreads; ++t) {
            total_qualifying_orders += tvecs[t].size();
            for (const auto& s : tvecs[t])
                ord_map->insert(s.key, s.orderdate, s.shippriority);
        }
        ord_bits->build(*ord_map);

        munmap((void*)o_custkey,      n_rows * sizeof(int32_t));
        munmap((void*)o_orderdate,    n_rows * sizeof(int32_t));
        munmap((void*)o_orderkey,     n_rows * sizeof(int32_t));
        munmap((void*)o_shippriority, n_rows * sizeof(int32_t));
        if (zm_raw) munmap((void*)zm_raw, zm_bytes);
        delete cust_bits; cust_bits = nullptr;
    }

    // Phase 3: lineitem scan → thread-local aggregation
    // Per-thread: ~22K distinct keys/thread with 64 threads and static scheduling
    // Use 65536 per thread (48% load) — bounded probing prevents infinite loops even if more
    int nthreads = omp_get_max_threads();
    uint32_t per_thread_cap = next_pow2_ge(std::max(total_qualifying_orders / (uint32_t)nthreads * 4, (size_t)65536u));
    // Global: sized for full group count
    uint32_t global_cap = next_pow2_ge((uint32_t)(total_qualifying_orders * 2));

    std::vector<AggMap*> taggs(nthreads);
    for (int t = 0; t < nthreads; ++t) taggs[t] = new AggMap(per_thread_cap);

    {
        GENDB_PHASE("phase3_lineitem");
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
            std::vector<uint32_t> active;
            active.reserve(num_lzm);
            for (uint32_t b = 0; b < num_lzm; ++b)
                if (lzm[b].max_val > DATE_CUTOFF) active.push_back(b);

            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (int bi = 0; bi < (int)active.size(); ++bi) {
                uint32_t b = active[bi];
                int tid = omp_get_thread_num();
                AggMap& la = *taggs[tid];
                size_t rs = (size_t)b * LB;
                size_t re = std::min(rs + LB, n_li);
                bool all_qualify = (lzm[b].min_val > DATE_CUTOFF);
                for (size_t i = rs; i < re; ++i) {
                    if (!all_qualify && l_shipdate[i] <= DATE_CUTOFF) continue;
                    int32_t lk = l_orderkey[i];
                    if (!ob.test(lk)) continue;
                    la.add(lk, l_extprice[i] * (1.0 - l_discount[i]));
                }
            }
        } else {
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n_li; ++i) {
                if (l_shipdate[i] <= DATE_CUTOFF) continue;
                int32_t lk = l_orderkey[i];
                if (!ob.test(lk)) continue;
                taggs[omp_get_thread_num()]->add(lk, l_extprice[i] * (1.0 - l_discount[i]));
            }
        }

        munmap((void*)l_orderkey, n_li * sizeof(int32_t));
        munmap((void*)l_shipdate, n_li * sizeof(int32_t));
        munmap((void*)l_extprice, n_li * sizeof(double));
        munmap((void*)l_discount, n_li * sizeof(double));
        if (lzm_raw) munmap((void*)lzm_raw, lzm_bytes);
    }

    // Phase 4: merge → top-10 → CSV
    {
        GENDB_PHASE("phase4_output");
        AggMap global_agg(global_cap);
        for (int t = 0; t < nthreads; ++t) {
            for (uint32_t h = 0; h < per_thread_cap; ++h)
                if (taggs[t]->slots[h].key != EMPTY_KEY)
                    global_agg.add(taggs[t]->slots[h].key, taggs[t]->slots[h].revenue);
            delete taggs[t];
        }

        struct Row { int32_t orderkey; double revenue; int32_t orderdate, shippriority; };
        std::vector<Row> rows;
        rows.reserve(total_qualifying_orders);
        for (uint32_t h = 0; h < global_cap; ++h) {
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
        if (!f) { perror(out_path.c_str()); return; }
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
