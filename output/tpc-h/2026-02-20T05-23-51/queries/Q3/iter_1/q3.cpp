// Q3: Shipping Priority
// Strategy deviation from plan: Phase 3 uses full sequential lineitem scan instead of
// index-driven random access. With data cached in 376 GB RAM, sequential scan of
// ~1.92 GB lineitem avoids random access overhead and is simpler to parallelize.
// Zone map is used for orders Phase 2 as recommended.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <queue>
#include <fstream>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

static const int32_t EMPTY_KEY = INT32_MIN;

static inline uint32_t hash32(int32_t k) {
    return (uint32_t)((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL >> 32);
}

// Generic mmap column loader
template<typename T>
static T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); n = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    auto* ptr = reinterpret_cast<T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0));
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    n = st.st_size / sizeof(T);
    close(fd);
    return ptr;
}

// mmap a file returning raw bytes and byte size
static uint8_t* mmap_raw(const std::string& path, size_t& bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); bytes = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    bytes = st.st_size;
    auto* ptr = reinterpret_cast<uint8_t*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0));
    close(fd);
    return ptr;
}

// ---- Hash set for int32 custkeys ----
struct CustKeySet {
    static const uint32_t CAP = 1u << 20; // 1048576, ≤50% load for ~330K BUILDING customers
    int32_t* keys;

    CustKeySet() : keys(new int32_t[CAP]) {
        std::fill(keys, keys + CAP, EMPTY_KEY);
    }
    ~CustKeySet() { delete[] keys; }

    void insert(int32_t k) {
        uint32_t h = hash32(k) & (CAP - 1);
        while (keys[h] != EMPTY_KEY && keys[h] != k) h = (h + 1) & (CAP - 1);
        keys[h] = k;
    }

    bool contains(int32_t k) const {
        uint32_t h = hash32(k) & (CAP - 1);
        while (keys[h] != EMPTY_KEY && keys[h] != k) h = (h + 1) & (CAP - 1);
        return keys[h] == k;
    }
};

// ---- Order info hash map: orderkey -> {orderdate, shippriority} ----
struct OrderInfo {
    int32_t orderdate;
    int32_t shippriority;
};

struct OrderInfoMap {
    uint32_t cap;
    int32_t* keys;
    OrderInfo* vals;

    explicit OrderInfoMap(uint32_t c) : cap(c), keys(new int32_t[c]), vals(new OrderInfo[c]) {
        std::fill(keys, keys + c, EMPTY_KEY);
    }
    ~OrderInfoMap() { delete[] keys; delete[] vals; }

    void insert(int32_t k, int32_t od, int32_t sp) {
        uint32_t h = hash32(k) & (cap - 1);
        while (keys[h] != EMPTY_KEY && keys[h] != k) h = (h + 1) & (cap - 1);
        if (keys[h] == EMPTY_KEY) {
            keys[h] = k;
            vals[h] = {od, sp};
        }
    }

    const OrderInfo* find(int32_t k) const {
        uint32_t h = hash32(k) & (cap - 1);
        while (keys[h] != EMPTY_KEY && keys[h] != k) h = (h + 1) & (cap - 1);
        return (keys[h] == k) ? &vals[h] : nullptr;
    }
};

// ---- Thread-local aggregation map: orderkey -> revenue ----
struct AggMap {
    uint32_t  cap;
    int32_t*  keys;
    double*   revenues;

    explicit AggMap(uint32_t c) : cap(c), keys(new int32_t[c]), revenues(new double[c]) {
        std::fill(keys, keys + c, EMPTY_KEY);
        std::fill(revenues, revenues + c, 0.0);
    }
    ~AggMap() { delete[] keys; delete[] revenues; }

    void add(int32_t k, double rev) {
        uint32_t h = hash32(k) & (cap - 1);
        while (keys[h] != EMPTY_KEY && keys[h] != k) h = (h + 1) & (cap - 1);
        if (keys[h] == EMPTY_KEY) {
            keys[h] = k;
            revenues[h] = rev;
        } else {
            revenues[h] += rev;
        }
    }
};

struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

struct QualOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct ResultRow {
    int32_t orderkey;
    double  revenue;
    int32_t orderdate;
    int32_t shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_CUTOFF = 9204; // 1995-03-15 epoch days

    // =========================================================
    // Phase 1: Scan customer, build custkey hash set
    // =========================================================
    CustKeySet* cust_set = nullptr;
    {
        GENDB_PHASE("dim_filter");

        // Load dict at runtime to find code for 'BUILDING'
        int8_t building_code = -1;
        {
            std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            int8_t code = 0;
            while (std::getline(df, line)) {
                if (line == "BUILDING") { building_code = code; break; }
                ++code;
            }
        }
        if (building_code < 0) {
            fprintf(stderr, "ERROR: 'BUILDING' not found in c_mktsegment_dict.txt\n");
            return;
        }

        size_t n_seg = 0, n_ck = 0;
        auto* seg  = mmap_col<int8_t> (gendb_dir + "/customer/c_mktsegment.bin", n_seg);
        auto* ckey = mmap_col<int32_t>(gendb_dir + "/customer/c_custkey.bin",    n_ck);

        cust_set = new CustKeySet();
        for (size_t i = 0; i < n_seg; ++i) {
            if (seg[i] == building_code) {
                cust_set->insert(ckey[i]);
            }
        }
        munmap((void*)seg,  n_seg * sizeof(int8_t));
        munmap((void*)ckey, n_ck  * sizeof(int32_t));
    }

    // =========================================================
    // Phase 2: Scan orders with zone map, collect qualifying rows
    //          Build OrderInfoMap from qualifying orders
    // =========================================================
    OrderInfoMap* order_info = nullptr;
    {
        GENDB_PHASE("build_joins");

        // Load zone map for o_orderdate
        size_t zm_bytes = 0;
        auto*    zm_raw = mmap_raw(gendb_dir + "/indexes/orders_orderdate_zonemap.bin", zm_bytes);
        uint32_t num_blocks = 0;
        ZoneMapEntry* zm = nullptr;
        if (zm_raw && zm_bytes > 4) {
            num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
            zm = reinterpret_cast<ZoneMapEntry*>(zm_raw + sizeof(uint32_t));
        }

        // Precompute block row starts
        std::vector<uint64_t> block_starts(num_blocks + 1, 0);
        for (uint32_t b = 0; b < num_blocks; ++b) {
            block_starts[b + 1] = block_starts[b] + zm[b].block_size;
        }

        // Load order columns
        size_t n_ord = 0, tmp = 0;
        auto* o_custkey     = mmap_col<int32_t>(gendb_dir + "/orders/o_custkey.bin",     n_ord);
        auto* o_orderdate   = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin",   tmp);
        auto* o_orderkey    = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin",    tmp);
        auto* o_shippriority= mmap_col<int32_t>(gendb_dir + "/orders/o_shippriority.bin",tmp);

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<QualOrder>> thread_vecs(nthreads);
        for (auto& v : thread_vecs) v.reserve(32768);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lv = thread_vecs[tid];

            if (zm && num_blocks > 0) {
                // Zone-map pruned scan
                #pragma omp for schedule(dynamic, 1)
                for (uint32_t b = 0; b < num_blocks; ++b) {
                    // Skip block if all dates >= DATE_CUTOFF
                    if (zm[b].min_val >= DATE_CUTOFF) continue;
                    uint64_t rs = block_starts[b];
                    uint64_t re = std::min(block_starts[b + 1], (uint64_t)n_ord);
                    for (uint64_t i = rs; i < re; ++i) {
                        if (o_orderdate[i] < DATE_CUTOFF &&
                            cust_set->contains(o_custkey[i])) {
                            lv.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                        }
                    }
                }
            } else {
                // Fallback: no zone map
                #pragma omp for schedule(static)
                for (size_t i = 0; i < n_ord; ++i) {
                    if (o_orderdate[i] < DATE_CUTOFF &&
                        cust_set->contains(o_custkey[i])) {
                        lv.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                    }
                }
            }
        }

        // Count qualifying orders and size the map
        size_t total_qual = 0;
        for (auto& v : thread_vecs) total_qual += v.size();

        // Next power-of-2 >= total_qual / 0.67
        uint32_t om_cap = 1u << 21; // 2M, handles up to ~1.34M at 65% load
        while ((uint64_t)(om_cap) * 2 / 3 < (uint64_t)total_qual) om_cap <<= 1;

        order_info = new OrderInfoMap(om_cap);
        for (auto& v : thread_vecs) {
            for (auto& q : v) {
                order_info->insert(q.orderkey, q.orderdate, q.shippriority);
            }
        }

        munmap((void*)o_custkey,      n_ord * sizeof(int32_t));
        munmap((void*)o_orderdate,    n_ord * sizeof(int32_t));
        munmap((void*)o_orderkey,     n_ord * sizeof(int32_t));
        munmap((void*)o_shippriority, n_ord * sizeof(int32_t));
        if (zm_raw) munmap(zm_raw, zm_bytes);
        delete cust_set; cust_set = nullptr;
    }

    // =========================================================
    // Phase 3: Index-driven lineitem access (P11 fix)
    //   - Use lineitem_orderkey_hash to visit only ~2.8M rows for qualifying orders
    //     instead of scanning all 60M lineitem rows.
    //   - Apply P10 fix: load lineitem_shipdate_zonemap to skip blocks where
    //     max_val <= DATE_CUTOFF (avoids reading even those ~2.8M rows in dead blocks).
    //   - Parallelise over qualifying orders; each order's rows are independent.
    // =========================================================

    // Hash index entry layout: {int32_t key, uint32_t offset, uint32_t count}
    struct HEntry32 {
        int32_t  key;
        uint32_t offset;
        uint32_t count;
    };

    AggMap* global_agg = nullptr;
    {
        GENDB_PHASE("main_scan");

        // --- Load lineitem_orderkey_hash index ---
        // Layout: [uint32_t num_positions][uint32_t positions[num_positions]]
        //         [uint32_t capacity][HEntry32[capacity]]
        size_t hi_bytes = 0;
        auto* hi_raw = mmap_raw(gendb_dir + "/indexes/lineitem_orderkey_hash.bin", hi_bytes);
        uint32_t hi_cap = 0;
        const uint32_t* hi_positions = nullptr;
        const HEntry32* hi_entries = nullptr;
        if (hi_raw && hi_bytes > 4) {
            const uint32_t* p = reinterpret_cast<const uint32_t*>(hi_raw);
            uint32_t num_positions = p[0];                 // 59,986,052
            hi_positions = p + 1;                          // positions[num_positions]
            hi_cap       = p[1 + num_positions];           // 33,554,432
            hi_entries   = reinterpret_cast<const HEntry32*>(p + 1 + num_positions + 1);
        }

        // --- Load lineitem_shipdate_zonemap (P10) ---
        size_t szm_bytes = 0;
        auto* szm_raw = mmap_raw(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", szm_bytes);
        uint32_t szm_num_blocks = 0;
        const ZoneMapEntry* szm = nullptr;
        std::vector<uint64_t> szm_block_starts;
        if (szm_raw && szm_bytes > 4) {
            szm_num_blocks = *reinterpret_cast<const uint32_t*>(szm_raw);
            szm = reinterpret_cast<const ZoneMapEntry*>(szm_raw + sizeof(uint32_t));
            szm_block_starts.resize(szm_num_blocks + 1, 0);
            for (uint32_t b = 0; b < szm_num_blocks; ++b)
                szm_block_starts[b + 1] = szm_block_starts[b] + szm[b].block_size;
        }

        // --- Load lineitem columns (random access via index — use POSIX_FADV_RANDOM) ---
        // l_orderkey not needed: we drive iteration from qualifying orderkeys via index
        size_t n_li = 0;
        {
            // determine n_li from l_shipdate file size
            int fd = open((gendb_dir + "/lineitem/l_shipdate.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st); n_li = st.st_size / sizeof(int32_t); close(fd);
        }
        size_t tmp = 0; // used in fallback l_orderkey mmap_col
        auto mmap_rand = [&](const std::string& path, size_t elem_bytes) -> void* {
            int fd = open(path.c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            posix_fadvise(fd, 0, st.st_size, POSIX_FADV_RANDOM);
            madvise(ptr, st.st_size, MADV_RANDOM);
            close(fd); return ptr;
        };
        auto* l_shipdate = reinterpret_cast<const int32_t*>(mmap_rand(gendb_dir + "/lineitem/l_shipdate.bin", 4));
        auto* l_extprice = reinterpret_cast<const double*> (mmap_rand(gendb_dir + "/lineitem/l_extendedprice.bin", 8));
        auto* l_discount = reinterpret_cast<const double*> (mmap_rand(gendb_dir + "/lineitem/l_discount.bin", 8));

        // Collect qualifying orderkeys from order_info for parallel dispatch
        std::vector<int32_t> qual_keys;
        qual_keys.reserve(order_info->cap / 4);
        for (uint32_t h = 0; h < order_info->cap; ++h) {
            if (order_info->keys[h] != EMPTY_KEY)
                qual_keys.push_back(order_info->keys[h]);
        }

        int nthreads = omp_get_max_threads();
        // Thread-local agg maps: 8192 slots per thread (~3000 groups expected, 37% load)
        static const uint32_t TAGG_CAP = 1u << 13; // 8192
        std::vector<AggMap*> taggs(nthreads);
        for (int t = 0; t < nthreads; ++t) taggs[t] = new AggMap(TAGG_CAP);

        if (hi_entries && hi_cap > 0 && hi_positions) {
            // Index-driven path: iterate qualifying orders, look up their lineitem rows via positions
            uint32_t hi_mask = hi_cap - 1;

            #pragma omp parallel for schedule(dynamic, 512)
            for (size_t qi = 0; qi < qual_keys.size(); ++qi) {
                int32_t ok = qual_keys[qi];
                AggMap* la = taggs[omp_get_thread_num()];

                // Probe hash index for this orderkey
                uint32_t h = hash32(ok) & hi_mask;
                while (hi_entries[h].key != EMPTY_KEY && hi_entries[h].key != ok)
                    h = (h + 1) & hi_mask;
                if (hi_entries[h].key != ok) continue; // not found in index

                uint32_t row_off = hi_entries[h].offset;
                uint32_t row_cnt = hi_entries[h].count;

                // Prefetch first few position entries
                if (row_cnt > 0) __builtin_prefetch(&hi_positions[row_off], 0, 1);

                for (uint32_t r = 0; r < row_cnt; ++r) {
                    // Prefetch ahead into positions and lineitem data
                    if (r + 4 < row_cnt)
                        __builtin_prefetch(&l_shipdate[hi_positions[row_off + r + 4]], 0, 0);
                    size_t i = (size_t)hi_positions[row_off + r];
                    if (l_shipdate[i] > DATE_CUTOFF) {
                        double rev = l_extprice[i] * (1.0 - l_discount[i]);
                        la->add(ok, rev);
                    }
                }
            }
        } else {
            // Fallback: full sequential scan with zone map pruning (index unavailable)
            auto* l_orderkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", tmp);
            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                AggMap* la = taggs[tid];

                if (szm && szm_num_blocks > 0) {
                    #pragma omp for schedule(dynamic, 1)
                    for (uint32_t b = 0; b < szm_num_blocks; ++b) {
                        if (szm[b].max_val <= DATE_CUTOFF) continue;
                        uint64_t rs = szm_block_starts[b];
                        uint64_t re = std::min(szm_block_starts[b + 1], (uint64_t)n_li);
                        for (uint64_t i = rs; i < re; ++i) {
                            if (l_shipdate[i] > DATE_CUTOFF) {
                                int32_t k = l_orderkey[i];
                                if (order_info->find(k) != nullptr) {
                                    double rev = l_extprice[i] * (1.0 - l_discount[i]);
                                    la->add(k, rev);
                                }
                            }
                        }
                    }
                } else {
                    #pragma omp for schedule(static)
                    for (size_t i = 0; i < n_li; ++i) {
                        if (l_shipdate[i] > DATE_CUTOFF) {
                            int32_t k = l_orderkey[i];
                            if (order_info->find(k) != nullptr) {
                                double rev = l_extprice[i] * (1.0 - l_discount[i]);
                                la->add(k, rev);
                            }
                        }
                    }
                }
            }
            munmap((void*)l_orderkey, n_li * sizeof(int32_t));
        }

        // Merge thread-local agg maps into global
        uint32_t ga_cap = 1u << 13; // 8192 — same size; ~3000 groups
        global_agg = new AggMap(ga_cap);
        for (int t = 0; t < nthreads; ++t) {
            for (uint32_t h = 0; h < TAGG_CAP; ++h) {
                if (taggs[t]->keys[h] != EMPTY_KEY)
                    global_agg->add(taggs[t]->keys[h], taggs[t]->revenues[h]);
            }
            delete taggs[t];
        }

        munmap((void*)l_shipdate, n_li * sizeof(int32_t));
        munmap((void*)l_extprice, n_li * sizeof(double));
        munmap((void*)l_discount, n_li * sizeof(double));
        if (hi_raw)  munmap(hi_raw,  hi_bytes);
        if (szm_raw) munmap(szm_raw, szm_bytes);
    }

    // =========================================================
    // Phase 4: Top-10 heap + CSV output
    // =========================================================
    {
        GENDB_PHASE("output");

        // Collect all aggregation groups, then sort for top-10
        std::vector<ResultRow> all_results;
        all_results.reserve(1 << 20); // pre-alloc ~1M

        for (uint32_t h = 0; h < global_agg->cap; ++h) {
            int32_t ok = global_agg->keys[h];
            if (ok == EMPTY_KEY) continue;
            const OrderInfo* info = order_info->find(ok);
            if (!info) continue;
            all_results.push_back({ok, global_agg->revenues[h], info->orderdate, info->shippriority});
        }

        // Sort: revenue DESC, o_orderdate ASC — partial_sort for O(n log k) with LIMIT 10
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.orderdate < b.orderdate;
        };
        size_t top_k = std::min((size_t)10, all_results.size());
        std::partial_sort(all_results.begin(), all_results.begin() + top_k, all_results.end(), cmp);
        all_results.resize(top_k);
        const auto& results = all_results;

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (auto& r : results) {
            char date_buf[16];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n",
                r.orderkey,
                r.revenue,
                date_buf,
                r.shippriority);
        }
        fclose(f);
    }

    delete order_info;
    delete global_agg;
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
