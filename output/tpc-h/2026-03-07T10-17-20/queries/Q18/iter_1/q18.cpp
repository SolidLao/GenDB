// Q18: Large Volume Customer — iter_1
// Two-pass morsel-driven parallel execution.
//
// Pass 1 Phase A (parallel): Scatter (l_orderkey, l_quantity) pairs to P=64
//   pre-allocated flat partition buffers. No hash map, no malloc in parallel.
//   Fix vs iter_0: buffers pre-allocated outside parallel region → zero malloc.
// Pass 1 Phase B (parallel): Each thread t gathers its P=64 scatter buffers
//   (one from each source thread) and aggregates into a compact QtyMap.
//   ~234K distinct keys per partition at 45% LF → no overflow.
// Pass 1 Phase C (serial): filter qualifying keys (sum>300) into TinyHashSet.
//
// Pass 2 (parallel): Scan lineitem, filter via L1-resident TinyHashSet,
//   index-probe orders + customer, accumulate thread-local group maps, merge.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

// ── Hash function (verbatim from build_indexes.cpp) ──────────────────────────
static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

// ── Probe open-addressing int32→int32 hash index ─────────────────────────────
static inline int32_t probe_index(const int32_t* __restrict__ keys,
                                   const int32_t* __restrict__ vals,
                                   uint64_t mask, int32_t key) {
    uint64_t h = (uint64_t)hash32((uint32_t)key) & mask;
    while (keys[h] != -1) {
        if (keys[h] == key) return vals[h];
        h = (h + 1) & mask;
    }
    return -1;
}

// ── Gregorian calendar: int32_t days since 1970-01-01 → "YYYY-MM-DD" ────────
static void decode_date(int32_t days, char out[11]) {
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    int32_t mp  = (5*doy + 2) / 153;
    int32_t d   = doy - (153*mp + 2)/5 + 1;
    int32_t m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
    snprintf(out, 11, "%04d-%02d-%02d", y, m, d);
}

// ── Compact open-addressing hash map: int32_t → double (SOA layout) ──────────
// Pre-allocated externally; sentinel: INT32_MIN.
struct QtyMap {
    static constexpr int32_t SENTINEL = INT32_MIN;
    uint32_t cap, mask;
    int32_t* keys;
    double*  vals;

    QtyMap() : cap(0), mask(0), keys(nullptr), vals(nullptr) {}

    void assign(uint32_t capacity, int32_t* k, double* v) {
        cap  = capacity;
        mask = capacity - 1;
        keys = k;
        vals = v;
    }

    void full_clear() {
        std::fill(keys, keys + cap, SENTINEL);
        std::fill(vals, vals + cap, 0.0);
    }

    __attribute__((always_inline))
    void add(int32_t key, double qty) {
        uint32_t h = hash32((uint32_t)key) & mask;
        while (keys[h] != SENTINEL && keys[h] != key)
            h = (h + 1) & mask;
        keys[h] = key;
        vals[h] += qty;
    }

    // Iterate AND clear occupied slots
    template<typename Fn>
    void for_each_and_clear(Fn&& fn) {
        for (uint32_t i = 0; i < cap; i++) {
            if (keys[i] != SENTINEL) {
                fn(keys[i], vals[i]);
                keys[i] = SENTINEL;
                vals[i] = 0.0;
            }
        }
    }
};

// ── Tiny flat hash set (qualifying orderkeys ~624 entries, 2048 slots → L1) ──
struct TinyHashSet {
    static constexpr uint32_t CAP   = 2048;
    static constexpr uint32_t MASK  = CAP - 1;
    static constexpr int32_t  EMPTY = INT32_MIN;
    int32_t slots[CAP];

    TinyHashSet() { std::fill(slots, slots + CAP, EMPTY); }

    void insert(int32_t key) {
        uint32_t h = hash32((uint32_t)key) & MASK;
        while (slots[h] != EMPTY && slots[h] != key)
            h = (h + 1) & MASK;
        slots[h] = key;
    }

    __attribute__((always_inline))
    bool contains(int32_t key) const {
        uint32_t h = hash32((uint32_t)key) & MASK;
        while (slots[h] != EMPTY) {
            if (slots[h] == key) return true;
            h = (h + 1) & MASK;
        }
        return false;
    }
};

// ── Pass-2 group entry ────────────────────────────────────────────────────────
struct GroupEntry {
    double  sum_qty;
    int32_t orders_row;
    int32_t cust_row;
};

// ── Result row ────────────────────────────────────────────────────────────────
struct Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb_dir(argv[1]);
    std::string results_dir(argv[2]);
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE_MS("total", total_ms);

    // =========================================================================
    // DATA LOADING
    // =========================================================================
    MmapColumn<int32_t> l_orderkey;
    MmapColumn<double>  l_quantity;
    MmapColumn<int32_t> o_custkey;
    MmapColumn<int32_t> o_orderdate;
    MmapColumn<double>  o_totalprice;
    MmapColumn<char>    c_name_raw;

    void*    okey_ptr = nullptr; size_t okey_sz = 0;
    void*    ckey_ptr = nullptr; size_t ckey_sz = 0;
    const int32_t* o_hash_keys = nullptr;
    const int32_t* o_hash_vals = nullptr;
    uint64_t o_mask = 0;
    const int32_t* c_hash_keys = nullptr;
    const int32_t* c_hash_vals = nullptr;
    uint64_t c_mask = 0;
    const char* c_name_buf = nullptr;

    {
        GENDB_PHASE("data_loading");

        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        mmap_prefetch_all(l_orderkey, l_quantity);

        {
            std::string p = gendb_dir + "/orders/o_orderkey_hash.bin";
            int fd = open(p.c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            okey_sz = st.st_size;
            okey_ptr = mmap(nullptr, okey_sz, PROT_READ, MAP_PRIVATE, fd, 0);
            close(fd);
            int64_t cap = ((int64_t*)okey_ptr)[0];
            o_mask      = (uint64_t)(cap - 1);
            o_hash_keys = (const int32_t*)((char*)okey_ptr + 16);
            o_hash_vals = o_hash_keys + cap;
            madvise((void*)o_hash_keys, (size_t)cap * 8, MADV_RANDOM);
        }

        {
            std::string p = gendb_dir + "/customer/c_custkey_hash.bin";
            int fd = open(p.c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st);
            ckey_sz = st.st_size;
            ckey_ptr = mmap(nullptr, ckey_sz, PROT_READ, MAP_PRIVATE, fd, 0);
            close(fd);
            int64_t cap = ((int64_t*)ckey_ptr)[0];
            c_mask      = (uint64_t)(cap - 1);
            c_hash_keys = (const int32_t*)((char*)ckey_ptr + 16);
            c_hash_vals = c_hash_keys + cap;
            madvise((void*)c_hash_keys, (size_t)cap * 8, MADV_RANDOM);
        }

        o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        o_totalprice.open(gendb_dir + "/orders/o_totalprice.bin");

        c_name_raw.open(gendb_dir + "/customer/c_name.bin");
        c_name_buf = (const char*)c_name_raw.data;
    }

    const int64_t  nlineitem = (int64_t)l_orderkey.size();
    const int32_t* lok = l_orderkey.data;
    const double*  lqt = l_quantity.data;

    const int nthreads = omp_get_max_threads();

    // P = next power-of-2 >= nthreads for hash partitioning
    int P = 1;
    while (P < nthreads) P <<= 1;
    const uint32_t P_mask = (uint32_t)(P - 1);

    // =========================================================================
    // PASS 1: Scatter/gather with pre-allocated flat buffers (no malloc)
    //
    // Key fix vs iter_0: ALL scatter buffer storage pre-allocated as two flat
    // arrays outside the parallel region. No push_back, no malloc, no realloc.
    //
    // Actual data: ~567K distinct orderkeys per thread chunk (lineitem sorted by
    // l_shipdate), so Phase A direct QtyMap aggregation would overflow at
    // SUB_CAP=524K. Scatter avoids this: Phase A just writes pairs, Phase B
    // aggregates by partition (234K distinct keys per partition = 45% LF, fine).
    // =========================================================================
    TinyHashSet qualifying_set;
    size_t n_qualifying = 0;

    {
        GENDB_PHASE("pass1_parallel_direct_aggregate");

        // ── Pre-allocate scatter buffer storage ────────────────────────────────
        // Layout: sc_k[tid * P * MAX_PER_BUF + p * MAX_PER_BUF + entry_idx]
        // MAX_PER_BUF = 32768 (2× expected 14.6K entries/buffer)
        // Total: 64 × 64 × 32K × 12 bytes = ~1.57GB
        //
        // CRITICAL: use malloc (lazy allocation, no serial page faults) rather than
        // std::vector (value-init writes 1.6GB serially → 400K page faults = ~2s).
        // Parallel pre-fault in the OpenMP region faults pages in parallel with
        // first-touch NUMA placement. sc_k/sc_v are write-before-read (guarded by
        // sc_cnt), so no initialization value is needed — just page presence.
        const int32_t MAX_PER_BUF = 32768;
        const int64_t sc_stride   = (int64_t)P * MAX_PER_BUF;  // per-thread scatter stride
        const size_t  sc_n        = (size_t)nthreads * (size_t)sc_stride;

        int32_t* sc_k = (int32_t*)malloc(sc_n * sizeof(int32_t));  // uninitialized (lazy pages)
        double*  sc_v = (double*) malloc(sc_n * sizeof(double));    // uninitialized (lazy pages)
        std::vector<int32_t> sc_cnt((size_t)nthreads * P, 0);  // MUST be zero; tiny (16KB)

        // ── Pre-allocate merge map storage for Phase B ─────────────────────────
        // MERGE_CAP = 524288 = 2^19 (512K slots @ 45% LF for 234K distinct keys)
        const uint32_t MERGE_CAP = 524288u;
        std::vector<int32_t> mrg_k_storage((size_t)nthreads * MERGE_CAP);
        std::vector<double>  mrg_v_storage((size_t)nthreads * MERGE_CAP);

        // Per-thread qualifying keys
        std::vector<std::vector<int32_t>> tq(nthreads);

        // ── Parallel region ────────────────────────────────────────────────────
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int nt  = omp_get_num_threads();

            // Initialize merge map with first-touch for NUMA placement
            int32_t* mrg_k = mrg_k_storage.data() + (size_t)tid * MERGE_CAP;
            double*  mrg_v = mrg_v_storage.data() + (size_t)tid * MERGE_CAP;
            std::fill(mrg_k, mrg_k + MERGE_CAP, QtyMap::SENTINEL);
            std::fill(mrg_v, mrg_v + MERGE_CAP, 0.0);

            // Pre-fault scatter buffer pages for THIS thread (parallel page faults,
            // NUMA-aware). sc_k and sc_v are write-before-read so we just need the
            // pages present. memset does a write touch of each page.
            int32_t* my_sc_k_init = sc_k + (size_t)tid * sc_stride;
            double*  my_sc_v_init = sc_v + (size_t)tid * sc_stride;
            memset(my_sc_k_init, 0, (size_t)sc_stride * sizeof(int32_t));
            memset(my_sc_v_init, 0, (size_t)sc_stride * sizeof(double));

            #pragma omp barrier  // all maps initialized + scatter pages pre-faulted

            // ── Phase A: scatter (key, val) to partition buffers ───────────────
            // No hash map, no malloc. Each thread writes to ITS OWN set of P
            // scatter buffers (indexed [tid * P * MAX + p * MAX + count]).
            int64_t chunk = (nlineitem + nt - 1) / nt;
            int64_t row0  = (int64_t)tid * chunk;
            int64_t row1  = std::min(row0 + chunk, nlineitem);

            int32_t* my_sc_k = sc_k + (size_t)tid * sc_stride;
            double*  my_sc_v = sc_v + (size_t)tid * sc_stride;
            int32_t* my_cnt  = sc_cnt.data() + tid * P;

            for (int64_t i = row0; i < row1; i++) {
                int32_t k = lok[i];
                double  v = lqt[i];
                int32_t p = (int32_t)(hash32((uint32_t)k) & P_mask);
                int32_t c = my_cnt[p];
                my_sc_k[p * MAX_PER_BUF + c] = k;
                my_sc_v[p * MAX_PER_BUF + c] = v;
                my_cnt[p] = c + 1;
            }

            #pragma omp barrier  // Phase A complete — all scatter buffers filled

            // ── Phase B: gather + aggregate partition tid ──────────────────────
            // Thread tid gathers from all nt source threads' buffers for partition tid.
            // Expected: nt * (nlineitem/nt/P) = nlineitem/P = 937K entries.
            // Distinct keys: 15M/P = 234K → MERGE_CAP=524K at 45% LF. No overflow.
            QtyMap merge_map;
            merge_map.assign(MERGE_CAP, mrg_k, mrg_v);

            for (int s = 0; s < nt; s++) {
                int32_t cnt = sc_cnt[s * P + tid];
                const int32_t* sk = sc_k + (size_t)s * sc_stride + (size_t)tid * MAX_PER_BUF;
                const double*  sv = sc_v + (size_t)s * sc_stride + (size_t)tid * MAX_PER_BUF;
                for (int32_t j = 0; j < cnt; j++) {
                    merge_map.add(sk[j], sv[j]);
                }
            }

            // Emit qualifying keys (sum > 300.0)
            merge_map.for_each_and_clear([&](int32_t key, double val) {
                if (val > 300.0) tq[tid].push_back(key);
            });

        } // end parallel

        // ── Phase C: Serial union into TinyHashSet (~624 keys) ─────────────────
        {
            GENDB_PHASE("pass1_serial_build_qualifying_set");
            for (int t = 0; t < nthreads; t++) {
                for (int32_t key : tq[t]) {
                    qualifying_set.insert(key);
                    ++n_qualifying;
                }
            }
        }
    }

    fprintf(stderr, "[INFO] Qualifying orderkeys: %zu\n", n_qualifying);

    // =========================================================================
    // PASS 2: Parallel scan — filter, join, aggregate
    // =========================================================================
    std::vector<std::unordered_map<int32_t, GroupEntry>> tl_groups(nthreads);
    for (auto& m : tl_groups) m.reserve(512);

    {
        GENDB_PHASE("pass2_scan_lineitem_filter_join_aggregate");

        const int32_t* ocust_col = o_custkey.data;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int nt  = omp_get_num_threads();
            int64_t chunk = (nlineitem + nt - 1) / nt;
            int64_t row0  = (int64_t)tid * chunk;
            int64_t row1  = std::min(row0 + chunk, nlineitem);

            auto& local_groups = tl_groups[tid];

            for (int64_t i = row0; i < row1; i++) {
                int32_t okey = lok[i];
                if (!qualifying_set.contains(okey)) continue;

                int32_t orow  = probe_index(o_hash_keys, o_hash_vals, o_mask, okey);
                int32_t ocust = ocust_col[orow];
                int32_t crow  = probe_index(c_hash_keys, c_hash_vals, c_mask, ocust);

                auto& g      = local_groups[okey];
                g.sum_qty   += lqt[i];
                g.orders_row = orow;
                g.cust_row   = crow;
            }
        }
    }

    // =========================================================================
    // PASS 2 MERGE: Single-threaded reduce (~624 groups)
    // =========================================================================
    std::unordered_map<int32_t, GroupEntry> global_groups;

    {
        GENDB_PHASE("pass2_merge_groups");
        global_groups.reserve(1024);
        for (int t = 0; t < nthreads; t++) {
            for (auto& [key, entry] : tl_groups[t]) {
                auto& g      = global_groups[key];
                g.sum_qty   += entry.sum_qty;
                g.orders_row = entry.orders_row;
                g.cust_row   = entry.cust_row;
            }
        }
        tl_groups.clear();
    }

    // =========================================================================
    // SORT + TOP-100 + OUTPUT
    // =========================================================================
    {
        GENDB_PHASE("sort_topk");

        const int32_t* odate_col  = o_orderdate.data;
        const double*  oprice_col = o_totalprice.data;
        const int32_t* ocust_col  = o_custkey.data;

        std::vector<Result> results;
        results.reserve(global_groups.size());

        for (auto& [okey, g] : global_groups) {
            int32_t orow = g.orders_row;
            int32_t crow = g.cust_row;
            Result r;
            r.o_orderkey   = okey;
            r.o_orderdate  = odate_col[orow];
            r.o_totalprice = oprice_col[orow];
            r.c_custkey    = ocust_col[orow];
            r.sum_qty      = g.sum_qty;
            memcpy(r.c_name, c_name_buf + (ptrdiff_t)crow * 26, 26);
            r.c_name[25] = '\0';
            results.push_back(r);
        }

        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        });

        {
            GENDB_PHASE("output");

            std::string out_path = results_dir + "/Q18.csv";
            FILE* fp = fopen(out_path.c_str(), "w");
            if (!fp) { fprintf(stderr, "Cannot open: %s\n", out_path.c_str()); return 1; }

            fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

            int limit = (int)std::min(results.size(), (size_t)100);
            char date_buf[11];
            for (int i = 0; i < limit; i++) {
                const Result& r = results[i];
                decode_date(r.o_orderdate, date_buf);
                fprintf(fp, "%s,%d,%d,%s,%.2f,%.2f\n",
                        r.c_name, r.c_custkey, r.o_orderkey,
                        date_buf, r.o_totalprice, r.sum_qty);
            }
            fclose(fp);
        }
    }

    munmap(okey_ptr, okey_sz);
    munmap(ckey_ptr, ckey_sz);
    return 0;
}
