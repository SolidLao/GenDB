// Q18: Large Volume Customer
// Two-pass morsel-driven parallel execution.
//
// Pass 1: Parallel scatter/gather directly from lineitem raw data:
//   Phase A (parallel): each thread scatters its row chunk to P=64 partition
//     buffers (keyed by hash32(orderkey) & P_mask). No intermediate hash maps.
//   Phase B (parallel): each thread reduces its partition's scatter data into
//     a pre-allocated 512K QtyMap (cleared between uses).
//
// Pass 2: parallel scan lineitem, filter via 2048-slot flat hash set (L1),
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
// Pre-allocated externally; supports sparse clear after for_each.
// Sentinel: INT32_MIN.
struct QtyMap {
    static constexpr int32_t SENTINEL = INT32_MIN;
    uint32_t cap, mask;
    int32_t* keys;
    double*  vals;

    QtyMap() : cap(0), mask(0), keys(nullptr), vals(nullptr) {}

    // Initialize with externally provided buffers (no allocation)
    void assign(uint32_t capacity, int32_t* k, double* v) {
        cap  = capacity;
        mask = capacity - 1;
        keys = k;
        vals = v;
    }

    // Full clear (for initial setup or full reset)
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

    // Iterate AND clear occupied slots (no separate clear pass needed).
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

    // =========================================================================
    // PASS 1: Scatter/gather from raw lineitem → qualifying set
    //
    // P = next power-of-2 ≥ nthreads (64 on 64-core machine).
    // Sub-map capacity SUB_CAP = 512K (2^19) handles ~234K distinct keys at
    // 45% load (15M keys / 64 partitions = 234K/partition).
    // Sub-maps are pre-allocated and cleared via for_each_and_clear.
    // Scatter buffers: vectors (no reserve → natural growth avoids large upfront alloc).
    // =========================================================================
    TinyHashSet qualifying_set;
    size_t n_qualifying = 0;

    {
        GENDB_PHASE("pass1_scan_lineitem_build_qty_sums");

        int P = 1;
        while (P < nthreads) P <<= 1;
        const uint32_t P_mask = (uint32_t)(P - 1);
        const int N_BUF = nthreads * P;

        // Scatter buffers: no upfront reserve → vectors grow naturally.
        // For 14.6K final entries, ~4 doublings = 4 malloc calls per buffer.
        std::vector<std::vector<int32_t>> sc_keys(N_BUF);
        std::vector<std::vector<double>>  sc_vals(N_BUF);

        // Per-thread qualifying keys (unioned serially after parallel reduce)
        std::vector<std::vector<int32_t>> tq(nthreads);

        // Pre-allocate sub-map storage: each thread gets one 512K sub-map.
        // Allocated outside the parallel region to avoid malloc contention.
        const uint32_t SUB_CAP = 1u << 19;  // 512K entries, 6MB (keys+vals)
        std::vector<int32_t> sub_key_storage((size_t)nthreads * SUB_CAP);
        std::vector<double>  sub_val_storage((size_t)nthreads * SUB_CAP);

        // Parallel initialization of sub-map storage (each thread touches its own)
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int32_t* kp = sub_key_storage.data() + (size_t)tid * SUB_CAP;
            double*  vp = sub_val_storage.data() + (size_t)tid * SUB_CAP;
            std::fill(kp, kp + SUB_CAP, QtyMap::SENTINEL);
            std::fill(vp, vp + SUB_CAP, 0.0);
        }

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int nt  = omp_get_num_threads();

            // ── Phase A: scatter raw lineitem rows ──────────────────────────
            // Each thread reads its sequential chunk and appends to P partition
            // buffers. No intermediate aggregation hash map needed.
            int64_t chunk = (nlineitem + nt - 1) / nt;
            int64_t row0  = (int64_t)tid * chunk;
            int64_t row1  = std::min(row0 + chunk, nlineitem);

            for (int64_t i = row0; i < row1; i++) {
                int32_t okey = lok[i];
                int p = (int)(hash32((uint32_t)okey) & P_mask);
                sc_keys[tid * P + p].push_back(okey);
                sc_vals[tid * P + p].push_back(lqt[i]);
            }

            #pragma omp barrier  // Phase A complete

            // ── Phase B: gather + reduce per partition ──────────────────────
            // Each thread handles one or more partitions.
            // Uses pre-allocated sub-map storage (avoids in-region malloc).
            int32_t* kp = sub_key_storage.data() + (size_t)tid * SUB_CAP;
            double*  vp = sub_val_storage.data() + (size_t)tid * SUB_CAP;

            // sub-map already cleared by initialization above.
            QtyMap sub_map;
            sub_map.assign(SUB_CAP, kp, vp);

            int parts_per_thread = P / nt;
            int part_start = tid * parts_per_thread;
            int part_end   = (tid == nt - 1) ? P : part_start + parts_per_thread;

            for (int my_part = part_start; my_part < part_end; my_part++) {
                // Gather from all source threads for this partition
                for (int t = 0; t < nt; t++) {
                    const auto& sk = sc_keys[t * P + my_part];
                    const auto& sv = sc_vals[t * P + my_part];
                    const int32_t n = (int32_t)sk.size();
                    for (int32_t i = 0; i < n; i++)
                        sub_map.add(sk[i], sv[i]);
                }

                // Collect qualifying keys and clear sub-map for next partition
                sub_map.for_each_and_clear([&](int32_t key, double val) {
                    if (val > 300.0) tq[tid].push_back(key);
                });
            }
        } // end parallel

        // Union qualifying keys (serial, ~624 keys)
        {
            GENDB_PHASE("pass1_merge_and_build_qualifying_set");
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
    // PASS 2 MERGE: Single-threaded reduce (tiny: ~624 groups)
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

        const int32_t* ocust_col  = o_custkey.data;
        const int32_t* odate_col  = o_orderdate.data;
        const double*  oprice_col = o_totalprice.data;

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
