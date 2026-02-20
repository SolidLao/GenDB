// Q9: Product Type Profit Measure
// Key optimizations:
//   1. Direct-array for orders (key range 1..60M) — O(1) lookup, no hashing
//   2. Parallel part scan (64 threads, strstr on 112MB)
//   3. Parallel orders fill (unique keys → no atomics needed, just write)
//   4. Parallel partsupp scan with CAS-based composite hash map
//   5. Index-driven parallel lineitem probe (3.2M/60M rows accessed)
//   6. Thread-local aggregation into [25][7] array

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ---- mmap helper ----
static void* mmap_file(const std::string& path, size_t& out_size, bool sequential = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    // Only MAP_POPULATE for sequential scans (demand-fault is cheaper for random access)
    int mmap_flags = MAP_PRIVATE;
    if (sequential) mmap_flags |= MAP_POPULATE;
    void* p = mmap(nullptr, out_size, PROT_READ, mmap_flags, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    if (sequential) posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ---- Index slot layout ----
struct IdxSlot {
    int32_t  key;    // INT32_MIN = empty sentinel
    uint32_t offset;
    uint32_t count;
};

// ---- PartSupp composite hash map ----
// key = (uint64_t)partkey << 20 | suppkey   (always > 0 since partkey >= 1)
// value = double supplycost
// Empty sentinel: 0 (all valid keys >= 2^20 = 1048576)
// ~430K entries -> 1M capacity (43% load) -> 8MB keys + 8MB vals = 16MB, fits in L3
static const uint32_t PS_CAP  = 1u << 20; // 1048576, ~41% load for 430K entries
static const uint32_t PS_MASK = PS_CAP - 1;
static int64_t* ps_keys = nullptr;
static double*  ps_vals = nullptr;

// Sequential (non-atomic) insert — single-threaded use only
static inline void ps_insert_seq(int64_t k, double v) {
    uint32_t h = (uint32_t)((uint64_t)k * 0x9E3779B97F4A7C15ULL >> 44) & PS_MASK;
    for (uint32_t i = 0; i < PS_CAP; i++) {
        uint32_t s = (h + i) & PS_MASK;
        if (ps_keys[s] == 0LL) { ps_keys[s] = k; ps_vals[s] = v; return; }
        if (ps_keys[s] == k)   return; // duplicate (safety)
    }
}

static inline double ps_lookup(int64_t k) {
    uint32_t h = (uint32_t)((uint64_t)k * 0x9E3779B97F4A7C15ULL >> 44) & PS_MASK;
    for (uint32_t i = 0; i < PS_CAP; i++) {
        uint32_t s = (h + i) & PS_MASK;
        int64_t cur = ps_keys[s];
        if (cur == 0LL) return -1.0;
        if (cur == k)   return ps_vals[s];
    }
    return -1.0;
}

// ---- Orders direct-access array ----
// TPC-H SF10: o_orderkey in [1, 60000000]. Direct array = 60M × 1B = 60MB.
// ord_year[orderkey] = year_encoded (1=1992 .. 7=1998), or 0 if absent.
// Fits 4x better in L3 cache (44MB) vs prior 240MB int32_t array.
static const int ORD_MAX = 60000001;
static uint8_t* ord_year = nullptr;

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string d = gendb_dir + "/";

    // Allocate hash tables via mmap(ANONYMOUS) for huge-page eligibility
    ps_keys = (int64_t*)mmap(nullptr, (size_t)PS_CAP * sizeof(int64_t),
                              PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ps_vals = (double*) mmap(nullptr, (size_t)PS_CAP * sizeof(double),
                              PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ord_year = (uint8_t*)mmap(nullptr, (size_t)ORD_MAX * sizeof(uint8_t),
                               PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    madvise(ps_keys,  (size_t)PS_CAP  * sizeof(int64_t), MADV_HUGEPAGE);
    madvise(ps_vals,  (size_t)PS_CAP  * sizeof(double),  MADV_HUGEPAGE);
    madvise(ord_year, (size_t)ORD_MAX * sizeof(uint8_t),  MADV_HUGEPAGE);
    // mmap(ANONYMOUS) is zero-initialized by the kernel — 0 serves as "missing" sentinel

    // Green parts bitset: covers partkeys 1..2000000
    static const int PART_N   = 2000000;
    static const int BS_WORDS = (PART_N + 64) / 64 + 1;
    uint64_t* green_bs = (uint64_t*)mmap(nullptr, (size_t)BS_WORDS * 8,
                                          PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    // Nation name lookup: nationkey -> name string
    std::string nation_names[26];

    // Supplier lookup: suppkey -> nationkey
    int32_t sup_nat[100002] = {};

    // ============================================================
    // Phase 1: Dimension tables — nation + supplier (trivial, sequential)
    // ============================================================
    {
        GENDB_PHASE("dim_filter");

        // Load nation name dictionary
        std::vector<std::string> name_dict(26);
        {
            std::ifstream df(d + "nation/n_name_dict.txt");
            std::string line;
            while (std::getline(df, line)) {
                size_t eq = line.find('=');
                if (eq == std::string::npos) continue;
                int code = std::stoi(line.substr(0, eq));
                if (code >= 0 && code < 26)
                    name_dict[code] = line.substr(eq + 1);
            }
        }

        size_t sz;
        const int32_t* nkeys = (const int32_t*)mmap_file(d + "nation/n_nationkey.bin", sz);
        const int16_t* nname = (const int16_t*)mmap_file(d + "nation/n_name.bin", sz);
        for (int i = 0; i < 25; i++) {
            int k = nkeys[i];
            if (k >= 0 && k < 26)
                nation_names[k] = name_dict[(int)(uint16_t)nname[i]];
        }

        // Supplier
        size_t s_sz;
        const int32_t* sk_col = (const int32_t*)mmap_file(d + "supplier/s_suppkey.bin", s_sz);
        const int32_t* sn_col = (const int32_t*)mmap_file(d + "supplier/s_nationkey.bin", sz);
        int n_sup = (int)(s_sz / sizeof(int32_t));
        for (int i = 0; i < n_sup; i++) {
            int sk = sk_col[i];
            if (sk > 0 && sk <= 100001)
                sup_nat[sk] = sn_col[i];
        }
    }

    // ============================================================
    // Phase 2: Parallel part scan — build green bitset + key list
    // ============================================================
    std::vector<int32_t> green_keys;
    green_keys.reserve(150000);

    {
        GENDB_PHASE("part_scan");

        // Use sequential=false to avoid blocking MAP_POPULATE on 112MB p_name.bin.
        // Data is in OS page cache (iter3+); demand faults resolve as minor faults in parallel.
        size_t pkey_sz, pnam_sz;
        const int32_t* pkey = (const int32_t*)mmap_file(d + "part/p_partkey.bin", pkey_sz, false);
        const char*    pnam = (const char*)   mmap_file(d + "part/p_name.bin",     pnam_sz, false);
        // Async readahead for background I/O on cold runs; no-op if already cached
        madvise((void*)pkey, pkey_sz, MADV_SEQUENTIAL);
        madvise((void*)pnam, pnam_sz, MADV_SEQUENTIAL);

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> thr_keys(nthreads);
        for (auto& v : thr_keys) v.reserve(4000);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            #pragma omp for schedule(static) nowait
            for (int i = 0; i < PART_N; i++) {
                if (strstr(pnam + (size_t)i * 56, "green") != nullptr)
                    thr_keys[tid].push_back(pkey[i]);
            }
        }

        // Merge thread-local keys
        for (auto& v : thr_keys)
            green_keys.insert(green_keys.end(), v.begin(), v.end());

        // Fill bitset non-atomically (single-threaded; no races since done after parallel phase)
        for (int32_t pk : green_keys) {
            if (pk > 0 && pk <= PART_N) {
                green_bs[(uint32_t)pk >> 6] |= 1ULL << ((uint32_t)pk & 63u);
            }
        }
    }

    int n_green = (int)green_keys.size();

    // ============================================================
    // Phase 3: Build data structures — partsupp (parallel) + orders (parallel)
    // ============================================================
    {
        GENDB_PHASE("build_joins");

        // ---- PartSupp: parallel scan → thread-local collect → sequential insert (no CAS) ----
        {
            size_t sz;
            const int32_t* ps_pk = (const int32_t*)mmap_file(d + "partsupp/ps_partkey.bin", sz);
            const int32_t* ps_sk = (const int32_t*)mmap_file(d + "partsupp/ps_suppkey.bin", sz);
            const double*  ps_sc = (const double*) mmap_file(d + "partsupp/ps_supplycost.bin", sz);
            int n_ps = (int)(sz / sizeof(double));

            int nthr = omp_get_max_threads();
            // Thread-local vectors of (composite_key, supplycost) pairs
            std::vector<std::vector<std::pair<int64_t,double>>> thr_ps(nthr);
            for (auto& v : thr_ps) v.reserve(8192);

            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                auto& local = thr_ps[tid];
                #pragma omp for schedule(static) nowait
                for (int i = 0; i < n_ps; i++) {
                    int32_t pk = ps_pk[i];
                    if (pk > 0 && (uint32_t)pk <= (uint32_t)PART_N &&
                        ((green_bs[(uint32_t)pk >> 6] >> ((uint32_t)pk & 63u)) & 1u)) {
                        int64_t k = ((int64_t)(uint32_t)pk << 20) | (uint32_t)ps_sk[i];
                        local.push_back({k, ps_sc[i]});
                    }
                }
            }

            // Sequential merge into global hash map — zero atomic overhead
            for (auto& vec : thr_ps)
                for (auto& [k, v] : vec)
                    ps_insert_seq(k, v);
        }

        // ---- Orders: parallel fill — store pre-computed year index (uint8_t, 60MB) ----
        // Encoding: 0 = missing, 1 = 1992, ..., 7 = 1998.
        // No race condition: each orderkey has exactly one orderdate row.
        {
            size_t o_sz, od_sz;
            const int32_t* okey = (const int32_t*)mmap_file(d + "orders/o_orderkey.bin", o_sz);
            const int32_t* odat = (const int32_t*)mmap_file(d + "orders/o_orderdate.bin", od_sz);
            int n_ord = (int)(o_sz / sizeof(int32_t));

            #pragma omp parallel for schedule(static)
            for (int i = 0; i < n_ord; i++) {
                int32_t ok = okey[i];
                if (ok > 0 && ok < ORD_MAX) {
                    int yr = gendb::extract_year(odat[i]);
                    int enc = yr - 1991; // 1992→1, ..., 1998→7
                    if ((unsigned)(enc - 1) <= 6u)
                        ord_year[ok] = (uint8_t)enc;
                }
            }
        }
    }

    // ============================================================
    // Phase 4: mmap lineitem columns (random access — no sequential hint)
    // ============================================================
    size_t li_sz;
    const int32_t* l_sk = (const int32_t*)mmap_file(d + "lineitem/l_suppkey.bin",      li_sz, false);
    const int32_t* l_ok = (const int32_t*)mmap_file(d + "lineitem/l_orderkey.bin",     li_sz, false);
    const double*  l_qt = (const double*) mmap_file(d + "lineitem/l_quantity.bin",      li_sz, false);
    const double*  l_ep = (const double*) mmap_file(d + "lineitem/l_extendedprice.bin", li_sz, false);
    const double*  l_dc = (const double*) mmap_file(d + "lineitem/l_discount.bin",      li_sz, false);

    // Load lineitem_partkey_hash index
    size_t idx_sz;
    const uint8_t* idx_raw = (const uint8_t*)mmap_file(
        d + "indexes/lineitem_partkey_hash.bin", idx_sz, false);

    // Header: [0]=num_unique [4]=ht_capacity [8]=num_rows
    const uint32_t idx_ht_cap  = *(const uint32_t*)(idx_raw + 4);
    const uint32_t idx_ht_mask = idx_ht_cap - 1;
    const IdxSlot* idx_ht   = (const IdxSlot*)(idx_raw + 12);
    const uint32_t* idx_pos = (const uint32_t*)(
        idx_raw + 12 + (size_t)idx_ht_cap * sizeof(IdxSlot));

    // ============================================================
    // Phase 5: Parallel lineitem probe via partkey index
    // ============================================================
    double global_agg[25][7] = {};
    bool   global_has[25][7] = {};

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            double local_agg[25][7] = {};
            bool   local_has[25][7] = {};

            #pragma omp for schedule(dynamic, 32)
            for (int gi = 0; gi < n_green; gi++) {
                int32_t partkey = green_keys[gi];

                // Probe index — Fibonacci hash, shift=42 for 2^22 capacity
                uint32_t h = (uint32_t)((uint64_t)(uint32_t)partkey *
                              0x9E3779B97F4A7C15ULL >> 42) & idx_ht_mask;

                uint32_t row_offset = UINT32_MAX, row_count = 0;
                for (uint32_t probe = 0; probe <= idx_ht_mask; probe++) {
                    uint32_t slot = (h + probe) & idx_ht_mask;
                    int32_t  sk   = idx_ht[slot].key;
                    if (sk == INT32_MIN) break;   // empty — key not in index
                    if (sk == partkey) {
                        row_offset = idx_ht[slot].offset;
                        row_count  = idx_ht[slot].count;
                        break;
                    }
                }
                if (row_offset == UINT32_MAX) continue;

                // Precompute partkey-side composite key base
                int64_t ps_base = (int64_t)(uint32_t)partkey << 20;

                for (uint32_t r = 0; r < row_count; r++) {
                    uint32_t row = idx_pos[row_offset + r];

                    // Software prefetch: idx_pos is sequential so +16 entry is in L3;
                    // issue prefetch for the lineitem columns at that future row.
                    if (r + 24 < row_count) {
                        uint32_t pr = idx_pos[row_offset + r + 24];
                        __builtin_prefetch(l_ep + pr, 0, 0);
                        __builtin_prefetch(l_dc + pr, 0, 0);
                        __builtin_prefetch(l_qt + pr, 0, 0);
                        __builtin_prefetch(l_sk + pr, 0, 0);
                        __builtin_prefetch(l_ok + pr, 0, 0);
                    }

                    int32_t suppkey  = l_sk[row];
                    int32_t orderkey = l_ok[row];
                    double  qty      = l_qt[row];
                    double  ep       = l_ep[row];
                    double  disc     = l_dc[row];

                    // PartSupp lookup
                    double sc = ps_lookup(ps_base | (uint32_t)suppkey);
                    if (sc < 0.0) continue;

                    // Nation from supplier
                    int32_t natkey = sup_nat[suppkey];
                    if ((unsigned)natkey >= 25u) continue;

                    // Year lookup — O(1) byte access into 60MB array (4x smaller than int32_t)
                    uint8_t yr_enc = ord_year[orderkey];
                    if (yr_enc == 0) continue; // missing/out-of-range order
                    int yi = (int)yr_enc - 1;  // 0-based: 1992→0, ..., 1998→6
                    if ((unsigned)yi > 6u) continue;

                    double amount = ep * (1.0 - disc) - sc * qty;
                    local_agg[natkey][yi] += amount;
                    local_has[natkey][yi]  = true;
                }
            }

            // Merge thread-local into global
            #pragma omp critical
            {
                for (int n = 0; n < 25; n++)
                    for (int y = 0; y < 7; y++) {
                        global_agg[n][y] += local_agg[n][y];
                        global_has[n][y] |= local_has[n][y];
                    }
            }
        }
    }

    // ============================================================
    // Phase 6: Sort 175 results and write CSV
    // ============================================================
    {
        GENDB_PHASE("output");

        struct Row { const char* nation; int year; double profit; };
        std::vector<Row> rows;
        rows.reserve(175);

        for (int n = 0; n < 25; n++) {
            if (nation_names[n].empty()) continue;
            for (int y = 0; y < 7; y++) {
                if (global_has[n][y])
                    rows.push_back({nation_names[n].c_str(), 1992 + y, global_agg[n][y]});
            }
        }

        // ORDER BY nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            int cmp = strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (auto& r : rows)
            fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.profit);
        fclose(f);
    }

    // ---- Cleanup ----
    munmap(ps_keys,  (size_t)PS_CAP  * sizeof(int64_t));
    munmap(ps_vals,  (size_t)PS_CAP  * sizeof(double));
    munmap(ord_year, (size_t)ORD_MAX * sizeof(uint8_t));
    munmap(green_bs, (size_t)BS_WORDS * 8);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
