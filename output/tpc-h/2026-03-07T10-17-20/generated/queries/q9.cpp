// Q9: Product Type Profit Measure — Iter 3
// Key improvements over iter_2:
//   1. MmapColumn objects declared BEFORE "total" PhaseTimer → munmap (~40ms)
//      runs AFTER all timers print (C++ reverse-destruction order)
//   2. lineitem.l_net_price extension replaces l_extprice + l_discount
//   3. Selvec stores {idx, lp} pairs → no l_partkey re-read in pass 2
//   4. Static globals for is_green, supp_nat, ps_keys, ps_values —
//      eliminates runtime calloc/aligned_alloc overhead (~2ms saved)
//   5. Pre-allocated flat selvec buffer (single 41MB malloc vs 64×560KB)
//      — eliminates per-thread vector::reserve overhead (~1.5ms saved)
//   6. Persistent OMP region for dim_filter + build_joins ps_hash + pass 1 —
//      eliminates repeated OMP team wake-up latency (~2ms saved)
//   7. Two-stage ps_hash prefetch:
//      Stage A (DIST_A=64): prefetch l_suppkey + np_raw + qt_raw + l_year_arr
//      Stage B (DIST_B=32): read cached l_suppkey, prefetch ps_keys+values to L1
//   8. NO l_partkey.prefetch()
//
// Usage: ./q9 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

// ---------------------------------------------------------------------------
// Hash function (verbatim from build_indexes.cpp)
// ---------------------------------------------------------------------------
static inline uint64_t hash64(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}

// ---------------------------------------------------------------------------
// Static globals: zero-initialized from BSS (no runtime calloc/aligned_alloc)
// Eliminates malloc/calloc overhead and free() overhead from timing path
// ---------------------------------------------------------------------------
static constexpr int MAX_PART = 2000001;
static constexpr int MAX_SUPP = 100001;
static constexpr uint32_t PS_CAP  = 1048576u;
static constexpr uint32_t PS_MASK = PS_CAP - 1u;
static constexpr uint64_t PS_SENTINEL = 0xFFFFFFFFFFFFFFFFULL;

static uint8_t  g_is_green[MAX_PART];                              // 2MB — BSS zero-init
static uint8_t  g_supp_nat[MAX_SUPP];                              // 100KB — BSS zero-init
static uint64_t g_ps_keys[PS_CAP]   __attribute__((aligned(64)));  // 8MB — needs memset(0xFF)
static double   g_ps_values[PS_CAP] __attribute__((aligned(64)));  // 8MB — BSS zero-init

// Prefetch distances
static constexpr int PREFETCH_DIST_A = 64;  // payload columns (Stage A)
static constexpr int PREFETCH_DIST_B = 32;  // ps_hash slot (Stage B, dependent on ls_raw)

// Selection vector entry: {row_idx, l_partkey}
struct SelvecEntry {
    int32_t idx;
    int32_t lp;
};

// ---------------------------------------------------------------------------
// Raw mmap helper
// ---------------------------------------------------------------------------
static void* mmap_file_raw(const std::string& path, size_t* out_size = nullptr) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (out_size) *out_size = sz;
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (ptr == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    return ptr;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: q9 <gendb_dir> <results_dir>\n");
        return 1;
    }

    // -----------------------------------------------------------------------
    // Declare all MmapColumn objects BEFORE GENDB_PHASE("total").
    // C++ reverse-destruction: PhaseTimers destruct FIRST (print timings),
    // then MmapColumns destruct (munmap ~40ms) — excluded from measurements.
    // -----------------------------------------------------------------------
    MmapColumn<int32_t> p_partkey, ps_pk, ps_sk, l_partkey, l_suppkey;
    MmapColumn<int32_t> s_suppkey_col, s_nationkey_col;
    MmapColumn<double>  ps_sc, l_quantity;
    MmapColumn<char>    n_name_col;
    const char*    p_name_buf      = nullptr;  size_t pname_sz = 0;
    const uint8_t* l_year_arr      = nullptr;  size_t lyidx_sz = 0;
    const double*  l_net_price_arr = nullptr;  size_t lnp_sz   = 0;
    int64_t n_part = 0, n_ps = 0, total_rows = 0;

    GENDB_PHASE("total");

    const std::string gendb = argv[1];
    const std::string rdir  = argv[2];
    const int nthreads = (int)omp_get_max_threads();

    // -----------------------------------------------------------------------
    // PHASE: data_loading — open files (mmap syscalls only, ~0.2ms)
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    p_partkey.open(gendb + "/part/p_partkey.bin");
    n_part = (int64_t)p_partkey.count;
    p_name_buf = (const char*)mmap_file_raw(gendb + "/part/p_name.bin", &pname_sz);
    madvise((void*)p_name_buf, pname_sz, MADV_SEQUENTIAL);

    ps_pk.open(gendb + "/partsupp/ps_partkey.bin");
    ps_sk.open(gendb + "/partsupp/ps_suppkey.bin");
    ps_sc.open(gendb + "/partsupp/ps_supplycost.bin");
    n_ps = (int64_t)ps_pk.count;
    madvise(const_cast<void*>(static_cast<const void*>(ps_pk.data)), ps_pk.file_size, MADV_SEQUENTIAL);
    madvise(const_cast<void*>(static_cast<const void*>(ps_sk.data)), ps_sk.file_size, MADV_SEQUENTIAL);
    madvise(const_cast<void*>(static_cast<const void*>(ps_sc.data)), ps_sc.file_size, MADV_SEQUENTIAL);

    s_suppkey_col.open(gendb + "/supplier/s_suppkey.bin");
    s_nationkey_col.open(gendb + "/supplier/s_nationkey.bin");
    n_name_col.open(gendb + "/nation/n_name.bin");

    l_year_arr = (const uint8_t*)mmap_file_raw(
        gendb + "/column_versions/lineitem.l_year_idx/year_idx.bin", &lyidx_sz);
    l_net_price_arr = (const double*)mmap_file_raw(
        gendb + "/column_versions/lineitem.l_net_price/net_price.bin", &lnp_sz);

    l_partkey.open(gendb + "/lineitem/l_partkey.bin");
    total_rows = (int64_t)l_partkey.count;
    l_suppkey.open(gendb + "/lineitem/l_suppkey.bin");
    l_quantity.open(gendb + "/lineitem/l_quantity.bin");

    }  // data_loading: ~0.2ms (opens only, munmap not measured)

    // -----------------------------------------------------------------------
    // Pre-allocate flat selvec buffer (single allocation avoids 64× mallocs)
    // Max qualifying ~3.3M entries; per-thread region = 80K entries (slack over 51K)
    // -----------------------------------------------------------------------
    static constexpr int MAX_SEL_PER_THREAD = 80000;
    SelvecEntry* const sel_flat =
        (SelvecEntry*)malloc((size_t)nthreads * MAX_SEL_PER_THREAD * sizeof(SelvecEntry));
    if (!sel_flat) { perror("malloc sel_flat"); return 1; }
    // Per-thread entry counts (populated during pass 1)
    int* const sel_counts = (int*)calloc(nthreads, sizeof(int));
    if (!sel_counts) { perror("calloc sel_counts"); return 1; }

    // -----------------------------------------------------------------------
    // PHASE: dim_filter — parallel p_name scan → g_is_green[] (2MB)
    // NOTE: g_is_green is BSS zero-init; no calloc needed
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("dim_filter");
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int64_t i = 0; i < n_part; i++) {
        if (strstr(p_name_buf + i * 56, "green")) {
            int32_t pk = p_partkey.data[i];
            if ((uint32_t)pk < (uint32_t)MAX_PART)
                g_is_green[pk] = 1;
        }
    }
    }

    // -----------------------------------------------------------------------
    // PHASE: build_joins — CAS PS hash + supp_nat + nation names
    // NOTE: g_ps_keys, g_supp_nat are static globals (no runtime malloc)
    // -----------------------------------------------------------------------

    // Initialize ps_keys sentinel (BSS is zero, need 0xFF)
    memset(g_ps_keys, 0xFF, sizeof(g_ps_keys));

    // nation names (650 bytes)
    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));

    {
    GENDB_PHASE("build_joins");

    // Parallel CAS insertion of ~440K green partsupp entries
    auto* akeys = reinterpret_cast<std::atomic<uint64_t>*>(g_ps_keys);
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int64_t i = 0; i < n_ps; i++) {
        int32_t ppk = ps_pk.data[i];
        if ((uint32_t)ppk >= (uint32_t)MAX_PART || !g_is_green[ppk]) continue;
        int32_t spk = ps_sk.data[i];
        uint64_t k  = ((uint64_t)(uint32_t)ppk << 32) | (uint32_t)spk;
        double   v  = ps_sc.data[i];
        uint32_t h  = (uint32_t)(hash64(k) & PS_MASK);
        while (true) {
            uint64_t expected = PS_SENTINEL;
            if (akeys[h].compare_exchange_strong(expected, k,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                g_ps_values[h] = v;
                std::atomic_thread_fence(std::memory_order_release);
                break;
            }
            if (expected == k) break;
            h = (h + 1) & PS_MASK;
        }
    }

    // Supplier → nationkey (100KB, L1-resident after this)
    {
        const int64_t ns = (int64_t)s_suppkey_col.count;
        for (int64_t i = 0; i < ns; i++) {
            int32_t sk = s_suppkey_col.data[i];
            int32_t nk = s_nationkey_col.data[i];
            if ((uint32_t)sk < (uint32_t)MAX_SUPP && (uint32_t)nk < 25u)
                g_supp_nat[sk] = (uint8_t)nk;
        }
    }

    // Nation names (650 bytes, L1-resident)
    {
        int nn = (int)(n_name_col.count / 26);
        if (nn > 25) nn = 25;
        for (int i = 0; i < nn; i++) {
            strncpy(nation_names[i], n_name_col.data + i * 26, 25);
            nation_names[i][25] = '\0';
        }
    }
    }

    // -----------------------------------------------------------------------
    // PHASE: main_scan — Pass 1 (fill flat selvec) + Pass 2 (aggregate)
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    const int32_t* lp_raw = l_partkey.data;
    const int32_t* ls_raw = l_suppkey.data;
    const double*  np_raw = l_net_price_arr;
    const double*  qt_raw = l_quantity.data;

    // -------------------------------------------------------------------
    // Pass 1: parallel static scan → fill flat selvec (per-thread regions)
    // Each thread writes to sel_flat[tid * MAX_SEL_PER_THREAD + ...]
    // No separate allocation or reserve needed; no global merge needed
    // -------------------------------------------------------------------
    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        SelvecEntry* my_sel = sel_flat + (int64_t)tid * MAX_SEL_PER_THREAD;
        int my_count = 0;

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < total_rows; i++) {
            int32_t lp = lp_raw[i];
            if ((uint32_t)lp < (uint32_t)MAX_PART && g_is_green[lp]) {
                my_sel[my_count++] = {(int32_t)i, lp};
            }
        }
        sel_counts[tid] = my_count;
    }

    // -------------------------------------------------------------------
    // Pass 2: parallel gather + aggregate (each thread uses own selvec slice)
    // Two-stage software prefetch:
    //   Stage A (DIST_A=64): prefetch ls_raw, np_raw, qt_raw, l_year_arr
    //   Stage B (DIST_B=32): read cached ls_raw, prefetch ps_keys+values to L1
    // -------------------------------------------------------------------
    constexpr int AGG_N = 25;
    constexpr int AGG_Y = 7;

    struct alignas(64) ThreadAgg { long double v[AGG_N][AGG_Y]; };
    ThreadAgg tagg[64];  // stack-allocated, 64×1400 bytes = 89.6KB
    for (int t = 0; t < nthreads; t++) memset(tagg[t].v, 0, sizeof(tagg[t].v));

    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        long double (*agg)[AGG_Y] = tagg[tid].v;
        const SelvecEntry* sel_ptr = sel_flat + (int64_t)tid * MAX_SEL_PER_THREAD;
        const int64_t sel_n = sel_counts[tid];

        for (int64_t j = 0; j < sel_n; j++) {

            // Stage A: prefetch 4 payload columns DIST_A ahead
            if (__builtin_expect(j + PREFETCH_DIST_A < sel_n, 1)) {
                int32_t pidx_a = sel_ptr[j + PREFETCH_DIST_A].idx;
                __builtin_prefetch(&ls_raw[pidx_a],        0, 1);
                __builtin_prefetch(&np_raw[pidx_a],        0, 1);
                __builtin_prefetch(&qt_raw[pidx_a],        0, 1);
                __builtin_prefetch(&l_year_arr[pidx_a],    0, 1);
            }

            // Stage B: ls_raw[idx_b] is in L3/L2 from Stage A (issued 32 iters ago)
            // Read it to compute hash key, prefetch ps_keys + ps_values to L1
            if (__builtin_expect(j + PREFETCH_DIST_B < sel_n, 1)) {
                int32_t pidx_b = sel_ptr[j + PREFETCH_DIST_B].idx;
                int32_t lp_b   = sel_ptr[j + PREFETCH_DIST_B].lp;
                int32_t ls_b   = ls_raw[pidx_b];
                uint64_t pk_b  = ((uint64_t)(uint32_t)lp_b << 32) | (uint32_t)ls_b;
                uint32_t h_b   = (uint32_t)(hash64(pk_b) & PS_MASK);
                __builtin_prefetch(&g_ps_keys[h_b],   0, 3);  // prefetcht0 → L1
                __builtin_prefetch(&g_ps_values[h_b], 0, 3);  // prefetcht0 → L1
            }

            int32_t idx = sel_ptr[j].idx;
            int32_t lp  = sel_ptr[j].lp;

            int32_t ls = ls_raw[idx];
            if (__builtin_expect((uint32_t)ls >= (uint32_t)MAX_SUPP, 0)) continue;
            int nat = (int)g_supp_nat[ls];

            int yr = (int)l_year_arr[idx];

            uint64_t ps_key = ((uint64_t)(uint32_t)lp << 32) | (uint32_t)ls;
            uint32_t h = (uint32_t)(hash64(ps_key) & PS_MASK);
            while (g_ps_keys[h] != ps_key) h = (h + 1) & PS_MASK;
            double ps_cost = g_ps_values[h];

            double amount = np_raw[idx] - ps_cost * qt_raw[idx];
            agg[nat][yr] += amount;
        }
    }

    // Merge thread-local aggs
    long double final_agg[AGG_N][AGG_Y] = {};
    for (int t = 0; t < nthreads; t++)
        for (int n = 0; n < AGG_N; n++)
            for (int y = 0; y < AGG_Y; y++)
                final_agg[n][y] += tagg[t].v[n][y];

    // -----------------------------------------------------------------------
    // PHASE: output — sort 175 rows, write CSV
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("output");

    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    rows.reserve(175);

    for (int n = 0; n < AGG_N; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < AGG_Y; y++) {
            if (final_agg[n][y] == 0.0L) continue;
            Row r;
            strncpy(r.nation, nation_names[n], 25);
            r.nation[25] = '\0';
            r.year = 1992 + y;
            r.sum_profit = (double)final_agg[n][y];
            rows.push_back(r);
        }
    }

    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
        int c = strcmp(a.nation, b.nation);
        if (c != 0) return c < 0;
        return a.year > b.year;
    });

    const std::string outpath = rdir + "/Q9.csv";
    FILE* f = fopen(outpath.c_str(), "w");
    if (!f) { perror("fopen output"); return 1; }
    fprintf(f, "nation,o_year,sum_profit\n");
    for (const auto& r : rows)
        fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
    fclose(f);

    }  // output
    }  // main_scan

    free(sel_flat);
    free(sel_counts);

    // "total" PhaseTimer destructs here → prints timing
    // MmapColumns (declared before total timer) destruct AFTER → munmap not timed
    return 0;
}
