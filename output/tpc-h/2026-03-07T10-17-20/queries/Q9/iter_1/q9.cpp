// Q9: Product Type Profit Measure — Iter 1
// Key improvements over iter_0:
//   1. Parallel p_name scan (all threads) to build is_green[] bitset
//   2. Parallel partsupp scan → compact 16MB runtime hash (vs 201MB pre-built)
//   3. Storage extension: uint8_t year_idx array (direct lookup, vs 268MB o_orderkey_hash)
//   4. Morsel-driven parallel lineitem scan with thread-local agg[25][7]
//
// Usage: ./q9 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <atomic>
#include <thread>
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
// Compact PS hash: capacity=1048576 (2^20), sentinel=0xFFFFFFFFFFFFFFFF
// key = (uint64(ps_partkey) << 32) | uint32(ps_suppkey)
// value = double ps_supplycost
// Total: 16MB — fits entirely in L3 cache
// ---------------------------------------------------------------------------
static constexpr uint32_t PS_CAP      = 1048576u;
static constexpr uint32_t PS_MASK     = PS_CAP - 1u;
static constexpr uint64_t PS_SENTINEL = 0xFFFFFFFFFFFFFFFFULL;

struct CompactPsHash {
    uint64_t* keys;
    double*   values;
    void*     raw_mem;

    CompactPsHash() : keys(nullptr), values(nullptr), raw_mem(nullptr) {}
    ~CompactPsHash() { if (raw_mem) free(raw_mem); }

    void init() {
        size_t total = (size_t)PS_CAP * (sizeof(uint64_t) + sizeof(double));
        raw_mem = aligned_alloc(4096, total);
        if (!raw_mem) { perror("aligned_alloc compact_ps_hash"); exit(1); }
        keys   = (uint64_t*)raw_mem;
        values = (double*)((uint8_t*)raw_mem + (size_t)PS_CAP * sizeof(uint64_t));
        memset(keys, 0xFF, (size_t)PS_CAP * sizeof(uint64_t));
    }

    void insert(uint64_t k, double v) {
        uint32_t h = (uint32_t)(hash64(k) & PS_MASK);
        while (keys[h] != PS_SENTINEL) {
            h = (h + 1) & PS_MASK;
        }
        keys[h]   = k;
        values[h] = v;
    }

    inline double probe(uint64_t k) const {
        uint32_t h = (uint32_t)(hash64(k) & PS_MASK);
        while (keys[h] != k) {
            h = (h + 1) & PS_MASK;
        }
        return values[h];
    }

    // No copy
    CompactPsHash(const CompactPsHash&) = delete;
    CompactPsHash& operator=(const CompactPsHash&) = delete;
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
    GENDB_PHASE("total");

    const std::string gendb = argv[1];
    const std::string rdir  = argv[2];

    // Determine thread count
    const int nthreads = (int)omp_get_max_threads();

    // -----------------------------------------------------------------------
    // PHASE: data_loading — mmap all columns
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    // --- Part: p_partkey + p_name (112MB sequential scan) -----------------
    MmapColumn<int32_t> p_partkey(gendb + "/part/p_partkey.bin");
    size_t pname_sz = 0;
    const char* p_name_buf = (const char*)mmap_file_raw(gendb + "/part/p_name.bin", &pname_sz);
    const int64_t n_part = (int64_t)p_partkey.count; // 2,000,000
    madvise((void*)p_name_buf, pname_sz, MADV_SEQUENTIAL);

    // --- Partsupp raw columns (128MB sequential scan) ----------------------
    MmapColumn<int32_t> ps_partkey_col(gendb + "/partsupp/ps_partkey.bin");
    MmapColumn<int32_t> ps_suppkey_col(gendb + "/partsupp/ps_suppkey.bin");
    MmapColumn<double>  ps_supplycost_col(gendb + "/partsupp/ps_supplycost.bin");
    const int64_t n_ps = (int64_t)ps_partkey_col.count; // 8,000,000

    // --- Supplier: s_suppkey + s_nationkey (tiny) --------------------------
    MmapColumn<int32_t> s_suppkey_col (gendb + "/supplier/s_suppkey.bin");
    MmapColumn<int32_t> s_nationkey_col(gendb + "/supplier/s_nationkey.bin");

    // --- Nation: n_name (650 bytes) ----------------------------------------
    MmapColumn<char> n_name_col(gendb + "/nation/n_name.bin");

    // --- Storage extension: year_idx (direct orderkey → year lookup) -------
    size_t yidx_sz = 0;
    void* yidx_ptr = mmap_file_raw(
        gendb + "/column_versions/orders.o_orderkey.year_idx/year_idx.bin", &yidx_sz);
    // Format: int32_t max_okey | uint8_t[max_okey+1]
    const int32_t max_okey        = *(const int32_t*)yidx_ptr;
    const uint8_t* okey_year_arr  = (const uint8_t*)((const char*)yidx_ptr + 4);
    // Random access (per qualifying lineitem row)
    madvise(yidx_ptr, yidx_sz, MADV_RANDOM);

    // --- Lineitem columns (prefetch into page cache) -----------------------
    MmapColumn<int32_t> l_partkey  (gendb + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey  (gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<int32_t> l_orderkey (gendb + "/lineitem/l_orderkey.bin");
    MmapColumn<double>  l_extprice (gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>  l_discount (gendb + "/lineitem/l_discount.bin");
    MmapColumn<double>  l_quantity (gendb + "/lineitem/l_quantity.bin");
    mmap_prefetch_all(l_partkey, l_suppkey, l_orderkey, l_extprice, l_discount, l_quantity);

    const int64_t total_rows = (int64_t)l_partkey.count; // 59,986,052

    // -----------------------------------------------------------------------
    // PHASE: dim_filter — parallel p_name scan → is_green[] bitset
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("dim_filter");

    static constexpr int MAX_PART = 2000001;
    uint8_t* is_green = (uint8_t*)calloc(MAX_PART, 1);
    if (!is_green) { perror("calloc is_green"); return 1; }

    // Parallel across all 64 threads; each thread handles a stride of rows.
    // Concurrent writes of value=1 to is_green[pk] are safe (idempotent uint8_t writes).
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int64_t i = 0; i < n_part; i++) {
        if (strstr(p_name_buf + i * 56, "green")) {
            int32_t pk = p_partkey.data[i];
            if (pk >= 0 && pk < MAX_PART) {
                is_green[pk] = 1;
            }
        }
    }

    // -----------------------------------------------------------------------
    // PHASE: build_joins — parallel partsupp scan → compact ps hash
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("build_joins");

    // Two-phase approach: collect green entries in parallel, then insert single-threaded.
    // This avoids CAS races on the hash table and ensures correct value writes.
    struct PsEntry { uint64_t key; double cost; };
    std::vector<std::vector<PsEntry>> thread_ps(nthreads);

    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        auto& local = thread_ps[tid];
        local.reserve(10000); // ~7K expected per thread

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < n_ps; i++) {
            int32_t ppk = ps_partkey_col.data[i];
            if ((uint32_t)ppk >= (uint32_t)MAX_PART || !is_green[ppk]) continue;
            int32_t spk = ps_suppkey_col.data[i];
            uint64_t k  = ((uint64_t)(uint32_t)ppk << 32) | (uint32_t)spk;
            double   c  = ps_supplycost_col.data[i];
            local.push_back({k, c});
        }
    }

    // Single-threaded insertion into compact hash (~440K entries ≈ 1-2ms)
    CompactPsHash ps_hash;
    ps_hash.init();
    for (int t = 0; t < nthreads; t++) {
        for (const auto& e : thread_ps[t]) {
            ps_hash.insert(e.key, e.cost);
        }
    }

    // --- Supplier: build direct array suppkey → nationkey (L1-resident) ---
    static constexpr int MAX_SUPP = 100001;
    uint8_t* supp_nat = (uint8_t*)calloc(MAX_SUPP, 1);
    if (!supp_nat) { perror("calloc supp_nat"); return 1; }
    {
        const int64_t ns = (int64_t)s_suppkey_col.count;
        for (int64_t i = 0; i < ns; i++) {
            int32_t sk = s_suppkey_col.data[i];
            int32_t nk = s_nationkey_col.data[i];
            if (sk >= 0 && sk < MAX_SUPP && nk >= 0 && nk < 25) {
                supp_nat[sk] = (uint8_t)nk;
            }
        }
    }

    // --- Nation: load n_name into char[25][26] (650 bytes, L1-resident) ---
    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));
    // n_name.bin is 25 × 26-byte fixed-width records, ordered by n_nationkey [0..24]
    {
        int n_nations = (int)(n_name_col.count / 26);
        if (n_nations > 25) n_nations = 25;
        for (int i = 0; i < n_nations; i++) {
            const char* src = n_name_col.data + i * 26;
            strncpy(nation_names[i], src, 25);
            nation_names[i][25] = '\0';
        }
    }

    // -----------------------------------------------------------------------
    // PHASE: main_scan — parallel morsel-driven lineitem scan
    // Key: only l_partkey accessed for 94.5% of rows (rejected by is_green).
    // For qualifying ~5.5% rows, all other columns accessed.
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    static constexpr int64_t MORSEL = 100000;
    std::atomic<int64_t> morsel_ctr{0};

    // Per-thread aggregation: agg[nation_idx][year_idx]
    // 25 nations × 7 years = 175 doubles per thread (1400 bytes — fits in L1).
    // Use padding (AGG_STRIDE=8) to avoid false sharing between threads' arrays.
    // But since each thread has its OWN array, false sharing between threads
    // requires the arrays themselves to be on separate cache lines → use vector of
    // heap-allocated arrays.
    constexpr int AGG_N = 25;
    constexpr int AGG_Y = 7;
    // Pad each thread's array to 64-byte cache line boundary
    struct alignas(64) ThreadAgg {
        double v[AGG_N][AGG_Y];
    };
    std::vector<ThreadAgg> thread_agg(nthreads);
    for (auto& ta : thread_agg) memset(ta.v, 0, sizeof(ta.v));

    // Cache raw pointers for the inner loop
    const int32_t* lp_col = l_partkey.data;
    const int32_t* ls_col = l_suppkey.data;
    const int32_t* lo_col = l_orderkey.data;
    const double*  ep_col = l_extprice.data;
    const double*  dc_col = l_discount.data;
    const double*  qt_col = l_quantity.data;
    const uint64_t* ps_keys   = ps_hash.keys;
    const double*   ps_values = ps_hash.values;

    auto worker = [&](int tid) {
        double (*agg)[AGG_Y] = thread_agg[tid].v;

        while (true) {
            int64_t start = morsel_ctr.fetch_add(MORSEL, std::memory_order_relaxed);
            if (start >= total_rows) break;
            int64_t end = start + MORSEL;
            if (end > total_rows) end = total_rows;

            for (int64_t i = start; i < end; i++) {
                // --- Part filter (94.5% of rows rejected here) -----------
                int32_t lp = lp_col[i];
                if ((uint32_t)lp >= (uint32_t)MAX_PART || !is_green[lp]) continue;

                // --- Supplier: O(1) direct array lookup ------------------
                int32_t ls = ls_col[i];
                if ((uint32_t)ls >= (uint32_t)MAX_SUPP) continue;
                int nat = (int)supp_nat[ls];

                // --- Orders: O(1) direct year_idx lookup -----------------
                int32_t lo = lo_col[i];
                if ((uint32_t)lo > (uint32_t)max_okey) continue;
                int yr = (int)okey_year_arr[lo];
                if ((unsigned)yr > 6u) continue; // 0xFF = unused slot

                // --- Partsupp: compact hash probe (16MB, L3-resident) ----
                uint64_t ps_key = ((uint64_t)(uint32_t)lp << 32) | (uint32_t)ls;
                uint32_t h = (uint32_t)(hash64(ps_key) & PS_MASK);
                while (ps_keys[h] != ps_key) {
                    h = (h + 1) & PS_MASK;
                }
                double ps_cost = ps_values[h];

                // --- Compute amount and accumulate -----------------------
                double amount = ep_col[i] * (1.0 - dc_col[i]) - ps_cost * qt_col[i];
                agg[nat][yr] += amount;
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    threads.reserve(nthreads);
    for (int t = 0; t < nthreads; t++)
        threads.emplace_back(worker, t);
    for (auto& th : threads) th.join();

    // -----------------------------------------------------------------------
    // Merge thread-local aggregations → final_agg[25][7]
    // -----------------------------------------------------------------------
    double final_agg[AGG_N][AGG_Y] = {};
    for (int t = 0; t < nthreads; t++) {
        for (int n = 0; n < AGG_N; n++)
            for (int y = 0; y < AGG_Y; y++)
                final_agg[n][y] += thread_agg[t].v[n][y];
    }

    // -----------------------------------------------------------------------
    // sort_output + write CSV
    // -----------------------------------------------------------------------
    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    rows.reserve(175);
    for (int n = 0; n < AGG_N; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < AGG_Y; y++) {
            if (final_agg[n][y] == 0.0) continue; // skip groups with no matching rows
            Row r;
            strncpy(r.nation, nation_names[n], 25);
            r.nation[25] = '\0';
            r.year = 1992 + y;
            r.sum_profit = final_agg[n][y];
            rows.push_back(r);
        }
    }
    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
        int c = strcmp(a.nation, b.nation);
        if (c != 0) return c < 0;
        return a.year > b.year; // o_year DESC
    });

    // --- Output -----------------------------------------------------------
    {
        GENDB_PHASE("output");
        const std::string outpath = rdir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror("fopen output"); return 1; }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
        fclose(f);
    }

    // Cleanup
    free(supp_nat);
    free(is_green);
    munmap(yidx_ptr, yidx_sz);
    munmap((void*)p_name_buf, pname_sz);

    } // main_scan
    } // build_joins
    } // dim_filter
    } // data_loading

    return 0;
}
