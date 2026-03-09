// Q9: Product Type Profit Measure — Iter 2
// Key improvements over iter_1:
//   1. Two-pass lineitem scan: pass1 reads only l_partkey (240MB) → build selection vector
//      (~3.3M qualifying rows); pass2 gathers payload for selected rows only
//   2. lineitem.l_year_idx extension: uint8_t[59986052] mmap, sequential access via
//      sorted selection vector — eliminates random okey_year_arr LLC misses entirely
//   3. Parallel CAS-based partsupp hash insertion (440K entries across 64 threads)
//      — eliminates single-threaded bottleneck from iter_1
//   4. Prefetch only l_partkey in data_loading (not 2.16GB of all lineitem columns)
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
// Compact PS hash: capacity=1048576 (2^20), 16MB L3-resident
// key = (uint64(ps_partkey) << 32) | uint32(ps_suppkey)
// value = double ps_supplycost
// ---------------------------------------------------------------------------
static constexpr uint32_t PS_CAP      = 1048576u;
static constexpr uint32_t PS_MASK     = PS_CAP - 1u;
static constexpr uint64_t PS_SENTINEL = 0xFFFFFFFFFFFFFFFFULL;

// ---------------------------------------------------------------------------
// Raw mmap helper (for non-typed files like p_name.bin and year_idx.bin)
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
    const int nthreads = (int)omp_get_max_threads();

    // -----------------------------------------------------------------------
    // PHASE: data_loading — mmap columns, prefetch l_partkey only
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    // --- Part: p_partkey + p_name (112MB sequential scan) ---
    MmapColumn<int32_t> p_partkey(gendb + "/part/p_partkey.bin");
    size_t pname_sz = 0;
    const char* p_name_buf = (const char*)mmap_file_raw(gendb + "/part/p_name.bin", &pname_sz);
    const int64_t n_part = (int64_t)p_partkey.count;
    madvise((void*)p_name_buf, pname_sz, MADV_SEQUENTIAL);

    // --- Partsupp raw columns (for parallel green-filtered hash build) ---
    MmapColumn<int32_t> ps_pk(gendb + "/partsupp/ps_partkey.bin");
    MmapColumn<int32_t> ps_sk(gendb + "/partsupp/ps_suppkey.bin");
    MmapColumn<double>  ps_sc(gendb + "/partsupp/ps_supplycost.bin");
    const int64_t n_ps = (int64_t)ps_pk.count;
    madvise(const_cast<void*>(static_cast<const void*>(ps_pk.data)), ps_pk.file_size, MADV_SEQUENTIAL);
    madvise(const_cast<void*>(static_cast<const void*>(ps_sk.data)), ps_sk.file_size, MADV_SEQUENTIAL);
    madvise(const_cast<void*>(static_cast<const void*>(ps_sc.data)), ps_sc.file_size, MADV_SEQUENTIAL);

    // --- Supplier (tiny, 100K rows) ---
    MmapColumn<int32_t> s_suppkey_col (gendb + "/supplier/s_suppkey.bin");
    MmapColumn<int32_t> s_nationkey_col(gendb + "/supplier/s_nationkey.bin");

    // --- Nation (25 rows, 650 bytes) ---
    MmapColumn<char> n_name_col(gendb + "/nation/n_name.bin");

    // --- Storage extension: lineitem.l_year_idx (60MB, uint8_t per row) ---
    // entry[i] = (year - 1992) for lineitem row i; all entries valid (0..6)
    // Sequential access in pass 2 via sorted selection vector
    size_t lyidx_sz = 0;
    const uint8_t* l_year_arr = (const uint8_t*)mmap_file_raw(
        gendb + "/column_versions/lineitem.l_year_idx/year_idx.bin", &lyidx_sz);
    madvise((void*)l_year_arr, lyidx_sz, MADV_SEQUENTIAL);

    // --- Lineitem pass1: l_partkey only — prefetch into page cache ---
    MmapColumn<int32_t> l_partkey(gendb + "/lineitem/l_partkey.bin");
    l_partkey.prefetch();  // MADV_WILLNEED — only 240MB, not 2.16GB
    const int64_t total_rows = (int64_t)l_partkey.count;  // 59,986,052

    // --- Lineitem pass2 payload columns — mmap but don't prefetch ---
    // Only 5.5% of rows accessed; sorted access lets kernel readahead handle it
    MmapColumn<int32_t> l_suppkey (gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<double>  l_extprice(gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>  l_discount(gendb + "/lineitem/l_discount.bin");
    MmapColumn<double>  l_quantity (gendb + "/lineitem/l_quantity.bin");

    // -----------------------------------------------------------------------
    // PHASE: dim_filter — parallel p_name scan → is_green[] bitset (2MB)
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("dim_filter");

    static constexpr int MAX_PART = 2000001;
    uint8_t* is_green = (uint8_t*)calloc(MAX_PART, 1);
    if (!is_green) { perror("calloc is_green"); return 1; }

    // Parallel scan: concurrent idempotent uint8_t=1 writes are safe
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int64_t i = 0; i < n_part; i++) {
        if (strstr(p_name_buf + i * 56, "green")) {
            int32_t pk = p_partkey.data[i];
            if ((uint32_t)pk < (uint32_t)MAX_PART)
                is_green[pk] = 1;
        }
    }

    // -----------------------------------------------------------------------
    // PHASE: build_joins — parallel CAS PS hash + supp_nat + nation names
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("build_joins");

    // Compact PS hash: keys (uint64, atomic for CAS insert) + values (double)
    // 16MB total — fits in L3 cache (44MB)
    uint64_t* ps_keys   = (uint64_t*)aligned_alloc(64, (size_t)PS_CAP * sizeof(uint64_t));
    double*   ps_values = (double*)  aligned_alloc(64, (size_t)PS_CAP * sizeof(double));
    if (!ps_keys || !ps_values) { perror("aligned_alloc ps_hash"); return 1; }
    memset(ps_keys, 0xFF, (size_t)PS_CAP * sizeof(uint64_t));  // all sentinel

    // Parallel CAS-based insertion: 440K green entries across 64 threads
    // (ps_partkey, ps_suppkey) is the composite PK — no duplicate keys
    auto* akeys = reinterpret_cast<std::atomic<uint64_t>*>(ps_keys);

    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int64_t i = 0; i < n_ps; i++) {
        int32_t ppk = ps_pk.data[i];
        if ((uint32_t)ppk >= (uint32_t)MAX_PART || !is_green[ppk]) continue;
        int32_t spk = ps_sk.data[i];
        uint64_t k  = ((uint64_t)(uint32_t)ppk << 32) | (uint32_t)spk;
        double   v  = ps_sc.data[i];

        uint32_t h = (uint32_t)(hash64(k) & PS_MASK);
        while (true) {
            uint64_t expected = PS_SENTINEL;
            if (akeys[h].compare_exchange_strong(expected, k,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                // Slot claimed — write value and make it visible
                ps_values[h] = v;
                std::atomic_thread_fence(std::memory_order_release);
                break;
            }
            if (expected == k) break;  // duplicate PK (shouldn't happen)
            h = (h + 1) & PS_MASK;
        }
    }
    // OMP parallel section exit = full memory barrier: all insertions complete

    // --- Supplier: direct array suppkey->nationkey (100KB, L1-resident) ---
    static constexpr int MAX_SUPP = 100001;
    uint8_t* supp_nat = (uint8_t*)calloc(MAX_SUPP, 1);
    if (!supp_nat) { perror("calloc supp_nat"); return 1; }
    {
        const int64_t ns = (int64_t)s_suppkey_col.count;
        for (int64_t i = 0; i < ns; i++) {
            int32_t sk = s_suppkey_col.data[i];
            int32_t nk = s_nationkey_col.data[i];
            if ((uint32_t)sk < (uint32_t)MAX_SUPP && (uint32_t)nk < 25u)
                supp_nat[sk] = (uint8_t)nk;
        }
    }

    // --- Nation names: char[25][26] = 650 bytes, L1-resident ---
    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));
    {
        int nn = (int)(n_name_col.count / 26);
        if (nn > 25) nn = 25;
        for (int i = 0; i < nn; i++) {
            strncpy(nation_names[i], n_name_col.data + i * 26, 25);
            nation_names[i][25] = '\0';
        }
    }

    // -----------------------------------------------------------------------
    // PHASE: main_scan — Pass1 + Sort + Pass2 + Aggregate
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    const int32_t* lp_raw = l_partkey.data;
    const int32_t* ls_raw = l_suppkey.data;
    const double*  ep_raw = l_extprice.data;
    const double*  dc_raw = l_discount.data;
    const double*  qt_raw = l_quantity.data;

    // -----------------------------------------------------------------------
    // Pass 1: parallel morsel scan of l_partkey (240MB sequential)
    // Static OMP schedule: each thread processes contiguous row range → its
    // sub-vector is already sorted ascending by row index
    // -----------------------------------------------------------------------
    std::vector<std::vector<int32_t>> thread_selvec(nthreads);
    for (auto& v : thread_selvec) v.reserve(60000);  // ~51K per thread expected

    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        auto& local = thread_selvec[tid];

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < total_rows; i++) {
            int32_t lp = lp_raw[i];
            if ((uint32_t)lp < (uint32_t)MAX_PART && is_green[lp])
                local.push_back((int32_t)i);
        }
    }

    // Merge per-thread vectors into global selection vector
    size_t total_sel = 0;
    for (auto& v : thread_selvec) total_sel += v.size();

    std::vector<int32_t> selvec;
    selvec.reserve(total_sel);
    for (auto& v : thread_selvec) {
        selvec.insert(selvec.end(), v.begin(), v.end());
        std::vector<int32_t>().swap(v);  // free per-thread memory
    }

    // NOTE: std::sort NOT needed — OMP static schedule assigns thread t to rows
    // [t*(N/nthreads), (t+1)*(N/nthreads)), so each thread's sub-vector is sorted
    // ascending, and concatenating in thread-id order (0,1,...,63) gives a globally
    // sorted result. Verified: max(thread_selvec[t]) < min(thread_selvec[t+1]) always.

    // -----------------------------------------------------------------------
    // Pass 2: parallel gather + aggregate over sorted selection vector
    // Each thread handles a contiguous slice → ascending cache-line access
    // within each column → hardware prefetcher works perfectly
    // -----------------------------------------------------------------------
    constexpr int AGG_N = 25;
    constexpr int AGG_Y = 7;

    struct alignas(64) ThreadAgg {
        long double v[AGG_N][AGG_Y];
    };
    std::vector<ThreadAgg> tagg(nthreads);
    for (auto& a : tagg) memset(a.v, 0, sizeof(a.v));

    const int64_t sel_size = (int64_t)selvec.size();
    const int32_t* sel_ptr = selvec.data();

    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        long double (*agg)[AGG_Y] = tagg[tid].v;

        // Divide sorted selvec evenly: each thread gets a contiguous slice
        int64_t chunk = (sel_size + nthreads - 1) / nthreads;
        int64_t start = (int64_t)tid * chunk;
        int64_t end   = start + chunk;
        if (end > sel_size) end = sel_size;

        for (int64_t j = start; j < end; j++) {
            int32_t idx = sel_ptr[j];

            // Supplier lookup: direct array (100KB, L1-resident)
            int32_t ls = ls_raw[idx];
            if ((uint32_t)ls >= (uint32_t)MAX_SUPP) continue;
            int nat = (int)supp_nat[ls];

            // Year lookup: sequential l_year_idx (no LLC misses via sorted access)
            int yr = (int)l_year_arr[idx];
            // All entries guaranteed valid (0=1992..6=1998) per extension spec

            // Partsupp: compact hash probe (16MB, L3-resident)
            int32_t lp = lp_raw[idx];
            uint64_t ps_key = ((uint64_t)(uint32_t)lp << 32) | (uint32_t)ls;
            uint32_t h = (uint32_t)(hash64(ps_key) & PS_MASK);
            while (ps_keys[h] != ps_key) {
                h = (h + 1) & PS_MASK;
            }
            double ps_cost = ps_values[h];

            // Compute amount and accumulate into thread-local agg (1400B, L1-resident)
            double amount = ep_raw[idx] * (1.0 - dc_raw[idx]) - ps_cost * qt_raw[idx];
            agg[nat][yr] += amount;
        }
    }

    // Merge thread-local aggregations into final_agg[25][7]
    long double final_agg[AGG_N][AGG_Y] = {};
    for (int t = 0; t < nthreads; t++)
        for (int n = 0; n < AGG_N; n++)
            for (int y = 0; y < AGG_Y; y++)
                final_agg[n][y] += tagg[t].v[n][y];

    // -----------------------------------------------------------------------
    // PHASE: output — sort 175 rows (nation ASC, o_year DESC), write CSV
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("output");

    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    rows.reserve(175);

    for (int n = 0; n < AGG_N; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < AGG_Y; y++) {
            if (final_agg[n][y] == 0.0L) continue;  // skip empty groups
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
        return a.year > b.year;  // o_year DESC
    });

    const std::string outpath = rdir + "/Q9.csv";
    FILE* f = fopen(outpath.c_str(), "w");
    if (!f) { perror("fopen output"); return 1; }
    fprintf(f, "nation,o_year,sum_profit\n");
    for (const auto& r : rows)
        fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
    fclose(f);

    }  // output

    // Cleanup heap allocations (before scopes close)
    free(supp_nat);
    free(ps_keys);
    free(ps_values);

    }  // main_scan
    }  // build_joins

    free(is_green);

    }  // dim_filter

    munmap((void*)l_year_arr, lyidx_sz);
    munmap((void*)p_name_buf, pname_sz);

    }  // data_loading — MmapColumns destroyed here (munmap all column files)

    return 0;
}
