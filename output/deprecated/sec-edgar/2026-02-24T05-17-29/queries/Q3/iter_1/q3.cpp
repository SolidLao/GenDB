#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cstdint>
#include <climits>
#include <algorithm>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}

static inline uint64_t hash_uint64(uint64_t key) {
    return key * 0x9E3779B97F4A7C15ULL;
}

// ---------------------------------------------------------------------------
// Aggregation map capacities
// CIK:  ~8000 distinct fy=2022 groups  → cap=16384 (≤50% load — C9)
// NC:   ~16000 distinct (name,cik) pairs → cap=32768 (≤50% load — C9)
// Thread-local memory budget (C9): 64 × (192KB + 512KB) = 44MB ≈ LLC
// Each thread only accesses its own 704KB → stays cache-resident
// ---------------------------------------------------------------------------
static constexpr uint32_t CIK_CAP  = 16384;
static constexpr uint32_t CIK_MASK = CIK_CAP - 1;
static constexpr uint32_t NC_CAP   = 32768;
static constexpr uint32_t NC_MASK  = NC_CAP - 1;

// Global aggregation maps — merge target (sequential write after parallel scan)
static int32_t  g_cik_keys[CIK_CAP];
static int64_t  g_cik_vals[CIK_CAP];
static uint64_t g_nc_keys[NC_CAP];
static int64_t  g_nc_vals[NC_CAP];

// ---------------------------------------------------------------------------
// Utility: mmap a file read-only
// ---------------------------------------------------------------------------
static const char* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    close(fd);
    return (const char*)p;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream ifs(path);
    if (!ifs) { fprintf(stderr, "Cannot open dict: %s\n", path.c_str()); exit(1); }
    std::string line;
    while (std::getline(ifs, line)) dict.push_back(line);
    return dict;
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // Column pointers declared at function scope (C32)
    const int16_t*  num_uom   = nullptr;
    const double*   num_value = nullptr;
    const int32_t*  num_adsh  = nullptr;
    const int32_t*  sub_fy    = nullptr;
    const int32_t*  sub_cik   = nullptr;
    const int32_t*  sub_name  = nullptr;
    const int32_t*  sub_adsh  = nullptr;
    size_t num_rows = 0;
    size_t sub_rows = 0;

    std::vector<std::string> uom_dict;
    std::vector<std::string> name_dict;
    int16_t usd_code = -1;

    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("data_loading");

        uom_dict  = load_dict(gendb_dir + "/num/uom_dict.txt");
        name_dict = load_dict(gendb_dir + "/sub/name_dict.txt");

        // Resolve USD dict code at startup (C2)
        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "USD") { usd_code = i; break; }
        if (usd_code < 0) { fprintf(stderr, "USD not found in uom_dict\n"); exit(1); }

        // mmap num columns (sequential scan — large files)
        size_t sz;
        num_uom   = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/num/uom.bin", sz));
        madvise((void*)num_uom, sz, MADV_SEQUENTIAL);
        num_rows  = sz / sizeof(int16_t);

        num_value = reinterpret_cast<const double*>(mmap_file(gendb_dir + "/num/value.bin", sz));
        madvise((void*)num_value, sz, MADV_SEQUENTIAL);

        num_adsh  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/adsh.bin", sz));
        madvise((void*)num_adsh, sz, MADV_SEQUENTIAL);

        // mmap sub dimension columns (small — WILLNEED to prefault)
        sub_fy    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/fy.bin", sz));
        madvise((void*)sub_fy, sz, MADV_WILLNEED);
        sub_rows  = sz / sizeof(int32_t);

        sub_cik   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/cik.bin", sz));
        madvise((void*)sub_cik, sz, MADV_WILLNEED);

        sub_name  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/name.bin", sz));
        madvise((void*)sub_name, sz, MADV_WILLNEED);

        // sub/adsh.bin needed for direct lookup table construction (344KB)
        sub_adsh  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/adsh.bin", sz));
        madvise((void*)sub_adsh, sz, MADV_WILLNEED);
    }

    // -----------------------------------------------------------------------
    // Build direct lookup arrays: adsh_code → (cik, name_code) for fy=2022 rows
    // adsh codes are in [0, sub_rows-1] = [0, 86134] (shared dict)
    // Array footprint: 86135 × 8 bytes = 672KB — fits in L3, shared read-only
    // Replaces 4.2MB hash-probe-based lookup with O(1) direct array access
    // -----------------------------------------------------------------------
    std::vector<int32_t> adsh_to_cik (sub_rows, -1);
    std::vector<int32_t> adsh_to_name(sub_rows, -1);

    {
        GENDB_PHASE("dim_filter");
        // Sequential scan of 86K sub rows — trivially fast
        for (size_t r = 0; r < sub_rows; r++) {
            if (sub_fy[r] == 2022) {
                int32_t ac       = sub_adsh[r];
                adsh_to_cik[ac]  = sub_cik[r];
                adsh_to_name[ac] = sub_name[r];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Allocate thread-local aggregation maps
    // Layout: thread t occupies [t*CAP, (t+1)*CAP) in each flat array
    // -----------------------------------------------------------------------
    int nthreads = omp_get_max_threads();

    std::vector<int32_t>  tl_cik_keys((size_t)nthreads * CIK_CAP);
    std::vector<int64_t>  tl_cik_vals((size_t)nthreads * CIK_CAP);
    std::vector<uint64_t> tl_nc_keys ((size_t)nthreads * NC_CAP);
    std::vector<int64_t>  tl_nc_vals ((size_t)nthreads * NC_CAP);

    // Parallel init: each thread initializes its own chunk (distributes page faults — P22)
    #pragma omp parallel for schedule(static)
    for (int t = 0; t < nthreads; t++) {
        int32_t*  ck = tl_cik_keys.data() + (size_t)t * CIK_CAP;
        int64_t*  cv = tl_cik_vals.data() + (size_t)t * CIK_CAP;
        uint64_t* nk = tl_nc_keys.data()  + (size_t)t * NC_CAP;
        int64_t*  nv = tl_nc_vals.data()  + (size_t)t * NC_CAP;
        // C20: std::fill for multi-byte sentinels — NEVER memset
        std::fill(ck, ck + CIK_CAP, INT32_MIN);
        std::fill(cv, cv + CIK_CAP, (int64_t)0);
        std::fill(nk, nk + NC_CAP,  (uint64_t)UINT64_MAX);
        std::fill(nv, nv + NC_CAP,  (int64_t)0);
    }

    // -----------------------------------------------------------------------
    // Parallel scan: each thread accumulates into its own thread-local maps
    // Parallelism: 64 threads × 8.6MB per thread vs 551MB single-thread
    // Direct array lookup replaces 4.2MB hash probe → O(1) cache-resident access
    // C29: accumulate as int64_t cents (llround) — HARD CONSTRAINT for num.value
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        const int64_t  total_rows = (int64_t)num_rows;
        const int32_t* a2cik      = adsh_to_cik.data();
        const int32_t* a2name     = adsh_to_name.data();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int32_t*  ck = tl_cik_keys.data() + (size_t)tid * CIK_CAP;
            int64_t*  cv = tl_cik_vals.data() + (size_t)tid * CIK_CAP;
            uint64_t* nk = tl_nc_keys.data()  + (size_t)tid * NC_CAP;
            int64_t*  nv = tl_nc_vals.data()  + (size_t)tid * NC_CAP;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < total_rows; i++) {
                if (num_uom[i] != usd_code) continue;

                double v = num_value[i];
                if (std::isnan(v)) continue;

                // Direct array lookup: O(1), no hash collision, 672KB stays in L3
                int32_t adsh_code = num_adsh[i];
                int32_t cik       = a2cik[adsh_code];
                if (cik < 0) continue;  // not fy=2022
                int32_t name_code = a2name[adsh_code];

                // C29: int64_t cents — mandatory for num.value (max ~1e16)
                int64_t iv = llround(v * 100.0);

                // Insert into thread-local CIK map
                uint32_t pos = (uint32_t)(hash_int32(cik) & CIK_MASK);
                for (uint32_t p = 0; p < CIK_CAP; p++) {  // C24: bounded probing
                    uint32_t s = (pos + p) & CIK_MASK;
                    if (ck[s] == INT32_MIN) { ck[s] = cik; cv[s]  = iv; break; }
                    if (ck[s] == cik)       {              cv[s] += iv; break; }
                }

                // C15: key MUST include BOTH GROUP BY dims (name_code and cik)
                uint64_t nc_key = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)cik;
                pos = (uint32_t)(hash_uint64(nc_key) & NC_MASK);
                for (uint32_t p = 0; p < NC_CAP; p++) {  // C24: bounded probing
                    uint32_t s = (pos + p) & NC_MASK;
                    if (nk[s] == UINT64_MAX) { nk[s] = nc_key; nv[s]  = iv; break; }
                    if (nk[s] == nc_key)     {                  nv[s] += iv; break; }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Init global maps before merge (C20: std::fill)
    std::fill(g_cik_keys, g_cik_keys + CIK_CAP, INT32_MIN);
    std::fill(g_cik_vals, g_cik_vals + CIK_CAP, (int64_t)0);
    std::fill(g_nc_keys,  g_nc_keys  + NC_CAP,  (uint64_t)UINT64_MAX);
    std::fill(g_nc_vals,  g_nc_vals  + NC_CAP,  (int64_t)0);

    // -----------------------------------------------------------------------
    // Sequential merge: thread-local maps → global maps
    // Cost: 64 × (16384 + 32768) = ~3M slot scans, all cache-resident → fast
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < nthreads; t++) {
            // Merge CIK map
            const int32_t* ck = tl_cik_keys.data() + (size_t)t * CIK_CAP;
            const int64_t* cv = tl_cik_vals.data() + (size_t)t * CIK_CAP;
            for (uint32_t s = 0; s < CIK_CAP; s++) {
                if (ck[s] == INT32_MIN) continue;
                int32_t  cik   = ck[s];
                int64_t  cents = cv[s];
                uint32_t pos   = (uint32_t)(hash_int32(cik) & CIK_MASK);
                for (uint32_t p = 0; p < CIK_CAP; p++) {  // C24: bounded
                    uint32_t gs = (pos + p) & CIK_MASK;
                    if (g_cik_keys[gs] == INT32_MIN) { g_cik_keys[gs] = cik; g_cik_vals[gs]  = cents; break; }
                    if (g_cik_keys[gs] == cik)       {                        g_cik_vals[gs] += cents; break; }
                }
            }

            // Merge NC map
            const uint64_t* nk = tl_nc_keys.data() + (size_t)t * NC_CAP;
            const int64_t*  nv = tl_nc_vals.data() + (size_t)t * NC_CAP;
            for (uint32_t s = 0; s < NC_CAP; s++) {
                if (nk[s] == UINT64_MAX) continue;
                uint64_t key   = nk[s];
                int64_t  cents = nv[s];
                uint32_t pos   = (uint32_t)(hash_uint64(key) & NC_MASK);
                for (uint32_t p = 0; p < NC_CAP; p++) {  // C24: bounded
                    uint32_t gs = (pos + p) & NC_MASK;
                    if (g_nc_keys[gs] == UINT64_MAX) { g_nc_keys[gs] = key; g_nc_vals[gs]  = cents; break; }
                    if (g_nc_keys[gs] == key)        {                       g_nc_vals[gs] += cents; break; }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compute threshold T = AVG(SUM(value) per cik) expressed in cents
    // C29: keep in cents; compare g_nc_vals[s] (cents) against T (cents)
    // -----------------------------------------------------------------------
    double T = 0.0;
    {
        int64_t total_cents = 0;
        int64_t cik_count   = 0;
        for (uint32_t s = 0; s < CIK_CAP; s++) {
            if (g_cik_keys[s] != INT32_MIN) {
                total_cents += g_cik_vals[s];
                cik_count++;
            }
        }
        if (cik_count > 0)
            T = (double)total_cents / (double)cik_count;
    }

    // -----------------------------------------------------------------------
    // Apply HAVING SUM > T, collect qualifying (name, cik) results
    // -----------------------------------------------------------------------
    struct Result {
        int32_t name_code;
        int32_t cik;
        int64_t sum_cents;
    };
    std::vector<Result> results;
    results.reserve(2048);

    for (uint32_t s = 0; s < NC_CAP; s++) {
        if (g_nc_keys[s] == UINT64_MAX) continue;
        int64_t sc = g_nc_vals[s];
        if ((double)sc > T) {
            int32_t name_code = (int32_t)(uint32_t)(g_nc_keys[s] >> 32);
            int32_t cik       = (int32_t)(uint32_t)(g_nc_keys[s] & 0xFFFFFFFFULL);
            results.push_back({name_code, cik, sc});
        }
    }

    // Top-100: ORDER BY total_value DESC, cik ASC (C33: stable tiebreaker)
    int topk = std::min((int)results.size(), 100);
    std::partial_sort(results.begin(), results.begin() + topk, results.end(),
        [](const Result& a, const Result& b) {
            if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
            return a.cik < b.cik;  // C33: stable unique tiebreaker
        });
    results.resize(topk);

    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); exit(1); }

        fprintf(f, "name,cik,total_value\n");
        for (const auto& r : results) {
            // C31: double-quote all string/dict output columns
            // C29: format int64_t cents as dollars with exactly 2 decimal places
            int64_t sc    = r.sum_cents;
            int64_t whole = sc / 100;
            int64_t frac  = std::abs(sc % 100);
            fprintf(f, "\"%s\",%d,%lld.%02lld\n",
                name_dict[r.name_code].c_str(),
                r.cik,
                (long long)whole, (long long)frac);
        }
        fclose(f);
    }
}

// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
