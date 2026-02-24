// Q3: JOIN num→sub on adsh, filter uom='USD' AND fy=2022 AND value IS NOT NULL
// GROUP BY (name, cik), HAVING SUM > AVG(per-cik sums), ORDER BY total_value DESC LIMIT 100
// Strategy: single parallel scan over num, thread-local dual aggregation, merge, HAVING filter, top-100
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
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
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------
static inline uint32_t hash_i32(int32_t k) {
    uint32_t x = (uint32_t)k;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint32_t hash_u64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return (uint32_t)k;
}

// ---------------------------------------------------------------------------
// Pre-built sub_adsh_index layout
// ---------------------------------------------------------------------------
struct SubSlot { int32_t adsh_code; int32_t sub_row; };

// ---------------------------------------------------------------------------
// Thread-local main aggregation: key = packed(name_code<<32 | cik), val = int64_t cents
// cap = next_pow2(27000*2) = 65536
// ---------------------------------------------------------------------------
static constexpr uint32_t MAIN_CAP   = 65536;
static constexpr uint32_t MAIN_MASK  = MAIN_CAP - 1;
static constexpr uint64_t MAIN_EMPTY = UINT64_MAX;

struct MainMap {
    uint64_t keys[MAIN_CAP];
    int64_t  vals[MAIN_CAP];

    void init() {
        std::fill(keys, keys + MAIN_CAP, MAIN_EMPTY);
        std::fill(vals, vals + MAIN_CAP, int64_t(0));
    }

    inline void add(uint64_t key, int64_t cents) {
        uint32_t h = hash_u64(key) & MAIN_MASK;
        for (uint32_t probe = 0; probe < MAIN_CAP; ++probe) {
            uint32_t s = (h + probe) & MAIN_MASK;
            if (keys[s] == MAIN_EMPTY) { keys[s] = key; vals[s] = cents; return; }
            if (keys[s] == key)        { vals[s] += cents; return; }
        }
    }
};

// ---------------------------------------------------------------------------
// Thread-local cik aggregation: key = int32_t cik, val = int64_t cents
// ---------------------------------------------------------------------------
static constexpr uint32_t CIK_CAP   = 65536;
static constexpr uint32_t CIK_MASK  = CIK_CAP - 1;
static constexpr int32_t  CIK_EMPTY = INT32_MIN;

struct CikMap {
    int32_t  keys[CIK_CAP];
    int64_t  vals[CIK_CAP];

    void init() {
        std::fill(keys, keys + CIK_CAP, CIK_EMPTY);
        std::fill(vals, vals + CIK_CAP, int64_t(0));
    }

    inline void add(int32_t key, int64_t cents) {
        uint32_t h = hash_i32(key) & CIK_MASK;
        for (uint32_t probe = 0; probe < CIK_CAP; ++probe) {
            uint32_t s = (h + probe) & CIK_MASK;
            if (keys[s] == CIK_EMPTY) { keys[s] = key; vals[s] = cents; return; }
            if (keys[s] == key)       { vals[s] += cents; return; }
        }
    }
};

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static const char* mmap_file(const std::string& path, size_t* out_size = nullptr) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st; fstat(fd, &st);
    if (out_size) *out_size = (size_t)st.st_size;
    const char* ptr = (const char*)mmap(nullptr, st.st_size, PROT_READ,
                                         MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    return ptr;
}

static inline int64_t to_cents(double v) {
    return (int64_t)llround(v * 100.0);
}

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Data loading ----
    size_t num_bytes = 0;
    const char* num_adsh_raw  = nullptr;
    const char* num_uom_raw   = nullptr;
    const char* num_value_raw = nullptr;
    const char* sub_fy_raw    = nullptr;
    const char* sub_name_raw  = nullptr;
    const char* sub_cik_raw   = nullptr;
    const char* sub_idx_raw   = nullptr;

    {
        GENDB_PHASE("data_loading");
        num_adsh_raw  = mmap_file(gendb_dir + "/num/adsh.bin",  &num_bytes);
        num_uom_raw   = mmap_file(gendb_dir + "/num/uom.bin");
        num_value_raw = mmap_file(gendb_dir + "/num/value.bin");
        sub_fy_raw    = mmap_file(gendb_dir + "/sub/fy.bin");
        sub_name_raw  = mmap_file(gendb_dir + "/sub/name.bin");
        sub_cik_raw   = mmap_file(gendb_dir + "/sub/cik.bin");
        sub_idx_raw   = mmap_file(gendb_dir + "/indexes/sub_adsh_index.bin");
    }

    const size_t   num_rows  = num_bytes / sizeof(int32_t);
    const int32_t* num_adsh  = reinterpret_cast<const int32_t*>(num_adsh_raw);
    const int16_t* num_uom   = reinterpret_cast<const int16_t*>(num_uom_raw);
    const double*  num_value = reinterpret_cast<const double*>(num_value_raw);
    const int32_t* sub_fy    = reinterpret_cast<const int32_t*>(sub_fy_raw);
    const int32_t* sub_name  = reinterpret_cast<const int32_t*>(sub_name_raw);
    const int32_t* sub_cik   = reinterpret_cast<const int32_t*>(sub_cik_raw);

    // C27/C32: parse sub_adsh_index header at function scope
    uint32_t       sub_cap  = *(const uint32_t*)sub_idx_raw;
    uint32_t       sub_mask = sub_cap - 1;
    const SubSlot* sub_ht   = (const SubSlot*)(sub_idx_raw + 4);

    // ---- Dimension filter: load uom dict, find USD code (C2) ----
    int16_t usd_code = -1;
    {
        GENDB_PHASE("dim_filter");
        std::ifstream f(gendb_dir + "/num/uom_dict.txt");
        std::string line;
        int16_t idx = 0;
        while (std::getline(f, line)) {
            if (line == "USD") { usd_code = idx; break; }
            ++idx;
        }
    }

    // Load name dict for output decode
    std::vector<std::string> name_dict;
    {
        std::ifstream f(gendb_dir + "/sub/name_dict.txt");
        std::string line;
        while (std::getline(f, line)) name_dict.push_back(line);
    }

    // ---- Parallel scan with thread-local dual aggregation (P20, P4) ----
    const int nthreads = omp_get_max_threads();
    std::vector<MainMap*> tl_main(nthreads, nullptr);
    std::vector<CikMap*>  tl_cik(nthreads, nullptr);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            tl_main[tid] = new MainMap();
            tl_cik[tid]  = new CikMap();
            tl_main[tid]->init();
            tl_cik[tid]->init();

            #pragma omp for schedule(static, 8192)
            for (size_t i = 0; i < num_rows; ++i) {
                // Filter: uom == USD (C2)
                if (num_uom[i] != usd_code) continue;
                // Filter: value IS NOT NULL
                double v = num_value[i];
                if (std::isnan(v)) continue;

                int32_t adsh_code = num_adsh[i];

                // Probe sub_adsh_index (C24: bounded loop, C27: sub_cap/sub_mask/sub_ht at fn scope)
                uint32_t slot = hash_i32(adsh_code) & sub_mask;
                for (uint32_t probe = 0; probe < sub_cap; ++probe) {
                    uint32_t s = (slot + probe) & sub_mask;
                    if (sub_ht[s].adsh_code == -1) break;       // sentinel: not found
                    if (sub_ht[s].adsh_code == adsh_code) {
                        int32_t sub_row = sub_ht[s].sub_row;
                        // Filter: fy == 2022
                        if (sub_fy[sub_row] != 2022) break;

                        int32_t name_code = sub_name[sub_row];
                        int32_t cik       = sub_cik[sub_row];

                        // C29 HARD: int64_t cents via long double intermediate
                        int64_t iv = to_cents(v);

                        // Main agg: key = packed(name_code<<32|cik) (C15: both GROUP BY cols)
                        uint64_t key = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)cik;
                        tl_main[tid]->add(key, iv);

                        // Cik agg: same pass (P4)
                        tl_cik[tid]->add(cik, iv);
                        break;
                    }
                }
            }
        } // end parallel
    }

    // ---- Merge thread-local maps ----
    MainMap* global_main = new MainMap();
    CikMap*  global_cik  = new CikMap();

    {
        GENDB_PHASE("aggregation_merge");
        global_main->init();
        global_cik->init();

        for (int t = 0; t < nthreads; ++t) {
            if (!tl_main[t]) continue;
            for (uint32_t i = 0; i < MAIN_CAP; ++i) {
                if (tl_main[t]->keys[i] != MAIN_EMPTY)
                    global_main->add(tl_main[t]->keys[i], tl_main[t]->vals[i]);
            }
            for (uint32_t i = 0; i < CIK_CAP; ++i) {
                if (tl_cik[t]->keys[i] != CIK_EMPTY)
                    global_cik->add(tl_cik[t]->keys[i], tl_cik[t]->vals[i]);
            }
            delete tl_main[t]; tl_main[t] = nullptr;
            delete tl_cik[t];  tl_cik[t]  = nullptr;
        }
    }

    // ---- Compute HAVING threshold ----
    // threshold_cents = AVG(per-cik SUM) as int64_t cents
    double threshold_cents = 0.0;
    {
        int64_t total_cik_sum = 0;
        int64_t n_ciks = 0;
        for (uint32_t i = 0; i < CIK_CAP; ++i) {
            if (global_cik->keys[i] != CIK_EMPTY) {
                total_cik_sum += global_cik->vals[i];
                ++n_ciks;
            }
        }
        if (n_ciks > 0) threshold_cents = (double)total_cik_sum / (double)n_ciks;
    }
    delete global_cik; global_cik = nullptr;

    // ---- Collect results passing HAVING ----
    struct Result {
        int32_t name_code;
        int32_t cik;
        int64_t sum_cents;
    };

    std::vector<Result> results;
    results.reserve(10000);

    for (uint32_t i = 0; i < MAIN_CAP; ++i) {
        if (global_main->keys[i] == MAIN_EMPTY) continue;
        int64_t sc = global_main->vals[i];
        if ((double)sc > threshold_cents) {
            uint64_t  k         = global_main->keys[i];
            int32_t   name_code = (int32_t)(k >> 32);
            int32_t   cik       = (int32_t)(k & 0xFFFFFFFFULL);
            results.push_back({name_code, cik, sc});
        }
    }
    delete global_main; global_main = nullptr;

    // ---- Top-100 partial sort (P6) ----
    // ORDER BY total_value DESC, name_code ASC, cik ASC (C33: stable tiebreaker)
    {
        GENDB_PHASE("sort_topk");
        size_t k = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const Result& a, const Result& b) -> bool {
                if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
                if (a.name_code != b.name_code) return a.name_code < b.name_code;
                return a.cik < b.cik;
            });
        results.resize(k);
    }

    // ---- Output CSV (C31: double-quote name string) ----
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q3.csv";
        FILE* out = fopen(outpath.c_str(), "w");
        if (!out) { perror(outpath.c_str()); return; }
        fprintf(out, "name,cik,total_value\n");
        for (const auto& r : results) {
            const std::string& name = name_dict[(size_t)r.name_code];
            int64_t sc     = r.sum_cents;
            int64_t sc_abs = std::abs(sc);
            int64_t whole  = sc_abs / 100;
            int64_t frac   = sc_abs % 100;
            if (sc < 0) {
                fprintf(out, "\"%s\",%d,-%lld.%02lld\n",
                        name.c_str(), r.cik,
                        (long long)whole, (long long)frac);
            } else {
                fprintf(out, "\"%s\",%d,%lld.%02lld\n",
                        name.c_str(), r.cik,
                        (long long)whole, (long long)frac);
            }
        }
        fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
