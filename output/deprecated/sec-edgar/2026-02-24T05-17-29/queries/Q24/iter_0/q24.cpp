#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <climits>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include <iostream>
#include "timing_utils.h"

// ─── Hash helpers ─────────────────────────────────────────────────────────────
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}
static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
}

// ─── Pre-built index slot (verbatim from build_indexes.cpp) ──────────────────
struct PreTripleSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_id;
};  // 16 bytes

// ─── Thread-local aggregation ─────────────────────────────────────────────────
static constexpr uint32_t AGG_CAP  = 16384;
static constexpr uint32_t AGG_MASK = AGG_CAP - 1;
static constexpr uint64_t EMPTY_KEY = UINT64_MAX;

struct AggSlot {
    uint64_t key;
    int64_t  count;
    int64_t  sum_cents;
};

static inline uint64_t make_agg_key(int32_t tag_code, int32_t ver_code) {
    return ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)ver_code;
}

static inline void agg_insert(AggSlot* ht, int32_t tc, int32_t vc, int64_t cents) {
    uint64_t k = make_agg_key(tc, vc);
    uint32_t pos = (uint32_t)(hash_combine(hash_int32(tc), hash_int32(vc)) & AGG_MASK);
    for (uint32_t probe = 0; probe < AGG_CAP; probe++) {  // C24: bounded
        uint32_t slot = (pos + probe) & AGG_MASK;
        if (ht[slot].key == EMPTY_KEY) {
            ht[slot].key       = k;
            ht[slot].count     = 1;
            ht[slot].sum_cents = cents;
            return;
        }
        if (ht[slot].key == k) {
            ht[slot].count++;
            ht[slot].sum_cents += cents;
            return;
        }
    }
}

// ─── Dictionary loader ───────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const void* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

// ─── Query Q24 ───────────────────────────────────────────────────────────────
void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Variables declared at function scope (C32) ────────────────────────────
    uint32_t             pth_cap  = 0;
    uint32_t             pth_mask = 0;
    const PreTripleSlot* pth_ht   = nullptr;

    const int16_t* num_uom   = nullptr;
    const int32_t* num_ddate = nullptr;
    const double*  num_value = nullptr;
    const int32_t* num_adsh  = nullptr;
    const int32_t* num_tag   = nullptr;
    const int32_t* num_ver   = nullptr;
    size_t         num_rows  = 0;
    int16_t        usd_code  = -1;

    std::vector<std::string> tag_dict;
    std::vector<std::string> version_dict;

    // ── DATA LOADING ──────────────────────────────────────────────────────────
    {
        GENDB_PHASE("data_loading");

        // Load uom dict to find usd_code at runtime (C2)
        {
            auto uom_dict = load_dict(gendb_dir + "/num/uom_dict.txt");
            for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
                if (uom_dict[i] == "USD") { usd_code = i; break; }
            if (usd_code < 0) { fprintf(stderr, "USD not found in uom_dict\n"); exit(1); }
        }

        tag_dict     = load_dict(gendb_dir + "/shared/tag_dict.txt");
        version_dict = load_dict(gendb_dir + "/shared/version_dict.txt");

        // mmap pre_triple_hash and prefetch immediately (P11, overlaps num loads)
        size_t pre_sz = 0;
        const char* pre_raw = (const char*)mmap_ro(
            gendb_dir + "/pre/indexes/pre_triple_hash.bin", pre_sz);
        madvise((void*)pre_raw, pre_sz, MADV_WILLNEED);

        // Parse header at function scope (C32, C27)
        pth_cap  = *(const uint32_t*)pre_raw;
        pth_mask = pth_cap - 1;
        pth_ht   = (const PreTripleSlot*)(pre_raw + 4);

        // mmap num columns (sequential access)
        size_t sz;

        sz = 0;
        num_uom   = (const int16_t*)mmap_ro(gendb_dir + "/num/uom.bin",     sz);
        num_rows  = sz / sizeof(int16_t);
        madvise((void*)num_uom,   sz, MADV_SEQUENTIAL);

        sz = 0;
        num_ddate = (const int32_t*)mmap_ro(gendb_dir + "/num/ddate.bin",   sz);
        madvise((void*)num_ddate, sz, MADV_SEQUENTIAL);

        sz = 0;
        num_value = (const double*)mmap_ro(gendb_dir + "/num/value.bin",    sz);
        madvise((void*)num_value, sz, MADV_SEQUENTIAL);

        sz = 0;
        num_adsh  = (const int32_t*)mmap_ro(gendb_dir + "/num/adsh.bin",    sz);
        madvise((void*)num_adsh,  sz, MADV_SEQUENTIAL);

        sz = 0;
        num_tag   = (const int32_t*)mmap_ro(gendb_dir + "/num/tag.bin",     sz);
        madvise((void*)num_tag,   sz, MADV_SEQUENTIAL);

        sz = 0;
        num_ver   = (const int32_t*)mmap_ro(gendb_dir + "/num/version.bin", sz);
        madvise((void*)num_ver,   sz, MADV_SEQUENTIAL);
    }

    // ── MAIN SCAN with thread-local aggregation (P17, P20) ───────────────────
    int nthreads = omp_get_max_threads();

    // Allocate thread-local aggregation maps (C20: use value-init, not memset)
    std::vector<std::vector<AggSlot>> tl_maps(nthreads, std::vector<AggSlot>(AGG_CAP));

    {
        GENDB_PHASE("main_scan");

        // Initialize thread-local maps with EMPTY_KEY sentinel
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int t = 0; t < nthreads; t++) {
            AggSlot* ht = tl_maps[t].data();
            for (uint32_t s = 0; s < AGG_CAP; s++) {
                ht[s].key       = EMPTY_KEY;
                ht[s].count     = 0;
                ht[s].sum_cents = 0;
            }
        }

        const size_t MORSEL = 100000;
        const size_t num_morsels = (num_rows + MORSEL - 1) / MORSEL;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            AggSlot* local_ht = tl_maps[tid].data();

            #pragma omp for schedule(dynamic, 1)
            for (size_t m = 0; m < num_morsels; m++) {
                size_t row_start = m * MORSEL;
                size_t row_end   = std::min(row_start + MORSEL, num_rows);

                for (size_t i = row_start; i < row_end; i++) {
                    // Filter 1: uom == USD (C2: runtime dict code)
                    if (num_uom[i] != usd_code) continue;

                    // Filter 2: ddate BETWEEN 20230101 AND 20231231
                    // (C1 exemption: raw int32_t YYYYMMDD, no epoch conversion)
                    int32_t dd = num_ddate[i];
                    if (dd < 20230101 || dd > 20231231) continue;

                    // Filter 3: value IS NOT NULL (NaN sentinel)
                    double v = num_value[i];
                    if (std::isnan(v)) continue;

                    // Anti-join probe into pre_triple_hash
                    int32_t ak = num_adsh[i];
                    int32_t tc = num_tag[i];
                    int32_t vc = num_ver[i];

                    uint64_t h = hash_combine(
                                     hash_combine(hash_int32(ak), hash_int32(tc)),
                                     hash_int32(vc));
                    uint32_t pos = (uint32_t)(h & pth_mask);

                    bool found_in_pre = false;
                    for (uint32_t probe = 0; probe < pth_cap; probe++) {  // C24: bounded
                        uint32_t slot = (pos + probe) & pth_mask;
                        if (pth_ht[slot].adsh_code == INT32_MIN) break;  // empty → not found
                        if (pth_ht[slot].adsh_code == ak &&
                            pth_ht[slot].tag_code   == tc &&
                            pth_ht[slot].ver_code   == vc) {
                            found_in_pre = true;
                            break;
                        }
                    }

                    if (!found_in_pre) {
                        // p.adsh IS NULL → include in aggregation
                        int64_t cents = llround(v * 100.0);  // C29: int64_t cents
                        agg_insert(local_ht, tc, vc, cents); // C15: both dimensions
                    }
                }
            }
        }
    }

    // ── MERGE THREAD-LOCAL MAPS INTO GLOBAL MAP ───────────────────────────────
    std::vector<AggSlot> global_ht(AGG_CAP);
    {
        GENDB_PHASE("aggregation_merge");

        // Init global map
        for (uint32_t s = 0; s < AGG_CAP; s++) {
            global_ht[s].key       = EMPTY_KEY;
            global_ht[s].count     = 0;
            global_ht[s].sum_cents = 0;
        }

        // Sequential merge (P17: ~5000 groups per thread → trivial cost)
        for (int t = 0; t < nthreads; t++) {
            const AggSlot* lht = tl_maps[t].data();
            for (uint32_t s = 0; s < AGG_CAP; s++) {
                if (lht[s].key == EMPTY_KEY) continue;
                uint64_t k = lht[s].key;
                int32_t  tc = (int32_t)(k >> 32);
                int32_t  vc = (int32_t)(k & 0xFFFFFFFFu);
                uint32_t pos = (uint32_t)(hash_combine(hash_int32(tc), hash_int32(vc)) & AGG_MASK);
                for (uint32_t probe = 0; probe < AGG_CAP; probe++) {  // C24: bounded
                    uint32_t gslot = (pos + probe) & AGG_MASK;
                    if (global_ht[gslot].key == EMPTY_KEY) {
                        global_ht[gslot].key       = k;
                        global_ht[gslot].count     = lht[s].count;
                        global_ht[gslot].sum_cents = lht[s].sum_cents;
                        break;
                    }
                    if (global_ht[gslot].key == k) {
                        global_ht[gslot].count     += lht[s].count;
                        global_ht[gslot].sum_cents += lht[s].sum_cents;
                        break;
                    }
                }
            }
        }
    }

    // ── HAVING FILTER + TOP-K SORT ────────────────────────────────────────────
    struct ResultRow {
        int32_t tag_code;
        int32_t ver_code;
        int64_t count;
        int64_t sum_cents;
    };

    std::vector<ResultRow> results;
    results.reserve(512);

    {
        GENDB_PHASE("sort_topk");

        // HAVING count > 10
        for (uint32_t s = 0; s < AGG_CAP; s++) {
            if (global_ht[s].key == EMPTY_KEY) continue;
            if (global_ht[s].count <= 10) continue;
            uint64_t k = global_ht[s].key;
            results.push_back({
                (int32_t)(k >> 32),
                (int32_t)(k & 0xFFFFFFFFu),
                global_ht[s].count,
                global_ht[s].sum_cents
            });
        }

        // ORDER BY cnt DESC, tag_code ASC, ver_code ASC (C33: stable tiebreaker)
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.count != b.count) return a.count > b.count;
            if (a.tag_code != b.tag_code) return a.tag_code < b.tag_code;
            return a.ver_code < b.ver_code;
        };

        // LIMIT 100 (P6: partial_sort when needed)
        if ((int)results.size() > 100) {
            std::partial_sort(results.begin(), results.begin() + 100, results.end(), cmp);
            results.resize(100);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }
    }

    // ── OUTPUT ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q24.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); exit(1); }

        fprintf(fout, "tag,version,cnt,total\n");

        for (const auto& r : results) {
            // C31: double-quote all string columns
            fprintf(fout, "\"%s\",\"%s\",%lld,",
                    tag_dict[r.tag_code].c_str(),
                    version_dict[r.ver_code].c_str(),
                    (long long)r.count);
            // C29: output sum as int64_t cents
            int64_t sc = r.sum_cents;
            fprintf(fout, "%lld.%02lld\n",
                    (long long)(sc / 100),
                    (long long)std::abs(sc % 100));
        }

        fclose(fout);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q24(gendb_dir, results_dir);
    return 0;
}
#endif
