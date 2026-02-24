#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <omp.h>

#include "timing_utils.h"

namespace {

// ─── Pre-join index slot ──────────────────────────────────────────────────────
struct PreSlot {
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  version_code;
    uint32_t payload_offset;
    uint32_t payload_count;
};
static_assert(sizeof(PreSlot) == 20, "PreSlot size mismatch");

// ─── Zone-map block ───────────────────────────────────────────────────────────
struct ZMBlock { int32_t min_ddate; int32_t max_ddate; uint32_t row_count; };
static_assert(sizeof(ZMBlock) == 12, "ZMBlock size mismatch");

// ─── Hash functions ───────────────────────────────────────────────────────────
static inline uint32_t hash_i32x3(int32_t a, int32_t b, int32_t c) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
    h ^= (uint64_t)(uint32_t)b * 2246822519ULL;
    h ^= (uint64_t)(uint32_t)c * 3266489917ULL;
    return (uint32_t)(h ^ (h >> 32));
}

static inline uint64_t hash_u64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

// ─── Next power of 2 ─────────────────────────────────────────────────────────
static inline uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    --n;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

// ─── Thread-local aggregation entry ──────────────────────────────────────────
struct AggEntry {
    uint64_t key;      // (uint64_t)(uint32_t)tag_code << 32 | (uint32_t)version_code; UINT64_MAX = sentinel
    int64_t  sum_cents;
    int64_t  cnt;
};

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const char* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    // No MAP_POPULATE: let zone-map-guided access do lazy page faulting;
    // avoids eagerly touching ~1GB of data when only ~20% of blocks qualify.
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const char*>(p);
}

// ─── Load dictionary ──────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f) { fprintf(stderr, "Cannot open dict: %s\n", path.c_str()); exit(1); }
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

// ─── Result struct ────────────────────────────────────────────────────────────
struct Result {
    int32_t tag_code;
    int32_t version_code;
    int64_t cnt;
    int64_t sum_cents;
};

// ─── Main query ───────────────────────────────────────────────────────────────
} // end anonymous namespace

void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── DATA LOADING ──────────────────────────────────────────────────────────
    const char* ddate_raw   = nullptr;
    const char* uom_raw     = nullptr;
    const char* adsh_raw    = nullptr;
    const char* tag_raw     = nullptr;
    const char* version_raw = nullptr;
    const char* value_raw   = nullptr;
    const char* zm_raw      = nullptr;
    const char* pre_idx_raw = nullptr;
    size_t ddate_sz, uom_sz, adsh_sz, tag_sz, version_sz, value_sz, zm_sz, pre_idx_sz;
    std::vector<std::string> tag_dict, version_dict;

    {
        GENDB_PHASE("data_loading");

        ddate_raw   = mmap_file(gendb_dir + "/num/ddate.bin",   ddate_sz);
        uom_raw     = mmap_file(gendb_dir + "/num/uom.bin",     uom_sz);
        adsh_raw    = mmap_file(gendb_dir + "/num/adsh.bin",    adsh_sz);
        tag_raw     = mmap_file(gendb_dir + "/num/tag.bin",     tag_sz);
        version_raw = mmap_file(gendb_dir + "/num/version.bin", version_sz);
        value_raw   = mmap_file(gendb_dir + "/num/value.bin",   value_sz);
        zm_raw      = mmap_file(gendb_dir + "/indexes/num_ddate_zone_map.bin", zm_sz);
        pre_idx_raw = mmap_file(gendb_dir + "/indexes/pre_join_index.bin",     pre_idx_sz);

        // Load output dicts here — avoids paying dict I/O cost inside output phase
        tag_dict     = load_dict(gendb_dir + "/num/tag_dict.txt");
        version_dict = load_dict(gendb_dir + "/num/version_dict.txt");
    }

    // Column pointers
    const int32_t* ddate_col   = reinterpret_cast<const int32_t*>(ddate_raw);
    const int16_t* uom_col     = reinterpret_cast<const int16_t*>(uom_raw);
    const int32_t* adsh_col    = reinterpret_cast<const int32_t*>(adsh_raw);
    const int32_t* tag_col     = reinterpret_cast<const int32_t*>(tag_raw);
    const int32_t* version_col = reinterpret_cast<const int32_t*>(version_raw);
    const double*  value_col   = reinterpret_cast<const double*>(value_raw);

    // Zone map
    uint32_t num_zm_blocks = *(const uint32_t*)zm_raw;
    const ZMBlock* zm = reinterpret_cast<const ZMBlock*>(zm_raw + 4);

    // Pre-join index header — C27/C32: declare ALL THREE at function scope
    uint32_t pre_cap  = *(const uint32_t*)pre_idx_raw;
    uint32_t pre_mask = pre_cap - 1;
    const PreSlot* pre_ht = reinterpret_cast<const PreSlot*>(pre_idx_raw + 4);

    // ── DIM FILTER — load uom dict, find USD code ─────────────────────────────
    int16_t usd_code = -1;
    {
        GENDB_PHASE("dim_filter");
        auto uom_dict = load_dict(gendb_dir + "/num/uom_dict.txt");
        for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c) {
            if (uom_dict[c] == "USD") { usd_code = c; break; }
        }
        if (usd_code == -1) { fprintf(stderr, "USD not found in uom_dict\n"); exit(1); }
    }

    // ── MAIN SCAN — morsel-driven parallel, thread-local aggregation ──────────
    // Estimated groups: ~50k (distinct non-pre-covered (tag,version) pairs)
    // Size for full cardinality (C9)
    const uint32_t AGG_CAP = next_pow2(100000u * 2u);  // 256K slots
    const uint64_t SENTINEL = UINT64_MAX;

    int nthreads = omp_get_max_threads();
    // Allocate per-thread aggregation maps + occupied-slot index for O(actual_groups) merge
    std::vector<std::vector<AggEntry>> tl_maps(nthreads);
    std::vector<std::vector<uint32_t>> tl_occupied(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // Initialize thread-local map with UINT64_MAX sentinel (C20: std::fill)
            tl_maps[tid].resize(AGG_CAP);
            std::fill(tl_maps[tid].begin(), tl_maps[tid].end(), AggEntry{SENTINEL, 0, 0});
            tl_occupied[tid].reserve(2048);  // reserve for expected groups per thread
            auto& local_map = tl_maps[tid];
            auto& local_occ = tl_occupied[tid];

            #pragma omp for schedule(dynamic, 4)
            for (uint32_t b = 0; b < num_zm_blocks; ++b) {
                // Zone-map block skip (C19)
                if (zm[b].max_ddate < 20230101 || zm[b].min_ddate > 20231231) continue;

                uint32_t row_start = b * 65536u;
                uint32_t block_rows = zm[b].row_count;

                for (uint32_t r = row_start; r < row_start + block_rows; ++r) {
                    // Filter: ddate BETWEEN 20230101 AND 20231231
                    int32_t ddate = ddate_col[r];
                    if (ddate < 20230101 || ddate > 20231231) continue;

                    // Filter: uom = 'USD' (C2: dict code compare)
                    if (uom_col[r] != usd_code) continue;

                    // Filter: value IS NOT NULL
                    double v = value_col[r];
                    if (std::isnan(v)) continue;

                    // Anti-join probe on pre_join_index (C24: bounded probe loop)
                    int32_t adsh_code    = adsh_col[r];
                    int32_t tag_code     = tag_col[r];
                    int32_t version_code = version_col[r];

                    uint32_t slot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
                    bool found_in_pre = false;
                    for (uint32_t probe = 0; probe < pre_cap; ++probe) {
                        uint32_t s = (slot + probe) & pre_mask;
                        if (pre_ht[s].adsh_code == -1) break;  // empty slot
                        if (pre_ht[s].adsh_code == adsh_code &&
                            pre_ht[s].tag_code  == tag_code &&
                            pre_ht[s].version_code == version_code) {
                            found_in_pre = true;
                            break;
                        }
                    }
                    if (found_in_pre) continue;  // anti-join: disqualify if found

                    // Accumulate into thread-local aggregation map
                    // Key: packed (tag_code, version_code)
                    uint64_t key = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)version_code;
                    int64_t iv = llround(v * 100.0);  // C29: int64_t cents

                    uint64_t h = hash_u64(key) & (AGG_CAP - 1);
                    for (uint32_t probe = 0; probe < AGG_CAP; ++probe) {
                        uint64_t s = (h + probe) & (AGG_CAP - 1);
                        if (local_map[s].key == SENTINEL) {
                            local_map[s].key       = key;
                            local_map[s].sum_cents  = iv;
                            local_map[s].cnt        = 1;
                            local_occ.push_back((uint32_t)s);  // track new occupied slot
                            break;
                        }
                        if (local_map[s].key == key) {
                            local_map[s].sum_cents += iv;
                            local_map[s].cnt++;
                            break;
                        }
                    }
                }
            }
        }  // end parallel
    }

    // ── AGGREGATION MERGE ─────────────────────────────────────────────────────
    // Merge all thread-local maps into a single global map (single-threaded)
    {
        GENDB_PHASE("aggregation_merge");

        // Global merge map
        const uint32_t GLOBAL_CAP = next_pow2(100000u * 2u);
        std::vector<AggEntry> global_map(GLOBAL_CAP);
        std::fill(global_map.begin(), global_map.end(), AggEntry{SENTINEL, 0, 0});

        // Merge only occupied slots — O(actual_groups × nthreads) not O(AGG_CAP × nthreads)
        for (int tid = 0; tid < nthreads; ++tid) {
            for (uint32_t idx : tl_occupied[tid]) {
                auto& e = tl_maps[tid][idx];

                uint64_t h = hash_u64(e.key) & (GLOBAL_CAP - 1);
                for (uint32_t probe = 0; probe < GLOBAL_CAP; ++probe) {
                    uint64_t s = (h + probe) & (GLOBAL_CAP - 1);
                    if (global_map[s].key == SENTINEL) {
                        global_map[s] = e;
                        break;
                    }
                    if (global_map[s].key == e.key) {
                        global_map[s].sum_cents += e.sum_cents;
                        global_map[s].cnt       += e.cnt;
                        break;
                    }
                }
            }
        }

        // Free thread-local maps
        for (auto& m : tl_maps) std::vector<AggEntry>().swap(m);
        for (auto& o : tl_occupied) std::vector<uint32_t>().swap(o);

        // ── HAVING + collect results ──────────────────────────────────────────
        std::vector<Result> results;
        results.reserve(4096);
        for (uint32_t i = 0; i < GLOBAL_CAP; ++i) {
            auto& e = global_map[i];
            if (e.key == SENTINEL) continue;
            if (e.cnt <= 10) continue;  // HAVING cnt > 10
            int32_t tc = (int32_t)(e.key >> 32);
            int32_t vc = (int32_t)(e.key & 0xFFFFFFFFu);
            results.push_back({tc, vc, e.cnt, e.sum_cents});
        }

        // ── TOP-K sort: cnt DESC, tag_code ASC (C33 stable tiebreaker) ────────
        size_t limit = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + limit, results.end(),
            [](const Result& a, const Result& b) {
                if (a.cnt != b.cnt) return a.cnt > b.cnt;
                if (a.tag_code != b.tag_code) return a.tag_code < b.tag_code;
                return a.version_code < b.version_code;
            });
        results.resize(limit);

        // ── OUTPUT ────────────────────────────────────────────────────────────
        {
            GENDB_PHASE("output");
            // tag_dict and version_dict already loaded in data_loading phase

            std::string outpath = results_dir + "/Q24.csv";
            FILE* fp = fopen(outpath.c_str(), "w");
            if (!fp) { perror(outpath.c_str()); exit(1); }

            fprintf(fp, "tag,version,cnt,total\n");
            for (auto& r : results) {
                const char* tag_str = tag_dict[r.tag_code].c_str();
                const char* ver_str = version_dict[r.version_code].c_str();
                int64_t whole  = r.sum_cents / 100;
                int64_t frac   = std::abs(r.sum_cents % 100);
                // Handle negative sums where whole == 0
                if (r.sum_cents < 0 && whole == 0) {
                    fprintf(fp, "\"%s\",\"%s\",%lld,-%lld.%02lld\n",
                        tag_str, ver_str,
                        (long long)r.cnt,
                        (long long)0, (long long)frac);
                } else {
                    fprintf(fp, "\"%s\",\"%s\",%lld,%lld.%02lld\n",
                        tag_str, ver_str,
                        (long long)r.cnt,
                        (long long)whole, (long long)frac);
                }
            }
            fclose(fp);
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q24(gendb_dir, results_dir);
    return 0;
}
#endif
