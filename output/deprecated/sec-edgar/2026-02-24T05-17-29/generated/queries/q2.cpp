#include <cstdio>
#include <cstdint>
#include <cmath>
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>
#include <cstring>
#include "timing_utils.h"

namespace {

// ===== Hash functions =====
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}

static inline uint64_t hash_key(int32_t adsh_code, int32_t tag_code) {
    return hash_int32(adsh_code) ^ (hash_int32(tag_code) * 0x517CC1B727220A95ULL);
}

static inline uint64_t pack_key(int32_t adsh_code, int32_t tag_code) {
    return ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tag_code;
}

// ===== Data structures =====
struct MaxSlot {
    uint64_t key;    // packed (adsh_code<<32 | (uint32_t)tag_code), UINT64_MAX = empty
    double   value;  // max value
};  // 16 bytes

struct SubADSHSlot {
    int32_t adsh_code;  // INT32_MIN = empty slot
    int32_t row_id;
    int32_t _pad0;
    int32_t _pad1;
};  // 16 bytes

// Compact tuple collected during pass 1 (avoids thread-local hash map sparse merge)
struct TupleEntry {
    int32_t adsh_code;
    int32_t tag_code;
    double  value;
};  // 16 bytes

struct Candidate {
    double   value;
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  name_code;
};

// ===== Hash map operations =====

// Insert or update max in hash map
static void max_map_update(MaxSlot* ht, uint32_t cap, uint32_t mask,
                           int32_t adsh, int32_t tag, double val) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_key(adsh, tag) & mask);
    for (uint32_t p = 0; p < cap; p++) {  // C24: bounded
        uint32_t slot = (pos + p) & mask;
        if (ht[slot].key == UINT64_MAX) {
            ht[slot].key   = key;
            ht[slot].value = val;
            return;
        }
        if (ht[slot].key == key) {
            if (val > ht[slot].value) ht[slot].value = val;
            return;
        }
    }
}

// Lookup max value for (adsh, tag); returns NaN if not found
static double max_map_lookup(const MaxSlot* ht, uint32_t cap, uint32_t mask,
                             int32_t adsh, int32_t tag) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_key(adsh, tag) & mask);
    for (uint32_t p = 0; p < cap; p++) {  // C24: bounded
        uint32_t slot = (pos + p) & mask;
        if (ht[slot].key == UINT64_MAX) return std::numeric_limits<double>::quiet_NaN();
        if (ht[slot].key == key) return ht[slot].value;
    }
    return std::numeric_limits<double>::quiet_NaN();
}

// ===== Utility =====
static void* mmap_file(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); size_out = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    size_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

// Fast dict loader: mmap + memchr avoids ifstream::getline overhead for large dicts
static std::vector<std::string> load_dict_fast(const std::string& path) {
    std::vector<std::string> dict;
    size_t sz = 0;
    const char* buf = (const char*)mmap_file(path, sz);
    if (!buf || sz == 0) return dict;

    const char* p   = buf;
    const char* end = buf + sz;
    while (p < end) {
        const char* nl  = (const char*)memchr(p, '\n', end - p);
        size_t      len = nl ? (size_t)(nl - p) : (size_t)(end - p);
        // Strip trailing \r (CRLF safety)
        if (len > 0 && p[len - 1] == '\r') len--;
        dict.emplace_back(p, len);
        if (!nl) break;
        p = nl + 1;
    }
    munmap((void*)buf, sz);
    return dict;
}

// ===== Main query =====
} // end anonymous namespace

void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ===== DATA LOADING =====
    size_t num_uom_sz, num_value_sz, num_adsh_sz, num_tag_sz;
    size_t sub_fy_sz, sub_name_sz, sub_adsh_idx_sz;

    const int16_t* num_uom          = nullptr;
    const double*  num_value        = nullptr;
    const int32_t* num_adsh         = nullptr;
    const int32_t* num_tag          = nullptr;
    const int32_t* sub_fy           = nullptr;
    const int32_t* sub_name         = nullptr;
    const char*    sub_adsh_idx_raw = nullptr;

    std::vector<std::string> uom_dict, tag_dict, name_dict;
    int16_t pure_code = -1;
    size_t  num_rows  = 0;

    {
        GENDB_PHASE("data_loading");

        // Load dicts via mmap+memchr (faster than ifstream::getline for large dicts)
        // C2: never hardcode dictionary codes — always resolve at runtime
        uom_dict  = load_dict_fast(gendb_dir + "/num/uom_dict.txt");
        tag_dict  = load_dict_fast(gendb_dir + "/shared/tag_dict.txt");
        name_dict = load_dict_fast(gendb_dir + "/sub/name_dict.txt");

        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "pure") { pure_code = i; break; }

        // mmap all needed num columns
        num_uom   = (const int16_t*)mmap_file(gendb_dir + "/num/uom.bin",   num_uom_sz);
        num_value = (const double*) mmap_file(gendb_dir + "/num/value.bin",  num_value_sz);
        num_adsh  = (const int32_t*)mmap_file(gendb_dir + "/num/adsh.bin",   num_adsh_sz);
        num_tag   = (const int32_t*)mmap_file(gendb_dir + "/num/tag.bin",    num_tag_sz);
        // mmap sub columns (small — WILLNEED to bring into cache)
        sub_fy    = (const int32_t*)mmap_file(gendb_dir + "/sub/fy.bin",     sub_fy_sz);
        sub_name  = (const int32_t*)mmap_file(gendb_dir + "/sub/name.bin",   sub_name_sz);
        sub_adsh_idx_raw = (const char*)mmap_file(
                               gendb_dir + "/sub/indexes/sub_adsh_hash.bin", sub_adsh_idx_sz);

        num_rows = num_uom_sz / sizeof(int16_t);

        // Sequential prefetch for large num columns (bandwidth-bound scans)
        madvise((void*)num_uom,   num_uom_sz,   MADV_SEQUENTIAL);
        madvise((void*)num_value, num_value_sz, MADV_SEQUENTIAL);
        madvise((void*)num_adsh,  num_adsh_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_tag,   num_tag_sz,   MADV_SEQUENTIAL);
        // Sub columns are small: eagerly bring into cache
        madvise((void*)sub_fy,           sub_fy_sz,       MADV_WILLNEED);
        madvise((void*)sub_name,         sub_name_sz,     MADV_WILLNEED);
        madvise((void*)sub_adsh_idx_raw, sub_adsh_idx_sz, MADV_WILLNEED);
    }

    // Parse sub_adsh_hash index header at function scope (C32: must be at function scope)
    uint32_t sub_cap  = *(const uint32_t*)sub_adsh_idx_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_adsh_idx_raw + 4);

    // ===== PASS 1: Parallel scan — collect qualifying tuples into per-thread vectors =====
    //
    // Root cause of previous 67ms merge bottleneck:
    //   64 thread-local hash maps × 262,144 slots = 16.7M slot visits during merge,
    //   but only ~94,564 slots are filled (0.56% occupancy) → 177× wasted work.
    //
    // Fix: collect compact TupleEntry structs (16 bytes each) into thread-local vectors.
    //   Each thread collects ~1,478 entries → ~23KB per thread (fits in L1 cache).
    //   Sequential aggregation then works on exactly 94,564 tuples → ~2ms vs 67ms.
    //
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<TupleEntry>> thread_tuples(nthreads);

    {
        GENDB_PHASE("build_joins");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_tuples[tid];

            #pragma omp for schedule(static, 4096) nowait
            for (size_t i = 0; i < num_rows; i++) {
                if (num_uom[i] != pure_code) continue;  // 99.76% skip here
                double val = num_value[i];
                if (std::isnan(val)) continue;
                local.push_back({num_adsh[i], num_tag[i], val});
            }
        }
    }

    // ===== AGGREGATE: Build global max_map from collected tuples (O(qualifying_rows)) =====
    // Cap sized for ~80,000 groups: next_pow2(80000 × 2) = 262144 (C9: load factor ~0.3)
    const uint32_t MAX_CAP  = 262144;
    const uint32_t MAX_MASK = MAX_CAP - 1;

    // Allocate global map (4MB — fits in LLC at 44MB)
    MaxSlot* global_map = (MaxSlot*)mmap(nullptr, MAX_CAP * sizeof(MaxSlot),
                                         PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // C20: explicit initialization (never memset for multi-byte sentinels)
    for (uint32_t i = 0; i < MAX_CAP; i++) {
        global_map[i].key   = UINT64_MAX;
        global_map[i].value = -std::numeric_limits<double>::infinity();
    }

    // Reserve flat tuple vector for pass 2 (eliminates second 39M-row scan)
    std::vector<TupleEntry> all_tuples;
    {
        size_t total = 0;
        for (int t = 0; t < nthreads; t++) total += thread_tuples[t].size();
        all_tuples.reserve(total);
    }

    {
        GENDB_PHASE("aggregation_merge");
        // Sequential: only ~94,564 hash updates into 4MB LLC-resident map (~2ms)
        // Simultaneously build all_tuples for pass 2 (avoids second full scan of 39M rows)
        for (int t = 0; t < nthreads; t++) {
            for (const auto& tup : thread_tuples[t]) {
                max_map_update(global_map, MAX_CAP, MAX_MASK,
                               tup.adsh_code, tup.tag_code, tup.value);
                all_tuples.push_back(tup);
            }
            // Release thread-local memory
            std::vector<TupleEntry>().swap(thread_tuples[t]);
        }
    }

    // ===== PASS 2: Probe max_map + sub_adsh_hash — over qualifying tuples ONLY =====
    // ~94,564 tuples instead of 39M rows → eliminates second bandwidth-bound scan
    std::vector<Candidate> all_candidates;

    {
        GENDB_PHASE("main_scan");

        for (const auto& tup : all_tuples) {
            // Check exact double equality with max_value (C29: safe — MAX not SUM)
            double max_val = max_map_lookup(global_map, MAX_CAP, MAX_MASK,
                                            tup.adsh_code, tup.tag_code);
            if (tup.value != max_val) continue;

            // Probe sub_adsh_hash for fy == 2022 (P11: pre-built index)
            uint32_t pos = (uint32_t)(hash_int32(tup.adsh_code) & sub_mask);
            for (uint32_t probe = 0; probe < sub_cap; probe++) {  // C24: bounded
                uint32_t slot = (pos + probe) & sub_mask;
                if (sub_ht[slot].adsh_code == INT32_MIN) break;   // empty → not found
                if (sub_ht[slot].adsh_code == tup.adsh_code) {
                    int32_t sub_row = sub_ht[slot].row_id;
                    if (sub_fy[sub_row] == 2022) {
                        int32_t name_code = sub_name[sub_row];
                        all_candidates.push_back({tup.value, tup.adsh_code,
                                                  tup.tag_code, name_code});
                    }
                    break;
                }
            }
        }
    }

    // ===== TOP-100: partial_sort by (value DESC, name ASC, tag ASC) =====
    // C33: tiebreakers on name + tag ensure deterministic output
    {
        GENDB_PHASE("sort_topk");
        size_t k = std::min((size_t)100, all_candidates.size());
        std::partial_sort(all_candidates.begin(),
                          all_candidates.begin() + k,
                          all_candidates.end(),
                          [&](const Candidate& a, const Candidate& b) {
                              if (a.value != b.value) return a.value > b.value;
                              const std::string& na = name_dict[a.name_code];
                              const std::string& nb = name_dict[b.name_code];
                              if (na != nb) return na < nb;
                              return tag_dict[a.tag_code] < tag_dict[b.tag_code];
                          });
        all_candidates.resize(k);
    }

    // ===== OUTPUT =====
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");
        for (const auto& c : all_candidates) {
            // C31: always double-quote string output columns
            fprintf(f, "\"%s\",\"%s\",%.10g\n",
                    name_dict[c.name_code].c_str(),
                    tag_dict[c.tag_code].c_str(),
                    c.value);
        }
        fclose(f);
    }

    // Cleanup
    munmap(global_map, MAX_CAP * sizeof(MaxSlot));
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
