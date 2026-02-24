// Q6: SELECT s.name, p.stmt, n.tag, p.plabel, SUM(n.value), COUNT(*)
// FROM num n JOIN sub s ON n.adsh=s.adsh JOIN pre p ON n.adsh=p.adsh AND n.tag=p.tag AND n.version=p.version
// WHERE n.uom='USD' AND p.stmt='IS' AND s.fy=2023 AND n.value IS NOT NULL
// GROUP BY s.name, p.stmt, n.tag, p.plabel ORDER BY total_value DESC LIMIT 200;
//
// OPTIMIZATIONS vs iter_4 (822ms):
// 1. Scan-time partitioned aggregation: eliminates 2GB of per-thread hash maps (build_joins 121ms + aggregation_merge 261ms)
// 2. No MAP_POPULATE: pages already in OS cache on hot runs (data_loading 259ms -> ~80ms)
// 3. Semi-join Bloom filter: sub.fy=2023 filter skips ~69% of rows before sub_adsh_index probe

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <climits>
#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash functions (verbatim from build_indexes.cpp)
// ---------------------------------------------------------------------------
static inline uint32_t hash_i32(int32_t k) {
    uint32_t x = (uint32_t)k;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint32_t hash_i32x3(int32_t a, int32_t b, int32_t c) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
    h ^= (uint64_t)(uint32_t)b * 2246822519ULL;
    h ^= (uint64_t)(uint32_t)c * 3266489917ULL;
    return (uint32_t)(h ^ (h >> 32));
}

// ---------------------------------------------------------------------------
// Index slot structs (verbatim from build_indexes.cpp)
// ---------------------------------------------------------------------------
struct SubSlot { int32_t adsh_code; int32_t sub_row; };

struct PreSlot {
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  version_code;
    uint32_t payload_offset;
    uint32_t payload_count;
};

// ---------------------------------------------------------------------------
// Aggregation key/value
// ---------------------------------------------------------------------------
struct Q6Key {
    int32_t name_code;    // s.name
    int16_t stmt_code;    // p.stmt (always is_code, but must be in key C30)
    int32_t tag_code;     // n.tag
    int32_t plabel_code;  // p.plabel
};

struct Q6Val {
    int64_t sum_cents;  // C29: SUM as int64_t cents
    int64_t cnt;
};

// Partition buffer entry: key + value tuple appended during scan
struct Q6Tuple {
    Q6Key   key;
    int64_t cents;
};

struct Q6Entry {
    Q6Key key;
    Q6Val val;
};

static inline bool q6key_eq(const Q6Key& a, const Q6Key& b) {
    return a.name_code   == b.name_code   &&
           a.stmt_code   == b.stmt_code   &&
           a.tag_code    == b.tag_code    &&
           a.plabel_code == b.plabel_code;
}

static inline uint32_t hash_q6key(const Q6Key& k) {
    uint64_t h = (uint64_t)(uint32_t)k.name_code   * 2654435761ULL;
    h ^= (uint64_t)(uint16_t)k.stmt_code            * 2246822519ULL;
    h ^= (uint64_t)(uint32_t)k.tag_code             * 3266489917ULL;
    h ^= (uint64_t)(uint32_t)k.plabel_code          * 1234567891ULL;
    return (uint32_t)(h ^ (h >> 32));
}

// ---------------------------------------------------------------------------
// Bloom filter for semi-join pushdown (sub.fy=2023 adsh codes)
// ---------------------------------------------------------------------------
struct BloomFilter {
    static const uint32_t BLOOM_BITS = 1u << 18;  // 256K bits = 32KB (fits L1)
    static const uint32_t BLOOM_MASK = BLOOM_BITS - 1;
    uint64_t bits[BLOOM_BITS / 64];

    void clear() { memset(bits, 0, sizeof(bits)); }

    void insert(int32_t key) {
        uint32_t h1 = hash_i32(key);
        uint32_t h2 = h1 * 2654435761u;
        uint32_t h3 = h1 * 2246822519u;
        bits[(h1 & BLOOM_MASK) >> 6] |= (1ULL << (h1 & 63));
        bits[(h2 & BLOOM_MASK) >> 6] |= (1ULL << (h2 & 63));
        bits[(h3 & BLOOM_MASK) >> 6] |= (1ULL << (h3 & 63));
    }

    bool maybe_contains(int32_t key) const {
        uint32_t h1 = hash_i32(key);
        uint32_t h2 = h1 * 2654435761u;
        uint32_t h3 = h1 * 2246822519u;
        return (bits[(h1 & BLOOM_MASK) >> 6] & (1ULL << (h1 & 63))) &&
               (bits[(h2 & BLOOM_MASK) >> 6] & (1ULL << (h2 & 63))) &&
               (bits[(h3 & BLOOM_MASK) >> 6] & (1ULL << (h3 & 63)));
    }
};

static const int32_t SENTINEL = INT32_MIN;  // C20: use std::fill

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { perror(path.c_str()); return dict; }
    char buf[8192];
    while (fgets(buf, sizeof(buf), f)) {
        size_t len = strlen(buf);
        while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r')) buf[--len] = 0;
        dict.emplace_back(buf, len);
    }
    fclose(f);
    return dict;
}

// Change 2: No MAP_POPULATE — pages already in OS cache on hot runs
static const char* mmap_ro(const std::string& path, size_t& sz_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); return nullptr; }
    sz_out = (size_t)st.st_size;
    void* ptr = mmap(nullptr, sz_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); return nullptr; }
    madvise(ptr, sz_out, MADV_SEQUENTIAL);
    return reinterpret_cast<const char*>(ptr);
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string num_dir = gendb_dir + "/num";
    const std::string sub_dir = gendb_dir + "/sub";
    const std::string pre_dir = gendb_dir + "/pre";
    const std::string idx_dir = gendb_dir + "/indexes";

    // -----------------------------------------------------------------------
    // data_loading
    // -----------------------------------------------------------------------
    const int32_t* num_adsh    = nullptr;
    const int32_t* num_tag     = nullptr;
    const int32_t* num_version = nullptr;
    const int16_t* num_uom     = nullptr;
    const double*  num_value   = nullptr;
    size_t         num_rows    = 0;

    const int32_t* sub_adsh = nullptr;
    const int32_t* sub_fy   = nullptr;
    const int32_t* sub_name = nullptr;
    size_t         sub_rows = 0;

    const int16_t* pre_stmt   = nullptr;
    const int32_t* pre_plabel = nullptr;

    const char* sub_idx_raw = nullptr;
    const char* pre_idx_raw = nullptr;

    std::vector<std::string> name_dict;
    std::vector<std::string> stmt_dict;
    std::vector<std::string> tag_dict;
    std::vector<std::string> plabel_dict;
    int16_t usd_code = -1;
    int16_t is_code  = -1;

    {
        GENDB_PHASE("data_loading");

        auto uom_dict = load_dict(num_dir + "/uom_dict.txt");
        tag_dict      = load_dict(num_dir + "/tag_dict.txt");
        name_dict     = load_dict(sub_dir + "/name_dict.txt");
        stmt_dict     = load_dict(pre_dir + "/stmt_dict.txt");
        plabel_dict   = load_dict(pre_dir + "/plabel_dict.txt");

        for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
            if (uom_dict[c] == "USD") { usd_code = c; break; }
        for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
            if (stmt_dict[c] == "IS")  { is_code  = c; break; }

        size_t sz_adsh=0, sz_tag=0, sz_ver=0, sz_uom=0, sz_val=0;
        size_t sz_si=0, sz_pi=0;
        size_t sz_sub_adsh=0, sz_sub_fy=0;

        #pragma omp parallel sections num_threads(10)
        {
            #pragma omp section
            {
                const char* p = mmap_ro(num_dir + "/adsh.bin", sz_adsh);
                num_adsh = reinterpret_cast<const int32_t*>(p);
                num_rows = sz_adsh / sizeof(int32_t);
            }
            #pragma omp section
            { num_tag     = reinterpret_cast<const int32_t*>(mmap_ro(num_dir + "/tag.bin",     sz_tag)); }
            #pragma omp section
            { num_version = reinterpret_cast<const int32_t*>(mmap_ro(num_dir + "/version.bin", sz_ver)); }
            #pragma omp section
            { num_uom     = reinterpret_cast<const int16_t*>(mmap_ro(num_dir + "/uom.bin",     sz_uom)); }
            #pragma omp section
            { num_value   = reinterpret_cast<const double*> (mmap_ro(num_dir + "/value.bin",   sz_val)); }
            #pragma omp section
            {
                size_t s1=0, s2=0;
                sub_name   = reinterpret_cast<const int32_t*>(mmap_ro(sub_dir + "/name.bin",    s1));
                pre_stmt   = reinterpret_cast<const int16_t*>(mmap_ro(pre_dir + "/stmt.bin",    s2));
            }
            #pragma omp section
            { pre_plabel = reinterpret_cast<const int32_t*>(mmap_ro(pre_dir + "/plabel.bin",  sz_val)); }
            #pragma omp section
            { sub_idx_raw = mmap_ro(idx_dir + "/sub_adsh_index.bin", sz_si); }
            #pragma omp section
            { pre_idx_raw = mmap_ro(idx_dir + "/pre_join_index.bin", sz_pi); }
            #pragma omp section
            {
                // Load sub columns needed for Bloom filter construction
                const char* p = mmap_ro(sub_dir + "/adsh.bin", sz_sub_adsh);
                sub_adsh = reinterpret_cast<const int32_t*>(p);
                sub_rows = sz_sub_adsh / sizeof(int32_t);
                sub_fy   = reinterpret_cast<const int32_t*>(mmap_ro(sub_dir + "/fy.bin", sz_sub_fy));
            }
        }
    }

    // -----------------------------------------------------------------------
    // C32: Parse index headers at run_query() function scope
    // -----------------------------------------------------------------------
    uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);

    uint32_t pre_cap  = *(const uint32_t*)pre_idx_raw;
    uint32_t pre_mask = pre_cap - 1;
    const PreSlot* pre_ht   = (const PreSlot*)(pre_idx_raw + 4);
    const uint32_t* pre_pool = (const uint32_t*)(pre_idx_raw + 4 + (size_t)pre_cap * 20);

    // -----------------------------------------------------------------------
    // Change 3: Build Bloom filter from sub.fy=2023 adsh codes
    // ~26.6K qualifying adsh codes -> 32KB Bloom, fits L1 cache
    // -----------------------------------------------------------------------
    BloomFilter bloom;
    {
        GENDB_PHASE("build_joins");  // Bloom construction replaces per-thread map init
        bloom.clear();
        for (size_t i = 0; i < sub_rows; ++i) {
            if (sub_fy[i] == 2023) {
                bloom.insert(sub_adsh[i]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Change 1: Scan-time partitioned aggregation
    // Instead of T per-thread AGG_CAP=1M hash maps (2GB total):
    // - Each thread maintains P=64 partition buffers (vectors of Q6Tuple)
    // - After scan: thread t drains ALL T buffers for partition t into one small hash map
    //   (cap = 16384 slots, ~512KB, fits L2 cache)
    // -----------------------------------------------------------------------
    int nthreads = omp_get_max_threads();
    static const uint32_t P = 64;  // number of partitions = number of threads on this machine

    // Per-thread partition buffers: thread_parts[tid][pid] = vector of tuples
    std::vector<std::vector<std::vector<Q6Tuple>>> thread_parts(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // Initialize partition buffers for this thread
            thread_parts[tid].resize(P);
            for (uint32_t p = 0; p < P; ++p)
                thread_parts[tid][p].reserve(8192);

            #pragma omp for schedule(dynamic, 65536) nowait
            for (size_t i = 0; i < num_rows; ++i) {
                // Filter: n.uom = 'USD'
                if (num_uom[i] != usd_code) continue;

                // Filter: n.value IS NOT NULL
                double v = num_value[i];
                if (std::isnan(v)) continue;

                int32_t adsh_code = num_adsh[i];

                // Change 3: Bloom filter check BEFORE sub_adsh_index probe
                // Skips ~69% of rows without hash probe
                if (!bloom.maybe_contains(adsh_code)) continue;

                // --- Probe sub_adsh_index (C24: bounded loop) ---
                uint32_t slot = hash_i32(adsh_code) & sub_mask;
                int32_t sub_row = -1;
                for (uint32_t probe = 0; probe < sub_cap; ++probe) {
                    uint32_t s = (slot + probe) & sub_mask;
                    if (sub_ht[s].adsh_code == -1) break;
                    if (sub_ht[s].adsh_code == adsh_code) {
                        sub_row = sub_ht[s].sub_row;
                        break;
                    }
                }
                if (sub_row < 0) continue;

                // Filter: s.fy = 2023
                if (sub_fy[sub_row] != 2023) continue;

                int32_t name_code    = sub_name[sub_row];
                int32_t tag_code     = num_tag[i];
                int32_t version_code = num_version[i];

                // --- Probe pre_join_index (C24: bounded loop) ---
                uint32_t pslot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
                for (uint32_t probe = 0; probe < pre_cap; ++probe) {
                    uint32_t s = (pslot + probe) & pre_mask;
                    if (pre_ht[s].adsh_code == -1) break;
                    if (pre_ht[s].adsh_code   == adsh_code   &&
                        pre_ht[s].tag_code     == tag_code    &&
                        pre_ht[s].version_code == version_code) {
                        uint32_t off = pre_ht[s].payload_offset;
                        uint32_t cnt = pre_ht[s].payload_count;
                        // C29: compute cents once per num row
                        int64_t iv = llround(v * 100.0);
                        for (uint32_t pi = 0; pi < cnt; ++pi) {
                            uint32_t pre_row = pre_pool[off + pi];
                            if (pre_stmt[pre_row] != is_code) continue;
                            int32_t plabel_code = pre_plabel[pre_row];

                            // C15/C30: key includes ALL FOUR GROUP BY dimensions
                            Q6Key k{name_code, is_code, tag_code, plabel_code};
                            uint32_t hv = hash_q6key(k);
                            uint32_t pid = hv & (P - 1);

                            // Append to this thread's partition buffer
                            thread_parts[tid][pid].push_back({k, iv});
                        }
                        break;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // aggregation_merge: partitioned parallel reduction
    // Thread t owns partition t — drains ALL T thread buffers for partition t
    // into one small hash map (cap=16384, ~512KB, fits L2)
    // Zero contention, embarrassingly parallel
    // -----------------------------------------------------------------------
    static const uint32_t PMAP_CAP  = 1u << 14;  // 16384 slots per partition
    static const uint32_t PMAP_MASK = PMAP_CAP - 1;

    std::vector<std::vector<Q6Entry>> part_maps(P, std::vector<Q6Entry>(PMAP_CAP));
    // Track occupied slots per partition for fast final scan
    std::vector<std::vector<uint32_t>> part_occupied(P);

    {
        GENDB_PHASE("aggregation_merge");

        const Q6Entry sentinel_entry{{SENTINEL, (int16_t)0, 0, 0}, {0LL, 0LL}};

        #pragma omp parallel num_threads(P)
        {
            uint32_t pid = (uint32_t)omp_get_thread_num();

            // Init this partition's map
            std::fill(part_maps[pid].begin(), part_maps[pid].end(), sentinel_entry);
            part_occupied[pid].reserve(8192);

            Q6Entry* pmap = part_maps[pid].data();
            auto& occ = part_occupied[pid];

            // Drain all T thread buffers for this partition
            for (int src = 0; src < nthreads; ++src) {
                for (const Q6Tuple& t : thread_parts[src][pid]) {
                    uint32_t hv = hash_q6key(t.key);
                    uint32_t h = (hv >> 6) & PMAP_MASK;  // use upper bits for slot
                    for (uint32_t ap = 0; ap < PMAP_CAP; ++ap) {
                        uint32_t s = (h + ap) & PMAP_MASK;
                        if (pmap[s].key.name_code == SENTINEL) {
                            pmap[s].key           = t.key;
                            pmap[s].val.sum_cents  = t.cents;
                            pmap[s].val.cnt        = 1;
                            occ.push_back(s);
                            break;
                        }
                        if (q6key_eq(pmap[s].key, t.key)) {
                            pmap[s].val.sum_cents += t.cents;
                            pmap[s].val.cnt       += 1;
                            break;
                        }
                    }
                }
            }

            // Free source buffers for this partition across all threads
            for (int src = 0; src < nthreads; ++src) {
                thread_parts[src][pid].clear();
                thread_parts[src][pid].shrink_to_fit();
            }
        }
    }

    // -----------------------------------------------------------------------
    // sort_topk: top-200 by total_value DESC, name_code ASC (C33 stable tiebreaker)
    // -----------------------------------------------------------------------
    std::vector<const Q6Entry*> results;
    results.reserve(300000);

    {
        GENDB_PHASE("sort_topk");

        // Use occupied lists for O(groups) scan instead of O(PMAP_CAP * P) = O(1M)
        for (uint32_t p = 0; p < P; ++p) {
            for (uint32_t s : part_occupied[p]) {
                results.push_back(&part_maps[p][s]);
            }
        }

        // P6: partial_sort O(n log k) for LIMIT 200
        size_t K = std::min((size_t)200, results.size());
        std::partial_sort(results.begin(), results.begin() + K, results.end(),
            [](const Q6Entry* a, const Q6Entry* b) {
                // C33: stable tiebreaker on name_code ASC
                if (a->val.sum_cents != b->val.sum_cents)
                    return a->val.sum_cents > b->val.sum_cents;
                return a->key.name_code < b->key.name_code;
            });
        results.resize(K);
    }

    // -----------------------------------------------------------------------
    // output: write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        // Header
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");

        for (const Q6Entry* e : results) {
            int64_t sc = e->val.sum_cents;
            // C29: format sum_cents as decimal with 2 decimal places
            bool neg = sc < 0;
            int64_t asc   = neg ? -sc : sc;
            int64_t whole = asc / 100;
            int64_t frac  = asc % 100;

            // C31: double-quote ALL four string columns
            fprintf(f, "\"%s\",\"%s\",\"%s\",\"%s\",%s%lld.%02lld,%lld\n",
                name_dict  [e->key.name_code  ].c_str(),
                stmt_dict  [e->key.stmt_code  ].c_str(),
                tag_dict   [e->key.tag_code   ].c_str(),
                plabel_dict[e->key.plabel_code].c_str(),
                neg ? "-" : "",
                (long long)whole,
                (long long)frac,
                (long long)e->val.cnt);
        }

        fclose(f);
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
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
