#include <iostream>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <omp.h>

#include "timing_utils.h"

// ============================================================
// Structs
// ============================================================
struct AtvSlot {
    int32_t adsh_code;
    int32_t tag_code;
    int32_t ver_code;
};

struct ZoneBlock {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

struct AggEntry {
    uint64_t key;       // packed (tag_code<<32|ver_code), UINT64_MAX = empty
    int64_t  sum_cents;
    int64_t  count;
};

// ============================================================
// Helpers
// ============================================================
static uint64_t next_pow2(uint64_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1; v |= v >> 2; v |= v >> 4;
    v |= v >> 8; v |= v >> 16; v |= v >> 32;
    return v + 1;
}

static const void* mmap_file_ro(const std::string& path, size_t& out_sz, bool seq_hint = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_sz = st.st_size;
    void* ptr = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    if (seq_hint) {
        posix_fadvise(fd, 0, out_sz, POSIX_FADV_SEQUENTIAL);
        madvise(ptr, out_sz, MADV_SEQUENTIAL);
    } else {
        madvise(ptr, out_sz, MADV_RANDOM);
    }
    close(fd);
    return ptr;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) { fprintf(stderr, "Cannot open dict: %s\n", path.c_str()); exit(1); }
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

// Hash for pre_atv probe — MUST match build_indexes exactly:
// 64-bit multiply then XOR-fold high bits back into low 32.
static inline uint32_t hash3(int32_t a, int32_t b, int32_t c) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761u;
    h ^= (uint64_t)(uint32_t)b * 1234567891u;
    h ^= (uint64_t)(uint32_t)c * 2246822519u;
    return (uint32_t)(h ^ (h >> 32));
}

// Hash for aggregation key (uint64 Fibonacci)
static inline uint64_t agg_hash64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

// Insert into thread-local open-addressing hash map
static inline void agg_upsert(AggEntry* ht, uint64_t mask,
                               uint64_t key, int64_t cents) {
    uint64_t h = agg_hash64(key) & mask;
    for (uint64_t p = 0; p <= mask; p++) {  // C24: bounded
        uint64_t slot = (h + p) & mask;
        AggEntry& e = ht[slot];
        if (e.key == UINT64_MAX) {
            e.key = key;
            e.sum_cents = cents;
            e.count = 1;
            return;
        }
        if (e.key == key) {
            e.sum_cents += cents;
            e.count++;
            return;
        }
    }
    fprintf(stderr, "thread-local agg hash table full!\n"); exit(1);
}

// Merge src -> dst (sequential)
static void agg_merge(AggEntry* dst, uint64_t dst_mask,
                      const AggEntry* src, uint64_t src_cap) {
    for (uint64_t i = 0; i < src_cap; i++) {
        if (src[i].key == UINT64_MAX) continue;
        uint64_t h = agg_hash64(src[i].key) & dst_mask;
        for (uint64_t p = 0; p <= dst_mask; p++) {
            uint64_t slot = (h + p) & dst_mask;
            AggEntry& d = dst[slot];
            if (d.key == UINT64_MAX) { d = src[i]; break; }
            if (d.key == src[i].key) {
                d.sum_cents += src[i].sum_cents;
                d.count     += src[i].count;
                break;
            }
        }
    }
}

// ============================================================
// Query
// ============================================================
void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- data_loading ----
    const int16_t* col_uom   = nullptr;
    const int32_t* col_ddate = nullptr;
    const int32_t* col_adsh  = nullptr;
    const int32_t* col_tag   = nullptr;
    const int32_t* col_ver   = nullptr;
    const double*  col_value = nullptr;
    size_t num_rows = 0;

    const AtvSlot* atv_ht   = nullptr;
    uint32_t       atv_cap  = 0;
    uint32_t       atv_mask = 0;

    const ZoneBlock* zones      = nullptr;
    uint32_t         num_blocks = 0;
    std::vector<uint32_t> block_offsets; // precomputed row start for each block

    std::vector<std::string> uom_dict, tag_dict, ver_dict;
    int16_t usd_code = -1;

    {
        GENDB_PHASE("data_loading");

        // Dictionaries
        uom_dict = load_dict(gendb_dir + "/num/uom_dict.txt");
        tag_dict = load_dict(gendb_dir + "/tag_global_dict.txt");
        ver_dict = load_dict(gendb_dir + "/version_global_dict.txt");

        for (int i = 0; i < (int)uom_dict.size(); i++) {
            if (uom_dict[i] == "USD") { usd_code = (int16_t)i; break; }
        }
        if (usd_code < 0) { fprintf(stderr, "USD not found in uom_dict\n"); exit(1); }

        // num columns
        size_t sz;
        col_uom   = reinterpret_cast<const int16_t*>(mmap_file_ro(gendb_dir + "/num/uom.bin",     sz));
        num_rows  = sz / sizeof(int16_t);
        col_ddate = reinterpret_cast<const int32_t*>(mmap_file_ro(gendb_dir + "/num/ddate.bin",   sz));
        col_adsh  = reinterpret_cast<const int32_t*>(mmap_file_ro(gendb_dir + "/num/adsh.bin",    sz));
        col_tag   = reinterpret_cast<const int32_t*>(mmap_file_ro(gendb_dir + "/num/tag.bin",     sz));
        col_ver   = reinterpret_cast<const int32_t*>(mmap_file_ro(gendb_dir + "/num/version.bin", sz));
        col_value = reinterpret_cast<const double*> (mmap_file_ro(gendb_dir + "/num/value.bin",   sz));

        // Zone map
        {
            size_t zsz;
            const uint8_t* zraw = reinterpret_cast<const uint8_t*>(
                mmap_file_ro(gendb_dir + "/indexes/num_ddate_zone_map.bin", zsz, false));
            num_blocks = *reinterpret_cast<const uint32_t*>(zraw);
            zones = reinterpret_cast<const ZoneBlock*>(zraw + sizeof(uint32_t));
            // Precompute block row offsets (O(n) once)
            block_offsets.resize(num_blocks + 1);
            block_offsets[0] = 0;
            for (uint32_t b = 0; b < num_blocks; b++)
                block_offsets[b + 1] = block_offsets[b] + zones[b].block_size;
        }

        // pre_atv_hash index (random access → MADV_RANDOM)
        {
            size_t asz;
            const uint8_t* araw = reinterpret_cast<const uint8_t*>(
                mmap_file_ro(gendb_dir + "/indexes/pre_atv_hash.bin", asz, false));
            atv_cap  = *reinterpret_cast<const uint32_t*>(araw);
            atv_ht   = reinterpret_cast<const AtvSlot*>(araw + sizeof(uint32_t));
            atv_mask = atv_cap - 1;
        }
    }

    // ---- main_scan: parallel zone-map guided, anti-join, aggregate ----
    const int NTHREADS = omp_get_max_threads();

    // Thread-local aggregation maps
    // C9: size for full key cardinality (not /nthreads). ~200K distinct groups → cap=524288
    const uint64_t TL_CAP  = next_pow2(200000 * 2);  // 524288
    const uint64_t TL_MASK = TL_CAP - 1;

    // Allocate all thread-local maps up front
    std::vector<std::vector<AggEntry>> tl_maps(NTHREADS);
    for (int t = 0; t < NTHREADS; t++) {
        tl_maps[t].assign(TL_CAP, AggEntry{UINT64_MAX, 0, 0});
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(NTHREADS)
        {
            int tid = omp_get_thread_num();
            AggEntry* local_ht = tl_maps[tid].data();

            #pragma omp for schedule(dynamic, 1)
            for (uint32_t b = 0; b < num_blocks; b++) {
                // Zone-map skip (C19: only on ddate column which has zone map)
                if (zones[b].max_val < 20230101 || zones[b].min_val > 20231231) continue;

                uint32_t row_start = block_offsets[b];
                uint32_t row_end   = block_offsets[b + 1];
                if (row_end > (uint32_t)num_rows) row_end = (uint32_t)num_rows;

                for (uint32_t r = row_start; r < row_end; r++) {
                    // Filter: uom = 'USD'
                    if (col_uom[r] != (int16_t)usd_code) continue;

                    // Filter: ddate BETWEEN 20230101 AND 20231231
                    int32_t dd = col_ddate[r];
                    if (dd < 20230101 || dd > 20231231) continue;

                    // Anti-join: probe pre_atv_hash for (adsh, tag, ver)
                    int32_t a = col_adsh[r];
                    int32_t t = col_tag[r];
                    int32_t v = col_ver[r];
                    uint32_t h = hash3(a, t, v) & atv_mask;
                    bool found = false;
                    for (uint32_t p = 0; p < atv_cap; p++) {  // C24: bounded probe
                        uint32_t slot = (h + p) & atv_mask;
                        const AtvSlot& s = atv_ht[slot];
                        if (s.adsh_code == INT32_MIN) break;   // empty slot → not found
                        if (s.adsh_code == a && s.tag_code == t && s.ver_code == v) {
                            found = true; break;
                        }
                    }
                    if (found) continue;  // EXISTS in pre → skip (anti-join)

                    // Aggregate: sum_cents (C29) and count
                    int64_t cents = llround(col_value[r] * 100.0);
                    uint64_t key  = ((uint64_t)(uint32_t)t << 32) | (uint32_t)v;
                    agg_upsert(local_ht, TL_MASK, key, cents);
                }
            }
        }
    }

    // ---- Merge thread-local maps into global map ----
    // Global cap: enough for all distinct groups across all threads
    const uint64_t G_CAP  = next_pow2(400000 * 2);  // 1048576, safe margin (P17/P20)
    const uint64_t G_MASK = G_CAP - 1;
    std::vector<AggEntry> global_map(G_CAP, AggEntry{UINT64_MAX, 0, 0});

    {
        GENDB_PHASE("aggregation_merge");
        for (int tid = 0; tid < NTHREADS; tid++) {
            agg_merge(global_map.data(), G_MASK,
                      tl_maps[tid].data(), TL_CAP);
        }
    }

    // ---- HAVING count > 10, sort by cnt DESC, LIMIT 100 ----
    std::vector<const AggEntry*> results;
    {
        GENDB_PHASE("sort_topk");
        results.reserve(8192);
        for (const auto& e : global_map) {
            if (e.key == UINT64_MAX) continue;
            if (e.count > 10) results.push_back(&e);
        }
        size_t top_k = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + top_k, results.end(),
            [](const AggEntry* a, const AggEntry* b) {
                return a->count > b->count;
            });
        results.resize(top_k);
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q24.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); exit(1); }

        fprintf(out, "tag,version,cnt,total\n");
        for (const auto* ep : results) {
            int32_t tag_code = (int32_t)(ep->key >> 32);
            int32_t ver_code = (int32_t)(ep->key & 0xFFFFFFFFULL);

            const std::string& tag_str = tag_dict[tag_code];
            const std::string& ver_str = ver_dict[ver_code];

            // C29: format int64_t cents as decimal
            int64_t sc = ep->sum_cents;
            int64_t abs_sc = (sc < 0) ? -sc : sc;
            int64_t int_part  = abs_sc / 100;
            int64_t frac_part = abs_sc % 100;
            if (sc < 0)
                fprintf(out, "%s,%s,%lld,-%lld.%02lld\n",
                        tag_str.c_str(), ver_str.c_str(),
                        (long long)ep->count,
                        (long long)int_part, (long long)frac_part);
            else
                fprintf(out, "%s,%s,%lld,%lld.%02lld\n",
                        tag_str.c_str(), ver_str.c_str(),
                        (long long)ep->count,
                        (long long)int_part, (long long)frac_part);
        }
        fclose(out);
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
