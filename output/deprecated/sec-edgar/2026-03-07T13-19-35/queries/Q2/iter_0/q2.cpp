// Q2: Two-pass strategy over num (uom='pure'), top-100 by value DESC
// Pass 1: build hash map of MAX(value) keyed by (adsh_code, tagver_code)
// Pass 2: filter by fy=2022 + exact max-value match, collect top-100
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <cassert>
#include <vector>
#include <string>
#include <algorithm>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ---- Constants ----
static constexpr size_t  BLOCK_SIZE = 100000;
static constexpr uint32_t MAP_CAP   = 262144;  // 2^18, ~36% load for 94564 entries
static constexpr uint32_t MAP_MASK  = MAP_CAP - 1;

// ---- Zone map struct (matches binary layout: 12 bytes) ----
// Offsets: min_uom@0, max_uom@1, pad@2-3, min_ddate@4, max_ddate@8
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int8_t  _pad0;
    int8_t  _pad1;
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// ---- Hash map slot: open-addressing linear probing ----
struct MaxSlot {
    uint64_t key;    // UINT64_MAX = empty sentinel
    double   value;  // stored max value
};
static_assert(sizeof(MaxSlot) == 16, "MaxSlot must be 16 bytes");

// ---- Result candidate ----
struct Candidate {
    double   value;
    int32_t  adsh_code;
    int32_t  tagver_code;
};

// ---- Hash function: 64-bit Murmur finalizer ----
static inline uint32_t hash64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return (uint32_t)k;
}

static inline uint64_t pack_key(int32_t adsh, int32_t tagver) {
    return ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tagver;
}

// Insert or update max value for key
static inline void map_update(MaxSlot* __restrict__ ht, uint64_t key, double val) {
    uint32_t pos = hash64(key) & MAP_MASK;
    while (true) {
        if (__builtin_expect(ht[pos].key == UINT64_MAX, 0)) {
            ht[pos].key   = key;
            ht[pos].value = val;
            return;
        }
        if (ht[pos].key == key) {
            if (val > ht[pos].value) ht[pos].value = val;
            return;
        }
        pos = (pos + 1) & MAP_MASK;
    }
}

// Merge src into dst (dst must be large enough)
static void map_merge(MaxSlot* __restrict__ dst, const MaxSlot* __restrict__ src) {
    for (uint32_t i = 0; i < MAP_CAP; i++) {
        if (src[i].key == UINT64_MAX) continue;
        map_update(dst, src[i].key, src[i].value);
    }
}

// Lookup: returns stored max or -inf if not found
static inline double map_lookup(const MaxSlot* __restrict__ ht, uint64_t key) {
    uint32_t pos = hash64(key) & MAP_MASK;
    while (true) {
        if (ht[pos].key == UINT64_MAX) return -std::numeric_limits<double>::infinity();
        if (ht[pos].key == key) return ht[pos].value;
        pos = (pos + 1) & MAP_MASK;
    }
}

// ---- Utility: mmap a file ----
static void* mmap_file(const std::string& path, size_t& sz_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); sz_out = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    sz_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror(path.c_str()); sz_out = 0; return nullptr; }
    return p;
}

// ---- Load pure_code from indexes/uom_codes.bin ----
// Format: uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }
static int8_t load_pure_code(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { perror(path.c_str()); return -99; }
    uint8_t N = 0;
    fread(&N, 1, 1, f);
    int8_t result = -99;
    char buf[256];
    for (uint8_t i = 0; i < N; i++) {
        int8_t  code = 0;
        uint8_t slen = 0;
        fread(&code, 1, 1, f);
        fread(&slen, 1, 1, f);
        fread(buf, 1, slen, f);
        if (slen == 4 && memcmp(buf, "pure", 4) == 0) {
            result = code;
        }
    }
    fclose(f);
    return result;
}

// ---- Main query function ----
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- DATA LOADING ----
    int8_t   pure_code = -99;
    uint32_t n_blocks  = 0;
    std::vector<ZoneMap> zmaps;

    size_t uom_sz, adsh_sz, tagver_sz, value_sz;
    size_t fy_sz, name_off_sz, name_data_sz, tag_off_sz, tag_data_sz;

    const int8_t*   uom_col      = nullptr;
    const int32_t*  adsh_col     = nullptr;
    const int32_t*  tagver_col   = nullptr;
    const double*   value_col    = nullptr;
    const int16_t*  fy_col       = nullptr;
    const uint32_t* name_offsets = nullptr;
    const char*     name_data    = nullptr;
    const uint32_t* tag_offsets  = nullptr;
    const char*     tag_data     = nullptr;

    size_t num_rows = 0;

    {
        GENDB_PHASE("data_loading");

        // Resolve 'pure' string to int8_t code
        pure_code = load_pure_code(gendb_dir + "/indexes/uom_codes.bin");

        // Load zone maps
        {
            FILE* f = fopen((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), "rb");
            if (!f) { perror("num_zone_maps.bin"); return; }
            fread(&n_blocks, 4, 1, f);
            zmaps.resize(n_blocks);
            fread(zmaps.data(), sizeof(ZoneMap), n_blocks, f);
            fclose(f);
        }

        // mmap num columns
        uom_col    = (const int8_t*) mmap_file(gendb_dir + "/num/uom_code.bin",    uom_sz);
        adsh_col   = (const int32_t*)mmap_file(gendb_dir + "/num/adsh_code.bin",   adsh_sz);
        tagver_col = (const int32_t*)mmap_file(gendb_dir + "/num/tagver_code.bin", tagver_sz);
        value_col  = (const double*) mmap_file(gendb_dir + "/num/value.bin",        value_sz);

        num_rows = uom_sz / sizeof(int8_t);

        // mmap sub fy (int16_t direct array)
        fy_col = (const int16_t*)mmap_file(gendb_dir + "/sub/fy.bin", fy_sz);

        // mmap string decode tables (accessed only for final 100 rows)
        name_offsets = (const uint32_t*)mmap_file(gendb_dir + "/sub/name_offsets.bin", name_off_sz);
        name_data    = (const char*)    mmap_file(gendb_dir + "/sub/name_data.bin",    name_data_sz);
        tag_offsets  = (const uint32_t*)mmap_file(gendb_dir + "/tag/tag_offsets.bin",  tag_off_sz);
        tag_data     = (const char*)    mmap_file(gendb_dir + "/tag/tag_data.bin",     tag_data_sz);

        // Advise kernel on access patterns
        madvise((void*)uom_col,    uom_sz,    MADV_SEQUENTIAL);
        madvise((void*)adsh_col,   adsh_sz,   MADV_SEQUENTIAL);
        madvise((void*)tagver_col, tagver_sz, MADV_SEQUENTIAL);
        madvise((void*)value_col,  value_sz,  MADV_SEQUENTIAL);
        madvise((void*)fy_col,     fy_sz,     MADV_WILLNEED);
        madvise((void*)name_offsets, name_off_sz,  MADV_WILLNEED);
        madvise((void*)name_data,    name_data_sz, MADV_WILLNEED);
        madvise((void*)tag_offsets,  tag_off_sz,   MADV_WILLNEED);
        madvise((void*)tag_data,     tag_data_sz,  MADV_WILLNEED);
    }

    // ---- Precompute valid blocks via zone maps ----
    // num is sorted by (uom_code, ddate), so 'pure' rows are contiguous → ~1 block range
    std::vector<uint32_t> valid_blocks;
    valid_blocks.reserve(8);
    for (uint32_t b = 0; b < n_blocks; b++) {
        if (zmaps[b].min_uom > pure_code || zmaps[b].max_uom < pure_code) continue;
        valid_blocks.push_back(b);
    }

    int nthreads = omp_get_max_threads();

    // ---- Allocate thread-local hash maps ----
    std::vector<MaxSlot*> thread_maps(nthreads, nullptr);
    for (int t = 0; t < nthreads; t++) {
        thread_maps[t] = (MaxSlot*)mmap(nullptr, MAP_CAP * sizeof(MaxSlot),
                                        PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    // Initialize each thread's map from within that thread (NUMA-local page faults)
    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        MaxSlot* ht = thread_maps[tid];
        for (uint32_t i = 0; i < MAP_CAP; i++) {
            ht[i].key   = UINT64_MAX;
            ht[i].value = -std::numeric_limits<double>::infinity();
        }
    }

    // ---- PASS 1: build thread-local MAX aggregation maps ----
    {
        GENDB_PHASE("build_joins");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            MaxSlot* local_ht = thread_maps[tid];

            #pragma omp for schedule(dynamic)
            for (size_t bi = 0; bi < valid_blocks.size(); bi++) {
                uint32_t b = valid_blocks[bi];
                size_t row_start = (size_t)b * BLOCK_SIZE;
                size_t row_end   = std::min(row_start + BLOCK_SIZE, num_rows);

                for (size_t i = row_start; i < row_end; i++) {
                    if (uom_col[i] != pure_code) continue;
                    int32_t tagver = tagver_col[i];
                    if (tagver == -1) continue;
                    int32_t adsh = adsh_col[i];
                    double  val  = value_col[i];
                    map_update(local_ht, pack_key(adsh, tagver), val);
                }
            }
        }
    }

    // ---- Merge thread-local maps into global map ----
    MaxSlot* global_map = (MaxSlot*)mmap(nullptr, MAP_CAP * sizeof(MaxSlot),
                                         PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // Initialize global map
    for (uint32_t i = 0; i < MAP_CAP; i++) {
        global_map[i].key   = UINT64_MAX;
        global_map[i].value = -std::numeric_limits<double>::infinity();
    }

    for (int t = 0; t < nthreads; t++) {
        map_merge(global_map, thread_maps[t]);
        munmap(thread_maps[t], MAP_CAP * sizeof(MaxSlot));
        thread_maps[t] = nullptr;
    }

    // ---- PASS 2: filter + join + collect candidates ----
    // Expected: ~94564 → ~18913 (fy=2022) → ~500 (max-val match)
    std::vector<std::vector<Candidate>> thread_cands(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local_cands = thread_cands[tid];

            #pragma omp for schedule(dynamic)
            for (size_t bi = 0; bi < valid_blocks.size(); bi++) {
                uint32_t b = valid_blocks[bi];
                size_t row_start = (size_t)b * BLOCK_SIZE;
                size_t row_end   = std::min(row_start + BLOCK_SIZE, num_rows);

                for (size_t i = row_start; i < row_end; i++) {
                    // Filter 1: uom = 'pure' (zone-map skipped most blocks already)
                    if (uom_col[i] != pure_code) continue;

                    // Filter 2: tagver_code != -1
                    int32_t tagver = tagver_col[i];
                    if (tagver == -1) continue;

                    int32_t adsh = adsh_col[i];

                    // Filter 3: sub.fy == 2022 (O(1) direct array, eliminates ~80%)
                    if (fy_col[adsh] != 2022) continue;

                    // Filter 4: value == MAX(value) for this (adsh, tagver) group
                    double val = value_col[i];
                    uint64_t key = pack_key(adsh, tagver);
                    double max_val = map_lookup(global_map, key);
                    if (val != max_val) continue;

                    local_cands.push_back({val, adsh, tagver});
                }
            }
        }
    }

    // ---- Merge thread-local candidate lists ----
    std::vector<Candidate> all_cands;
    all_cands.reserve(1024);
    for (int t = 0; t < nthreads; t++) {
        all_cands.insert(all_cands.end(),
                         thread_cands[t].begin(), thread_cands[t].end());
    }

    // ---- String decode + sort + output ----
    {
        GENDB_PHASE("output");

        // Decode strings for all candidates (only ~500 rows)
        struct ResultRow {
            double      value;
            std::string name;
            std::string tag;
        };

        std::vector<ResultRow> rows;
        rows.reserve(all_cands.size());
        for (const auto& c : all_cands) {
            // Decode name: sub/name_offsets[adsh_code] .. sub/name_offsets[adsh_code+1]
            uint32_t n0 = name_offsets[c.adsh_code];
            uint32_t n1 = name_offsets[c.adsh_code + 1];
            std::string name(name_data + n0, n1 - n0);

            // Decode tag: tag/tag_offsets[tagver_code] .. tag/tag_offsets[tagver_code+1]
            uint32_t t0 = tag_offsets[(uint32_t)c.tagver_code];
            uint32_t t1 = tag_offsets[(uint32_t)c.tagver_code + 1];
            std::string tag(tag_data + t0, t1 - t0);

            rows.push_back({c.value, std::move(name), std::move(tag)});
        }

        // Sort by (value DESC, name ASC, tag ASC)
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.value != b.value) return a.value > b.value;
            if (a.name  != b.name)  return a.name  < b.name;
            return a.tag < b.tag;
        });

        // Keep top 100
        if (rows.size() > 100) rows.resize(100);

        // Write CSV output
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");
        for (const auto& r : rows) {
            fprintf(f, "\"%s\",\"%s\",%.10g\n",
                    r.name.c_str(), r.tag.c_str(), r.value);
        }
        fclose(f);
    }

    // Cleanup
    munmap(global_map, MAP_CAP * sizeof(MaxSlot));
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    run_q2(argv[1], argv[2]);
    return 0;
}
#endif
