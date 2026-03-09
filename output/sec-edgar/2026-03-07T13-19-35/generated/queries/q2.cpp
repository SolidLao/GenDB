// Q2 iter_1: Single-threaded single-pass strategy
// - One scan over ~1 valid block (~100K rows, ~1.7MB data)
// - During scan: build MAX map AND collect candidates, both restricted to fy=2022
// - Eliminates 64-thread init overhead (was ~80ms of 90ms total in iter_0)
// - 65536-slot hash map (1MB) on heap → zero page-fault overhead
// - Post-scan: filter candidates where value==max, sort, decode top-100
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <cassert>
#include <vector>
#include <string>
#include <algorithm>
#include <limits>
#include <new>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

namespace {

// ---- Constants ----
static constexpr size_t   BLOCK_SIZE = 100000;
static constexpr uint32_t MAP_CAP    = 65536;   // 2^16, ~29% load for ~18913 entries
static constexpr uint32_t MAP_MASK   = MAP_CAP - 1;
static constexpr uint64_t EMPTY_KEY  = UINT64_MAX;

// ---- Zone map struct (matches binary layout: 12 bytes) ----
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int8_t  _pad0;
    int8_t  _pad1;
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// ---- Hash map slot: open-addressing linear probing, 16B ----
struct alignas(16) MaxSlot {
    uint64_t key;   // EMPTY_KEY = empty sentinel
    double   value; // stored max
};
static_assert(sizeof(MaxSlot) == 16, "MaxSlot must be 16 bytes");

// ---- Result candidate ----
struct Candidate {
    double  value;
    int32_t adsh_code;
    int32_t tagver_code;
};

// ---- Hash: 64-bit Murmur finalizer ----
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

// Insert or update max value
static inline void map_upsert(MaxSlot* __restrict__ ht, uint64_t key, double val) {
    uint32_t pos = hash64(key) & MAP_MASK;
    while (true) {
        if (__builtin_expect(ht[pos].key == EMPTY_KEY, 0)) {
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

// Lookup: returns stored max or -inf if not found
static inline double map_get(const MaxSlot* __restrict__ ht, uint64_t key) {
    uint32_t pos = hash64(key) & MAP_MASK;
    while (true) {
        if (ht[pos].key == EMPTY_KEY) return -std::numeric_limits<double>::infinity();
        if (ht[pos].key == key)       return ht[pos].value;
        pos = (pos + 1) & MAP_MASK;
    }
}

// ---- Utility: mmap a file read-only ----
static const void* mmap_ro(const std::string& path, size_t& sz_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); sz_out = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    sz_out = (size_t)st.st_size;
    if (sz_out == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, sz_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror(path.c_str()); sz_out = 0; return nullptr; }
    return p;
}

// ---- Load pure_code from indexes/uom_codes.bin ----
// Layout: uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }
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
        if (slen == 4 && memcmp(buf, "pure", 4) == 0) result = code;
    }
    fclose(f);
    return result;
}

// ---- Main query function ----
} // end anonymous namespace

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

        pure_code = load_pure_code(gendb_dir + "/indexes/uom_codes.bin");

        // Load zone maps
        {
            FILE* fz = fopen((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), "rb");
            if (!fz) { perror("num_zone_maps.bin"); return; }
            fread(&n_blocks, 4, 1, fz);
            zmaps.resize(n_blocks);
            fread(zmaps.data(), sizeof(ZoneMap), n_blocks, fz);
            fclose(fz);
        }

        // mmap num columns (sequential scan — only ~1 valid block will be read)
        uom_col    = (const int8_t*)  mmap_ro(gendb_dir + "/num/uom_code.bin",    uom_sz);
        adsh_col   = (const int32_t*) mmap_ro(gendb_dir + "/num/adsh_code.bin",   adsh_sz);
        tagver_col = (const int32_t*) mmap_ro(gendb_dir + "/num/tagver_code.bin", tagver_sz);
        value_col  = (const double*)  mmap_ro(gendb_dir + "/num/value.bin",       value_sz);
        num_rows   = uom_sz / sizeof(int8_t);

        // mmap sub fy array (172KB — fits in L2; direct index by adsh_code)
        fy_col = (const int16_t*) mmap_ro(gendb_dir + "/sub/fy.bin", fy_sz);

        // mmap string decode tables (deferred — accessed for final 100 rows only)
        name_offsets = (const uint32_t*) mmap_ro(gendb_dir + "/sub/name_offsets.bin", name_off_sz);
        name_data    = (const char*)     mmap_ro(gendb_dir + "/sub/name_data.bin",    name_data_sz);
        tag_offsets  = (const uint32_t*) mmap_ro(gendb_dir + "/tag/tag_offsets.bin",  tag_off_sz);
        tag_data     = (const char*)     mmap_ro(gendb_dir + "/tag/tag_data.bin",     tag_data_sz);

        // Sequential hints for scan columns (OS prefetcher already handles this well)
        madvise((void*)uom_col,    uom_sz,    MADV_SEQUENTIAL);
        madvise((void*)adsh_col,   adsh_sz,   MADV_SEQUENTIAL);
        madvise((void*)tagver_col, tagver_sz, MADV_SEQUENTIAL);
        madvise((void*)value_col,  value_sz,  MADV_SEQUENTIAL);
        // Prefetch small dimension arrays
        madvise((void*)fy_col,       fy_sz,       MADV_WILLNEED);
        madvise((void*)name_offsets, name_off_sz, MADV_WILLNEED);
        madvise((void*)tag_offsets,  tag_off_sz,  MADV_WILLNEED);
    }

    // ---- Zone map: collect valid blocks ----
    // num sorted by (uom_code, ddate) → 'pure' rows contiguous → ~1 block range
    std::vector<uint32_t> valid_blocks;
    valid_blocks.reserve(4);
    for (uint32_t b = 0; b < n_blocks; b++) {
        if (zmaps[b].min_uom > pure_code || zmaps[b].max_uom < pure_code) continue;
        valid_blocks.push_back(b);
    }

    // ---- Allocate global MAX hash map on heap (65536 × 16B = 1MB) ----
    // Heap alloc → no anonymous mmap → no ~65K page-fault overhead from iter_0
    MaxSlot* max_map = new MaxSlot[MAP_CAP];
    for (uint32_t i = 0; i < MAP_CAP; i++) {
        max_map[i].key   = EMPTY_KEY;
        max_map[i].value = -std::numeric_limits<double>::infinity();
    }

    // Candidate buffer: rows passing all three filters (uom + tagver + fy)
    // Expected ~18913 entries; reserve with headroom
    std::vector<Candidate> candidates;
    candidates.reserve(32768);

    // ---- SINGLE PASS: build MAX map AND collect candidates ----
    // Restricting aggregation to fy=2022 adsh_codes is semantically equivalent
    // to the full subquery because n.adsh=m.adsh pins each outer row to one
    // adsh_code with fixed fy — non-fy=2022 adsh_codes never appear in result.
    {
        GENDB_PHASE("main_scan");

        for (size_t bi = 0; bi < valid_blocks.size(); bi++) {
            uint32_t b = valid_blocks[bi];
            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = std::min(row_start + BLOCK_SIZE, num_rows);

            for (size_t i = row_start; i < row_end; i++) {
                // Filter 1: uom = 'pure' (zone maps skipped ~394/395 blocks)
                if (uom_col[i] != pure_code) continue;

                // Filter 2: tagver_code != -1
                int32_t tagver = tagver_col[i];
                if (tagver == -1) continue;

                int32_t adsh = adsh_col[i];

                // Filter 3: sub.fy == 2022 (O(1) direct array; ~80% selectivity)
                if (fy_col[adsh] != (int16_t)2022) continue;

                double val = value_col[i];
                uint64_t key = pack_key(adsh, tagver);

                // Aggregate: MAX(value) per (adsh_code, tagver_code)
                map_upsert(max_map, key, val);

                // Collect candidate for post-filter
                candidates.push_back({val, adsh, tagver});
            }
        }
    }

    // ---- POST-FILTER + SORT ----
    {
        GENDB_PHASE("build_joins");

        // Keep only rows where value == MAX(value) for their (adsh, tagver) group
        size_t out = 0;
        for (size_t i = 0; i < candidates.size(); i++) {
            const Candidate& c = candidates[i];
            if (c.value == map_get(max_map, pack_key(c.adsh_code, c.tagver_code))) {
                candidates[out++] = c;
            }
        }
        candidates.resize(out);
    }

    delete[] max_map;
    max_map = nullptr;

    // ---- STRING DECODE + OUTPUT ----
    {
        GENDB_PHASE("output");

        struct ResultRow {
            double      value;
            std::string name;
            std::string tag;
        };

        // Decode strings for all post-filtered candidates (~500 expected)
        std::vector<ResultRow> rows;
        rows.reserve(candidates.size());
        for (const auto& c : candidates) {
            uint32_t n0 = name_offsets[c.adsh_code];
            uint32_t n1 = name_offsets[c.adsh_code + 1];
            std::string name(name_data + n0, n1 - n0);

            uint32_t t0 = tag_offsets[(uint32_t)c.tagver_code];
            uint32_t t1 = tag_offsets[(uint32_t)c.tagver_code + 1];
            std::string tag(tag_data + t0, t1 - t0);

            rows.push_back({c.value, std::move(name), std::move(tag)});
        }

        // Sort: value DESC, name ASC, tag ASC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.value != b.value) return a.value > b.value;
            if (a.name  != b.name)  return a.name  < b.name;
            return a.tag < b.tag;
        });

        // Limit to top 100
        if (rows.size() > 100) rows.resize(100);

        // Write CSV output
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");
        for (const auto& r : rows) {
            // Quote fields containing commas (per CSV standard)
            bool nq = (r.name.find(',') != std::string::npos);
            bool tq = (r.tag.find(',')  != std::string::npos);
            if (nq) fprintf(f, "\"%s\",", r.name.c_str());
            else    fprintf(f, "%s,",     r.name.c_str());
            if (tq) fprintf(f, "\"%s\",", r.tag.c_str());
            else    fprintf(f, "%s,",     r.tag.c_str());
            fprintf(f, "%.2f\n", r.value);
        }
        fclose(f);
    }
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
