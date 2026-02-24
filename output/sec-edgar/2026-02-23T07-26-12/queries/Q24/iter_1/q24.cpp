/*
 * Q24 — Anti-join: num rows (USD, 2023, !NaN) with NO matching pre row
 * Strategy: bloom filter pre-filter → pre_atv_hash exact check → thread-local hash agg
 */

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <climits>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <omp.h>
#include "timing_utils.h"

// ─── Constants ────────────────────────────────────────────────────────────────
static constexpr uint64_t NUM_ROWS    = 39401761ULL;
static constexpr uint32_t BLOCK_SIZE  = 100000;

// Aggregation hash map: next_power_of_2(200000 * 2) = 524288 (C9)
static constexpr uint32_t AGG_CAP     = 524288;
static constexpr uint32_t AGG_MASK    = AGG_CAP - 1;
static constexpr uint64_t AGG_EMPTY   = UINT64_MAX;  // sentinel: tag=-1,ver=-1 never valid

// Bloom filter: 16 MB, power-of-2 bits for fast masking
static constexpr uint32_t BLOOM_BYTES = 1u << 24;        // 16,777,216 bytes
static constexpr uint64_t BLOOM_BITS  = (uint64_t)BLOOM_BYTES * 8;  // 2^27

// ─── Structs ──────────────────────────────────────────────────────────────────
struct ZoneBlockI16 { int16_t min_val; int16_t max_val; uint32_t row_count; };
struct ZoneBlockI32 { int32_t min_val; int32_t max_val; uint32_t row_count; };
struct PreATVSlot   { int32_t adsh;    int32_t tag;     int32_t version;    int32_t row_id; };

struct AggSlot {
    uint64_t key;
    int32_t  cnt;
    int32_t  _pad;
    int64_t  sum_cents;
};

// ─── mmap helper ──────────────────────────────────────────────────────────────
static const void* mmap_ro(const std::string& path, size_t* sz_out = nullptr) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; exit(1); }
    struct stat st; fstat(fd, &st);
    if (sz_out) *sz_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    close(fd);
    return p;
}

// ─── Dict loader ──────────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { std::cerr << "Cannot open dict: " << path << "\n"; exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

// ─── Page-aligned madvise ─────────────────────────────────────────────────────
static void madvise_range(const void* ptr, size_t bytes, int advice) {
    static const uintptr_t PAGE = 4096;
    uintptr_t start = (uintptr_t)ptr & ~(PAGE - 1);
    uintptr_t end   = (uintptr_t)ptr + bytes;
    end = (end + PAGE - 1) & ~(PAGE - 1);
    madvise((void*)start, (size_t)(end - start), advice);
}

// ─── Bloom filter ─────────────────────────────────────────────────────────────
// Double-hashing with k=7 hash functions; power-of-2 BLOOM_BITS for fast masking
static inline void bloom_set(uint8_t* bloom, int32_t adsh, int32_t tag, int32_t ver) {
    const uint64_t a = (uint64_t)(uint32_t)adsh;
    const uint64_t t = (uint64_t)(uint32_t)tag;
    const uint64_t v = (uint64_t)(uint32_t)ver;
    uint64_t h1 = a * 0x9E3779B97F4A7C15ULL ^ t * 0x517CC1B727220A95ULL ^ v * 0x6C62272E07BB0142ULL;
    uint64_t h2 = a * 0xBF58476D1CE4E5B9ULL ^ t * 0x94D049BB133111EBULL ^ v * 0xA9CB4D1F4E76AE3BULL;
    h2 |= 1ULL;  // ensure h2 is odd for double hashing
    for (int k = 0; k < 7; ++k) {
        uint64_t pos = (h1 + (uint64_t)k * h2) & (BLOOM_BITS - 1);
        __atomic_fetch_or(&bloom[pos >> 3], (uint8_t)(1u << (pos & 7)), __ATOMIC_RELAXED);
    }
}

static inline bool bloom_test(const uint8_t* bloom, int32_t adsh, int32_t tag, int32_t ver) {
    const uint64_t a = (uint64_t)(uint32_t)adsh;
    const uint64_t t = (uint64_t)(uint32_t)tag;
    const uint64_t v = (uint64_t)(uint32_t)ver;
    uint64_t h1 = a * 0x9E3779B97F4A7C15ULL ^ t * 0x517CC1B727220A95ULL ^ v * 0x6C62272E07BB0142ULL;
    uint64_t h2 = a * 0xBF58476D1CE4E5B9ULL ^ t * 0x94D049BB133111EBULL ^ v * 0xA9CB4D1F4E76AE3BULL;
    h2 |= 1ULL;
    for (int k = 0; k < 7; ++k) {
        uint64_t pos = (h1 + (uint64_t)k * h2) & (BLOOM_BITS - 1);
        if (!(bloom[pos >> 3] & (1u << (pos & 7)))) return false;
    }
    return true;
}

// ─── pre_atv_hash probe (C24: bounded for-loop) ───────────────────────────────
static inline bool probe_pre(const PreATVSlot* slots, uint64_t cap,
                              int32_t adsh, int32_t tag, int32_t ver) {
    uint64_t h = (uint64_t)(uint32_t)adsh * 2654435761ULL
               ^ (uint64_t)(uint32_t)tag  * 40503ULL
               ^ (uint64_t)(uint32_t)ver  * 48271ULL;
    for (uint64_t p = 0; p < cap; ++p) {
        uint64_t idx = (h + p) & (cap - 1);
        if (slots[idx].adsh == INT32_MIN) return false;        // empty slot → not found
        if (slots[idx].adsh == adsh && slots[idx].tag == tag && slots[idx].version == ver)
            return true;
    }
    return false;
}

// ─── Aggregation hash map: splitmix64-based hash ──────────────────────────────
static inline uint32_t hash_agg(uint64_t key) {
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return (uint32_t)(key & AGG_MASK);
}

static inline void agg_upsert(AggSlot* __restrict__ map,
                               uint64_t key, int32_t dcnt, int64_t dcents) {
    uint32_t idx = hash_agg(key);
    for (uint32_t p = 0; p < AGG_CAP; ++p) {
        uint32_t s = (idx + p) & AGG_MASK;
        if (map[s].key == AGG_EMPTY) {
            map[s].key       = key;
            map[s].cnt       = dcnt;
            map[s].sum_cents = dcents;
            return;
        }
        if (map[s].key == key) {
            map[s].cnt       += dcnt;
            map[s].sum_cents += dcents;
            return;
        }
    }
    std::cerr << "AGG MAP FULL!\n";
    exit(1);
}

// ─── Main query ───────────────────────────────────────────────────────────────
void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Data Loading ──────────────────────────────────────────────────────────
    const int16_t*      num_uom     = nullptr;
    const int32_t*      num_ddate   = nullptr;
    const double*       num_value   = nullptr;
    const int32_t*      num_adsh    = nullptr;
    const int32_t*      num_tag     = nullptr;
    const int32_t*      num_version = nullptr;
    const int32_t*      pre_adsh_c  = nullptr;
    const int32_t*      pre_tag_c   = nullptr;
    const int32_t*      pre_ver_c   = nullptr;
    uint64_t            pre_n       = 0;
    const ZoneBlockI16* uom_zone    = nullptr;
    uint32_t            uom_nblocks = 0;
    const ZoneBlockI32* ddate_zone  = nullptr;
    uint32_t            ddate_nblocks = 0;
    const PreATVSlot*   pre_atv_slots = nullptr;
    uint64_t            pre_atv_cap   = 0;
    size_t              pre_atv_sz    = 0;

    {
        GENDB_PHASE("data_loading");

        // num columns (no prefault — zone-map-guided madvise below)
        num_uom     = (const int16_t*)mmap_ro(gendb_dir + "/num/uom.bin");
        num_ddate   = (const int32_t*)mmap_ro(gendb_dir + "/num/ddate.bin");
        num_value   = (const double* )mmap_ro(gendb_dir + "/num/value.bin");
        num_adsh    = (const int32_t*)mmap_ro(gendb_dir + "/num/adsh.bin");
        num_tag     = (const int32_t*)mmap_ro(gendb_dir + "/num/tag.bin");
        num_version = (const int32_t*)mmap_ro(gendb_dir + "/num/version.bin");

        // pre columns — full sequential scan for bloom build
        {
            size_t sz;
            pre_adsh_c = (const int32_t*)mmap_ro(gendb_dir + "/pre/adsh.bin", &sz);
            pre_n      = sz / sizeof(int32_t);
        }
        pre_tag_c = (const int32_t*)mmap_ro(gendb_dir + "/pre/tag.bin");
        pre_ver_c = (const int32_t*)mmap_ro(gendb_dir + "/pre/version.bin");
        madvise_range(pre_adsh_c, pre_n * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise_range(pre_tag_c,  pre_n * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise_range(pre_ver_c,  pre_n * sizeof(int32_t), MADV_SEQUENTIAL);

        // Zone maps (small, prefetch immediately)
        {
            size_t sz;
            const uint8_t* raw = (const uint8_t*)mmap_ro(gendb_dir + "/indexes/num_uom_zone_map.bin", &sz);
            madvise_range(raw, sz, MADV_WILLNEED);
            uom_nblocks = *(const uint32_t*)raw;
            uom_zone    = (const ZoneBlockI16*)(raw + sizeof(uint32_t));
        }
        {
            size_t sz;
            const uint8_t* raw = (const uint8_t*)mmap_ro(gendb_dir + "/indexes/num_ddate_zone_map.bin", &sz);
            madvise_range(raw, sz, MADV_WILLNEED);
            ddate_nblocks = *(const uint32_t*)raw;
            ddate_zone    = (const ZoneBlockI32*)(raw + sizeof(uint32_t));
        }

        // pre_atv_hash: mmap + WILLNEED to prefetch 536 MB from HDD (P11, P19)
        {
            const uint8_t* raw = (const uint8_t*)mmap_ro(gendb_dir + "/indexes/pre_atv_hash.bin", &pre_atv_sz);
            madvise_range(raw, pre_atv_sz, MADV_WILLNEED);
            pre_atv_cap   = *(const uint64_t*)raw;
            pre_atv_slots = (const PreATVSlot*)(raw + sizeof(uint64_t));
        }
    }

    // ── Load dicts + find USD code (C2) ──────────────────────────────────────
    auto uom_dict     = load_dict(gendb_dir + "/num/uom_dict.txt");
    auto tag_dict     = load_dict(gendb_dir + "/num/tag_dict.txt");
    auto version_dict = load_dict(gendb_dir + "/num/version_dict.txt");

    int16_t usd_code = -1;
    for (size_t i = 0; i < uom_dict.size(); ++i) {
        if (uom_dict[i] == "USD") { usd_code = (int16_t)i; break; }
    }
    if (usd_code < 0) { std::cerr << "USD not found in uom_dict\n"; exit(1); }

    // ── Zone-map-guided selective madvise for num columns (P13) ──────────────
    for (uint32_t b = 0; b < uom_nblocks; ++b) {
        if (uom_zone[b].max_val < usd_code || uom_zone[b].min_val > usd_code) continue;
        if (b < ddate_nblocks &&
            (ddate_zone[b].max_val < 20230101 || ddate_zone[b].min_val > 20231231)) continue;
        size_t   row_off = (size_t)b * BLOCK_SIZE;
        uint32_t rcount  = (uint32_t)std::min((uint64_t)BLOCK_SIZE, NUM_ROWS - row_off);
        madvise_range(num_uom     + row_off, rcount * sizeof(int16_t), MADV_WILLNEED);
        madvise_range(num_ddate   + row_off, rcount * sizeof(int32_t), MADV_WILLNEED);
        madvise_range(num_value   + row_off, rcount * sizeof(double),  MADV_WILLNEED);
        madvise_range(num_adsh    + row_off, rcount * sizeof(int32_t), MADV_WILLNEED);
        madvise_range(num_tag     + row_off, rcount * sizeof(int32_t), MADV_WILLNEED);
        madvise_range(num_version + row_off, rcount * sizeof(int32_t), MADV_WILLNEED);
    }

    // ── Build Bloom Filter ────────────────────────────────────────────────────
    uint8_t* bloom = nullptr;
    {
        GENDB_PHASE("build_bloom");
        bloom = (uint8_t*)mmap(nullptr, BLOOM_BYTES,
                               PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (bloom == MAP_FAILED) { perror("bloom mmap"); exit(1); }
        memset(bloom, 0, BLOOM_BYTES);  // zero-init OK for byte sentinel

        // Parallel bloom build with atomic OR writes (P21, P22)
        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < (int64_t)pre_n; ++i) {
            bloom_set(bloom, pre_adsh_c[i], pre_tag_c[i], pre_ver_c[i]);
        }
    }

    // ── Main Scan: Anti-Join + Thread-Local Aggregation (P20) ────────────────
    const int nthreads = omp_get_max_threads();
    std::vector<AggSlot*> thread_maps(nthreads, nullptr);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();

            // Allocate + initialize thread-local aggregation map (C9, C20)
            AggSlot* my_map = new AggSlot[AGG_CAP];
            for (uint32_t i = 0; i < AGG_CAP; ++i) {
                my_map[i].key       = AGG_EMPTY;
                my_map[i].cnt       = 0;
                my_map[i]._pad      = 0;
                my_map[i].sum_cents = 0;
            }
            thread_maps[tid] = my_map;

            // Morsel-driven parallel scan over zone-map-qualified blocks
            #pragma omp for schedule(dynamic, 4)
            for (int32_t b = 0; b < (int32_t)uom_nblocks; ++b) {
                // Block-level zone map skip (C19)
                if (uom_zone[b].max_val < usd_code || uom_zone[b].min_val > usd_code) continue;
                if ((uint32_t)b < ddate_nblocks &&
                    (ddate_zone[b].max_val < 20230101 || ddate_zone[b].min_val > 20231231)) continue;

                const uint64_t row_start = (uint64_t)b * BLOCK_SIZE;
                const uint64_t row_end   = row_start +
                    std::min((uint64_t)uom_zone[b].row_count, NUM_ROWS - row_start);

                for (uint64_t i = row_start; i < row_end; ++i) {
                    // Row-level filters
                    if (num_uom[i] != usd_code)                           continue;
                    if (num_ddate[i] < 20230101 || num_ddate[i] > 20231231) continue;
                    const double v = num_value[i];
                    if (std::isnan(v))                                     continue;

                    // Anti-join: bloom filter → exact hash probe
                    const int32_t a  = num_adsh[i];
                    const int32_t t  = num_tag[i];
                    const int32_t vr = num_version[i];

                    if (bloom_test(bloom, a, t, vr)) {
                        // Bloom-positive (~1%): verify with pre_atv_hash (P19, C24)
                        if (probe_pre(pre_atv_slots, pre_atv_cap, a, t, vr)) continue;
                    }
                    // Anti-join passes → aggregate (C29: int64_t cents)
                    const uint64_t gkey  = ((uint64_t)(uint32_t)t << 32) | (uint32_t)vr;
                    const int64_t  cents = llround(v * 100.0);
                    agg_upsert(my_map, gkey, 1, cents);
                }
            }
        }  // end omp parallel
    }

    // ── Aggregation Merge (P17, P20) ──────────────────────────────────────────
    std::vector<AggSlot> global_map(AGG_CAP);
    {
        GENDB_PHASE("aggregation_merge");
        // Initialize global map (C20: use fill-loop, not memset)
        for (uint32_t i = 0; i < AGG_CAP; ++i) {
            global_map[i].key       = AGG_EMPTY;
            global_map[i].cnt       = 0;
            global_map[i].sum_cents = 0;
        }

        // Single-pass sequential merge from thread-local maps
        AggSlot* gmap = global_map.data();
        for (int tid = 0; tid < nthreads; ++tid) {
            AggSlot* lmap = thread_maps[tid];
            if (!lmap) continue;
            for (uint32_t i = 0; i < AGG_CAP; ++i) {
                if (lmap[i].key == AGG_EMPTY) continue;
                agg_upsert(gmap, lmap[i].key, lmap[i].cnt, lmap[i].sum_cents);
            }
            delete[] lmap;
            thread_maps[tid] = nullptr;
        }
    }

    // ── Filter HAVING + Top-K Sort + Output ───────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Group {
            int32_t tag_code;
            int32_t ver_code;
            int32_t cnt;
            int64_t sum_cents;
        };

        // Collect groups with cnt > 10 (HAVING)
        std::vector<Group> groups;
        groups.reserve(200000);
        for (uint32_t i = 0; i < AGG_CAP; ++i) {
            const AggSlot& s = global_map[i];
            if (s.key == AGG_EMPTY || s.cnt <= 10) continue;
            const int32_t tc = (int32_t)(s.key >> 32);
            const int32_t vc = (int32_t)(s.key & 0xFFFFFFFFULL);
            groups.push_back({tc, vc, s.cnt, s.sum_cents});
        }

        // Top-100 by cnt DESC (P6: partial_sort O(n log k))
        const size_t k = std::min((size_t)100, groups.size());
        std::partial_sort(groups.begin(), groups.begin() + k, groups.end(),
            [](const Group& a, const Group& b) { return a.cnt > b.cnt; });
        groups.resize(k);

        // Write CSV (C18: decode codes to strings)
        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q24.csv");
        if (!out) { std::cerr << "Cannot open output file\n"; exit(1); }
        out << "tag,version,cnt,total\n";

        for (const auto& g : groups) {
            const std::string& tname = (g.tag_code >= 0 && (size_t)g.tag_code < tag_dict.size())
                                       ? tag_dict[g.tag_code]
                                       : std::to_string(g.tag_code);
            const std::string& vname = (g.ver_code >= 0 && (size_t)g.ver_code < version_dict.size())
                                       ? version_dict[g.ver_code]
                                       : std::to_string(g.ver_code);

            // C29: output sum_cents as decimal with 2 decimal places
            const int64_t sc  = g.sum_cents;
            const int64_t asc = (sc < 0) ? -sc : sc;
            const int64_t whl = asc / 100;
            const int64_t frc = asc % 100;

            out << tname << "," << vname << "," << g.cnt << ",";
            if (sc < 0) out << "-";
            out << whl << "." << std::setw(2) << std::setfill('0') << frc << "\n";
        }
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q24(gendb_dir, results_dir);
    return 0;
}
#endif
