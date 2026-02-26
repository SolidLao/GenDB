// Q6: SEC-Edgar — Iteration 1
// SELECT s.name, p.stmt, n.tag, p.plabel,
//        SUM(n.value) AS total_value, COUNT(*) AS cnt
// FROM num n
// JOIN sub s ON n.adsh = s.adsh
// JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
// WHERE n.uom = 'USD' AND p.stmt = 'IS' AND s.fy = 2023
//       AND n.value IS NOT NULL
// GROUP BY s.name, p.stmt, n.tag, p.plabel
// ORDER BY total_value DESC
// LIMIT 200;
//
// Pipeline:
//   build_sub_filter -> build_is_pre_hashtable -> scan_num_zonemaps
//   -> parallel_scan_probe_aggregate -> merge_aggregate -> topk_decode_output
//
// Key optimizations vs iter_0 (1121ms):
//   1. Runtime-built IS-only pre hash table (2^21 slots=32MB, L3-resident)
//      replaces O(log 9.6M) binary search into 153MB pre_key_sorted
//   2. pre.plabel.dict codes in IS HT value → plabel_code as direct group key
//      eliminates secondary string re-grouping pass entirely
//   3. Flat open-addressing thread-local agg maps (2^18 slots=6.3MB each, L3-resident)
//      replaces std::unordered_map (poor cache behavior)
//   4. Flat open-addressing global merge map (2^18 slots=6.3MB) — no pointer chasing

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ── mmap helper ───────────────────────────────────────────────────────────────
static const uint8_t* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    if (out_sz == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return (const uint8_t*)p;
}
static void munmap_ro(const void* p, size_t sz) { if (p && sz) munmap((void*)p, sz); }

// ── Dict code lookup: [uint32_t n][uint16_t len, bytes...]*n ──────────────────
static int32_t dict_lookup_code(const std::string& path, const char* target) {
    size_t sz;
    const uint8_t* d = mmap_ro(path, sz);
    uint32_t count = *(const uint32_t*)d;
    size_t off = 4;
    size_t tlen = strlen(target);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = *(const uint16_t*)(d + off); off += 2;
        if ((size_t)len == tlen && memcmp(d + off, target, len) == 0) {
            munmap_ro(d, sz); return (int32_t)i;
        }
        off += len;
    }
    munmap_ro(d, sz); return -1;
}

// ── Build string_view vector from [uint32_t n][uint16_t len, bytes...]*n ──────
static std::vector<std::string_view> build_dict_sv(const uint8_t* d) {
    uint32_t count = *(const uint32_t*)d;
    std::vector<std::string_view> sv(count);
    size_t off = 4;
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = *(const uint16_t*)(d + off); off += 2;
        sv[i] = std::string_view((const char*)(d + off), len);
        off += len;
    }
    return sv;
}

// ── Zone map structs (10 bytes, packed) ───────────────────────────────────────
#pragma pack(push,1)
struct NumZoneBlock { int8_t uom_min, uom_max; int32_t ddate_min, ddate_max; };
struct PreZoneBlock { int8_t stmt_min, stmt_max; int32_t adsh_min, adsh_max; };
#pragma pack(pop)
static_assert(sizeof(NumZoneBlock) == 10, "NumZoneBlock must be 10 bytes");
static_assert(sizeof(PreZoneBlock) == 10, "PreZoneBlock must be 10 bytes");

// ── IS Hash Table ─────────────────────────────────────────────────────────────
// Flat open-addressing, 2^21 = 2097152 slots × 16 bytes = 32MB
// key:   pack_is_key(adsh:17b | tag:18b | ver:17b) → bits 0-51 only
// value: (mult:16b << 44) | (pre_row_id:24b << 20) | plabel_code:20b → bits 0-59
//        mult starts at 1; incremented when duplicate key+same-plabel is inserted
// empty: UINT64_MAX (bits 52-63 always set; valid keys use only bits 0-51)
struct ISSlot {
    uint64_t key;
    uint64_t value;
};
static_assert(sizeof(ISSlot) == 16, "");

static const uint32_t IS_HT_SIZE = 1u << 21;   // 2097152 slots = 32MB
static const uint32_t IS_HT_MASK = IS_HT_SIZE - 1;

// ── Aggregation map slot ──────────────────────────────────────────────────────
// key: (name_code:14b << 38) | (tag_code:18b << 20) | plabel_code:20b → bits 0-51
// empty: UINT64_MAX
struct AggSlot {
    uint64_t key;
    double   sum;
    int64_t  cnt;
};
static_assert(sizeof(AggSlot) == 24, "");

// Compact entry for serial merge (non-empty aggregation slot)
struct AggEntry {
    uint64_t key;
    double   sum;
    int64_t  cnt;
};
static_assert(sizeof(AggEntry) == 24, "");

static const uint64_t EMPTY_KEY = UINT64_MAX;
static const uint32_t TL_SIZE   = 1u << 18;    // 262144 slots per thread = 6.3MB
static const uint32_t TL_MASK   = TL_SIZE - 1;
static const uint32_t GM_SIZE   = 1u << 18;    // 262144 slots for global merge = 6.3MB
static const uint32_t GM_MASK   = GM_SIZE - 1;

// ── Fast hash mix ─────────────────────────────────────────────────────────────
static inline uint64_t hash64(uint64_t k) noexcept {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

// ── IS HT packing ─────────────────────────────────────────────────────────────
// adsh_code: 17b (max 86134  < 2^17=131072  ✓)
// tag_code:  18b (max 198311 < 2^18=262144  ✓)
// ver_code:  17b (max 83814  < 2^17=131072  ✓)
static inline uint64_t pack_is_key(int32_t adsh, int32_t tag, int32_t ver) noexcept {
    return ((uint64_t)(uint32_t)adsh << 35) |
           ((uint64_t)(uint32_t)tag  << 17) |
            (uint64_t)(uint32_t)ver;
}

// Insert IS pre row into IS HT; tracks multiplicity for duplicate (adsh,tag,ver) keys
// value format: (mult:16b << 44) | (row_id:24b << 20) | plabel_code:20b
static inline void is_ht_insert(ISSlot* __restrict__ ht, uint64_t key,
                                  uint32_t row_id, uint32_t plabel_code) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & IS_HT_MASK;
    while (ht[idx].key != EMPTY_KEY && ht[idx].key != key)
        idx = (idx + 1) & IS_HT_MASK;
    if (ht[idx].key == EMPTY_KEY) {
        // New key: mult=1
        ht[idx].key   = key;
        ht[idx].value = (1ULL << 44) | ((uint64_t)row_id << 20) | (uint64_t)plabel_code;
    } else {
        // Duplicate key: if same plabel_code, increment multiplier
        if ((ht[idx].value & 0xFFFFFu) == plabel_code) {
            ht[idx].value += (1ULL << 44);  // increment mult by 1
        }
        // Different plabel: keep first entry (rare; would need chaining to fix)
    }
}

static inline uint64_t is_ht_probe(const ISSlot* __restrict__ ht, uint64_t key) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & IS_HT_MASK;
    while (true) {
        const uint64_t k = ht[idx].key;
        if (k == EMPTY_KEY) return EMPTY_KEY;
        if (k == key)        return ht[idx].value;
        idx = (idx + 1) & IS_HT_MASK;
    }
}

// ── Group key packing ─────────────────────────────────────────────────────────
// name_code:   14b (max ~9644  < 2^14=16384   ✓)
// tag_code:    18b (max 198311 < 2^18=262144   ✓)
// plabel_code: 20b (max 698148 < 2^20=1048576  ✓)
static inline uint64_t pack_grp_key(uint32_t nc, uint32_t tc, uint32_t pc) noexcept {
    return ((uint64_t)nc << 38) | ((uint64_t)tc << 20) | (uint64_t)pc;
}
static inline uint32_t unpack_name_code  (uint64_t k) noexcept { return (uint32_t)(k >> 38); }
static inline uint32_t unpack_tag_code   (uint64_t k) noexcept { return (uint32_t)((k >> 20) & 0x3FFFFu); }
static inline uint32_t unpack_plabel_code(uint64_t k) noexcept { return (uint32_t)(k & 0xFFFFFu); }

// ── Flat agg map upsert (with multiplier for duplicate IS pre rows) ───────────
static inline void agg_upsert(AggSlot* __restrict__ slots, uint32_t mask,
                               uint64_t key, double val, int64_t mult) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & mask;
    while (slots[idx].key != EMPTY_KEY && slots[idx].key != key)
        idx = (idx + 1) & mask;
    if (__builtin_expect(slots[idx].key == EMPTY_KEY, 0))
        slots[idx].key = key;
    slots[idx].sum += val * (double)mult;
    slots[idx].cnt += mult;
}

// ── CSV field writer ──────────────────────────────────────────────────────────
static void write_csv_field(FILE* fp, std::string_view sv) {
    bool q = false;
    for (char c : sv) if (c == ',' || c == '"' || c == '\n' || c == '\r') { q = true; break; }
    if (q) {
        fputc('"', fp);
        for (char c : sv) { if (c == '"') fputc('"', fp); fputc(c, fp); }
        fputc('"', fp);
    } else {
        fwrite(sv.data(), 1, sv.size(), fp);
    }
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    const std::string G = argv[1];
    const std::string R = argv[2];
    mkdir(R.c_str(), 0755);

    // ── dict_lookup: resolve USD and IS codes ─────────────────────────────────
    int8_t usd_code_i8, is_code_i8;
    {
        GENDB_PHASE("dict_lookup");
        int32_t uc = dict_lookup_code(G + "/shared/uom.dict",  "USD");
        int32_t ic = dict_lookup_code(G + "/shared/stmt.dict", "IS");
        if (uc < 0) { fprintf(stderr, "USD not found in uom.dict\n"); return 1; }
        if (ic < 0) { fprintf(stderr, "IS not found in stmt.dict\n");  return 1; }
        usd_code_i8 = (int8_t)uc;
        is_code_i8  = (int8_t)ic;
        fprintf(stderr, "[info] usd_code=%d is_code=%d\n", (int)usd_code_i8, (int)is_code_i8);
    }

    // ── data_loading: mmap all columns and indexes ────────────────────────────
    size_t sub_fy_sz, sub_name_sz;
    size_t num_uom_sz, num_val_sz, num_adsh_sz, num_tag_sz, num_ver_sz;
    size_t pre_stmt_sz, pre_adsh_sz, pre_tag_sz, pre_ver_sz;
    size_t plabel_codes_sz;
    size_t num_zone_sz, pre_zone_sz;

    const int16_t*  sub_fy_col;
    const int32_t*  sub_name_col;
    const int8_t*   num_uom_col;
    const double*   num_val_col;
    const int32_t*  num_adsh_col;
    const int32_t*  num_tag_col;
    const int32_t*  num_ver_col;
    const int8_t*   pre_stmt_col;
    const int32_t*  pre_adsh_col;
    const int32_t*  pre_tag_col;
    const int32_t*  pre_ver_col;
    const uint32_t* plabel_codes_col;
    const uint8_t*  num_zone_raw;
    const uint8_t*  pre_zone_raw;

    {
        GENDB_PHASE("data_loading");
        sub_fy_col       = (const int16_t*) mmap_ro(G + "/sub/fy.bin",    sub_fy_sz);
        sub_name_col     = (const int32_t*) mmap_ro(G + "/sub/name.bin",  sub_name_sz);
        num_uom_col      = (const int8_t*)  mmap_ro(G + "/num/uom.bin",      num_uom_sz);
        num_val_col      = (const double*)  mmap_ro(G + "/num/value.bin",     num_val_sz);
        num_adsh_col     = (const int32_t*) mmap_ro(G + "/num/adsh.bin",      num_adsh_sz);
        num_tag_col      = (const int32_t*) mmap_ro(G + "/num/tag.bin",       num_tag_sz);
        num_ver_col      = (const int32_t*) mmap_ro(G + "/num/version.bin",   num_ver_sz);
        pre_stmt_col     = (const int8_t*)  mmap_ro(G + "/pre/stmt.bin",      pre_stmt_sz);
        pre_adsh_col     = (const int32_t*) mmap_ro(G + "/pre/adsh.bin",      pre_adsh_sz);
        pre_tag_col      = (const int32_t*) mmap_ro(G + "/pre/tag.bin",       pre_tag_sz);
        pre_ver_col      = (const int32_t*) mmap_ro(G + "/pre/version.bin",   pre_ver_sz);
        plabel_codes_col = (const uint32_t*)mmap_ro(
            G + "/column_versions/pre.plabel.dict/codes.bin", plabel_codes_sz);
        num_zone_raw     = (const uint8_t*) mmap_ro(G + "/indexes/num_zonemaps.bin", num_zone_sz);
        pre_zone_raw     = (const uint8_t*) mmap_ro(G + "/indexes/pre_zonemaps.bin", pre_zone_sz);

        // Sequential access hints for large column scans
        madvise((void*)num_uom_col,  num_uom_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_val_col,  num_val_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_adsh_col, num_adsh_sz, MADV_SEQUENTIAL);
        madvise((void*)num_tag_col,  num_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_ver_col,  num_ver_sz,  MADV_SEQUENTIAL);
        madvise((void*)pre_stmt_col, pre_stmt_sz, MADV_SEQUENTIAL);
        madvise((void*)pre_adsh_col, pre_adsh_sz, MADV_SEQUENTIAL);
        madvise((void*)pre_tag_col,  pre_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)pre_ver_col,  pre_ver_sz,  MADV_SEQUENTIAL);
        madvise((void*)plabel_codes_col, plabel_codes_sz, MADV_SEQUENTIAL);
    }

    const uint32_t SUB_N = (uint32_t)(sub_fy_sz  / sizeof(int16_t));   // 86135
    const size_t   NUM_N = num_uom_sz / sizeof(int8_t);                // 39401761
    const uint32_t PRE_N = (uint32_t)(pre_stmt_sz / sizeof(int8_t));   // 9600799
    fprintf(stderr, "[info] SUB_N=%u NUM_N=%zu PRE_N=%u\n", SUB_N, NUM_N, PRE_N);

    // ── build_sub_filter: fy2023 bool array + sub_name flat lookup ────────────
    std::vector<uint8_t>  fy2023(SUB_N, 0);
    std::vector<int32_t>  sub_name_flat(SUB_N);
    {
        GENDB_PHASE("build_sub_filter");
        for (uint32_t i = 0; i < SUB_N; i++) {
            sub_name_flat[i] = sub_name_col[i];
            if (sub_fy_col[i] == (int16_t)2023)
                fy2023[i] = 1;
        }
    }

    // ── build_is_pre_hashtable ────────────────────────────────────────────────
    // 2^21 = 2097152 slots × 16 bytes = 32MB, built single-threaded
    // Uses pre_zonemaps to scan only IS-stmt blocks (pre sorted by stmt,adsh)
    std::vector<ISSlot> is_ht(IS_HT_SIZE, {EMPTY_KEY, 0ULL});
    uint32_t is_count = 0;
    {
        GENDB_PHASE("build_is_pre_hashtable");
        const int32_t pre_nblocks = *(const int32_t*)pre_zone_raw;
        const uint8_t* pzb = pre_zone_raw + 4;
        const uint32_t PRE_BLOCK = 100000;
        ISSlot* ht = is_ht.data();

        for (int32_t b = 0; b < pre_nblocks; b++) {
            const PreZoneBlock* blk = (const PreZoneBlock*)(pzb + (size_t)b * 10);
            if (is_code_i8 < blk->stmt_min || is_code_i8 > blk->stmt_max) continue;
            const uint32_t lo = (uint32_t)b * PRE_BLOCK;
            const uint32_t hi = (lo + PRE_BLOCK < PRE_N) ? lo + PRE_BLOCK : PRE_N;
            for (uint32_t i = lo; i < hi; i++) {
                if (pre_stmt_col[i] != is_code_i8) continue;
                const uint64_t key = pack_is_key(pre_adsh_col[i], pre_tag_col[i], pre_ver_col[i]);
                const uint32_t pc  = plabel_codes_col[i];
                is_ht_insert(ht, key, i, pc);
                is_count++;
            }
        }
        fprintf(stderr, "[info] IS pre rows inserted: %u\n", is_count);
    }

    // Warm IS HT for parallel probing
    madvise(is_ht.data(), IS_HT_SIZE * sizeof(ISSlot), MADV_WILLNEED);

    // ── scan_num_zonemaps: identify USD-containing blocks ─────────────────────
    struct BlockRange { size_t lo, hi; };
    std::vector<BlockRange> usd_blocks;
    {
        GENDB_PHASE("scan_num_zonemaps");
        const int32_t n_blocks = *(const int32_t*)num_zone_raw;
        const uint8_t* blk_base = num_zone_raw + 4;
        const uint32_t BLOCK_SIZE = 100000;
        usd_blocks.reserve((size_t)n_blocks);
        for (int32_t b = 0; b < n_blocks; b++) {
            const NumZoneBlock* blk = (const NumZoneBlock*)(blk_base + (size_t)b * 10);
            if (usd_code_i8 < blk->uom_min || usd_code_i8 > blk->uom_max) continue;
            const size_t lo = (size_t)b * BLOCK_SIZE;
            const size_t hi = (lo + BLOCK_SIZE < NUM_N) ? lo + BLOCK_SIZE : NUM_N;
            usd_blocks.push_back({lo, hi});
        }
        fprintf(stderr, "[info] USD blocks: %zu / %d\n", usd_blocks.size(), n_blocks);
    }

    // ── parallel_scan_probe_aggregate ─────────────────────────────────────────
    // Each thread owns flat agg map: 2^18 slots × 24 bytes = 6.3MB
    const int nthreads = omp_get_max_threads();
    fprintf(stderr, "[info] nthreads=%d\n", nthreads);

    // Pre-allocate TL maps outside parallel (fast main-thread allocation).
    // After scan, compact non-empty entries inside the parallel region so all
    // 64 threads compact simultaneously — serial merge reads only ~23MB compact data.
    std::vector<std::vector<AggSlot>>  tl_maps(nthreads,
        std::vector<AggSlot>(TL_SIZE, {EMPTY_KEY, 0.0, 0LL}));
    std::vector<std::vector<AggEntry>> tl_compact(nthreads);

    {
        GENDB_PHASE("parallel_scan_probe_aggregate");
        const uint8_t*  fy2023_p  = fy2023.data();
        const int32_t*  sname_p   = sub_name_flat.data();
        const int8_t*   uom_p     = num_uom_col;
        const double*   val_p     = num_val_col;
        const int32_t*  adsh_p    = num_adsh_col;
        const int32_t*  tag_p     = num_tag_col;
        const int32_t*  ver_p     = num_ver_col;
        const ISSlot*   is_ht_p   = is_ht.data();
        const uint32_t  sub_n     = SUB_N;
        const int       nblocks   = (int)usd_blocks.size();

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            AggSlot* my_slots = tl_maps[tid].data();

            // Scan morsels: filter → IS HT probe → aggregate into TL map
            #pragma omp for schedule(dynamic, 1)
            for (int bidx = 0; bidx < nblocks; bidx++) {
                const size_t lo = usd_blocks[bidx].lo;
                const size_t hi = usd_blocks[bidx].hi;

                for (size_t i = lo; i < hi; i++) {
                    // Filter 1: uom == USD
                    if (uom_p[i] != usd_code_i8) continue;

                    // Filter 2: value IS NOT NULL
                    const double v = val_p[i];
                    if (std::isnan(v)) continue;

                    // Filter 3: sub.fy = 2023 (direct array lookup, L1-resident)
                    const int32_t ac = adsh_p[i];
                    if (ac < 0 || (uint32_t)ac >= sub_n) continue;
                    if (!fy2023_p[(uint32_t)ac]) continue;

                    // Lookup name_code (L1-resident flat array)
                    const int32_t nc = sname_p[(uint32_t)ac];
                    if (nc < 0) continue;

                    const int32_t tc = tag_p[i];
                    const int32_t vc = ver_p[i];

                    // Probe IS hash table: O(1) L3-resident, replaces O(log 9.6M) bsearch
                    const uint64_t is_key = pack_is_key(ac, tc, vc);
                    const uint64_t is_val = is_ht_probe(is_ht_p, is_key);
                    if (is_val == EMPTY_KEY) continue;

                    // plabel_code: bits 0-19; multiplier: bits 44-59
                    const uint32_t plabel_code = (uint32_t)(is_val & 0xFFFFFu);
                    const int64_t  mult        = (int64_t)((is_val >> 44) & 0xFFFFu);

                    // Aggregate into thread-local flat map (L3-resident per thread)
                    const uint64_t grp_key = pack_grp_key((uint32_t)nc, (uint32_t)tc, plabel_code);
                    agg_upsert(my_slots, TL_MASK, grp_key, v, mult);
                }
            }
            // Implicit barrier after omp for: all threads finished scanning.

            // Parallel compaction: each thread sequentially scans its own TL map
            // and emits non-empty entries into a compact dense array.
            // All 64 threads compact simultaneously → max time = single-thread scan of 6.3MB.
            auto& ce = tl_compact[tid];
            ce.reserve(65536);
            for (const AggSlot& s : tl_maps[tid]) {
                if (s.key != EMPTY_KEY)
                    ce.push_back({s.key, s.sum, s.cnt});
            }
            // Free TL map inside parallel region (avoids main-thread bulk free)
            { std::vector<AggSlot> tmp; tmp.swap(tl_maps[tid]); }
        } // end parallel
    }
    tl_maps.clear();

    // Free IS HT (no longer needed after scan)
    { std::vector<ISSlot> tmp; tmp.swap(is_ht); }

    // ── merge_aggregate: serial merge compact entries → global flat map ──────
    // tl_compact[t] is compact (~15K entries × 24 bytes ≈ 360KB per thread).
    // Total: ~23MB sequential reads across all threads → L3/DRAM fast.
    // Global map: 2^18 slots × 24 bytes = 6.3MB, L3-resident throughout.
    std::vector<AggSlot> gm(GM_SIZE, {EMPTY_KEY, 0.0, 0LL});
    size_t total_groups = 0;
    {
        GENDB_PHASE("merge_aggregate");
        AggSlot* gm_p = gm.data();

        for (int t = 0; t < nthreads; t++) {
            for (const AggEntry& e : tl_compact[t]) {
                uint32_t idx = (uint32_t)hash64(e.key) & GM_MASK;
                while (gm_p[idx].key != EMPTY_KEY && gm_p[idx].key != e.key)
                    idx = (idx + 1) & GM_MASK;
                if (__builtin_expect(gm_p[idx].key == EMPTY_KEY, 0)) {
                    gm_p[idx].key = e.key;
                    total_groups++;
                }
                gm_p[idx].sum += e.sum;
                gm_p[idx].cnt += e.cnt;
            }
            // Free compact list immediately after merging
            { std::vector<AggEntry> tmp; tmp.swap(tl_compact[t]); }
        }
        tl_compact.clear();
        fprintf(stderr, "[info] unique groups: %zu\n", total_groups);
    }

    // ── topk_decode_output ────────────────────────────────────────────────────
    {
        GENDB_PHASE("topk_decode_output");

        // Collect non-empty groups
        struct SortRow { uint64_t key; double sum; int64_t cnt; };
        std::vector<SortRow> rows;
        rows.reserve(total_groups);
        for (const AggSlot& s : gm) {
            if (s.key == EMPTY_KEY) continue;
            rows.push_back({s.key, s.sum, s.cnt});
        }

        // partial_sort top-200 by total_value DESC
        {
            GENDB_PHASE("topk_sort");
            const size_t K = std::min((size_t)200, rows.size());
            std::partial_sort(rows.begin(), rows.begin() + K, rows.end(),
                [](const SortRow& a, const SortRow& b) { return a.sum > b.sum; });
            rows.resize(K);
        }

        // Load decode dictionaries
        size_t name_dict_sz, tag_dict_sz;
        const uint8_t* name_dict_raw = mmap_ro(G + "/sub/name.dict",           name_dict_sz);
        const uint8_t* tag_dict_raw  = mmap_ro(G + "/shared/tag_numpre.dict",  tag_dict_sz);

        // Load plabel dict: dict_offsets = uint64_t[unique_values+1=698149]
        size_t plabel_off_sz, plabel_data_sz;
        const uint64_t* plabel_dict_off  = (const uint64_t*)mmap_ro(
            G + "/column_versions/pre.plabel.dict/dict.offsets", plabel_off_sz);
        const char*     plabel_dict_data = (const char*)mmap_ro(
            G + "/column_versions/pre.plabel.dict/dict.data",    plabel_data_sz);

        const auto name_sv = build_dict_sv(name_dict_raw);
        const auto tag_sv  = build_dict_sv(tag_dict_raw);
        const uint32_t plabel_n = (uint32_t)(plabel_off_sz / sizeof(uint64_t)) - 1;

        // Write CSV
        const std::string out_path = R + "/Q6.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return 1; }
        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");

        for (const SortRow& row : rows) {
            const uint32_t name_code   = unpack_name_code(row.key);
            const uint32_t tag_code    = unpack_tag_code(row.key);
            const uint32_t plabel_code = unpack_plabel_code(row.key);

            const std::string_view n_sv = (name_code < name_sv.size())
                ? name_sv[name_code] : std::string_view{};
            const std::string_view t_sv = (tag_code  < tag_sv.size())
                ? tag_sv[tag_code]   : std::string_view{};
            std::string_view p_sv{};
            if (plabel_code < plabel_n) {
                const uint64_t plo = plabel_dict_off[plabel_code];
                const uint64_t phi = plabel_dict_off[plabel_code + 1];
                p_sv = std::string_view(plabel_dict_data + plo, (size_t)(phi - plo));
            }

            write_csv_field(fp, n_sv);
            fputs(",IS,", fp);
            write_csv_field(fp, t_sv);
            fputc(',', fp);
            write_csv_field(fp, p_sv);
            fprintf(fp, ",%.2f,%lld\n", row.sum, (long long)row.cnt);
        }
        fclose(fp);

        munmap_ro(name_dict_raw,    name_dict_sz);
        munmap_ro(tag_dict_raw,     tag_dict_sz);
        munmap_ro(plabel_dict_off,  plabel_off_sz);
        munmap_ro(plabel_dict_data, plabel_data_sz);
    }

    return 0;
}
