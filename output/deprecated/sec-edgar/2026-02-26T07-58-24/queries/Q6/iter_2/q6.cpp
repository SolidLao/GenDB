// Q6: SEC-Edgar — Iteration 2
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
// Pipeline: parallel_preinit -> build_sub_filter -> build_is_pre_hashtable
//           -> scan_num_zonemaps -> parallel_scan_probe_aggregate
//           -> parallel_merge_aggregate -> topk_decode_output
//
// Key changes vs iter_1 (472ms):
//   1. parallel_preinit: single contiguous alloc for TL+IS+scatter; parallel
//      page-fault init eliminates ~163ms single-thread page-fault cost.
//   2. build_is_pre_hashtable: fy2023 filter → ~325K rows (vs 1.73M); IS_HT
//      2^21→2^20=16MB, 31% load, avg 1.5 probes. Fits in L3 during scan.
//   3. Flat scatter buffer (no dynamic alloc): compact step writes to pre-alloc'd
//      scatter_buf[src][dst][entry]; merge step reads only its column (~3K entries).
//      Eliminates O(n) per-thread scan of iter_1 serial merge.

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
// 2^20 = 1048576 slots x 16B = 16MB (halved from 2^21 via fy2023 filter at build)
// key:   pack_is_key(adsh:17b | tag:18b | ver:17b)
// value: (mult:16b << 44) | (row_id:24b << 20) | plabel_code:20b
// empty: UINT64_MAX
struct ISSlot { uint64_t key; uint64_t value; };
static_assert(sizeof(ISSlot) == 16, "");
static const uint32_t IS_HT_SIZE = 1u << 20;
static const uint32_t IS_HT_MASK = IS_HT_SIZE - 1;

// ── Aggregation map slot ──────────────────────────────────────────────────────
// key: (name_code:14b << 38) | (tag_code:18b << 20) | plabel_code:20b
// empty: UINT64_MAX
struct AggSlot { uint64_t key; double sum; int64_t cnt; };
static_assert(sizeof(AggSlot) == 24, "");

// Compact entry (same layout as AggSlot but conceptually separate)
struct AggEntry { uint64_t key; double sum; int64_t cnt; };
static_assert(sizeof(AggEntry) == 24, "");

static const uint64_t EMPTY_KEY  = UINT64_MAX;
static const uint32_t TL_SIZE    = 1u << 18;   // 262144 slots/thread = 6.3MB
static const uint32_t TL_MASK    = TL_SIZE - 1;
// Sub-HT for partitioned merge: 2^13 = 8192 slots x 24B = 192KB/thread (L3-resident)
// 200K groups / 64 threads ≈ 3125 entries → 38% load → avg 1.3 probes
static const uint32_t SUB_HT_SIZE = 1u << 13;
static const uint32_t SUB_HT_MASK = SUB_HT_SIZE - 1;

// Scatter buffer: flat [src][dst][entry], no dynamic allocation.
// SCATTER_BIN_CAP: max AggEntry per (src, dst) bin.
// With ~31K entries/thread and 64 bins: avg 484/bin, 3-sigma max ≈ 550.
// Use 1024 for safety. Memory: 64*64*1024*24 = 96MB (allocated once).
static const size_t SCATTER_BIN_CAP = 1024;

// ── Fast hash mix ─────────────────────────────────────────────────────────────
static inline uint64_t hash64(uint64_t k) noexcept {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return k;
}

// ── IS HT key/value packing ───────────────────────────────────────────────────
static inline uint64_t pack_is_key(int32_t adsh, int32_t tag, int32_t ver) noexcept {
    return ((uint64_t)(uint32_t)adsh << 35) | ((uint64_t)(uint32_t)tag << 17) | (uint64_t)(uint32_t)ver;
}

static inline void is_ht_insert(ISSlot* __restrict__ ht, uint64_t key,
                                  uint32_t row_id, uint32_t plabel_code) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & IS_HT_MASK;
    while (ht[idx].key != EMPTY_KEY && ht[idx].key != key)
        idx = (idx + 1) & IS_HT_MASK;
    if (ht[idx].key == EMPTY_KEY) {
        ht[idx].key   = key;
        ht[idx].value = (1ULL << 44) | ((uint64_t)row_id << 20) | (uint64_t)plabel_code;
    } else {
        if ((ht[idx].value & 0xFFFFFu) == plabel_code)
            ht[idx].value += (1ULL << 44);
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
static inline uint64_t pack_grp_key(uint32_t nc, uint32_t tc, uint32_t pc) noexcept {
    return ((uint64_t)nc << 38) | ((uint64_t)tc << 20) | (uint64_t)pc;
}
static inline uint32_t unpack_name_code  (uint64_t k) noexcept { return (uint32_t)(k >> 38); }
static inline uint32_t unpack_tag_code   (uint64_t k) noexcept { return (uint32_t)((k >> 20) & 0x3FFFFu); }
static inline uint32_t unpack_plabel_code(uint64_t k) noexcept { return (uint32_t)(k & 0xFFFFFu); }

// ── Flat agg map upsert (with multiplier) ────────────────────────────────────
static inline void agg_upsert(AggSlot* __restrict__ slots, uint32_t mask,
                               uint64_t key, double val, int64_t mult) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & mask;
    while (slots[idx].key != EMPTY_KEY && slots[idx].key != key)
        idx = (idx + 1) & mask;
    if (__builtin_expect(slots[idx].key == EMPTY_KEY, 0)) slots[idx].key = key;
    slots[idx].sum += val * (double)mult;
    slots[idx].cnt += mult;
}

// ── Sub-HT upsert (for partitioned merge) ────────────────────────────────────
static inline void sub_ht_upsert(AggSlot* __restrict__ slots,
                                   uint64_t key, double val, int64_t cnt) noexcept {
    uint32_t idx = (uint32_t)hash64(key) & SUB_HT_MASK;
    while (slots[idx].key != EMPTY_KEY && slots[idx].key != key)
        idx = (idx + 1) & SUB_HT_MASK;
    if (__builtin_expect(slots[idx].key == EMPTY_KEY, 0)) slots[idx].key = key;
    slots[idx].sum += val;
    slots[idx].cnt += cnt;
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

    // ── dict_lookup ───────────────────────────────────────────────────────────
    int8_t usd_code_i8, is_code_i8;
    {
        GENDB_PHASE("dict_lookup");
        int32_t uc = dict_lookup_code(G + "/shared/uom.dict",  "USD");
        int32_t ic = dict_lookup_code(G + "/shared/stmt.dict", "IS");
        if (uc < 0) { fprintf(stderr, "USD not found\n"); return 1; }
        if (ic < 0) { fprintf(stderr, "IS not found\n");  return 1; }
        usd_code_i8 = (int8_t)uc;
        is_code_i8  = (int8_t)ic;
        fprintf(stderr, "[info] usd_code=%d is_code=%d\n", (int)usd_code_i8, (int)is_code_i8);
    }

    // ── data_loading: mmap all columns and indexes ────────────────────────────
    size_t sub_fy_sz, sub_name_sz;
    size_t num_uom_sz, num_val_sz, num_adsh_sz, num_tag_sz, num_ver_sz;
    size_t pre_stmt_sz, pre_adsh_sz, pre_tag_sz, pre_ver_sz, plabel_codes_sz;
    size_t num_zone_sz, pre_zone_sz;
    const int16_t*  sub_fy_col;   const int32_t*  sub_name_col;
    const int8_t*   num_uom_col;  const double*   num_val_col;
    const int32_t*  num_adsh_col; const int32_t*  num_tag_col; const int32_t* num_ver_col;
    const int8_t*   pre_stmt_col; const int32_t*  pre_adsh_col;
    const int32_t*  pre_tag_col;  const int32_t*  pre_ver_col;
    const uint32_t* plabel_codes_col;
    const uint8_t*  num_zone_raw; const uint8_t*  pre_zone_raw;

    {
        GENDB_PHASE("data_loading");
        sub_fy_col       = (const int16_t*) mmap_ro(G + "/sub/fy.bin",       sub_fy_sz);
        sub_name_col     = (const int32_t*) mmap_ro(G + "/sub/name.bin",     sub_name_sz);
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
        num_zone_raw = (const uint8_t*)mmap_ro(G + "/indexes/num_zonemaps.bin", num_zone_sz);
        pre_zone_raw = (const uint8_t*)mmap_ro(G + "/indexes/pre_zonemaps.bin", pre_zone_sz);
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

    const uint32_t SUB_N = (uint32_t)(sub_fy_sz / sizeof(int16_t));
    const size_t   NUM_N = num_uom_sz / sizeof(int8_t);
    const uint32_t PRE_N = (uint32_t)(pre_stmt_sz / sizeof(int8_t));
    fprintf(stderr, "[info] SUB_N=%u NUM_N=%zu PRE_N=%u\n", SUB_N, NUM_N, PRE_N);

    // ── thread count: cap at 64 ───────────────────────────────────────────────
    int nthreads = omp_get_max_threads();
    if (nthreads > 64) nthreads = 64;
    const size_t NPART = (size_t)nthreads;
    fprintf(stderr, "[info] nthreads=%d\n", nthreads);

    // ── parallel_preinit ──────────────────────────────────────────────────────
    // Single contiguous allocations initialized in parallel to eliminate
    // ~163ms of single-threaded page faults.
    //
    // Allocations:
    //   is_ht_raw:   IS_HT_SIZE × 16B = 16MB
    //   tl_raw:      nthreads × TL_SIZE × 24B = 64 × 6.3MB = 403MB
    //   scatter_buf: nthreads × nthreads × SCATTER_BIN_CAP × 24B = 96MB
    //   scatter_cnt: nthreads × nthreads × 4B = 16KB (contiguous, fast)
    //
    // Each thread initializes: IS HT slice + TL slice + scatter_buf slice + scatter_cnt slice.
    ISSlot*  is_ht_raw   = new ISSlot[IS_HT_SIZE];
    AggSlot* tl_raw      = new AggSlot[(size_t)nthreads * TL_SIZE];
    AggEntry* scatter_buf = new AggEntry[(size_t)nthreads * NPART * SCATTER_BIN_CAP];
    uint32_t* scatter_cnt = new uint32_t[(size_t)nthreads * NPART];

    {
        GENDB_PHASE("parallel_preinit");
        const size_t is_per_thread  = IS_HT_SIZE / (size_t)nthreads;
        const size_t scat_per_thread = NPART * SCATTER_BIN_CAP; // entries per src row

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();

            // IS HT slice: 0xFF → key = EMPTY_KEY
            const size_t is_lo = (size_t)tid * is_per_thread;
            const size_t is_hi = ((size_t)tid == (size_t)(nthreads-1))
                                 ? (size_t)IS_HT_SIZE : is_lo + is_per_thread;
            memset(is_ht_raw + is_lo, 0xFF, (is_hi - is_lo) * sizeof(ISSlot));

            // TL map slice: key=EMPTY_KEY, sum=0, cnt=0
            AggSlot* my_tl = tl_raw + (size_t)tid * TL_SIZE;
            const AggSlot es{EMPTY_KEY, 0.0, 0LL};
            for (size_t j = 0; j < TL_SIZE; j++) my_tl[j] = es;

            // scatter_buf slice for this src thread: touch pages (writes during compact
            // will find them hot). Just memset to 0; data doesn't matter until written.
            AggEntry* my_scat = scatter_buf + (size_t)tid * scat_per_thread;
            memset(my_scat, 0, scat_per_thread * sizeof(AggEntry));

            // scatter_cnt slice: zero all destination counters
            uint32_t* my_cnt = scatter_cnt + (size_t)tid * NPART;
            memset(my_cnt, 0, NPART * sizeof(uint32_t));
        }
    }

    // ── build_sub_filter ──────────────────────────────────────────────────────
    std::vector<uint8_t>  fy2023(SUB_N, 0);
    std::vector<int32_t>  sub_name_flat(SUB_N);
    {
        GENDB_PHASE("build_sub_filter");
        for (uint32_t i = 0; i < SUB_N; i++) {
            sub_name_flat[i] = sub_name_col[i];
            if (sub_fy_col[i] == (int16_t)2023) fy2023[i] = 1;
        }
    }

    // ── build_is_pre_hashtable ────────────────────────────────────────────────
    // KEY CHANGE: fy2023 filter during build → ~325K rows instead of 1.73M.
    // IS HT at 2^20=16MB: 31% load, avg 1.5 probes. Fits in L3 during scan.
    uint32_t is_count = 0;
    {
        GENDB_PHASE("build_is_pre_hashtable");
        const int32_t pre_nblocks = *(const int32_t*)pre_zone_raw;
        const uint8_t* pzb = pre_zone_raw + 4;
        const uint32_t PRE_BLOCK = 100000;
        ISSlot* ht = is_ht_raw;
        const uint8_t* fy2023_p = fy2023.data();

        for (int32_t b = 0; b < pre_nblocks; b++) {
            const PreZoneBlock* blk = (const PreZoneBlock*)(pzb + (size_t)b * 10);
            if (is_code_i8 < blk->stmt_min || is_code_i8 > blk->stmt_max) continue;
            const uint32_t lo = (uint32_t)b * PRE_BLOCK;
            const uint32_t hi = (lo + PRE_BLOCK < PRE_N) ? lo + PRE_BLOCK : PRE_N;
            for (uint32_t i = lo; i < hi; i++) {
                if (pre_stmt_col[i] != is_code_i8) continue;
                const int32_t adsh_i = pre_adsh_col[i];
                if ((uint32_t)adsh_i >= SUB_N || !fy2023_p[(uint32_t)adsh_i]) continue;
                const uint64_t key = pack_is_key(adsh_i, pre_tag_col[i], pre_ver_col[i]);
                is_ht_insert(ht, key, i, plabel_codes_col[i]);
                is_count++;
            }
        }
        fprintf(stderr, "[info] IS+fy2023 rows inserted: %u\n", is_count);
    }

    madvise(is_ht_raw, IS_HT_SIZE * sizeof(ISSlot), MADV_WILLNEED);

    // ── scan_num_zonemaps ─────────────────────────────────────────────────────
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
        fprintf(stderr, "[info] USD blocks: %zu\n", usd_blocks.size());
    }

    // ── parallel_scan_probe_aggregate (+ compact + partition into scatter_buf) ─
    // After scan, each thread compacts its TL map and scatters entries into
    // scatter_buf[src * NPART * SCATTER_BIN_CAP + dst * SCATTER_BIN_CAP + pos].
    // No dynamic allocation: all memory pre-faulted in parallel_preinit.
    // scatter_cnt[src * NPART + dst] = count of entries in that bin.
    {
        GENDB_PHASE("parallel_scan_probe_aggregate");
        const uint8_t*  fy2023_p  = fy2023.data();
        const int32_t*  sname_p   = sub_name_flat.data();
        const int8_t*   uom_p     = num_uom_col;
        const double*   val_p     = num_val_col;
        const int32_t*  adsh_p    = num_adsh_col;
        const int32_t*  tag_p     = num_tag_col;
        const int32_t*  ver_p     = num_ver_col;
        const ISSlot*   is_ht_p   = is_ht_raw;
        const uint32_t  sub_n     = SUB_N;
        const int       nblocks   = (int)usd_blocks.size();
        const uint64_t  nth       = (uint64_t)nthreads;

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            AggSlot* my_slots = tl_raw + (size_t)tid * TL_SIZE;

            // Morsel-driven scan
            #pragma omp for schedule(dynamic, 1)
            for (int bidx = 0; bidx < nblocks; bidx++) {
                const size_t lo = usd_blocks[bidx].lo;
                const size_t hi = usd_blocks[bidx].hi;
                for (size_t i = lo; i < hi; i++) {
                    if (uom_p[i] != usd_code_i8) continue;
                    const double v = val_p[i];
                    if (std::isnan(v)) continue;
                    const int32_t ac = adsh_p[i];
                    if (ac < 0 || (uint32_t)ac >= sub_n) continue;
                    if (!fy2023_p[(uint32_t)ac]) continue;
                    const int32_t nc = sname_p[(uint32_t)ac];
                    if (nc < 0) continue;
                    const int32_t tc = tag_p[i];
                    const int32_t vc = ver_p[i];
                    const uint64_t is_key = pack_is_key(ac, tc, vc);
                    const uint64_t is_val = is_ht_probe(is_ht_p, is_key);
                    if (is_val == EMPTY_KEY) continue;
                    const uint32_t plabel_code = (uint32_t)(is_val & 0xFFFFFu);
                    const int64_t  mult        = (int64_t)((is_val >> 44) & 0xFFFFu);
                    const uint64_t grp_key = pack_grp_key((uint32_t)nc, (uint32_t)tc, plabel_code);
                    agg_upsert(my_slots, TL_MASK, grp_key, v, mult);
                }
            }
            // Implicit barrier after omp for.

            // Compact TL map into scatter_buf (pre-allocated, no malloc needed).
            // Access pattern: sequential scan of 6.3MB TL slice;
            // writes to my_scatter[dst * SCATTER_BIN_CAP + pos] — 64 active bins,
            // each accessed sequentially → hot in L2.
            AggEntry* __restrict__ my_scat = scatter_buf + (size_t)tid * NPART * SCATTER_BIN_CAP;
            uint32_t* __restrict__ my_cnt  = scatter_cnt + (size_t)tid * NPART;
            for (size_t j = 0; j < TL_SIZE; j++) {
                const AggSlot& s = my_slots[j];
                if (s.key != EMPTY_KEY) {
                    const int dst = (int)(hash64(s.key) % nth);
                    const uint32_t pos = my_cnt[dst]++;
                    // SCATTER_BIN_CAP=1024 >> avg 484 entries/bin; overflow extremely unlikely
                    if (__builtin_expect(pos < SCATTER_BIN_CAP, 1))
                        my_scat[(size_t)dst * SCATTER_BIN_CAP + pos] = {s.key, s.sum, s.cnt};
                }
            }
        } // end parallel scan
    }

    // Free TL maps and IS HT
    delete[] tl_raw;    tl_raw    = nullptr;
    delete[] is_ht_raw; is_ht_raw = nullptr;

    // ── parallel_merge_aggregate ──────────────────────────────────────────────
    // Partitioned merge: thread `part` reads scatter_buf[src * NPART * BIN_CAP + part * BIN_CAP]
    // for each src=0..nthreads-1. Total per thread: ~3K entries = 74KB. L2-resident.
    // Zero contention. Each thread writes only to its private sub-HT.
    AggSlot* sub_hts = new AggSlot[(size_t)nthreads * SUB_HT_SIZE];

    {
        GENDB_PHASE("parallel_merge_aggregate");

        #pragma omp parallel num_threads(nthreads)
        {
            int part = omp_get_thread_num();
            AggSlot* my_sub = sub_hts + (size_t)part * SUB_HT_SIZE;

            // Init sub-HT
            const AggSlot es{EMPTY_KEY, 0.0, 0LL};
            for (size_t j = 0; j < SUB_HT_SIZE; j++) my_sub[j] = es;

            // Read only this partition's bin from each source thread
            for (int src = 0; src < nthreads; src++) {
                const AggEntry* __restrict__ bin =
                    scatter_buf + (size_t)src * NPART * SCATTER_BIN_CAP
                                + (size_t)part * SCATTER_BIN_CAP;
                const uint32_t cnt = scatter_cnt[(size_t)src * NPART + (size_t)part];
                for (uint32_t k = 0; k < cnt; k++)
                    sub_ht_upsert(my_sub, bin[k].key, bin[k].sum, bin[k].cnt);
            }
        } // end parallel merge

        delete[] scatter_buf; scatter_buf = nullptr;
        delete[] scatter_cnt; scatter_cnt = nullptr;
    }

    // ── topk_decode_output ────────────────────────────────────────────────────
    {
        GENDB_PHASE("topk_decode_output");

        // Collect all groups from per-thread sub-HTs
        struct SortRow { uint64_t key; double sum; int64_t cnt; };
        std::vector<SortRow> rows;
        rows.reserve(220000);
        size_t total_groups = 0;

        for (int t = 0; t < nthreads; t++) {
            const AggSlot* my_sub = sub_hts + (size_t)t * SUB_HT_SIZE;
            for (size_t j = 0; j < SUB_HT_SIZE; j++) {
                if (my_sub[j].key != EMPTY_KEY) {
                    rows.push_back({my_sub[j].key, my_sub[j].sum, my_sub[j].cnt});
                    total_groups++;
                }
            }
        }
        delete[] sub_hts; sub_hts = nullptr;
        fprintf(stderr, "[info] unique groups: %zu\n", total_groups);

        // partial_sort top-200 by total_value DESC
        {
            GENDB_PHASE("topk_sort");
            const size_t K = std::min((size_t)200, rows.size());
            std::partial_sort(rows.begin(), rows.begin() + K, rows.end(),
                [](const SortRow& a, const SortRow& b) { return a.sum > b.sum; });
            rows.resize(K);
        }

        // Load decode dictionaries
        size_t name_dict_sz, tag_dict_sz, plabel_off_sz, plabel_data_sz;
        const uint8_t* name_dict_raw = mmap_ro(G + "/sub/name.dict",          name_dict_sz);
        const uint8_t* tag_dict_raw  = mmap_ro(G + "/shared/tag_numpre.dict", tag_dict_sz);
        const uint64_t* plabel_dict_off  = (const uint64_t*)mmap_ro(
            G + "/column_versions/pre.plabel.dict/dict.offsets", plabel_off_sz);
        const char*     plabel_dict_data = (const char*)mmap_ro(
            G + "/column_versions/pre.plabel.dict/dict.data",    plabel_data_sz);

        const auto name_sv   = build_dict_sv(name_dict_raw);
        const auto tag_sv    = build_dict_sv(tag_dict_raw);
        const uint32_t plabel_n = (uint32_t)(plabel_off_sz / sizeof(uint64_t)) - 1;

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
