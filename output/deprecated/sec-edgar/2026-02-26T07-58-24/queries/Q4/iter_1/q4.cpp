// Q4 — SEC EDGAR (iter_1)
// SELECT s.sic, t.tlabel, p.stmt,
//        COUNT(DISTINCT s.cik) AS num_companies,
//        SUM(n.value) AS total_value,
//        AVG(n.value) AS avg_value
// FROM num n
// JOIN sub s ON n.adsh = s.adsh
// JOIN tag t ON n.tag = t.tag AND n.version = t.version
// JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
// WHERE n.uom = 'USD' AND p.stmt = 'EQ'
//       AND s.sic BETWEEN 4000 AND 4999
//       AND n.value IS NOT NULL AND t.abstract = 0
// GROUP BY s.sic, t.tlabel, p.stmt
// HAVING COUNT(DISTINCT s.cik) >= 2
// ORDER BY total_value DESC LIMIT 500;
//
// Strategy (iter_1):
//   - Eliminate build_tag_join_map (was 1175ms bottleneck)
//   - Build pre_eq_sub_set: scan pre once for EQ+sub_valid rows (~18ms)
//     Compact flat hash set (131072 entries x 8B = 1 MB, L2-resident)
//   - During num scan: probe pre_eq_sub_set O(1) instead of binary search into 154MB
//   - Defer tag abstract/tlabel resolution to output stage (few K probes)

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>

#include "timing_utils.h"

// ============================================================
// Constants
// ============================================================
static constexpr int32_t SUB_N       = 86135;
static constexpr int64_t NUM_ROWS    = 39401761;
static constexpr int32_t BLOCK_SIZE  = 100000;

// pre_eq_sub_set: flat open-addressing hash set
// key = adsh_code<<36 | tag_code<<18 | ver_code (packed uint64_t)
// adsh_code: 17 bits (< 86135), tag_code: 18 bits (< 198312), ver_code: 18 bits (< 83816)
// bit layout: [53..36]=adsh(18 alloc), [35..18]=tag(18), [17..0]=ver(18) -- no overlap
static constexpr uint32_t PRE_SET_CAP  = 131072u; // next_pow2(64656*2)
static constexpr uint32_t PRE_SET_MASK = PRE_SET_CAP - 1u;
static constexpr uint64_t PRE_SET_EMPTY = UINT64_MAX;

// sub_valid bitset words
static constexpr int32_t BITSET_WORDS = (SUB_N + 63) / 64; // 1346

// ============================================================
// Static global data (avoids stack overflow for large arrays)
// ============================================================
static uint64_t g_sub_valid[BITSET_WORDS]; // 10.5 KB -- L1-resident
static int16_t  g_sub_sic[SUB_N];          // 168 KB
static int32_t  g_sub_cik[SUB_N];          // 336 KB
static uint64_t g_pre_set[PRE_SET_CAP];    // 1 MB -- L2-resident
static uint16_t g_pre_cnt[PRE_SET_CAP];    // 256 KB -- count of EQ pre rows per key

// ============================================================
// mmap helper
// ============================================================
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
    out_size = (size_t)st.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

// Binary dict loader: [n:uint32][len:uint16, bytes...]*n
static std::vector<std::string> load_binary_dict(const std::string& path) {
    size_t sz;
    const uint8_t* d = (const uint8_t*)mmap_file(path, sz);
    uint32_t n = *(const uint32_t*)d;
    std::vector<std::string> dict;
    dict.reserve(n);
    size_t off = 4;
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = *(const uint16_t*)(d + off); off += 2;
        dict.emplace_back((const char*)(d + off), len); off += len;
    }
    return dict;
}

// ============================================================
// pre_eq_sub_set -- flat open-addressing hash set (linear probing)
// ============================================================
static inline uint32_t pre_set_slot(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    return (uint32_t)key & PRE_SET_MASK;
}

// Insert key or increment its count (count = number of EQ pre rows with this key)
static inline void pre_set_insert(uint64_t key) {
    uint32_t pos = pre_set_slot(key);
    for (uint32_t i = 0; i < PRE_SET_CAP; i++) {
        uint64_t cur = g_pre_set[pos];
        if (cur == PRE_SET_EMPTY) {
            g_pre_set[pos] = key;
            g_pre_cnt[pos] = 1;
            return;
        }
        if (cur == key) {
            g_pre_cnt[pos]++;
            return;
        }
        pos = (pos + 1) & PRE_SET_MASK;
    }
}

// Returns count of EQ pre rows for this (adsh,tag,ver) key, 0 if not found
static inline uint32_t pre_set_get_count(uint64_t key) {
    uint32_t pos = pre_set_slot(key);
    for (uint32_t i = 0; i < PRE_SET_CAP; i++) {
        uint64_t cur = g_pre_set[pos];
        if (cur == PRE_SET_EMPTY) return 0;
        if (cur == key) return g_pre_cnt[pos];
        pos = (pos + 1) & PRE_SET_MASK;
    }
    return 0;
}

// ============================================================
// FNV-64a hash (for tag_pk_hash probing at output stage)
// ============================================================
static inline uint64_t fnv64(const char* data, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint8_t)data[i];
        h *= 1099511628211ULL;
    }
    return h ? h : 1;
}

// ============================================================
// Struct definitions
// ============================================================

// tag_pk_hash slot (from build_indexes.cpp, #pragma pack(push,1))
#pragma pack(push, 1)
struct TagHashSlot {
    uint64_t key_hash;
    int32_t  row_id;  // INT32_MIN = empty
    int32_t  _pad;
};
#pragma pack(pop)
static_assert(sizeof(TagHashSlot) == 16, "TagHashSlot must be 16 bytes");

// num zone map entry (from build_indexes.cpp)
#pragma pack(push, 1)
struct NumZoneEntry {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};
#pragma pack(pop)
static_assert(sizeof(NumZoneEntry) == 10, "NumZoneEntry must be 10 bytes");

// CIK entry for sort-based distinct counting
struct CIKEntry {
    uint64_t group_key;
    int32_t  cik;
    int32_t  _pad;
};

// Per-group aggregation accumulator
struct AggAccum {
    double  sum_val = 0.0;
    int64_t cnt     = 0;
};

// ============================================================
// CSV field writer with RFC-4180 quoting
// ============================================================
static void write_csv_field(FILE* fp, const char* data, size_t len) {
    bool needs_quote = false;
    for (size_t i = 0; i < len; i++) {
        char c = data[i];
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quote = true; break;
        }
    }
    if (needs_quote) {
        fputc('"', fp);
        for (size_t i = 0; i < len; i++) {
            if (data[i] == '"') fputc('"', fp);
            fputc(data[i], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(data, 1, len, fp);
    }
}

// ============================================================
// Main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // --------------------------------------------------------
    // Phase: data_loading
    // Mmap num columns + load shared dicts
    // --------------------------------------------------------
    std::vector<std::string> uom_dict, stmt_dict, tag_dict, ver_dict;
    const int8_t*  num_uom  = nullptr;
    const double*  num_val  = nullptr;
    const int32_t* num_adsh = nullptr;
    const int32_t* num_tag  = nullptr;
    const int32_t* num_ver  = nullptr;
    int8_t  usd_code = -1;
    int8_t  eq_code  = -1;
    int64_t num_N    = 0;
    size_t  num_uom_sz, num_val_sz, num_adsh_sz, num_tag_sz, num_ver_sz;

    {
        GENDB_PHASE("data_loading");

        uom_dict  = load_binary_dict(gendb_dir + "/shared/uom.dict");
        stmt_dict = load_binary_dict(gendb_dir + "/shared/stmt.dict");
        tag_dict  = load_binary_dict(gendb_dir + "/shared/tag_numpre.dict");
        ver_dict  = load_binary_dict(gendb_dir + "/shared/version_numpre.dict");

        for (size_t i = 0; i < uom_dict.size(); i++)
            if (uom_dict[i] == "USD") { usd_code = (int8_t)i; break; }
        for (size_t i = 0; i < stmt_dict.size(); i++)
            if (stmt_dict[i] == "EQ")  { eq_code  = (int8_t)i; break; }

        if (usd_code < 0 || eq_code < 0) {
            fprintf(stderr, "FATAL: USD or EQ code not found in dicts\n");
            return 1;
        }

        num_uom  = (const int8_t* )mmap_file(gendb_dir + "/num/uom.bin",     num_uom_sz);
        num_val  = (const double*  )mmap_file(gendb_dir + "/num/value.bin",   num_val_sz);
        num_adsh = (const int32_t* )mmap_file(gendb_dir + "/num/adsh.bin",    num_adsh_sz);
        num_tag  = (const int32_t* )mmap_file(gendb_dir + "/num/tag.bin",     num_tag_sz);
        num_ver  = (const int32_t* )mmap_file(gendb_dir + "/num/version.bin", num_ver_sz);

        num_N = (int64_t)(num_uom_sz / sizeof(int8_t));

        madvise((void*)num_uom,  num_uom_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_val,  num_val_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_adsh, num_adsh_sz, MADV_SEQUENTIAL);
        madvise((void*)num_tag,  num_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_ver,  num_ver_sz,  MADV_SEQUENTIAL);
    }

    // --------------------------------------------------------
    // Phase: build_sub_arrays
    // Populate sub_valid bitset, sub_sic_arr, sub_cik_arr
    // --------------------------------------------------------
    {
        GENDB_PHASE("build_sub_arrays");

        size_t sic_sz, cik_sz;
        const int16_t* sub_sic_col = (const int16_t*)mmap_file(gendb_dir + "/sub/sic.bin", sic_sz);
        const int32_t* sub_cik_col = (const int32_t*)mmap_file(gendb_dir + "/sub/cik.bin", cik_sz);

        int32_t sub_rows = (int32_t)(sic_sz / sizeof(int16_t));
        if (sub_rows > SUB_N) sub_rows = SUB_N;

        memset(g_sub_valid, 0, sizeof(g_sub_valid));

        for (int32_t i = 0; i < sub_rows; i++) {
            int16_t sic = sub_sic_col[i];
            g_sub_sic[i] = sic;
            g_sub_cik[i] = sub_cik_col[i];
            if (sic >= 4000 && sic <= 4999) {
                g_sub_valid[i >> 6] |= (1ULL << (i & 63));
            }
        }
    }

    // --------------------------------------------------------
    // Phase: build_pre_eq_sub_set
    // Scan pre sequentially: keep rows where stmt==EQ AND adsh_code in sub_valid
    // Pack key = adsh_code<<36 | tag_code<<18 | ver_code (no bit overlap)
    // Insert into flat open-addressing hash set (1 MB, L2-resident)
    // Replaces 154 MB pre_key_sorted binary search with O(1) probe
    // --------------------------------------------------------
    {
        GENDB_PHASE("build_pre_eq_sub_set");

        // Initialize hash set with empty sentinels and zero counts
        for (uint32_t i = 0; i < PRE_SET_CAP; i++) { g_pre_set[i] = PRE_SET_EMPTY; g_pre_cnt[i] = 0; }

        size_t pre_stmt_sz, pre_adsh_sz, pre_tag_sz, pre_ver_sz;
        const int8_t*  pre_stmt = (const int8_t* )mmap_file(gendb_dir + "/pre/stmt.bin",    pre_stmt_sz);
        const int32_t* pre_adsh = (const int32_t*)mmap_file(gendb_dir + "/pre/adsh.bin",    pre_adsh_sz);
        const int32_t* pre_tag  = (const int32_t*)mmap_file(gendb_dir + "/pre/tag.bin",     pre_tag_sz);
        const int32_t* pre_ver  = (const int32_t*)mmap_file(gendb_dir + "/pre/version.bin", pre_ver_sz);

        madvise((void*)pre_stmt, pre_stmt_sz, MADV_SEQUENTIAL);
        madvise((void*)pre_adsh, pre_adsh_sz, MADV_SEQUENTIAL);
        madvise((void*)pre_tag,  pre_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)pre_ver,  pre_ver_sz,  MADV_SEQUENTIAL);

        int64_t pre_N = (int64_t)(pre_stmt_sz / sizeof(int8_t));

        for (int64_t i = 0; i < pre_N; i++) {
            if (pre_stmt[i] != eq_code) continue;

            int32_t adsh_c = pre_adsh[i];
            if ((uint32_t)adsh_c >= (uint32_t)SUB_N) continue;
            if (!(g_sub_valid[adsh_c >> 6] & (1ULL << (adsh_c & 63)))) continue;

            // Pack: adsh bits 36-52, tag bits 18-35, ver bits 0-17
            uint64_t key = ((uint64_t)(uint32_t)adsh_c << 36) |
                           ((uint64_t)(uint32_t)pre_tag[i] << 18) |
                           ((uint64_t)(uint32_t)pre_ver[i]);
            pre_set_insert(key);
        }
    }

    // --------------------------------------------------------
    // Load num_zonemaps for block-level USD filtering
    // --------------------------------------------------------
    size_t zm_sz;
    const uint8_t* zm_raw = (const uint8_t*)mmap_file(gendb_dir + "/indexes/num_zonemaps.bin", zm_sz);
    int32_t n_zm_blocks = *(const int32_t*)zm_raw;
    const NumZoneEntry* zonemaps = (const NumZoneEntry*)(zm_raw + 4);

    // --------------------------------------------------------
    // Phase: main_scan (parallel morsel-driven over num blocks)
    // Inner loop filters: uom + sub_valid bitset + pre_eq_sub_set
    // Accumulate: group_key -> (sum_val, cnt) + CIK entries for distinct count
    // --------------------------------------------------------
    int nthreads = omp_get_max_threads();

    std::vector<std::unordered_map<uint64_t, AggAccum>> tl_agg(nthreads);
    std::vector<std::vector<CIKEntry>>                   tl_cik(nthreads);

    for (int t = 0; t < nthreads; t++) {
        tl_agg[t].reserve(8192);
        tl_cik[t].reserve(16384);
    }

    std::atomic<int32_t> block_counter{0};

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_agg = tl_agg[tid];
            auto& local_cik = tl_cik[tid];

            while (true) {
                int32_t b = block_counter.fetch_add(1, std::memory_order_relaxed);
                if (b >= n_zm_blocks) break;

                const NumZoneEntry& zm = zonemaps[b];

                // Zone map: skip blocks with no USD rows.
                // num sorted by uom; USD=0. If uom_min > usd_code, no USD rows in block.
                if (zm.uom_min > usd_code) continue;

                bool all_usd = (zm.uom_min == usd_code && zm.uom_max == usd_code);

                int64_t row_start = (int64_t)b * BLOCK_SIZE;
                int64_t row_end   = std::min(row_start + (int64_t)BLOCK_SIZE, num_N);

                for (int64_t i = row_start; i < row_end; i++) {
                    // Filter 1: uom == USD
                    if (!all_usd && num_uom[i] != usd_code) continue;

                    // Filter 2: value IS NOT NULL (NaN sentinel)
                    double v = num_val[i];
                    if (__builtin_expect(std::isnan(v), 0)) continue;

                    // Filter 3: adsh_code in sub_valid bitset (sic in [4000,4999])
                    int32_t adsh_c = num_adsh[i];
                    if ((uint32_t)adsh_c >= (uint32_t)SUB_N) continue;
                    if (!(g_sub_valid[adsh_c >> 6] & (1ULL << (adsh_c & 63)))) continue;

                    int32_t tag_c = num_tag[i];
                    int32_t ver_c = num_ver[i];

                    // Filter 4: probe pre_eq_sub_set (EQ + sub_valid)
                    // O(1) with ~1.25MB L2-resident hash set -- replaces 154MB binary search
                    // Returns count of EQ pre rows for this (adsh,tag,ver) key
                    // SQL semantics: JOIN multiplies rows, so sum and count by pre_cnt
                    uint64_t pre_key = ((uint64_t)(uint32_t)adsh_c << 36) |
                                       ((uint64_t)(uint32_t)tag_c << 18) |
                                       ((uint64_t)(uint32_t)ver_c);
                    uint32_t pre_cnt = pre_set_get_count(pre_key);
                    if (pre_cnt == 0) continue;

                    // Aggregate: group key = sic<<48 | tag_code<<18 | ver_code
                    // Bit layout: [63..48]=sic(16b), [47..36]=gap, [35..18]=tag(18b), [17..0]=ver(18b)
                    int16_t  sic = g_sub_sic[adsh_c];
                    uint64_t gk  = ((uint64_t)(uint16_t)sic << 48) |
                                   ((uint64_t)(uint32_t)tag_c << 18) |
                                   ((uint64_t)(uint32_t)ver_c);

                    auto& acc = local_agg[gk];
                    acc.sum_val += v * (double)pre_cnt; // each pre row contributes value once
                    acc.cnt     += (int64_t)pre_cnt;    // for AVG denominator

                    // COUNT(DISTINCT cik): distinct per num row (not per pre row)
                    int32_t cik = g_sub_cik[adsh_c];
                    local_cik.push_back({gk, cik, 0});
                }
            }
        }
    }

    // --------------------------------------------------------
    // Phase: merge_aggregates
    // Union thread-local agg maps + sort CIK entries for distinct counting
    // --------------------------------------------------------
    std::unordered_map<uint64_t, AggAccum> global_agg;
    global_agg.reserve(16384);

    std::vector<CIKEntry> all_cik;

    {
        GENDB_PHASE("merge_aggregates");

        for (int t = 0; t < nthreads; t++) {
            for (auto& [k, v] : tl_agg[t]) {
                auto& g = global_agg[k];
                g.sum_val += v.sum_val;
                g.cnt     += v.cnt;
            }
        }

        size_t total_cik = 0;
        for (int t = 0; t < nthreads; t++) total_cik += tl_cik[t].size();
        all_cik.reserve(total_cik);
        for (int t = 0; t < nthreads; t++)
            all_cik.insert(all_cik.end(), tl_cik[t].begin(), tl_cik[t].end());

        // Sort by (group_key, cik) for distinct CIK counting
        std::sort(all_cik.begin(), all_cik.end(),
            [](const CIKEntry& a, const CIKEntry& b) {
                if (a.group_key != b.group_key) return a.group_key < b.group_key;
                return a.cik < b.cik;
            });
    }

    // Count distinct CIKs per original group_key (before tlabel merge)
    std::unordered_map<uint64_t, int64_t> gk_cik_count;
    gk_cik_count.reserve(global_agg.size() * 2);
    if (!all_cik.empty()) {
        uint64_t cur_gk  = all_cik[0].group_key;
        int32_t  prev_ck = INT32_MIN;
        int64_t  cnt     = 0;
        for (const CIKEntry& e : all_cik) {
            if (e.group_key != cur_gk) {
                gk_cik_count[cur_gk] = cnt;
                cur_gk  = e.group_key;
                prev_ck = INT32_MIN;
                cnt     = 0;
            }
            if (e.cik != prev_ck) { cnt++; prev_ck = e.cik; }
        }
        gk_cik_count[cur_gk] = cnt;
    }

    // --------------------------------------------------------
    // Phase: resolve_tlabels_and_abstract
    // For each aggregation group (tag_code, ver_code):
    //   1. Look up tag string and ver string from shared dicts
    //   2. Compute combined FNV hash matching build_indexes.cpp
    //   3. Probe tag_pk_hash -> row_id
    //   4. Check abstract[row_id] == 0
    //   5. Read tlabel string
    //   6. Re-key groups by (sic, tlabel) and merge
    // Only few K probes -- negligible cost compared to scan
    // --------------------------------------------------------
    size_t tph_sz, tag_abs_sz, tag_tl_off_sz, tag_tl_dat_sz;
    const uint8_t*  tph_raw      = (const uint8_t* )mmap_file(gendb_dir + "/indexes/tag_pk_hash.bin", tph_sz);
    const int8_t*   tag_abstract = (const int8_t*  )mmap_file(gendb_dir + "/tag/abstract.bin",        tag_abs_sz);
    const int64_t*  tag_tl_offs  = (const int64_t* )mmap_file(gendb_dir + "/tag/tlabel.offsets",      tag_tl_off_sz);
    const char*     tag_tl_data  = (const char*    )mmap_file(gendb_dir + "/tag/tlabel.data",         tag_tl_dat_sz);

    uint32_t tph_cap  = *(const uint32_t*)(tph_raw);
    // uint32_t tph_entries = *(const uint32_t*)(tph_raw + 4); // not needed
    const TagHashSlot* tph_slots = (const TagHashSlot*)(tph_raw + 8);
    uint32_t tph_mask = tph_cap - 1;

    // Final groups: (sic, tlabel) -> accumulated (sum, cnt, num_companies)
    struct FinalGroup {
        int16_t     sic;
        std::string tlabel;
        double      sum_val;
        int64_t     cnt;
        int64_t     num_companies;
    };

    std::vector<FinalGroup>               fg_arr;
    std::unordered_map<uint64_t, int32_t> gk_to_fgid;      // orig group_key -> final group id
    std::unordered_map<std::string, int32_t> tlabel_to_fgid; // (sic,tlabel) key -> final group id

    fg_arr.reserve(global_agg.size());
    gk_to_fgid.reserve(global_agg.size() * 2);
    tlabel_to_fgid.reserve(global_agg.size() * 2);

    {
        GENDB_PHASE("resolve_tlabels_and_abstract");

        for (auto& [gk, acc] : global_agg) {
            // Extract sic, tag_code, ver_code from group key
            int16_t sic   = (int16_t)(uint16_t)(gk >> 48);
            int32_t tag_c = (int32_t)((gk >> 18) & 0x3FFFFu); // 18 bits
            int32_t ver_c = (int32_t)(gk & 0x3FFFFu);          // 18 bits

            // Bounds check
            if ((uint32_t)tag_c >= (uint32_t)tag_dict.size() ||
                (uint32_t)ver_c >= (uint32_t)ver_dict.size()) {
                gk_to_fgid[gk] = -1;
                continue;
            }

            const std::string& tag_str = tag_dict[(size_t)tag_c];
            const std::string& ver_str = ver_dict[(size_t)ver_c];

            // Compute combined hash (matches build_indexes.cpp hash function)
            uint64_t h1 = fnv64(tag_str.data(), tag_str.size());
            uint64_t h2 = fnv64(ver_str.data(), ver_str.size());
            uint64_t kh = h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
            if (!kh) kh = 1;

            // Probe tag_pk_hash (linear probing, sentinel = INT32_MIN row_id)
            int32_t row_id = INT32_MIN;
            uint32_t pos = (uint32_t)(kh & (uint64_t)tph_mask);
            for (uint32_t probe = 0; probe < tph_cap; probe++) {
                uint32_t slot = (pos + probe) & tph_mask;
                if (tph_slots[slot].row_id == INT32_MIN) break; // empty slot
                if (tph_slots[slot].key_hash == kh) {
                    row_id = tph_slots[slot].row_id;
                    break;
                }
            }

            if (row_id == INT32_MIN) {
                gk_to_fgid[gk] = -1; // (tag, version) not in tag table
                continue;
            }

            // Check abstract == 0 (plan notes: eliminates 0 rows in practice)
            if (tag_abstract[row_id] != 0) {
                gk_to_fgid[gk] = -1;
                continue;
            }

            // Read tlabel string (varlen)
            int64_t tl0 = tag_tl_offs[row_id];
            int64_t tl1 = tag_tl_offs[row_id + 1];
            std::string tlabel(tag_tl_data + tl0, (size_t)(tl1 - tl0));

            // Build final group key: binary sic (2 bytes) + separator + tlabel
            char sic_buf[3];
            memcpy(sic_buf, &sic, 2);
            sic_buf[2] = '\x01'; // separator char unlikely to appear in tlabel
            std::string fg_key(sic_buf, 3);
            fg_key += tlabel;

            // Get or create final group
            auto [it, inserted] = tlabel_to_fgid.emplace(fg_key, (int32_t)fg_arr.size());
            int32_t fgid = it->second;
            if (inserted) {
                fg_arr.push_back(FinalGroup{sic, std::move(tlabel), 0.0, 0LL, 0LL});
            }

            gk_to_fgid[gk] = fgid;

            fg_arr[(size_t)fgid].sum_val += acc.sum_val;
            fg_arr[(size_t)fgid].cnt     += acc.cnt;
        }
    }

    // Re-count distinct CIKs per final group
    // Handles case where multiple (tag_code, ver_code) map to same (sic, tlabel)
    {
        struct FGCIKEntry { int32_t fgid; int32_t cik; };
        std::vector<FGCIKEntry> fg_cik;
        fg_cik.reserve(all_cik.size());

        for (const CIKEntry& e : all_cik) {
            auto it = gk_to_fgid.find(e.group_key);
            if (it == gk_to_fgid.end() || it->second < 0) continue;
            fg_cik.push_back({it->second, e.cik});
        }

        std::sort(fg_cik.begin(), fg_cik.end(),
            [](const FGCIKEntry& a, const FGCIKEntry& b) {
                if (a.fgid != b.fgid) return a.fgid < b.fgid;
                return a.cik < b.cik;
            });

        if (!fg_cik.empty()) {
            int32_t cur_fgid = fg_cik[0].fgid;
            int32_t prev_cik = INT32_MIN;
            int64_t cnt      = 0;
            for (const FGCIKEntry& e : fg_cik) {
                if (e.fgid != cur_fgid) {
                    fg_arr[(size_t)cur_fgid].num_companies = cnt;
                    cur_fgid = e.fgid;
                    prev_cik = INT32_MIN;
                    cnt      = 0;
                }
                if (e.cik != prev_cik) { cnt++; prev_cik = e.cik; }
            }
            fg_arr[(size_t)cur_fgid].num_companies = cnt;
        }
    }

    // --------------------------------------------------------
    // Phase: having_filter
    // Keep groups with COUNT(DISTINCT cik) >= 2
    // --------------------------------------------------------
    std::vector<int32_t> result_ids;
    {
        GENDB_PHASE("having_filter");
        result_ids.reserve(fg_arr.size());
        for (int32_t i = 0; i < (int32_t)fg_arr.size(); i++) {
            if (fg_arr[(size_t)i].num_companies >= 2) result_ids.push_back(i);
        }
    }

    // --------------------------------------------------------
    // Phase: topk_sort
    // Partial sort top 500 by total_value DESC
    // --------------------------------------------------------
    {
        GENDB_PHASE("topk_sort");
        size_t k = std::min((size_t)500, result_ids.size());
        std::partial_sort(result_ids.begin(), result_ids.begin() + (ptrdiff_t)k, result_ids.end(),
            [&](int32_t a, int32_t b) {
                return fg_arr[(size_t)a].sum_val > fg_arr[(size_t)b].sum_val;
            });
        result_ids.resize(k);
    }

    // --------------------------------------------------------
    // Phase: decode_output
    // Write CSV: sic, tlabel, stmt, num_companies, total_value, avg_value
    // --------------------------------------------------------
    {
        GENDB_PHASE("decode_output");

        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q4.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return 1; }

        fprintf(fp, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");

        for (int32_t fgid : result_ids) {
            const FinalGroup& fg = fg_arr[(size_t)fgid];
            double avg = (fg.cnt > 0) ? fg.sum_val / (double)fg.cnt : 0.0;

            fprintf(fp, "%d,", (int)fg.sic);
            write_csv_field(fp, fg.tlabel.data(), fg.tlabel.size());
            fprintf(fp, ",EQ,%lld,%.2f,%.2f\n",
                    (long long)fg.num_companies,
                    fg.sum_val,
                    avg);
        }

        fclose(fp);
    }

    return 0;
}
