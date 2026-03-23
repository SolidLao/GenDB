// Q6: SEC-Edgar
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
// Strategy:
//   1. Build fy2023[86135] bool array from sub/fy.bin (int16_t)
//   2. Load sub/name.bin (int32_t) for name_code lookup
//   3. Scan num with num_zonemaps to skip non-USD blocks
//   4. Per row: uom==USD + !isnan(value) + fy2023[adsh_code]
//   5. Binary search pre_key_sorted for (adsh_code, tag_code, ver_code)
//   6. Check pre/stmt.bin[row_id] == is_code
//   7. Aggregate in thread-local hash maps; merge; secondary string group; top-200

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <climits>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const uint8_t* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    if (out_sz == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return (const uint8_t*)p;
}
static void munmap_ro(const void* p, size_t sz) { if (p && sz) munmap((void*)p, sz); }

// ─── Dict lookup: [uint32_t n][uint16_t len, bytes...] * n ──────────────────
// Returns the 0-based index (code) of the target string, UINT32_MAX if not found
static uint32_t dict_lookup(const std::string& path, const char* target) {
    size_t sz;
    const uint8_t* d = mmap_ro(path, sz);
    uint32_t count = *(const uint32_t*)d;
    size_t off = 4;
    size_t tlen = strlen(target);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = *(const uint16_t*)(d + off); off += 2;
        if ((size_t)len == tlen && memcmp(d + off, target, len) == 0) {
            munmap_ro(d, sz); return i;
        }
        off += len;
    }
    munmap_ro(d, sz); return UINT32_MAX;
}

// ─── Build string_view array from dict ──────────────────────────────────────
static std::vector<std::string_view> build_dict_sv(const uint8_t* d, size_t sz) {
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

// ─── PreKeyEntry (sorted index) ──────────────────────────────────────────────
#pragma pack(push,1)
struct PreKeyEntry {
    int32_t adsh, tag, ver, row_id;
};
#pragma pack(pop)
static_assert(sizeof(PreKeyEntry) == 16, "PreKeyEntry must be 16 bytes");

// ─── NumZoneBlock: 10 bytes packed ──────────────────────────────────────────
#pragma pack(push,1)
struct NumZoneBlock {
    int8_t  uom_min, uom_max;
    int32_t ddate_min, ddate_max;
};
#pragma pack(pop)
static_assert(sizeof(NumZoneBlock) == 10, "NumZoneBlock must be 10 bytes");

// ─── Key packing: name_code(14b) | tag_code(18b) | pre_row(24b) → 56 bits ──
// name_code max 9645  → 14 bits (2^14=16384)
// tag_code  max 198311 → 18 bits (2^18=262144)
// pre_row   max 9600799 → 24 bits (2^24=16777216)
static inline uint64_t pack_key(uint32_t name_code, uint32_t tag_code, uint32_t pre_row) {
    return ((uint64_t)name_code << 42) | ((uint64_t)tag_code << 24) | (uint64_t)pre_row;
}
static inline uint32_t unpack_name_code(uint64_t k) { return (uint32_t)(k >> 42); }
static inline uint32_t unpack_tag_code (uint64_t k) { return (uint32_t)((k >> 24) & 0x3FFFFu); }
static inline uint32_t unpack_pre_row  (uint64_t k) { return (uint32_t)(k & 0xFFFFFFu); }

// ─── CSV field writer ─────────────────────────────────────────────────────────
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

// ─── Aggregation value ───────────────────────────────────────────────────────
struct AggVal { double sum; int64_t cnt; };

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    const std::string G = argv[1];
    const std::string R = argv[2];

    // Create results directory
    mkdir(R.c_str(), 0755);

    // ─── Phase 1: Resolve dict codes ─────────────────────────────────────────
    int8_t usd_code_i8, is_code_i8;
    {
        GENDB_PHASE("dict_lookup");
        uint32_t uc = dict_lookup(G + "/shared/uom.dict",  "USD");
        uint32_t ic = dict_lookup(G + "/shared/stmt.dict", "IS");
        if (uc == UINT32_MAX) { fprintf(stderr, "USD not found in uom.dict\n"); return 1; }
        if (ic == UINT32_MAX) { fprintf(stderr, "IS not found in stmt.dict\n"); return 1; }
        usd_code_i8 = (int8_t)uc;
        is_code_i8  = (int8_t)ic;
        fprintf(stderr, "[info] usd_code=%d is_code=%d\n", (int)usd_code_i8, (int)is_code_i8);
    }

    // ─── Phase 2: Load data ───────────────────────────────────────────────────
    size_t sub_fy_sz, sub_name_sz;
    size_t num_uom_sz, num_val_sz, num_adsh_sz, num_tag_sz, num_ver_sz;
    size_t pre_stmt_sz;
    size_t num_zone_sz;

    const int16_t*     sub_fy_col;
    const int32_t*     sub_name_col;
    const int8_t*      num_uom_col;
    const double*      num_val_col;
    const int32_t*     num_adsh_col;
    const int32_t*     num_tag_col;
    const int32_t*     num_ver_col;
    const int8_t*      pre_stmt_col;
    const uint8_t*     num_zone_raw;
    const PreKeyEntry* pre_key_entries;
    uint32_t           pre_key_n;

    {
        GENDB_PHASE("data_loading");

        sub_fy_col   = (const int16_t*)mmap_ro(G + "/sub/fy.bin",          sub_fy_sz);
        sub_name_col = (const int32_t*)mmap_ro(G + "/sub/name.bin",        sub_name_sz);

        num_uom_col  = (const int8_t*) mmap_ro(G + "/num/uom.bin",         num_uom_sz);
        num_val_col  = (const double*) mmap_ro(G + "/num/value.bin",        num_val_sz);
        num_adsh_col = (const int32_t*)mmap_ro(G + "/num/adsh.bin",        num_adsh_sz);
        num_tag_col  = (const int32_t*)mmap_ro(G + "/num/tag.bin",         num_tag_sz);
        num_ver_col  = (const int32_t*)mmap_ro(G + "/num/version.bin",     num_ver_sz);

        pre_stmt_col = (const int8_t*) mmap_ro(G + "/pre/stmt.bin",        pre_stmt_sz);

        num_zone_raw = (const uint8_t*)mmap_ro(G + "/indexes/num_zonemaps.bin", num_zone_sz);

        // pre_key_sorted: [uint32_t n][PreKeyEntry * n]
        size_t pks_sz;
        const uint8_t* pks_raw = mmap_ro(G + "/indexes/pre_key_sorted.bin", pks_sz);
        pre_key_n       = *(const uint32_t*)pks_raw;
        pre_key_entries = (const PreKeyEntry*)(pks_raw + 4);
        madvise((void*)pre_key_entries, pre_key_n * sizeof(PreKeyEntry), MADV_WILLNEED);

        // Sequential access hints for num columns
        madvise((void*)num_uom_col,  num_uom_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_val_col,  num_val_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_adsh_col, num_adsh_sz, MADV_SEQUENTIAL);
        madvise((void*)num_tag_col,  num_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_ver_col,  num_ver_sz,  MADV_SEQUENTIAL);
        // pre/stmt.bin is small (9.6MB) — warm it into L3
        madvise((void*)pre_stmt_col, pre_stmt_sz, MADV_WILLNEED);
    }

    const uint32_t SUB_N = (uint32_t)(sub_fy_sz / sizeof(int16_t));   // 86135
    const size_t   NUM_N = num_uom_sz / sizeof(int8_t);               // 39401761
    const uint32_t PRE_N = (uint32_t)(pre_stmt_sz / sizeof(int8_t));  // 9600799

    fprintf(stderr, "[info] SUB_N=%u NUM_N=%zu PRE_N=%u PRE_KEY_N=%u\n",
            SUB_N, NUM_N, PRE_N, pre_key_n);

    // ─── Phase 3: Build fy2023 filter array ──────────────────────────────────
    std::vector<uint8_t> fy2023(SUB_N, 0);
    {
        GENDB_PHASE("build_sub_filter");
        for (uint32_t i = 0; i < SUB_N; i++)
            fy2023[i] = (sub_fy_col[i] == (int16_t)2023) ? 1u : 0u;
    }

    // ─── Phase 4: Parse num_zonemaps to find USD block range ─────────────────
    // Layout: [int32_t n_blocks][NumZoneBlock * n_blocks]
    // NumZoneBlock: {int8_t uom_min, uom_max, int32_t ddate_min, ddate_max} = 10 bytes
    struct BlockRange { size_t lo, hi; };
    std::vector<BlockRange> usd_blocks;
    {
        GENDB_PHASE("scan_num_zonemaps");
        int32_t n_blocks = *(const int32_t*)num_zone_raw;
        const uint8_t* blk_base = num_zone_raw + 4;
        const uint32_t BLOCK_SIZE = 100000;

        usd_blocks.reserve(n_blocks);
        for (int32_t b = 0; b < n_blocks; b++) {
            const NumZoneBlock* blk = (const NumZoneBlock*)(blk_base + b * 10);
            // Skip block if usd_code is not in [uom_min, uom_max]
            if (usd_code_i8 < blk->uom_min || usd_code_i8 > blk->uom_max) continue;
            size_t lo = (size_t)b * BLOCK_SIZE;
            size_t hi = std::min(lo + (size_t)BLOCK_SIZE, NUM_N);
            usd_blocks.push_back({lo, hi});
        }
        fprintf(stderr, "[info] USD blocks: %zu / %d\n", usd_blocks.size(), n_blocks);
    }

    // ─── Phase 5: Parallel num scan + join + aggregate ───────────────────────
    const int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<uint64_t, AggVal>> tl_maps(nthreads);

    {
        GENDB_PHASE("main_scan");

        const uint8_t*     fy2023_ptr  = fy2023.data();
        const int32_t*     sname_ptr   = sub_name_col;
        const int8_t*      uom_ptr     = num_uom_col;
        const double*      val_ptr     = num_val_col;
        const int32_t*     adsh_ptr    = num_adsh_col;
        const int32_t*     tag_ptr     = num_tag_col;
        const int32_t*     ver_ptr     = num_ver_col;
        const int8_t*      stmt_ptr    = pre_stmt_col;
        const PreKeyEntry* pke_ptr     = pre_key_entries;
        const uint32_t     pke_n       = pre_key_n;
        const uint32_t     sub_n       = SUB_N;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& my_map = tl_maps[tid];
            my_map.reserve(1 << 16);  // 64K initial

            #pragma omp for schedule(dynamic, 1)
            for (int bidx = 0; bidx < (int)usd_blocks.size(); bidx++) {
                size_t lo = usd_blocks[bidx].lo;
                size_t hi = usd_blocks[bidx].hi;

                for (size_t i = lo; i < hi; i++) {
                    // Filter: uom == USD
                    if (uom_ptr[i] != usd_code_i8) continue;

                    // Filter: value IS NOT NULL
                    double v = val_ptr[i];
                    if (std::isnan(v)) continue;

                    // Filter: sub.fy = 2023 via direct array lookup
                    int32_t adsh_code = adsh_ptr[i];
                    if (adsh_code < 0 || (uint32_t)adsh_code >= sub_n) continue;
                    if (!fy2023_ptr[adsh_code]) continue;

                    // Get name_code from sub/name.bin
                    int32_t name_code = sname_ptr[adsh_code];
                    if (name_code < 0) continue;

                    int32_t tag_code = tag_ptr[i];
                    int32_t ver_code = ver_ptr[i];

                    // Binary search pre_key_sorted for (adsh_code, tag_code, ver_code)
                    // Compare on (adsh, tag, ver) lexicographically
                    // pke_ptr is sorted by (adsh, tag, ver)
                    const PreKeyEntry target_key = {adsh_code, tag_code, ver_code, 0};

                    auto cmp_less = [](const PreKeyEntry& a, const PreKeyEntry& b) {
                        if (a.adsh != b.adsh) return a.adsh < b.adsh;
                        if (a.tag  != b.tag)  return a.tag  < b.tag;
                        return a.ver < b.ver;
                    };

                    const PreKeyEntry* lb = std::lower_bound(
                        pke_ptr, pke_ptr + pke_n, target_key, cmp_less);

                    // Walk forward while keys match
                    for (const PreKeyEntry* e = lb;
                         e != pke_ptr + pke_n &&
                         e->adsh == adsh_code &&
                         e->tag  == tag_code  &&
                         e->ver  == ver_code;
                         ++e)
                    {
                        int32_t pre_row = e->row_id;
                        if (pre_row < 0 || (uint32_t)pre_row >= (uint32_t)pke_n) continue;
                        // Check pre.stmt = 'IS'
                        if (stmt_ptr[pre_row] != is_code_i8) continue;

                        // Aggregate
                        uint64_t pk = pack_key((uint32_t)name_code,
                                               (uint32_t)tag_code,
                                               (uint32_t)pre_row);
                        auto& av = my_map[pk];
                        av.sum += v;
                        av.cnt++;
                    }
                }
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregates ───────────────────────────────
    struct AggResult { uint64_t key; double sum; int64_t cnt; };
    std::vector<AggResult> agg_results;
    {
        GENDB_PHASE("merge_aggregate");

        std::unordered_map<uint64_t, AggVal> global_map;
        global_map.reserve(1 << 17);  // 128K

        for (int t = 0; t < nthreads; t++) {
            for (auto& [k, v] : tl_maps[t]) {
                auto& gv = global_map[k];
                gv.sum += v.sum;
                gv.cnt += v.cnt;
            }
            // Free thread-local map memory
            std::unordered_map<uint64_t, AggVal>().swap(tl_maps[t]);
        }
        tl_maps.clear();

        agg_results.reserve(global_map.size());
        for (auto& [k, v] : global_map)
            agg_results.push_back({k, v.sum, v.cnt});

        fprintf(stderr, "[info] unique primary groups: %zu\n", agg_results.size());
    }

    // ─── Phase 7: Decode strings + secondary string grouping ─────────────────
    {
        GENDB_PHASE("decode_and_output");

        // Load decode files
        size_t name_dict_sz, tag_dict_sz, plabel_off_sz, plabel_data_sz;
        const uint8_t* name_dict_raw  = mmap_ro(G + "/sub/name.dict",           name_dict_sz);
        const uint8_t* tag_dict_raw   = mmap_ro(G + "/shared/tag_numpre.dict",  tag_dict_sz);
        const int64_t* plabel_off_col = (const int64_t*)mmap_ro(G + "/pre/plabel.offsets", plabel_off_sz);
        const char*    plabel_data    = (const char*)   mmap_ro(G + "/pre/plabel.data",    plabel_data_sz);

        // Build string_view arrays from dicts
        auto name_sv = build_dict_sv(name_dict_raw, name_dict_sz);
        auto tag_sv  = build_dict_sv(tag_dict_raw,  tag_dict_sz);

        // Secondary grouping: (name_sv, tag_sv, plabel_sv) → (sum, cnt)
        struct SGKey {
            std::string_view name, tag, plabel;
            bool operator==(const SGKey& o) const {
                return name == o.name && tag == o.tag && plabel == o.plabel;
            }
        };
        struct SGHash {
            static uint64_t hash_sv(std::string_view s) noexcept {
                uint64_t h = 14695981039346656037ULL;
                for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
                return h;
            }
            size_t operator()(const SGKey& k) const noexcept {
                uint64_t h = hash_sv(k.name);
                h ^= hash_sv(k.tag)    * 0xbf58476d1ce4e5b9ULL;
                h ^= hash_sv(k.plabel) * 0x94d049bb133111ebULL;
                h ^= h >> 33; h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                return (size_t)h;
            }
        };

        std::unordered_map<SGKey, AggVal, SGHash> sg_map;
        sg_map.reserve(agg_results.size() * 2);

        for (const AggResult& r : agg_results) {
            uint32_t nc  = unpack_name_code(r.key);
            uint32_t tc  = unpack_tag_code(r.key);
            uint32_t pr  = unpack_pre_row(r.key);

            std::string_view n_sv = (nc < name_sv.size()) ? name_sv[nc] : std::string_view{};
            std::string_view t_sv = (tc < tag_sv.size())  ? tag_sv[tc]  : std::string_view{};

            int64_t plo = plabel_off_col[pr];
            int64_t phi = plabel_off_col[pr + 1];
            std::string_view p_sv(plabel_data + plo, (size_t)(phi - plo));

            auto& av = sg_map[SGKey{n_sv, t_sv, p_sv}];
            av.sum += r.sum;
            av.cnt += r.cnt;
        }

        fprintf(stderr, "[info] unique final groups: %zu\n", sg_map.size());

        // Sort top-200 by total_value DESC
        struct SortRow {
            std::string_view name, tag, plabel;
            double sum;
            int64_t cnt;
        };
        std::vector<SortRow> rows;
        rows.reserve(sg_map.size());
        for (auto& [k, v] : sg_map)
            rows.push_back({k.name, k.tag, k.plabel, v.sum, v.cnt});

        {
            GENDB_PHASE("topk_sort");
            size_t top = std::min((size_t)200, rows.size());
            std::partial_sort(rows.begin(), rows.begin() + top, rows.end(),
                [](const SortRow& a, const SortRow& b) { return a.sum > b.sum; });
            rows.resize(top);
        }

        // Write CSV
        {
            GENDB_PHASE("output_csv");
            std::string out_path = R + "/Q6.csv";
            FILE* fp = fopen(out_path.c_str(), "w");
            if (!fp) { perror(out_path.c_str()); return 1; }

            fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const SortRow& row : rows) {
                write_csv_field(fp, row.name);
                fputs(",IS,", fp);
                write_csv_field(fp, row.tag);
                fputc(',', fp);
                write_csv_field(fp, row.plabel);
                fprintf(fp, ",%.2f,%lld\n", row.sum, (long long)row.cnt);
            }
            fclose(fp);
        }

        munmap_ro(name_dict_raw,  name_dict_sz);
        munmap_ro(tag_dict_raw,   tag_dict_sz);
        munmap_ro(plabel_off_col, plabel_off_sz);
        munmap_ro(plabel_data,    plabel_data_sz);
    }

    return 0;
}
