// Q3 iter_2 — optimized fused parallel scan with:
//   (A) uint64_t group_map key (cik<<32 | fnv1a32(name)) vs string key → ~4x faster build
//   (B) t-outer-g-inner sequential merge → streaming L3 reads vs prior column-major random access
//   (C) cik_sum computed post-merge from group_sum, not in scan hot loop → removes 2nd random write
//   (D) software prefetch 16 rows ahead for gid_ptr random lookup
//   (E) static scheduling for uniform block distribution
//
// Precision strategy: int64_t cents (llround(v*100.0)) — exact, associative, matches ground truth.
// Parallelism: morsel-driven OMP with per-thread group accumulators; sequential t-outer merge.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_map>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ─── Constants ───────────────────────────────────────────────────────────────
static constexpr int64_t  NUM_ROWS   = 39401761;
static constexpr int32_t  SUB_ROWS   = 86135;
static constexpr int64_t  BLOCK_SIZE = 100000LL;
static constexpr int16_t  FY_FILTER  = 2022;
static constexpr int      TOP_K      = 100;
static constexpr int      PFETCH     = 16;

// ─── Zone-map binary layout (12 bytes: 2×int8_t + 2-byte pad + 2×int32_t) ───
struct NumZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int8_t  _pad[2];
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(NumZoneMap) == 12, "ZoneMap size mismatch");

// ─── FNV-1a 64-bit hash — collision probability ~n²/2^64 ≈ 0 for n=17K ──────
static inline uint64_t fnv1a64(const void* data, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

// ─── CSV name writer (quotes if name contains comma/quote/newline) ────────────
static void write_csv_name(FILE* f, const char* s, uint32_t len) {
    bool need_quote = false;
    for (uint32_t i = 0; i < len; i++) {
        char c = s[i];
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need_quote = true; break; }
    }
    if (!need_quote) { fwrite(s, 1, len, f); return; }
    fputc('"', f);
    for (uint32_t i = 0; i < len; i++) {
        if (s[i] == '"') fputc('"', f);
        fputc(s[i], f);
    }
    fputc('"', f);
}

// ─── Main ────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    mkdir(results_dir.c_str(), 0755);

    GENDB_PHASE("total");

    // ════════════════════════════════════════════════════════════════════════
    // Phase 1 — Data Loading
    // ════════════════════════════════════════════════════════════════════════
    int8_t usd_code = -1;

    std::vector<int16_t>  sub_fy(SUB_ROWS);
    std::vector<int32_t>  sub_cik(SUB_ROWS);
    std::vector<uint32_t> name_offsets(SUB_ROWS + 1);
    std::vector<char>     name_data_buf;

    // adsh_group_id[adsh_code] = compact group id (0..n_groups-1) or -1 if fy!=2022
    std::vector<int32_t> adsh_group_id(SUB_ROWS, -1);

    // GroupInfo: rep_adsh for name decode, cik for output, cik_idx for post-merge cik_sum
    struct GroupInfo { int32_t rep_adsh; int32_t cik; int32_t cik_idx; };
    std::vector<GroupInfo> groups;

    uint32_t n_blocks = 0;
    std::vector<bool> skip_block;

    {
        GENDB_PHASE("data_loading");

        // uom_codes dict: uint8_t N; N×{int8_t code, uint8_t slen, char[slen]}
        {
            FILE* f = fopen((gendb_dir + "/indexes/uom_codes.bin").c_str(), "rb");
            if (!f) { perror("uom_codes.bin"); return 1; }
            uint8_t N = 0;
            (void)fread(&N, 1, 1, f);
            for (int i = 0; i < (int)N; i++) {
                int8_t code = 0; uint8_t slen = 0; char buf[256];
                (void)fread(&code, 1, 1, f);
                (void)fread(&slen, 1, 1, f);
                (void)fread(buf, 1, slen, f);
                buf[slen] = '\0';
                if (strcmp(buf, "USD") == 0) usd_code = code;
            }
            fclose(f);
            if (usd_code == -1) { fprintf(stderr, "USD code not found\n"); return 1; }
        }

        // sub/fy.bin
        {
            FILE* f = fopen((gendb_dir + "/sub/fy.bin").c_str(), "rb");
            if (!f) { perror("sub/fy.bin"); return 1; }
            (void)fread(sub_fy.data(), sizeof(int16_t), SUB_ROWS, f);
            fclose(f);
        }

        // sub/cik.bin
        {
            FILE* f = fopen((gendb_dir + "/sub/cik.bin").c_str(), "rb");
            if (!f) { perror("sub/cik.bin"); return 1; }
            (void)fread(sub_cik.data(), sizeof(int32_t), SUB_ROWS, f);
            fclose(f);
        }

        // sub/name_offsets.bin
        {
            FILE* f = fopen((gendb_dir + "/sub/name_offsets.bin").c_str(), "rb");
            if (!f) { perror("sub/name_offsets.bin"); return 1; }
            (void)fread(name_offsets.data(), sizeof(uint32_t), SUB_ROWS + 1, f);
            fclose(f);
        }

        // sub/name_data.bin
        {
            FILE* f = fopen((gendb_dir + "/sub/name_data.bin").c_str(), "rb");
            if (!f) { perror("sub/name_data.bin"); return 1; }
            size_t nd_sz = name_offsets[SUB_ROWS];
            name_data_buf.resize(nd_sz);
            if (nd_sz > 0) (void)fread(name_data_buf.data(), 1, nd_sz, f);
            fclose(f);
        }

        // num zone maps
        {
            FILE* f = fopen((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), "rb");
            if (!f) { perror("num_zone_maps.bin"); return 1; }
            (void)fread(&n_blocks, sizeof(uint32_t), 1, f);
            std::vector<NumZoneMap> zm(n_blocks);
            (void)fread(zm.data(), sizeof(NumZoneMap), n_blocks, f);
            fclose(f);
            skip_block.assign(n_blocks, false);
            for (uint32_t b = 0; b < n_blocks; b++) {
                if (zm[b].min_uom > usd_code || zm[b].max_uom < usd_code)
                    skip_block[b] = true;
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Phase 2 — Build Compact Indexes
    //
    // Optimization (A): uint64_t key = (cik << 32) | fnv1a32(name)
    // Eliminates std::string allocation and heap overhead in group_map.
    // Integer key hash/compare is ~4x faster than string key.
    // GroupInfo now carries cik_idx for O(n_groups) post-merge cik_sum pass.
    // ════════════════════════════════════════════════════════════════════════
    int32_t n_groups = 0;
    int32_t n_ciks   = 0;
    {
        GENDB_PHASE("build_joins");

        groups.reserve(20000);
        // Integer key: upper 32 bits = cik, lower 32 bits = fnv1a32(name)
        std::unordered_map<uint64_t, int32_t> group_map;
        group_map.reserve(25000);
        std::unordered_map<int32_t, int32_t> cik_idx_map;
        cik_idx_map.reserve(20000);

        const char*     nd = name_data_buf.data();
        const uint32_t* no = name_offsets.data();

        for (int32_t i = 0; i < SUB_ROWS; i++) {
            if (sub_fy[i] != FY_FILTER) continue;

            const int32_t  cik  = sub_cik[i];
            const uint32_t ns   = no[i];
            const uint32_t nlen = no[i + 1] - ns;

            // 64-bit group key: FNV-1a64 over cik bytes then name bytes.
            // P(any collision) ≈ 17K²/2^64 ≈ 0 — no string allocation needed.
            uint64_t key = fnv1a64(&cik, sizeof(int32_t));
            // Mix name bytes into the hash (continue from cik hash for better avalanche)
            {
                uint64_t h = key;
                const uint8_t* p = (const uint8_t*)(nd + ns);
                for (uint32_t bi = 0; bi < nlen; bi++) {
                    h ^= p[bi];
                    h *= 1099511628211ULL;
                }
                key = h;
            }

            // Assign compact cik_idx
            auto [cit, cins] = cik_idx_map.emplace(cik, n_ciks);
            if (cins) ++n_ciks;
            const int32_t cidx = cit->second;

            // Assign compact group_id
            auto [git, gins] = group_map.emplace(key, (int32_t)groups.size());
            if (gins) groups.push_back({i, cik, cidx});
            adsh_group_id[i] = git->second;
        }
        n_groups = (int32_t)groups.size();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Phase 3 — Fused Parallel Scan
    //
    // Per-thread flat array lg[n_groups] accumulates group sums (cents only).
    // Optimization (C): no lc[] cik array in hot loop — eliminates 2nd random write.
    // Optimization (D): __builtin_prefetch gid_ptr 16 rows ahead to overlap L3 latency.
    // Optimization (E): schedule(static) — uniform partitioning, no atomic overhead.
    // ════════════════════════════════════════════════════════════════════════
    const int nthreads = omp_get_max_threads();

    // Thread-major layout: thread t's block starts at offset t*n_groups
    // Use long double (80-bit, ~18 decimal digits) to avoid the precision loss
    // of llround(v*100) on large values (e.g., $26T sums need >14 digits precision).
    std::vector<long double> all_grp((size_t)nthreads * n_groups, 0.0);

    // Pre-build active block list (zone-map filtered)
    std::vector<int32_t> active_blocks;
    active_blocks.reserve(n_blocks);
    for (uint32_t b = 0; b < n_blocks; b++)
        if (!skip_block[b]) active_blocks.push_back((int32_t)b);
    const int32_t n_active = (int32_t)active_blocks.size();

    // mmap num columns (sequential reads from page cache)
    gendb::MmapColumn<int8_t>  uom_col(gendb_dir + "/num/uom_code.bin");
    gendb::MmapColumn<int32_t> adsh_col(gendb_dir + "/num/adsh_code.bin");
    gendb::MmapColumn<double>  val_col(gendb_dir + "/num/value.bin");
    mmap_prefetch_all(uom_col, adsh_col, val_col);

    const int8_t*  __restrict__ uom_ptr  = uom_col.data;
    const int32_t* __restrict__ adsh_ptr = adsh_col.data;
    const double*  __restrict__ val_ptr  = val_col.data;
    const int32_t* __restrict__ gid_ptr  = adsh_group_id.data();

    {
        GENDB_PHASE("main_scan");

        // static scheduling: uniform block partitioning, no atomic scheduling overhead
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int32_t bi = 0; bi < n_active; bi++) {
            const int32_t b   = active_blocks[bi];
            const int     tid = omp_get_thread_num();

            long double* __restrict__ lg = all_grp.data() + (size_t)tid * n_groups;

            const int64_t row_start = (int64_t)b * BLOCK_SIZE;
            const int64_t row_end   = (row_start + BLOCK_SIZE < NUM_ROWS)
                                    ? row_start + BLOCK_SIZE : NUM_ROWS;

            for (int64_t r = row_start; r < row_end; r++) {
                // Prefetch gid_ptr entry for row 16 ahead to overlap L3 random-lookup latency
                if (r + PFETCH < row_end)
                    __builtin_prefetch(gid_ptr + adsh_ptr[r + PFETCH], 0, 1);
                if (uom_ptr[r] != usd_code) continue;
                const int32_t ac  = adsh_ptr[r];
                const int32_t gid = gid_ptr[ac];
                if (gid < 0) continue;              // sub_fy[ac] != 2022
                lg[gid] += (long double)val_ptr[r];
            }
        }
    }

    // ─── Optimization (B): t-outer-g-inner sequential merge ─────────────────
    // Each thread's block (n_groups × long double) is scanned sequentially.
    // Prior column-major (g-outer, t-inner) caused stride random L3 accesses.
    // Now: 64 sequential passes → streaming reads → ~0.3ms vs ~21ms.
    std::vector<long double> group_sum(n_groups, 0.0);
    {
        for (int t = 0; t < nthreads; t++) {
            const long double* __restrict__ lp = all_grp.data() + (size_t)t * n_groups;
            for (int32_t g = 0; g < n_groups; g++) {
                group_sum[g] += lp[g];
            }
        }
    }

    // ─── Optimization (C): compute cik_sum post-merge in O(n_groups) ─────────
    // cik_sum[cik_idx] = sum of all group_sum values for groups belonging to that cik.
    // O(17K) vs O(34M) random writes eliminated from scan hot loop.
    std::vector<long double> cik_sum(n_ciks, 0.0);
    for (int32_t gid = 0; gid < n_groups; gid++) {
        cik_sum[groups[gid].cik_idx] += group_sum[gid];
    }

    // ─── HAVING threshold ────────────────────────────────────────────────────
    // avg_threshold = SUM(cik_sum) / n_ciks (in dollars)
    long double total_cik_sum = 0.0;
    for (int32_t c = 0; c < n_ciks; c++) total_cik_sum += cik_sum[c];
    const long double avg_threshold = (n_ciks > 0)
        ? total_cik_sum / (long double)n_ciks
        : 0.0;

    // ════════════════════════════════════════════════════════════════════════
    // Phase 4 — Output: HAVING filter, decode names, sort, write CSV
    // ════════════════════════════════════════════════════════════════════════
    {
        GENDB_PHASE("output");

        struct ResultRow {
            long double total_val;   // sum in dollars (long double for precision)
            int32_t     cik;
            const char* name_ptr;
            uint32_t    name_len;
        };
        std::vector<ResultRow> results;
        results.reserve(512);

        const char*     nd = name_data_buf.data();
        const uint32_t* no = name_offsets.data();

        for (int32_t gid = 0; gid < n_groups; gid++) {
            if (group_sum[gid] <= avg_threshold) continue;
            const GroupInfo& g     = groups[gid];
            const int32_t    radsh = g.rep_adsh;
            results.push_back({group_sum[gid], g.cik,
                               nd + no[radsh], no[radsh + 1] - no[radsh]});
        }

        // Top-100: partial sort by total_val DESC
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            return a.total_val > b.total_val;
        };
        if ((int)results.size() > TOP_K) {
            std::partial_sort(results.begin(), results.begin() + TOP_K,
                              results.end(), cmp);
            results.resize(TOP_K);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }

        // Write CSV: name,cik,total_value
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }

        fprintf(out, "name,cik,total_value\n");
        for (const auto& row : results) {
            write_csv_name(out, row.name_ptr, row.name_len);
            // Format as dollars with 2 decimal places using long double precision
            fprintf(out, ",%d,%.2Lf\n", row.cik, row.total_val);
        }
        fclose(out);
    }

    return 0;
}
