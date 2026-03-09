// Q3 — fused single-pass flat-array scan with int64_t cents accumulation
//
// Precision strategy: convert each double value to int64_t cents via llround().
// SEC financial values are exact cent multiples, so llround(v*100) is lossless.
// Integer accumulation is associative — same result regardless of order.
// This matches the ground truth exactly without floating-point rounding issues.
//
// Performance strategy: flat-array O(1) lookup replaces hash-map probing.
// adsh_group_id[adsh_code] and adsh_cik_idx[adsh_code] are compact integers
// built at load time, enabling direct array indexing in the scan critical path.
//
// Parallelism: OMP parallel for over active blocks; per-thread flat arrays;
// element-wise merge after scan. Integer merge is exact regardless of merge order.

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

// ─── Zone-map binary layout (12 bytes with 2-byte alignment pad) ──────────────
struct NumZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int8_t  _pad[2];
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(NumZoneMap) == 12, "ZoneMap size mismatch");

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

    // Compact index arrays indexed by adsh_code (= sub row index, 0..86134)
    std::vector<int32_t> adsh_group_id(SUB_ROWS, -1);  // gid or -1 if fy!=2022
    std::vector<int32_t> adsh_cik_idx (SUB_ROWS, -1);  // cik_idx or -1 if fy!=2022

    struct GroupInfo { int32_t rep_adsh; int32_t cik; };
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
    // For each fy=2022 sub row: assign a compact group_id (by (name,cik))
    // and a compact cik_idx. Store in flat arrays indexed by adsh_code.
    // ════════════════════════════════════════════════════════════════════════
    int32_t n_groups = 0;
    int32_t n_ciks   = 0;
    {
        GENDB_PHASE("build_joins");

        groups.reserve(20000);
        std::unordered_map<std::string, int32_t> group_map;
        group_map.reserve(25000);
        std::unordered_map<int32_t, int32_t> cik_idx_map;
        cik_idx_map.reserve(25000);

        const char*     nd = name_data_buf.data();
        const uint32_t* no = name_offsets.data();

        for (int32_t i = 0; i < SUB_ROWS; i++) {
            if (sub_fy[i] != FY_FILTER) continue;

            int32_t  cik  = sub_cik[i];
            uint32_t ns   = no[i];
            uint32_t nlen = no[i + 1] - ns;

            // Group key = 4-byte cik || name bytes (unique per (cik, name) pair)
            std::string key;
            key.reserve(4 + nlen);
            key.append(reinterpret_cast<const char*>(&cik), 4);
            key.append(nd + ns, nlen);

            auto [git, gins] = group_map.emplace(std::move(key), (int32_t)groups.size());
            if (gins) groups.push_back({i, cik});
            adsh_group_id[i] = git->second;

            auto [cit, cins] = cik_idx_map.emplace(cik, n_ciks);
            if (cins) ++n_ciks;
            adsh_cik_idx[i] = cit->second;
        }
        n_groups = (int32_t)groups.size();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Phase 3 — Fused Parallel Scan
    //
    // Per-thread flat arrays (int64_t cents, no hash probing):
    //   lg[n_groups]: group accumulator (for output values)
    //   lc[n_ciks]:   cik accumulator  (for HAVING threshold)
    //
    // int64_t cents: exact integer arithmetic; merge is commutative/associative;
    // result is independent of thread-block assignment order.
    // ════════════════════════════════════════════════════════════════════════
    const int nthreads = omp_get_max_threads();

    // Contiguous allocation; thread t block starts at t*n_groups (or t*n_ciks)
    std::vector<int64_t> all_grp((size_t)nthreads * n_groups, 0LL);
    std::vector<int64_t> all_cik((size_t)nthreads * n_ciks,   0LL);

    // Pre-build active block list
    std::vector<int32_t> active_blocks;
    active_blocks.reserve(n_blocks);
    for (uint32_t b = 0; b < n_blocks; b++)
        if (!skip_block[b]) active_blocks.push_back((int32_t)b);
    const int32_t n_active = (int32_t)active_blocks.size();

    // mmap num columns (large sequential reads from page cache)
    gendb::MmapColumn<int8_t>  uom_col(gendb_dir + "/num/uom_code.bin");
    gendb::MmapColumn<int32_t> adsh_col(gendb_dir + "/num/adsh_code.bin");
    gendb::MmapColumn<double>  val_col(gendb_dir + "/num/value.bin");
    mmap_prefetch_all(uom_col, adsh_col, val_col);

    const int8_t*  __restrict__ uom_ptr  = uom_col.data;
    const int32_t* __restrict__ adsh_ptr = adsh_col.data;
    const double*  __restrict__ val_ptr  = val_col.data;
    const int32_t* __restrict__ gid_ptr  = adsh_group_id.data();
    const int32_t* __restrict__ cidx_ptr = adsh_cik_idx.data();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 8) num_threads(nthreads)
        for (int32_t bi = 0; bi < n_active; bi++) {
            const int32_t b   = active_blocks[bi];
            const int     tid = omp_get_thread_num();

            int64_t* __restrict__ lg = all_grp.data() + (size_t)tid * n_groups;
            int64_t* __restrict__ lc = all_cik.data() + (size_t)tid * n_ciks;

            const int64_t row_start = (int64_t)b * BLOCK_SIZE;
            const int64_t row_end   = (row_start + BLOCK_SIZE < NUM_ROWS)
                                    ? row_start + BLOCK_SIZE : NUM_ROWS;

            for (int64_t r = row_start; r < row_end; r++) {
                if (uom_ptr[r] != usd_code) continue;
                const int32_t ac  = adsh_ptr[r];
                const int32_t gid = gid_ptr[ac];
                if (gid < 0) continue;              // sub_fy[ac] != 2022
                const int64_t vc  = llround(val_ptr[r] * 100.0);
                lg[gid]           += vc;
                lc[cidx_ptr[ac]]  += vc;
            }
        }
    }

    // ─── Element-wise merge of per-thread arrays ──────────────────────────
    std::vector<int64_t> group_sum(n_groups, 0LL);
    std::vector<int64_t> cik_sum  (n_ciks,   0LL);

    // group_sum merge: parallel (integer, exact)
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (int32_t g = 0; g < n_groups; g++) {
        int64_t s = 0;
        for (int t = 0; t < nthreads; t++)
            s += all_grp[(size_t)t * n_groups + g];
        group_sum[g] = s;
    }

    // cik_sum merge: serial (small array, n_ciks ≈ 10K)
    for (int t = 0; t < nthreads; t++) {
        const int64_t* lc = all_cik.data() + (size_t)t * n_ciks;
        for (int32_t c = 0; c < n_ciks; c++)
            cik_sum[c] += lc[c];
    }

    // ─── HAVING threshold (in cents) ─────────────────────────────────────
    // avg_threshold_cents = SUM(cik_sum_cents) / n_ciks
    int64_t total_cik_cents = 0;
    for (int32_t c = 0; c < n_ciks; c++) total_cik_cents += cik_sum[c];
    const double avg_threshold_cents = (n_ciks > 0)
        ? (double)total_cik_cents / (double)n_ciks
        : 0.0;

    // ════════════════════════════════════════════════════════════════════════
    // Phase 4 — Output: HAVING filter, decode names, sort, write CSV
    // ════════════════════════════════════════════════════════════════════════
    {
        GENDB_PHASE("output");

        struct ResultRow {
            int64_t  total_cents;
            int32_t  cik;
            const char* name_ptr;
            uint32_t    name_len;
        };
        std::vector<ResultRow> results;
        results.reserve(512);

        const char*     nd = name_data_buf.data();
        const uint32_t* no = name_offsets.data();

        for (int32_t gid = 0; gid < n_groups; gid++) {
            if ((double)group_sum[gid] <= avg_threshold_cents) continue;
            const GroupInfo& g = groups[gid];
            const int32_t    radsh = g.rep_adsh;
            results.push_back({group_sum[gid], g.cik,
                               nd + no[radsh], no[radsh + 1] - no[radsh]});
        }

        // Top-100: sort by total_cents DESC, ties broken by cik ASC
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.total_cents != b.total_cents) return a.total_cents > b.total_cents;
            return a.cik < b.cik;
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
            // Format cents as dollars.cents (e.g. 68575227011529962 → 685752270115299.62)
            int64_t dollars = row.total_cents / 100LL;
            int64_t cents   = row.total_cents % 100LL;
            if (cents < 0) { dollars--; cents += 100; }    // handle negative values
            fprintf(out, ",%d,%lld.%02lld\n", row.cik, (long long)dollars, (long long)cents);
        }
        fclose(out);
    }

    return 0;
}
