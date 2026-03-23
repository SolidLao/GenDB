// Q1: SELECT stmt, rfile, COUNT(*) AS cnt, COUNT(DISTINCT adsh) AS num_filings, AVG(line) AS avg_line_num
//     FROM pre WHERE stmt IS NOT NULL GROUP BY stmt, rfile ORDER BY cnt DESC
//
// Strategy: morsel-driven parallel scan, direct array aggregation [256][256] for cnt/sum_line,
//           bitset<86135> per group per thread for COUNT(DISTINCT adsh).

#include <bitset>
#include <vector>
#include <string>
#include <algorithm>
#include <atomic>
#include <thread>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <climits>
#include <cassert>
#include <sys/stat.h>
#include <sys/types.h>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

// ─── Constants ──────────────────────────────────────────────────────────────
static constexpr int   ADSH_RANGE   = 86135;
static constexpr int   MAX_GROUPS   = 64;    // upper bound for safety
static constexpr size_t MORSEL_SIZE = 100000;

// ─── Global dict state ───────────────────────────────────────────────────────
static std::string g_stmt_inv[256];
static std::string g_rfile_inv[256];
static bool        g_stmt_exists[256]  = {};
static bool        g_rfile_exists[256] = {};

// ─── Group mapping ───────────────────────────────────────────────────────────
struct GroupInfo { uint8_t stmt_byte; uint8_t rfile_byte; };
static int       g_n_groups = 0;
static GroupInfo g_groups[MAX_GROUPS];
static uint8_t   g_group_map[256][256]; // 0xFF = invalid/not-a-group

// ─── Per-thread aggregation state ───────────────────────────────────────────
struct alignas(64) ThreadState {
    int64_t cnt[MAX_GROUPS];
    int64_t sum_line[MAX_GROUPS];
    int64_t cnt_line[MAX_GROUPS]; // count of non-null (non-zero) line values for AVG denominator
    std::bitset<ADSH_RANGE> adsh_bits[MAX_GROUPS];

    ThreadState() {
        memset(cnt,      0, sizeof(cnt));
        memset(sum_line, 0, sizeof(sum_line));
        memset(cnt_line, 0, sizeof(cnt_line));
        // std::bitset default ctor zeros all bits
    }
};

// ─── Dict loading ────────────────────────────────────────────────────────────
static int8_t load_stmt_dict(const std::string& path) {
    int8_t null_code = (int8_t)-99; // sentinel: no empty-string entry
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint8_t n; fread(&n, 1, 1, f);
    for (int i = 0; i < (int)n; i++) {
        int8_t  code; fread(&code, 1, 1, f);
        uint8_t slen; fread(&slen, 1, 1, f);
        char buf[256] = {};
        if (slen > 0) fread(buf, 1, slen, f);
        std::string s(buf, slen);
        g_stmt_inv[(uint8_t)code]   = s;
        g_stmt_exists[(uint8_t)code] = true;
        if (s.empty()) null_code = code;
    }
    fclose(f);
    return null_code;
}

static void load_rfile_dict(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint8_t n; fread(&n, 1, 1, f);
    for (int i = 0; i < (int)n; i++) {
        int8_t  code; fread(&code, 1, 1, f);
        uint8_t slen; fread(&slen, 1, 1, f);
        char buf[256] = {};
        if (slen > 0) fread(buf, 1, slen, f);
        g_rfile_inv[(uint8_t)code]    = std::string(buf, slen);
        g_rfile_exists[(uint8_t)code] = true;
    }
    fclose(f);
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

    int8_t null_stmt_code = (int8_t)-99;

    // ── Phase: data_loading ──────────────────────────────────────────────────
    MmapColumn<int8_t>  col_stmt;
    MmapColumn<int8_t>  col_rfile;
    MmapColumn<int32_t> col_adsh;
    MmapColumn<uint8_t> col_line;
    {
        GENDB_PHASE("data_loading");

        // Load dictionaries
        null_stmt_code = load_stmt_dict(gendb_dir + "/indexes/stmt_codes.bin");
        load_rfile_dict(gendb_dir + "/indexes/rfile_codes.bin");

        // Build group map: Cartesian product of valid stmt (excl. null) × valid rfile
        memset(g_group_map, 0xFF, sizeof(g_group_map));
        for (int sc = 0; sc < 256; sc++) {
            if (!g_stmt_exists[sc]) continue;
            // Skip null stmt code (rows with this code are filtered out)
            if (null_stmt_code != (int8_t)-99 &&
                (uint8_t)sc == (uint8_t)null_stmt_code) continue;
            for (int rc = 0; rc < 256; rc++) {
                if (!g_rfile_exists[rc]) continue;
                if (g_n_groups >= MAX_GROUPS) {
                    fprintf(stderr, "Warning: exceeded MAX_GROUPS\n");
                    break;
                }
                g_group_map[sc][rc]       = (uint8_t)g_n_groups;
                g_groups[g_n_groups++]    = { (uint8_t)sc, (uint8_t)rc };
            }
        }

        // Memory-map columns and issue prefetch
        col_stmt.open(gendb_dir  + "/pre/stmt_code.bin");
        col_rfile.open(gendb_dir + "/pre/rfile_code.bin");
        col_adsh.open(gendb_dir  + "/pre/adsh_code.bin");
        col_line.open(gendb_dir  + "/pre/line.bin");

        // Prefetch all columns into page cache (HDD: overlap I/O with setup)
        mmap_prefetch_all(col_stmt, col_rfile, col_adsh, col_line);
    }

    const size_t total_rows = col_stmt.size();

    // ── Phase: main_scan ─────────────────────────────────────────────────────
    int n_threads = (int)std::thread::hardware_concurrency();
    if (n_threads < 1) n_threads = 1;

    // Allocate per-thread states on the heap (each ~5 MB with bitsets)
    std::vector<ThreadState*> states(n_threads);
    for (int t = 0; t < n_threads; t++) states[t] = new ThreadState();

    {
        GENDB_PHASE("main_scan");

        std::atomic<size_t> morsel_counter{0};

        const int8_t* __restrict__  sc_data   = col_stmt.data;
        const int8_t* __restrict__  rc_data   = col_rfile.data;
        const int32_t* __restrict__ adsh_data = col_adsh.data;
        const uint8_t* __restrict__ line_data = col_line.data;

        const int8_t null_sc  = null_stmt_code;
        const bool   has_null = (null_sc != (int8_t)-99);
        const int    n_grps   = g_n_groups;

        auto worker = [&](int tid) {
            ThreadState& state     = *states[tid];
            int64_t*     cnt       = state.cnt;
            int64_t*     sum_line  = state.sum_line;
            int64_t*     cnt_line  = state.cnt_line;
            auto*        adsh_bits = state.adsh_bits;

            while (true) {
                size_t start = morsel_counter.fetch_add(MORSEL_SIZE, std::memory_order_relaxed);
                if (start >= total_rows) break;
                size_t end = std::min(start + MORSEL_SIZE, total_rows);

                if (has_null) {
                    for (size_t i = start; i < end; i++) {
                        int8_t sc = sc_data[i];
                        if (sc == null_sc) continue;
                        uint8_t gid = g_group_map[(uint8_t)sc][(uint8_t)rc_data[i]];
                        if (gid == 0xFF) continue;
                        cnt[gid]++;
                        uint8_t lv = line_data[i];
                        if (lv != 0) { sum_line[gid] += (int64_t)lv; cnt_line[gid]++; }
                        int32_t adsh = adsh_data[i];
                        if (__builtin_expect(adsh >= 0 && adsh < ADSH_RANGE, 1))
                            adsh_bits[gid].set((size_t)adsh);
                    }
                } else {
                    // No null filtering — accept all rows
                    for (size_t i = start; i < end; i++) {
                        int8_t sc  = sc_data[i];
                        uint8_t gid = g_group_map[(uint8_t)sc][(uint8_t)rc_data[i]];
                        if (gid == 0xFF) continue;
                        cnt[gid]++;
                        uint8_t lv = line_data[i];
                        if (lv != 0) { sum_line[gid] += (int64_t)lv; cnt_line[gid]++; }
                        int32_t adsh = adsh_data[i];
                        if (__builtin_expect(adsh >= 0 && adsh < ADSH_RANGE, 1))
                            adsh_bits[gid].set((size_t)adsh);
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(n_threads);
        for (int t = 0; t < n_threads; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads) th.join();

        // ── Merge thread-local states into states[0] ──────────────────────
        ThreadState& merged = *states[0];
        for (int t = 1; t < n_threads; t++) {
            ThreadState& ts = *states[t];
            for (int g = 0; g < n_grps; g++) {
                merged.cnt[g]      += ts.cnt[g];
                merged.sum_line[g] += ts.sum_line[g];
                merged.cnt_line[g] += ts.cnt_line[g];
                merged.adsh_bits[g] |= ts.adsh_bits[g];
            }
        }
    }

    // ── Phase: output ────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        const ThreadState& merged = *states[0];

        // Build result rows
        struct ResultRow {
            std::string stmt;
            std::string rfile;
            int64_t     cnt;
            int64_t     num_filings;
            double      avg_line_num;
        };

        std::vector<ResultRow> rows;
        rows.reserve(g_n_groups);

        for (int g = 0; g < g_n_groups; g++) {
            if (merged.cnt[g] == 0) continue; // skip empty groups
            ResultRow r;
            r.stmt        = g_stmt_inv[g_groups[g].stmt_byte];
            r.rfile       = g_rfile_inv[g_groups[g].rfile_byte];
            r.cnt         = merged.cnt[g];
            r.num_filings = (int64_t)merged.adsh_bits[g].count();
            int64_t lc = merged.cnt_line[g];
            r.avg_line_num = (lc > 0) ? (double)merged.sum_line[g] / (double)lc : 0.0;
            rows.push_back(std::move(r));
        }

        // Sort by cnt DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.cnt > b.cnt;
        });

        // Write CSV
        const std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { fprintf(stderr, "Cannot open output %s\n", out_path.c_str()); return 1; }

        fprintf(out, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : rows) {
            fprintf(out, "%s,%s,%lld,%lld,%.2f\n",
                r.stmt.c_str(),
                r.rfile.c_str(),
                (long long)r.cnt,
                (long long)r.num_filings,
                r.avg_line_num);
        }
        fclose(out);
    }

    // Cleanup
    for (int t = 0; t < n_threads; t++) delete states[t];

    return 0;
}
