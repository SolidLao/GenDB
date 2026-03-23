// Q1 — iter_2
// SELECT stmt, rfile, COUNT(*) AS cnt,
//        COUNT(DISTINCT adsh) AS num_filings,
//        AVG(line) AS avg_line_num
// FROM pre WHERE stmt IS NOT NULL
// GROUP BY stmt, rfile ORDER BY cnt DESC;
//
// Key changes from iter_0/1:
//   1. Use int16_t line column (column_versions/pre.line.int16/line_int16.bin)
//      to fix truncation of line values ≥128. line==0 → NULL (excluded from AVG).
//   2. Replace per-thread bitset<86135>[64] (~44MB) with a single shared
//      atomic uint64_t bitset[14][1347] (150KB, L2-resident).
//      → Eliminates large allocation/fault overhead, drops hot time to ~10ms.
//
// Strategy:
//   • group_map[256][256]: (uint8_t stmt_code, uint8_t rfile_code) → group_id
//     Built from dict files; null-stmt rows map to 0xFF automatically (filtered).
//   • Morsel-driven parallel scan; per-thread cnt/sum_line/cnt_line (336 bytes).
//   • Shared atomic bitset for COUNT(DISTINCT adsh) — relaxed fetch_or per row.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <stdexcept>
#include <sys/stat.h>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

// ── Constants ───────────────────────────────────────────────────────────────
static constexpr int    MAX_GROUPS      = 32;  // 8 valid stmts × 2 rfiles = 16 max
static constexpr int    ADSH_MAX        = 86135;           // adsh_code ∈ [0, 86134]
static constexpr int    WORDS_PER_GROUP = (ADSH_MAX + 63) / 64; // 1347 words
static constexpr size_t MORSEL_SIZE     = 100000;

// ── Shared aggregation state ─────────────────────────────────────────────────
// Atomic bitset for COUNT(DISTINCT adsh_code) per group.
// Group g owns words [g*WORDS_PER_GROUP .. (g+1)*WORDS_PER_GROUP).
alignas(64) static std::atomic<uint64_t> adsh_bitset[MAX_GROUPS * WORDS_PER_GROUP];

// Group lookup: (uint8_t stmt_code, uint8_t rfile_code) → group_id | 0xFF=skip
static uint8_t group_map[256][256];
static int8_t  group_stmt_code[MAX_GROUPS];
static int8_t  group_rfile_code[MAX_GROUPS];
static int     n_groups = 0;

// String decode tables
static std::string stmt_inv[256];
static std::string rfile_inv[256];

// ── Dict loading ─────────────────────────────────────────────────────────────
static std::unordered_map<std::string, int8_t> load_dict(const std::string& path) {
    std::unordered_map<std::string, int8_t> m;
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
    uint8_t n; fread(&n, 1, 1, f);
    for (int i = 0; i < (int)n; ++i) {
        int8_t  code; fread(&code, 1, 1, f);
        uint8_t slen; fread(&slen, 1, 1, f);
        char buf[256]{}; fread(buf, 1, slen, f);
        m[std::string(buf, slen)] = code;
    }
    fclose(f);
    return m;
}

// ── Per-thread aggregation state ─────────────────────────────────────────────
// alignas(64) causes sizeof to be rounded up to 384 bytes → no false sharing.
struct alignas(64) ThreadState {
    int64_t cnt[MAX_GROUPS];       // COUNT(*) per group
    int64_t sum_line[MAX_GROUPS];  // SUM(line) for non-null rows
    int64_t cnt_line[MAX_GROUPS];  // COUNT(line != 0) for AVG denominator
};

// ── Main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    mkdir(results_dir.c_str(), 0755);

    GENDB_PHASE("total");

    // ── Phase: data_loading ─────────────────────────────────────────────────
    {
        GENDB_PHASE("data_loading");

        auto stmt_map  = load_dict(gendb_dir + "/indexes/stmt_codes.bin");
        auto rfile_map = load_dict(gendb_dir + "/indexes/rfile_codes.bin");

        for (auto& [s, c] : stmt_map)  stmt_inv[(uint8_t)c]  = s;
        for (auto& [s, c] : rfile_map) rfile_inv[(uint8_t)c] = s;

        // Build group_map from dict cross-product (valid stmt × rfile).
        // Null-stmt code (empty string key) is NOT added → its slot stays 0xFF.
        memset(group_map, 0xFF, sizeof(group_map));
        n_groups = 0;
        for (auto& [ss, sc] : stmt_map) {
            if (ss.empty()) continue;   // skip IS-NULL / empty-string code
            for (auto& [rs, rc] : rfile_map) {
                uint8_t si = (uint8_t)sc;
                uint8_t ri = (uint8_t)rc;
                if (group_map[si][ri] == 0xFF && n_groups < MAX_GROUPS) {
                    group_map[si][ri]         = (uint8_t)n_groups;
                    group_stmt_code[n_groups]  = sc;
                    group_rfile_code[n_groups] = rc;
                    ++n_groups;
                }
            }
        }
    }

    // ── mmap all scan columns ────────────────────────────────────────────────
    MmapColumn<int8_t>  stmt_col (gendb_dir + "/pre/stmt_code.bin");
    MmapColumn<int8_t>  rfile_col(gendb_dir + "/pre/rfile_code.bin");
    MmapColumn<int32_t> adsh_col (gendb_dir + "/pre/adsh_code.bin");
    // int16_t line column: 0 = NULL (excluded from AVG); 1-482 = valid
    MmapColumn<int16_t> line_col (gendb_dir +
        "/column_versions/pre.line.int16/line_int16.bin");

    const size_t N = stmt_col.count;

    // Fire async readahead on all columns (overlaps HDD I/O with setup)
    mmap_prefetch_all(stmt_col, rfile_col, adsh_col, line_col);

    // build_joins phase: adsh_bitset is already zero-init as global; nothing to do.
    { GENDB_PHASE("build_joins"); }

    // ── Phase: main_scan ────────────────────────────────────────────────────
    const int nthreads = std::max(1, (int)std::thread::hardware_concurrency());
    std::vector<ThreadState> ts_arr(nthreads);
    for (auto& ts : ts_arr) memset(&ts, 0, sizeof(ts));

    std::atomic<size_t> morsel_counter{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            ThreadState& ts   = ts_arr[tid];
            const int8_t*  sc = stmt_col.data;
            const int8_t*  rc = rfile_col.data;
            const int32_t* ac = adsh_col.data;
            const int16_t* lc = line_col.data;

            while (true) {
                size_t start = morsel_counter.fetch_add(MORSEL_SIZE,
                                                        std::memory_order_relaxed);
                if (start >= N) break;
                size_t end = std::min(start + MORSEL_SIZE, N);

                for (size_t i = start; i < end; ++i) {
                    // group_map naturally filters null-stmt and unknown pairs (0xFF)
                    uint8_t gid = group_map[(uint8_t)sc[i]][(uint8_t)rc[i]];
                    if (__builtin_expect(gid == 0xFF, 0)) continue;

                    ts.cnt[gid]++;

                    // line==0 means NULL → exclude from AVG
                    int16_t line = lc[i];
                    if (line != 0) {
                        ts.sum_line[gid] += line;
                        ts.cnt_line[gid]++;
                    }

                    // COUNT(DISTINCT adsh): atomic OR into shared bitset
                    int32_t adsh = ac[i];
                    adsh_bitset[(int)gid * WORDS_PER_GROUP + (adsh >> 6)]
                        .fetch_or(1ULL << (adsh & 63), std::memory_order_relaxed);
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; ++t) threads.emplace_back(worker, t);
        for (auto& t : threads) t.join();
    }

    // ── Merge per-thread aggregates ──────────────────────────────────────────
    int64_t cnt[MAX_GROUPS]      = {};
    int64_t sum_line[MAX_GROUPS] = {};
    int64_t cnt_line[MAX_GROUPS] = {};
    for (int t = 0; t < nthreads; ++t) {
        for (int g = 0; g < n_groups; ++g) {
            cnt[g]      += ts_arr[t].cnt[g];
            sum_line[g] += ts_arr[t].sum_line[g];
            cnt_line[g] += ts_arr[t].cnt_line[g];
        }
    }

    // COUNT(DISTINCT adsh) via popcount of shared atomic bitset
    int64_t num_filings[MAX_GROUPS] = {};
    for (int g = 0; g < n_groups; ++g) {
        const int base = g * WORDS_PER_GROUP;
        for (int w = 0; w < WORDS_PER_GROUP; ++w) {
            num_filings[g] += __builtin_popcountll(
                adsh_bitset[base + w].load(std::memory_order_relaxed));
        }
    }

    // Sort groups by cnt DESC
    std::vector<int> order(n_groups);
    for (int i = 0; i < n_groups; ++i) order[i] = i;
    std::sort(order.begin(), order.end(),
              [&](int a, int b) { return cnt[a] > cnt[b]; });

    // ── Phase: output ────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) {
            fprintf(stderr, "Cannot open output: %s\n", out_path.c_str());
            return 1;
        }
        fprintf(out, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (int i = 0; i < n_groups; ++i) {
            int g = order[i];
            if (cnt[g] == 0) continue;
            double avg_line = cnt_line[g] > 0
                ? (double)sum_line[g] / (double)cnt_line[g]
                : 0.0;
            fprintf(out, "%s,%s,%lld,%lld,%.2f\n",
                    stmt_inv[(uint8_t)group_stmt_code[g]].c_str(),
                    rfile_inv[(uint8_t)group_rfile_code[g]].c_str(),
                    (long long)cnt[g],
                    (long long)num_filings[g],
                    avg_line);
        }
        fclose(out);
    }

    return 0;
}
