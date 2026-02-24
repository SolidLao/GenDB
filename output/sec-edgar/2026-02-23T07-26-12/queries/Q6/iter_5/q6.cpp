// Q6 — SEC-EDGAR: SUM(value), COUNT(*) per (name, stmt, tag, plabel)
// Strategy:
//   1. Build sub(fy=2023) hash via zone-map-guided scan → adsh_code→name_code (16K cap)
//   2. Build pre(stmt=IS) runtime hash via zone-map scan → (adsh,tag,version)→plabel_code (4M cap)
//   3. Parallel morsel-driven num scan (USD segment via zone map)
//      - fused: check uom==USD, !isnan(value), probe sub hash, probe pre hash
//      - thread-local aggregation: (name,stmt,tag,plabel) → {sum_cents,cnt}
//   4. Merge thread-local agg maps into global
//   5. Top-200 by total_value DESC via partial_sort

#include <cstdint>
#include <cmath>
#include <climits>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <parallel/algorithm>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// ─── Zone map entry layouts ────────────────────────────────────────────────────
// Format: [uint32_t num_blocks][ZMEntry × num_blocks]
// ZMEntry: min(T), max(T), uint32_t block_size
struct ZM16 { int16_t min_val, max_val; uint32_t block_size; };  // 8 bytes
struct ZM32 { int32_t min_val, max_val; uint32_t block_size; };  // 12 bytes

// ─── Hash table capacities ─────────────────────────────────────────────────────
// sub: ~8.6K filtered rows → cap = next_pow2(8600*2) = next_pow2(17200) = 32768
static constexpr uint32_t SUB_CAP  = 32768;
static constexpr uint32_t SUB_MASK = SUB_CAP - 1;

// pre IS: ~1.73M filtered rows → cap = next_pow2(1730000*2) = 4194304
static constexpr uint32_t PRE_CAP  = 4194304;
static constexpr uint32_t PRE_MASK = PRE_CAP - 1;

// agg: ~860K groups (8.6K companies × ~100 IS tags each) → cap = next_pow2(860K*2) = 2097152 (C9)
// Under-sizing caused overflowed table → O(CAP) probes per row → 10s scan + wrong results (C24)
static constexpr uint32_t AGG_CAP  = 2097152;
static constexpr uint32_t AGG_MASK = AGG_CAP - 1;

// ─── Hash table slot types ─────────────────────────────────────────────────────
struct SubSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t name_code;
};

// Flat sorted array entry for pre IS rows (supports multi-plabel per key)
struct PreEntry {
    int32_t adsh_code;
    int32_t tag_code;
    int32_t version_code;
    int32_t plabel_code;
};

struct PreSlot {
    int32_t adsh_code;   // INT32_MIN = empty
    int32_t tag_code;
    int32_t version_code;
    int32_t start_idx;   // index into sorted pre_entries array
};

struct AggSlot {
    int32_t name_code;    // INT32_MIN = empty
    int16_t stmt_code;
    int16_t _pad;
    int32_t tag_code;
    int32_t plabel_code;
    int64_t sum_cents;    // C29: int64_t cents accumulation
    int64_t cnt;
};

// ─── mmap helper ──────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_col(const std::string& path, size_t* out_nrows = nullptr) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    if (p == MAP_FAILED) { perror("mmap"); std::exit(1); }
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    if (out_nrows) *out_nrows = sz / sizeof(T);
    return p;
}

// ─── Dictionary loader ─────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { std::cerr << "Cannot open dict: " << path << "\n"; std::exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

template<typename T>
static T find_code(const std::vector<std::string>& dict, const std::string& val) {
    for (int i = 0; i < (int)dict.size(); i++)
        if (dict[i] == val) return (T)i;
    std::cerr << "Code not found in dict: " << val << "\n"; std::exit(1);
}

// ─── Hash functions ────────────────────────────────────────────────────────────
inline uint32_t sub_hash_fn(int32_t adsh) {
    return (uint32_t)adsh * 2654435761u;
}

inline uint32_t pre_hash_fn(int32_t adsh, int32_t tag, int32_t version) {
    uint64_t h = (uint64_t)(uint32_t)adsh    * 2654435761ULL
               ^ (uint64_t)(uint32_t)tag     * 40503ULL
               ^ (uint64_t)(uint32_t)version * 48271ULL;
    return (uint32_t)(h ^ (h >> 32));
}

inline uint32_t agg_hash_fn(int32_t name, int16_t stmt, int32_t tag, int32_t plabel) {
    uint64_t h = (uint64_t)(uint32_t)name          * 0x9E3779B97F4A7C15ULL
               ^ (uint64_t)(uint16_t)stmt           * 0x517CC1B727220A95ULL
               ^ (uint64_t)(uint32_t)tag            * 0x6C62272E07BB0142ULL
               ^ (uint64_t)(uint32_t)plabel         * 0xD2A98B26625EEE7BULL;
    return (uint32_t)(h ^ (h >> 32));
}

// ─── CSV safe field write ──────────────────────────────────────────────────────
static void csv_field(FILE* f, const std::string& s) {
    bool need_q = (s.find(',') != std::string::npos ||
                   s.find('"') != std::string::npos ||
                   s.find('\n') != std::string::npos);
    if (!need_q) { fwrite(s.data(), 1, s.size(), f); return; }
    fputc('"', f);
    for (char c : s) { if (c == '"') fputc('"', f); fputc(c, f); }
    fputc('"', f);
}

// ─── Main query ────────────────────────────────────────────────────────────────
void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string sub_d = gendb_dir + "/sub";
    const std::string pre_d = gendb_dir + "/pre";
    const std::string num_d = gendb_dir + "/num";
    const std::string idx_d = gendb_dir + "/indexes";

    // ── dictionaries (C2: load at runtime, never hardcode) ───────────────────
    auto uom_dict    = load_dict(num_d + "/uom_dict.txt");
    auto stmt_dict   = load_dict(pre_d + "/stmt_dict.txt");
    auto name_dict   = load_dict(sub_d + "/name_dict.txt");
    auto tag_dict    = load_dict(num_d + "/tag_dict.txt");
    auto plabel_dict = load_dict(pre_d + "/plabel_dict.txt");

    const int16_t usd_code = find_code<int16_t>(uom_dict, "USD");
    const int16_t is_code  = find_code<int16_t>(stmt_dict, "IS");

    // ── data loading ──────────────────────────────────────────────────────────
    const int32_t *sub_adsh, *sub_fy, *sub_name;
    const int32_t *pre_adsh, *pre_tag, *pre_ver, *pre_plabel;
    const int16_t *pre_stmt;
    const int32_t *num_adsh, *num_tag, *num_ver;
    const int16_t *num_uom;
    const double  *num_value;
    const uint8_t *sub_fy_zm_raw, *pre_stmt_zm_raw, *num_uom_zm_raw;
    size_t num_n;

    {
        GENDB_PHASE("data_loading");
        sub_adsh = mmap_col<int32_t>(sub_d + "/adsh.bin");
        sub_fy   = mmap_col<int32_t>(sub_d + "/fy.bin");
        sub_name = mmap_col<int32_t>(sub_d + "/name.bin");

        pre_adsh   = mmap_col<int32_t>(pre_d + "/adsh.bin");
        pre_tag    = mmap_col<int32_t>(pre_d + "/tag.bin");
        pre_ver    = mmap_col<int32_t>(pre_d + "/version.bin");
        pre_stmt   = mmap_col<int16_t>(pre_d + "/stmt.bin");
        pre_plabel = mmap_col<int32_t>(pre_d + "/plabel.bin");

        num_adsh  = mmap_col<int32_t>(num_d + "/adsh.bin");
        num_tag   = mmap_col<int32_t>(num_d + "/tag.bin");
        num_ver   = mmap_col<int32_t>(num_d + "/version.bin");
        num_uom   = mmap_col<int16_t>(num_d + "/uom.bin");
        num_value = mmap_col<double>(num_d + "/value.bin", &num_n);

        sub_fy_zm_raw   = mmap_col<uint8_t>(idx_d + "/sub_fy_zone_map.bin");
        pre_stmt_zm_raw = mmap_col<uint8_t>(idx_d + "/pre_stmt_zone_map.bin");
        num_uom_zm_raw  = mmap_col<uint8_t>(idx_d + "/num_uom_zone_map.bin");
    }

    // ── Phase: build sub hash (fy=2023) via zone-map-guided scan ─────────────
    SubSlot* sub_ht = nullptr;
    {
        GENDB_PHASE("dim_filter");

        sub_ht = new SubSlot[SUB_CAP];
        std::fill(sub_ht, sub_ht + SUB_CAP, SubSlot{INT32_MIN, INT32_MIN});  // C20

        const uint32_t  zm_nb = *reinterpret_cast<const uint32_t*>(sub_fy_zm_raw);
        const ZM32*     zm    = reinterpret_cast<const ZM32*>(sub_fy_zm_raw + 4);
        const int32_t   fy_target = 2023;

        size_t row_off = 0;
        for (uint32_t b = 0; b < zm_nb; b++) {
            uint32_t bsz = zm[b].block_size;
            if (zm[b].min_val <= fy_target && fy_target <= zm[b].max_val) {
                for (size_t r = row_off; r < row_off + bsz; r++) {
                    if (sub_fy[r] != fy_target) continue;
                    int32_t adsh = sub_adsh[r];
                    int32_t name = sub_name[r];
                    uint32_t h = sub_hash_fn(adsh) & SUB_MASK;
                    for (uint32_t p = 0; p < SUB_CAP; p++) {  // C24: bounded probing
                        uint32_t sl = (h + p) & SUB_MASK;
                        if (sub_ht[sl].adsh_code == INT32_MIN) {
                            sub_ht[sl] = {adsh, name};
                            break;
                        }
                        if (sub_ht[sl].adsh_code == adsh) break;  // already inserted
                    }
                }
            }
            row_off += bsz;
        }
    }

    // ── Phase: build pre IS hash via zone-map block skip ─────────────────────
    // Correctness fix: (adsh,tag,ver) can map to MULTIPLE plabels in pre.
    // We collect all IS entries, sort by key, then hash to start_idx.
    // Num scan iterates over all plabels per key → correct multi-group join.
    PreSlot* pre_ht = nullptr;
    std::vector<PreEntry> pre_entries;
    {
        GENDB_PHASE("build_joins");

        // Step 1: collect all pre IS entries into flat vector
        pre_entries.reserve(2000000);
        const uint32_t zm_nb = *reinterpret_cast<const uint32_t*>(pre_stmt_zm_raw);
        const ZM16*    zm    = reinterpret_cast<const ZM16*>(pre_stmt_zm_raw + 4);

        // pre sorted by stmt asc; IS (is_code=0) occupies first blocks
        size_t row_off = 0;
        for (uint32_t b = 0; b < zm_nb; b++) {
            uint32_t bsz = zm[b].block_size;
            if (zm[b].min_val > is_code) break;  // C19: sorted column, safe to break early
            if (zm[b].max_val >= is_code) {
                for (size_t r = row_off; r < row_off + bsz; r++) {
                    if (pre_stmt[r] != is_code) continue;
                    pre_entries.push_back({pre_adsh[r], pre_tag[r], pre_ver[r], pre_plabel[r]});
                }
            }
            row_off += bsz;
        }

        // Step 2: sort by (adsh, tag, version) so plabels for same key are contiguous
        // Use GNU parallel sort to exploit all cores (~1.73M entries, O(n log n))
        __gnu_parallel::sort(pre_entries.begin(), pre_entries.end(),
            [](const PreEntry& a, const PreEntry& b) {
                if (a.adsh_code    != b.adsh_code)    return a.adsh_code    < b.adsh_code;
                if (a.tag_code     != b.tag_code)     return a.tag_code     < b.tag_code;
                return a.version_code < b.version_code;
            });

        // Step 3: build hash (adsh, tag, ver) → start_idx into pre_entries
        pre_ht = new PreSlot[PRE_CAP];
        std::fill(pre_ht, pre_ht + PRE_CAP, PreSlot{INT32_MIN, INT32_MIN, INT32_MIN, -1});  // C20

        int32_t n_pre = (int32_t)pre_entries.size();
        for (int32_t idx = 0; idx < n_pre; idx++) {
            const PreEntry& e = pre_entries[idx];
            // Only insert the first occurrence of each (adsh, tag, ver) key
            if (idx > 0 &&
                pre_entries[idx-1].adsh_code    == e.adsh_code &&
                pre_entries[idx-1].tag_code     == e.tag_code  &&
                pre_entries[idx-1].version_code == e.version_code) {
                continue;  // already indexed via first occurrence
            }
            uint32_t h = pre_hash_fn(e.adsh_code, e.tag_code, e.version_code) & PRE_MASK;
            for (uint32_t p = 0; p < PRE_CAP; p++) {  // C24
                uint32_t sl = (h + p) & PRE_MASK;
                if (pre_ht[sl].adsh_code == INT32_MIN) {
                    pre_ht[sl] = {e.adsh_code, e.tag_code, e.version_code, idx};
                    break;
                }
            }
        }
    }

    // ── Phase: find USD segment in num via zone map ───────────────────────────
    size_t usd_start = num_n, usd_end = 0;
    {
        const uint32_t zm_nb = *reinterpret_cast<const uint32_t*>(num_uom_zm_raw);
        const ZM16*    zm    = reinterpret_cast<const ZM16*>(num_uom_zm_raw + 4);
        size_t row_off = 0;
        for (uint32_t b = 0; b < zm_nb; b++) {
            uint32_t bsz = zm[b].block_size;
            if (zm[b].min_val <= usd_code && usd_code <= zm[b].max_val) {
                if (row_off < usd_start) usd_start = row_off;
                usd_end = row_off + bsz;
            }
            row_off += bsz;
        }
    }

    // ── Phase: parallel num scan with thread-local aggregation ────────────────
    // P22: allocate + init inside parallel section for distributed page faults.
    // AGG_CAP=2M×64 threads×32 bytes=4GB; sequential init would stall on page faults.
    int nthreads = omp_get_max_threads();
    std::vector<AggSlot*> tl_agg(nthreads, nullptr);
    // Track occupied slot indices per thread to avoid scanning empty slots during merge
    std::vector<std::vector<uint32_t>> tl_occupied(nthreads);
    for (int t = 0; t < nthreads; t++) tl_occupied[t].reserve(65536);

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // Each thread allocates and initializes its own table (distributed page faults)
            tl_agg[tid] = new AggSlot[AGG_CAP];
            // C20: use std::fill for INT32_MIN sentinel (NOT memset)
            std::fill(tl_agg[tid], tl_agg[tid] + AGG_CAP,
                      AggSlot{INT32_MIN, 0, 0, 0, 0, 0LL, 0LL});
            #pragma omp barrier
            AggSlot* agg = tl_agg[tid];

            #pragma omp for schedule(dynamic, 65536)
            for (size_t i = usd_start; i < usd_end; i++) {
                // Fused filter: uom==USD
                if (num_uom[i] != usd_code) continue;

                // Fused filter: value IS NOT NULL (NULL=NaN, C29)
                double v = num_value[i];
                if (std::isnan(v)) continue;

                int32_t adsh = num_adsh[i];

                // Probe sub hash → get name_code
                int32_t name_code = INT32_MIN;
                {
                    uint32_t h = sub_hash_fn(adsh) & SUB_MASK;
                    for (uint32_t p = 0; p < SUB_CAP; p++) {  // C24
                        uint32_t sl = (h + p) & SUB_MASK;
                        if (sub_ht[sl].adsh_code == INT32_MIN) break;
                        if (sub_ht[sl].adsh_code == adsh) {
                            name_code = sub_ht[sl].name_code;
                            break;
                        }
                    }
                }
                if (name_code == INT32_MIN) continue;

                // Probe pre IS hash → get start_idx into pre_entries
                int32_t tag = num_tag[i];
                int32_t ver = num_ver[i];
                int32_t start_idx = -1;
                {
                    uint32_t h = pre_hash_fn(adsh, tag, ver) & PRE_MASK;
                    for (uint32_t p = 0; p < PRE_CAP; p++) {  // C24
                        uint32_t sl = (h + p) & PRE_MASK;
                        if (pre_ht[sl].adsh_code == INT32_MIN) break;
                        if (pre_ht[sl].adsh_code == adsh &&
                            pre_ht[sl].tag_code == tag &&
                            pre_ht[sl].version_code == ver) {
                            start_idx = pre_ht[sl].start_idx;
                            break;
                        }
                    }
                }
                if (start_idx < 0) continue;

                // C29: int64_t cents accumulation
                int64_t iv = llround(v * 100.0);

                // Iterate over ALL plabels for this (adsh, tag, ver) — correctness fix
                // pre_entries is sorted so same-key plabels are contiguous
                const int32_t n_pre_entries = (int32_t)pre_entries.size();
                const PreEntry* pe = pre_entries.data();
                for (int32_t k = start_idx; k < n_pre_entries; k++) {
                    if (pe[k].adsh_code    != adsh ||
                        pe[k].tag_code     != tag  ||
                        pe[k].version_code != ver) break;

                    int32_t plabel_code = pe[k].plabel_code;

                    // Accumulate into thread-local agg hash
                    uint32_t h = agg_hash_fn(name_code, is_code, tag, plabel_code) & AGG_MASK;
                    for (uint32_t p = 0; p < AGG_CAP; p++) {  // C24
                        uint32_t sl = (h + p) & AGG_MASK;
                        if (agg[sl].name_code == INT32_MIN) {
                            agg[sl].name_code   = name_code;
                            agg[sl].stmt_code   = is_code;
                            agg[sl].tag_code    = tag;
                            agg[sl].plabel_code = plabel_code;
                            agg[sl].sum_cents   = iv;
                            agg[sl].cnt         = 1;
                            tl_occupied[tid].push_back(sl);  // track new slot for fast merge
                            break;
                        }
                        // C15: check ALL group-by columns
                        if (agg[sl].name_code   == name_code &&
                            agg[sl].stmt_code   == is_code   &&
                            agg[sl].tag_code    == tag        &&
                            agg[sl].plabel_code == plabel_code) {
                            agg[sl].sum_cents += iv;
                            agg[sl].cnt++;
                            break;
                        }
                    }
                }
            }
        }
    }

    // ── Phase: merge thread-local agg maps — sort-merge approach ─────────────
    // Binary tree hash-merge scanned full 2M-slot tables (64MB each) for ~31K occupied
    // slots per thread → 97% wasted work + cache-miss-heavy random probes into 64MB DST.
    // New approach: collect only occupied slots (tracked during scan), parallel sort,
    // then linear reduce. Replaces random hash probing with sequential sort.
    // Expected: 500ms → ~50ms.
    std::vector<AggSlot> results;
    {
        GENDB_PHASE("aggregation_merge");

        // Collect only occupied slots using tracked indices (avoids scanning 2M empty slots)
        size_t total_occ = 0;
        for (int t = 0; t < nthreads; t++) total_occ += tl_occupied[t].size();

        results.reserve(total_occ);
        for (int t = 0; t < nthreads; t++) {
            AggSlot* tab = tl_agg[t];
            for (uint32_t idx : tl_occupied[t]) {
                results.push_back(tab[idx]);
            }
            delete[] tab;
            tl_agg[t] = nullptr;
        }
        tl_occupied.clear();

        // Parallel sort by (name_code, tag_code, plabel_code) to group same-key entries
        __gnu_parallel::sort(results.begin(), results.end(),
            [](const AggSlot& a, const AggSlot& b) {
                if (a.name_code   != b.name_code)   return a.name_code   < b.name_code;
                if (a.tag_code    != b.tag_code)     return a.tag_code    < b.tag_code;
                return a.plabel_code < b.plabel_code;
            });

        // Sequential reduce: merge consecutive equal-key entries (sorted guarantees contiguity)
        std::vector<AggSlot> merged;
        merged.reserve(results.size());  // upper bound
        for (const auto& s : results) {
            if (!merged.empty() &&
                merged.back().name_code   == s.name_code &&
                merged.back().tag_code    == s.tag_code  &&
                merged.back().plabel_code == s.plabel_code) {
                merged.back().sum_cents += s.sum_cents;
                merged.back().cnt       += s.cnt;
            } else {
                merged.push_back(s);
            }
        }
        results = std::move(merged);
    }

    // ── Phase: top-200 partial sort by total_value DESC (P6) ─────────────────
    {
        GENDB_PHASE("sort_topk");
        size_t k = std::min((size_t)200, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const AggSlot& a, const AggSlot& b) {
                return a.sum_cents > b.sum_cents;  // DESC
            });
        results.resize(k);
    }

    // ── Phase: CSV output ──────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); std::exit(1); }

        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");

        for (const auto& row : results) {
            // C18: decode codes to strings
            const std::string& name_s   = name_dict[row.name_code];
            const std::string& stmt_s   = stmt_dict[row.stmt_code];
            const std::string& tag_s    = tag_dict[row.tag_code];
            const std::string& plabel_s = plabel_dict[row.plabel_code];

            // C29: output int64_t cents as decimal with 2 places
            int64_t s = row.sum_cents;
            csv_field(f, name_s);  fputc(',', f);
            csv_field(f, stmt_s);  fputc(',', f);
            csv_field(f, tag_s);   fputc(',', f);
            csv_field(f, plabel_s); fputc(',', f);

            // Handle negative sums correctly
            if (s < 0) {
                int64_t abs_s = -s;
                fprintf(f, "-%lld.%02lld", (long long)(abs_s / 100), (long long)(abs_s % 100));
            } else {
                fprintf(f, "%lld.%02lld", (long long)(s / 100), (long long)(s % 100));
            }
            fprintf(f, ",%lld\n", (long long)row.cnt);
        }
        fclose(f);
    }

    // Cleanup
    delete[] sub_ht;
    delete[] pre_ht;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
