#include <iostream>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <fstream>
#include <filesystem>
#include <climits>

#include "timing_utils.h"

namespace {

// ==================== Index slot layouts ====================
struct SubSlot { int32_t adsh_code; int32_t sub_row; };
struct TagSlot { int32_t tag_code; int32_t version_code; int32_t tag_row; };
struct PreSlot {
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  version_code;
    uint32_t payload_offset;
    uint32_t payload_count;
};

// ==================== Hash functions (verbatim from build_indexes.cpp) ====================
static inline uint32_t hash_i32(int32_t k) {
    uint32_t x = (uint32_t)k;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint32_t hash_i32x2(int32_t a, int32_t b) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL
               ^ (uint64_t)(uint32_t)b * 2246822519ULL;
    return (uint32_t)(h ^ (h >> 32));
}

static inline uint32_t hash_i32x3(int32_t a, int32_t b, int32_t c) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
    h ^= (uint64_t)(uint32_t)b * 2246822519ULL;
    h ^= (uint64_t)(uint32_t)c * 3266489917ULL;
    return (uint32_t)(h ^ (h >> 32));
}

// ==================== Filtered-sub lookup (pre-filter sic BETWEEN 4000-4999) ====================
// 16384 slots × 12 bytes = 196KB → fits in L2 cache (per-core private)
struct FilteredSubSlot {
    int32_t adsh_code;  // -1 = empty sentinel
    int32_t sic;        // pre-loaded sic value (GROUP BY key)
    int32_t cik;        // pre-loaded cik value (COUNT DISTINCT)
};

// ==================== Aggregation key/value ====================
// C15: ALL THREE GROUP BY dimensions in key (sic, tlabel_code, stmt_code)
struct Q4Key {
    int32_t sic;
    int32_t tlabel_code;
    int16_t stmt_code;
    bool operator==(const Q4Key& o) const {
        return sic == o.sic && tlabel_code == o.tlabel_code && stmt_code == o.stmt_code;
    }
};

struct Q4KeyHash {
    size_t operator()(const Q4Key& k) const {
        uint64_t h = (uint64_t)(uint32_t)k.sic * 2654435761ULL;
        h ^= (uint64_t)(uint32_t)k.tlabel_code * 2246822519ULL;
        h ^= (uint64_t)(uint16_t)k.stmt_code   * 3266489917ULL;
        return (size_t)(h ^ (h >> 32));
    }
};

struct Q4Value {
    int64_t sum_cents = 0;   // C29: int64_t cents accumulator
    int64_t cnt       = 0;
    std::vector<int32_t> ciks;  // flat unsorted unique CIK list (small per group, dedup inline)
};

using Q4Map = std::unordered_map<Q4Key, Q4Value, Q4KeyHash>;

// ==================== Utilities ====================
// mmap_file: no MAP_POPULATE — on hot runs page faults are near-zero (soft faults
// happen lazily during parallel scan, hidden by compute). MAP_POPULATE adds 50-100ms
// of serial page-table-setup overhead for ~950MB of column data.
static const void* mmap_file(const std::string& path, size_t& out_size, bool sequential = false) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    if (sequential && st.st_size > 0)
        madvise(ptr, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return ptr;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(std::move(line));
    return dict;
}

// ==================== Query ====================
} // end anonymous namespace

void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Data loading ----
    const int32_t* num_adsh    = nullptr;
    const int32_t* num_tag     = nullptr;
    const int32_t* num_version = nullptr;
    const int16_t* num_uom     = nullptr;
    const double*  num_value   = nullptr;
    size_t num_rows = 0;

    const int32_t* sub_sic = nullptr;
    const int32_t* sub_cik = nullptr;
    const int32_t* tag_abstract = nullptr;
    const int32_t* tag_tlabel   = nullptr;
    const int16_t* pre_stmt     = nullptr;

    const char* sub_idx_raw = nullptr;
    const char* tag_idx_raw = nullptr;
    const char* pre_idx_raw = nullptr;

    std::vector<std::string> uom_dict, stmt_dict, tlabel_dict;
    int16_t usd_code = -1, eq_code = -1;

    {
        GENDB_PHASE("data_loading");
        size_t sz_adsh, sz_tag, sz_ver, sz_uom, sz_val;
        size_t sz_ssic, sz_scik, sz_tabs, sz_tlab, sz_pstmt;
        size_t sz_sidx, sz_tidx, sz_pidx;

        // Parallel mmap: overlap kernel VMA setup across columns
        #pragma omp parallel sections
        {
            #pragma omp section
            { num_adsh    = (const int32_t*)mmap_file(gendb_dir + "/num/adsh.bin",    sz_adsh, true); }
            #pragma omp section
            { num_tag     = (const int32_t*)mmap_file(gendb_dir + "/num/tag.bin",     sz_tag, true); }
            #pragma omp section
            { num_version = (const int32_t*)mmap_file(gendb_dir + "/num/version.bin", sz_ver, true); }
            #pragma omp section
            { num_uom     = (const int16_t*)mmap_file(gendb_dir + "/num/uom.bin",     sz_uom, true); }
            #pragma omp section
            { num_value   = (const double* )mmap_file(gendb_dir + "/num/value.bin",   sz_val, true); }
            #pragma omp section
            { sub_sic     = (const int32_t*)mmap_file(gendb_dir + "/sub/sic.bin",      sz_ssic); }
            #pragma omp section
            { sub_cik     = (const int32_t*)mmap_file(gendb_dir + "/sub/cik.bin",      sz_scik); }
            #pragma omp section
            { tag_abstract= (const int32_t*)mmap_file(gendb_dir + "/tag/abstract.bin", sz_tabs); }
            #pragma omp section
            { tag_tlabel  = (const int32_t*)mmap_file(gendb_dir + "/tag/tlabel.bin",   sz_tlab); }
            #pragma omp section
            { pre_stmt    = (const int16_t*)mmap_file(gendb_dir + "/pre/stmt.bin",     sz_pstmt); }
            #pragma omp section
            { sub_idx_raw = (const char*)mmap_file(gendb_dir + "/indexes/sub_adsh_index.bin", sz_sidx); }
            #pragma omp section
            { tag_idx_raw = (const char*)mmap_file(gendb_dir + "/indexes/tag_index.bin",      sz_tidx); }
            #pragma omp section
            { pre_idx_raw = (const char*)mmap_file(gendb_dir + "/indexes/pre_join_index.bin", sz_pidx); }
        }
        num_rows = sz_adsh / sizeof(int32_t);

        // Load dictionaries and find codes (C2)
        uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
        stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");

        for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
            if (uom_dict[c] == "USD") { usd_code = c; break; }
        for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
            if (stmt_dict[c] == "EQ")  { eq_code  = c; break; }
    }

    // ---- Parse index headers at function scope (C27, C32) ----
    uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
    const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);

    uint32_t tag_cap  = *(const uint32_t*)tag_idx_raw;
    uint32_t tag_mask = tag_cap - 1;
    const TagSlot* tag_ht = (const TagSlot*)(tag_idx_raw + 4);

    uint32_t pre_cap  = *(const uint32_t*)pre_idx_raw;
    uint32_t pre_mask = pre_cap - 1;
    const PreSlot*   pre_ht   = (const PreSlot*)(pre_idx_raw + 4);
    const uint32_t*  pre_pool = (const uint32_t*)(pre_idx_raw + 4 + (size_t)pre_cap * 20);

    // ---- Build pre-filtered sub hash table (sic BETWEEN 4000-4999 only) ----
    // Only ~5250 qualifying entries → 16384 slots × 12 bytes = 196KB → fits in L2 cache.
    // Replaces 2MB sub_adsh_index probe per num row with L2-resident lookup.
    // Embeds sic + cik inline: no secondary lookups into sub_sic[]/sub_cik[].
    static constexpr uint32_t FSUB_CAP  = 16384;
    static constexpr uint32_t FSUB_MASK = FSUB_CAP - 1;
    std::vector<FilteredSubSlot> fsub_ht(FSUB_CAP, {-1, 0, 0});

    for (uint32_t i = 0; i < sub_cap; ++i) {
        if (sub_ht[i].adsh_code == -1) continue;
        int32_t srow = sub_ht[i].sub_row;
        int32_t sic  = sub_sic[srow];
        if (sic < 4000 || sic > 4999) continue;
        int32_t adsh_code = sub_ht[i].adsh_code;
        int32_t cik       = sub_cik[srow];
        uint32_t slot = hash_i32(adsh_code) & FSUB_MASK;
        for (uint32_t p = 0; p < FSUB_CAP; ++p) {
            uint32_t s = (slot + p) & FSUB_MASK;
            if (fsub_ht[s].adsh_code == -1) {
                fsub_ht[s] = {adsh_code, sic, cik};
                break;
            }
        }
    }
    const FilteredSubSlot* fsub = fsub_ht.data();

    // ---- Main scan + thread-local aggregation (P17/P20) ----
    int nthreads = omp_get_max_threads();
    std::vector<Q4Map> thread_maps(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            Q4Map& local_map = thread_maps[tid];
            local_map.reserve(4096);  // pre-size to avoid rehashing (medium group cardinality)

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < num_rows; ++i) {
                // Filter: uom == 'USD' (C2) — cheapest first, 87.2% pass
                if (num_uom[i] != usd_code) continue;

                int32_t adsh_code = num_adsh[i];

                // ---- Probe 1: filtered sub table (sic BETWEEN 4000-4999 pre-filtered) ----
                // 196KB table → L2-resident; 93.9% of probes hit sentinel quickly.
                // Embeds sic + cik → no secondary lookups needed.
                // tag/version/value deferred: 93.9% rejection avoids loading 600MB of columns.
                int32_t sic = -1, cik = -1;
                {
                    uint32_t slot = hash_i32(adsh_code) & FSUB_MASK;
                    for (uint32_t probe = 0; probe < FSUB_CAP; ++probe) {  // C24: bounded loop
                        uint32_t s = (slot + probe) & FSUB_MASK;
                        if (fsub[s].adsh_code == -1) break;
                        if (fsub[s].adsh_code == adsh_code) {
                            sic = fsub[s].sic;
                            cik = fsub[s].cik;
                            break;
                        }
                    }
                }
                if (sic < 0) continue;  // not in sic 4000-4999 (93.9% rejection)

                // Load value, tag, version only for the ~6.1% of rows that pass sub filter.
                // Saves ~540MB of memory bandwidth per scan (value=300MB, tag=150MB, ver=150MB).
                double v = num_value[i];
                if (std::isnan(v)) continue;

                int32_t tag_code     = num_tag[i];
                int32_t version_code = num_version[i];

                // ---- Probe 2: pre_join_index → stmt == 'EQ' (second most selective) ----
                // SQL JOIN pre produces one result row PER matching pre row.
                // If 2 pre rows match stmt='EQ', n.value is contributed TWICE to SUM/cnt.
                uint32_t eq_match_count = 0;
                {
                    uint32_t slot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
                    for (uint32_t probe = 0; probe < pre_cap; ++probe) {  // C24: bounded loop
                        uint32_t s = (slot + probe) & pre_mask;
                        if (pre_ht[s].adsh_code == -1) break;
                        if (pre_ht[s].adsh_code == adsh_code &&
                            pre_ht[s].tag_code == tag_code &&
                            pre_ht[s].version_code == version_code) {
                            for (uint32_t pi = 0; pi < pre_ht[s].payload_count; ++pi) {
                                uint32_t pre_row = pre_pool[pre_ht[s].payload_offset + pi];
                                if (pre_stmt[pre_row] == eq_code) {
                                    ++eq_match_count;
                                }
                            }
                            break;
                        }
                    }
                }
                if (eq_match_count == 0) continue;  // selectivity 0.112

                // ---- Probe 3: tag_index → abstract == 0, get tlabel_code (least selective) ----
                int32_t tag_row = -1;
                {
                    uint32_t slot = hash_i32x2(tag_code, version_code) & tag_mask;
                    for (uint32_t probe = 0; probe < tag_cap; ++probe) {  // C24: bounded loop
                        uint32_t s = (slot + probe) & tag_mask;
                        if (tag_ht[s].tag_code == -1) break;
                        if (tag_ht[s].tag_code == tag_code &&
                            tag_ht[s].version_code == version_code) {
                            tag_row = tag_ht[s].tag_row;
                            break;
                        }
                    }
                }
                if (tag_row < 0) continue;
                if (tag_abstract[tag_row] != 0) continue;  // selectivity 0.95
                int32_t tlabel_code = tag_tlabel[tag_row];

                // ---- Aggregate (C29: int64_t cents) ----
                // Multiply by eq_match_count: SQL JOIN semantics require one contribution
                // per matching pre row (e.g. 2 pre rows with stmt='EQ' → value added twice)
                int64_t iv = llround(v * 100.0);

                // C15: key must include ALL THREE GROUP BY dimensions
                Q4Key key{ sic, tlabel_code, eq_code };
                Q4Value& agg = local_map[key];
                agg.sum_cents += iv * (int64_t)eq_match_count;
                agg.cnt       += (int64_t)eq_match_count;
                // COUNT(DISTINCT cik): flat vector dedup (small per group, O(n) linear OK)
                bool found = false;
                for (int32_t c : agg.ciks) { if (c == cik) { found = true; break; } }
                if (!found) agg.ciks.push_back(cik);
            }
        }
    }

    // ---- Merge thread-local maps (P17/P20) ----
    Q4Map final_map;
    final_map.reserve(4096);
    {
        GENDB_PHASE("aggregation_merge");
        for (auto& local_map : thread_maps) {
            for (auto& [key, val] : local_map) {
                Q4Value& agg = final_map[key];
                agg.sum_cents += val.sum_cents;
                agg.cnt       += val.cnt;
                // Merge flat CIK vectors: linear dedup (small cardinality per group)
                for (int32_t cik : val.ciks) {
                    bool found = false;
                    for (int32_t c : agg.ciks) { if (c == cik) { found = true; break; } }
                    if (!found) agg.ciks.push_back(cik);
                }
            }
        }
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");

        struct ResultRow {
            int32_t sic;
            int32_t tlabel_code;
            int16_t stmt_code;
            int64_t sum_cents;
            int64_t cnt;
            size_t  num_companies;
        };

        std::vector<ResultRow> results;
        results.reserve(final_map.size());

        // Apply HAVING: COUNT(DISTINCT cik) >= 2
        for (auto& [key, val] : final_map) {
            if (val.ciks.size() < 2) continue;
            results.push_back({
                key.sic, key.tlabel_code, key.stmt_code,
                val.sum_cents, val.cnt,
                val.ciks.size()
            });
        }

        // Sort: total_value DESC, sic ASC tiebreaker (C33)
        size_t limit = std::min((size_t)500, results.size());
        std::partial_sort(results.begin(), results.begin() + limit, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
                return a.sic < b.sic;  // C33: secondary sort for determinism
            });

        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q4.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); exit(1); }

        fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");

        for (size_t i = 0; i < limit; ++i) {
            const ResultRow& r = results[i];
            const char* tlabel = tlabel_dict[r.tlabel_code].c_str();  // C18
            const char* stmt   = stmt_dict[r.stmt_code].c_str();       // C18

            // SUM format: handle sign correctly
            int64_t sc = r.sum_cents;
            long long sc_abs = (sc < 0) ? -(long long)sc : (long long)sc;
            long long sc_int  = sc_abs / 100;
            long long sc_frac = sc_abs % 100;

            // AVG format
            double avg = (double)r.sum_cents / (double)r.cnt / 100.0;

            // C31: double-quote ALL string columns (tlabel may contain commas)
            if (sc < 0) {
                fprintf(out, "%d,\"%s\",\"%s\",%zu,-%lld.%02lld,%.2f\n",
                        r.sic, tlabel, stmt, r.num_companies,
                        sc_int, sc_frac, avg);
            } else {
                fprintf(out, "%d,\"%s\",\"%s\",%zu,%lld.%02lld,%.2f\n",
                        r.sic, tlabel, stmt, r.num_companies,
                        sc_int, sc_frac, avg);
            }
        }
        fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
