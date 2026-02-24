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
#include <future>
#include <climits>

#include "timing_utils.h"

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
        // tlabel_dict deferred: loaded in background async to overlap with main_scan
        uom_dict  = load_dict(gendb_dir + "/num/uom_dict.txt");
        stmt_dict = load_dict(gendb_dir + "/pre/stmt_dict.txt");

        for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
            if (uom_dict[c] == "USD") { usd_code = c; break; }
        for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
            if (stmt_dict[c] == "EQ")  { eq_code  = c; break; }
    }

    // Start background async load of tlabel_dict — overlaps with main_scan + merge (49+10ms)
    auto tlabel_future = std::async(std::launch::async, [&]() {
        return load_dict(gendb_dir + "/tag/tlabel_dict.txt");
    });

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
    // Two-pass design: Phase 1 collects survivors (uom+sub filters) into a per-thread buffer
    // and emits __builtin_prefetch for the pre_join_index (710MB >> 44MB L3, every probe
    // is a DRAM miss). Phase 2 processes survivors with data already warm in L3/L2.
    int nthreads = omp_get_max_threads();
    std::vector<Q4Map> thread_maps(nthreads);

    {
        GENDB_PHASE("main_scan");

        struct Surv {
            int32_t adsh, tag, ver, sic, cik;
            int64_t cents;
        };

        #pragma omp parallel
        {
            int tid     = omp_get_thread_num();
            int nthr    = omp_get_num_threads();
            Q4Map& local_map = thread_maps[tid];
            local_map.reserve(4096);

            // Manual static partitioning (matches schedule(static) distribution)
            size_t chunk = (num_rows + (size_t)nthr - 1) / (size_t)nthr;
            size_t my_start = (size_t)tid * chunk;
            size_t my_end   = std::min(my_start + chunk, num_rows);

            // Phase 1: scan rows, apply cheap filters, collect survivors into buffer.
            // Defers the expensive pre_join_index (710MB) and tag_index (50MB) probes.
            std::vector<Surv> survivors;
            survivors.reserve((my_end - my_start) / 16 + 512);  // ~6.1% pass rate

            for (size_t i = my_start; i < my_end; ++i) {
                // Filter: uom == 'USD' (87.2% pass)
                if (num_uom[i] != usd_code) continue;

                int32_t adsh_code = num_adsh[i];

                // ---- Probe 1: L2-resident fsub table (sic 4000-4999 pre-filtered, 196KB) ----
                int32_t sic = -1, cik = -1;
                {
                    uint32_t slot = hash_i32(adsh_code) & FSUB_MASK;
                    for (uint32_t probe = 0; probe < FSUB_CAP; ++probe) {  // C24
                        uint32_t s = (slot + probe) & FSUB_MASK;
                        if (fsub[s].adsh_code == -1) break;
                        if (fsub[s].adsh_code == adsh_code) {
                            sic = fsub[s].sic;
                            cik = fsub[s].cik;
                            break;
                        }
                    }
                }
                if (sic < 0) continue;  // 93.9% rejection

                double v = num_value[i];
                if (std::isnan(v)) continue;  // 1% rejection

                int32_t tag_code     = num_tag[i];
                int32_t version_code = num_version[i];

                // Issue prefetch for pre_join_index slot for THIS survivor.
                // It will be used PDIST survivors later (by the time Phase 2 processes it),
                // giving DRAM latency (70ns) time to resolve.
                {
                    uint32_t pslot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
                    __builtin_prefetch(&pre_ht[pslot], 0, 0);
                    // Also prefetch tag_index slot (50MB, also > L3)
                    uint32_t tslot = hash_i32x2(tag_code, version_code) & tag_mask;
                    __builtin_prefetch(&tag_ht[tslot], 0, 0);
                }

                survivors.push_back({adsh_code, tag_code, version_code, sic, cik,
                                     llround(v * 100.0)});
            }

            // Phase 2: process survivors — pre_join_index and tag_index data is warmer now.
            // Use look-ahead prefetch to continue warming cache for upcoming survivors.
            const int PDIST = 24;  // look-ahead distance (tuned: ~24 survivors ≈ 1.2µs gap)
            const int nsurv = (int)survivors.size();

            for (int j = 0; j < nsurv; ++j) {
                // Issue prefetch PDIST entries ahead for both large tables
                if (j + PDIST < nsurv) {
                    const Surv& fs = survivors[j + PDIST];
                    uint32_t pslot = hash_i32x3(fs.adsh, fs.tag, fs.ver) & pre_mask;
                    __builtin_prefetch(&pre_ht[pslot], 0, 0);
                    uint32_t tslot = hash_i32x2(fs.tag, fs.ver) & tag_mask;
                    __builtin_prefetch(&tag_ht[tslot], 0, 0);
                }

                const Surv& s = survivors[j];

                // ---- Probe 2: pre_join_index → stmt == 'EQ' ----
                uint32_t eq_match_count = 0;
                {
                    uint32_t slot = hash_i32x3(s.adsh, s.tag, s.ver) & pre_mask;
                    for (uint32_t probe = 0; probe < pre_cap; ++probe) {  // C24
                        uint32_t ps = (slot + probe) & pre_mask;
                        if (pre_ht[ps].adsh_code == -1) break;
                        if (pre_ht[ps].adsh_code == s.adsh &&
                            pre_ht[ps].tag_code   == s.tag  &&
                            pre_ht[ps].version_code == s.ver) {
                            for (uint32_t pi = 0; pi < pre_ht[ps].payload_count; ++pi) {
                                uint32_t pre_row = pre_pool[pre_ht[ps].payload_offset + pi];
                                if (pre_stmt[pre_row] == eq_code) ++eq_match_count;
                            }
                            break;
                        }
                    }
                }
                if (eq_match_count == 0) continue;  // 88.8% rejection

                // ---- Probe 3: tag_index → abstract == 0, get tlabel_code ----
                int32_t tag_row = -1;
                {
                    uint32_t slot = hash_i32x2(s.tag, s.ver) & tag_mask;
                    for (uint32_t probe = 0; probe < tag_cap; ++probe) {  // C24
                        uint32_t ts = (slot + probe) & tag_mask;
                        if (tag_ht[ts].tag_code == -1) break;
                        if (tag_ht[ts].tag_code == s.tag && tag_ht[ts].version_code == s.ver) {
                            tag_row = tag_ht[ts].tag_row;
                            break;
                        }
                    }
                }
                if (tag_row < 0) continue;
                if (tag_abstract[tag_row] != 0) continue;  // 5% rejection
                int32_t tlabel_code = tag_tlabel[tag_row];

                // ---- Aggregate (C29: int64_t cents, C15: all 3 GROUP BY keys) ----
                Q4Key key{ s.sic, tlabel_code, eq_code };
                Q4Value& agg = local_map[key];
                agg.sum_cents += s.cents * (int64_t)eq_match_count;
                agg.cnt       += (int64_t)eq_match_count;
                // COUNT(DISTINCT cik): flat vector dedup (small cardinality per group)
                bool found = false;
                for (int32_t c : agg.ciks) { if (c == s.cik) { found = true; break; } }
                if (!found) agg.ciks.push_back(s.cik);
            }
        }
    }

    // ---- Merge thread-local maps (P17/P20) ----
    // CIK dedup strategy: concatenate all per-group CIK vectors during merge (O(1) per entry),
    // then sort+unique each group's cik list after all maps are merged (O(k log k) once).
    // This eliminates the O(n²) linear search across 64 thread contributions per group.
    Q4Map final_map;
    final_map.reserve(4096);
    {
        GENDB_PHASE("aggregation_merge");
        for (auto& local_map : thread_maps) {
            for (auto& [key, val] : local_map) {
                Q4Value& agg = final_map[key];
                agg.sum_cents += val.sum_cents;
                agg.cnt       += val.cnt;
                // Concatenate without dedup — sort+unique applied after full merge
                for (int32_t cik : val.ciks) agg.ciks.push_back(cik);
            }
        }
        // Single sort+unique pass per group — O(k log k) vs O(k²) for 64-thread merge
        for (auto& [key, val] : final_map) {
            std::sort(val.ciks.begin(), val.ciks.end());
            val.ciks.erase(std::unique(val.ciks.begin(), val.ciks.end()), val.ciks.end());
        }
    }

    // ---- Output ----
    // Retrieve tlabel_dict (started async after data_loading, overlapped with main_scan+merge)
    tlabel_dict = tlabel_future.get();
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
