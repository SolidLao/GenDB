#include <iostream>
#include <cstdint>
#include <cstdio>
#include <cmath>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>
#include <unordered_map>

#include "timing_utils.h"

// ============================================================
// Hash functions
// ============================================================
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}
static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
}

// ============================================================
// Pre-built index slot structs
// ============================================================
struct SubADSHSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t row_id;
    int32_t _pad0;
    int32_t _pad1;
};  // 16 bytes

struct TagPairSlot {
    int32_t tag_code;   // INT32_MIN = empty
    int32_t ver_code;
    int32_t row_id;
    int32_t _pad;
};  // 16 bytes

struct PreTripleSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_id;     // FIRST row in sorted pre
};  // 16 bytes

// ============================================================
// Aggregation key
// ============================================================
struct AggKey {
    int32_t sic;
    int32_t tlabel_code;
    int16_t stmt_code;
    int16_t _pad;
};  // 12 bytes

static inline uint64_t hash_agg_key(const AggKey& k) {
    uint64_t h = hash_combine(hash_int32(k.sic), hash_int32(k.tlabel_code));
    h = hash_combine(h, hash_int32((int32_t)k.stmt_code));
    return h;
}

// ============================================================
// Thread-local agg slot — NO cik_vec (eliminates heap fragmentation).
// CIK tracking is done via a separate flat per-thread CIKEntry buffer.
// This removes the 128K random heap pointer chases that cost ~12ms during merge.
// ============================================================
static constexpr uint32_t AGG_CAP  = 4096;
static constexpr uint32_t AGG_MASK = AGG_CAP - 1;

struct TLAggSlot {
    AggKey  key;           // 12 bytes
    bool    occupied;      // 1 byte
    int8_t  _pad[3];       // 3 bytes → align to 8
    int64_t sum_cents;     // 8 bytes
    int64_t count;         // 8 bytes
};  // 32 bytes total — 64 threads × 4096 × 32 = 8MB fits in LLC

static inline void tl_agg_insert(TLAggSlot* ht, const AggKey& key, int64_t cents) {
    uint64_t h = hash_agg_key(key);
    uint32_t pos = (uint32_t)(h & AGG_MASK);
    for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
        uint32_t slot = (pos + probe) & AGG_MASK;
        if (!ht[slot].occupied) {
            ht[slot].occupied  = true;
            ht[slot].key       = key;
            ht[slot].sum_cents = cents;
            ht[slot].count     = 1;
            return;
        }
        const AggKey& k2 = ht[slot].key;
        if (k2.sic == key.sic && k2.tlabel_code == key.tlabel_code && k2.stmt_code == key.stmt_code) {
            ht[slot].sum_cents += cents;
            ht[slot].count     += 1;
            return;
        }
    }
    // Should never happen with proper sizing
}

// ============================================================
// CIK entry — flat contiguous buffer per thread.
// group_key = (int64_t)sic << 32 | (uint32_t)tlabel_code.
// stmt is always eq_code so no need to encode it.
// After scan: concatenate all buffers, sort by (group_key, cik),
// count distinct CIKs per group with a single linear pass.
// ============================================================
struct CIKEntry {
    int64_t group_key;  // (sic << 32) | tlabel_code
    int32_t cik;
    int32_t _pad;
};  // 16 bytes

// ============================================================
// mmap helper
// ============================================================
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { perror(path.c_str()); exit(1); }
    char buf[4096];
    while (fgets(buf, sizeof(buf), f)) {
        size_t len = strlen(buf);
        while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r')) buf[--len] = 0;
        dict.push_back(std::string(buf, len));
    }
    fclose(f);
    return dict;
}

// ============================================================
// Main query function
// ============================================================
void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // --------------------------------------------------------
    // Data loading
    // --------------------------------------------------------
    size_t sub_idx_sz, tag_idx_sz, pre_idx_sz;
    size_t num_adsh_sz, num_tag_sz, num_ver_sz, num_uom_sz, num_val_sz;
    size_t sub_sic_sz, sub_cik_sz;
    size_t tag_abstract_sz, tag_tlabel_sz;
    size_t pre_adsh_sz, pre_tag_sz, pre_ver_sz, pre_stmt_sz;

    const void* sub_raw_v;
    const void* tag_raw_v;
    const void* pre_raw_v;
    const int32_t* num_adsh;
    const int32_t* num_tag;
    const int32_t* num_ver;
    const int16_t* num_uom;
    const double*  num_val;
    const int32_t* sub_sic;
    const int32_t* sub_cik;
    const int32_t* tag_abstract;
    const int32_t* tag_tlabel;
    const int32_t* pre_adsh;
    const int32_t* pre_tag;
    const int32_t* pre_ver;
    const int16_t* pre_stmt;

    std::vector<std::string> uom_dict, stmt_dict, tlabel_dict;
    int16_t usd_code = -1, eq_code = -1;

    {
        GENDB_PHASE("data_loading");

        // Load dicts
        uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
        stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");

        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "USD") { usd_code = i; break; }
        for (int16_t i = 0; i < (int16_t)stmt_dict.size(); i++)
            if (stmt_dict[i] == "EQ") { eq_code = i; break; }

        // mmap indexes and columns
        sub_raw_v   = mmap_file(gendb_dir + "/sub/indexes/sub_adsh_hash.bin", sub_idx_sz);
        tag_raw_v   = mmap_file(gendb_dir + "/tag/indexes/tag_pair_hash.bin", tag_idx_sz);
        pre_raw_v   = mmap_file(gendb_dir + "/pre/indexes/pre_triple_hash.bin", pre_idx_sz);

        num_adsh    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/adsh.bin", num_adsh_sz));
        num_tag     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/tag.bin", num_tag_sz));
        num_ver     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/version.bin", num_ver_sz));
        num_uom     = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/num/uom.bin", num_uom_sz));
        num_val     = reinterpret_cast<const double*>(mmap_file(gendb_dir + "/num/value.bin", num_val_sz));

        sub_sic     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/sic.bin", sub_sic_sz));
        sub_cik     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/cik.bin", sub_cik_sz));

        tag_abstract = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/tag/abstract.bin", tag_abstract_sz));
        tag_tlabel  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/tag/tlabel.bin", tag_tlabel_sz));

        pre_adsh    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/adsh.bin", pre_adsh_sz));
        pre_tag     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/tag.bin", pre_tag_sz));
        pre_ver     = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/version.bin", pre_ver_sz));
        pre_stmt    = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/stmt.bin", pre_stmt_sz));

        // Concurrent madvise (P27)
        #pragma omp parallel sections
        {
            #pragma omp section
            { madvise((void*)pre_raw_v, pre_idx_sz, MADV_WILLNEED); }
            #pragma omp section
            { madvise((void*)num_val, num_val_sz, MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)num_adsh, num_adsh_sz, MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)num_tag, num_tag_sz, MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)num_ver, num_ver_sz, MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)num_uom, num_uom_sz, MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)tag_raw_v, tag_idx_sz, MADV_WILLNEED); }
            #pragma omp section
            { madvise((void*)sub_raw_v, sub_idx_sz, MADV_WILLNEED); }
        }
    }

    // --------------------------------------------------------
    // Parse index headers at function scope (C32)
    // --------------------------------------------------------
    const char* sub_raw = reinterpret_cast<const char*>(sub_raw_v);
    const char* tag_raw = reinterpret_cast<const char*>(tag_raw_v);
    const char* pre_raw = reinterpret_cast<const char*>(pre_raw_v);

    uint32_t sub_cap  = *(const uint32_t*)sub_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_raw + 4);

    uint32_t tph_cap  = *(const uint32_t*)tag_raw;
    uint32_t tph_mask = tph_cap - 1;
    const TagPairSlot* tph_ht = (const TagPairSlot*)(tag_raw + 4);

    uint32_t pth_cap  = *(const uint32_t*)pre_raw;
    uint32_t pth_mask = pth_cap - 1;
    const PreTripleSlot* pth_ht = (const PreTripleSlot*)(pre_raw + 4);

    int64_t num_N = (int64_t)(num_val_sz / sizeof(double));
    int64_t pre_N = (int64_t)(pre_stmt_sz / sizeof(int16_t));

    // --------------------------------------------------------
    // Allocate per-thread structures
    // --------------------------------------------------------
    int nthreads = omp_get_max_threads();

    // Thread-local agg maps (lean: no cik_vec, 32 bytes/slot)
    // 64 × 4096 × 32 = 8MB — fits in LLC
    std::vector<std::vector<TLAggSlot>> tl_maps(nthreads, std::vector<TLAggSlot>(AGG_CAP));
    for (int t = 0; t < nthreads; t++) {
        for (uint32_t s = 0; s < AGG_CAP; s++) {
            tl_maps[t][s].occupied = false;
        }
    }

    // Per-thread flat CIK buffers (contiguous, cache-friendly for merge)
    // Expected: ~148K total qualifying rows / 64 threads = ~2313 per thread
    std::vector<std::vector<CIKEntry>> tl_cik(nthreads);
    for (int t = 0; t < nthreads; t++) {
        tl_cik[t].reserve(4096);  // pre-allocate to avoid realloc
    }

    // --------------------------------------------------------
    // Main scan with thread-local aggregation
    // --------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 100000)
        for (int64_t i = 0; i < num_N; i++) {
            // Filter: uom == USD
            if (num_uom[i] != usd_code) continue;
            // Filter: value IS NOT NULL (C29: accumulate as int64_t cents)
            double v = num_val[i];
            if (std::isnan(v)) continue;

            int32_t adsh_c = num_adsh[i];
            int32_t tag_c  = num_tag[i];
            int32_t ver_c  = num_ver[i];

            // --- Probe sub_adsh_hash (4MB, L3-resident) ---
            uint64_t sh = hash_int32(adsh_c);
            uint32_t spos = (uint32_t)(sh & sub_mask);
            int32_t sub_row = -1;
            for (uint32_t probe = 0; probe < sub_cap; probe++) {
                uint32_t slot = (spos + probe) & sub_mask;
                if (sub_ht[slot].adsh_code == INT32_MIN) break;
                if (sub_ht[slot].adsh_code == adsh_c) {
                    sub_row = sub_ht[slot].row_id;
                    break;
                }
            }
            if (sub_row < 0) continue;

            // Filter: sic BETWEEN 4000 AND 4999 (threshold_constant=4000)
            int32_t sic = sub_sic[sub_row];
            if (sic < 4000 || sic > 4999) continue;
            int32_t cik = sub_cik[sub_row];

            // --- Prefetch tag and pre hash slots before probing ---
            // Both are large (64MB, 512MB) and will cause DRAM latency.
            // Issuing prefetches now overlaps latency with sub_sic/sub_cik reads.
            uint64_t th = hash_combine(hash_int32(tag_c), hash_int32(ver_c));
            uint32_t tpos = (uint32_t)(th & tph_mask);
            __builtin_prefetch(&tph_ht[tpos], 0, 0);

            uint64_t ph = hash_combine(hash_combine(hash_int32(adsh_c), hash_int32(tag_c)),
                                       hash_int32(ver_c));
            uint32_t ppos = (uint32_t)(ph & pth_mask);
            __builtin_prefetch(&pth_ht[ppos], 0, 0);

            // --- Probe tag_pair_hash (using pre-computed tpos) ---
            int32_t tag_row = -1;
            for (uint32_t probe = 0; probe < tph_cap; probe++) {
                uint32_t slot = (tpos + probe) & tph_mask;
                if (tph_ht[slot].tag_code == INT32_MIN) break;
                if (tph_ht[slot].tag_code == tag_c && tph_ht[slot].ver_code == ver_c) {
                    tag_row = tph_ht[slot].row_id;
                    break;
                }
            }
            if (tag_row < 0) continue;

            // Filter: abstract == 0
            if (tag_abstract[tag_row] != 0) continue;
            int32_t tlabel_c = tag_tlabel[tag_row];

            // --- Probe pre_triple_hash (using pre-computed ppos) ---
            int32_t first_pre_row = -1;
            for (uint32_t probe = 0; probe < pth_cap; probe++) {
                uint32_t slot = (ppos + probe) & pth_mask;
                if (pth_ht[slot].adsh_code == INT32_MIN) break;
                if (pth_ht[slot].adsh_code == adsh_c &&
                    pth_ht[slot].tag_code == tag_c &&
                    pth_ht[slot].ver_code == ver_c) {
                    first_pre_row = pth_ht[slot].row_id;
                    break;
                }
            }
            if (first_pre_row < 0) continue;

            // Scan forward in sorted pre for matching (adsh,tag,ver) with stmt=EQ
            int tid = omp_get_thread_num();
            int64_t cents = llround(v * 100.0);  // C29: fixed-point accumulation
            // group_key encodes (sic, tlabel_code); stmt always eq_code
            int64_t gk = ((int64_t)sic << 32) | (uint32_t)tlabel_c;

            for (int64_t r = first_pre_row;
                 r < pre_N &&
                 pre_adsh[r] == adsh_c &&
                 pre_tag[r]  == tag_c  &&
                 pre_ver[r]  == ver_c;
                 r++) {
                if (pre_stmt[r] != eq_code) continue;

                AggKey key;
                key.sic         = sic;
                key.tlabel_code = tlabel_c;
                key.stmt_code   = eq_code;  // always eq_code (C15: all GROUP BY cols)
                key._pad        = 0;

                // Aggregate sum/count (no cik tracking here)
                tl_agg_insert(tl_maps[tid].data(), key, cents);

                // Track CIK in flat contiguous buffer (no heap allocation per entry)
                tl_cik[tid].push_back({gk, cik, 0});
            }
        }
    }

    // --------------------------------------------------------
    // Merge — two phases, both cache-friendly
    // Phase 1: merge lean sum/count maps (no cik_vec to chase)
    // Phase 2: collect flat CIK entries, sort, count distinct per group
    // --------------------------------------------------------
    static constexpr uint32_t GLOBAL_CAP  = 8192;
    static constexpr uint32_t GLOBAL_MASK = GLOBAL_CAP - 1;

    struct GlobalVal {
        int64_t sum_cents = 0;
        int64_t count     = 0;
    };
    struct GlobalSlot {
        AggKey    key;
        GlobalVal val;
        bool      occupied = false;
    };
    std::vector<GlobalSlot> global_map(GLOBAL_CAP);

    auto global_insert = [&](const AggKey& key, int64_t sum_c, int64_t cnt) {
        uint64_t h = hash_agg_key(key);
        uint32_t pos = (uint32_t)(h & GLOBAL_MASK);
        for (uint32_t probe = 0; probe < GLOBAL_CAP; probe++) {
            uint32_t slot = (pos + probe) & GLOBAL_MASK;
            if (!global_map[slot].occupied) {
                global_map[slot].occupied       = true;
                global_map[slot].key            = key;
                global_map[slot].val.sum_cents  = sum_c;
                global_map[slot].val.count      = cnt;
                return;
            }
            const AggKey& k2 = global_map[slot].key;
            if (k2.sic == key.sic && k2.tlabel_code == key.tlabel_code && k2.stmt_code == key.stmt_code) {
                global_map[slot].val.sum_cents += sum_c;
                global_map[slot].val.count     += cnt;
                return;
            }
        }
    };

    // Count of distinct CIKs per group (keyed by group_key = sic<<32|tlabel_code)
    std::unordered_map<int64_t, size_t> cik_count_map;
    cik_count_map.reserve(4096);

    {
        GENDB_PHASE("aggregation_merge");

        // Phase 1: merge lean agg maps (8MB total, no heap chasing)
        for (int t = 0; t < nthreads; t++) {
            for (uint32_t s = 0; s < AGG_CAP; s++) {
                if (!tl_maps[t][s].occupied) continue;
                global_insert(tl_maps[t][s].key,
                              tl_maps[t][s].sum_cents,
                              tl_maps[t][s].count);
            }
        }

        // Phase 2: collect all CIK entries into one contiguous vector
        size_t total_cik_entries = 0;
        for (int t = 0; t < nthreads; t++) total_cik_entries += tl_cik[t].size();

        std::vector<CIKEntry> all_ciks;
        all_ciks.reserve(total_cik_entries);
        for (int t = 0; t < nthreads; t++) {
            all_ciks.insert(all_ciks.end(), tl_cik[t].begin(), tl_cik[t].end());
        }

        // Sort by (group_key, cik) — contiguous data, cache-friendly
        std::sort(all_ciks.begin(), all_ciks.end(),
                  [](const CIKEntry& a, const CIKEntry& b) {
                      if (a.group_key != b.group_key) return a.group_key < b.group_key;
                      return a.cik < b.cik;
                  });

        // Linear scan to count distinct CIKs per group
        if (!all_ciks.empty()) {
            int64_t cur_gk   = all_ciks[0].group_key;
            int32_t prev_cik = INT32_MIN;
            size_t  cnt      = 0;
            for (const CIKEntry& e : all_ciks) {
                if (e.group_key != cur_gk) {
                    cik_count_map[cur_gk] = cnt;
                    cur_gk   = e.group_key;
                    prev_cik = INT32_MIN;
                    cnt      = 0;
                }
                if (e.cik != prev_cik) {
                    cnt++;
                    prev_cik = e.cik;
                }
            }
            cik_count_map[cur_gk] = cnt;
        }
    }

    // --------------------------------------------------------
    // Apply HAVING, sort, LIMIT
    // --------------------------------------------------------
    struct ResultRow {
        int32_t sic;
        int32_t tlabel_code;
        int16_t stmt_code;
        size_t  num_companies;
        int64_t sum_cents;
        int64_t count;
    };

    std::vector<ResultRow> results;
    results.reserve(2048);

    for (uint32_t s = 0; s < GLOBAL_CAP; s++) {
        if (!global_map[s].occupied) continue;
        const GlobalSlot& gs = global_map[s];

        int64_t gk = ((int64_t)gs.key.sic << 32) | (uint32_t)gs.key.tlabel_code;
        auto it = cik_count_map.find(gk);
        size_t nc = (it != cik_count_map.end()) ? it->second : 0;

        // HAVING: COUNT(DISTINCT cik) >= 2
        if (nc < 2) continue;

        ResultRow row;
        row.sic           = gs.key.sic;
        row.tlabel_code   = gs.key.tlabel_code;
        row.stmt_code     = gs.key.stmt_code;
        row.num_companies = nc;
        row.sum_cents     = gs.val.sum_cents;
        row.count         = gs.val.count;
        results.push_back(row);
    }

    // Sort: total_value DESC, sic ASC, tlabel_code ASC (C33 stable tiebreaker)
    std::partial_sort(results.begin(),
                      results.begin() + std::min((size_t)500, results.size()),
                      results.end(),
                      [](const ResultRow& a, const ResultRow& b) {
                          if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
                          if (a.sic != b.sic) return a.sic < b.sic;
                          return a.tlabel_code < b.tlabel_code;
                      });

    size_t out_count = std::min((size_t)500, results.size());

    // --------------------------------------------------------
    // Output
    // --------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q4.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); exit(1); }

        fprintf(out, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");

        for (size_t i = 0; i < out_count; i++) {
            const ResultRow& r = results[i];
            int64_t sc     = r.sum_cents;
            int64_t sc_abs = std::abs(sc);
            // C29: print as fixed-point decimal, C31: double-quote string columns
            if (sc < 0) {
                fprintf(out, "%d,\"%s\",\"%s\",%zu,-%lld.%02lld,%.2f\n",
                        r.sic,
                        tlabel_dict[r.tlabel_code].c_str(),
                        stmt_dict[r.stmt_code].c_str(),
                        r.num_companies,
                        (long long)(sc_abs / 100),
                        (long long)(sc_abs % 100),
                        (double)sc / (100.0 * r.count));
            } else {
                fprintf(out, "%d,\"%s\",\"%s\",%zu,%lld.%02lld,%.2f\n",
                        r.sic,
                        tlabel_dict[r.tlabel_code].c_str(),
                        stmt_dict[r.stmt_code].c_str(),
                        r.num_companies,
                        (long long)(sc / 100),
                        (long long)(sc % 100),
                        (double)sc / (100.0 * r.count));
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
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
