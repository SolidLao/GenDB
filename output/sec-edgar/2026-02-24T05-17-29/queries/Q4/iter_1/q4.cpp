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
// SIC filter: tiny hash table adsh_code -> (sic, cik)
// Only rows with sic BETWEEN 4000 AND 4999.
// ~3532 entries; capacity = next_pow2(3532*2) = 8192 → 128KB, L2-resident
// ============================================================
static constexpr uint32_t SIC_CAP  = 8192;
static constexpr uint32_t SIC_MASK = SIC_CAP - 1;

struct SicSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t sic;
    int32_t cik;
    int32_t _pad;
};  // 16 bytes

// ============================================================
// Scan tuple: one per qualifying join result row
// Collected per-thread in flat vectors (no dynamic allocation)
// ============================================================
struct ScanTuple {
    int32_t sic;
    int32_t tlabel_code;
    int32_t cik;
    int32_t _pad;
    int64_t cents;  // C29: already converted via llround(v * 100.0)
};  // 24 bytes

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

        uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
        stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");

        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "USD") { usd_code = i; break; }
        for (int16_t i = 0; i < (int16_t)stmt_dict.size(); i++)
            if (stmt_dict[i] == "EQ") { eq_code = i; break; }

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
    // Pre-build SIC filter: tiny hash table adsh_code -> (sic, cik)
    // Contains only sub rows where sic BETWEEN 4000 AND 4999.
    // 128KB → L2-resident; replaces 33.6M sub_adsh_hash + sub_sic probes
    // with single L2-resident lookup per qualifying num row.
    // Correctness anchor: threshold_constant = 4000
    // --------------------------------------------------------
    std::vector<SicSlot> sic_ht(SIC_CAP, {INT32_MIN, 0, 0, 0});

    {
        GENDB_PHASE("pre_build_sic");
        // Sequential scan of sub_adsh_hash (262K slots, 4MB, fast sequential read)
        for (uint32_t s = 0; s < sub_cap; s++) {
            if (sub_ht[s].adsh_code == INT32_MIN) continue;
            int32_t sub_row = sub_ht[s].row_id;
            int32_t sic = sub_sic[sub_row];
            // Correctness anchor: threshold_constant = 4000; range 4000-4999
            if (sic < 4000 || sic > 4999) continue;
            int32_t adsh_c = sub_ht[s].adsh_code;
            int32_t cik    = sub_cik[sub_row];
            // Insert into sic_ht (adsh is unique in sub — no duplicates)
            uint32_t pos = (uint32_t)(hash_int32(adsh_c) & SIC_MASK);
            for (uint32_t probe = 0; probe < SIC_CAP; probe++) {  // C24: bounded
                uint32_t slot = (pos + probe) & SIC_MASK;
                if (sic_ht[slot].adsh_code == INT32_MIN) {
                    sic_ht[slot] = {adsh_c, sic, cik, 0};
                    break;
                }
            }
        }
    }

    // --------------------------------------------------------
    // Main scan with thread-local flat tuple collection
    // No dynamic allocation in hot path — eliminates unordered_set overhead
    // --------------------------------------------------------
    int nthreads = omp_get_max_threads();

    // Pre-allocate per-thread tuple vectors (no unordered_set, no heap allocation per row)
    // Expected ~148K total tuples / 64 threads ≈ 2.3K per thread; reserve 16K for headroom
    std::vector<std::vector<ScanTuple>> tl_tuples(nthreads);
    for (int t = 0; t < nthreads; t++) tl_tuples[t].reserve(16384);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < num_N; i++) {
            // Filter: uom == USD (12.8% rejection)
            if (num_uom[i] != usd_code) continue;
            // Filter: value IS NOT NULL (~2% rejection)
            double v = num_val[i];
            if (std::isnan(v)) continue;

            int32_t adsh_c = num_adsh[i];

            // --- Probe sic_ht (128KB, L2-resident) ---
            // Combines sub join + sic filter + cik lookup in one L2 probe.
            // Rejects 95.9% of rows here (sic outside 4000-4999 or adsh not in sub).
            uint32_t spos = (uint32_t)(hash_int32(adsh_c) & SIC_MASK);
            int32_t sic = INT32_MIN, cik = 0;
            for (uint32_t probe = 0; probe < SIC_CAP; probe++) {  // C24: bounded
                uint32_t slot = (spos + probe) & SIC_MASK;
                if (sic_ht[slot].adsh_code == INT32_MIN) break;
                if (sic_ht[slot].adsh_code == adsh_c) {
                    sic = sic_ht[slot].sic;
                    cik = sic_ht[slot].cik;
                    break;
                }
            }
            if (__builtin_expect(sic == INT32_MIN, 1)) continue;  // ~95.9% rejected

            // Only load tag/ver after sic passes (avoids loads for 95.9% of rows)
            int32_t tag_c = num_tag[i];
            int32_t ver_c = num_ver[i];

            // --- Probe tag_pair_hash ---
            uint64_t th = hash_combine(hash_int32(tag_c), hash_int32(ver_c));
            uint32_t tpos = (uint32_t)(th & tph_mask);
            int32_t tag_row = -1;
            for (uint32_t probe = 0; probe < tph_cap; probe++) {  // C24: bounded
                uint32_t slot = (tpos + probe) & tph_mask;
                if (tph_ht[slot].tag_code == INT32_MIN) break;
                if (tph_ht[slot].tag_code == tag_c && tph_ht[slot].ver_code == ver_c) {
                    tag_row = tph_ht[slot].row_id;
                    break;
                }
            }
            if (tag_row < 0) continue;

            // Filter: abstract == 0 (~2% rejection)
            if (tag_abstract[tag_row] != 0) continue;
            int32_t tlabel_c = tag_tlabel[tag_row];

            // --- Probe pre_triple_hash ---
            uint64_t ph = hash_combine(
                              hash_combine(hash_int32(adsh_c), hash_int32(tag_c)),
                              hash_int32(ver_c));
            uint32_t ppos = (uint32_t)(ph & pth_mask);
            int32_t first_pre_row = -1;
            for (uint32_t probe = 0; probe < pth_cap; probe++) {  // C24: bounded
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

            // C29: convert to cents once; accumulate as int64_t
            int64_t cents = llround(v * 100.0);
            int tid = omp_get_thread_num();

            // Scan forward in sorted pre for EQ rows (SQL join semantics)
            for (int64_t r = first_pre_row;
                 r < pre_N &&
                 pre_adsh[r] == adsh_c &&
                 pre_tag[r]  == tag_c  &&
                 pre_ver[r]  == ver_c;
                 r++) {
                if (pre_stmt[r] != eq_code) continue;
                // Append flat tuple — no hash map, no dynamic allocation
                tl_tuples[tid].push_back({sic, tlabel_c, cik, 0, cents});
            }
        }
    }

    // --------------------------------------------------------
    // Sort + single-pass aggregate (replaces unordered_set merge)
    // --------------------------------------------------------
    struct ResultRow {
        int32_t sic;
        int32_t tlabel_code;
        int16_t stmt_code;
        int16_t _pad;
        size_t  num_companies;
        int64_t sum_cents;
        int64_t count;
    };

    std::vector<ResultRow> results;

    {
        GENDB_PHASE("sort_aggregate");

        // Merge thread-local vectors into one flat array
        size_t total = 0;
        for (int t = 0; t < nthreads; t++) total += tl_tuples[t].size();

        std::vector<ScanTuple> all_tuples;
        all_tuples.reserve(total);
        for (int t = 0; t < nthreads; t++) {
            all_tuples.insert(all_tuples.end(),
                              tl_tuples[t].begin(), tl_tuples[t].end());
        }
        // Free thread-local memory before sort
        { std::vector<std::vector<ScanTuple>>().swap(tl_tuples); }

        // Sort by (sic, tlabel_code, cik): enables single-pass distinct-cik counting
        std::sort(all_tuples.begin(), all_tuples.end(),
                  [](const ScanTuple& a, const ScanTuple& b) {
                      if (a.sic         != b.sic)         return a.sic         < b.sic;
                      if (a.tlabel_code != b.tlabel_code) return a.tlabel_code < b.tlabel_code;
                      return a.cik < b.cik;
                  });

        // Single-pass aggregation: distinct CIK via sorted cik order
        results.reserve(2048);
        for (size_t i = 0; i < all_tuples.size(); ) {
            int32_t cur_sic    = all_tuples[i].sic;
            int32_t cur_tlabel = all_tuples[i].tlabel_code;
            int64_t sum_cents  = 0;
            int64_t count      = 0;
            int32_t prev_cik   = INT32_MIN;
            size_t  distinct   = 0;

            while (i < all_tuples.size() &&
                   all_tuples[i].sic         == cur_sic &&
                   all_tuples[i].tlabel_code == cur_tlabel) {
                sum_cents += all_tuples[i].cents;
                count++;
                if (all_tuples[i].cik != prev_cik) {
                    distinct++;
                    prev_cik = all_tuples[i].cik;
                }
                i++;
            }

            // HAVING: COUNT(DISTINCT cik) >= 2
            if (distinct >= 2) {
                ResultRow row;
                row.sic          = cur_sic;
                row.tlabel_code  = cur_tlabel;
                row.stmt_code    = eq_code;   // all qualifying rows have stmt='EQ'
                row._pad         = 0;
                row.num_companies = distinct;
                row.sum_cents    = sum_cents;
                row.count        = count;
                results.push_back(row);
            }
        }
    }

    // Sort: total_value DESC, sic ASC, tlabel_code ASC (C33 stable tiebreaker)
    std::partial_sort(results.begin(),
                      results.begin() + std::min((size_t)500, results.size()),
                      results.end(),
                      [](const ResultRow& a, const ResultRow& b) {
                          if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
                          if (a.sic       != b.sic)       return a.sic       < b.sic;
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
            double  avg    = (double)sc / (100.0 * (double)r.count);

            if (sc < 0) {
                // C31: double-quote all string columns (tlabels often contain commas)
                fprintf(out, "%d,\"%s\",\"%s\",%zu,-%lld.%02lld,%.2f\n",
                        r.sic,
                        tlabel_dict[r.tlabel_code].c_str(),
                        stmt_dict[r.stmt_code].c_str(),
                        r.num_companies,
                        (long long)(sc_abs / 100),
                        (long long)(sc_abs % 100),
                        avg);
            } else {
                fprintf(out, "%d,\"%s\",\"%s\",%zu,%lld.%02lld,%.2f\n",
                        r.sic,
                        tlabel_dict[r.tlabel_code].c_str(),
                        stmt_dict[r.stmt_code].c_str(),
                        r.num_companies,
                        (long long)(sc / 100),
                        (long long)(sc % 100),
                        avg);
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
