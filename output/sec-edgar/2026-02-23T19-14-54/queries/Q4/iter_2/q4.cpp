// Q4: SEC-EDGAR - sic 4000-4999, joins: num→sub→tag→pre(EQ), COUNT(DISTINCT cik), SUM/AVG(value)
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <omp.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "timing_utils.h"

// ─── Dictionary helpers ───────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

static int32_t find_code32(const std::vector<std::string>& d, const std::string& v) {
    for (int32_t i = 0; i < (int32_t)d.size(); i++) if (d[i] == v) return i;
    return -1;
}
static int16_t find_code16(const std::vector<std::string>& d, const std::string& v) {
    for (int32_t i = 0; i < (int32_t)d.size(); i++) if (d[i] == v) return (int16_t)i;
    return -1;
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const uint8_t* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const uint8_t*>(ptr);
}

// ─── Utility: next power of 2 ────────────────────────────────────────────────
static inline uint32_t next_pow2(uint32_t n) {
    n--; n |= n>>1; n |= n>>2; n |= n>>4; n |= n>>8; n |= n>>16; return n+1;
}

// ─── Pre-built index slot structs ────────────────────────────────────────────
struct TagSlot { int32_t tag_code; int32_t ver_code; uint32_t row_idx; };
struct AtvSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; };

// ─── Qualifying ADSH map (sub sic filter) ─────────────────────────────────────
static constexpr int32_t QA_SENTINEL = INT32_MIN;
struct QaEntry { int32_t adsh_code; int32_t sic; int32_t cik; };

// ─── Thread-local aggregation ─────────────────────────────────────────────────
// key = ((uint64_t)(uint32_t)sic << 32) | (uint32_t)tlabel_code
static constexpr uint64_t AGG_SENTINEL = UINT64_MAX;
static constexpr uint32_t AGG_CAP = 32768; // next_pow2(10000*2), C9

struct AggVal {
    int64_t sum_cents = 0;
    int64_t count     = 0;
    std::unordered_set<int32_t> ciks;
};

struct AggEntry {
    uint64_t key = AGG_SENTINEL;
    AggVal   val;
};

inline uint32_t agg_hash(uint64_t key, uint32_t mask) {
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFFu);
    uint32_t hi = (uint32_t)(key >> 32);
    return (hi * 2654435761u ^ lo * 1234567891u) & mask;
}

struct ThreadAgg {
    AggEntry table[AGG_CAP]; // Stack-like; heap allocated via new

    void insert(uint64_t key, int64_t cents, int32_t cik) {
        uint32_t h = agg_hash(key, AGG_CAP - 1);
        for (uint32_t p = 0; p < AGG_CAP; p++) { // C24: bounded
            uint32_t slot = (h + p) & (AGG_CAP - 1);
            if (table[slot].key == AGG_SENTINEL) {
                table[slot].key = key;
                table[slot].val.sum_cents = cents;
                table[slot].val.count     = 1;
                table[slot].val.ciks.insert(cik);
                return;
            }
            if (table[slot].key == key) {
                table[slot].val.sum_cents += cents;
                table[slot].val.count++;
                table[slot].val.ciks.insert(cik);
                return;
            }
        }
        // Should never happen with 10K groups and cap=32768
        fprintf(stderr, "AGG table full!\n"); abort();
    }
};

// ─── Result row for output ────────────────────────────────────────────────────
struct ResultRow {
    int32_t sic;
    int32_t tlabel_code;
    int64_t sum_cents;
    int64_t count_rows;
    int32_t num_companies;
};

// ─── Main query function ──────────────────────────────────────────────────────
void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Data Loading ──────────────────────────────────────────────────────────
    size_t sz;
    const int32_t* num_adsh  = nullptr;
    const int32_t* num_tag   = nullptr;
    const int32_t* num_ver   = nullptr;
    const int16_t* num_uom   = nullptr;
    const double*  num_value = nullptr;
    const int32_t* sub_sic   = nullptr;
    const int32_t* sub_cik   = nullptr;
    const int32_t* sub_adsh  = nullptr;
    const int32_t* tag_abstract = nullptr;
    const int32_t* tag_tlabel   = nullptr;
    size_t num_rows = 0, sub_rows = 0;
    const uint8_t* tag_tv_raw = nullptr;
    const uint8_t* pre_eq_raw = nullptr;

    {
        GENDB_PHASE("data_loading");
        { auto* p = mmap_file(gendb_dir + "/num/adsh.bin", sz);
          num_adsh = reinterpret_cast<const int32_t*>(p); num_rows = sz / 4; }
        { auto* p = mmap_file(gendb_dir + "/num/tag.bin", sz);
          num_tag = reinterpret_cast<const int32_t*>(p); }
        { auto* p = mmap_file(gendb_dir + "/num/version.bin", sz);
          num_ver = reinterpret_cast<const int32_t*>(p); }
        { auto* p = mmap_file(gendb_dir + "/num/uom.bin", sz);
          num_uom = reinterpret_cast<const int16_t*>(p); }
        { auto* p = mmap_file(gendb_dir + "/num/value.bin", sz);
          num_value = reinterpret_cast<const double*>(p); }

        { auto* p = mmap_file(gendb_dir + "/sub/sic.bin", sz);
          sub_sic = reinterpret_cast<const int32_t*>(p); sub_rows = sz / 4; }
        { auto* p = mmap_file(gendb_dir + "/sub/cik.bin", sz);
          sub_cik = reinterpret_cast<const int32_t*>(p); }
        { auto* p = mmap_file(gendb_dir + "/sub/adsh.bin", sz);
          sub_adsh = reinterpret_cast<const int32_t*>(p); }

        { auto* p = mmap_file(gendb_dir + "/tag/abstract.bin", sz);
          tag_abstract = reinterpret_cast<const int32_t*>(p); }
        { auto* p = mmap_file(gendb_dir + "/tag/tlabel.bin", sz);
          tag_tlabel = reinterpret_cast<const int32_t*>(p); }

        tag_tv_raw = mmap_file(gendb_dir + "/indexes/tag_tv_hash.bin", sz);
        pre_eq_raw = mmap_file(gendb_dir + "/indexes/pre_eq_hash.bin", sz);
    }

    // Load dictionaries (C2: runtime, never hardcode)
    auto uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
    auto tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");
    auto stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");

    int16_t usd_code = find_code16(uom_dict, "USD");
    // eq_code for output decoding only
    int16_t eq_code  = find_code16(stmt_dict, "EQ");
    std::string stmt_str = (eq_code >= 0 && (size_t)eq_code < stmt_dict.size())
                           ? stmt_dict[eq_code] : "EQ";

    // Parse pre-built index headers
    uint32_t tv_cap  = *reinterpret_cast<const uint32_t*>(tag_tv_raw);
    uint32_t tv_mask = tv_cap - 1;
    const TagSlot* tag_ht = reinterpret_cast<const TagSlot*>(tag_tv_raw + 4);

    uint32_t eq_cap  = *reinterpret_cast<const uint32_t*>(pre_eq_raw);
    uint32_t eq_mask = eq_cap - 1;
    const AtvSlot* eq_ht  = reinterpret_cast<const AtvSlot*>(pre_eq_raw + 4);

    // ── Dim Filter: build qualifying ADSH map ─────────────────────────────────
    // Scan sub, filter sic [4000,4999], store adsh→{sic,cik}
    uint32_t qa_cap  = next_pow2(4307 * 2); // 8192, C9
    uint32_t qa_mask = qa_cap - 1;
    std::vector<QaEntry> qa_map(qa_cap, {QA_SENTINEL, 0, 0}); // C20: not memset

    {
        GENDB_PHASE("dim_filter");
        for (uint32_t i = 0; i < (uint32_t)sub_rows; i++) {
            int32_t sic = sub_sic[i];
            if (sic < 4000 || sic > 4999) continue;
            int32_t adsh = sub_adsh[i];
            int32_t cik  = sub_cik[i];
            uint32_t h = ((uint32_t)adsh * 2654435761u) & qa_mask;
            for (uint32_t p = 0; p < qa_cap; p++) { // C24: bounded
                uint32_t slot = (h + p) & qa_mask;
                if (qa_map[slot].adsh_code == QA_SENTINEL) {
                    qa_map[slot] = {adsh, sic, cik};
                    break;
                }
                if (qa_map[slot].adsh_code == adsh) break; // duplicate
            }
        }
    }

    // ── Main Scan (parallel, morsel-driven) ───────────────────────────────────
    int nthreads = omp_get_max_threads();
    // Heap-allocate ThreadAgg to avoid stack overflow (each ~3.5MB)
    std::vector<std::unique_ptr<ThreadAgg>> thread_aggs(nthreads);
    for (int t = 0; t < nthreads; t++) {
        thread_aggs[t] = std::make_unique<ThreadAgg>();
        // Initialize all keys to AGG_SENTINEL (default AggEntry ctor does this)
    }

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            ThreadAgg& lagg = *thread_aggs[tid];

            #pragma omp for schedule(dynamic, 65536)
            for (int64_t i = 0; i < (int64_t)num_rows; i++) {
                // Filter: uom == 'USD'
                if (num_uom[i] != usd_code) continue;

                // Filter: value IS NOT NULL (NaN sentinel)
                double val = num_value[i];
                if (std::isnan(val)) continue;

                // Filter: adsh in qualifying set (sic 4000-4999)
                int32_t adsh_i = num_adsh[i];
                uint32_t h_qa = ((uint32_t)adsh_i * 2654435761u) & qa_mask;
                int32_t sic_i = -1, cik_i = -1;
                for (uint32_t p = 0; p < qa_cap; p++) { // C24
                    uint32_t slot = (h_qa + p) & qa_mask;
                    if (qa_map[slot].adsh_code == QA_SENTINEL) break;
                    if (qa_map[slot].adsh_code == adsh_i) {
                        sic_i = qa_map[slot].sic;
                        cik_i = qa_map[slot].cik;
                        break;
                    }
                }
                if (sic_i < 0) continue;

                // Probe tag_tv_hash for (tag, version) → tag_row
                // hash2: 64-bit intermediates + fold (must match build_indexes.cpp)
                int32_t tag_i = num_tag[i];
                int32_t ver_i = num_ver[i];
                uint64_t h_tv64 = (uint64_t)(uint32_t)tag_i * 2654435761u;
                h_tv64 ^= (uint64_t)(uint32_t)ver_i * 1234567891u;
                uint32_t h_tv = (uint32_t)(h_tv64 ^ (h_tv64 >> 32)) & tv_mask;
                uint32_t tag_row = UINT32_MAX;
                for (uint32_t p = 0; p < tv_cap; p++) { // C24
                    uint32_t slot = (h_tv + p) & tv_mask;
                    if (tag_ht[slot].tag_code == INT32_MIN) break;
                    if (tag_ht[slot].tag_code == tag_i && tag_ht[slot].ver_code == ver_i) {
                        tag_row = tag_ht[slot].row_idx;
                        break;
                    }
                }
                if (tag_row == UINT32_MAX) continue;
                // Filter: abstract == 0
                if (tag_abstract[tag_row] != 0) continue;
                int32_t tlabel_i = tag_tlabel[tag_row];

                // Probe pre_eq_hash: existence check for (adsh, tag, version) with stmt='EQ'
                // hash3: 64-bit intermediates + fold (must match build_indexes.cpp)
                uint64_t h_eq64 = (uint64_t)(uint32_t)adsh_i * 2654435761u;
                h_eq64 ^= (uint64_t)(uint32_t)tag_i  * 1234567891u;
                h_eq64 ^= (uint64_t)(uint32_t)ver_i  * 2246822519u;
                uint32_t h_eq = (uint32_t)(h_eq64 ^ (h_eq64 >> 32)) & eq_mask;
                bool found_eq = false;
                for (uint32_t p = 0; p < eq_cap; p++) { // C24
                    uint32_t slot = (h_eq + p) & eq_mask;
                    if (eq_ht[slot].adsh_code == INT32_MIN) break;
                    if (eq_ht[slot].adsh_code == adsh_i &&
                        eq_ht[slot].tag_code  == tag_i  &&
                        eq_ht[slot].ver_code  == ver_i) {
                        found_eq = true; break;
                    }
                }
                if (!found_eq) continue;

                // Aggregate: key = packed(sic, tlabel_code), C15
                int64_t cents = llround(val * 100.0); // C29: int64_t cents
                uint64_t key = ((uint64_t)(uint32_t)sic_i << 32) | (uint32_t)tlabel_i;
                lagg.insert(key, cents, cik_i);
            }
        }
    }

    // ── Aggregation Merge ─────────────────────────────────────────────────────
    std::unordered_map<uint64_t, AggVal> merged;
    merged.reserve(16384);

    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; t++) {
            const ThreadAgg& tagg = *thread_aggs[t];
            for (uint32_t s = 0; s < AGG_CAP; s++) {
                const AggEntry& e = tagg.table[s];
                if (e.key == AGG_SENTINEL) continue;
                AggVal& m = merged[e.key];
                m.sum_cents += e.val.sum_cents;
                m.count     += e.val.count;
                for (int32_t cik : e.val.ciks) m.ciks.insert(cik);
            }
        }
    }

    // ── Output ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // Apply HAVING COUNT(DISTINCT cik) >= 2 and collect
        std::vector<ResultRow> results;
        results.reserve(merged.size());
        for (auto& [key, val] : merged) {
            if ((int)val.ciks.size() < 2) continue;
            int32_t sic         = (int32_t)(key >> 32);
            int32_t tlabel_code = (int32_t)(key & 0xFFFFFFFFu);
            results.push_back({sic, tlabel_code, val.sum_cents, val.count,
                                (int32_t)val.ciks.size()});
        }

        // Sort by total_value DESC, LIMIT 500 (P6: partial_sort)
        int k = std::min((int)results.size(), 500);
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                return a.sum_cents > b.sum_cents;
            });
        results.resize(k);

        // Write CSV
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q4.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); exit(1); }

        fprintf(fout, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");

        for (const auto& row : results) {
            const char* tlabel = "";
            if (row.tlabel_code >= 0 && (size_t)row.tlabel_code < tlabel_dict.size())
                tlabel = tlabel_dict[row.tlabel_code].c_str();

            // Format total_value (int64_t cents → decimal, handle negatives)
            int64_t sc = row.sum_cents;
            char total_buf[32];
            if (sc < 0) {
                int64_t pos = -sc;
                snprintf(total_buf, sizeof(total_buf), "-%lld.%02lld",
                         (long long)(pos / 100), (long long)(pos % 100));
            } else {
                snprintf(total_buf, sizeof(total_buf), "%lld.%02lld",
                         (long long)(sc / 100), (long long)(sc % 100));
            }

            // avg_value: (double)sum_cents / 100.0 / count
            double avg_val = (double)row.sum_cents / 100.0 / (double)row.count_rows;

            fprintf(fout, "%d,%s,%s,%d,%s,%.6g\n",
                    row.sic, tlabel, stmt_str.c_str(),
                    row.num_companies, total_buf, avg_val);
        }

        fclose(fout);
        fprintf(stderr, "[Q4] Output %d rows to %s\n", k, out_path.c_str());
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
