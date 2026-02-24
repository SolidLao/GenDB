// Q4 — SIC/tlabel group-by with COUNT(DISTINCT cik), SUM, AVG
// Joins: num ⋈ sub ⋈ tag ⋈ pre
// Filters: uom=USD, sic BETWEEN 4000 AND 4999, stmt=EQ, abstract=0
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unordered_map>
// unordered_set removed — using custom open-addressing + sort-dedup for distinct CIK
#include <vector>
#include <omp.h>

#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { std::cerr << "Cannot open dict: " << path << "\n"; exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

static uint64_t next_pow2(uint64_t n) {
    uint64_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

static void* mmap_ro(const std::string& path, size_t& out_bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_bytes = (size_t)st.st_size;
    void* p = mmap(nullptr, out_bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    close(fd);
    return p;
}

// ---------------------------------------------------------------------------
// Tagver pre-built hash index structs
// ---------------------------------------------------------------------------
struct TagverSlot {
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_idx;
    int32_t valid;   // 0 = empty
};

// ---------------------------------------------------------------------------
// Pre hash map: key=(adsh,tag,ver) → count of EQ rows in pre
// Entry: k1=((uint64_t)(uint32_t)adsh<<32)|(uint32_t)tag, k2=(uint32_t)ver
// count=0 means empty slot; count>=1 is number of matching EQ pre rows
// ---------------------------------------------------------------------------
struct PreSlot {
    uint64_t k1;
    uint32_t k2;
    uint32_t count; // 0=empty; >=1 = number of EQ pre rows for this key
};

// ---------------------------------------------------------------------------
// Aggregation per group — custom open-addressing slot (no heap alloc)
// ---------------------------------------------------------------------------
struct AggSlot {
    uint64_t key;     // UINT64_MAX = empty sentinel
    double   sum_val;
    int64_t  cnt_val;
};

static constexpr uint64_t AGG_EMPTY = UINT64_MAX;
static constexpr uint64_t AGG_CAP   = 262144ULL; // next_pow2(100000 * 2); est ~100K worst-case (sic,tlabel) groups
static constexpr uint64_t AGG_MASK  = AGG_CAP - 1ULL;

// Open-addressing lookup/insert — returns slot reference (C24: bounded)
static inline AggSlot& agg_get(std::vector<AggSlot>& slots, uint64_t key) {
    uint64_t h = key * 11400714819323198485ULL;
    for (uint64_t p = 0; p < AGG_CAP; p++) {
        AggSlot& s = slots[(h + p) & AGG_MASK];
        if (s.key == AGG_EMPTY) { s.key = key; s.sum_val = 0.0; s.cnt_val = 0; return s; }
        if (s.key == key)       return s;
    }
    __builtin_unreachable();
}

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // data_loading: dictionaries + mmap columns
    // -----------------------------------------------------------------------
    size_t sz_tmp;
    const int32_t *num_adsh, *num_uom, *num_tag, *num_ver;
    const double  *num_value;
    const int32_t *sub_sic_raw, *sub_cik_raw;
    const int32_t *tag_abstract_col, *tag_tlabel_col;
    const uint8_t *tagver_raw;
    const int32_t *pre_adsh_col, *pre_tag_col, *pre_ver_col, *pre_stmt_col;
    size_t num_rows, sub_rows, pre_rows;

    std::vector<std::string> uom_dict, stmt_dict, tlabel_dict;

    {
        GENDB_PHASE("data_loading");

        uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
        stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");

        size_t s;
        num_adsh     = (const int32_t*)mmap_ro(gendb_dir + "/num/adsh.bin",    s); num_rows = s/4;
        num_uom      = (const int32_t*)mmap_ro(gendb_dir + "/num/uom.bin",     s);
        num_value    = (const double*) mmap_ro(gendb_dir + "/num/value.bin",    s);
        num_tag      = (const int32_t*)mmap_ro(gendb_dir + "/num/tag.bin",      s);
        num_ver      = (const int32_t*)mmap_ro(gendb_dir + "/num/version.bin",  s);

        sub_sic_raw  = (const int32_t*)mmap_ro(gendb_dir + "/sub/sic.bin",     s); sub_rows = s/4;
        sub_cik_raw  = (const int32_t*)mmap_ro(gendb_dir + "/sub/cik.bin",     s);

        tag_abstract_col = (const int32_t*)mmap_ro(gendb_dir + "/tag/abstract.bin", s);
        tag_tlabel_col   = (const int32_t*)mmap_ro(gendb_dir + "/tag/tlabel.bin",   s);
        tagver_raw       = (const uint8_t*) mmap_ro(gendb_dir + "/tag/indexes/tagver_hash.bin", s);

        pre_adsh_col  = (const int32_t*)mmap_ro(gendb_dir + "/pre/adsh.bin",    s); pre_rows = s/4;
        pre_tag_col   = (const int32_t*)mmap_ro(gendb_dir + "/pre/tag.bin",     s);
        pre_ver_col   = (const int32_t*)mmap_ro(gendb_dir + "/pre/version.bin", s);
        pre_stmt_col  = (const int32_t*)mmap_ro(gendb_dir + "/pre/stmt.bin",    s);
    }

    // Lookup dictionary codes at runtime (C2)
    int32_t usd_code = -1;
    for (int i = 0; i < (int)uom_dict.size(); i++)
        if (uom_dict[i] == "USD") { usd_code = i; break; }
    int32_t eq_code = -1;
    for (int i = 0; i < (int)stmt_dict.size(); i++)
        if (stmt_dict[i] == "EQ") { eq_code = i; break; }
    if (usd_code < 0 || eq_code < 0) {
        std::cerr << "ERROR: USD or EQ not found in dicts\n"; exit(1);
    }

    // Parse tagver index header
    uint64_t tagver_ht_size = *reinterpret_cast<const uint64_t*>(tagver_raw);
    const TagverSlot* tagver_slots = reinterpret_cast<const TagverSlot*>(tagver_raw + sizeof(uint64_t));
    uint64_t tagver_mask = tagver_ht_size - 1;

    // -----------------------------------------------------------------------
    // dim_filter: build sub filter arrays
    // -----------------------------------------------------------------------
    std::vector<bool>    sub_passes(sub_rows, false);
    std::vector<int32_t> sub_sic_arr(sub_rows, 0);
    std::vector<int32_t> sub_cik_arr(sub_rows, 0);

    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < sub_rows; i++) {
            sub_sic_arr[i] = sub_sic_raw[i];
            sub_cik_arr[i] = sub_cik_raw[i];
            if (sub_sic_raw[i] >= 4000 && sub_sic_raw[i] <= 4999)
                sub_passes[i] = true;
        }
    }

    // -----------------------------------------------------------------------
    // build_joins: build pre hash set for (adsh, tag, ver) where stmt=EQ
    // C9: capacity = next_pow2(est_eq_rows * 2); estimate ~1.24M EQ rows → 4194304
    // -----------------------------------------------------------------------
    uint64_t pre_cap  = next_pow2((uint64_t)pre_rows / 4);  // ~2.4M → 4194304
    if (pre_cap < 4194304ULL) pre_cap = 4194304ULL;
    uint64_t pre_mask = pre_cap - 1;

    std::vector<PreSlot> pre_ht(pre_cap);
    // C20: use std::fill, not memset
    std::fill(pre_ht.begin(), pre_ht.end(), PreSlot{0ULL, 0U, 0U}); // count=0 → empty

    {
        GENDB_PHASE("build_joins");
        for (size_t i = 0; i < pre_rows; i++) {
            if (pre_stmt_col[i] != eq_code) continue;

            uint64_t k1 = ((uint64_t)(uint32_t)pre_adsh_col[i] << 32) | (uint32_t)pre_tag_col[i];
            uint32_t k2 = (uint32_t)pre_ver_col[i];
            uint64_t h  = k1 * 6364136223846793005ULL ^ (uint64_t)k2 * 2246822519ULL;

            // C24: bounded probe; increment count if key already present
            for (uint64_t p = 0; p < pre_cap; p++) {
                uint64_t slot = (h + p) & pre_mask;
                if (!pre_ht[slot].count) {
                    pre_ht[slot] = {k1, k2, 1U};
                    break;
                }
                if (pre_ht[slot].k1 == k1 && pre_ht[slot].k2 == k2) {
                    pre_ht[slot].count++;  // duplicate EQ row: num will contribute N times
                    break;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // main_scan: parallel morsel-driven scan of num with thread-local agg
    // Thread-local: custom open-addressing AggSlot map (no heap alloc per row)
    // Distinct CIK: collect (grp_key, cik) pairs per thread → sort+dedup at merge
    // -----------------------------------------------------------------------
    int nthreads = omp_get_max_threads();

    // Thread-local aggregation maps (custom open-addressing, C9: full cardinality)
    std::vector<std::vector<AggSlot>> tl_agg(nthreads, std::vector<AggSlot>(AGG_CAP));
    // Thread-local cik pair lists for distinct counting
    std::vector<std::vector<std::pair<uint64_t,int32_t>>> tl_cik(nthreads);

    for (int t = 0; t < nthreads; t++) {
        // C20: std::fill, not memset — initialize empty sentinel
        std::fill(tl_agg[t].begin(), tl_agg[t].end(), AggSlot{AGG_EMPTY, 0.0, 0});
        tl_cik[t].reserve(16384);
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local_agg = tl_agg[tid];
            auto& local_cik = tl_cik[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < num_rows; i++) {
                // Filter 1: uom == USD
                if (num_uom[i] != usd_code) continue;

                // Filter 2: sub_passes (sic BETWEEN 4000 AND 4999)
                int32_t adsh = num_adsh[i];
                if ((size_t)adsh >= sub_rows || !sub_passes[(size_t)adsh]) continue;

                // Filter 3: value IS NOT NULL
                double val = num_value[i];
                if (std::isnan(val)) continue;

                // Tag lookup via pre-built tagver_hash.bin (P11, C24)
                int32_t tc = num_tag[i];
                int32_t vc = num_ver[i];
                uint64_t th = ((uint64_t)(uint32_t)tc * 2654435761ULL)
                            ^ ((uint64_t)(uint32_t)vc * 2246822519ULL);
                int32_t tag_row = -1;
                for (uint64_t p = 0; p < tagver_ht_size; p++) {
                    uint64_t slot = (th + p) & tagver_mask;
                    const TagverSlot& ts = tagver_slots[slot];
                    if (!ts.valid) break;
                    if (ts.tag_code == tc && ts.ver_code == vc) {
                        tag_row = ts.row_idx;
                        break;
                    }
                }
                if (tag_row < 0) continue;

                // Filter: abstract == 0
                if (tag_abstract_col[tag_row] != 0) continue;

                // Pre hash map probe (C24: bounded) → get match count
                uint64_t pk1 = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tc;
                uint32_t pk2 = (uint32_t)vc;
                uint64_t ph  = pk1 * 6364136223846793005ULL ^ (uint64_t)pk2 * 2246822519ULL;
                uint32_t pre_count = 0;
                for (uint64_t p = 0; p < pre_cap; p++) {
                    uint64_t slot = (ph + p) & pre_mask;
                    const PreSlot& ps = pre_ht[slot];
                    if (!ps.count) break;
                    if (ps.k1 == pk1 && ps.k2 == pk2) { pre_count = ps.count; break; }
                }
                if (!pre_count) continue;

                // Aggregate: group by (sic, tlabel_code); stmt=EQ constant
                // If pre had N matching EQ rows, this num row contributes N times (SQL inner join semantics)
                int32_t sic         = sub_sic_arr[(size_t)adsh];
                int32_t tlabel_code = tag_tlabel_col[tag_row];
                int32_t cik         = sub_cik_arr[(size_t)adsh];

                // C15: pack ALL GROUP BY columns in key; stmt is constant EQ
                uint64_t grp_key = ((uint64_t)(uint32_t)sic << 32) | (uint32_t)tlabel_code;

                // Custom open-addressing insert/update (P1: no unordered_map)
                AggSlot& s = agg_get(local_agg, grp_key);
                s.sum_val += val * (double)pre_count;
                s.cnt_val += (int64_t)pre_count;

                // Distinct CIK: collect pairs for sort+dedup merge (no unordered_set alloc)
                local_cik.emplace_back(grp_key, cik);
            }
        }
    }

    // -----------------------------------------------------------------------
    // aggregation_merge: linear merge of open-addressing slots + sort-dedup CIK
    // -----------------------------------------------------------------------

    // Global aggregation map (same open-addressing design)
    std::vector<AggSlot> global_agg(AGG_CAP);
    std::fill(global_agg.begin(), global_agg.end(), AggSlot{AGG_EMPTY, 0.0, 0});

    // Collect all cik pairs for sort-based distinct counting
    size_t total_pairs = 0;
    for (int t = 0; t < nthreads; t++) total_pairs += tl_cik[t].size();
    std::vector<std::pair<uint64_t,int32_t>> all_cik_pairs;
    all_cik_pairs.reserve(total_pairs);

    {
        GENDB_PHASE("aggregation_merge");

        // Merge agg slots: linear scan, skip empty (O(AGG_CAP * nthreads) = O(4M))
        for (int t = 0; t < nthreads; t++) {
            for (const auto& s : tl_agg[t]) {
                if (s.key == AGG_EMPTY) continue;
                AggSlot& g = agg_get(global_agg, s.key);
                g.sum_val += s.sum_val;
                g.cnt_val += s.cnt_val;
            }
            // Collect cik pairs
            for (const auto& p : tl_cik[t]) all_cik_pairs.push_back(p);
        }

        // Sort pairs by (grp_key, cik) for distinct counting
        std::sort(all_cik_pairs.begin(), all_cik_pairs.end());
    }

    // Compute COUNT(DISTINCT cik) per group from sorted pairs
    // P1: flat parallel array indexed by same open-addressing scheme as global_agg
    std::vector<int64_t> distinct_cik_count(AGG_CAP, -1LL);  // -1 = unused slot
    {
        size_t i = 0;
        while (i < all_cik_pairs.size()) {
            uint64_t cur_key = all_cik_pairs[i].first;
            int32_t  prev_cik = all_cik_pairs[i].second;
            int64_t  cnt = 1;
            i++;
            while (i < all_cik_pairs.size() && all_cik_pairs[i].first == cur_key) {
                if (all_cik_pairs[i].second != prev_cik) {
                    cnt++;
                    prev_cik = all_cik_pairs[i].second;
                }
                i++;
            }
            // Find this key's slot in global_agg using same probe sequence, store count there
            uint64_t h = cur_key * 11400714819323198485ULL;
            for (uint64_t p = 0; p < AGG_CAP; p++) {
                uint64_t idx = (h + p) & AGG_MASK;
                if (global_agg[idx].key == cur_key) { distinct_cik_count[idx] = cnt; break; }
                if (global_agg[idx].key == AGG_EMPTY) break;  // key absent (shouldn't happen)
            }
        }
    }

    // -----------------------------------------------------------------------
    // output: HAVING, sort, write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Row {
            int32_t sic;
            int32_t tlabel_code;
            int64_t num_companies;
            double  total_value;
            double  avg_value;
        };

        std::vector<Row> rows;
        rows.reserve(1024);

        for (uint64_t idx = 0; idx < AGG_CAP; idx++) {
            const AggSlot& s = global_agg[idx];
            if (s.key == AGG_EMPTY) continue;
            int64_t dc = distinct_cik_count[idx];
            if (dc < 2) continue;  // HAVING COUNT(DISTINCT cik) >= 2
            int32_t sic         = (int32_t)(s.key >> 32);
            int32_t tlabel_code = (int32_t)(s.key & 0xFFFFFFFFULL);
            double avg = (s.cnt_val > 0) ? s.sum_val / (double)s.cnt_val : 0.0;
            rows.push_back({sic, tlabel_code, dc, s.sum_val, avg});
        }

        // P6: partial_sort for LIMIT 500
        size_t lim = std::min((size_t)500, rows.size());
        std::partial_sort(rows.begin(), rows.begin() + lim, rows.end(),
            [](const Row& a, const Row& b) { return a.total_value > b.total_value; });
        rows.resize(lim);

        // Write CSV (C18: decode codes via dicts)
        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q4.csv");
        out << "sic,tlabel,stmt,num_companies,total_value,avg_value\n";
        out << std::fixed << std::setprecision(2);

        // CSV quoting helper: wrap field in quotes if it contains a comma
        auto csv_field = [](const std::string& s) -> std::string {
            if (s.find(',') != std::string::npos)
                return "\"" + s + "\"";
            return s;
        };

        for (auto& r : rows) {
            std::string tlabel_str;
            if (r.tlabel_code >= 0 && r.tlabel_code < (int32_t)tlabel_dict.size())
                tlabel_str = tlabel_dict[r.tlabel_code];

            // stmt is constant EQ for all surviving rows (filtered by eq_code in pre hash)
            const std::string& stmt_str = stmt_dict[(size_t)eq_code];

            out << r.sic << ","
                << csv_field(tlabel_str) << ","
                << csv_field(stmt_str) << ","
                << r.num_companies << ","
                << r.total_value << ","
                << r.avg_value << "\n";
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
