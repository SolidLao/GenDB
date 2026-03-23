// Q3: SEC-EDGAR — HAVING subquery
//
// Correct GROUP BY: outer query groups by (s.name, s.cik), NOT just s.cik.
// 259 CIKs have multiple distinct names in fy=2022 (company renames/splits).
// Grouping by cik alone merges them, inflating their sum and raising avg_threshold.
//
// Algorithm:
//   data_loading: build adsh_group_id[] array: adsh_code → compact group int
//                 (two adsh values share a group iff they have identical (name,cik))
//   pass1 (sequential): cik → SUM(value) → avg_threshold  [per SQL HAVING subquery]
//   pass2 (parallel):   group_id → SUM(value)             [per SQL outer query]
//   having_filter: group_sum[g] > avg_threshold
//   topk_sort + output

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_map>
#include <cassert>
#include <sys/stat.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"
#include "hash_utils.h"

// -------------------------------------------------------------------
// Constants
// -------------------------------------------------------------------
static constexpr size_t  BLOCK_SIZE = 100000;
static constexpr size_t  NUM_ROWS   = 39401761;
static constexpr size_t  SUB_ROWS   = 86135;
static constexpr int16_t FY_FILTER  = 2022;
static constexpr int     TOP_K      = 100;

// Zone map layout: { int8_t min_uom, max_uom; 2-byte pad; int32_t min_ddate, max_ddate }
struct NumZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int8_t  _pad[2];
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(NumZoneMap) == 12, "ZoneMap size mismatch");

// -------------------------------------------------------------------
// Accumulate — find+increment or insert (avoids Robin Hood re-entry)
// -------------------------------------------------------------------
// For cik-sum (pass1 threshold): double accumulation
static inline void map_add(gendb::CompactHashMap<int32_t, double>& m,
                           int32_t key, double val) {
    double* v = m.find(key);
    if (__builtin_expect(v != nullptr, 1)) *v += val;
    else m.insert(key, val);
}

// For group-sum (pass2 output): long double accumulation avoids ULP-level
// rounding differences at magnitude ~10^13-10^14 (ULP = 1/32 to 1/256).
static inline void map_add_ld(gendb::CompactHashMap<int32_t, long double>& m,
                              int32_t key, long double val) {
    long double* v = m.find(key);
    if (__builtin_expect(v != nullptr, 1)) *v += val;
    else m.insert(key, val);
}

// -------------------------------------------------------------------
// CSV field writer — quotes if field contains comma / quote / newline
// -------------------------------------------------------------------
static void write_csv_field(FILE* f, const char* s, size_t len) {
    bool q = false;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { q = true; break; }
    }
    if (!q) { fwrite(s, 1, len, f); return; }
    fputc('"', f);
    for (size_t i = 0; i < len; i++) {
        if (s[i] == '"') fputc('"', f);
        fputc(s[i], f);
    }
    fputc('"', f);
}

// -------------------------------------------------------------------
// Group descriptor: representative adsh + cik for output decoding
// -------------------------------------------------------------------
struct Group {
    int32_t rep_adsh;  // any adsh_code belonging to this group
    int32_t cik;
};

// -------------------------------------------------------------------
// Main
// -------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    mkdir(results_dir.c_str(), 0755);

    GENDB_PHASE_MS("total", total_ms);

    // ----------------------------------------------------------------
    // Phase: data_loading
    // ----------------------------------------------------------------
    int8_t usd_code = -1;
    std::vector<int16_t>  sub_fy(SUB_ROWS);
    std::vector<int32_t>  sub_cik(SUB_ROWS);
    std::vector<uint32_t> name_offsets(SUB_ROWS + 1);
    std::vector<char>     name_data;

    // adsh_group_id[adsh_code] = compact group integer (same (name,cik) → same group)
    // -1 for adsh values not in fy=2022
    std::vector<int32_t>  adsh_group_id(SUB_ROWS, -1);
    std::vector<Group>    groups;       // groups[group_id] = {rep_adsh, cik}

    std::vector<bool> skip_block;

    {
        GENDB_PHASE("data_loading");

        // --- uom_codes dict ---
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

        // --- sub/fy.bin ---
        {
            FILE* f = fopen((gendb_dir + "/sub/fy.bin").c_str(), "rb");
            if (!f) { perror("sub/fy.bin"); return 1; }
            (void)fread(sub_fy.data(), sizeof(int16_t), SUB_ROWS, f);
            fclose(f);
        }

        // --- sub/cik.bin ---
        {
            FILE* f = fopen((gendb_dir + "/sub/cik.bin").c_str(), "rb");
            if (!f) { perror("sub/cik.bin"); return 1; }
            (void)fread(sub_cik.data(), sizeof(int32_t), SUB_ROWS, f);
            fclose(f);
        }

        // --- sub/name_offsets.bin ---
        {
            FILE* f = fopen((gendb_dir + "/sub/name_offsets.bin").c_str(), "rb");
            if (!f) { perror("sub/name_offsets.bin"); return 1; }
            (void)fread(name_offsets.data(), sizeof(uint32_t), SUB_ROWS + 1, f);
            fclose(f);
        }

        // --- sub/name_data.bin ---
        {
            FILE* f = fopen((gendb_dir + "/sub/name_data.bin").c_str(), "rb");
            if (!f) { perror("sub/name_data.bin"); return 1; }
            size_t sz = name_offsets[SUB_ROWS];
            name_data.resize(sz);
            if (sz > 0) (void)fread(name_data.data(), 1, sz, f);
            fclose(f);
        }

        // --- Build adsh_group_id: group by (name_string, cik) ---
        // Use a string hash map. Key = name+NUL+cik_decimal to avoid collision.
        // Only ~17K rows have fy==2022, so this is fast.
        {
            std::unordered_map<std::string, int32_t> key_to_gid;
            key_to_gid.reserve(25000);
            groups.reserve(25000);

            const char*     nd = name_data.data();
            const uint32_t* no = name_offsets.data();

            for (int i = 0; i < (int)SUB_ROWS; i++) {
                if (sub_fy[i] != FY_FILTER) continue;
                uint32_t noff = no[i];
                uint32_t nlen = no[i + 1] - noff;
                int32_t  cik  = sub_cik[i];

                // Build key: (cik_bytes | name_bytes)
                std::string key;
                key.reserve(4 + nlen);
                key.append((const char*)&cik, 4);
                key.append(nd + noff, nlen);

                auto [it, inserted] = key_to_gid.emplace(std::move(key), (int32_t)groups.size());
                if (inserted) {
                    groups.push_back({i, cik});
                }
                adsh_group_id[i] = it->second;
            }
        }

        // --- num zone maps → per-block skip flags ---
        {
            FILE* f = fopen((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), "rb");
            if (!f) { perror("num_zone_maps.bin"); return 1; }
            uint32_t n_blocks = 0;
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

    // Pre-build active block list
    std::vector<int> active_blocks;
    active_blocks.reserve(skip_block.size());
    for (int b = 0; b < (int)skip_block.size(); b++)
        if (!skip_block[b]) active_blocks.push_back(b);

    // ----------------------------------------------------------------
    // mmap num columns
    // ----------------------------------------------------------------
    gendb::MmapColumn<int8_t>  uom_col(gendb_dir + "/num/uom_code.bin");
    gendb::MmapColumn<int32_t> adsh_col(gendb_dir + "/num/adsh_code.bin");
    gendb::MmapColumn<double>  val_col(gendb_dir + "/num/value.bin");
    mmap_prefetch_all(uom_col, adsh_col, val_col);

    const int16_t* __restrict__ fy_arr        = sub_fy.data();
    const int32_t* __restrict__ cik_arr       = sub_cik.data();
    const int32_t* __restrict__ grp_arr       = adsh_group_id.data();
    const int n_active  = (int)active_blocks.size();
    const int nthreads  = omp_get_max_threads();

    // ----------------------------------------------------------------
    // Pass 1 (sequential): GROUP BY cik → SUM(value) → avg_threshold
    // Sequential scan ensures stable accumulation order for the threshold.
    // ----------------------------------------------------------------
    double avg_threshold = 0.0;
    {
        GENDB_PHASE("pass1_scan_aggregate_cik");

        gendb::CompactHashMap<int32_t, double> cik_sum(25000);
        cik_sum.reserve(25000);

        for (int bi = 0; bi < n_active; bi++) {
            int    b         = active_blocks[bi];
            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = std::min(row_start + BLOCK_SIZE, NUM_ROWS);
            size_t n         = row_end - row_start;

            const int8_t*  __restrict__ uom  = uom_col.data  + row_start;
            const int32_t* __restrict__ adsh = adsh_col.data + row_start;
            const double*  __restrict__ val  = val_col.data  + row_start;

            for (size_t r = 0; r < n; r++) {
                if (uom[r] != usd_code) continue;
                int32_t ac = adsh[r];
                if (fy_arr[ac] != FY_FILTER) continue;
                map_add(cik_sum, cik_arr[ac], val[r]);
            }
        }

        double total_sum   = 0.0;
        size_t group_count = cik_sum.size();
        for (const auto& [k, v] : cik_sum) total_sum += v;
        avg_threshold = (group_count > 0) ? (total_sum / (double)group_count) : 0.0;
    }

    // ----------------------------------------------------------------
    // Pass 2 (parallel): GROUP BY group_id → SUM(value) using long double
    // Long double (80-bit x86) gives ~19 decimal digits precision, ensuring
    // that rounding to 2 decimal places at magnitude 10^14 (ULP ~0.03) is correct.
    // ----------------------------------------------------------------
    int n_groups = (int)groups.size();
    gendb::CompactHashMap<int32_t, long double> group_sum(n_groups * 2);
    {
        GENDB_PHASE("pass2_scan_aggregate_adsh");

        std::vector<gendb::CompactHashMap<int32_t, long double>> local_maps(nthreads);
        for (auto& m : local_maps) m.reserve(n_groups + 4096);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& lmap = local_maps[tid];

            #pragma omp for schedule(dynamic, 8)
            for (int bi = 0; bi < n_active; bi++) {
                int    b         = active_blocks[bi];
                size_t row_start = (size_t)b * BLOCK_SIZE;
                size_t row_end   = std::min(row_start + BLOCK_SIZE, NUM_ROWS);
                size_t n         = row_end - row_start;

                const int8_t*  __restrict__ uom  = uom_col.data  + row_start;
                const int32_t* __restrict__ adsh = adsh_col.data + row_start;
                const double*  __restrict__ val  = val_col.data  + row_start;

                for (size_t r = 0; r < n; r++) {
                    if (uom[r] != usd_code) continue;
                    int32_t ac = adsh[r];
                    if (fy_arr[ac] != FY_FILTER) continue;
                    int32_t gid = grp_arr[ac];  // O(1) group lookup
                    map_add_ld(lmap, gid, (long double)val[r]);
                }
            }
        }

        // Merge thread-local maps (long double → long double)
        group_sum.reserve(n_groups * 2);
        for (auto& lm : local_maps) {
            for (const auto& [k, v] : lm) {
                map_add_ld(group_sum, k, v);
            }
        }
    }

    // ----------------------------------------------------------------
    // HAVING filter + decode names
    // ----------------------------------------------------------------
    struct ResultRow {
        const char* name_ptr;
        uint32_t    name_len;
        int32_t     cik;
        long double total_value;
    };
    std::vector<ResultRow> results;
    results.reserve(1024);

    {
        GENDB_PHASE("having_filter");

        const char*     nd = name_data.data();
        const uint32_t* no = name_offsets.data();

        for (const auto& [gid, total] : group_sum) {
            if ((double)total > avg_threshold) {
                const Group& g = groups[gid];
                int32_t radsh = g.rep_adsh;
                results.push_back({nd + no[radsh], no[radsh + 1] - no[radsh],
                                   g.cik, total});
            }
        }
    }

    // ----------------------------------------------------------------
    // Top-K sort by total_value DESC, limit 100
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("topk_sort");

        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            return a.total_value > b.total_value;
        };
        if ((int)results.size() > TOP_K) {
            std::partial_sort(results.begin(), results.begin() + TOP_K,
                              results.end(), cmp);
            results.resize(TOP_K);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }
    }

    // ----------------------------------------------------------------
    // Output CSV:  name,cik,total_value
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror("Q3.csv"); return 1; }

        fprintf(out, "name,cik,total_value\n");
        for (const auto& row : results) {
            write_csv_field(out, row.name_ptr, row.name_len);
            fprintf(out, ",%d,%.2Lf\n", row.cik, row.total_value);
        }
        fclose(out);
    }

    return 0;
}
