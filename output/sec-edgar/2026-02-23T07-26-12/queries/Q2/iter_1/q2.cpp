#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cfloat>
#include <climits>
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"

// ─── Dict loading ─────────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> v;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) v.push_back(line);
    return v;
}

static int16_t find_code16(const std::vector<std::string>& dict, const std::string& val) {
    for (int i = 0; i < (int)dict.size(); ++i)
        if (dict[i] == val) return (int16_t)i;
    return -1;
}

// ─── mmap helpers ─────────────────────────────────────────────────────────────
// Large files: no MAP_POPULATE (avoids eager page-table walk for 700MB+ of num cols).
// Use MADV_RANDOM to suppress read-ahead on sparse zone-map-guided scans.
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, st.st_size, MADV_RANDOM);
    close(fd);
    n_rows = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

// Small files (sub cols, zone maps): MAP_POPULATE OK — tiny footprint.
template<typename T>
static const T* mmap_col_small(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    n_rows = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

static const void* mmap_raw(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st); sz = st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;  // zone maps and sub files are small — MAP_POPULATE is fine
}

// ─── Zone map structs ─────────────────────────────────────────────────────────
struct ZoneBlockI16 { int16_t min_val; int16_t max_val; uint32_t row_count; };
struct ZoneBlockI32 { int32_t min_val; int32_t max_val; uint32_t row_count; };

// ─── Hash functions (multiply-shift) ─────────────────────────────────────────
static inline uint64_t hash_i32(int32_t k) {
    return (uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL;
}
static inline uint64_t hash_u64(uint64_t k) {
    return k * 0x9E3779B97F4A7C15ULL;
}

// ─── Sub FY hash table: adsh_code → name_code ─────────────────────────────────
// cap = next_power_of_2(27000*2) = 65536
static const uint32_t SUB_CAP  = 65536;
static const uint32_t SUB_MASK = SUB_CAP - 1;
static const int32_t  SUB_EMPTY = INT32_MIN;

struct SubFySlot { int32_t adsh_code; int32_t name_code; };

// ─── Max map: packed(adsh_code,tag_code) → max double ─────────────────────────
// cap = next_power_of_2(94000*2) = 262144
static const uint32_t MAX_CAP  = 262144;
static const uint32_t MAX_MASK = MAX_CAP - 1;
static const uint64_t MAX_EMPTY_KEY = UINT64_MAX;

// ─── Qualifying row buffer struct ─────────────────────────────────────────────
struct NumQRow {
    int32_t adsh_code;
    int32_t tag_code;
    double  value;
    int32_t name_code;
};

// ─── Result struct ────────────────────────────────────────────────────────────
struct Result {
    double  value;
    int32_t name_code;
    int32_t tag_code;
};

// ─── Query runner ─────────────────────────────────────────────────────────────
void run_q2(const std::string& db, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Data loading ──────────────────────────────────────────────────────────
    // tag_dict and name_dict are deferred to output phase (only needed for 100 results).
    std::vector<std::string> uom_dict;
    std::vector<std::string> tag_dict;   // loaded lazily in output
    std::vector<std::string> name_dict;  // loaded lazily in output

    const int32_t* sub_adsh_col = nullptr;
    const int32_t* sub_fy_col   = nullptr;
    const int32_t* sub_name_col = nullptr;
    size_t sub_rows = 0;

    const int16_t* num_uom_col   = nullptr;
    const double*  num_value_col = nullptr;
    const int32_t* num_adsh_col  = nullptr;
    const int32_t* num_tag_col   = nullptr;
    size_t num_rows = 0;

    const void* num_uom_zm_raw = nullptr;
    const void* sub_fy_zm_raw  = nullptr;
    size_t num_uom_zm_sz = 0, sub_fy_zm_sz = 0;

    {
        GENDB_PHASE("data_loading");
        // Load only uom_dict now — tag_dict (198K entries) and name_dict (86K entries)
        // are deferred to output phase where they are actually needed.
        uom_dict = load_dict(db + "/num/uom_dict.txt");

        size_t tmp;
        // Sub files are tiny (~344KB each) — use MAP_POPULATE for instant access
        sub_adsh_col = mmap_col_small<int32_t>(db + "/sub/adsh.bin", tmp);
        sub_fy_col   = mmap_col_small<int32_t>(db + "/sub/fy.bin",   sub_rows);
        sub_name_col = mmap_col_small<int32_t>(db + "/sub/name.bin", tmp);

        // Num files are large (700MB+ total) — no MAP_POPULATE, MADV_RANDOM applied.
        // Zone-map-guided madvise(MADV_WILLNEED) on qualifying blocks happens in main_scan.
        num_uom_col   = mmap_col<int16_t>(db + "/num/uom.bin",   num_rows);
        num_value_col = mmap_col<double> (db + "/num/value.bin", tmp);
        num_adsh_col  = mmap_col<int32_t>(db + "/num/adsh.bin",  tmp);
        num_tag_col   = mmap_col<int32_t>(db + "/num/tag.bin",   tmp);

        num_uom_zm_raw = mmap_raw(db + "/indexes/num_uom_zone_map.bin", num_uom_zm_sz);
        sub_fy_zm_raw  = mmap_raw(db + "/indexes/sub_fy_zone_map.bin",  sub_fy_zm_sz);
    }

    // ── Dim filter: find pure_code ────────────────────────────────────────────
    int16_t pure_code = -1;
    {
        GENDB_PHASE("dim_filter");
        pure_code = find_code16(uom_dict, "pure");
        if (pure_code < 0) { fprintf(stderr, "ERROR: 'pure' not found in uom_dict\n"); exit(1); }
    }

    // ── Build sub_fy_map: adsh_code → name_code for fy=2022 ──────────────────
    std::vector<SubFySlot> sub_fy_ht(SUB_CAP);

    {
        GENDB_PHASE("build_joins");
        // C20: use std::fill, NOT memset, for multi-byte sentinel
        for (auto& s : sub_fy_ht) s.adsh_code = SUB_EMPTY;

        // Parse sub fy zone map (int32_t blocks, 10K rows/block)
        const uint8_t* zm_ptr = reinterpret_cast<const uint8_t*>(sub_fy_zm_raw);
        uint32_t sub_nblocks = *reinterpret_cast<const uint32_t*>(zm_ptr);
        const ZoneBlockI32* sub_zm = reinterpret_cast<const ZoneBlockI32*>(zm_ptr + sizeof(uint32_t));
        const uint32_t SUB_BLOCK_SIZE = 10000;

        for (uint32_t b = 0; b < sub_nblocks; ++b) {
            // C19: zone-map skip only on zone-map-indexed columns
            if (sub_zm[b].min_val > 2022 || sub_zm[b].max_val < 2022) continue;

            uint64_t row_start = (uint64_t)b * SUB_BLOCK_SIZE;
            uint64_t row_end   = row_start + sub_zm[b].row_count;
            if (row_end > sub_rows) row_end = sub_rows;

            for (uint64_t i = row_start; i < row_end; ++i) {
                if (sub_fy_col[i] != 2022) continue;
                int32_t acode = sub_adsh_col[i];
                int32_t ncode = sub_name_col[i];

                // C24: bounded probing (for-loop)
                uint32_t h = (uint32_t)(hash_i32(acode) & SUB_MASK);
                for (uint32_t p = 0; p < SUB_CAP; ++p) {
                    uint32_t idx = (h + p) & SUB_MASK;
                    if (sub_fy_ht[idx].adsh_code == SUB_EMPTY) {
                        sub_fy_ht[idx].adsh_code = acode;
                        sub_fy_ht[idx].name_code = ncode;
                        break;
                    }
                    if (sub_fy_ht[idx].adsh_code == acode) break; // already inserted
                }
            }
        }
    }

    // ── Main scan: zone-map-guided num scan + collect + max + filter ──────────
    std::vector<Result> results;
    std::vector<Result> candidates;  // populated in main_scan, sorted in output

    {
        GENDB_PHASE("main_scan");

        // Parse num uom zone map (int16_t blocks, 100K rows/block)
        const uint8_t* zm_ptr = reinterpret_cast<const uint8_t*>(num_uom_zm_raw);
        uint32_t num_nblocks = *reinterpret_cast<const uint32_t*>(zm_ptr);
        const ZoneBlockI16* num_zm = reinterpret_cast<const ZoneBlockI16*>(zm_ptr + sizeof(uint32_t));
        const uint64_t NUM_BLOCK_SIZE = 100000;

        // ── P13/P16: Zone-map-guided madvise — prefetch only qualifying blocks ─
        // Since num is sorted by uom, the qualifying blocks are contiguous.
        // Prefetch just those block ranges for each num column before scanning.
        {
            uint64_t first_row = UINT64_MAX, last_row = 0;
            for (uint32_t b = 0; b < num_nblocks; ++b) {
                if (num_zm[b].min_val > pure_code || num_zm[b].max_val < pure_code) continue;
                uint64_t rs = (uint64_t)b * NUM_BLOCK_SIZE;
                uint64_t re = rs + num_zm[b].row_count;
                if (rs < first_row) first_row = rs;
                if (re > last_row)  last_row  = re;
            }
            if (first_row < last_row) {
                // uom: int16_t
                size_t uom_off = first_row * sizeof(int16_t);
                size_t uom_len = (last_row - first_row) * sizeof(int16_t);
                madvise((void*)(num_uom_col + first_row), uom_len, MADV_WILLNEED);
                madvise((void*)(num_uom_col + first_row), uom_len, MADV_SEQUENTIAL);
                // value: double
                size_t val_off = first_row * sizeof(double);
                size_t val_len = (last_row - first_row) * sizeof(double);
                madvise((void*)(num_value_col + first_row), val_len, MADV_WILLNEED);
                madvise((void*)(num_value_col + first_row), val_len, MADV_SEQUENTIAL);
                // adsh: int32_t
                size_t adsh_len = (last_row - first_row) * sizeof(int32_t);
                madvise((void*)(num_adsh_col + first_row), adsh_len, MADV_WILLNEED);
                madvise((void*)(num_adsh_col + first_row), adsh_len, MADV_SEQUENTIAL);
                // tag: int32_t
                madvise((void*)(num_tag_col + first_row), adsh_len, MADV_WILLNEED);
                madvise((void*)(num_tag_col + first_row), adsh_len, MADV_SEQUENTIAL);
                (void)uom_off; (void)val_off;
            }
        }

        // ── Step 1: Collect qualifying rows ──────────────────────────────────
        std::vector<NumQRow> qbuf;
        qbuf.reserve(150000);

        for (uint32_t b = 0; b < num_nblocks; ++b) {
            // C19: zone-map skip — num is sorted by uom
            if (num_zm[b].min_val > pure_code || num_zm[b].max_val < pure_code) continue;

            uint64_t row_start = (uint64_t)b * NUM_BLOCK_SIZE;
            uint64_t row_end   = row_start + num_zm[b].row_count;
            if (row_end > num_rows) row_end = num_rows;

            for (uint64_t i = row_start; i < row_end; ++i) {
                // Late materialization: filter uom + value first
                if (num_uom_col[i] != pure_code) continue;
                double v = num_value_col[i];
                if (std::isnan(v)) continue;

                int32_t acode = num_adsh_col[i];

                // Probe sub_fy_map for fy=2022
                bool found = false;
                int32_t ncode = 0;
                uint32_t h = (uint32_t)(hash_i32(acode) & SUB_MASK);
                for (uint32_t p = 0; p < SUB_CAP; ++p) {
                    uint32_t idx = (h + p) & SUB_MASK;
                    if (sub_fy_ht[idx].adsh_code == SUB_EMPTY) break;
                    if (sub_fy_ht[idx].adsh_code == acode) {
                        ncode = sub_fy_ht[idx].name_code;
                        found = true;
                        break;
                    }
                }
                if (!found) continue;

                int32_t tcode = num_tag_col[i];
                qbuf.push_back({acode, tcode, v, ncode});
            }
        }

        // ── Step 2: Build max_map: packed(adsh,tag) → max double ─────────────
        // C9: cap = next_power_of_2(94K*2) = 262144
        // C20: std::fill for sentinel initialization
        std::vector<uint64_t> max_keys(MAX_CAP);
        std::vector<double>   max_vals(MAX_CAP);
        std::fill(max_keys.begin(), max_keys.end(), MAX_EMPTY_KEY);
        std::fill(max_vals.begin(), max_vals.end(), -DBL_MAX);

        for (const auto& row : qbuf) {
            uint64_t key = ((uint64_t)(uint32_t)row.adsh_code << 32) | (uint32_t)row.tag_code;
            uint32_t h = (uint32_t)(hash_u64(key) & MAX_MASK);
            for (uint32_t p = 0; p < MAX_CAP; ++p) {
                uint32_t idx = (h + p) & MAX_MASK;
                if (max_keys[idx] == MAX_EMPTY_KEY) {
                    max_keys[idx] = key;
                    max_vals[idx] = row.value;
                    break;
                }
                if (max_keys[idx] == key) {
                    if (row.value > max_vals[idx]) max_vals[idx] = row.value;
                    break;
                }
            }
        }

        // ── Step 3: Filter rows where value == max for their (adsh,tag) ──────
        candidates.reserve(qbuf.size());

        for (const auto& row : qbuf) {
            uint64_t key = ((uint64_t)(uint32_t)row.adsh_code << 32) | (uint32_t)row.tag_code;
            uint32_t h = (uint32_t)(hash_u64(key) & MAX_MASK);
            for (uint32_t p = 0; p < MAX_CAP; ++p) {
                uint32_t idx = (h + p) & MAX_MASK;
                if (max_keys[idx] == MAX_EMPTY_KEY) break;
                if (max_keys[idx] == key) {
                    if (row.value == max_vals[idx]) {
                        candidates.push_back({row.value, row.name_code, row.tag_code});
                    }
                    break;
                }
            }
        }
        // Sort and decode happen in output phase (after dicts are loaded).
    }

    // ── Output ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // Load large dicts here — only needed for the final 100 rows.
        // Deferring saves ~15-20ms from data_loading (198K + 86K string allocs).
        tag_dict  = load_dict(db + "/num/tag_dict.txt");
        name_dict = load_dict(db + "/sub/name_dict.txt");

        // ── Top-100 partial_sort (P6) ─────────────────────────────────────────
        size_t take = std::min((size_t)100, candidates.size());
        if (take > 0) {
            std::partial_sort(
                candidates.begin(),
                candidates.begin() + (ptrdiff_t)take,
                candidates.end(),
                [&](const Result& a, const Result& b) {
                    if (a.value != b.value) return a.value > b.value;
                    const std::string& na = name_dict[a.name_code];
                    const std::string& nb = name_dict[b.name_code];
                    if (na != nb) return na < nb;
                    return tag_dict[a.tag_code] < tag_dict[b.tag_code];
                }
            );
        }
        results.assign(candidates.begin(), candidates.begin() + (ptrdiff_t)take);

        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        // CSV quoting helper: quote field if it contains comma, quote, or newline
        auto csv_field = [&](FILE* fp, const std::string& s) {
            bool needs_quote = (s.find(',') != std::string::npos ||
                                s.find('"') != std::string::npos ||
                                s.find('\n') != std::string::npos);
            if (!needs_quote) {
                fputs(s.c_str(), fp);
            } else {
                fputc('"', fp);
                for (char c : s) {
                    if (c == '"') fputc('"', fp); // escape double-quote
                    fputc(c, fp);
                }
                fputc('"', fp);
            }
        };

        fprintf(f, "name,tag,value\n");
        for (const auto& r : results) {
            const std::string& name = name_dict[r.name_code];
            const std::string& tag  = tag_dict[r.tag_code];
            csv_field(f, name);
            fputc(',', f);
            csv_field(f, tag);
            fprintf(f, ",%.2f\n", r.value);
        }
        fclose(f);
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
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
