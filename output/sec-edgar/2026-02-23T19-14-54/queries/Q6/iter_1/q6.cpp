// Q6: JOIN num→sub (adsh, fy=2023) + JOIN num→pre (adsh+tag+version, stmt='IS')
// GROUP BY name, stmt, tag, plabel | ORDER BY SUM(value) DESC LIMIT 200
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <climits>
#include <omp.h>
#include "timing_utils.h"

// ---- Structs ----------------------------------------------------------------

struct Fy23Slot {
    int32_t adsh_code;
    int32_t name_code;
};

struct IsSlot {
    int32_t adsh_code;
    int32_t tag_code;
    int32_t ver_code;
    int32_t plabel_code;
};

struct AggSlot {
    int32_t name_code;   // INT32_MIN = empty sentinel
    int32_t is_code;     // always 'IS' code, kept for C30
    int32_t tag_code;
    int32_t plabel_code;
    int64_t sum_cents;   // C29: accumulate as int64_t cents
    int64_t count;
};

// ---- Hash functions ---------------------------------------------------------

// Single-value hash
inline uint32_t hash1u(uint32_t a) {
    return a * 2654435761u;
}

// Triple hash for pre_is_hash probing (must EXACTLY match index builder in build_indexes.cpp)
// Builder uses 64-bit intermediates then folds: (h ^ (h >> 32)) & mask
inline uint32_t hash3(int32_t a, int32_t b, int32_t c, uint32_t mask) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761u;
    h ^= (uint64_t)(uint32_t)b * 1234567891u;
    h ^= (uint64_t)(uint32_t)c * 2246822519u;
    return (uint32_t)(h ^ (h >> 32)) & mask;
}

// Quad hash for aggregation key (name, is, tag, plabel) — 64-bit intermediates for quality
inline uint32_t hash4(int32_t a, int32_t b, int32_t c, int32_t d) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761u;
    h ^= (uint64_t)(uint32_t)b * 1234567891u;
    h ^= (uint64_t)(uint32_t)c * 2246822519u;
    h ^= (uint64_t)(uint32_t)d * 374761393u;
    return (uint32_t)(h ^ (h >> 32));
}

// ---- Utilities --------------------------------------------------------------

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream ifs(path);
    if (!ifs) { std::cerr << "Cannot open dict: " << path << "\n"; exit(1); }
    std::string line;
    while (std::getline(ifs, line)) dict.push_back(std::move(line));
    return dict;
}

static const void* do_mmap(const std::string& path, size_t& nbytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    nbytes = (size_t)st.st_size;
    void* p = mmap(nullptr, nbytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, (off_t)nbytes, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ---- Main query -------------------------------------------------------------

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Load dictionaries (C2: at runtime, never hardcode codes) ----
    auto uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
    auto stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
    auto name_dict   = load_dict(gendb_dir + "/sub/name_dict.txt");
    auto tag_dict    = load_dict(gendb_dir + "/tag_global_dict.txt");
    auto plabel_dict = load_dict(gendb_dir + "/pre/plabel_dict.txt");

    // Find dictionary codes at runtime
    int16_t usd_code = -1;
    for (size_t i = 0; i < uom_dict.size(); i++)
        if (uom_dict[i] == "USD") { usd_code = (int16_t)i; break; }
    if (usd_code < 0) { std::cerr << "USD not found in uom_dict\n"; exit(1); }

    int32_t is_code_val = -1;
    for (size_t i = 0; i < stmt_dict.size(); i++)
        if (stmt_dict[i] == "IS") { is_code_val = (int32_t)i; break; }
    if (is_code_val < 0) { std::cerr << "IS not found in stmt_dict\n"; exit(1); }

    // ---- Data loading ----
    int64_t  num_rows = 0, sub_rows = 0;
    const int32_t* num_adsh = nullptr;
    const int32_t* num_tag  = nullptr;
    const int32_t* num_ver  = nullptr;
    const int16_t* num_uom  = nullptr;
    const double*  num_val  = nullptr;
    const int32_t* sub_adsh    = nullptr;
    const int32_t* sub_fy_col  = nullptr;
    const int32_t* sub_name    = nullptr;
    const char*    is_hash_raw = nullptr;

    {
        GENDB_PHASE("data_loading");

        size_t nb = 0;
        num_adsh   = (const int32_t*)do_mmap(gendb_dir + "/num/adsh.bin", nb);   num_rows = (int64_t)(nb / 4);
        num_tag    = (const int32_t*)do_mmap(gendb_dir + "/num/tag.bin", nb);
        num_ver    = (const int32_t*)do_mmap(gendb_dir + "/num/version.bin", nb);
        num_uom    = (const int16_t*)do_mmap(gendb_dir + "/num/uom.bin", nb);
        num_val    = (const double*)  do_mmap(gendb_dir + "/num/value.bin", nb);
        sub_adsh   = (const int32_t*)do_mmap(gendb_dir + "/sub/adsh.bin", nb);   sub_rows = (int64_t)(nb / 4);
        sub_fy_col = (const int32_t*)do_mmap(gendb_dir + "/sub/fy.bin", nb);
        sub_name   = (const int32_t*)do_mmap(gendb_dir + "/sub/name.bin", nb);
        is_hash_raw = (const char*)  do_mmap(gendb_dir + "/indexes/pre_is_hash.bin", nb);
    }

    // Parse pre_is_hash index header
    uint32_t is_cap  = *(const uint32_t*)is_hash_raw;
    uint32_t is_mask = is_cap - 1;
    const IsSlot* is_ht = (const IsSlot*)(is_hash_raw + 4);

    // ---- dim_filter: scan sub, build fy2023 adsh→name open-addressing map ----
    // cap=32768 for ~8.6K qualifying rows (next_pow2(8600*2)=32768, ~27% load)
    static const int     FY23_CAP  = 32768;
    static const uint32_t FY23_MASK = FY23_CAP - 1;

    std::vector<Fy23Slot> fy23_map(FY23_CAP, Fy23Slot{INT32_MIN, -1});

    {
        GENDB_PHASE("dim_filter");
        for (int64_t i = 0; i < sub_rows; i++) {
            if (sub_fy_col[i] != 2023) continue;
            int32_t adsh = sub_adsh[i];
            int32_t nm   = sub_name[i];
            uint32_t h   = hash1u((uint32_t)adsh) & FY23_MASK;
            for (uint32_t p = 0; p < FY23_CAP; p++) {  // C24: bounded
                uint32_t slot = (h + p) & FY23_MASK;
                if (fy23_map[slot].adsh_code == INT32_MIN) {
                    fy23_map[slot] = {adsh, nm};
                    break;
                }
                if (fy23_map[slot].adsh_code == adsh) break; // duplicate (shouldn't happen)
            }
        }
    }

    // ---- Thread-local aggregation setup ----
    // AGG_CAP = next_pow2(186K * 2) = 524288 (C9 — sized for FULL cardinality per thread)
    static const int      AGG_CAP  = 524288;
    static const uint32_t AGG_MASK = AGG_CAP - 1;

    int nthreads = omp_get_max_threads();

    // Allocate per-thread maps (P22: parallel init distributes page faults)
    std::vector<AggSlot*>              local_maps(nthreads, nullptr);
    std::vector<std::vector<int32_t>>  local_occ(nthreads);   // occupied slot indices

    // Parallel allocation + sentinel fill (P22: avoids single-thread page fault stall)
    #pragma omp parallel for schedule(static, 1)
    for (int t = 0; t < nthreads; t++) {
        local_maps[t] = new AggSlot[AGG_CAP];
        // C20: use std::fill, NEVER memset for multi-byte sentinels
        std::fill(local_maps[t], local_maps[t] + AGG_CAP,
                  AggSlot{INT32_MIN, 0, 0, 0, 0LL, 0LL});
        local_occ[t].reserve(30000);
    }

    // ---- main_scan: parallel scan of num with thread-local aggregation ----
    {
        GENDB_PHASE("main_scan");

        const Fy23Slot* fy23_ptr = fy23_map.data();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggSlot*              lmap = local_maps[tid];
            std::vector<int32_t>& locc = local_occ[tid];

            #pragma omp for schedule(dynamic, 65536)
            for (int64_t i = 0; i < num_rows; i++) {

                // --- Filter 1: uom = 'USD' ---
                if (num_uom[i] != usd_code) continue;

                int32_t adsh = num_adsh[i];

                // --- Filter 2: adsh in fy2023 set → get name_code ---
                int32_t name_code = -1;
                {
                    uint32_t h = hash1u((uint32_t)adsh) & FY23_MASK;
                    for (uint32_t p = 0; p < FY23_CAP; p++) {  // C24: bounded
                        uint32_t slot = (h + p) & FY23_MASK;
                        if (fy23_ptr[slot].adsh_code == INT32_MIN) break;  // not found
                        if (fy23_ptr[slot].adsh_code == adsh) {
                            name_code = fy23_ptr[slot].name_code;
                            break;
                        }
                    }
                }
                if (name_code < 0) continue;

                // --- Filter 3: value IS NOT NULL ---
                double val = num_val[i];
                if (std::isnan(val)) continue;

                int32_t tag = num_tag[i];
                int32_t ver = num_ver[i];

                // --- Filter 4 + Join: probe pre_is_hash for (adsh,tag,ver)→plabel_code ---
                // pre_is_hash implicitly filters stmt='IS'; returns plabel_code or -1
                int32_t plabel_code = -1;
                {
                    uint32_t h = hash3(adsh, tag, ver, is_mask);
                    for (uint32_t p = 0; p < is_cap; p++) {  // C24: bounded
                        uint32_t slot = (h + p) & is_mask;
                        if (is_ht[slot].adsh_code == INT32_MIN) break;  // not found
                        if (is_ht[slot].adsh_code == adsh &&
                            is_ht[slot].tag_code   == tag  &&
                            is_ht[slot].ver_code   == ver) {
                            plabel_code = is_ht[slot].plabel_code;
                            break;
                        }
                    }
                }
                if (plabel_code < 0) continue;

                // --- C29: accumulate as int64_t cents (no double/Kahan) ---
                int64_t cents = llround(val * 100.0);

                // --- Thread-local aggregation (C30: ALL 4 dimensions in key) ---
                uint32_t h = hash4(name_code, is_code_val, tag, plabel_code) & AGG_MASK;
                for (uint32_t p = 0; p < AGG_CAP; p++) {  // C24: bounded
                    uint32_t slot = (h + p) & AGG_MASK;
                    if (lmap[slot].name_code == INT32_MIN) {
                        // New group
                        lmap[slot] = {name_code, is_code_val, tag, plabel_code, cents, 1LL};
                        locc.push_back((int32_t)slot);
                        break;
                    }
                    if (lmap[slot].name_code   == name_code  &&
                        lmap[slot].is_code     == is_code_val &&
                        lmap[slot].tag_code    == tag        &&
                        lmap[slot].plabel_code == plabel_code) {
                        lmap[slot].sum_cents += cents;
                        lmap[slot].count++;
                        break;
                    }
                }
            }  // end omp for
        }  // end omp parallel
    }

    // ---- aggregation_merge: merge thread-local maps into final map ----
    // Total occupied entries ~64 × ~10K = ~640K (much less than 12M worst case)
    // because only ~10K qualifying rows per thread after fy2023+IS filters
    std::vector<AggSlot> final_map(AGG_CAP, AggSlot{INT32_MIN, 0, 0, 0, 0LL, 0LL});

    {
        GENDB_PHASE("aggregation_merge");

        AggSlot* fmap = final_map.data();

        for (int t = 0; t < nthreads; t++) {
            AggSlot* lmap = local_maps[t];
            for (int32_t slot_idx : local_occ[t]) {
                const AggSlot& src = lmap[slot_idx];
                uint32_t h = hash4(src.name_code, src.is_code,
                                   src.tag_code,  src.plabel_code) & AGG_MASK;
                for (uint32_t p = 0; p < AGG_CAP; p++) {  // C24: bounded
                    uint32_t slot = (h + p) & AGG_MASK;
                    if (fmap[slot].name_code == INT32_MIN) {
                        fmap[slot] = src;
                        break;
                    }
                    if (fmap[slot].name_code   == src.name_code  &&
                        fmap[slot].is_code     == src.is_code    &&
                        fmap[slot].tag_code    == src.tag_code   &&
                        fmap[slot].plabel_code == src.plabel_code) {
                        fmap[slot].sum_cents += src.sum_cents;
                        fmap[slot].count     += src.count;
                        break;
                    }
                }
            }
            // Free thread-local map
            delete[] local_maps[t];
            local_maps[t] = nullptr;
        }
    }

    // ---- output: top-200 sort + decode + CSV ----
    {
        GENDB_PHASE("output");

        // Collect non-empty entries
        std::vector<const AggSlot*> results;
        results.reserve(300000);
        for (const auto& slot : final_map)
            if (slot.name_code != INT32_MIN)
                results.push_back(&slot);

        // Top-200 by sum_cents DESC (P6: partial_sort = O(n log 200))
        const int K = 200;
        if ((int)results.size() > K) {
            std::partial_sort(results.begin(), results.begin() + K, results.end(),
                [](const AggSlot* a, const AggSlot* b) {
                    return a->sum_cents > b->sum_cents;
                });
            results.resize(K);
        } else {
            std::sort(results.begin(), results.end(),
                [](const AggSlot* a, const AggSlot* b) {
                    return a->sum_cents > b->sum_cents;
                });
        }

        // Write CSV
        std::string out_path = results_dir + "/Q6.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); exit(1); }

        fprintf(fout, "name,stmt,tag,plabel,total_value,cnt\n");

        // Helper: write a CSV field, quoting with "" if it contains a comma or quote
        auto write_field = [&](const char* s) {
            bool needs_quote = false;
            for (const char* p = s; *p; p++) {
                if (*p == ',' || *p == '"' || *p == '\n') { needs_quote = true; break; }
            }
            if (needs_quote) {
                fputc('"', fout);
                for (const char* p = s; *p; p++) {
                    if (*p == '"') fputc('"', fout); // escape double-quote
                    fputc(*p, fout);
                }
                fputc('"', fout);
            } else {
                fputs(s, fout);
            }
        };

        for (const AggSlot* r : results) {
            const char* name   = name_dict[(size_t)r->name_code].c_str();
            const char* stmt   = stmt_dict[(size_t)r->is_code].c_str();
            const char* tag    = tag_dict[(size_t)r->tag_code].c_str();
            const char* plabel = plabel_dict[(size_t)r->plabel_code].c_str();

            write_field(name);   fputc(',', fout);
            write_field(stmt);   fputc(',', fout);
            write_field(tag);    fputc(',', fout);
            write_field(plabel); fputc(',', fout);

            // C29: exact fixed-point output from int64_t cents
            int64_t cents = r->sum_cents;
            if (cents < 0) {
                int64_t ac = -cents;
                fprintf(fout, "-%lld.%02lld,%lld\n",
                        (long long)(ac / 100), (long long)(ac % 100),
                        (long long)r->count);
            } else {
                fprintf(fout, "%lld.%02lld,%lld\n",
                        (long long)(cents / 100), (long long)(cents % 100),
                        (long long)r->count);
            }
        }

        fclose(fout);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n";
        return 1;
    }
    run_q6(argv[1], argv[2]);
    return 0;
}
#endif
