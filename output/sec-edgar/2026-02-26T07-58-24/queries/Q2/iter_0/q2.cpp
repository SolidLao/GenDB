// Q2: SEC-EDGAR query — decorrelated MAX subquery
// SELECT s.name, n.tag, n.value
// FROM num n JOIN sub s ON n.adsh = s.adsh
// JOIN (SELECT adsh, tag, MAX(value) FROM num WHERE uom='pure' AND value IS NOT NULL GROUP BY adsh,tag) m
//   ON n.adsh=m.adsh AND n.tag=m.tag AND n.value=m.max_value
// WHERE n.uom='pure' AND s.fy=2022 AND n.value IS NOT NULL
// ORDER BY n.value DESC, s.name, n.tag LIMIT 100

#include <cstdio>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>
#include "timing_utils.h"

// ===== Hash functions =====
static inline uint64_t hash_int32x2(int32_t a, int32_t b) {
    uint64_t ha = (uint64_t)(uint32_t)a * 0x9E3779B97F4A7C15ULL;
    uint64_t hb = (uint64_t)(uint32_t)b * 0x9E3779B97F4A7C15ULL;
    return ha ^ (hb * 0x517CC1B727220A95ULL + 0x6C62272E07BB0142ULL + (ha << 6) + (ha >> 2));
}

static inline uint64_t pack_key(int32_t adsh, int32_t tag) {
    return ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag;
}

// ===== Open-addressing hash map: {adsh_code,tag_code} -> max_value =====
// Empty sentinel: key == UINT64_MAX
struct MaxSlot {
    uint64_t key;   // packed adsh<<32|tag, UINT64_MAX = empty
    double   value; // max value
};

static void max_map_update(MaxSlot* ht, uint32_t mask,
                           int32_t adsh, int32_t tag, double val) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_int32x2(adsh, tag) & mask);
    while (true) {
        if (ht[pos].key == UINT64_MAX) {
            ht[pos].key   = key;
            ht[pos].value = val;
            return;
        }
        if (ht[pos].key == key) {
            if (val > ht[pos].value) ht[pos].value = val;
            return;
        }
        pos = (pos + 1) & mask;
    }
}

static void max_map_merge(MaxSlot* ht, uint32_t mask,
                          uint64_t packed_key, double val) {
    int32_t adsh = (int32_t)(uint32_t)(packed_key >> 32);
    int32_t tag  = (int32_t)(uint32_t)packed_key;
    uint32_t pos = (uint32_t)(hash_int32x2(adsh, tag) & mask);
    while (true) {
        if (ht[pos].key == UINT64_MAX) {
            ht[pos].key   = packed_key;
            ht[pos].value = val;
            return;
        }
        if (ht[pos].key == packed_key) {
            if (val > ht[pos].value) ht[pos].value = val;
            return;
        }
        pos = (pos + 1) & mask;
    }
}

static inline double max_map_lookup(const MaxSlot* ht, uint32_t mask,
                                    int32_t adsh, int32_t tag) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_int32x2(adsh, tag) & mask);
    while (true) {
        if (ht[pos].key == UINT64_MAX) return std::numeric_limits<double>::quiet_NaN();
        if (ht[pos].key == key) return ht[pos].value;
        pos = (pos + 1) & mask;
    }
}

// ===== Utility: mmap file =====
static void* mmap_file(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); size_out = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    size_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); size_out = 0; return nullptr; }
    return p;
}

// ===== Load binary dict: [n:uint32][len:uint16, bytes...]*n =====
static std::vector<std::string> load_binary_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { perror(path.c_str()); return dict; }
    uint32_t n = 0;
    if (fread(&n, 4, 1, f) != 1) { fclose(f); return dict; }
    dict.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = 0;
        if (fread(&len, 2, 1, f) != 1) break;
        std::string s(len, '\0');
        if (len > 0) fread(&s[0], 1, len, f);
        dict.push_back(std::move(s));
    }
    fclose(f);
    return dict;
}

// ===== Candidate for top-100 output =====
struct Candidate {
    double  value;
    int32_t adsh_code;
    int32_t tag_code;
    int32_t name_code;
};

// ===== Zone map entry (10 bytes each, packed) =====
#pragma pack(push, 1)
struct ZoneEntry {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};
#pragma pack(pop)

// ===== Main query =====
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ===== DATA LOADING =====
    size_t num_uom_sz, num_value_sz, num_adsh_sz, num_tag_sz;
    size_t sub_fy_sz, sub_name_sz, zonemaps_sz;

    const int8_t*  num_uom   = nullptr;
    const double*  num_value = nullptr;
    const int32_t* num_adsh  = nullptr;
    const int32_t* num_tag   = nullptr;
    const int16_t* sub_fy    = nullptr;
    const int32_t* sub_name  = nullptr;
    const void*    zonemaps_raw = nullptr;

    std::vector<std::string> tag_dict, name_dict;
    int8_t pure_code = -1;
    size_t num_rows = 0;

    // Zone map qualifying blocks
    std::vector<int> qualifying_blocks;

    {
        GENDB_PHASE("data_loading");

        // Load UOM dict to resolve pure_code
        auto uom_dict = load_binary_dict(gendb_dir + "/shared/uom.dict");
        for (int i = 0; i < (int)uom_dict.size(); i++) {
            if (uom_dict[i] == "pure") { pure_code = (int8_t)i; break; }
        }
        if (pure_code < 0) {
            std::cerr << "ERROR: 'pure' not found in uom.dict\n";
            return;
        }

        // Load dicts for output decoding (defer string access to output phase)
        tag_dict  = load_binary_dict(gendb_dir + "/shared/tag_numpre.dict");
        name_dict = load_binary_dict(gendb_dir + "/sub/name.dict");

        // mmap num columns
        num_uom   = (const int8_t*) mmap_file(gendb_dir + "/num/uom.bin",   num_uom_sz);
        num_value = (const double*)  mmap_file(gendb_dir + "/num/value.bin",  num_value_sz);
        num_adsh  = (const int32_t*) mmap_file(gendb_dir + "/num/adsh.bin",   num_adsh_sz);
        num_tag   = (const int32_t*) mmap_file(gendb_dir + "/num/tag.bin",    num_tag_sz);
        // mmap sub columns (small, fit in RAM)
        sub_fy    = (const int16_t*) mmap_file(gendb_dir + "/sub/fy.bin",     sub_fy_sz);
        sub_name  = (const int32_t*) mmap_file(gendb_dir + "/sub/name.bin",   sub_name_sz);

        num_rows = num_uom_sz / sizeof(int8_t);

        // mmap zone maps
        zonemaps_raw = mmap_file(gendb_dir + "/indexes/num_zonemaps.bin", zonemaps_sz);

        // Prefetch sub columns immediately (small, frequently accessed)
        if (sub_fy)   madvise((void*)sub_fy,   sub_fy_sz,   MADV_WILLNEED);
        if (sub_name) madvise((void*)sub_name,  sub_name_sz, MADV_WILLNEED);

        // ===== Zone map filtering =====
        if (zonemaps_raw) {
            int32_t n_blocks = *(const int32_t*)zonemaps_raw;
            const ZoneEntry* zones = (const ZoneEntry*)((const char*)zonemaps_raw + 4);
            for (int b = 0; b < n_blocks; b++) {
                // Skip block if pure_code out of [uom_min, uom_max]
                if (pure_code < zones[b].uom_min || pure_code > zones[b].uom_max) continue;
                qualifying_blocks.push_back(b);
            }
        } else {
            // No zone maps: process all blocks
            int32_t n_blocks = (int32_t)((num_rows + 99999) / 100000);
            for (int b = 0; b < n_blocks; b++) qualifying_blocks.push_back(b);
        }

        // Advise sequential on qualified block ranges
        for (int b : qualifying_blocks) {
            size_t off   = (size_t)b * 100000;
            size_t count = std::min((size_t)100000, num_rows - off);
            madvise((void*)(num_uom   + off),       count * sizeof(int8_t),  MADV_WILLNEED);
            madvise((void*)(num_value + off),       count * sizeof(double),  MADV_WILLNEED);
            madvise((void*)(num_adsh  + off),       count * sizeof(int32_t), MADV_WILLNEED);
            madvise((void*)(num_tag   + off),       count * sizeof(int32_t), MADV_WILLNEED);
        }
    }

    // ===== PHASE 1: Build thread-local max_maps (hash_group_by) =====
    // Capacity: next pow2 >= 118000 * 2 = 236000 -> 262144
    static const uint32_t MAX_CAP  = 262144;
    static const uint32_t MAX_MASK = MAX_CAP - 1;

    int nthreads = omp_get_max_threads();
    // Cap threads at number of qualifying blocks for efficiency
    int active_threads = std::max(1, std::min(nthreads, (int)qualifying_blocks.size()));
    std::vector<MaxSlot*> thread_maps(active_threads, nullptr);

    // Allocate + initialize thread-local maps (NUMA-local via parallel init)
    for (int t = 0; t < active_threads; t++) {
        thread_maps[t] = (MaxSlot*)mmap(nullptr, MAX_CAP * sizeof(MaxSlot),
                                         PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    #pragma omp parallel for num_threads(active_threads) schedule(static)
    for (int t = 0; t < active_threads; t++) {
        MaxSlot* ht = thread_maps[t];
        for (uint32_t i = 0; i < MAX_CAP; i++) {
            ht[i].key   = UINT64_MAX;
            ht[i].value = -std::numeric_limits<double>::infinity();
        }
    }

    {
        GENDB_PHASE("build_joins");

        int nb = (int)qualifying_blocks.size();

        #pragma omp parallel num_threads(active_threads)
        {
            int tid = omp_get_thread_num();
            MaxSlot* local_ht = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1)
            for (int bi = 0; bi < nb; bi++) {
                int b = qualifying_blocks[bi];
                size_t row_start = (size_t)b * 100000;
                size_t row_end   = std::min(row_start + 100000, num_rows);

                for (size_t i = row_start; i < row_end; i++) {
                    if (num_uom[i] != pure_code) continue;
                    double val = num_value[i];
                    if (std::isnan(val)) continue;
                    int32_t adsh = num_adsh[i];
                    int32_t tag  = num_tag[i];
                    max_map_update(local_ht, MAX_MASK, adsh, tag, val);
                }
            }
        }
    }

    // ===== MERGE thread-local maps -> global max_map =====
    MaxSlot* global_map = (MaxSlot*)mmap(nullptr, MAX_CAP * sizeof(MaxSlot),
                                          PROT_READ | PROT_WRITE,
                                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    for (uint32_t i = 0; i < MAX_CAP; i++) {
        global_map[i].key   = UINT64_MAX;
        global_map[i].value = -std::numeric_limits<double>::infinity();
    }

    {
        GENDB_PHASE("dim_filter"); // merge phase

        for (int t = 0; t < active_threads; t++) {
            MaxSlot* local_ht = thread_maps[t];
            for (uint32_t i = 0; i < MAX_CAP; i++) {
                if (local_ht[i].key == UINT64_MAX) continue;
                max_map_merge(global_map, MAX_MASK, local_ht[i].key, local_ht[i].value);
            }
            munmap(local_ht, MAX_CAP * sizeof(MaxSlot));
            thread_maps[t] = nullptr;
        }
    }

    // ===== PHASE 2: Probe max_map + sub_fy filter, collect top-100 =====
    std::vector<std::vector<Candidate>> thread_candidates(active_threads);

    {
        GENDB_PHASE("main_scan");

        int nb = (int)qualifying_blocks.size();

        #pragma omp parallel num_threads(active_threads)
        {
            int tid = omp_get_thread_num();
            auto& local_cands = thread_candidates[tid];

            #pragma omp for schedule(dynamic, 1)
            for (int bi = 0; bi < nb; bi++) {
                int b = qualifying_blocks[bi];
                size_t row_start = (size_t)b * 100000;
                size_t row_end   = std::min(row_start + 100000, num_rows);

                for (size_t i = row_start; i < row_end; i++) {
                    if (num_uom[i] != pure_code) continue;
                    double val = num_value[i];
                    if (std::isnan(val)) continue;

                    int32_t adsh = num_adsh[i];
                    int32_t tag  = num_tag[i];

                    // Probe max_map: keep only rows where value == max_value
                    double max_val = max_map_lookup(global_map, MAX_MASK, adsh, tag);
                    if (val != max_val) continue;

                    // Direct array lookup: sub_fy[adsh_code] (adsh_code == sub row_id)
                    if (sub_fy[adsh] != (int16_t)2022) continue;

                    // Row passes all filters — collect name_code for output
                    int32_t name_code = sub_name[adsh];
                    local_cands.push_back({val, adsh, tag, name_code});
                }
            }
        }
    }

    // ===== Merge candidates =====
    std::vector<Candidate> all_candidates;
    {
        size_t total = 0;
        for (int t = 0; t < active_threads; t++) total += thread_candidates[t].size();
        all_candidates.reserve(total);
        for (int t = 0; t < active_threads; t++) {
            auto& v = thread_candidates[t];
            all_candidates.insert(all_candidates.end(), v.begin(), v.end());
        }
    }

    // ===== TOP-100: partial_sort by (value DESC, name ASC, tag ASC) =====
    {
        GENDB_PHASE("output");

        size_t k = std::min((size_t)100, all_candidates.size());
        std::partial_sort(all_candidates.begin(),
                          all_candidates.begin() + k,
                          all_candidates.end(),
                          [&](const Candidate& a, const Candidate& b) {
                              if (a.value != b.value) return a.value > b.value;
                              const std::string& na = name_dict[a.name_code];
                              const std::string& nb_str = name_dict[b.name_code];
                              if (na != nb_str) return na < nb_str;
                              return tag_dict[a.tag_code] < tag_dict[b.tag_code];
                          });
        all_candidates.resize(k);

        // Write output CSV
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");
        for (const auto& c : all_candidates) {
            const std::string& name = name_dict[c.name_code];
            const std::string& tag  = tag_dict[c.tag_code];
            // Standard CSV quoting: quote fields containing commas, quotes, or newlines
            bool name_needs_quote = (name.find(',') != std::string::npos ||
                                     name.find('"') != std::string::npos);
            bool tag_needs_quote  = (tag.find(',')  != std::string::npos ||
                                     tag.find('"')  != std::string::npos);
            if (name_needs_quote) {
                // escape any internal quotes
                std::string escaped;
                escaped.reserve(name.size() + 2);
                for (char ch : name) {
                    if (ch == '"') escaped += "\"\"";
                    else escaped += ch;
                }
                fprintf(f, "\"%s\"", escaped.c_str());
            } else {
                fprintf(f, "%s", name.c_str());
            }
            fprintf(f, ",");
            if (tag_needs_quote) {
                std::string escaped;
                for (char ch : tag) {
                    if (ch == '"') escaped += "\"\"";
                    else escaped += ch;
                }
                fprintf(f, "\"%s\"", escaped.c_str());
            } else {
                fprintf(f, "%s", tag.c_str());
            }
            fprintf(f, ",%.2f\n", c.value);
        }
        fclose(f);
    }

    // Cleanup
    munmap(global_map, MAX_CAP * sizeof(MaxSlot));
    if (num_uom)      munmap((void*)num_uom,    num_uom_sz);
    if (num_value)    munmap((void*)num_value,  num_value_sz);
    if (num_adsh)     munmap((void*)num_adsh,   num_adsh_sz);
    if (num_tag)      munmap((void*)num_tag,    num_tag_sz);
    if (sub_fy)       munmap((void*)sub_fy,     sub_fy_sz);
    if (sub_name)     munmap((void*)sub_name,   sub_name_sz);
    if (zonemaps_raw) munmap((void*)zonemaps_raw, zonemaps_sz);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argv[2];
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
