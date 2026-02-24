// Q2: joins=2, agg=True (MAX), tables=2 (num + sub), LIMIT 100
// Strategy:
//   1. Parallel scan num where uom=='pure' && !isnan(value) → thread-local row vectors
//   2. Sequential build: open-addressing max_map (adsh<<32|tag) → MAX(value)
//   3. Filter: value == max_map[key] && sub_fy[adsh] == 2022
//   4. partial_sort top-100 by (value DESC, name ASC, tag ASC); emit CSV

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "timing_utils.h"

// ── Open-addressing hash map: (adsh<<32|tag) → MAX(double) ──────────────────
// Capacity = next_power_of_2(39000 * 2) = 131072  (C9 fix: ×2 safety factor applied)
static const uint32_t HT_CAP  = 131072u;
static const uint32_t HT_MASK = 131071u;
static const uint64_t EMPTY_KEY64 = UINT64_MAX;   // sentinel (C20: use std::fill)

struct MaxMap {
    uint64_t keys[HT_CAP];
    double   vals[HT_CAP];
};

static MaxMap g_max_map;  // global to avoid stack issues (1 MB)

static inline uint32_t ht_slot(uint64_t key) {
    // multiply-shift hash; >> 48 gives 16 bits for CAP=65536
    return static_cast<uint32_t>((key * 0x9E3779B97F4A7C15ULL) >> 48) & HT_MASK;
}

static void max_map_update(MaxMap& m, uint64_t key, double val) {
    uint32_t slot = ht_slot(key);
    for (uint32_t p = 0; p < HT_CAP; p++) {  // bounded probing (C24)
        uint32_t idx = (slot + p) & HT_MASK;
        if (m.keys[idx] == EMPTY_KEY64) {
            m.keys[idx] = key;
            m.vals[idx] = val;
            return;
        }
        if (m.keys[idx] == key) {
            if (val > m.vals[idx]) m.vals[idx] = val;
            return;
        }
    }
}

static double max_map_lookup(const MaxMap& m, uint64_t key) {
    uint32_t slot = ht_slot(key);
    for (uint32_t p = 0; p < HT_CAP; p++) {  // bounded probing (C24)
        uint32_t idx = (slot + p) & HT_MASK;
        if (m.keys[idx] == EMPTY_KEY64) return std::numeric_limits<double>::quiet_NaN();
        if (m.keys[idx] == key) return m.vals[idx];
    }
    return std::numeric_limits<double>::quiet_NaN();
}

// ── Utilities ────────────────────────────────────────────────────────────────
struct NumRow { int32_t adsh; int32_t tag; double value; };

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { fprintf(stderr, "ERROR: cannot open dict: %s\n", path.c_str()); exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

template<typename T>
static const T* mmap_col(const std::string& path, size_t* n_out = nullptr) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror(("mmap " + path).c_str()); exit(1); }
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    if (n_out) *n_out = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// ── Main query ────────────────────────────────────────────────────────────────
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Phase 1: Data loading ─────────────────────────────────────────────
    const int32_t* num_uom   = nullptr;
    const double*  num_value = nullptr;
    const int32_t* num_adsh  = nullptr;
    const int32_t* num_tag   = nullptr;
    const int32_t* sub_fy    = nullptr;
    const int32_t* sub_name  = nullptr;
    size_t num_rows = 0;

    std::vector<std::string> uom_dict, tag_dict, name_dict;
    int pure_code = -1;

    {
        GENDB_PHASE("data_loading");

        uom_dict  = load_dict(gendb_dir + "/num/uom_dict.txt");
        tag_dict  = load_dict(gendb_dir + "/num/tag_dict.txt");
        name_dict = load_dict(gendb_dir + "/sub/name_dict.txt");

        // Find pure_code at runtime (C2: never hardcode)
        for (int i = 0; i < (int)uom_dict.size(); i++) {
            if (uom_dict[i] == "pure") { pure_code = i; break; }
        }
        if (pure_code < 0) { fprintf(stderr, "ERROR: 'pure' not found in uom_dict\n"); exit(1); }

        num_uom   = mmap_col<int32_t>(gendb_dir + "/num/uom.bin",   &num_rows);
        num_value = mmap_col<double> (gendb_dir + "/num/value.bin");
        num_adsh  = mmap_col<int32_t>(gendb_dir + "/num/adsh.bin");
        num_tag   = mmap_col<int32_t>(gendb_dir + "/num/tag.bin");
        sub_fy    = mmap_col<int32_t>(gendb_dir + "/sub/fy.bin");
        sub_name  = mmap_col<int32_t>(gendb_dir + "/sub/name.bin");
    }

    // ── Phase 2: Parallel scan — collect rows where uom==pure && !isnan ───
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<NumRow>> thread_rows(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<NumRow>& local = thread_rows[tid];
            local.reserve(2048);

            #pragma omp for schedule(static, 100000)
            for (size_t i = 0; i < num_rows; i++) {
                if (num_uom[i] == pure_code && !std::isnan(num_value[i])) {
                    local.push_back({num_adsh[i], num_tag[i], num_value[i]});
                }
            }
        }
    }

    // ── Phase 3: Build max_map sequentially (tiny: ~39k rows) ────────────
    {
        GENDB_PHASE("build_joins");

        // Init sentinels (C20: std::fill, NOT memset)
        std::fill(g_max_map.keys, g_max_map.keys + HT_CAP, EMPTY_KEY64);
        std::fill(g_max_map.vals, g_max_map.vals + HT_CAP,
                  std::numeric_limits<double>::lowest());

        for (auto& tv : thread_rows) {
            for (const auto& r : tv) {
                // C15: key includes BOTH adsh AND tag
                uint64_t key = ((uint64_t)(uint32_t)r.adsh << 32) | (uint32_t)r.tag;
                max_map_update(g_max_map, key, r.value);
            }
        }
    }

    // ── Phase 4: Filter-join: value==max && sub.fy==2022 ─────────────────
    std::vector<NumRow> result;
    result.reserve(1024);

    {
        GENDB_PHASE("dim_filter");

        for (auto& tv : thread_rows) {
            for (const auto& r : tv) {
                // Check sub.fy == 2022 (adsh_code IS sub row index — O(1))
                if (sub_fy[r.adsh] != 2022) continue;
                // Check n.value == MAX(value) for (adsh, tag)
                uint64_t key = ((uint64_t)(uint32_t)r.adsh << 32) | (uint32_t)r.tag;
                double mx = max_map_lookup(g_max_map, key);
                if (r.value != mx) continue;
                result.push_back(r);
            }
        }
    }

    // ── Phase 5: Sort top-100, decode, emit CSV ───────────────────────────
    {
        GENDB_PHASE("output");

        // Comparator: value DESC, name ASC, tag ASC (C18: decode via dict)
        auto cmp = [&](const NumRow& a, const NumRow& b) -> bool {
            if (a.value != b.value) return a.value > b.value;
            const std::string& na = name_dict[sub_name[a.adsh]];
            const std::string& nb = name_dict[sub_name[b.adsh]];
            if (na != nb) return na < nb;
            return tag_dict[a.tag] < tag_dict[b.tag];
        };

        size_t topk = std::min((size_t)100, result.size());
        // P6: partial_sort for LIMIT 100 — O(n log k) not O(n log n)
        std::partial_sort(result.begin(), result.begin() + topk, result.end(), cmp);

        // Write CSV
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen " + out_path).c_str()); exit(1); }

        fprintf(f, "name,tag,value\n");
        for (size_t i = 0; i < topk; i++) {
            const auto& r = result[i];
            const std::string& name = name_dict[sub_name[r.adsh]];
            const std::string& tag  = tag_dict[r.tag];
            // Quote name if it contains a comma (CSV correctness)
            if (name.find(',') != std::string::npos)
                fprintf(f, "\"%s\",%s,%.2f\n", name.c_str(), tag.c_str(), r.value);
            else
                fprintf(f, "%s,%s,%.2f\n", name.c_str(), tag.c_str(), r.value);
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
