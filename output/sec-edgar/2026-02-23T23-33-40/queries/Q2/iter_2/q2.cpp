#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
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
#include "timing_utils.h"

// ── Utility: load dictionary ──────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

// ── Utility: mmap file ────────────────────────────────────────────────────────
static const void* mmap_file(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_out = (size_t)st.st_size;
    void* p = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, size_out, POSIX_FADV_SEQUENTIAL);
    madvise(p, size_out, MADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ── Sub adsh index slot ───────────────────────────────────────────────────────
struct SubSlot { int32_t adsh_code; int32_t sub_row; };

static inline uint32_t hash_i32(int32_t k) {
    uint32_t x = (uint32_t)k;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

// ── Open-addressing max-value hash map ───────────────────────────────────────
static const uint64_t SENTINEL_KEY = UINT64_MAX;

struct MaxSlot {
    uint64_t key;  // packed: ((uint32_t)adsh_code << 32) | (uint32_t)tag_code
    double   val;
};

static inline uint64_t pack_key(int32_t adsh_code, int32_t tag_code) {
    return ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tag_code;
}

static inline uint32_t hash_key64(uint64_t k) {
    k = (k ^ (k >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    k = (k ^ (k >> 27)) * UINT64_C(0x94d049bb133111eb);
    k ^= k >> 31;
    return (uint32_t)k;
}

struct MaxMap {
    uint32_t cap;
    uint32_t mask;
    MaxSlot* slots;
    std::vector<uint32_t> occupied; // slot indices of non-sentinel slots for fast merge

    MaxMap(uint32_t c) : cap(c), mask(c - 1) {
        slots = new MaxSlot[c];
        // C20: std::fill for sentinel, never memset
        std::fill(slots, slots + c, MaxSlot{SENTINEL_KEY, -std::numeric_limits<double>::infinity()});
        occupied.reserve(2048);
    }
    ~MaxMap() { delete[] slots; }

    // Insert or update MAX; track newly inserted slots in occupied list
    void update(uint64_t key, double val) {
        uint32_t h = hash_key64(key) & mask;
        // C24: bounded probing
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t s = (h + p) & mask;
            if (slots[s].key == SENTINEL_KEY) {
                slots[s].key = key;
                slots[s].val = val;
                occupied.push_back(s); // record newly occupied slot
                return;
            }
            if (slots[s].key == key) {
                if (val > slots[s].val) slots[s].val = val;
                return;
            }
        }
        // Table full — should not happen given C9 sizing
        fprintf(stderr, "MaxMap overflow!\n");
    }

    double lookup(uint64_t key) const {
        uint32_t h = hash_key64(key) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t s = (h + p) & mask;
            if (slots[s].key == SENTINEL_KEY) return std::numeric_limits<double>::quiet_NaN();
            if (slots[s].key == key) return slots[s].val;
        }
        return std::numeric_limits<double>::quiet_NaN();
    }
};

// ── Filtered row struct (materialized during scan) ────────────────────────────
struct FilteredRow {
    int32_t adsh_code;
    int32_t tag_code;
    double  value;
};

// ── Result row struct ─────────────────────────────────────────────────────────
struct ResultRow {
    double  value;
    int32_t name_code;
    int32_t tag_code;
};

// ── Query runner ──────────────────────────────────────────────────────────────
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── data_loading ──────────────────────────────────────────────────────────
    std::vector<std::string> uom_dict, tag_dict, name_dict;
    const int32_t* num_adsh = nullptr;
    const int32_t* num_tag  = nullptr;
    const int16_t* num_uom  = nullptr;
    const double*  num_val  = nullptr;
    const int32_t* sub_fy   = nullptr;
    const int32_t* sub_name = nullptr;
    const char*    sub_idx_raw = nullptr;
    size_t num_rows = 0, sub_rows = 0;

    {
        GENDB_PHASE("data_loading");
        // C2: load dicts at runtime, never hardcode codes
        uom_dict  = load_dict(gendb_dir + "/num/uom_dict.txt");
        tag_dict  = load_dict(gendb_dir + "/num/tag_dict.txt");
        name_dict = load_dict(gendb_dir + "/sub/name_dict.txt");

        size_t sz;
        num_adsh = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/adsh.bin", sz));
        num_rows = sz / sizeof(int32_t);
        num_tag  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/tag.bin",  sz));
        num_uom  = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/num/uom.bin",  sz));
        num_val  = reinterpret_cast<const double* >(mmap_file(gendb_dir + "/num/value.bin", sz));

        sub_fy   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/fy.bin",   sz));
        sub_rows = sz / sizeof(int32_t);
        sub_name = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/name.bin", sz));

        sub_idx_raw = reinterpret_cast<const char*>(mmap_file(gendb_dir + "/indexes/sub_adsh_index.bin", sz));
    }

    // C2: find pure_code at runtime
    int16_t pure_code = -1;
    for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
        if (uom_dict[c] == "pure") { pure_code = c; break; }
    if (pure_code < 0) { fprintf(stderr, "pure_code not found!\n"); exit(1); }

    // C27/C32: parse sub_adsh_index header at function scope (before any loop)
    uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);

    // ── dim_filter: build sub_fy_ok direct array ──────────────────────────────
    std::vector<bool> sub_fy_ok(sub_rows, false);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < sub_rows; ++i)
            sub_fy_ok[i] = (sub_fy[i] == 2022);
    }

    // ── main_scan: parallel scan over num ─────────────────────────────────────
    // C9: thread-local maps must be sized for FULL key cardinality (79K unique groups),
    // not per-thread estimate. schedule(static) on unsorted columns means any thread
    // can encounter most or all unique groups in its chunk.
    static const uint32_t MAP_CAP_LOCAL  = 262144; // next_pow2(79K*2), full cardinality per C9
    static const uint32_t MAP_CAP_GLOBAL = 262144; // next_pow2(79K*2) for global merged map

    int nthreads = omp_get_max_threads();
    std::vector<MaxMap*> thread_maps(nthreads, nullptr);
    std::vector<std::vector<FilteredRow>> thread_rows(nthreads);

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // Per-thread map sized to per-thread group cardinality (P23)
            thread_maps[tid] = new MaxMap(MAP_CAP_LOCAL);
            auto& local_rows = thread_rows[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < num_rows; ++i) {
                // Filter: uom == 'pure'
                if (num_uom[i] != pure_code) continue;
                // Filter: value IS NOT NULL
                double v = num_val[i];
                if (std::isnan(v)) continue;

                int32_t ac = num_adsh[i];
                int32_t tc = num_tag[i];
                uint64_t key = pack_key(ac, tc);

                // C15: key includes BOTH adsh_code AND tag_code
                thread_maps[tid]->update(key, v);
                local_rows.push_back({ac, tc, v});
            }
        }
    }

    // ── aggregation_merge: merge thread-local max maps into global ────────────
    // Use occupied lists to skip empty slots — merges only ~79K entries total
    // vs scanning 16.7M slots (16MB vs 256MB sequential read).
    MaxMap global_map(MAP_CAP_GLOBAL);
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; ++t) {
            MaxMap* m = thread_maps[t];
            if (!m) continue;
            // Only iterate occupied slots (tracked during update)
            for (uint32_t idx : m->occupied) {
                global_map.update(m->slots[idx].key, m->slots[idx].val);
            }
            delete thread_maps[t];
            thread_maps[t] = nullptr;
        }
    }

    // ── probe_filter: probe each materialized row against max map + sub join ──
    std::vector<ResultRow> results;
    results.reserve(4096);
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < nthreads; ++t) {
            for (const auto& row : thread_rows[t]) {
                uint64_t key = pack_key(row.adsh_code, row.tag_code);

                // C29: MAX comparison — double bit-equality safe since both read from same binary
                double max_val = global_map.lookup(key);
                if (row.value != max_val) continue;

                // Probe sub_adsh_index: adsh_code -> sub_row
                uint32_t slot = hash_i32(row.adsh_code) & sub_mask;
                int32_t sub_row = -1;
                // C24: bounded probing
                for (uint32_t p = 0; p < sub_cap; ++p) {
                    uint32_t s = (slot + p) & sub_mask;
                    if (sub_ht[s].adsh_code == -1) break;  // empty slot
                    if (sub_ht[s].adsh_code == row.adsh_code) {
                        sub_row = sub_ht[s].sub_row;
                        break;
                    }
                }
                if (sub_row < 0) continue;

                // Filter: sub.fy == 2022
                if (!sub_fy_ok[(size_t)sub_row]) continue;

                results.push_back({row.value, sub_name[sub_row], row.tag_code});
            }
        }
    }

    // ── sort_topk: top-100 by (value DESC, name ASC, tag ASC) ─────────────────
    {
        GENDB_PHASE("sort_topk");
        const size_t K = 100;
        // C33: stable three-key sort for deterministic top-100
        auto cmp = [&](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.value != b.value) return a.value > b.value;
            int nc = name_dict[a.name_code].compare(name_dict[b.name_code]);
            if (nc != 0) return nc < 0;
            return tag_dict[a.tag_code].compare(tag_dict[b.tag_code]) < 0;
        };
        if (results.size() > K) {
            std::partial_sort(results.begin(), results.begin() + K, results.end(), cmp);
            results.resize(K);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }
    }

    // ── output ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); exit(1); }
        // C31: double-quote all string columns
        fprintf(f, "name,tag,value\n");
        size_t lim = std::min(results.size(), size_t(100));
        for (size_t i = 0; i < lim; ++i) {
            const auto& r = results[i];
            // C29: use %.10g for sufficient precision on large double values
            fprintf(f, "\"%s\",\"%s\",%.10g\n",
                name_dict[r.name_code].c_str(),
                tag_dict[r.tag_code].c_str(),
                r.value);
        }
        fclose(f);
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
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
