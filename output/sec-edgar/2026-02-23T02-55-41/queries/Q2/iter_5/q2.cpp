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
#include <iostream>
#include <limits>
#include <string>
#include <thread>
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
    // multiply-shift hash; >> 47 gives 17 bits for CAP=131072
    return static_cast<uint32_t>((key * 0x9E3779B97F4A7C15ULL) >> 47) & HT_MASK;
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

// ── Thread-local hash map (full key cardinality, C9 fix) ─────────────────────
// Capacity 131072: covers full ~39k groups (×2 safety factor, ≤50% load).
// Skewed distributions (e.g. uom='pure' rows on one thread) won't overflow.
static const uint32_t TL_CAP  = 131072u;
static const uint32_t TL_MASK2 = 131071u;

struct TLMaxMap {
    uint64_t keys[TL_CAP];
    double   vals[TL_CAP];
};

static inline uint32_t tl_slot(uint64_t key) {
    // Same multiply-shift hash; >> 47 gives 17 bits for CAP=131072
    return static_cast<uint32_t>((key * 0x9E3779B97F4A7C15ULL) >> 47) & TL_MASK2;
}

static void tl_map_update(TLMaxMap* m, uint64_t key, double val) {
    uint32_t slot = tl_slot(key);
    for (uint32_t p = 0; p < TL_CAP; p++) {  // bounded probing (C24)
        uint32_t idx = (slot + p) & TL_MASK2;
        if (m->keys[idx] == EMPTY_KEY64) {
            m->keys[idx] = key;
            m->vals[idx] = val;
            return;
        }
        if (m->keys[idx] == key) {
            if (val > m->vals[idx]) m->vals[idx] = val;
            return;
        }
    }
}

// ── Utilities ────────────────────────────────────────────────────────────────
struct NumRow { int32_t adsh; int32_t tag; double value; };

// Fast dict loader: single fread + manual newline split.
// ~10-20x faster than ifstream/getline for large dicts (86k entries).
static std::vector<std::string> fast_load_dict(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "ERROR: cannot open dict: %s\n", path.c_str()); exit(1); }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    std::vector<char> buf(sz + 1);
    if (sz > 0) { long rd = (long)fread(buf.data(), 1, sz, f); (void)rd; }
    fclose(f);
    buf[sz] = '\n';  // sentinel so every entry ends with \n

    std::vector<std::string> d;
    d.reserve(131072);  // over-reserve; avoids rehash for large dicts
    char* p   = buf.data();
    char* end = buf.data() + sz;
    while (p < end) {
        char* nl = (char*)memchr(p, '\n', end - p);
        if (!nl) nl = end;
        size_t len = (size_t)(nl - p);
        if (len > 0 && p[len - 1] == '\r') --len;  // strip \r\n
        d.emplace_back(p, len);
        p = nl + 1;
    }
    return d;
}


template<typename T>
static const T* mmap_col(const std::string& path, size_t* n_out = nullptr, bool sequential = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open " + path).c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    // MAP_SHARED: share kernel page cache directly (no COW overhead).
    // No MAP_POPULATE: avoid single-threaded page-fault pre-loading (~75ms for 750MB).
    // Page faults happen lazily during parallel scan, distributed across 64 threads.
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror(("mmap " + path).c_str()); exit(1); }
    if (sequential && st.st_size > 0)
        madvise(ptr, st.st_size, MADV_SEQUENTIAL);
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

    // Background thread: loads tag_dict + name_dict while scan runs.
    // Only needed at output phase; declare outside data_loading block so it
    // outlives the scope and can be joined before sort.
    std::thread dict_thread;

    {
        GENDB_PHASE("data_loading");

        // uom_dict is tiny (~25 entries) and needed immediately for pure_code.
        uom_dict = fast_load_dict(gendb_dir + "/num/uom_dict.txt");

        // Find pure_code at runtime (C2: never hardcode)
        for (int i = 0; i < (int)uom_dict.size(); i++) {
            if (uom_dict[i] == "pure") { pure_code = i; break; }
        }
        if (pure_code < 0) { fprintf(stderr, "ERROR: 'pure' not found in uom_dict\n"); exit(1); }

        // Only uom.bin is truly scanned sequentially (every row checked).
        // value/adsh/tag are accessed randomly for ~0.1% of rows — no SEQUENTIAL hint.
        num_uom   = mmap_col<int32_t>(gendb_dir + "/num/uom.bin",   &num_rows, true  /*sequential*/);
        num_value = mmap_col<double> (gendb_dir + "/num/value.bin", nullptr,   false /*random*/);
        num_adsh  = mmap_col<int32_t>(gendb_dir + "/num/adsh.bin",  nullptr,   false /*random*/);
        num_tag   = mmap_col<int32_t>(gendb_dir + "/num/tag.bin",   nullptr,   false /*random*/);
        sub_fy    = mmap_col<int32_t>(gendb_dir + "/sub/fy.bin",    nullptr,   false /*tiny*/);
        sub_name  = mmap_col<int32_t>(gendb_dir + "/sub/name.bin",  nullptr,   false /*tiny*/);

        // Launch background thread to load large dicts while the parallel scan runs.
        // tag_dict (XBRL tags, potentially 10k–100k entries) and
        // name_dict (86k company names) are only needed in the output phase.
        dict_thread = std::thread([&]() {
            tag_dict  = fast_load_dict(gendb_dir + "/num/tag_dict.txt");
            name_dict = fast_load_dict(gendb_dir + "/sub/name_dict.txt");
        });
    }

    // ── Phase 2: Parallel scan — build thread-local max maps + collect fy rows ─
    // Each thread:
    //   (a) Updates a small (32KB, L1-resident) thread-local hash map for max agg.
    //   (b) Pre-filters fy==2022 into thread_fy_rows (avoids re-scan in dim_filter).
    // This folds the sequential build_joins cost into the parallel scan.
    int nthreads = omp_get_max_threads();
    std::vector<TLMaxMap*> tl_maps(nthreads, nullptr);
    std::vector<std::vector<NumRow>> thread_fy_rows(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();

            // malloc inside parallel region → first-touch NUMA-local allocation
            TLMaxMap* lm = (TLMaxMap*)malloc(sizeof(TLMaxMap));
            // C20: std::fill for sentinel init
            std::fill(lm->keys, lm->keys + TL_CAP, EMPTY_KEY64);
            std::fill(lm->vals, lm->vals + TL_CAP, std::numeric_limits<double>::lowest());
            tl_maps[tid] = lm;

            std::vector<NumRow>& fy_local = thread_fy_rows[tid];
            fy_local.reserve(512);

            #pragma omp for schedule(static, 100000)
            for (size_t i = 0; i < num_rows; i++) {
                // common case: uom != pure_code (99.9%) — hint branch predictor
                if (__builtin_expect(num_uom[i] != pure_code, 1)) continue;
                double v = num_value[i];
                if (std::isnan(v)) continue;
                int32_t adsh = num_adsh[i];
                int32_t tag  = num_tag[i];
                // C15: key includes BOTH adsh AND tag
                uint64_t key = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag;
                // Update L1-resident thread-local map (no contention)
                tl_map_update(lm, key, v);
                // Fy pre-filter: sub lookup is O(1) (adsh_code == sub_row_index)
                if (sub_fy[adsh] == 2022) {
                    fy_local.push_back({adsh, tag, v});
                }
            }
        }
    }

    // ── Phase 3: Merge thread-local maps → global max_map ────────────────
    // Sequential merge of 64 × 2048-slot maps (128KB read) into 2MB global map.
    // Expected: ~39k non-empty entries across all thread-local maps.
    {
        GENDB_PHASE("build_joins");

        // C20: std::fill for sentinel init (NOT memset)
        std::fill(g_max_map.keys, g_max_map.keys + HT_CAP, EMPTY_KEY64);
        std::fill(g_max_map.vals, g_max_map.vals + HT_CAP,
                  std::numeric_limits<double>::lowest());

        for (int t = 0; t < nthreads; t++) {
            TLMaxMap* lm = tl_maps[t];
            for (uint32_t s = 0; s < TL_CAP; s++) {
                if (lm->keys[s] != EMPTY_KEY64) {
                    max_map_update(g_max_map, lm->keys[s], lm->vals[s]);
                }
            }
            free(lm);
            tl_maps[t] = nullptr;
        }
    }

    // ── Phase 4: Filter-join on pre-filtered fy rows (12k vs 39k) ────────
    // thread_fy_rows already filtered to sub.fy==2022; only need max-value check.
    std::vector<NumRow> result;
    result.reserve(512);

    {
        GENDB_PHASE("dim_filter");

        for (int t = 0; t < nthreads; t++) {
            for (const auto& r : thread_fy_rows[t]) {
                // Check n.value == MAX(value) for (adsh, tag) across all pure rows
                uint64_t key = ((uint64_t)(uint32_t)r.adsh << 32) | (uint32_t)r.tag;
                double mx = max_map_lookup(g_max_map, key);
                if (r.value == mx) result.push_back(r);
            }
        }
    }

    // ── Phase 5: Pre-decode, sort top-100, emit CSV ──────────────────────
    {
        GENDB_PHASE("output");

        // Ensure background dict loading is complete before we use the dicts.
        if (dict_thread.joinable()) dict_thread.join();

        // Pre-decode: compute name/tag string pointers ONCE per result row.
        // Avoids repeated dict lookups during the O(n log k) partial_sort comparisons.
        // C18: decode via dict[code], not raw int.
        struct DecRow { double value; const char* name; const char* tag; };
        std::vector<DecRow> dec;
        dec.reserve(result.size());
        for (const auto& r : result) {
            dec.push_back({r.value,
                           name_dict[sub_name[r.adsh]].c_str(),
                           tag_dict[r.tag].c_str()});
        }

        size_t topk = std::min((size_t)100, dec.size());
        // P6: partial_sort for LIMIT 100 — O(n log k) not O(n log n)
        std::partial_sort(dec.begin(), dec.begin() + topk, dec.end(),
            [](const DecRow& a, const DecRow& b) {
                if (a.value != b.value) return a.value > b.value;
                int nc = strcmp(a.name, b.name);
                if (nc != 0) return nc < 0;
                return strcmp(a.tag, b.tag) < 0;
            });

        // Write CSV
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen " + out_path).c_str()); exit(1); }

        fprintf(f, "name,tag,value\n");
        for (size_t i = 0; i < topk; i++) {
            const char* name = dec[i].name;
            const char* tag  = dec[i].tag;
            // Quote name if it contains a comma (CSV correctness)
            if (strchr(name, ','))
                fprintf(f, "\"%s\",%s,%.2f\n", name, tag, dec[i].value);
            else
                fprintf(f, "%s,%s,%.2f\n", name, tag, dec[i].value);
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
