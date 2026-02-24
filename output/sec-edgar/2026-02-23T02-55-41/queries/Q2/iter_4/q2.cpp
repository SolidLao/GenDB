// Q2: joins=2, agg=True (MAX), tables=2 (num + sub), LIMIT 100
// Strategy:
//   1. Parallel scan num where uom=='pure' && !isnan(value) → thread-local row vectors
//   2. Merge + sort by (adsh, tag, value DESC): first row in each group = MAX(value)
//   3. Single sequential pass: emit where value==max_val && sub_fy[adsh]==2022
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

    // ── Phase 3: Merge + Sort-based MAX aggregation ───────────────────────
    // Replace 2MB open-addressing HT (cold/NUMA-miss heavy) with:
    //   merge → sort by (adsh,tag,value DESC) → first row per group = MAX
    // 39k rows × 16B = 624KB → fits in L2, sequential access, cache-friendly.
    std::vector<NumRow> all_rows;

    {
        GENDB_PHASE("build_joins");

        // Merge thread-local vectors into one flat array
        size_t total = 0;
        for (auto& tv : thread_rows) total += tv.size();
        all_rows.reserve(total + 1);
        for (auto& tv : thread_rows) {
            all_rows.insert(all_rows.end(), tv.begin(), tv.end());
        }

        // Sort by (adsh ASC, tag ASC, value DESC).
        // After sort: first entry in each (adsh,tag) group has MAX(value).
        std::sort(all_rows.begin(), all_rows.end(), [](const NumRow& a, const NumRow& b) {
            if (a.adsh != b.adsh) return a.adsh < b.adsh;
            if (a.tag  != b.tag)  return a.tag  < b.tag;
            return a.value > b.value;  // DESC: largest first
        });
    }

    // ── Phase 4: Single sequential pass — emit (value==max && fy==2022) ──
    std::vector<NumRow> result;
    result.reserve(1024);

    {
        GENDB_PHASE("dim_filter");

        const size_t n = all_rows.size();
        for (size_t i = 0; i < n; ) {
            const int32_t ga  = all_rows[i].adsh;
            const int32_t gt  = all_rows[i].tag;
            const double  mx  = all_rows[i].value;  // MAX for this group
            const bool fy_ok  = (sub_fy[ga] == 2022);

            // Collect all rows in this group that equal the max value
            // (ties are allowed — all appear in output if fy==2022)
            size_t j = i;
            while (j < n && all_rows[j].adsh == ga && all_rows[j].tag == gt
                         && all_rows[j].value == mx) {
                if (fy_ok) result.push_back(all_rows[j]);
                ++j;
            }
            // Skip remaining (non-max) rows of this group
            while (j < n && all_rows[j].adsh == ga && all_rows[j].tag == gt) ++j;
            i = j;
        }
    }

    // ── Phase 5: Sort top-100, decode, emit CSV ───────────────────────────
    {
        GENDB_PHASE("output");

        // Ensure background dict loading is complete before we use the dicts.
        if (dict_thread.joinable()) dict_thread.join();

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
