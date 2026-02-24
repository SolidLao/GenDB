// Q3 — SEC-EDGAR: SUM(value) per (name,cik) with HAVING AVG scalar subquery
// Strategy: direct sub lookup (adsh_code == sub_row), dual accumulation single pass,
//           sequential scan for deterministic FP, partial_sort top-100.

#include <cstdint>
#include <cmath>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

namespace {

// ─── constants ───────────────────────────────────────────────────────────────
static constexpr int      OUTER_CAP   = 65536;      // next_pow2(27000*2) (C9)
static constexpr int      INNER_CAP   = 65536;      // next_pow2(20000*2) (C9)
static constexpr uint64_t OUTER_EMPTY = UINT64_MAX; // sentinel
static constexpr int32_t  INNER_EMPTY = INT32_MIN;  // sentinel (std::fill, C20)
static constexpr uint32_t OUTER_MASK  = OUTER_CAP - 1;
static constexpr uint32_t INNER_MASK  = INNER_CAP - 1;

// ─── mmap helper ─────────────────────────────────────────────────────────────
template<typename T>
struct MmapCol {
    const T* data  = nullptr;
    size_t   nrows = 0;
    int      fd    = -1;
    size_t   bytes = 0;

    void open(const std::string& path, int advice = POSIX_FADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = st.st_size;
        nrows = bytes / sizeof(T);
        data  = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); std::exit(1); }
        posix_fadvise(fd, 0, bytes, advice);
    }
    void hint(int advice) { madvise((void*)data, bytes, advice); }
    ~MmapCol() {
        if (data && data != MAP_FAILED) munmap((void*)data, bytes);
        if (fd >= 0) close(fd);
    }
};

// ─── open-addressing map: (uint64_t key → double val, simple summation) ──────
struct OuterMap {
    uint64_t keys[OUTER_CAP];
    double   vals[OUTER_CAP]; // simple sum (matches reference implementation)

    void init() {
        std::fill(keys, keys + OUTER_CAP, OUTER_EMPTY); // C20: no memset
        std::fill(vals, vals + OUTER_CAP, 0.0);
    }

    inline void accumulate(uint64_t key, double v) {
        uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 48) & OUTER_MASK;
        for (int p = 0; p < OUTER_CAP; ++p) { // C24: bounded probing
            uint32_t slot = (h + p) & OUTER_MASK;
            if (keys[slot] == OUTER_EMPTY) {
                keys[slot] = key; vals[slot] = v; return;
            }
            if (keys[slot] == key) {
                vals[slot] += v;
                return;
            }
        }
    }
};

// ─── open-addressing map: (int32_t key → double val, simple summation) ───────
struct InnerMap {
    int32_t keys[INNER_CAP];
    double  vals[INNER_CAP]; // simple sum (matches reference implementation)

    void init() {
        std::fill(keys, keys + INNER_CAP, INNER_EMPTY); // C20: no memset
        std::fill(vals, vals + INNER_CAP, 0.0);
    }

    inline void accumulate(int32_t key, double v) {
        uint32_t h = ((uint32_t)key * 2654435761u) & INNER_MASK;
        for (int p = 0; p < INNER_CAP; ++p) { // C24: bounded probing
            uint32_t slot = (h + p) & INNER_MASK;
            if (keys[slot] == INNER_EMPTY) {
                keys[slot] = key; vals[slot] = v; return;
            }
            if (keys[slot] == key) {
                vals[slot] += v;
                return;
            }
        }
    }
};

// ─── load dictionary ──────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { std::cerr << "Cannot open dict: " << path << "\n"; std::exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

// ─── CSV-safe field output (quote if contains comma or quote) ─────────────────
static void write_csv_field(FILE* f, const std::string& s) {
    bool needs_quote = (s.find(',') != std::string::npos ||
                        s.find('"') != std::string::npos ||
                        s.find('\n') != std::string::npos);
    if (!needs_quote) {
        fwrite(s.data(), 1, s.size(), f);
    } else {
        fputc('"', f);
        for (char c : s) {
            if (c == '"') fputc('"', f); // escape quote by doubling
            fputc(c, f);
        }
        fputc('"', f);
    }
}

// ─── main query function ──────────────────────────────────────────────────────
} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string sub_dir = gendb_dir + "/sub";
    const std::string num_dir = gendb_dir + "/num";

    // ── dictionaries ──────────────────────────────────────────────────────────
    std::vector<std::string> uom_dict, name_dict;
    int usd_code = -1;
    {
        GENDB_PHASE("data_loading");
        uom_dict  = load_dict(num_dir + "/uom_dict.txt");
        name_dict = load_dict(sub_dir + "/name_dict.txt");
        // find USD code (C2)
        for (int i = 0; i < (int)uom_dict.size(); ++i) {
            if (uom_dict[i] == "USD") { usd_code = i; break; }
        }
        if (usd_code < 0) { std::cerr << "USD not found in uom_dict\n"; std::exit(1); }
    }

    // ── mmap columns ─────────────────────────────────────────────────────────
    MmapCol<int32_t> sub_fy, sub_name, sub_cik;
    MmapCol<int32_t> num_adsh, num_uom;
    MmapCol<double>  num_value;
    {
        GENDB_PHASE("data_loading");
        sub_fy  .open(sub_dir + "/fy.bin",    POSIX_FADV_SEQUENTIAL);
        sub_name.open(sub_dir + "/name.bin",  POSIX_FADV_RANDOM);
        sub_cik .open(sub_dir + "/cik.bin",   POSIX_FADV_RANDOM);
        num_adsh .open(num_dir + "/adsh.bin",  POSIX_FADV_SEQUENTIAL);
        num_uom  .open(num_dir + "/uom.bin",   POSIX_FADV_SEQUENTIAL);
        num_value.open(num_dir + "/value.bin", POSIX_FADV_SEQUENTIAL);
        sub_name.hint(MADV_RANDOM);
        sub_cik .hint(MADV_RANDOM);
        num_adsh .hint(MADV_SEQUENTIAL);
        num_uom  .hint(MADV_SEQUENTIAL);
        num_value.hint(MADV_SEQUENTIAL);
    }

    const size_t sub_n = sub_fy.nrows;
    const size_t num_n = num_adsh.nrows;

    // ── build fy2022 filter array ─────────────────────────────────────────────
    std::vector<bool> fy2022(sub_n, false);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < sub_n; ++i)
            fy2022[i] = (sub_fy.data[i] == 2022);
    }

    // ── single-pass sequential scan: dual accumulation ────────────────────────
    // Sequential for deterministic FP (matches reference implementation order)
    OuterMap* global_outer = new OuterMap();
    InnerMap* global_inner = new InnerMap();
    global_outer->init();
    global_inner->init();

    {
        GENDB_PHASE("main_scan");

        const int32_t* __restrict__ adsh_col = num_adsh.data;
        const int32_t* __restrict__ uom_col  = num_uom.data;
        const double*  __restrict__ val_col  = num_value.data;
        const int32_t* __restrict__ name_col = sub_name.data;
        const int32_t* __restrict__ cik_col  = sub_cik.data;
        const size_t N = num_n;

        for (size_t i = 0; i < N; ++i) {
            if (uom_col[i] != usd_code) continue;
            double v = val_col[i];
            if (std::isnan(v)) continue;
            int32_t sub_row = adsh_col[i];
            if (!fy2022[sub_row]) continue;

            int32_t name_code = name_col[sub_row];
            int32_t cik       = cik_col[sub_row];

            // outer: group by (name_code, cik)
            uint64_t outer_key = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)cik;
            global_outer->accumulate(outer_key, v);

            // inner: group by cik (scalar subquery)
            global_inner->accumulate(cik, v);
        }
    }

    // ── compute HAVING threshold: AVG(per-cik sum) ────────────────────────────
    double threshold = 0.0;
    {
        GENDB_PHASE("subquery_precompute");
        double total_sum = 0.0;
        int64_t cik_count = 0;
        for (int i = 0; i < INNER_CAP; ++i) {
            if (global_inner->keys[i] != INNER_EMPTY) {
                total_sum += global_inner->vals[i];
                ++cik_count;
            }
        }
        threshold = (cik_count > 0) ? (total_sum / (double)cik_count) : 0.0;
        delete global_inner;
    }

    // ── collect outer groups passing HAVING, sort DESC, limit 100 ────────────
    struct Row { int32_t name_code; int32_t cik; double total_value; };
    std::vector<Row> passing;
    passing.reserve(4096);

    for (int i = 0; i < OUTER_CAP; ++i) {
        if (global_outer->keys[i] == OUTER_EMPTY) continue;
        double tv = global_outer->vals[i];
        if (tv > threshold) {
            uint64_t k = global_outer->keys[i];
            int32_t nc  = (int32_t)(k >> 32);
            int32_t cik = (int32_t)(k & 0xFFFFFFFFULL);
            passing.push_back({nc, cik, tv});
        }
    }
    delete global_outer;

    // partial sort: top 100 DESC (P6)
    {
        GENDB_PHASE("sort_topk");
        size_t k = std::min((size_t)100, passing.size());
        std::partial_sort(passing.begin(), passing.begin() + k, passing.end(),
            [](const Row& a, const Row& b) { return a.total_value > b.total_value; });
        passing.resize(k);
    }

    // ── output CSV ────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q3.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); std::exit(1); }

        fprintf(fout, "name,cik,total_value\n");
        for (const Row& r : passing) {
            const std::string& name_str = name_dict[(size_t)r.name_code]; // C18
            write_csv_field(fout, name_str);
            fprintf(fout, ",%d,%.2f\n", r.cik, r.total_value);
        }
        fclose(fout);
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
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
