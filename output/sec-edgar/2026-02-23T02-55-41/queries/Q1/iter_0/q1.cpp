#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <omp.h>
#include "timing_utils.h"

// -----------------------------------------------------------------------
// Dictionary loader: reads a text file (one entry per line) into a vector.
// Returns the vector of strings; the index == the dict code.
// -----------------------------------------------------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Cannot open dict: " << path << "\n";
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

// -----------------------------------------------------------------------
// mmap helper: returns pointer + size
// -----------------------------------------------------------------------
static const int32_t* mmap_int32(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    n_rows = sz / sizeof(int32_t);
    auto* ptr = reinterpret_cast<const int32_t*>(
        mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return ptr;
}

// -----------------------------------------------------------------------
// Main query function
// -----------------------------------------------------------------------
void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Data loading ----
    const int32_t* stmt_col  = nullptr;
    const int32_t* rfile_col = nullptr;
    const int32_t* adsh_col  = nullptr;
    const int32_t* line_col  = nullptr;
    size_t n_rows = 0;

    std::vector<std::string> stmt_dict, rfile_dict;

    {
        GENDB_PHASE("data_loading");

        stmt_dict  = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        rfile_dict = load_dict(gendb_dir + "/pre/rfile_dict.txt");

        size_t nr1, nr2, nr3, nr4;
        stmt_col  = mmap_int32(gendb_dir + "/pre/stmt.bin",  nr1);
        rfile_col = mmap_int32(gendb_dir + "/pre/rfile.bin", nr2);
        adsh_col  = mmap_int32(gendb_dir + "/pre/adsh.bin",  nr3);
        line_col  = mmap_int32(gendb_dir + "/pre/line.bin",  nr4);
        n_rows = nr1;

        madvise(const_cast<int32_t*>(stmt_col),  nr1 * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(const_cast<int32_t*>(rfile_col), nr2 * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(const_cast<int32_t*>(adsh_col),  nr3 * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(const_cast<int32_t*>(line_col),  nr4 * sizeof(int32_t), MADV_SEQUENTIAL);
    }

    // Find null sentinel code for stmt (empty-string entry)
    int32_t null_stmt_code = -1;
    int n_stmt  = (int)stmt_dict.size();
    int n_rfile = (int)rfile_dict.size();
    for (int i = 0; i < n_stmt; ++i) {
        if (stmt_dict[i].empty()) { null_stmt_code = (int32_t)i; break; }
    }

    int n_groups = n_stmt * n_rfile;

    // ---- Main scan (parallel, thread-local aggregation) ----
    int nthreads = omp_get_max_threads();

    // Thread-local accumulators
    std::vector<std::vector<int64_t>> tl_cnt(nthreads, std::vector<int64_t>(n_groups, 0));
    std::vector<std::vector<int64_t>> tl_sum_line(nthreads, std::vector<int64_t>(n_groups, 0));
    std::vector<std::vector<int64_t>> tl_cnt_line(nthreads, std::vector<int64_t>(n_groups, 0));
    // For COUNT(DISTINCT adsh): thread-local sets per group
    std::vector<std::vector<std::unordered_set<int32_t>>> tl_adsh(
        nthreads, std::vector<std::unordered_set<int32_t>>(n_groups));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lcnt      = tl_cnt[tid];
            auto& lsum_line = tl_sum_line[tid];
            auto& lcnt_line = tl_cnt_line[tid];
            auto& ladsh     = tl_adsh[tid];

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n_rows; ++i) {
                int32_t sc = stmt_col[i];
                // Filter: skip null stmt
                if (sc == null_stmt_code) continue;
                int32_t rc = rfile_col[i];
                int grp = sc * n_rfile + rc;
                lcnt[grp]++;
                int32_t lv = line_col[i];
                lsum_line[grp] += lv;
                lcnt_line[grp]++;
                ladsh[grp].insert(adsh_col[i]);
            }
        }
    }

    // ---- Merge thread-local results ----
    std::vector<int64_t> cnt(n_groups, 0);
    std::vector<int64_t> sum_line(n_groups, 0);
    std::vector<int64_t> cnt_line(n_groups, 0);
    std::vector<std::unordered_set<int32_t>> distinct_adsh(n_groups);

    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; ++t) {
            for (int g = 0; g < n_groups; ++g) {
                cnt[g]      += tl_cnt[t][g];
                sum_line[g] += tl_sum_line[t][g];
                cnt_line[g] += tl_cnt_line[t][g];
                // merge distinct sets
                for (int32_t v : tl_adsh[t][g]) {
                    distinct_adsh[g].insert(v);
                }
            }
        }
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");

        // Build result rows for non-zero groups
        struct Row {
            std::string stmt_str;
            std::string rfile_str;
            int64_t     cnt_val;
            int64_t     num_filings;
            double      avg_line_num;
        };
        std::vector<Row> rows;
        rows.reserve(n_groups);

        for (int sc = 0; sc < n_stmt; ++sc) {
            if (sc == null_stmt_code) continue;
            for (int rc = 0; rc < n_rfile; ++rc) {
                int g = sc * n_rfile + rc;
                if (cnt[g] == 0) continue;
                Row r;
                r.stmt_str    = stmt_dict[sc];
                r.rfile_str   = rfile_dict[rc];
                r.cnt_val     = cnt[g];
                r.num_filings = (int64_t)distinct_adsh[g].size();
                r.avg_line_num = (cnt_line[g] > 0)
                    ? (double)sum_line[g] / (double)cnt_line[g]
                    : 0.0;
                rows.push_back(r);
            }
        }

        // Sort by cnt DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            return a.cnt_val > b.cnt_val;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { std::cerr << "Cannot open output: " << out_path << "\n"; return; }
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : rows) {
            fprintf(f, "%s,%s,%ld,%ld,%.2f\n",
                r.stmt_str.c_str(), r.rfile_str.c_str(),
                r.cnt_val, r.num_filings, r.avg_line_num);
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
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
