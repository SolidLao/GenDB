// Q1: Single-table aggregation on pre
// SELECT stmt, rfile, COUNT(*) AS cnt,
//        COUNT(DISTINCT adsh) AS num_filings,
//        AVG(line) AS avg_line_num
// FROM pre WHERE stmt IS NOT NULL
// GROUP BY stmt, rfile ORDER BY cnt DESC

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// --- Constants ---
static constexpr int MAX_SLOTS  = 64;      // max stmt_codes * max rfile_codes

// --- Dict helpers ---
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

static int16_t find_code(const std::vector<std::string>& d, const std::string& val) {
    for (int i = 0; i < (int)d.size(); i++)
        if (d[i] == val) return (int16_t)i;
    return -1;
}

// Fast line counter — avoids building 86K string objects just to get dict size
static int fast_count_lines(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return 0; }
    struct stat st;
    fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return 0; }
    const char* p = (const char*)mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) return 0;
    int n = 0;
    const char* end = p + st.st_size;
    for (const char* c = p; c < end; c++) n += (*c == '\n');
    // Account for last line if file doesn't end with newline
    if (st.st_size > 0 && end[-1] != '\n') n++;
    munmap((void*)p, (size_t)st.st_size);
    return n;
}

// --- mmap helper ---
template<typename T>
static const T* mmap_col(const std::string& path, size_t& out_n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_n = (size_t)st.st_size / sizeof(T);
    void* p = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, (size_t)st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// --- Per-thread aggregation state ---
struct ThreadAgg {
    std::vector<int64_t>  count;      // [MAX_SLOTS]
    std::vector<int64_t>  sum_line;   // [MAX_SLOTS]
    std::vector<uint64_t> adsh_bits;  // [MAX_SLOTS * adsh_words]

    void init(int adsh_words) {
        count.assign(MAX_SLOTS, 0LL);
        sum_line.assign(MAX_SLOTS, 0LL);
        adsh_bits.assign((size_t)MAX_SLOTS * adsh_words, 0ULL);
    }
};

// --- Query entry point ---
void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- data_loading ----
    std::vector<std::string> stmt_dict, rfile_dict;
    int16_t null_code   = -1;
    int     max_rfile   = 0;
    int     adsh_count  = 0;
    int     adsh_words  = 0;

    const int16_t* stmt_col  = nullptr;
    const int16_t* rfile_col = nullptr;
    const int32_t* adsh_col  = nullptr;
    const int32_t* line_col  = nullptr;
    size_t nrows = 0;

    {
        GENDB_PHASE("data_loading");

        stmt_dict  = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        rfile_dict = load_dict(gendb_dir + "/pre/rfile_dict.txt");
        null_code  = find_code(stmt_dict, "");
        max_rfile  = (int)rfile_dict.size();

        // Count adsh dict entries without loading strings — we only need the cardinality
        adsh_count = fast_count_lines(gendb_dir + "/adsh_global_dict.txt");
        adsh_words = (adsh_count + 63) / 64;

        // Verify slot bounds
        int total_slots = (int)stmt_dict.size() * max_rfile;
        if (total_slots > MAX_SLOTS) {
            fprintf(stderr, "ERROR: total_slots=%d exceeds MAX_SLOTS=%d\n",
                    total_slots, MAX_SLOTS);
            exit(1);
        }

        size_t n1, n2, n3, n4;
        stmt_col  = mmap_col<int16_t>(gendb_dir + "/pre/stmt.bin",  n1);
        rfile_col = mmap_col<int16_t>(gendb_dir + "/pre/rfile.bin", n2);
        adsh_col  = mmap_col<int32_t>(gendb_dir + "/pre/adsh.bin",  n3);
        line_col  = mmap_col<int32_t>(gendb_dir + "/pre/line.bin",  n4);
        nrows = n1;
    }

    // ---- main_scan (parallel with thread-local aggregation) ----
    int nthreads = omp_get_max_threads();
    std::vector<ThreadAgg> thread_agg(nthreads);
    for (auto& ta : thread_agg) ta.init(adsh_words);

    // Hoist runtime-constant null check out of the hot loop
    const int16_t nc       = null_code;        // -1 if no null code → branch never taken
    const bool    has_null = (null_code >= 0);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            ThreadAgg& agg = thread_agg[tid];

            int64_t*  lcnt  = agg.count.data();
            int64_t*  lsum  = agg.sum_line.data();
            uint64_t* lbits = agg.adsh_bits.data();

            #pragma omp for schedule(static)
            for (size_t i = 0; i < nrows; i++) {
                int16_t sc = stmt_col[i];
                // Filter: skip rows where stmt maps to empty string (NULL sentinel)
                if (has_null && sc == nc) continue;

                int16_t rc   = rfile_col[i];
                int32_t adsh = adsh_col[i];
                int32_t line = line_col[i];

                int slot = (int)sc * max_rfile + (int)rc;

                lcnt[slot]++;
                lsum[slot] += line;

                // Set bit for this adsh code in the per-slot bitset
                lbits[(size_t)slot * adsh_words + (adsh >> 6)] |=
                    (1ULL << (adsh & 63));
            }
        }
    }

    // ---- aggregation_merge ----
    // Global arrays (heap-allocated, ~672 KB each)
    std::vector<int64_t>  g_count(MAX_SLOTS, 0LL);
    std::vector<int64_t>  g_sum_line(MAX_SLOTS, 0LL);
    std::vector<uint64_t> g_adsh_bits((size_t)MAX_SLOTS * adsh_words, 0ULL);

    {
        GENDB_PHASE("aggregation_merge");

        // Parallelize over slots: each thread owns one slot's reduction
        #pragma omp parallel for num_threads(nthreads) schedule(static)
        for (int s = 0; s < MAX_SLOTS; s++) {
            int64_t  cnt  = 0, sline = 0;
            uint64_t* gsw = g_adsh_bits.data() + (size_t)s * adsh_words;
            for (int t = 0; t < nthreads; t++) {
                cnt   += thread_agg[t].count[s];
                sline += thread_agg[t].sum_line[s];
                const uint64_t* tsw = thread_agg[t].adsh_bits.data() + (size_t)s * adsh_words;
                for (int w = 0; w < adsh_words; w++)
                    gsw[w] |= tsw[w];
            }
            g_count[s]    = cnt;
            g_sum_line[s] = sline;
        }
    }

    // ---- collect groups, sort, output ----
    {
        GENDB_PHASE("output");

        struct Group {
            int     stmt_code;
            int     rfile_code;
            int64_t cnt;
            int64_t num_filings;
            double  avg_line;
        };

        std::vector<Group> groups;
        groups.reserve(MAX_SLOTS);

        int max_stmt = (int)stmt_dict.size();
        const uint64_t* gbits = g_adsh_bits.data();

        for (int sc = 0; sc < max_stmt; sc++) {
            if (null_code >= 0 && sc == (int)null_code) continue;
            for (int rc = 0; rc < max_rfile; rc++) {
                int slot = sc * max_rfile + rc;
                if (g_count[slot] == 0) continue;

                // COUNT(DISTINCT adsh) via popcount
                int64_t pc = 0;
                const uint64_t* gsw = gbits + (size_t)slot * adsh_words;
                for (int w = 0; w < adsh_words; w++)
                    pc += (int64_t)__builtin_popcountll(gsw[w]);

                Group g;
                g.stmt_code   = sc;
                g.rfile_code  = rc;
                g.cnt         = g_count[slot];
                g.num_filings = pc;
                g.avg_line    = (double)g_sum_line[slot] / (double)g_count[slot];
                groups.push_back(g);
            }
        }

        // Sort by cnt DESC
        std::sort(groups.begin(), groups.end(), [](const Group& a, const Group& b) {
            return a.cnt > b.cnt;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q1.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror(outpath.c_str()); exit(1); }

        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& g : groups) {
            fprintf(f, "%s,%s,%lld,%lld,%.6f\n",
                    stmt_dict[g.stmt_code].c_str(),
                    rfile_dict[g.rfile_code].c_str(),
                    (long long)g.cnt,
                    (long long)g.num_filings,
                    g.avg_line);
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
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
