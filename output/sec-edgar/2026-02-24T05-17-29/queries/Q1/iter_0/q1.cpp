// Q1: SELECT stmt, rfile, COUNT(*), COUNT(DISTINCT adsh), AVG(line)
//     FROM pre WHERE stmt IS NOT NULL GROUP BY stmt, rfile ORDER BY cnt DESC
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <iostream>
#include "timing_utils.h"

// -- Constants --
// stmt_dict has 8 entries, rfile_dict has 2 entries → max 16 groups
static constexpr int MAX_STMT   = 8;
static constexpr int MAX_RFILE  = 2;
static constexpr int NUM_GROUPS = MAX_STMT * MAX_RFILE;   // 16

static constexpr int ADSH_DISTINCT = 86135;
static constexpr int BITSET_BYTES  = (ADSH_DISTINCT + 7) / 8;  // 10767

// -- Aggregation slot --
struct AggSlot {
    int64_t cnt        = 0;
    int64_t sum_line   = 0;
    int64_t count_line = 0;
    uint8_t adsh_bits[BITSET_BYTES];
};

// Thread-local array of 16 slots
struct ThreadAgg {
    AggSlot slots[NUM_GROUPS];
    void init() {
        for (int k = 0; k < NUM_GROUPS; k++) {
            slots[k].cnt        = 0;
            slots[k].sum_line   = 0;
            slots[k].count_line = 0;
            std::fill(slots[k].adsh_bits, slots[k].adsh_bits + BITSET_BYTES, (uint8_t)0);
        }
    }
};

// -- Helper: mmap a file, return typed pointer --
static const void* mmap_file(const std::string& path, size_t* out_size = nullptr) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    if (out_size) *out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(ptr, st.st_size, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return ptr;
}

// -- Load a simple text dictionary (one entry per line) --
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        d.push_back(line);
    }
    return d;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    static constexpr int64_t N = 9600799;

    const int16_t* stmt_col  = nullptr;
    const int16_t* rfile_col = nullptr;
    const int32_t* adsh_col  = nullptr;
    const int32_t* line_col  = nullptr;

    std::vector<std::string> stmt_dict;
    std::vector<std::string> rfile_dict;

    // ---- Data Loading ----
    {
        GENDB_PHASE("data_loading");

        stmt_dict  = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        rfile_dict = load_dict(gendb_dir + "/pre/rfile_dict.txt");

        stmt_col  = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/stmt.bin"));
        rfile_col = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/rfile.bin"));
        adsh_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/adsh.bin"));
        line_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/line.bin"));
    }

    // ---- Main Scan (parallel, thread-local aggregation) ----
    int nthreads = omp_get_max_threads();
    std::vector<ThreadAgg> tagg(nthreads);

    {
        GENDB_PHASE("main_scan");

        // Initialize thread-local slots in parallel (avoids page-fault stall)
        #pragma omp parallel for schedule(static)
        for (int t = 0; t < nthreads; t++) {
            tagg[t].init();
        }

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggSlot* local = tagg[tid].slots;

            #pragma omp for schedule(static) nowait
            for (int64_t i = 0; i < N; i++) {
                int16_t sc = stmt_col[i];
                if (sc == (int16_t)-1) continue;   // WHERE stmt IS NOT NULL

                int16_t rc = rfile_col[i];
                int key    = (int)sc * 2 + (int)rc; // [0..15]

                AggSlot& s = local[key];
                s.cnt++;
                s.sum_line += (int64_t)line_col[i];
                s.count_line++;
                int32_t ac = adsh_col[i];            // adsh_code in [0, 86134]
                s.adsh_bits[ac >> 3] |= (uint8_t)(1u << (ac & 7));
            }
        }
    }

    // ---- Merge thread-local aggregations ----
    AggSlot global_slots[NUM_GROUPS];
    {
        GENDB_PHASE("aggregation_merge");

        for (int k = 0; k < NUM_GROUPS; k++) {
            global_slots[k].cnt        = 0;
            global_slots[k].sum_line   = 0;
            global_slots[k].count_line = 0;
            std::fill(global_slots[k].adsh_bits,
                      global_slots[k].adsh_bits + BITSET_BYTES, (uint8_t)0);
        }

        for (int t = 0; t < nthreads; t++) {
            for (int k = 0; k < NUM_GROUPS; k++) {
                const AggSlot& s = tagg[t].slots[k];
                global_slots[k].cnt        += s.cnt;
                global_slots[k].sum_line   += s.sum_line;
                global_slots[k].count_line += s.count_line;

                // Word-level bitset OR (10767 bytes = 1345 uint64_t + 7 bytes)
                const uint64_t* src = reinterpret_cast<const uint64_t*>(s.adsh_bits);
                uint64_t*       dst = reinterpret_cast<uint64_t*>(global_slots[k].adsh_bits);
                static constexpr int NWORDS = BITSET_BYTES / 8;        // 1345
                for (int w = 0; w < NWORDS; w++) dst[w] |= src[w];
                // Remaining tail bytes
                for (int b = NWORDS*8; b < BITSET_BYTES; b++)
                    global_slots[k].adsh_bits[b] |= s.adsh_bits[b];
            }
        }
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string stmt_str;
            std::string rfile_str;
            int64_t cnt;
            int64_t num_filings;
            double  avg_line;
        };

        std::vector<ResultRow> rows;
        rows.reserve(NUM_GROUPS);

        int n_stmt  = (int)stmt_dict.size();
        int n_rfile = (int)rfile_dict.size();

        for (int sc = 0; sc < n_stmt; sc++) {
            for (int rc = 0; rc < n_rfile; rc++) {
                int key = sc * 2 + rc;
                const AggSlot& g = global_slots[key];
                if (g.cnt == 0) continue;

                // COUNT(DISTINCT adsh) via popcount over bitset
                int64_t num_filings = 0;
                const uint64_t* words = reinterpret_cast<const uint64_t*>(g.adsh_bits);
                static constexpr int NWORDS = BITSET_BYTES / 8;
                for (int w = 0; w < NWORDS; w++)
                    num_filings += __builtin_popcountll(words[w]);
                for (int b = NWORDS*8; b < BITSET_BYTES; b++)
                    num_filings += __builtin_popcount(g.adsh_bits[b]);

                double avg_line = (double)g.sum_line / (double)g.count_line;

                rows.push_back({stmt_dict[sc], rfile_dict[rc],
                                g.cnt, num_filings, avg_line});
            }
        }

        // ORDER BY cnt DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.cnt > b.cnt;
        });

        // Write CSV — C31: quote all string columns
        std::string outpath = results_dir + "/Q1.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { perror(outpath.c_str()); return; }
        fprintf(fp, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : rows) {
            fprintf(fp, "\"%s\",\"%s\",%ld,%ld,%.2f\n",
                    r.stmt_str.c_str(), r.rfile_str.c_str(),
                    (long)r.cnt, (long)r.num_filings, r.avg_line);
        }
        fclose(fp);
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
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
