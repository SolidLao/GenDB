// Q1: SELECT stmt, rfile, COUNT(*), COUNT(DISTINCT adsh), AVG(line)
//     FROM pre WHERE stmt IS NOT NULL GROUP BY stmt, rfile ORDER BY cnt DESC
//
// Strategy:
//   - Binary dict loading for stmt (n=8) and rfile (n=2)
//   - Zone-map pruning: skip blocks where stmt_max == -1
//   - Morsel-driven parallel scan over 97 blocks × 100,000 rows
//   - Thread-local direct_array[16] aggregation (8 stmt × 2 rfile)
//   - Bitset-based COUNT(DISTINCT adsh): 10767 bytes/group (86135 adsh codes)
//   - Merge via word-level bitset OR; final popcount for distinct count

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

using namespace gendb;

static constexpr int64_t N          = 9'600'799;
static constexpr int64_t BLOCK_SIZE = 100'000;

// Zone map block layout (packed, 10 bytes per block)
struct __attribute__((packed)) ZoneBlock {
    int8_t  stmt_min;
    int8_t  stmt_max;
    int32_t adsh_min;
    int32_t adsh_max;
};
static_assert(sizeof(ZoneBlock) == 10, "ZoneBlock must be 10 bytes");

// Helper: mmap a file read-only
static const void* mmap_file(const std::string& path, size_t* out_size = nullptr) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    if (out_size) *out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr != MAP_FAILED && st.st_size > 0) {
        madvise(ptr, st.st_size, MADV_SEQUENTIAL);
        posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    }
    close(fd);
    return (ptr == MAP_FAILED) ? nullptr : ptr;
}

// Helper: load binary dict: [n:uint32][len:uint16, bytes...]*n
static std::vector<std::string> load_bin_dict(const std::string& path) {
    std::vector<std::string> result;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return result; }
    uint32_t n = 0;
    if (read(fd, &n, sizeof(n)) != sizeof(n)) { close(fd); return result; }
    result.resize(n);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = 0;
        read(fd, &len, sizeof(len));
        result[i].resize(len);
        if (len > 0) read(fd, &result[i][0], len);
    }
    close(fd);
    return result;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // Runtime-determined parameters
    int n_stmt       = 0;
    int n_rfile      = 0;
    int n_groups     = 0;
    int adsh_count   = 0;
    int bitset_bytes = 0;

    std::vector<std::string> stmt_dict, rfile_dict;

    const int8_t*  stmt_col  = nullptr;
    const int8_t*  rfile_col = nullptr;
    const int32_t* adsh_col  = nullptr;
    const int32_t* line_col  = nullptr;

    size_t zm_size = 0;
    const uint8_t* zm_data = nullptr;

    // ---- Data Loading ----
    {
        GENDB_PHASE("data_loading");

        // Load dicts (binary format: [n:uint32][len:uint16, bytes...]*n)
        stmt_dict  = load_bin_dict(gendb_dir + "/shared/stmt.dict");
        rfile_dict = load_bin_dict(gendb_dir + "/shared/rfile.dict");
        n_stmt     = (int)stmt_dict.size();   // 8
        n_rfile    = (int)rfile_dict.size();  // 2
        n_groups   = n_stmt * n_rfile;        // 16

        // Determine adsh code range from sub/adsh.bin (N×char[20])
        struct stat st_adsh;
        stat((gendb_dir + "/sub/adsh.bin").c_str(), &st_adsh);
        adsh_count   = (int)(st_adsh.st_size / 20);  // 86135
        bitset_bytes = (adsh_count + 7) / 8;          // 10767

        // mmap data columns
        stmt_col  = (const int8_t*)mmap_file(gendb_dir + "/pre/stmt.bin");
        rfile_col = (const int8_t*)mmap_file(gendb_dir + "/pre/rfile.bin");
        adsh_col  = (const int32_t*)mmap_file(gendb_dir + "/pre/adsh.bin");
        line_col  = (const int32_t*)mmap_file(gendb_dir + "/pre/line.bin");

        // mmap zone maps
        zm_data = (const uint8_t*)mmap_file(
            gendb_dir + "/indexes/pre_zonemaps.bin", &zm_size);
    }

    // Parse zone map header
    int32_t n_blocks = *(const int32_t*)zm_data;
    const ZoneBlock* zone_blocks = (const ZoneBlock*)(zm_data + 4);

    int nthreads = omp_get_max_threads();

    // Thread-local aggregation structs
    // Each thread holds: cnt[n_groups], sum_line[n_groups], count_line[n_groups],
    //                    adsh_bits[n_groups * bitset_bytes]
    struct ThreadAgg {
        std::vector<int64_t> cnt;
        std::vector<int64_t> sum_line;
        std::vector<int64_t> count_line;
        std::vector<uint8_t> adsh_bits;  // n_groups × bitset_bytes, group-major
    };
    std::vector<ThreadAgg> tagg(nthreads);

    // Initialize thread-local storage in parallel (avoids page-fault stall)
    #pragma omp parallel for schedule(static)
    for (int t = 0; t < nthreads; t++) {
        tagg[t].cnt.assign(n_groups, 0);
        tagg[t].sum_line.assign(n_groups, 0);
        tagg[t].count_line.assign(n_groups, 0);
        tagg[t].adsh_bits.assign((size_t)n_groups * bitset_bytes, 0);
    }

    // ---- Main Scan (morsel-driven parallel scan over blocks) ----
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            ThreadAgg& agg = tagg[tid];
            int64_t* __restrict__ lcnt   = agg.cnt.data();
            int64_t* __restrict__ lsum   = agg.sum_line.data();
            int64_t* __restrict__ lcnt2  = agg.count_line.data();
            uint8_t* __restrict__ lbits  = agg.adsh_bits.data();

            #pragma omp for schedule(dynamic, 1) nowait
            for (int blk = 0; blk < n_blocks; blk++) {
                // Zone map pruning: skip blocks where every row has NULL stmt
                if (zone_blocks[blk].stmt_max == (int8_t)-1) continue;

                int64_t row_start = (int64_t)blk * BLOCK_SIZE;
                int64_t row_end   = std::min(row_start + BLOCK_SIZE, N);

                for (int64_t i = row_start; i < row_end; i++) {
                    int8_t sc = stmt_col[i];
                    if (sc == (int8_t)-1) continue;  // WHERE stmt IS NOT NULL

                    int8_t rc = rfile_col[i];
                    // Group key: sc in [0, n_stmt-1], rc in [0, n_rfile-1]
                    int gk = (int)sc * n_rfile + (int)rc;

                    lcnt[gk]++;
                    lsum[gk]  += (int64_t)line_col[i];
                    lcnt2[gk]++;

                    // Bitset for COUNT(DISTINCT adsh)
                    int32_t ac = adsh_col[i];  // code in [0, adsh_count-1]
                    uint8_t* bits = lbits + (size_t)gk * bitset_bytes;
                    bits[ac >> 3] |= (uint8_t)(1u << (ac & 7));
                }
            }
        }
    }

    // ---- Merge thread-local aggregations ----
    std::vector<int64_t> gcnt(n_groups, 0);
    std::vector<int64_t> gsum(n_groups, 0);
    std::vector<int64_t> gcnt2(n_groups, 0);
    std::vector<uint8_t> gbits((size_t)n_groups * bitset_bytes, 0);

    {
        GENDB_PHASE("aggregation_merge");

        const int nwords = bitset_bytes / 8;
        const int tail   = bitset_bytes - nwords * 8;

        for (int t = 0; t < nthreads; t++) {
            const ThreadAgg& src = tagg[t];
            for (int g = 0; g < n_groups; g++) {
                gcnt[g]  += src.cnt[g];
                gsum[g]  += src.sum_line[g];
                gcnt2[g] += src.count_line[g];

                // Word-level bitset OR
                const uint64_t* sw = (const uint64_t*)(src.adsh_bits.data() + (size_t)g * bitset_bytes);
                uint64_t*       dw = (uint64_t*)(gbits.data() + (size_t)g * bitset_bytes);
                for (int w = 0; w < nwords; w++) dw[w] |= sw[w];

                // Tail bytes
                const uint8_t* sb = (const uint8_t*)sw + nwords * 8;
                uint8_t*       db = (uint8_t*)dw + nwords * 8;
                for (int b = 0; b < tail; b++) db[b] |= sb[b];
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
        rows.reserve(n_groups);

        const int nwords = bitset_bytes / 8;
        const int tail   = bitset_bytes - nwords * 8;

        for (int sc = 0; sc < n_stmt; sc++) {
            for (int rc = 0; rc < n_rfile; rc++) {
                int g = sc * n_rfile + rc;
                if (gcnt[g] == 0) continue;

                // COUNT(DISTINCT adsh) via popcount over merged bitset
                int64_t num_filings = 0;
                const uint64_t* words = (const uint64_t*)(gbits.data() + (size_t)g * bitset_bytes);
                for (int w = 0; w < nwords; w++)
                    num_filings += __builtin_popcountll(words[w]);
                const uint8_t* tail_bytes = (const uint8_t*)words + nwords * 8;
                for (int b = 0; b < tail; b++)
                    num_filings += __builtin_popcount(tail_bytes[b]);

                double avg_line = (double)gsum[g] / (double)gcnt2[g];
                rows.push_back({stmt_dict[sc], rfile_dict[rc],
                                gcnt[g], num_filings, avg_line});
            }
        }

        // ORDER BY cnt DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.cnt > b.cnt;
        });

        // Write CSV
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
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    run_q1(argv[1], argv[2]);
    return 0;
}
#endif
