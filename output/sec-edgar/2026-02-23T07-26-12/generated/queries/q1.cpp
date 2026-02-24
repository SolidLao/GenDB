/*
 * Q1: SELECT stmt, rfile, COUNT(*), COUNT(DISTINCT adsh), AVG(line)
 *     FROM pre WHERE stmt IS NOT NULL
 *     GROUP BY stmt, rfile ORDER BY cnt DESC
 */
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

namespace {

// ---- Zone map block layout ----
struct ZoneBlock16 {
    int16_t min_val;
    int16_t max_val;
    uint32_t row_count;
};

// ---- Dict loader ----
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty()) dict.push_back(line);
    }
    return dict;
}

// ---- mmap helper (returns typed pointer, sets n = element count) ----
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    close(fd);
    n = st.st_size / sizeof(T);
    return p;
}

// ---- mmap helper (raw bytes) ----
static const uint8_t* mmap_bytes(const std::string& path, size_t& bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    bytes = st.st_size;
    auto* p = reinterpret_cast<const uint8_t*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    close(fd);
    return p;
}

} // end anonymous namespace

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- data_loading ----
    std::vector<std::string> stmt_dict, rfile_dict, adsh_dict;
    const int16_t* stmt_col  = nullptr;
    const int16_t* rfile_col = nullptr;
    const int32_t* adsh_col  = nullptr;
    const int32_t* line_col  = nullptr;
    const uint8_t* zm_raw    = nullptr;
    size_t N = 0, zm_bytes = 0;

    {
        GENDB_PHASE("data_loading");
        std::string pre = gendb_dir + "/pre";

        stmt_dict  = load_dict(pre + "/stmt_dict.txt");
        rfile_dict = load_dict(pre + "/rfile_dict.txt");
        adsh_dict  = load_dict(pre + "/adsh_dict.txt");

        size_t n2, n3, n4;
        stmt_col  = mmap_col<int16_t>(pre + "/stmt.bin",  N);
        rfile_col = mmap_col<int16_t>(pre + "/rfile.bin", n2);
        adsh_col  = mmap_col<int32_t>(pre + "/adsh.bin",  n3);
        line_col  = mmap_col<int32_t>(pre + "/line.bin",  n4);

        zm_raw = mmap_bytes(gendb_dir + "/indexes/pre_stmt_zone_map.bin", zm_bytes);
    }

    const int ADSH_CODES = (int)adsh_dict.size();
    const int ADSH_WORDS = (ADSH_CODES + 63) / 64;

    const int S = (int)stmt_dict.size();   // 8
    const int R = (int)rfile_dict.size();  // 2
    const int slots = S * R;               // 16

    // ---- Parse zone map → build valid block ranges ----
    struct BlockRange { int64_t start, end; };
    std::vector<BlockRange> valid_ranges;
    {
        uint32_t num_blocks;
        memcpy(&num_blocks, zm_raw, sizeof(uint32_t));
        const ZoneBlock16* blocks = reinterpret_cast<const ZoneBlock16*>(zm_raw + 4);
        int64_t row_off = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            int64_t bstart = row_off;
            int64_t bend   = row_off + blocks[b].row_count;
            // Skip block only if ALL rows are NULL (max_val == -1)
            if (blocks[b].max_val != (int16_t)-1) {
                valid_ranges.push_back({bstart, bend});
            }
            row_off = bend;
        }
    }

    const int nranges  = (int)valid_ranges.size();
    const int nthreads = omp_get_max_threads();

    // ---- Thread-local aggregation arrays ----
    // cnt[slots], sum_line[slots]: 16 int64_t each = 128B per thread
    // adsh_bits[slots * ADSH_WORDS]: 16 * 1347 * 8B = ~172KB per thread
    std::vector<std::vector<int64_t>>  tl_cnt(nthreads, std::vector<int64_t>(slots, 0));
    std::vector<std::vector<int64_t>>  tl_sum(nthreads, std::vector<int64_t>(slots, 0));
    std::vector<std::vector<uint64_t>> tl_bits(nthreads,
        std::vector<uint64_t>((size_t)slots * ADSH_WORDS, 0ULL));

    // ---- main_scan: fused scan + filter + aggregate ----
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (int ri = 0; ri < nranges; ri++) {
            const int tid = omp_get_thread_num();
            auto& my_cnt  = tl_cnt[tid];
            auto& my_sum  = tl_sum[tid];
            auto& my_bits = tl_bits[tid];

            const int64_t bstart = valid_ranges[ri].start;
            const int64_t bend   = valid_ranges[ri].end;

            for (int64_t i = bstart; i < bend; i++) {
                const int16_t sc = stmt_col[i];
                if (sc == (int16_t)-1) continue;       // stmt IS NOT NULL
                const int16_t rc = rfile_col[i];
                const int key = (int)sc * R + (int)rc;

                my_cnt[key]++;
                my_sum[key] += (int64_t)line_col[i];

                // Set bit in adsh bitset for this group
                const int32_t ac = adsh_col[i];        // 0..86134
                my_bits[(size_t)key * ADSH_WORDS + (ac >> 6)] |= (1ULL << (ac & 63));
            }
        }
    }

    // ---- aggregation_merge: fold thread-locals into thread 0 ----
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 1; t < nthreads; t++) {
            for (int k = 0; k < slots; k++) {
                tl_cnt[0][k] += tl_cnt[t][k];
                tl_sum[0][k] += tl_sum[t][k];
                const size_t base = (size_t)k * ADSH_WORDS;
                for (int w = 0; w < ADSH_WORDS; w++) {
                    tl_bits[0][base + w] |= tl_bits[t][base + w];
                }
            }
        }
    }

    // ---- output: decode, popcount, sort, write CSV ----
    {
        GENDB_PHASE("output");

        struct Row {
            const char* stmt_str;
            const char* rfile_str;
            int64_t     cnt;
            int64_t     num_filings;
            double      avg_line;
        };

        std::vector<Row> rows;
        rows.reserve(slots);

        for (int sc = 0; sc < S; sc++) {
            for (int rc = 0; rc < R; rc++) {
                const int key = sc * R + rc;
                const int64_t c = tl_cnt[0][key];
                if (c == 0) continue;

                // COUNT(DISTINCT adsh) via popcount
                int64_t ndist = 0;
                const size_t base = (size_t)key * ADSH_WORDS;
                for (int w = 0; w < ADSH_WORDS; w++) {
                    ndist += (int64_t)__builtin_popcountll(tl_bits[0][base + w]);
                }

                const double avg = (double)tl_sum[0][key] / (double)c;
                rows.push_back({stmt_dict[sc].c_str(), rfile_dict[rc].c_str(),
                                c, ndist, avg});
            }
        }

        // Sort by cnt DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            return a.cnt > b.cnt;
        });

        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q1.csv");
        out << "stmt,rfile,cnt,num_filings,avg_line_num\n";
        out << std::fixed;
        out.precision(2);
        for (const auto& r : rows) {
            out << r.stmt_str << ',' << r.rfile_str << ','
                << r.cnt << ',' << r.num_filings << ','
                << r.avg_line << '\n';
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
