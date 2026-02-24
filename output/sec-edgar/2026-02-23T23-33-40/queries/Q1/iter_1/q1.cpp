#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"

// ─── Open-addressing hash map for aggregation ───────────────────────────────
// Key: uint32_t composite = (uint16_t)stmt_code << 16 | (uint16_t)rfile_code
static constexpr int AGG_CAP = 32; // next_pow2(12*2), bounded linear probe

struct AggSlot {
    uint32_t key;          // composite key
    bool     occupied;
    int64_t  cnt;
    int64_t  line_sum;
    int64_t  line_count;
    uint16_t stmt_code;
    uint16_t rfile_code;
    // adsh distinct count handled via per-thread bitset (see tl_adsh)
};

static inline uint32_t make_key(int16_t sc, int16_t rc) {
    return ((uint32_t)(uint16_t)sc << 16) | (uint16_t)rc;
}

static inline int agg_find_or_insert(AggSlot* slots, uint32_t key, uint16_t sc, uint16_t rc) {
    uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & (AGG_CAP - 1);
    for (int probe = 0; probe < AGG_CAP; ++probe) {
        int idx = (h + probe) & (AGG_CAP - 1);
        if (!slots[idx].occupied) {
            slots[idx].occupied   = true;
            slots[idx].key        = key;
            slots[idx].stmt_code  = sc;
            slots[idx].rfile_code = rc;
            slots[idx].cnt        = 0;
            slots[idx].line_sum   = 0;
            slots[idx].line_count = 0;
            return idx;
        }
        if (slots[idx].key == key) return idx;
    }
    return -1; // should never happen with cap=32 and ~12 groups
}

// ─── Utility: load dict file ─────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st; fstat(fd, &st);
    out_size = st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ─── Main query ───────────────────────────────────────────────────────────────
void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Data loading ──────────────────────────────────────────────────────────
    std::vector<std::string> stmt_dict, rfile_dict;
    const int16_t* stmt_col = nullptr;
    const int16_t* rfile_col = nullptr;
    const int32_t* adsh_col = nullptr;
    const int32_t* line_col = nullptr;
    size_t n_rows = 0;

    {
        GENDB_PHASE("data_loading");

        // Load dictionaries
        stmt_dict  = load_dict(gendb_dir + "/pre/stmt_dict.txt");
        rfile_dict = load_dict(gendb_dir + "/pre/rfile_dict.txt");

        // mmap columns
        size_t sz_stmt, sz_rfile, sz_adsh, sz_line;
        stmt_col  = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/stmt.bin",  sz_stmt));
        rfile_col = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/rfile.bin", sz_rfile));
        adsh_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/adsh.bin",  sz_adsh));
        line_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/line.bin",  sz_line));

        n_rows = sz_stmt / sizeof(int16_t);
    }

    // Count adsh dict size for bitset sizing (lines = number of distinct adsh codes)
    size_t adsh_dict_size = 0;
    {
        std::ifstream f(gendb_dir + "/pre/adsh_dict.txt");
        std::string tmp;
        while (std::getline(f, tmp)) ++adsh_dict_size;
    }
    // words_per_group: number of uint64_t words needed to cover all adsh codes
    size_t words_per_group = (adsh_dict_size + 63) / 64;

    // Find null stmt code at runtime (C2)
    int16_t null_stmt_code = -1;
    for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c) {
        if (stmt_dict[c].empty()) { null_stmt_code = c; break; }
    }

    // ── Main scan: parallel fused scan+filter+aggregate ───────────────────────
    int nthreads = omp_get_max_threads();
    // Thread-local aggregation arrays
    std::vector<std::vector<AggSlot>> tl_slots(nthreads, std::vector<AggSlot>(AGG_CAP));
    // Initialize all slots to unoccupied
    for (int t = 0; t < nthreads; ++t)
        for (int s = 0; s < AGG_CAP; ++s)
            tl_slots[t][s].occupied = false;

    // Per-thread per-group bitset for COUNT(DISTINCT adsh).
    // Layout: tl_adsh[tid][slot_idx * words_per_group + word] — all zeroed.
    // This replaces std::unordered_set: a bitset OR is orders of magnitude faster.
    std::vector<std::vector<uint64_t>> tl_adsh(
        nthreads, std::vector<uint64_t>(AGG_CAP * words_per_group, 0ULL));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggSlot* slots = tl_slots[tid].data();
            uint64_t* adsh_bits = tl_adsh[tid].data(); // bitset for this thread

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_rows; ++i) {
                int16_t sc = stmt_col[i];
                if (sc == null_stmt_code) continue; // filter: stmt IS NOT NULL

                int16_t rc   = rfile_col[i];
                int32_t adsh = adsh_col[i];
                int32_t lv   = line_col[i];

                uint32_t key = make_key(sc, rc);
                int idx = agg_find_or_insert(slots, key, (uint16_t)sc, (uint16_t)rc);
                if (idx < 0) continue; // should not happen

                slots[idx].cnt++;
                slots[idx].line_sum   += lv;
                slots[idx].line_count++;
                // Bitset OR: mark adsh code as seen for this group slot
                adsh_bits[(size_t)idx * words_per_group + (uint32_t)adsh / 64]
                    |= (1ULL << ((uint32_t)adsh & 63));
            }
        }
    }

    // ── Aggregation merge ─────────────────────────────────────────────────────
    // Global result array (small: ~12 groups)
    std::vector<AggSlot> global_slots(AGG_CAP);
    for (auto& s : global_slots) s.occupied = false;

    // Global adsh bitset: [slot_idx][word] — zeroed, merged via bitwise OR
    std::vector<uint64_t> global_adsh(AGG_CAP * words_per_group, 0ULL);

    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < nthreads; ++t) {
            AggSlot* src = tl_slots[t].data();
            const uint64_t* src_bits = tl_adsh[t].data();
            for (int s = 0; s < AGG_CAP; ++s) {
                if (!src[s].occupied) continue;
                uint32_t key = src[s].key;
                // find or insert in global
                uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & (AGG_CAP - 1);
                for (int probe = 0; probe < AGG_CAP; ++probe) {
                    int idx = (h + probe) & (AGG_CAP - 1);
                    if (!global_slots[idx].occupied) {
                        global_slots[idx].occupied   = true;
                        global_slots[idx].key        = key;
                        global_slots[idx].stmt_code  = src[s].stmt_code;
                        global_slots[idx].rfile_code = src[s].rfile_code;
                        global_slots[idx].cnt        = src[s].cnt;
                        global_slots[idx].line_sum   = src[s].line_sum;
                        global_slots[idx].line_count = src[s].line_count;
                        // Copy bitset words for this slot
                        const uint64_t* sb = src_bits + (size_t)s * words_per_group;
                        uint64_t*       gb = global_adsh.data() + (size_t)idx * words_per_group;
                        for (size_t w = 0; w < words_per_group; ++w) gb[w] = sb[w];
                        break;
                    }
                    if (global_slots[idx].key == key) {
                        global_slots[idx].cnt        += src[s].cnt;
                        global_slots[idx].line_sum   += src[s].line_sum;
                        global_slots[idx].line_count += src[s].line_count;
                        // OR bitset words for COUNT(DISTINCT adsh) — no dynamic alloc
                        const uint64_t* sb = src_bits + (size_t)s * words_per_group;
                        uint64_t*       gb = global_adsh.data() + (size_t)idx * words_per_group;
                        for (size_t w = 0; w < words_per_group; ++w) gb[w] |= sb[w];
                        break;
                    }
                }
            }
        }
    }

    // ── Collect non-empty groups and sort ─────────────────────────────────────
    struct Result {
        std::string stmt_str;
        std::string rfile_str;
        int64_t cnt;
        size_t  num_filings;
        double  avg_line_num;
    };
    std::vector<Result> results;
    results.reserve(16);

    for (int s = 0; s < AGG_CAP; ++s) {
        if (!global_slots[s].occupied) continue;
        Result r;
        r.stmt_str    = stmt_dict[global_slots[s].stmt_code];
        r.rfile_str   = rfile_dict[global_slots[s].rfile_code];
        r.cnt         = global_slots[s].cnt;
        // COUNT(DISTINCT adsh): popcount the global bitset for this slot
        size_t distinct_adsh = 0;
        const uint64_t* gb = global_adsh.data() + (size_t)s * words_per_group;
        for (size_t w = 0; w < words_per_group; ++w)
            distinct_adsh += (size_t)__builtin_popcountll(gb[w]);
        r.num_filings = distinct_adsh;
        r.avg_line_num = (global_slots[s].line_count > 0)
                         ? (double)global_slots[s].line_sum / global_slots[s].line_count
                         : 0.0;
        results.push_back(std::move(r));
    }

    // Sort by cnt DESC
    std::sort(results.begin(), results.end(),
              [](const Result& a, const Result& b) { return a.cnt > b.cnt; });

    // ── Output ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return; }

        fprintf(fp, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : results) {
            fprintf(fp, "\"%s\",\"%s\",%ld,%zu,%.2f\n",
                    r.stmt_str.c_str(),
                    r.rfile_str.c_str(),
                    r.cnt,
                    r.num_filings,
                    r.avg_line_num);
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
    std::string gendb_dir   = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
