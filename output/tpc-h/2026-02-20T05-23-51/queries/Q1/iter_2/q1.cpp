#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <iostream>
#include "timing_utils.h"

// Group key encoding:
//   rflag: 'A'->0, 'N'->1, 'R'->2
//   lstatus: 'F'->0, 'O'->1
//   idx = rflag_idx*2 + lstatus_idx  (0..5)
// Sorted output order (A<N<R, F<O): 0,1,2,3,4,5

static inline int rflag_idx(int8_t c) {
    if (c == 'A') return 0;
    if (c == 'N') return 1;
    return 2; // 'R'
}
static inline int lstatus_idx(int8_t c) {
    return (c == 'O') ? 1 : 0;
}
static inline int group_idx(int8_t rf, int8_t ls) {
    return rflag_idx(rf) * 2 + lstatus_idx(ls);
}

// Plain double accumulators — all values are positive, %.2f output,
// no catastrophic cancellation; Kahan overhead prevents vectorization.
// Padded to 64 bytes so each Accum occupies exactly one cache line.
struct alignas(64) Accum {
    double  sum_qty        = 0.0;
    double  sum_price      = 0.0;
    double  sum_disc_price = 0.0;
    double  sum_charge     = 0.0;
    double  sum_disc       = 0.0;
    int64_t count          = 0;
    // 5*8 + 8 = 48 bytes used; pad 16 bytes to reach 64
    char    _pad[16];
};

struct ZMEntry {
    int32_t  min;
    int32_t  max;
    uint32_t block_size;
};

// mmap helper — no MAP_POPULATE; let threads fault pages in parallel during scan
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    auto* p = reinterpret_cast<const T*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (p != MAP_FAILED)
        madvise(const_cast<T*>(p), st.st_size, MADV_SEQUENTIAL);
    n_rows = st.st_size / sizeof(T);
    return p;
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const int32_t CUTOFF = 10471; // DATE '1998-09-02'
    const int NUM_GROUPS = 6;

    // Decode group idx -> (rflag char, lstatus char)
    // idx = rf_idx*2 + ls_idx
    static const char RFLAG_CHARS[3]   = {'A','N','R'};
    static const char LSTATUS_CHARS[2] = {'F','O'};

    // ----------------------------------------------------------------
    // Load zone map
    // ----------------------------------------------------------------
    uint32_t num_blocks = 0;
    const ZMEntry* zm = nullptr;
    {
        GENDB_PHASE("zone_map_load");
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat st; fstat(fd, &st);
            auto* raw = reinterpret_cast<const uint8_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0));
            close(fd);
            num_blocks = *reinterpret_cast<const uint32_t*>(raw);
            zm = reinterpret_cast<const ZMEntry*>(raw + sizeof(uint32_t));
        }
    }

    // ----------------------------------------------------------------
    // Collect qualifying block ranges
    // ----------------------------------------------------------------
    std::vector<uint64_t> block_starts; // row offsets of qualifying blocks
    std::vector<uint64_t> block_ends;
    {
        GENDB_PHASE("zone_map_prune");
        uint64_t row_off = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            uint32_t bsz = zm ? zm[b].block_size : 100000u;
            // Skip block if ALL rows have shipdate > CUTOFF
            bool skip = zm && (zm[b].min > CUTOFF);
            if (!skip) {
                block_starts.push_back(row_off);
                block_ends.push_back(row_off + bsz);
            }
            row_off += bsz;
        }
    }

    // ----------------------------------------------------------------
    // mmap columns
    // ----------------------------------------------------------------
    size_t n_rows = 0;
    const int32_t* col_shipdate;
    const int8_t*  col_rflag;
    const int8_t*  col_lstatus;
    const double*  col_qty;
    const double*  col_price;
    const double*  col_disc;
    const double*  col_tax;

    {
        GENDB_PHASE("mmap_columns");
        size_t tmp = 0;
        col_shipdate = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",    n_rows);
        col_rflag    = mmap_col<int8_t> (gendb_dir + "/lineitem/l_returnflag.bin",  tmp);
        col_lstatus  = mmap_col<int8_t> (gendb_dir + "/lineitem/l_linestatus.bin",  tmp);
        col_qty      = mmap_col<double> (gendb_dir + "/lineitem/l_quantity.bin",     tmp);
        col_price    = mmap_col<double> (gendb_dir + "/lineitem/l_extendedprice.bin",tmp);
        col_disc     = mmap_col<double> (gendb_dir + "/lineitem/l_discount.bin",     tmp);
        col_tax      = mmap_col<double> (gendb_dir + "/lineitem/l_tax.bin",          tmp);
    }

    // ----------------------------------------------------------------
    // Parallel scan + aggregate
    // ----------------------------------------------------------------
    const int MAX_THREADS = 64;
    // alignas(64): thread 0 starts on a cache line boundary.
    // Each thread uses 6 * 64 = 384 bytes = exactly 6 cache lines → no false sharing.
    alignas(64) Accum thread_acc[MAX_THREADS][NUM_GROUPS];
    // zero-initialize
    for (int t = 0; t < MAX_THREADS; t++)
        for (int g = 0; g < NUM_GROUPS; g++)
            thread_acc[t][g] = Accum{};

    size_t num_qualifying = block_starts.size();

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t bi = 0; bi < num_qualifying; bi++) {
            int tid = omp_get_thread_num();
            Accum* acc = thread_acc[tid];

            uint64_t row_start = block_starts[bi];
            uint64_t row_end   = block_ends[bi];
            if (row_end > n_rows) row_end = n_rows;

            for (uint64_t row = row_start; row < row_end; row++) {
                if (col_shipdate[row] > CUTOFF) continue;

                int8_t rf = col_rflag[row];
                int8_t ls = col_lstatus[row];
                int    gi = group_idx(rf, ls);

                double qty   = col_qty[row];
                double price = col_price[row];
                double disc  = col_disc[row];
                double tax   = col_tax[row];

                double disc_price = price * (1.0 - disc);
                double charge     = disc_price * (1.0 + tax);

                acc[gi].sum_qty        += qty;
                acc[gi].sum_price      += price;
                acc[gi].sum_disc_price += disc_price;
                acc[gi].sum_charge     += charge;
                acc[gi].sum_disc       += disc;
                acc[gi].count          += 1;
            }
        }
    }

    // ----------------------------------------------------------------
    // Merge thread-local accumulators
    // ----------------------------------------------------------------
    alignas(64) Accum global_acc[NUM_GROUPS] = {};
    {
        GENDB_PHASE("merge");
        for (int t = 0; t < MAX_THREADS; t++) {
            for (int g = 0; g < NUM_GROUPS; g++) {
                global_acc[g].sum_qty        += thread_acc[t][g].sum_qty;
                global_acc[g].sum_price      += thread_acc[t][g].sum_price;
                global_acc[g].sum_disc_price += thread_acc[t][g].sum_disc_price;
                global_acc[g].sum_charge     += thread_acc[t][g].sum_charge;
                global_acc[g].sum_disc       += thread_acc[t][g].sum_disc;
                global_acc[g].count          += thread_acc[t][g].count;
            }
        }
    }

    // ----------------------------------------------------------------
    // Output CSV
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        // Iterate in sorted order: (A,F),(A,O),(N,F),(N,O),(R,F),(R,O)
        for (int rfi = 0; rfi < 3; rfi++) {
            for (int lsi = 0; lsi < 2; lsi++) {
                int gi = rfi * 2 + lsi;
                if (global_acc[gi].count == 0) continue;

                char rf_ch = RFLAG_CHARS[rfi];
                char ls_ch = LSTATUS_CHARS[lsi];
                int64_t cnt = global_acc[gi].count;
                double sq   = global_acc[gi].sum_qty;
                double sp   = global_acc[gi].sum_price;
                double sdp  = global_acc[gi].sum_disc_price;
                double sc   = global_acc[gi].sum_charge;
                double sd   = global_acc[gi].sum_disc;
                double avg_qty   = sq / cnt;
                double avg_price = sp / cnt;
                double avg_disc  = sd / cnt;

                fprintf(f, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                        rf_ch, ls_ch,
                        sq, sp, sdp, sc,
                        avg_qty, avg_price, avg_disc,
                        cnt);
            }
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
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
