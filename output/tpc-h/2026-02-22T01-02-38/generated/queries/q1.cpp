// Q1: TPC-H Pricing Summary Report
// Single-table scan with filter + aggregation.
// Strategy: zone-map tail-skip, 64-thread morsel scan, thread-local 6-slot accumulators.

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <string>
#include <iostream>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// Per-group accumulator using long double (80-bit extended precision on x86-64) to eliminate
// floating-point accumulation error from parallel summation over 29M+ rows.
// Padded to 128 bytes (2 cache lines) to avoid false sharing between threads.
// 5 * sizeof(long double) = 5*16 = 80 bytes + uint64_t(8) = 88 bytes → padded to 128.
struct alignas(64) GroupAcc {
    long double sum_qty        = 0.0L;
    long double sum_base       = 0.0L;
    long double sum_disc_price = 0.0L;
    long double sum_charge     = 0.0L;
    long double sum_disc       = 0.0L;
    uint64_t    count          = 0;
    // pad to 128 bytes: 88 used, 40 padding
    char _pad[40];
};
static_assert(sizeof(GroupAcc) == 128, "GroupAcc must be 128 bytes");

// Group slot encoding:
//   returnflag: 'A'->0, 'N'->1, 'R'->2
//   linestatus: 'F'->0, 'O'->1
//   slot = rf_idx*2 + ls_idx
// Slot order: (A,F)=0, (A,O)=1, (N,F)=2, (N,O)=3, (R,F)=4, (R,O)=5
// Already in ASC order for (returnflag, linestatus) output.

static inline int rf_to_idx(int8_t rf) {
    if (rf == 'A') return 0;
    if (rf == 'N') return 1;
    return 2; // 'R'
}

static inline int ls_to_idx(int8_t ls) {
    return (ls == 'O') ? 1 : 0; // 'F'->0, 'O'->1
}

// Thread-local accumulator banks: [thread_id][group_slot]
// 64 threads * 6 groups * 128 bytes = 49,152 bytes total (~48 KB, fits in L2)
static GroupAcc g_thread_accs[64][6];

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // l_shipdate <= DATE '1998-09-02' => epoch days <= 10471
    const int32_t DATE_THRESHOLD = 10471;
    const size_t  BLOCK_SIZE     = 100000;
    const int     NUM_GROUPS     = 6;
    const int     NUM_THREADS    = 64;

    // -----------------------------------------------------------------------
    // Phase 0: Data loading — mmap all columns, issue parallel madvise
    // -----------------------------------------------------------------------

    size_t n_rows            = 0;
    size_t total_rows_to_scan= 0;

    const int32_t* col_shipdate      = nullptr;
    const int8_t*  col_returnflag    = nullptr;
    const int8_t*  col_linestatus    = nullptr;
    const double*  col_quantity      = nullptr;
    const double*  col_extendedprice = nullptr;
    const double*  col_discount      = nullptr;
    const double*  col_tax           = nullptr;

    size_t sz_sd=0, sz_rf=0, sz_ls=0, sz_qty=0, sz_ep=0, sz_disc=0, sz_tax=0;

    {
        GENDB_PHASE("data_loading");

        // --- Zone map (tiny ~4.8 KB): load synchronously to determine scan range ---
        {
            std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
            int zm_fd = open(zm_path.c_str(), O_RDONLY);
            if (zm_fd < 0) { perror(zm_path.c_str()); exit(1); }
            struct stat zm_st; fstat(zm_fd, &zm_st);
            const uint8_t* zm_raw = reinterpret_cast<const uint8_t*>(
                mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, zm_fd, 0));
            if (zm_raw == MAP_FAILED) { perror("mmap zone map"); exit(1); }
            close(zm_fd);

            uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
            const int32_t* zm_blocks = reinterpret_cast<const int32_t*>(zm_raw + sizeof(uint32_t));

            // Find first block where block_min > DATE_THRESHOLD (sorted data → all rows in that
            // block fail the filter). Since lineitem is sorted by l_shipdate, once we find this
            // block we can stop scanning entirely.
            uint32_t last_valid_block = num_blocks;
            for (uint32_t b = 0; b < num_blocks; b++) {
                int32_t bmin = zm_blocks[b * 2]; // [min, max] per block
                if (bmin > DATE_THRESHOLD) {
                    last_valid_block = b;
                    break;
                }
            }

            munmap(const_cast<uint8_t*>(zm_raw), zm_st.st_size);

            // We'll still apply row-level filter inside the last qualifying block
            // (it may contain a mix of rows <= and > threshold at the boundary).
            total_rows_to_scan = (size_t)last_valid_block * BLOCK_SIZE;
        }

        // Helper lambda: open + mmap a column file without MAP_POPULATE (async prefetch via madvise)
        auto do_mmap_col = [&](const std::string& path, size_t& sz) -> const void* {
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) { perror(path.c_str()); exit(1); }
            struct stat st; fstat(fd, &st);
            sz = st.st_size;
            const void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
            if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
            posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
            close(fd);
            return ptr;
        };

        col_shipdate      = reinterpret_cast<const int32_t*>(do_mmap_col(gendb_dir + "/lineitem/l_shipdate.bin",      sz_sd));
        col_returnflag    = reinterpret_cast<const int8_t* >(do_mmap_col(gendb_dir + "/lineitem/l_returnflag.bin",    sz_rf));
        col_linestatus    = reinterpret_cast<const int8_t* >(do_mmap_col(gendb_dir + "/lineitem/l_linestatus.bin",    sz_ls));
        col_quantity      = reinterpret_cast<const double* >(do_mmap_col(gendb_dir + "/lineitem/l_quantity.bin",      sz_qty));
        col_extendedprice = reinterpret_cast<const double* >(do_mmap_col(gendb_dir + "/lineitem/l_extendedprice.bin", sz_ep));
        col_discount      = reinterpret_cast<const double* >(do_mmap_col(gendb_dir + "/lineitem/l_discount.bin",      sz_disc));
        col_tax           = reinterpret_cast<const double* >(do_mmap_col(gendb_dir + "/lineitem/l_tax.bin",           sz_tax));

        n_rows = sz_sd / sizeof(int32_t);
        if (total_rows_to_scan > n_rows) total_rows_to_scan = n_rows;

        // Issue parallel madvise(MADV_WILLNEED) to saturate HDD I/O bandwidth.
        // Each call is async; the kernel fetches pages in the background.
        size_t scan_bytes_i32 = total_rows_to_scan * sizeof(int32_t);
        size_t scan_bytes_i8  = total_rows_to_scan * sizeof(int8_t);
        size_t scan_bytes_dbl = total_rows_to_scan * sizeof(double);

        #pragma omp parallel for num_threads(7) schedule(static, 1)
        for (int i = 0; i < 7; i++) {
            if      (i == 0) madvise(const_cast<int32_t*>(col_shipdate),      scan_bytes_i32, MADV_WILLNEED);
            else if (i == 1) madvise(const_cast<int8_t* >(col_returnflag),    scan_bytes_i8,  MADV_WILLNEED);
            else if (i == 2) madvise(const_cast<int8_t* >(col_linestatus),    scan_bytes_i8,  MADV_WILLNEED);
            else if (i == 3) madvise(const_cast<double* >(col_quantity),      scan_bytes_dbl, MADV_WILLNEED);
            else if (i == 4) madvise(const_cast<double* >(col_extendedprice), scan_bytes_dbl, MADV_WILLNEED);
            else if (i == 5) madvise(const_cast<double* >(col_discount),      scan_bytes_dbl, MADV_WILLNEED);
            else             madvise(const_cast<double* >(col_tax),           scan_bytes_dbl, MADV_WILLNEED);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 1: Main scan — fused filter + aggregate, 64 threads
    // -----------------------------------------------------------------------

    // Zero-initialize all thread-local accumulators.
    // g_thread_accs is static so already zero-initialized, but memset for safety across repeated calls.
    memset(g_thread_accs, 0, sizeof(g_thread_accs));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            const int tid = omp_get_thread_num();
            GroupAcc* __restrict__ local = g_thread_accs[tid];

            // Morsel-driven partitioning: each thread gets a contiguous chunk of rows.
            const size_t chunk     = (total_rows_to_scan + NUM_THREADS - 1) / NUM_THREADS;
            const size_t row_start = (size_t)tid * chunk;
            const size_t row_end   = std::min(row_start + chunk, total_rows_to_scan);

            // Scalar inner loop — the compiler will auto-vectorize with AVX512 given -march=native.
            // Filter + accumulate fused into a single pass.
            for (size_t i = row_start; i < row_end; i++) {
                if (col_shipdate[i] > DATE_THRESHOLD) continue;

                const int slot = rf_to_idx(col_returnflag[i]) * 2 + ls_to_idx(col_linestatus[i]);

                const long double qty  = col_quantity[i];
                const long double ep   = col_extendedprice[i];
                const long double disc = col_discount[i];
                const long double tax  = col_tax[i];

                const long double disc_price = ep * (1.0L - disc);

                local[slot].sum_qty        += qty;
                local[slot].sum_base       += ep;
                local[slot].sum_disc_price += disc_price;
                local[slot].sum_charge     += disc_price * (1.0L + tax);
                local[slot].sum_disc       += disc;
                local[slot].count          += 1;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Merge thread-local accumulators into final 6-slot result
    // -----------------------------------------------------------------------
    GroupAcc final_acc[NUM_GROUPS] = {};

    for (int t = 0; t < NUM_THREADS; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            final_acc[g].sum_qty        += g_thread_accs[t][g].sum_qty;
            final_acc[g].sum_base       += g_thread_accs[t][g].sum_base;
            final_acc[g].sum_disc_price += g_thread_accs[t][g].sum_disc_price;
            final_acc[g].sum_charge     += g_thread_accs[t][g].sum_charge;
            final_acc[g].sum_disc       += g_thread_accs[t][g].sum_disc;
            final_acc[g].count          += g_thread_accs[t][g].count;
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Output — write CSV (slot order is already ASC by rf, ls)
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Slot layout: (A,F)=0, (A,O)=1, (N,F)=2, (N,O)=3, (R,F)=4, (R,O)=5
        // This is naturally sorted by (returnflag ASC, linestatus ASC).
        const char rf_chars[3] = {'A', 'N', 'R'};
        const char ls_chars[2] = {'F', 'O'};

        std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); exit(1); }

        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                    "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int rfi = 0; rfi < 3; rfi++) {
            for (int lsi = 0; lsi < 2; lsi++) {
                const int slot = rfi * 2 + lsi;
                const GroupAcc& acc = final_acc[slot];
                if (acc.count == 0) continue;

                const long double cnt = static_cast<long double>(acc.count);
                fprintf(fp, "%c,%c,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%llu\n",
                    rf_chars[rfi],
                    ls_chars[lsi],
                    acc.sum_qty,
                    acc.sum_base,
                    acc.sum_disc_price,
                    acc.sum_charge,
                    acc.sum_qty        / cnt,
                    acc.sum_base       / cnt,
                    acc.sum_disc       / cnt,
                    static_cast<unsigned long long>(acc.count));
            }
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
