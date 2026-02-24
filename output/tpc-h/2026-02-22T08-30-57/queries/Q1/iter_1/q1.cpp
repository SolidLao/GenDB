// Q1: TPC-H Pricing Summary Report
// Strategy: Zone-map-guided parallel scan, thread-local 6-slot accumulators,
//           direct-array aggregation (no hash table), single fused pass.

#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <vector>
#include <omp.h>

#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t  SHIPDATE_THRESHOLD = 10471;   // 1998-09-02 epoch days
static constexpr uint32_t BLOCK_SIZE         = 100000;
static constexpr int      NUM_GROUPS         = 6;       // 3 rf × 2 ls
static constexpr int      MAX_THREADS        = 64;

// ---------------------------------------------------------------------------
// Group-key mapping lookup tables (computed at compile time)
//   rf: A(65)→0, N(78)→1, R(82)→2   (else 0)
//   ls: F(70)→0, O(79)→1             (else 0)
//   group = rf_idx * 2 + ls_idx
// ---------------------------------------------------------------------------
static inline int rf_idx(int8_t c) {
    switch ((uint8_t)c) {
        case 65: return 0;  // A
        case 78: return 1;  // N
        case 82: return 2;  // R
        default: return 0;
    }
}

static inline int ls_idx(int8_t c) {
    return ((uint8_t)c == 79) ? 1 : 0;  // O=79→1, F=70→0
}

// ---------------------------------------------------------------------------
// Per-group accumulator — use long double to minimize floating-point errors
// across parallel threads (80-bit extended precision on x86_64)
// ---------------------------------------------------------------------------
struct GroupAcc {
    long double sum_qty       = 0.0L;
    long double sum_base      = 0.0L;
    long double sum_disc_price= 0.0L;
    long double sum_charge    = 0.0L;
    long double sum_discount  = 0.0L;
    int64_t     count         = 0;
};

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& out_n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Cannot open: %s\n", path.c_str());
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "mmap failed: %s\n", path.c_str());
        return nullptr;
    }
    out_n = sz / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// ---------------------------------------------------------------------------
// Zone-map types
// ---------------------------------------------------------------------------
struct ZoneBlock { int32_t mn, mx; };

// ---------------------------------------------------------------------------
// Run query
// ---------------------------------------------------------------------------
void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Data loading
    // -----------------------------------------------------------------------
    size_t n_rows = 0;

    const int32_t*  col_shipdate      = nullptr;
    const int8_t*   col_returnflag    = nullptr;
    const int8_t*   col_linestatus    = nullptr;
    const double*   col_quantity      = nullptr;
    const double*   col_extprice      = nullptr;
    const double*   col_discount      = nullptr;
    const double*   col_tax           = nullptr;

    const uint32_t* zonemap_raw       = nullptr;
    uint32_t        num_blocks        = 0;

    {
        GENDB_PHASE("data_loading");

        size_t n_tmp = 0;
        col_shipdate   = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",   n_tmp);
        n_rows = n_tmp;
        col_returnflag = mmap_col<int8_t>(gendb_dir + "/lineitem/l_returnflag.bin",  n_tmp);
        col_linestatus = mmap_col<int8_t>(gendb_dir + "/lineitem/l_linestatus.bin",  n_tmp);
        col_quantity   = mmap_col<double>(gendb_dir + "/lineitem/l_quantity.bin",    n_tmp);
        col_extprice   = mmap_col<double>(gendb_dir + "/lineitem/l_extendedprice.bin", n_tmp);
        col_discount   = mmap_col<double>(gendb_dir + "/lineitem/l_discount.bin",    n_tmp);
        col_tax        = mmap_col<double>(gendb_dir + "/lineitem/l_tax.bin",         n_tmp);

        // madvise sequential for near-full scan
        madvise((void*)col_shipdate,   n_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise((void*)col_returnflag, n_rows * sizeof(int8_t),  MADV_SEQUENTIAL);
        madvise((void*)col_linestatus, n_rows * sizeof(int8_t),  MADV_SEQUENTIAL);
        madvise((void*)col_quantity,   n_rows * sizeof(double),  MADV_SEQUENTIAL);
        madvise((void*)col_extprice,   n_rows * sizeof(double),  MADV_SEQUENTIAL);
        madvise((void*)col_discount,   n_rows * sizeof(double),  MADV_SEQUENTIAL);
        madvise((void*)col_tax,        n_rows * sizeof(double),  MADV_SEQUENTIAL);

        // Zone map
        {
            size_t zm_n = 0;
            zonemap_raw = mmap_col<uint32_t>(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zm_n);
            num_blocks = zonemap_raw[0];
        }
    }

    // Build list of valid blocks (pass zone map)
    const ZoneBlock* zone_blocks = reinterpret_cast<const ZoneBlock*>(zonemap_raw + 1);

    std::vector<uint32_t> valid_blocks;
    valid_blocks.reserve(num_blocks);
    for (uint32_t b = 0; b < num_blocks; ++b) {
        if (zone_blocks[b].mn <= SHIPDATE_THRESHOLD) {
            valid_blocks.push_back(b);
        }
    }

    // -----------------------------------------------------------------------
    // Main scan — parallel morsel-driven over valid blocks
    // -----------------------------------------------------------------------
    // Thread-local accumulators: [thread_id][group_id]
    GroupAcc tl_acc[MAX_THREADS][NUM_GROUPS] = {};

    {
        GENDB_PHASE("main_scan");

        int nblocks = (int)valid_blocks.size();

        #pragma omp parallel for schedule(dynamic, 4) num_threads(MAX_THREADS)
        for (int bi = 0; bi < nblocks; ++bi) {
            int tid = omp_get_thread_num();
            GroupAcc* local = tl_acc[tid];

            uint32_t b = valid_blocks[bi];
            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = std::min(row_start + BLOCK_SIZE, n_rows);

            // Check if this block can be skipped entirely
            // (block_max <= threshold means all rows pass — most blocks)
            // (block_min > threshold means skip — already filtered above)
            bool full_pass = (zone_blocks[b].mx <= SHIPDATE_THRESHOLD);

            if (full_pass) {
                // All rows in block pass filter — tight inner loop
                for (size_t r = row_start; r < row_end; ++r) {
                    int g = rf_idx(col_returnflag[r]) * 2 + ls_idx(col_linestatus[r]);
                    long double qty  = col_quantity[r];
                    long double ep   = col_extprice[r];
                    long double disc = col_discount[r];
                    long double tax  = col_tax[r];
                    long double disc1 = 1.0L - disc;
                    local[g].sum_qty        += qty;
                    local[g].sum_base       += ep;
                    local[g].sum_disc_price += ep * disc1;
                    local[g].sum_charge     += ep * disc1 * (1.0L + tax);
                    local[g].sum_discount   += disc;
                    local[g].count          += 1;
                }
            } else {
                // Partial block — must apply filter
                for (size_t r = row_start; r < row_end; ++r) {
                    if (col_shipdate[r] > SHIPDATE_THRESHOLD) continue;
                    int g = rf_idx(col_returnflag[r]) * 2 + ls_idx(col_linestatus[r]);
                    long double qty  = col_quantity[r];
                    long double ep   = col_extprice[r];
                    long double disc = col_discount[r];
                    long double tax  = col_tax[r];
                    long double disc1 = 1.0L - disc;
                    local[g].sum_qty        += qty;
                    local[g].sum_base       += ep;
                    local[g].sum_disc_price += ep * disc1;
                    local[g].sum_charge     += ep * disc1 * (1.0L + tax);
                    local[g].sum_discount   += disc;
                    local[g].count          += 1;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Merge thread-local accumulators
    // -----------------------------------------------------------------------
    GroupAcc global_acc[NUM_GROUPS] = {};
    {
        GENDB_PHASE("aggregation_merge");
        int nthreads = omp_get_max_threads();
        if (nthreads > MAX_THREADS) nthreads = MAX_THREADS;
        for (int t = 0; t < nthreads; ++t) {
            for (int g = 0; g < NUM_GROUPS; ++g) {
                global_acc[g].sum_qty        += tl_acc[t][g].sum_qty;
                global_acc[g].sum_base       += tl_acc[t][g].sum_base;
                global_acc[g].sum_disc_price += tl_acc[t][g].sum_disc_price;
                global_acc[g].sum_charge     += tl_acc[t][g].sum_charge;
                global_acc[g].sum_discount   += tl_acc[t][g].sum_discount;
                global_acc[g].count          += tl_acc[t][g].count;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Collect non-empty groups and sort
    // -----------------------------------------------------------------------
    // Mapping from group index back to (rf_char, ls_char)
    // group = rf_idx * 2 + ls_idx
    // rf: 0→A(65), 1→N(78), 2→R(82)
    // ls: 0→F(70), 1→O(79)
    static const uint8_t RF_CHARS[3] = {65, 78, 82};  // A, N, R
    static const uint8_t LS_CHARS[2] = {70, 79};       // F, O

    struct ResultRow {
        char rf;
        char ls;
        double sum_qty;
        double sum_base;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_GROUPS);

    for (int rf = 0; rf < 3; ++rf) {
        for (int ls = 0; ls < 2; ++ls) {
            int g = rf * 2 + ls;
            if (global_acc[g].count == 0) continue;
            ResultRow row;
            row.rf            = (char)RF_CHARS[rf];
            row.ls            = (char)LS_CHARS[ls];
            row.sum_qty       = (double)global_acc[g].sum_qty;
            row.sum_base      = (double)global_acc[g].sum_base;
            row.sum_disc_price= (double)global_acc[g].sum_disc_price;
            row.sum_charge    = (double)global_acc[g].sum_charge;
            row.avg_qty       = (double)(global_acc[g].sum_qty      / (long double)global_acc[g].count);
            row.avg_price     = (double)(global_acc[g].sum_base     / (long double)global_acc[g].count);
            row.avg_disc      = (double)(global_acc[g].sum_discount / (long double)global_acc[g].count);
            row.count         = global_acc[g].count;
            results.push_back(row);
        }
    }

    // Sort by l_returnflag ASC, l_linestatus ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.rf != b.rf) return a.rf < b.rf;
        return a.ls < b.ls;
    });

    // -----------------------------------------------------------------------
    // Output CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return;
        }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : results) {
            fprintf(f, "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%" PRId64 "\n",
                r.rf, r.ls,
                r.sum_qty,
                r.sum_base,
                r.sum_disc_price,
                r.sum_charge,
                r.avg_qty,
                r.avg_price,
                r.avg_disc,
                r.count);
        }

        fclose(f);
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
