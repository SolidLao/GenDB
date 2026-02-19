// Q1: Pricing Summary Report
// Parallel fused scan with thread-local 6-slot accumulators
// No hash table needed: group_idx = returnflag_code * 2 + linestatus_code

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <fstream>
#include <vector>
#include <array>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// ─── Data structures ────────────────────────────────────────────────────────

// Use long double for aggregates to avoid floating-point precision loss
// over ~29M-row groups (observed ~0.005 error with plain double)
struct Acc {
    int64_t    count         = 0;
    long double sum_qty      = 0.0L;
    long double sum_price    = 0.0L;
    long double sum_disc_price = 0.0L;
    long double sum_charge   = 0.0L;
    long double sum_disc     = 0.0L;
};

// ─── Helpers ────────────────────────────────────────────────────────────────

static const void* mmap_col(const std::string& path, size_t& out_bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_bytes = st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

// ─── Main query function ─────────────────────────────────────────────────────

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string li = gendb_dir + "/lineitem/";
    size_t sz;

    // Phase 1: mmap all required columns
    const int32_t* shipdate   = nullptr;
    const int8_t*  retflag    = nullptr;
    const int8_t*  linestatus = nullptr;
    const double*  quantity   = nullptr;
    const double*  extprice   = nullptr;
    const double*  discount   = nullptr;
    const double*  tax        = nullptr;
    size_t n_rows = 0;

    {
        GENDB_PHASE("dim_filter");
        shipdate   = reinterpret_cast<const int32_t*>(mmap_col(li + "l_shipdate.bin",    sz));
        n_rows     = sz / sizeof(int32_t);
        retflag    = reinterpret_cast<const int8_t* >(mmap_col(li + "l_returnflag.bin",  sz));
        linestatus = reinterpret_cast<const int8_t* >(mmap_col(li + "l_linestatus.bin",  sz));
        quantity   = reinterpret_cast<const double*  >(mmap_col(li + "l_quantity.bin",    sz));
        extprice   = reinterpret_cast<const double*  >(mmap_col(li + "l_extendedprice.bin", sz));
        discount   = reinterpret_cast<const double*  >(mmap_col(li + "l_discount.bin",    sz));
        tax        = reinterpret_cast<const double*  >(mmap_col(li + "l_tax.bin",         sz));
    }

    // Phase 2: zone-map precompute row offsets (minimal benefit; used to skip ~7 trailing blocks)
    // We'll use it to find the valid row range end (rows after threshold can be trimmed).
    size_t valid_end = n_rows; // conservative default: scan all
    {
        GENDB_PHASE("build_joins");
        // Load zone map for shipdate
        const std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        size_t zm_bytes;
        const uint32_t* zm = reinterpret_cast<const uint32_t*>(mmap_col(zm_path, zm_bytes));
        uint32_t num_blocks = zm[0];

        struct BlockInfo { int32_t min, max; uint32_t nrows; };
        const BlockInfo* blocks = reinterpret_cast<const BlockInfo*>(zm + 1);

        // Precompute row offsets once (O(num_blocks) = 600 iterations, trivial)
        // Data is sorted by shipdate → first block with min > 10471 marks the end
        size_t row_offset = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (blocks[b].min > 10471) {
                // All rows from here onward can be skipped (sorted data)
                valid_end = row_offset;
                break;
            }
            row_offset += blocks[b].nrows;
        }
        // If we never broke out, valid_end stays n_rows (all rows qualify)
        munmap((void*)zm, zm_bytes);
    }

    // Phase 3: parallel fused scan with thread-local accumulators
    // group_idx = retflag[i] * 2 + linestatus[i]  →  6 possible groups
    const int32_t DATE_THRESHOLD = 10471;

    int n_threads = omp_get_max_threads();
    // Pad to avoid false sharing (each Acc is 6*8+8 = 56 bytes; pad to 64)
    // Use a 2D array: [thread][group]
    std::vector<std::array<Acc, 6>> thread_acc(n_threads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(n_threads)
        {
            int tid = omp_get_thread_num();
            std::array<Acc, 6>& acc = thread_acc[tid];

            // Zero-init (already default-constructed, but be explicit)
            for (auto& a : acc) {
                a.count = 0; a.sum_qty = 0; a.sum_price = 0;
                a.sum_disc_price = 0; a.sum_charge = 0; a.sum_disc = 0;
            }

            #pragma omp for schedule(static)
            for (size_t i = 0; i < valid_end; i++) {
                // Predicate check (also handles any straggler rows within blocks)
                if (shipdate[i] > DATE_THRESHOLD) continue;

                int g = retflag[i] * 2 + linestatus[i];
                double qty  = quantity[i];
                double ep   = extprice[i];
                double disc = discount[i];
                double tx   = tax[i];

                acc[g].count++;
                acc[g].sum_qty        += qty;
                acc[g].sum_price      += ep;
                acc[g].sum_disc_price += ep * (1.0 - disc);
                acc[g].sum_charge     += ep * (1.0 - disc) * (1.0 + tx);
                acc[g].sum_disc       += disc;
            }
        }
    }

    // Phase 4: merge thread-local accumulators into global
    Acc global[6];
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < n_threads; t++) {
            for (int g = 0; g < 6; g++) {
                global[g].count         += thread_acc[t][g].count;
                global[g].sum_qty       += thread_acc[t][g].sum_qty;
                global[g].sum_price     += thread_acc[t][g].sum_price;
                global[g].sum_disc_price+= thread_acc[t][g].sum_disc_price;
                global[g].sum_charge    += thread_acc[t][g].sum_charge;
                global[g].sum_disc      += thread_acc[t][g].sum_disc;
            }
        }
    }

    // Phase 5: output results
    {
        GENDB_PHASE("output");

        // Dictionaries (from query guide)
        // returnflag dict: 0=A, 1=N, 2=R
        // linestatus dict: 0=F, 1=O
        const char* rf_dict[] = {"A", "N", "R"};
        const char* ls_dict[] = {"F", "O"};

        // Group index → (rf_code, ls_code)
        // group_idx = rf * 2 + ls
        // Order: (A,F)=0, (A,O)=1, (N,F)=2, (N,O)=3, (R,F)=4, (R,O)=5
        // This is already correct ASCII ORDER BY l_returnflag, l_linestatus

        std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); exit(1); }

        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                    "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int g = 0; g < 6; g++) {
            if (global[g].count == 0) continue; // skip empty groups

            int rf = g / 2;
            int ls = g % 2;
            long double cnt = static_cast<long double>(global[g].count);

            long double avg_qty   = global[g].sum_qty   / cnt;
            long double avg_price = global[g].sum_price  / cnt;
            long double avg_disc  = global[g].sum_disc   / cnt;

            fprintf(fp,
                "%s,%s,%.2Lf,%.2Lf,%.4Lf,%.6Lf,%.2Lf,%.2Lf,%.2Lf,%lld\n",
                rf_dict[rf],
                ls_dict[ls],
                global[g].sum_qty,
                global[g].sum_price,
                global[g].sum_disc_price,
                global[g].sum_charge,
                avg_qty,
                avg_price,
                avg_disc,
                static_cast<long long>(global[g].count)
            );
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
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
