#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= 8766 AND l_shipdate < 9131
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t row_count;
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // Date constants
    const int32_t DATE_LO = 8766; // 1994-01-01
    const int32_t DATE_HI = 9131; // 1995-01-01
    const double  DISC_LO = 0.05 - 1e-10;
    const double  DISC_HI = 0.07 + 1e-10;
    const double  QTY_MAX = 24.0;
    const uint32_t BLOCK_SIZE = 100000;

    // -------------------------------------------------------------------------
    // Phase 1: Load zone map and find qualifying block range
    // -------------------------------------------------------------------------
    uint32_t first_block = 0, last_block = 0;
    uint64_t total_rows = 0;
    uint32_t num_blocks = 0;

    const ZoneEntry* zones = nullptr;
    void*   zonemap_ptr = nullptr;
    size_t  zonemap_size = 0;

    {
        GENDB_PHASE("zone_map");

        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        if (fd < 0) { perror("open zonemap"); std::exit(1); }
        struct stat st; fstat(fd, &st);
        zonemap_size = st.st_size;
        zonemap_ptr = mmap(nullptr, zonemap_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        close(fd);
        if (zonemap_ptr == MAP_FAILED) { perror("mmap zonemap"); std::exit(1); }

        num_blocks = *reinterpret_cast<const uint32_t*>(zonemap_ptr);
        zones = reinterpret_cast<const ZoneEntry*>(
            reinterpret_cast<const uint8_t*>(zonemap_ptr) + sizeof(uint32_t));

        // Binary search: first block whose max_val >= DATE_LO
        uint32_t lo = 0, hi = num_blocks;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (zones[mid].max_val < DATE_LO) lo = mid + 1;
            else hi = mid;
        }
        first_block = lo;

        // Binary search: last block whose min_val < DATE_HI
        lo = first_block; hi = num_blocks;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (zones[mid].min_val < DATE_HI) lo = mid + 1;
            else hi = mid;
        }
        // last_block is inclusive
        last_block = (lo > first_block) ? lo - 1 : first_block;

        // Compute total rows in qualifying range
        for (uint32_t b = first_block; b <= last_block; ++b)
            total_rows += zones[b].row_count;
    }

    uint64_t row_start = (uint64_t)first_block * BLOCK_SIZE;
    uint64_t row_end   = row_start + total_rows;

    // -------------------------------------------------------------------------
    // Phase 2: mmap the four columns
    // -------------------------------------------------------------------------
    const int32_t* col_shipdate     = nullptr;
    const double*  col_discount     = nullptr;
    const double*  col_quantity     = nullptr;
    const double*  col_extendedprice= nullptr;
    size_t sz_sd, sz_disc, sz_qty, sz_price;
    int fd_sd, fd_disc, fd_qty, fd_price;

    {
        GENDB_PHASE("mmap_columns");

        auto open_col = [&](const std::string& rel, const std::string& col) -> int {
            std::string p = gendb_dir + "/" + rel + "/" + col + ".bin";
            int fd = open(p.c_str(), O_RDONLY);
            if (fd < 0) { perror(("open " + p).c_str()); std::exit(1); }
            return fd;
        };

        fd_sd    = open_col("lineitem", "l_shipdate");
        fd_disc  = open_col("lineitem", "l_discount");
        fd_qty   = open_col("lineitem", "l_quantity");
        fd_price = open_col("lineitem", "l_extendedprice");

        struct stat st;
        auto mmap_col = [&](int fd, size_t& sz) -> void* {
            fstat(fd, &st);
            sz = st.st_size;
            void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            if (p == MAP_FAILED) { perror("mmap col"); std::exit(1); }
            posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
            return p;
        };

        col_shipdate      = reinterpret_cast<const int32_t*>(mmap_col(fd_sd,    sz_sd));
        col_discount      = reinterpret_cast<const double* >(mmap_col(fd_disc,  sz_disc));
        col_quantity      = reinterpret_cast<const double* >(mmap_col(fd_qty,   sz_qty));
        col_extendedprice = reinterpret_cast<const double* >(mmap_col(fd_price, sz_price));
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel scan with thread-local accumulators
    // -------------------------------------------------------------------------
    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        omp_set_num_threads(64);

        #pragma omp parallel reduction(+:global_revenue)
        {
            double local_rev = 0.0;

            #pragma omp for schedule(static, 131072)
            for (int64_t i = (int64_t)row_start; i < (int64_t)row_end; ++i) {
                int32_t sd   = col_shipdate[i];
                double  disc = col_discount[i];
                double  qty  = col_quantity[i];
                // All three predicates
                if (sd   >= DATE_LO && sd < DATE_HI &&
                    disc >= DISC_LO && disc <= DISC_HI &&
                    qty  <  QTY_MAX) {
                    local_rev += col_extendedprice[i] * disc;
                }
            }

            global_revenue += local_rev;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror("fopen output"); std::exit(1); }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", global_revenue);
        fclose(f);
    }

    // Cleanup
    munmap(zonemap_ptr, zonemap_size);
    munmap((void*)col_shipdate,      sz_sd);
    munmap((void*)col_discount,      sz_disc);
    munmap((void*)col_quantity,      sz_qty);
    munmap((void*)col_extendedprice, sz_price);
    close(fd_sd); close(fd_disc); close(fd_qty); close(fd_price);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
