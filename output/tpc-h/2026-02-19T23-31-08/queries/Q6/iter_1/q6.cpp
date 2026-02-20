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
    // Phase 2: mmap only the qualifying row range (partial mmap)
    // -------------------------------------------------------------------------
    const int32_t* col_shipdate     = nullptr;
    const double*  col_discount     = nullptr;
    const double*  col_quantity     = nullptr;
    const double*  col_extendedprice= nullptr;
    size_t sz_sd, sz_disc, sz_qty, sz_price;
    int fd_sd, fd_disc, fd_qty, fd_price;
    // raw mmap bases (before elem adjustment) — needed for munmap
    void* base_sd = nullptr, *base_disc = nullptr, *base_qty = nullptr, *base_price = nullptr;

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

        const size_t PAGE = 4096;

        // Byte ranges for the qualifying row range
        size_t byte_lo_i32 = row_start * sizeof(int32_t);
        size_t byte_hi_i32 = row_end   * sizeof(int32_t);
        size_t byte_lo_f64 = row_start * sizeof(double);
        size_t byte_hi_f64 = row_end   * sizeof(double);

        // Page-aligned offsets
        size_t alo_i32 = (byte_lo_i32 / PAGE) * PAGE;
        size_t alo_f64 = (byte_lo_f64 / PAGE) * PAGE;
        size_t ahi_i32 = ((byte_hi_i32 + PAGE - 1) / PAGE) * PAGE;
        size_t ahi_f64 = ((byte_hi_f64 + PAGE - 1) / PAGE) * PAGE;

        // Issue readahead for all 4 columns before mmap (async, sequential, HDD-friendly)
        posix_fadvise(fd_sd,    alo_i32, ahi_i32 - alo_i32, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
        posix_fadvise(fd_disc,  alo_f64, ahi_f64 - alo_f64, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
        posix_fadvise(fd_qty,   alo_f64, ahi_f64 - alo_f64, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
        posix_fadvise(fd_price, alo_f64, ahi_f64 - alo_f64, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);

        // Clamp map sizes to actual file sizes
        struct stat st;
        auto clamp_sz = [&](int fd, size_t desired) -> size_t {
            fstat(fd, &st);
            return (desired > (size_t)st.st_size) ? (size_t)st.st_size : desired;
        };

        sz_sd    = clamp_sz(fd_sd,    ahi_i32) - alo_i32;
        sz_disc  = clamp_sz(fd_disc,  ahi_f64) - alo_f64;
        sz_qty   = clamp_sz(fd_qty,   ahi_f64) - alo_f64;
        sz_price = clamp_sz(fd_price, ahi_f64) - alo_f64;

        base_sd    = mmap(nullptr, sz_sd,    PROT_READ, MAP_PRIVATE, fd_sd,    alo_i32);
        base_disc  = mmap(nullptr, sz_disc,  PROT_READ, MAP_PRIVATE, fd_disc,  alo_f64);
        base_qty   = mmap(nullptr, sz_qty,   PROT_READ, MAP_PRIVATE, fd_qty,   alo_f64);
        base_price = mmap(nullptr, sz_price, PROT_READ, MAP_PRIVATE, fd_price, alo_f64);

        if (base_sd    == MAP_FAILED || base_disc  == MAP_FAILED ||
            base_qty   == MAP_FAILED || base_price == MAP_FAILED) {
            perror("mmap partial col"); std::exit(1);
        }

        madvise(base_sd,    sz_sd,    MADV_SEQUENTIAL);
        madvise(base_disc,  sz_disc,  MADV_SEQUENTIAL);
        madvise(base_qty,   sz_qty,   MADV_SEQUENTIAL);
        madvise(base_price, sz_price, MADV_SEQUENTIAL);

        // Adjust pointers to row_start (compensate for page alignment)
        size_t adj_i32 = (byte_lo_i32 - alo_i32) / sizeof(int32_t);
        size_t adj_f64 = (byte_lo_f64 - alo_f64) / sizeof(double);

        col_shipdate      = reinterpret_cast<const int32_t*>(base_sd)    + adj_i32;
        col_discount      = reinterpret_cast<const double* >(base_disc)  + adj_f64;
        col_quantity      = reinterpret_cast<const double* >(base_qty)   + adj_f64;
        col_extendedprice = reinterpret_cast<const double* >(base_price) + adj_f64;
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
            for (int64_t i = 0; i < (int64_t)total_rows; ++i) {
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
    munmap(base_sd,    sz_sd);
    munmap(base_disc,  sz_disc);
    munmap(base_qty,   sz_qty);
    munmap(base_price, sz_price);
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
