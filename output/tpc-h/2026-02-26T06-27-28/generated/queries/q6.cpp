// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;
//
// Strategy:
//   1. Zone-map prune: read indexes/lineitem_shipdate_zonemap.bin to find
//      the contiguous [first_block, last_block] that overlap [8766, 9131).
//      Eliminates ~86% of blocks (lineitem is sorted by l_shipdate).
//   2. Parallel morsel-driven scan: partition [first_block, last_block] across
//      all available threads; each accumulates a local double revenue.
//   3. OpenMP reduction to sum thread-local accumulators.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <filesystem>
#include <climits>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Filter constants (all compile-time)
    // -------------------------------------------------------------------------
    constexpr int32_t SHIP_LO    = 8766;   // DATE '1994-01-01'
    constexpr int32_t SHIP_HI    = 9131;   // DATE '1995-01-01'
    constexpr double  DISC_LO    = 0.05;
    constexpr double  DISC_HI    = 0.07;
    constexpr double  QTY_MAX    = 24.0;
    constexpr size_t  BLOCK_SIZE = 100000;

    // -------------------------------------------------------------------------
    // Phase: data_loading — zone map prune
    // -------------------------------------------------------------------------
    size_t first_block = SIZE_MAX;
    size_t last_block  = 0;
    uint64_t n_blocks  = 0;

    {
        GENDB_PHASE("data_loading");

        const std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = ::open(zm_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::fprintf(stderr, "Cannot open zone map: %s\n", zm_path.c_str());
            return 1;
        }

        // Layout: uint64_t n_blocks, then n_blocks × {int32_t min, int32_t max}
        if (::read(fd, &n_blocks, sizeof(uint64_t)) != sizeof(uint64_t)) {
            ::close(fd);
            std::fprintf(stderr, "Short read on zone map header\n");
            return 1;
        }

        for (size_t b = 0; b < n_blocks; ++b) {
            int32_t mn, mx;
            if (::read(fd, &mn, 4) != 4 || ::read(fd, &mx, 4) != 4) {
                ::close(fd);
                std::fprintf(stderr, "Short read on zone map entry %zu\n", b);
                return 1;
            }
            // Block overlaps [SHIP_LO, SHIP_HI) if mx >= SHIP_LO && mn < SHIP_HI
            if (mx >= SHIP_LO && mn < SHIP_HI) {
                if (b < first_block) first_block = b;
                last_block = b;
            }
        }
        ::close(fd);
    }

    if (first_block == SIZE_MAX) {
        // No qualifying blocks — output zero
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        std::fprintf(f, "revenue\n0.00\n");
        std::fclose(f);
        std::printf("revenue: 0.00\n");
        return 0;
    }

    // -------------------------------------------------------------------------
    // Mmap all four columns
    // -------------------------------------------------------------------------
    const std::string li_dir = gendb_dir + "/lineitem";

    gendb::MmapColumn<int32_t> col_shipdate(li_dir + "/l_shipdate.bin");
    gendb::MmapColumn<double>  col_discount(li_dir + "/l_discount.bin");
    gendb::MmapColumn<double>  col_quantity(li_dir + "/l_quantity.bin");
    gendb::MmapColumn<double>  col_extprice(li_dir + "/l_extendedprice.bin");

    const size_t total_rows = col_shipdate.size();

    // Fire readahead only on the qualifying row range to avoid wasted I/O
    {
        size_t row_lo = first_block * BLOCK_SIZE;
        size_t row_hi = std::min((last_block + 1) * BLOCK_SIZE, total_rows);
        size_t byte_lo_32 = row_lo * sizeof(int32_t);
        size_t byte_len_32 = (row_hi - row_lo) * sizeof(int32_t);
        size_t byte_lo_64 = row_lo * sizeof(double);
        size_t byte_len_64 = (row_hi - row_lo) * sizeof(double);

        madvise(const_cast<void*>(static_cast<const void*>(col_shipdate.data + row_lo)), byte_len_32, MADV_WILLNEED);
        madvise(const_cast<void*>(static_cast<const void*>(col_discount.data + row_lo)), byte_len_64, MADV_WILLNEED);
        madvise(const_cast<void*>(static_cast<const void*>(col_quantity.data  + row_lo)), byte_len_64, MADV_WILLNEED);
        madvise(const_cast<void*>(static_cast<const void*>(col_extprice.data  + row_lo)), byte_len_64, MADV_WILLNEED);
        (void)byte_lo_32; (void)byte_lo_64;
    }

    // -------------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven scan over qualifying blocks
    // -------------------------------------------------------------------------
    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        const int32_t fb = static_cast<int32_t>(first_block);
        const int32_t lb = static_cast<int32_t>(last_block);

        const int32_t* __restrict__ ship = col_shipdate.data;
        const double*  __restrict__ disc = col_discount.data;
        const double*  __restrict__ qty  = col_quantity.data;
        const double*  __restrict__ ext  = col_extprice.data;

        #pragma omp parallel reduction(+:global_revenue)
        {
            double local_sum = 0.0;

            #pragma omp for schedule(dynamic, 4) nowait
            for (int32_t b = fb; b <= lb; ++b) {
                size_t row_lo = static_cast<size_t>(b) * BLOCK_SIZE;
                size_t row_hi = std::min(row_lo + BLOCK_SIZE, total_rows);
                size_t len    = row_hi - row_lo;

                const int32_t* s = ship + row_lo;
                const double*  d = disc + row_lo;
                const double*  q = qty  + row_lo;
                const double*  e = ext  + row_lo;

                double block_sum = 0.0;
                for (size_t i = 0; i < len; ++i) {
                    // Apply cheapest filter first (int32 range on sorted column)
                    if (s[i] >= SHIP_LO & s[i] < SHIP_HI &
                        d[i] >= DISC_LO & d[i] <= DISC_HI &
                        q[i] < QTY_MAX) {
                        block_sum += e[i] * d[i];
                    }
                }
                local_sum += block_sum;
            }

            global_revenue += local_sum;
        }
    }

    // -------------------------------------------------------------------------
    // Phase: output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q6.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return 1;
        }
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", global_revenue);
        std::fclose(f);
        std::printf("revenue: %.2f\n", global_revenue);
    }

    return 0;
}
