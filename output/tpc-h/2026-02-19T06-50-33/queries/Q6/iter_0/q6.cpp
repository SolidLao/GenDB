#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <vector>
#include <string>
#include <numeric>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

// Date thresholds (epoch days since 1970-01-01):
//   1994-01-01 = 8766
//   1995-01-01 = 9131

static constexpr int32_t SHIPDATE_LO = 8766;
static constexpr int32_t SHIPDATE_HI = 9131;
static constexpr double  DISCOUNT_LO = 0.05;
static constexpr double  DISCOUNT_HI = 0.07;
static constexpr double  QUANTITY_MAX = 24.0;

#pragma pack(push, 1)
struct ShipdateZMEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_nrows;
};

struct DiscountZMEntry {
    double   min_val;
    double   max_val;
    uint32_t block_nrows;
};
#pragma pack(pop)

// Helper: mmap a file, return pointer and size
static void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::fprintf(stderr, "ERROR: cannot open %s\n", path.c_str());
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::fprintf(stderr, "ERROR: mmap failed for %s\n", path.c_str());
        return nullptr;
    }
    return ptr;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Phase 1: Load zone maps and identify qualifying blocks
    // -------------------------------------------------------------------------
    std::vector<uint64_t> block_row_offsets;   // starting row index of each qualifying block
    std::vector<uint32_t> block_row_counts;    // row count of each qualifying block

    {
        GENDB_PHASE("dim_filter");

        // --- Shipdate zone map ---
        size_t szm_size = 0;
        void* szm_ptr = mmap_file(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", szm_size);
        if (!szm_ptr) return;
        madvise(szm_ptr, szm_size, MADV_SEQUENTIAL);

        uint32_t num_blocks_s = *reinterpret_cast<const uint32_t*>(szm_ptr);
        const ShipdateZMEntry* szm_entries = reinterpret_cast<const ShipdateZMEntry*>(
            static_cast<const uint8_t*>(szm_ptr) + sizeof(uint32_t));

        // --- Discount zone map (secondary filter) ---
        size_t dzm_size = 0;
        void* dzm_ptr = mmap_file(gendb_dir + "/indexes/lineitem_discount_zonemap.bin", dzm_size);
        if (!dzm_ptr) return;

        uint32_t num_blocks_d = *reinterpret_cast<const uint32_t*>(dzm_ptr);
        const DiscountZMEntry* dzm_entries = reinterpret_cast<const DiscountZMEntry*>(
            static_cast<const uint8_t*>(dzm_ptr) + sizeof(uint32_t));

        uint32_t num_blocks = num_blocks_s;

        block_row_offsets.reserve(num_blocks);
        block_row_counts.reserve(num_blocks);

        uint64_t row_offset = 0;
        for (uint32_t b = 0; b < num_blocks; ++b) {
            uint32_t nrows = szm_entries[b].block_nrows;
            int32_t  smin  = szm_entries[b].min_val;
            int32_t  smax  = szm_entries[b].max_val;

            // Shipdate zone map prune
            bool skip = (smax < SHIPDATE_LO) || (smin >= SHIPDATE_HI);

            if (!skip && b < num_blocks_d) {
                // Discount zone map secondary prune
                double dmin = dzm_entries[b].min_val;
                double dmax = dzm_entries[b].max_val;
                if (dmax < DISCOUNT_LO || dmin > DISCOUNT_HI) {
                    skip = true;
                }
            }

            if (!skip) {
                block_row_offsets.push_back(row_offset);
                block_row_counts.push_back(nrows);
            }
            row_offset += nrows;
        }

        munmap(szm_ptr, szm_size);
        munmap(dzm_ptr, dzm_size);
    }

    int num_qual_blocks = (int)block_row_offsets.size();

    // -------------------------------------------------------------------------
    // Phase 2: mmap the 4 columns
    // -------------------------------------------------------------------------
    size_t sz_shipdate = 0, sz_discount = 0, sz_quantity = 0, sz_extprice = 0;

    void* ptr_shipdate  = mmap_file(gendb_dir + "/lineitem/l_shipdate.bin",      sz_shipdate);
    void* ptr_discount  = mmap_file(gendb_dir + "/lineitem/l_discount.bin",      sz_discount);
    void* ptr_quantity  = mmap_file(gendb_dir + "/lineitem/l_quantity.bin",      sz_quantity);
    void* ptr_extprice  = mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", sz_extprice);

    if (!ptr_shipdate || !ptr_discount || !ptr_quantity || !ptr_extprice) return;

    const int32_t* col_shipdate  = reinterpret_cast<const int32_t*>(ptr_shipdate);
    const double*  col_discount  = reinterpret_cast<const double*>(ptr_discount);
    const double*  col_quantity  = reinterpret_cast<const double*>(ptr_quantity);
    const double*  col_extprice  = reinterpret_cast<const double*>(ptr_extprice);

    // Advise sequential access patterns for qualifying data
    // (blocks are contiguous since lineitem sorted by shipdate)
    if (num_qual_blocks > 0) {
        uint64_t first_row = block_row_offsets[0];
        uint64_t last_block_end = block_row_offsets.back() + block_row_counts.back();
        size_t scan_bytes_int32 = (last_block_end - first_row) * sizeof(int32_t);
        size_t scan_bytes_dbl   = (last_block_end - first_row) * sizeof(double);
        madvise((void*)(col_shipdate + first_row), scan_bytes_int32, MADV_SEQUENTIAL);
        madvise((void*)(col_discount + first_row), scan_bytes_dbl,   MADV_SEQUENTIAL);
        madvise((void*)(col_quantity + first_row), scan_bytes_dbl,   MADV_SEQUENTIAL);
        madvise((void*)(col_extprice + first_row), scan_bytes_dbl,   MADV_SEQUENTIAL);
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel scan over qualifying blocks
    // -------------------------------------------------------------------------
    double global_sum = 0.0;

    {
        GENDB_PHASE("main_scan");

        int nthreads = std::min(64, num_qual_blocks > 0 ? 64 : 1);
        omp_set_num_threads(nthreads);

        #pragma omp parallel reduction(+:global_sum)
        {
            double local_sum = 0.0;

            #pragma omp for schedule(dynamic, 1)
            for (int b = 0; b < num_qual_blocks; ++b) {
                uint64_t row_start = block_row_offsets[b];
                uint32_t nrows     = block_row_counts[b];

                const int32_t* sd = col_shipdate + row_start;
                const double*  di = col_discount  + row_start;
                const double*  qt = col_quantity   + row_start;
                const double*  ep = col_extprice   + row_start;

                double block_sum = 0.0;

                // Scalar loop — compiler will auto-vectorize with -O3 -march=native
                for (uint32_t i = 0; i < nrows; ++i) {
                    int32_t shipdate = sd[i];
                    double  discount = di[i];
                    double  quantity = qt[i];

                    if (shipdate >= SHIPDATE_LO && shipdate < SHIPDATE_HI &&
                        discount >= DISCOUNT_LO && discount <= DISCOUNT_HI &&
                        quantity < QUANTITY_MAX) {
                        block_sum += ep[i] * discount;
                    }
                }

                local_sum += block_sum;
            }

            global_sum += local_sum;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: cannot open output file %s\n", out_path.c_str());
            return;
        }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", global_sum);
        fclose(f);
    }

    munmap(ptr_shipdate, sz_shipdate);
    munmap(ptr_discount, sz_discount);
    munmap(ptr_quantity, sz_quantity);
    munmap(ptr_extprice, sz_extprice);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
