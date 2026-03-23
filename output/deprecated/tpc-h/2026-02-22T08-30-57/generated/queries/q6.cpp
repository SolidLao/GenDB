// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

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
#include <immintrin.h>
#include <filesystem>
#include <iostream>

#include "timing_utils.h"

namespace {

// --- Constants ---
static constexpr int32_t  DATE_LO   = 8766;   // 1994-01-01
static constexpr int32_t  DATE_HI   = 9131;   // 1995-01-01 (exclusive)
static constexpr double   DISC_LO   = 0.05;
static constexpr double   DISC_HI   = 0.07;
static constexpr double   QTY_HI    = 24.0;   // exclusive
static constexpr uint32_t BLOCK_SZ  = 100000;
static constexpr int      NUM_THREADS = 64;

// --- mmap helper ---
struct MmapFile {
    void*  ptr  = MAP_FAILED;
    size_t size = 0;
    int    fd   = -1;

    bool open(const std::string& path, bool populate = false) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        int flags = MAP_PRIVATE | (populate ? MAP_POPULATE : 0);
        ptr = mmap(nullptr, size, PROT_READ, flags, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); return false; }
        return true;
    }

    void advise_range(size_t byte_off, size_t byte_len) {
        // madvise on page-aligned range
        size_t pg   = (size_t)sysconf(_SC_PAGESIZE);
        size_t aoff = (byte_off / pg) * pg;
        size_t alen = byte_len + (byte_off - aoff);
        madvise((char*)ptr + aoff, alen, MADV_WILLNEED);
        madvise((char*)ptr + aoff, alen, MADV_SEQUENTIAL);
    }

    void close() {
        if (ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0)           ::close(fd);
    }

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
};

// --- Zone map helpers ---
// All zone maps use INTERLEAVED layout: [uint32_t num_blocks][min0,max0,min1,max1,...]
// shipdate: int32_t pairs; discount/quantity: double pairs

struct ShipdateZM {
    uint32_t        num_blocks;
    const int32_t*  pairs;  // interleaved: pairs[2*b]=min, pairs[2*b+1]=max
    int32_t block_min(uint32_t b) const { return pairs[2*b];   }
    int32_t block_max(uint32_t b) const { return pairs[2*b+1]; }
};

struct DoubleZM {
    uint32_t       num_blocks;
    const double*  pairs;  // interleaved: pairs[2*b]=min, pairs[2*b+1]=max
    double block_min(uint32_t b) const { return pairs[2*b];   }
    double block_max(uint32_t b) const { return pairs[2*b+1]; }
};

static ShipdateZM parse_shipdate_zm(const MmapFile& f) {
    ShipdateZM z;
    const uint8_t* p = (const uint8_t*)f.ptr;
    z.num_blocks = *reinterpret_cast<const uint32_t*>(p);
    p += 4;
    z.pairs = reinterpret_cast<const int32_t*>(p);
    return z;
}

static DoubleZM parse_double_zm(const MmapFile& f) {
    DoubleZM z;
    const uint8_t* p = (const uint8_t*)f.ptr;
    z.num_blocks = *reinterpret_cast<const uint32_t*>(p);
    p += 4;
    z.pairs = reinterpret_cast<const double*>(p);
    return z;
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Data loading ----
    MmapFile f_date, f_disc, f_qty, f_ep;
    MmapFile f_zm_date, f_zm_disc, f_zm_qty;

    {
        GENDB_PHASE("data_loading");

        f_zm_date.open(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", true);
        f_zm_disc.open(gendb_dir + "/indexes/lineitem_discount_zonemap.bin",  true);
        f_zm_qty .open(gendb_dir + "/indexes/lineitem_quantity_zonemap.bin",  true);

        f_date.open(gendb_dir + "/lineitem/l_shipdate.bin");
        f_disc.open(gendb_dir + "/lineitem/l_discount.bin");
        f_qty .open(gendb_dir + "/lineitem/l_quantity.bin");
        f_ep  .open(gendb_dir + "/lineitem/l_extendedprice.bin");
    }

    // ---- Zone map pruning: find qualifying block range ----
    ShipdateZM zm_date = parse_shipdate_zm(f_zm_date);
    DoubleZM   zm_disc = parse_double_zm  (f_zm_disc);
    DoubleZM   zm_qty  = parse_double_zm  (f_zm_qty);

    uint32_t nb = zm_date.num_blocks;

    // Binary search: first block where block_max >= DATE_LO
    uint32_t first_block = nb;  // default: none
    {
        uint32_t lo = 0, hi = nb;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (zm_date.block_max(mid) >= DATE_LO) { first_block = mid; hi = mid; }
            else lo = mid + 1;
        }
    }

    // Binary search: last block where block_min < DATE_HI
    uint32_t last_block = 0;  // default: none
    {
        uint32_t lo = 0, hi = nb;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (zm_date.block_min(mid) < DATE_HI) { last_block = mid; lo = mid + 1; }
            else hi = mid;
        }
    }

    // Collect candidate blocks (date range passes + discount/qty zone maps)
    std::vector<uint32_t> candidate_blocks;
    candidate_blocks.reserve(128);

    if (first_block <= last_block) {
        for (uint32_t b = first_block; b <= last_block; ++b) {
            // Discount zone map skip
            if (zm_disc.block_max(b) < DISC_LO || zm_disc.block_min(b) > DISC_HI) continue;
            // Quantity zone map skip
            if (zm_qty.block_min(b) >= QTY_HI) continue;
            candidate_blocks.push_back(b);
        }
    }

    // Selective madvise on qualifying block byte ranges
    {
        size_t row_bytes_date = sizeof(int32_t);
        size_t row_bytes_dbl  = sizeof(double);
        for (uint32_t b : candidate_blocks) {
            uint64_t row_start = (uint64_t)b * BLOCK_SZ;
            uint64_t row_end   = std::min((uint64_t)(b + 1) * BLOCK_SZ,
                                          f_date.size / sizeof(int32_t));
            uint64_t cnt = row_end - row_start;

            f_date.advise_range(row_start * row_bytes_date, cnt * row_bytes_date);
            f_disc.advise_range(row_start * row_bytes_dbl,  cnt * row_bytes_dbl);
            f_qty .advise_range(row_start * row_bytes_dbl,  cnt * row_bytes_dbl);
            f_ep  .advise_range(row_start * row_bytes_dbl,  cnt * row_bytes_dbl);
        }
    }

    // ---- Main scan ----
    double revenue = 0.0;

    const int32_t* col_date = f_date.as<int32_t>();
    const double*  col_disc = f_disc.as<double>();
    const double*  col_qty  = f_qty .as<double>();
    const double*  col_ep   = f_ep  .as<double>();

    uint64_t total_rows = f_date.size / sizeof(int32_t);

    {
        GENDB_PHASE("main_scan");

        int ncb = (int)candidate_blocks.size();

        #pragma omp parallel for schedule(dynamic,1) reduction(+:revenue) num_threads(NUM_THREADS)
        for (int ci = 0; ci < ncb; ++ci) {
            uint32_t b = candidate_blocks[ci];
            uint64_t row_start = (uint64_t)b * BLOCK_SZ;
            uint64_t row_end   = std::min(row_start + BLOCK_SZ, total_rows);
            [[maybe_unused]] uint64_t row_cnt = row_end - row_start;

            // Determine if this is an interior block (date predicate always true)
            bool interior = (zm_date.block_min(b) >= DATE_LO && zm_date.block_max(b) < DATE_HI);

            double local_sum = 0.0;

            if (interior) {
                // Skip per-row date check
                for (uint64_t r = row_start; r < row_end; ++r) {
                    double disc = col_disc[r];
                    double qty  = col_qty[r];
                    if (disc >= DISC_LO && disc <= DISC_HI && qty < QTY_HI) {
                        local_sum += col_ep[r] * disc;
                    }
                }
            } else {
                // Boundary block: full predicate check
                for (uint64_t r = row_start; r < row_end; ++r) {
                    int32_t sd   = col_date[r];
                    double  disc = col_disc[r];
                    double  qty  = col_qty[r];
                    if (sd >= DATE_LO && sd < DATE_HI &&
                        disc >= DISC_LO && disc <= DISC_HI &&
                        qty < QTY_HI) {
                        local_sum += col_ep[r] * disc;
                    }
                }
            }

            revenue += local_sum;
        }
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q6.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); return; }
        fprintf(fout, "revenue\n");
        fprintf(fout, "%.2f\n", revenue);
        fclose(fout);
    }

    f_date.close(); f_disc.close(); f_qty.close(); f_ep.close();
    f_zm_date.close(); f_zm_disc.close(); f_zm_qty.close();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
