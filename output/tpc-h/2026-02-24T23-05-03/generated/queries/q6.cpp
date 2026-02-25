#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

namespace {

// Zero-copy mmap helper
static const void* mmap_ro(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // C11

    // Date constants — C1, C7
    const int32_t lo = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t hi = gendb::add_years(lo, 1);  // 1995-01-01

    // Column files
    const std::string zm_file   = gendb_dir + "/lineitem/indexes/l_shipdate_zone_map.bin";
    const std::string sd_file   = gendb_dir + "/lineitem/l_shipdate.bin";
    const std::string disc_file = gendb_dir + "/lineitem/l_discount.bin";
    const std::string qty_file  = gendb_dir + "/lineitem/l_quantity.bin";
    const std::string ep_file   = gendb_dir + "/lineitem/l_extendedprice.bin";

    size_t zm_sz, sd_sz, disc_sz, qty_sz, ep_sz;
    const void*    zm_raw_v   = nullptr;
    const int32_t* sd         = nullptr;
    const double*  disc       = nullptr;
    const double*  qty        = nullptr;
    const double*  ep         = nullptr;

    {
        GENDB_PHASE("data_loading");

        // Load zone map
        zm_raw_v = mmap_ro(zm_file, zm_sz);

        // mmap columns
        sd   = reinterpret_cast<const int32_t*>(mmap_ro(sd_file,   sd_sz));
        disc = reinterpret_cast<const double*>  (mmap_ro(disc_file, disc_sz));
        qty  = reinterpret_cast<const double*>  (mmap_ro(qty_file,  qty_sz));
        ep   = reinterpret_cast<const double*>  (mmap_ro(ep_file,   ep_sz));

        // Zone-map selective madvise (P13): only hint sequential access for qualifying blocks.
        // Since 85% are skipped on HDD, we do not WILLNEED the entire columns.
        // Let OS handle prefetch naturally; zone-map guidance avoids most I/O.
        madvise((void*)sd,   sd_sz,   MADV_SEQUENTIAL);
        madvise((void*)disc, disc_sz, MADV_SEQUENTIAL);
        madvise((void*)qty,  qty_sz,  MADV_SEQUENTIAL);
        madvise((void*)ep,   ep_sz,   MADV_SEQUENTIAL);
    }

    // Parse zone map
    const uint32_t* zm_raw = reinterpret_cast<const uint32_t*>(zm_raw_v);
    uint32_t num_blocks = zm_raw[0];
    struct ZMBlock { int32_t mn, mx; uint32_t cnt; };
    const ZMBlock* blocks = reinterpret_cast<const ZMBlock*>(zm_raw + 1);

    long double revenue = 0.0L;

    {
        GENDB_PHASE("main_scan");

        // Morsel-driven OpenMP parallel reduction over blocks (C35: long double accumulator)
        #pragma omp parallel for schedule(dynamic) reduction(+:revenue)
        for (uint32_t b = 0; b < num_blocks; b++) {
            // Zone-map skip: C19
            if (blocks[b].mx < lo || blocks[b].mn >= hi) continue;

            size_t row_start = (size_t)b * 65536;
            size_t row_end   = row_start + blocks[b].cnt;

            long double local_rev = 0.0L;
            for (size_t i = row_start; i < row_end; i++) {
                // Filter order: most selective first (discount ~27.3%, quantity ~46%, shipdate ~15.4%)
                double d = disc[i];
                if (d < 0.05 || d > 0.07) continue;
                if (qty[i] >= 24.0) continue;
                if (sd[i] < lo || sd[i] >= hi) continue;
                // Accumulate — C35: use long double for derived expression
                local_rev += (long double)ep[i] * d;
            }
            revenue += local_rev;
        }
    }

    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2Lf\n", revenue);
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
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
