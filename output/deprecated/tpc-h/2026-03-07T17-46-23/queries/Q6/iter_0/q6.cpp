// Q6: Forecasting Revenue Change — Zonemap + parallel block scan + scalar sum
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "timing_utils.h"
#include "mmap_utils.h"

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE_MS("total", total_ms);

    // Constants
    constexpr int32_t DATE_LO = 8766;   // 1994-01-01
    constexpr int32_t DATE_HI = 9131;   // 1995-01-01
    constexpr double DISC_LO = 0.05;
    constexpr double DISC_HI = 0.07;
    constexpr double QTY_THR = 24.0;

    // --- Phase 1: Load zonemap and identify qualifying blocks ---
    struct ZoneEntry8 { int32_t min_date; int32_t max_date; };
    uint32_t num_blocks = 0, block_size = 0;
    std::vector<ZoneEntry8> zones;
    std::vector<uint32_t> qual_blocks;
    {
        GENDB_PHASE("data_loading");
        FILE* zf = fopen((gendb + "/indexes/lineitem_shipdate_zonemap.bin").c_str(), "rb");
        fread(&num_blocks, 4, 1, zf);
        fread(&block_size, 4, 1, zf);
        zones.resize(num_blocks);
        fread(zones.data(), sizeof(ZoneEntry8), num_blocks, zf);
        fclose(zf);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_date < DATE_LO || zones[b].min_date >= DATE_HI) continue;
            qual_blocks.push_back(b);
        }
    }

    // --- Phase 2: mmap columns ---
    gendb::MmapColumn<int32_t> col_shipdate(gendb + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<double>  col_discount(gendb + "/lineitem/l_discount.bin");
    gendb::MmapColumn<double>  col_quantity(gendb + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<double>  col_extprice(gendb + "/lineitem/l_extendedprice.bin");
    size_t nrows = col_shipdate.count;

    // --- Phase 3: Parallel scan of qualifying blocks ---
    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel reduction(+:revenue)
        {
            #pragma omp for schedule(dynamic, 1)
            for (size_t qi = 0; qi < qual_blocks.size(); qi++) {
                uint32_t b = qual_blocks[qi];
                size_t start = (size_t)b * block_size;
                size_t end   = std::min(start + block_size, nrows);
                double local_sum = 0.0;

                const int32_t* sd = col_shipdate.data + start;
                const double* disc = col_discount.data + start;
                const double* qty = col_quantity.data + start;
                const double* ep = col_extprice.data + start;
                size_t cnt = end - start;

                for (size_t i = 0; i < cnt; i++) {
                    if (sd[i] >= DATE_LO && sd[i] < DATE_HI &&
                        disc[i] >= DISC_LO && disc[i] <= DISC_HI &&
                        qty[i] < QTY_THR) {
                        local_sum += ep[i] * disc[i];
                    }
                }
                revenue += local_sum;
            }
        }
    }

    // --- Phase 4: Output ---
    {
        GENDB_PHASE("output");
        std::string outpath = results + "/Q6.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "revenue\n%.4f\n", revenue);
        fclose(f);
    }

    return 0;
}
