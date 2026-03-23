// Q6: Forecasting Revenue Change — Best version: pre-warm + block scan + _exit
#include <cstdio>
#include <cstdint>
#include <cstdlib>
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

    constexpr int32_t DATE_LO = 8766;
    constexpr int32_t DATE_HI = 9131;
    constexpr double DISC_LO = 0.05;
    constexpr double DISC_HI = 0.07;
    constexpr double QTY_THR = 24.0;

    // Pre-warm
    #pragma omp parallel
    { volatile int x = omp_get_thread_num(); (void)x; }

    gendb::MmapColumn<int32_t> col_shipdate(gendb + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<double>  col_discount(gendb + "/lineitem/l_discount.bin");
    gendb::MmapColumn<double>  col_quantity(gendb + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<double>  col_extprice(gendb + "/lineitem/l_extendedprice.bin");
    const size_t nrows = col_shipdate.count;

    std::string outpath = results + "/Q6.csv";
    { std::string cmd = "mkdir -p " + results; system(cmd.c_str()); }

    {
        GENDB_PHASE_MS("total", total_ms);

        struct ZoneEntry8 { int32_t min_date; int32_t max_date; };
        uint32_t num_blocks = 0, block_size = 0;
        std::vector<uint32_t> qual_blocks;
        size_t n_full = 0;
        {
            GENDB_PHASE("data_loading");
            FILE* zf = fopen((gendb + "/indexes/lineitem_shipdate_zonemap.bin").c_str(), "rb");
            fread(&num_blocks, 4, 1, zf);
            fread(&block_size, 4, 1, zf);
            std::vector<ZoneEntry8> zones(num_blocks);
            fread(zones.data(), sizeof(ZoneEntry8), num_blocks, zf);
            fclose(zf);

            std::vector<uint32_t> partial;
            for (uint32_t b = 0; b < num_blocks; b++) {
                if (zones[b].max_date < DATE_LO || zones[b].min_date >= DATE_HI) continue;
                if (zones[b].min_date >= DATE_LO && zones[b].max_date < DATE_HI) {
                    qual_blocks.push_back(b);
                } else {
                    partial.push_back(b);
                }
            }
            n_full = qual_blocks.size();
            for (auto b : partial) qual_blocks.push_back(b);
        }

        double revenue = 0.0;
        {
            GENDB_PHASE("main_scan");
            const size_t nqual = qual_blocks.size();

            #pragma omp parallel reduction(+:revenue)
            {
                #pragma omp for schedule(dynamic, 1)
                for (size_t qi = 0; qi < nqual; qi++) {
                    uint32_t b = qual_blocks[qi];
                    size_t start = (size_t)b * block_size;
                    size_t end   = std::min(start + block_size, nrows);
                    size_t cnt   = end - start;

                    const int32_t* __restrict__ sd   = col_shipdate.data + start;
                    const double*  __restrict__ disc = col_discount.data + start;
                    const double*  __restrict__ qty  = col_quantity.data + start;
                    const double*  __restrict__ ep   = col_extprice.data + start;

                    double local_sum = 0.0;
                    if (qi < n_full) {
                        for (size_t i = 0; i < cnt; i++) {
                            if (disc[i] >= DISC_LO && disc[i] <= DISC_HI && qty[i] < QTY_THR) {
                                local_sum += ep[i] * disc[i];
                            }
                        }
                    } else {
                        for (size_t i = 0; i < cnt; i++) {
                            if (sd[i] >= DATE_LO && sd[i] < DATE_HI &&
                                disc[i] >= DISC_LO && disc[i] <= DISC_HI &&
                                qty[i] < QTY_THR) {
                                local_sum += ep[i] * disc[i];
                            }
                        }
                    }
                    revenue += local_sum;
                }
            }
        }

        {
            GENDB_PHASE("output");
            FILE* f = fopen(outpath.c_str(), "w");
            fprintf(f, "revenue\n%.4f\n", revenue);
            fclose(f);
        }
    }

    fflush(stdout);
    fflush(stderr);
    _exit(0);
}
