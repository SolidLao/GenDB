// Q6 iter_1: OpenMP parallel scan with int64 reduction
// Usage: ./q6 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"
using namespace gendb;

// Date range [1994-01-01, 1995-01-01): stored [731, 1096)
static const uint16_t SD_LO = 731;
static const uint16_t SD_HI = 1096;

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");

    std::string gendb = argv[1];
    std::string rdir  = argv[2];

    MmapColumn<uint16_t> shipdate(gendb + "/lineitem/l_shipdate.bin");
    MmapColumn<int32_t>  extprice(gendb + "/lineitem/l_extprice.bin");
    MmapColumn<uint8_t>  discount(gendb + "/lineitem/l_discount.bin");
    MmapColumn<uint8_t>  quantity(gendb + "/lineitem/l_quantity.bin");
    mmap_prefetch_all(shipdate, extprice, discount, quantity);

    const size_t n = shipdate.count;
    const uint16_t* sd = shipdate.data;
    const int32_t*  ep = extprice.data;
    const uint8_t*  dc = discount.data;
    const uint8_t*  qt = quantity.data;

    // Binary search for [SD_LO, SD_HI)
    size_t lo_idx = 0, hi_idx = n;
    {
        size_t lo=0, hi=n;
        while(lo<hi){size_t m=(lo+hi)>>1; if(sd[m]<SD_LO)lo=m+1; else hi=m;}
        lo_idx=lo;
    }
    {
        size_t lo=0, hi=n;
        while(lo<hi){size_t m=(lo+hi)>>1; if(sd[m]<SD_HI)lo=m+1; else hi=m;}
        hi_idx=lo;
    }

    // Parallel int64 reduction for exact arithmetic
    int64_t revenue_cents = 0;
    #pragma omp parallel for reduction(+:revenue_cents) schedule(static)
    for (size_t i = lo_idx; i < hi_idx; i++) {
        uint8_t d = dc[i];
        if (d >= 5 && d <= 7 && qt[i] < 24)
            revenue_cents += (int64_t)ep[i] * d;
    }

    double revenue = (double)revenue_cents * (1.0 / 10000.0);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((rdir+"/Q6.csv").c_str(), "w");
        fprintf(f, "revenue\n%.4f\n", revenue);
        fclose(f);
    }
    return 0;
}
