// Q6: Forecasting Revenue Change
// Filter lineitem by shipdate range, discount range, quantity < 24; sum revenue
// Usage: ./q6 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <algorithm>
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

// Date range: [1994-01-01, 1995-01-01)
// epoch: 1994-01-01=8766, 1995-01-01=9131
// stored: 8766-8035=731, 9131-8035=1096
static const uint16_t SD_LO = 731;
static const uint16_t SD_HI = 1096;

// discount BETWEEN 0.05 AND 0.07 -> stored [5, 7]
// quantity < 24 -> stored < 24

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: q6 <gendb> <results>\n"); return 1; }
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
        size_t lo = 0, hi = n;
        while (lo < hi) { size_t m = (lo+hi)>>1; if (sd[m] < SD_LO) lo=m+1; else hi=m; }
        lo_idx = lo;
    }
    {
        size_t lo = 0, hi = n;
        while (lo < hi) { size_t m = (lo+hi)>>1; if (sd[m] < SD_HI) lo=m+1; else hi=m; }
        hi_idx = lo;
    }

    double revenue = 0.0;
    for (size_t i = lo_idx; i < hi_idx; i++) {
        uint8_t d = dc[i];
        if (d >= 5 && d <= 7 && qt[i] < 24) {
            revenue += (double)ep[i] * d;
        }
    }
    revenue *= (1.0 / 10000.0); // cents * pct/100 / 100 = actual dollars

    {
        GENDB_PHASE("output");
        std::string out = rdir + "/Q6.csv";
        FILE* f = fopen(out.c_str(), "w");
        fprintf(f, "revenue\n%.4f\n", revenue);
        fclose(f);
    }
    return 0;
}
