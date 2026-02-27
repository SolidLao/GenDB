// Q6 — Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

#include <cstdio>
#include <cstdlib>
#include <vector>
#include <thread>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"
#include "date_utils.h"

using namespace std;

template<typename T>
static T* mmap_col(const char* dir, const char* name, size_t& count) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return (T*)p;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr,"Usage: q6 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        size_t N;
        size_t Nd, Nq, Np;
        int32_t* shipdate = mmap_col<int32_t>(gendb_dir, "li_shipdate.bin", N);
        int8_t*  discount = mmap_col<int8_t> (gendb_dir, "li_discount.bin", Nd);
        int8_t*  qty      = mmap_col<int8_t> (gendb_dir, "li_qty.bin",      Nq);
        double*  extprice = mmap_col<double> (gendb_dir, "li_extprice.bin", Np);

        // Shipdate range: [1994-01-01, 1995-01-01)
        int32_t d_lo = gendb::date_str_to_epoch_days("1994-01-01");
        int32_t d_hi = gendb::date_str_to_epoch_days("1995-01-01");

        // Binary search for [lo_idx, hi_idx)
        size_t lo_idx, hi_idx;
        {
            size_t l=0, r=N;
            while (l<r) { size_t m=(l+r)/2; if (shipdate[m]<d_lo) l=m+1; else r=m; }
            lo_idx=l;
        }
        {
            size_t l=0, r=N;
            while (l<r) { size_t m=(l+r)/2; if (shipdate[m]<d_hi) l=m+1; else r=m; }
            hi_idx=l;
        }

        // Discount: BETWEEN 0.05 AND 0.07 → int8 5,6,7
        // Quantity: < 24 → int8 < 24

        int NT = min((int)thread::hardware_concurrency(), 64);
        vector<double> partial(NT, 0.0);
        size_t chunk = (hi_idx - lo_idx + NT - 1) / NT;

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = lo_idx + t * chunk;
            size_t end   = min(start + chunk, hi_idx);
            threads.emplace_back([&, t, start, end]() {
                double sum = 0.0;
                for (size_t i = start; i < end; ++i) {
                    int8_t d = discount[i];
                    int8_t q = qty[i];
                    if (d >= 5 && d <= 7 && q < 24) {
                        sum += extprice[i] * (d * 0.01);
                    }
                }
                partial[t] = sum;
            });
        }
        for (auto& th : threads) th.join();

        double revenue = 0.0;
        for (double v : partial) revenue += v;

        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q6.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "revenue\n%.4f\n", revenue);
            fclose(out);
        }
    }
    return 0;
}
