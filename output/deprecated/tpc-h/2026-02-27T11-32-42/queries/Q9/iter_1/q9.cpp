// Q9 iter_1 — Pre-joined columns: li_sup_nat, li_order_year, li_ps_cost
// No hash map lookups at query time — pure scan with direct column access.
// SELECT nation, o_year, SUM(amount) FROM profit GROUP BY nation,o_year ORDER BY nation,o_year DESC

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"

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
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: q9 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        size_t N, tmp;
        // Pre-joined columns
        int32_t* li_pkey       = mmap_col<int32_t>(gendb_dir, "li_partkey.bin",    N);
        double*  li_ep         = mmap_col<double> (gendb_dir, "li_extprice.bin",   tmp);
        int8_t*  li_disc       = mmap_col<int8_t> (gendb_dir, "li_discount.bin",   tmp);
        int8_t*  li_qty        = mmap_col<int8_t> (gendb_dir, "li_qty.bin",        tmp);
        double*  li_ps_cost    = mmap_col<double> (gendb_dir, "li_ps_cost.bin",    tmp);
        int8_t*  li_sup_nat    = mmap_col<int8_t> (gendb_dir, "li_sup_nat.bin",    tmp);
        int16_t* li_order_year = mmap_col<int16_t>(gendb_dir, "li_order_year.bin", tmp);
        // Part green filter
        size_t   pg_sz;
        uint8_t* part_green_arr = mmap_col<uint8_t>(gendb_dir, "part_green_arr.bin", pg_sz);
        // Nation names: 25 × 26 chars
        size_t   Nnat;
        char*    nat_names = (char*)mmap_col<char>(gendb_dir, "nat_names.bin", Nnat);

        const int NYEARS   = 20;
        const int NNATIONS = 25;

        int NT = min((int)thread::hardware_concurrency(), 64);
        vector<vector<double>> local_profit(NT, vector<double>(NNATIONS * NYEARS, 0.0));
        size_t chunk = (N + NT - 1) / NT;

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk;
            size_t end   = min(start + chunk, N);
            threads.emplace_back([&, t, start, end]() {
                auto& prof = local_profit[t];
                for (size_t i = start; i < end; ++i) {
                    int32_t pk = li_pkey[i];
                    if ((size_t)pk >= pg_sz || !part_green_arr[pk]) continue;
                    int8_t  nat  = li_sup_nat[i];
                    if (nat < 0 || nat >= NNATIONS) continue;
                    int16_t yr   = li_order_year[i];
                    int     yidx = yr - 1990;
                    if (yidx < 0 || yidx >= NYEARS) continue;
                    double ep   = li_ep[i];
                    double d    = li_disc[i] * 0.01;
                    double q    = (double)li_qty[i];
                    double sc   = li_ps_cost[i];
                    prof[nat * NYEARS + yidx] += ep * (1.0 - d) - sc * q;
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge
        vector<double> total_profit(NNATIONS * NYEARS, 0.0);
        for (auto& lp : local_profit)
            for (int i = 0; i < NNATIONS * NYEARS; ++i)
                total_profit[i] += lp[i];

        // Collect non-zero entries
        struct Row { const char* nation; int year; double profit; };
        vector<Row> rows;
        for (int n = 0; n < NNATIONS; ++n)
            for (int y = 0; y < NYEARS; ++y) {
                double p = total_profit[n * NYEARS + y];
                if (p != 0.0) rows.push_back({nat_names + n*26, 1990+y, p});
            }

        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
            int c = strcmp(a.nation, b.nation);
            return c != 0 ? c < 0 : a.year > b.year;
        });

        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q9.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "nation,o_year,sum_profit\n");
            for (auto& r : rows) fprintf(out, "%s,%d,%.4f\n", r.nation, r.year, r.profit);
            fclose(out);
        }
    }
    return 0;
}
