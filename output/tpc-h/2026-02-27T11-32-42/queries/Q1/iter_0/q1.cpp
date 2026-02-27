// Q1 — Pricing Summary Report
// SELECT l_returnflag, l_linestatus,
//   SUM(l_quantity), SUM(l_extendedprice),
//   SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
//   AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
// FROM lineitem WHERE l_shipdate <= '1998-09-02'
// GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus

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
#include "date_utils.h"

using namespace std;

// Memory-map a binary file, return pointer + element count
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

struct Agg {
    double sum_qty       = 0.0;
    double sum_price     = 0.0;
    double sum_disc_price= 0.0;
    double sum_charge    = 0.0;
    double sum_disc      = 0.0;
    int64_t count        = 0;
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: q1 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        // Load columns
        size_t N;
        size_t Ns, Nrf, Nls, Nq, Np, Nd, Nt;
        int32_t* shipdate   = mmap_col<int32_t>(gendb_dir, "li_shipdate.bin",  Ns);
        uint8_t* rflag      = mmap_col<uint8_t>(gendb_dir, "li_rflag.bin",     Nrf);
        uint8_t* lstatus    = mmap_col<uint8_t>(gendb_dir, "li_lstatus.bin",   Nls);
        int8_t*  qty        = mmap_col<int8_t> (gendb_dir, "li_qty.bin",       Nq);
        double*  extprice   = mmap_col<double> (gendb_dir, "li_extprice.bin",  Np);
        int8_t*  discount   = mmap_col<int8_t> (gendb_dir, "li_discount.bin",  Nd);
        int8_t*  tax        = mmap_col<int8_t> (gendb_dir, "li_tax.bin",       Nt);
        N = Ns;

        // Cutoff: '1998-12-01' - 90 days = '1998-09-02'
        int32_t cutoff = gendb::date_str_to_epoch_days("1998-12-01");
        cutoff -= 90;

        // Binary search: find upper bound (first index where shipdate > cutoff)
        // lineitem is sorted by shipdate
        size_t hi = 0, lo = 0, mx = N;
        {
            size_t l = 0, r = N;
            while (l < r) {
                size_t m = (l+r)/2;
                if (shipdate[m] <= cutoff) l = m+1; else r = m;
            }
            hi = l; // all rows [0, hi) have shipdate <= cutoff
        }

        // 4 groups: (returnflag, linestatus) → just use index rflag*2+lstatus
        // Known values: 'A','N','R' for rflag; 'F','O' for lstatus
        // Use a 256*256 lookup (sparse) or encode to 0-3
        // We'll use a small map: rflag∈{65='A',78='N',82='R'}, lstatus∈{70='F',79='O'}
        // Process in parallel
        int NT = min((int)thread::hardware_concurrency(), 64);
        // Local aggregation per thread per group (max 6 groups to be safe)
        struct GroupAgg {
            uint8_t rflag, lstatus;
            Agg agg;
        };
        vector<array<GroupAgg,6>> local_aggs(NT);
        for (auto& la : local_aggs)
            for (auto& g : la) { g.rflag=0; g.lstatus=0; }

        size_t chunk = (hi + NT - 1) / NT;
        vector<thread> threads;
        threads.reserve(NT);

        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk;
            size_t end   = min(start + chunk, hi);
            threads.emplace_back([&, t, start, end]() {
                // local group accumulator: key = rflag<<8|lstatus
                uint32_t key_map[65536] = {};  // rflag<<8|lstatus -> group index (1-based)
                Agg groups[6];
                uint8_t keys_rf[6], keys_ls[6];
                int ngroups = 0;

                for (size_t i = start; i < end; ++i) {
                    uint32_t key = ((uint32_t)rflag[i] << 8) | lstatus[i];
                    int gid = key_map[key];
                    if (__builtin_expect(gid == 0, 0)) {
                        gid = ++ngroups;
                        key_map[key] = gid;
                        keys_rf[gid-1] = rflag[i];
                        keys_ls[gid-1] = lstatus[i];
                    }
                    --gid;
                    double q  = (double)qty[i];
                    double ep = extprice[i];
                    double d  = discount[i] * 0.01;
                    double tx = tax[i] * 0.01;
                    groups[gid].sum_qty        += q;
                    groups[gid].sum_price      += ep;
                    groups[gid].sum_disc_price += ep * (1.0 - d);
                    groups[gid].sum_charge     += ep * (1.0 - d) * (1.0 + tx);
                    groups[gid].sum_disc       += d;
                    groups[gid].count          += 1;
                }
                for (int g = 0; g < ngroups; ++g) {
                    local_aggs[t][g].rflag   = keys_rf[g];
                    local_aggs[t][g].lstatus = keys_ls[g];
                    local_aggs[t][g].agg     = groups[g];
                }
                // zero out unused
                for (int g = ngroups; g < 6; ++g)
                    local_aggs[t][g].rflag = local_aggs[t][g].lstatus = 0;
            });
        }
        for (auto& th : threads) th.join();

        // Merge local aggregations
        struct MKey { uint8_t rf, ls; };
        Agg final_aggs[6];
        MKey final_keys[6];
        int nfinal = 0;
        // Use simple array search (at most 4 groups)
        for (int t = 0; t < NT; ++t) {
            for (int g = 0; g < 6; ++g) {
                if (local_aggs[t][g].rflag == 0 && local_aggs[t][g].lstatus == 0 && g > 0) continue;
                if (local_aggs[t][g].agg.count == 0) continue;
                uint8_t rf = local_aggs[t][g].rflag;
                uint8_t ls = local_aggs[t][g].lstatus;
                // find in final
                int fid = -1;
                for (int f = 0; f < nfinal; ++f)
                    if (final_keys[f].rf == rf && final_keys[f].ls == ls) { fid = f; break; }
                if (fid < 0) {
                    fid = nfinal++;
                    final_keys[fid] = {rf, ls};
                    final_aggs[fid] = Agg{};
                }
                final_aggs[fid].sum_qty        += local_aggs[t][g].agg.sum_qty;
                final_aggs[fid].sum_price      += local_aggs[t][g].agg.sum_price;
                final_aggs[fid].sum_disc_price += local_aggs[t][g].agg.sum_disc_price;
                final_aggs[fid].sum_charge     += local_aggs[t][g].agg.sum_charge;
                final_aggs[fid].sum_disc       += local_aggs[t][g].agg.sum_disc;
                final_aggs[fid].count          += local_aggs[t][g].agg.count;
            }
        }

        // Sort by (rflag, lstatus)
        int order[6]; for (int i=0;i<nfinal;++i) order[i]=i;
        sort(order, order+nfinal, [&](int a, int b){
            if (final_keys[a].rf != final_keys[b].rf) return final_keys[a].rf < final_keys[b].rf;
            return final_keys[a].ls < final_keys[b].ls;
        });

        // Write output
        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q1.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
            for (int i = 0; i < nfinal; ++i) {
                int idx = order[i];
                const Agg& a = final_aggs[idx];
                double cnt = (double)a.count;
                fprintf(out, "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%lld\n",
                        (char)final_keys[idx].rf,
                        (char)final_keys[idx].ls,
                        a.sum_qty,
                        a.sum_price,
                        a.sum_disc_price,
                        a.sum_charge,
                        a.sum_qty / cnt,
                        a.sum_price / cnt,
                        a.sum_disc / cnt,
                        (long long)a.count);
            }
            fclose(out);
        }
    }
    return 0;
}
