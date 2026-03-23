// Q1 iter_1: OpenMP parallel scan with thread-local aggregates
// Exact int64 arithmetic, 64-thread parallelism
// Usage: ./q1 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"
using namespace gendb;

// Cutoff: 1998-09-02 stored = 2436
static const uint16_t CUTOFF = 2436;

struct Agg {
    int64_t sum_qty=0, sum_base=0, sum_disc=0, sum_chg=0, sum_dc_pct=0, count=0;
};

static inline int gidx(uint8_t rf, uint8_t ls) {
    return ((rf=='A')?0:(rf=='N')?1:2)*2 + ((ls=='O')?1:0);
}

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");
    std::string g = argv[1]; std::string r = argv[2];

    MmapColumn<uint16_t> sd(g+"/lineitem/l_shipdate.bin");
    MmapColumn<uint8_t>  rf(g+"/lineitem/l_returnflag.bin");
    MmapColumn<uint8_t>  ls(g+"/lineitem/l_linestatus.bin");
    MmapColumn<uint8_t>  qt(g+"/lineitem/l_quantity.bin");
    MmapColumn<int32_t>  ep(g+"/lineitem/l_extprice.bin");
    MmapColumn<uint8_t>  dc(g+"/lineitem/l_discount.bin");
    MmapColumn<uint8_t>  tx(g+"/lineitem/l_tax.bin");
    mmap_prefetch_all(sd,rf,ls,qt,ep,dc,tx);

    size_t n=sd.count;
    // Binary search for cutoff
    { size_t lo=0, hi=n;
      while(lo<hi){size_t m=(lo+hi)>>1; if(sd.data[m]<=CUTOFF)lo=m+1;else hi=m;}
      n=lo; }

    const uint8_t *R=rf.data,*L=ls.data,*Q=qt.data,*D=dc.data,*T=tx.data;
    const int32_t *E=ep.data;

    const int NGROUPS = 6;
    int nth = omp_get_max_threads();
    std::vector<Agg> tagg(nth * NGROUPS);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        Agg* my = &tagg[tid * NGROUPS];

        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < n; i++) {
            int gi = gidx(R[i], L[i]);
            int64_t e=(int64_t)E[i], d=(int64_t)D[i], t=(int64_t)T[i];
            int64_t dp = e*(100-d);
            my[gi].sum_qty    += Q[i];
            my[gi].sum_base   += e;
            my[gi].sum_disc   += dp;
            my[gi].sum_chg    += dp*(100+t);
            my[gi].sum_dc_pct += d;
            my[gi].count++;
        }
    }

    // Reduce thread-local aggregates
    Agg agg[NGROUPS] = {};
    for (int t = 0; t < nth; t++) {
        for (int g = 0; g < NGROUPS; g++) {
            Agg& s = tagg[t*NGROUPS+g];
            agg[g].sum_qty    += s.sum_qty;
            agg[g].sum_base   += s.sum_base;
            agg[g].sum_disc   += s.sum_disc;
            agg[g].sum_chg    += s.sum_chg;
            agg[g].sum_dc_pct += s.sum_dc_pct;
            agg[g].count      += s.count;
        }
    }

    {
        GENDB_PHASE("output");
        FILE* f=fopen((r+"/Q1.csv").c_str(),"w");
        fprintf(f,"l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
        struct Row{const char*rf,*ls;int g;};
        Row rows[]={{"A","F",0},{"N","F",2},{"N","O",3},{"R","F",4}};
        for(auto& row:rows){
            Agg& a=agg[row.g]; if(!a.count)continue;
            double cnt=(double)a.count;
            double sqty=(double)a.sum_qty;
            double sbase=(double)a.sum_base/100.0;
            double sdisc=(double)a.sum_disc/10000.0;
            double schg=(double)a.sum_chg/1000000.0;
            fprintf(f,"%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                row.rf,row.ls,sqty,sbase,sdisc,schg,
                sqty/cnt,sbase/cnt,(double)a.sum_dc_pct/(100.0*cnt),(long)a.count);
        }
        fclose(f);
    }
    return 0;
}
