// Q1 tail — Pricing Summary Report
// Consumes: scan_lineitem_full (mmap'd columns)
// Operators: residual_filter → parallel_aggregate → finalize → sort (implicit) → output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <omp.h>

namespace mqo::tails {

void run_Q1(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q1_tail");

    // -- Get shared scan columns --
    const auto& cols = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const size_t n = cols.n_rows;

    // Pointers to needed columns
    const int32_t* __restrict__ shipdate      = cols.l_shipdate;
    const int8_t*  __restrict__ returnflag    = cols.l_returnflag;
    const int8_t*  __restrict__ linestatus    = cols.l_linestatus;
    const double*  __restrict__ quantity      = cols.l_quantity;
    const double*  __restrict__ extendedprice = cols.l_extendedprice;
    const double*  __restrict__ discount      = cols.l_discount;
    const double*  __restrict__ tax           = cols.l_tax;

    // Direct-array encoding: 6 groups (3 returnflag × 2 linestatus)
    // rf: A=65→0, N=78→1, R=82→2   ls: F=70→0, O=79→1
    // index = rf_idx * 2 + ls_idx
    // Accumulators per group: sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_discount, count
    static constexpr int N_GROUPS = 6;
    static constexpr int N_ACCS = 6;
    // Acc layout: [0]=sum_qty [1]=sum_base_price [2]=sum_disc_price [3]=sum_charge [4]=sum_discount [5]=count

    // Map ASCII code to index; use a small LUT
    int rf_map[256];
    int ls_map[256];
    std::memset(rf_map, -1, sizeof(rf_map));
    std::memset(ls_map, -1, sizeof(ls_map));
    rf_map[65] = 0;  // 'A'
    rf_map[78] = 1;  // 'N'
    rf_map[82] = 2;  // 'R'
    ls_map[70] = 0;  // 'F'
    ls_map[79] = 1;  // 'O'

    // Global accumulator
    double global_agg[N_GROUPS][N_ACCS];
    std::memset(global_agg, 0, sizeof(global_agg));

    {
        MQO_TIME_PHASE("Q1_aggregate");

        static constexpr int32_t SHIPDATE_THRESHOLD = 10471; // 1998-09-02 as epoch days

        #pragma omp parallel
        {
            // Thread-local accumulators (stack-allocated, 288 bytes — fits in L1)
            double local_agg[N_GROUPS][N_ACCS];
            std::memset(local_agg, 0, sizeof(local_agg));

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; ++i) {
                if (shipdate[i] <= SHIPDATE_THRESHOLD) {
                    int rf_idx = rf_map[(unsigned char)returnflag[i]];
                    int ls_idx = ls_map[(unsigned char)linestatus[i]];
                    int gidx = rf_idx * 2 + ls_idx;

                    double qty  = quantity[i];
                    double ep   = extendedprice[i];
                    double disc = discount[i];
                    double t    = tax[i];
                    double disc_price = ep * (1.0 - disc);

                    local_agg[gidx][0] += qty;
                    local_agg[gidx][1] += ep;
                    local_agg[gidx][2] += disc_price;
                    local_agg[gidx][3] += disc_price * (1.0 + t);
                    local_agg[gidx][4] += disc;
                    local_agg[gidx][5] += 1.0;
                }
            }

            // Merge into global
            #pragma omp critical
            {
                for (int g = 0; g < N_GROUPS; ++g) {
                    for (int a = 0; a < N_ACCS; ++a) {
                        global_agg[g][a] += local_agg[g][a];
                    }
                }
            }
        }
    }

    // -- Finalize and output --
    // Group order by index naturally gives ASC order: A/F, A/O, N/F, N/O, R/F, R/O
    // Map back: rf_idx {0→'A', 1→'N', 2→'R'}, ls_idx {0→'F', 1→'O'}
    static const char rf_chars[3] = {'A', 'N', 'R'};
    static const char ls_chars[2] = {'F', 'O'};

    {
        MQO_TIME_PHASE("Q1_output");

        std::string outpath = ctx.output_dir + "/q1.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");

        std::fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int g = 0; g < N_GROUPS; ++g) {
            double count = global_agg[g][5];
            if (count == 0.0) continue;  // skip empty groups

            int rf_idx = g / 2;
            int ls_idx = g % 2;

            double sum_qty        = global_agg[g][0];
            double sum_base_price = global_agg[g][1];
            double sum_disc_price = global_agg[g][2];
            double sum_charge     = global_agg[g][3];
            double avg_qty        = sum_qty / count;
            double avg_price      = sum_base_price / count;
            double avg_disc       = global_agg[g][4] / count;
            long long count_order = (long long)count;

            std::fprintf(fp, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lld\n",
                         rf_chars[rf_idx], ls_chars[ls_idx],
                         sum_qty, sum_base_price, sum_disc_price, sum_charge,
                         avg_qty, avg_price, avg_disc, count_order);
        }

        std::fclose(fp);
    }
}

}  // namespace mqo::tails
