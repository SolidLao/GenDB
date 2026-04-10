// Q1 tail — Pricing Summary Report
// Consumes: scan_lineitem_full (shared)
// Operators: residual_filter (l_shipdate <= 19980902), parallel_aggregate (6 groups),
//            finalize_averages, sort_and_project (implicit order from group index)

#include "mqo_profile.hpp"
#include "shared/scan_lineitem_full.hpp"

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>

namespace mqo { namespace tails {

void run_Q1(const Context& ctx) {
    MQO_TIME_TAIL("Q1_tail");

    // Fetch shared scan
    const auto& cols = mqo::shared::scan_lineitem_full::get();
    const size_t n = cols.n_rows;

    // Date threshold: 1998-09-02 as days since 1970-01-01
    // 1998-09-02 = days_since_epoch calculated:
    // 1970-01-01 = 0, 1998-09-02 = 10471
    // But let's compute correctly: years 1970-1997 = 28 years
    // 28*365 + 7 leap days (72,76,80,84,88,92,96) = 10227
    // Jan=31, Feb=28, Mar=31, Apr=30, May=31, Jun=30, Jul=31, Aug=31 = 243
    // + 2 days in Sep = 245
    // Total = 10227 + 245 = 10472? Let me just use a known reference.
    // 1998-12-01 = 10561 (well-known). 10561 - 90 = 10471.
    // Actually: from 1970-01-01.
    // Let me compute: 1970 to 1998 = 28 years = 28*365 = 10220 + leap days
    // Leap years between 1970-1997: 1972,1976,1980,1984,1988,1992,1996 = 7
    // So Jan 1 1998 = 10227
    // 1998: Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30 = 334 days to Dec 1
    // So 1998-12-01 = 10227 + 334 = 10561
    // 10561 - 90 = 10471
    constexpr int32_t SHIPDATE_THRESHOLD = 10471; // 1998-09-02

    // Aggregation struct per group (6 groups: 3 returnflag x 2 linestatus)
    struct AggBucket {
        double sum_qty        = 0.0;
        double sum_base_price = 0.0;
        double sum_disc_price = 0.0;
        double sum_charge     = 0.0;
        double sum_discount   = 0.0;
        int64_t count         = 0;
    };

    // Group key mapping:
    //   l_returnflag: 'A'=65 -> idx 0, 'N'=78 -> idx 1, 'R'=82 -> idx 2
    //   l_linestatus: 'F'=70 -> idx 0, 'O'=79 -> idx 1
    // group_index = returnflag_idx * 2 + linestatus_idx

    // Build lookup tables for char -> index
    // returnflag is stored as int8 (char)
    int rf_map[256];
    int ls_map[256];
    std::memset(rf_map, -1, sizeof(rf_map));
    std::memset(ls_map, -1, sizeof(ls_map));
    rf_map[(unsigned char)'A'] = 0;
    rf_map[(unsigned char)'N'] = 1;
    rf_map[(unsigned char)'R'] = 2;
    ls_map[(unsigned char)'F'] = 0;
    ls_map[(unsigned char)'O'] = 1;

    // Global aggregation array
    AggBucket global[6] = {};

    {
        MQO_TIME_PHASE("Q1_aggregate");

        // Use OpenMP for parallel aggregation with thread-local buckets
        #pragma omp parallel
        {
            AggBucket local[6] = {};

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; ++i) {
                if (cols.l_shipdate[i] > SHIPDATE_THRESHOLD) continue;

                int rf_idx = rf_map[(unsigned char)cols.l_returnflag[i]];
                int ls_idx = ls_map[(unsigned char)cols.l_linestatus[i]];
                if (rf_idx < 0 || ls_idx < 0) continue;

                int gidx = rf_idx * 2 + ls_idx;
                double qty   = cols.l_quantity[i];
                double price = cols.l_extendedprice[i];
                double disc  = cols.l_discount[i];
                double tax   = cols.l_tax[i];
                double disc_price = price * (1.0 - disc);

                local[gidx].sum_qty        += qty;
                local[gidx].sum_base_price += price;
                local[gidx].sum_disc_price += disc_price;
                local[gidx].sum_charge     += disc_price * (1.0 + tax);
                local[gidx].sum_discount   += disc;
                local[gidx].count          += 1;
            }

            // Merge into global
            #pragma omp critical
            {
                for (int g = 0; g < 6; ++g) {
                    global[g].sum_qty        += local[g].sum_qty;
                    global[g].sum_base_price += local[g].sum_base_price;
                    global[g].sum_disc_price += local[g].sum_disc_price;
                    global[g].sum_charge     += local[g].sum_charge;
                    global[g].sum_discount   += local[g].sum_discount;
                    global[g].count          += local[g].count;
                }
            }
        }
    }

    // Output in sorted order (group index already gives ASC returnflag, ASC linestatus)
    {
        MQO_TIME_PHASE("Q1_output");

        static const char* rf_chars[] = {"A", "N", "R"};
        static const char* ls_chars[] = {"F", "O"};

        std::string outpath = ctx.output_dir + "/q1.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) return;

        std::fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int rf = 0; rf < 3; ++rf) {
            for (int ls = 0; ls < 2; ++ls) {
                int gidx = rf * 2 + ls;
                const auto& b = global[gidx];
                if (b.count == 0) continue;

                double avg_qty   = b.sum_qty        / (double)b.count;
                double avg_price = b.sum_base_price  / (double)b.count;
                double avg_disc  = b.sum_discount    / (double)b.count;

                std::fprintf(fp, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                    rf_chars[rf], ls_chars[ls],
                    b.sum_qty, b.sum_base_price, b.sum_disc_price, b.sum_charge,
                    avg_qty, avg_price, avg_disc, (long)b.count);
            }
        }

        std::fclose(fp);
    }
}

}} // namespace mqo::tails
