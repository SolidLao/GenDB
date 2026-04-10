// Q6 tail: Forecasting Revenue Change
// Consumes: scan_lineitem_full (shared mmap'd lineitem columns)
// Operators: residual_filter → scalar_aggregate → project
// Output: single row with revenue = SUM(l_extendedprice * l_discount)

#include "mqo_profile.hpp"
#include "shared/scan_lineitem_full.hpp"

#include <cstdio>
#include <string>

namespace mqo { namespace tails {

void run_Q6(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q6_tail");

    const auto& cols = mqo::shared::scan_lineitem_full::get();
    const size_t n = cols.n_rows;

    // Pre-compute date boundaries (days since 1970-01-01)
    // 1994-01-01 = 8766,  1995-01-01 = 9131
    constexpr int32_t DATE_LO = 8766;
    constexpr int32_t DATE_HI = 9131;
    constexpr double  DISC_LO = 0.05;
    constexpr double  DISC_HI = 0.07;
    constexpr double  QTY_HI  = 24.0;

    const int32_t* __restrict__ shipdate = cols.l_shipdate;
    const double*  __restrict__ discount = cols.l_discount;
    const double*  __restrict__ quantity = cols.l_quantity;
    const double*  __restrict__ extprice = cols.l_extendedprice;

    double revenue = 0.0;

    {
        MQO_TIME_PHASE("Q6_filter_agg");

        for (size_t i = 0; i < n; ++i) {
            int32_t sd = shipdate[i];
            if (sd >= DATE_LO && sd < DATE_HI) {
                double d = discount[i];
                if (d >= DISC_LO && d <= DISC_HI) {
                    if (quantity[i] < QTY_HI) {
                        revenue += extprice[i] * d;
                    }
                }
            }
        }
    }

    // Write output
    {
        MQO_TIME_PHASE("Q6_output");
        std::string path = ctx.output_dir + "/q6.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", revenue);
        std::fclose(f);
    }
}

}} // namespace mqo::tails
