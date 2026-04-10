// Q6 tail — Forecasting Revenue Change
// Consumes: scan_lineitem_shipdate_1994 (shipdate already filtered to [1994-01-01, 1995-01-01))
// Residual filters: l_discount BETWEEN 0.05 AND 0.07, l_quantity < 24
// Aggregate: SUM(l_extendedprice * l_discount) → revenue

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_shipdate_1994.hpp"

#include <cstdio>
#include <string>

namespace mqo::tails {

void run_Q6(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q6_tail");

    const auto& scan = mqo::shared::scan_lineitem_shipdate_1994::get();
    const size_t n = scan.n_rows;

    const double* __restrict__ qty   = scan.l_quantity;
    const double* __restrict__ price = scan.l_extendedprice;
    const double* __restrict__ disc  = scan.l_discount;

    // Scalar aggregate with Kahan summation for numerical stability
    double sum = 0.0;
    double comp = 0.0;  // Kahan compensation

    {
        MQO_TIME_PHASE("Q6_filter_agg");

        for (size_t i = 0; i < n; ++i) {
            const double d = disc[i];
            if (d >= 0.05 && d <= 0.07 && qty[i] < 24.0) {
                double y = price[i] * d - comp;
                double t = sum + y;
                comp = (t - sum) - y;
                sum = t;
            }
        }
    }

    // Output
    {
        MQO_TIME_PHASE("Q6_output");
        std::string path = ctx.output_dir + "/q6.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", sum);
        std::fclose(f);
    }
}

}  // namespace mqo::tails
