// Q15 tail — Top Supplier
// Consumes: scan_lineitem_shipdate_1995_1996, hash_supplier_by_suppkey
// Steps: residual date filter → aggregate by l_suppkey (direct array) →
//        find max revenue → filter matching → probe supplier hash → sort → output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_shipdate_1995_1996.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo::tails {

void run_Q15(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q15_tail");

    // Residual date filter constants: 1996-01-01 <= l_shipdate < 1996-04-01
    static constexpr int32_t DATE_LO = mqo::io::to_epoch_days(1996, 1, 1);
    static constexpr int32_t DATE_HI = mqo::io::to_epoch_days(1996, 4, 1);  // exclusive

    // Fetch shared inputs
    const auto& scan = mqo::shared::scan_lineitem_shipdate_1995_1996::get();
    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();

    // --- Step 1+2: Residual filter + aggregate SUM(l_extendedprice * (1-l_discount))
    //               GROUP BY l_suppkey using direct array (suppkey 1..100000)
    static constexpr int32_t ARRAY_SIZE = 100001;

    // Revenue array — zero-initialized
    auto* revenue = static_cast<double*>(std::calloc(ARRAY_SIZE, sizeof(double)));
    // Track which suppkeys have data (for sparse iteration)
    auto* has_data = static_cast<uint8_t*>(std::calloc(ARRAY_SIZE, sizeof(uint8_t)));

    {
        MQO_TIME_PHASE("Q15_filter_agg");
        const size_t n = scan.n_rows;
        const int32_t* __restrict__ sk = scan.l_suppkey;
        const double*  __restrict__ ep = scan.l_extendedprice;
        const double*  __restrict__ dc = scan.l_discount;
        const int32_t* __restrict__ sd = scan.l_shipdate;

        for (size_t i = 0; i < n; ++i) {
            const int32_t d = sd[i];
            if (d >= DATE_LO && d < DATE_HI) {
                const int32_t key = sk[i];
                revenue[key] += ep[i] * (1.0 - dc[i]);
                has_data[key] = 1;
            }
        }
    }

    // --- Step 3: Find MAX(total_revenue)
    double max_revenue = 0.0;
    {
        MQO_TIME_PHASE("Q15_find_max");
        for (int32_t k = 1; k < ARRAY_SIZE; ++k) {
            if (has_data[k] && revenue[k] > max_revenue) {
                max_revenue = revenue[k];
            }
        }
    }

    // --- Step 4+5: Filter matching suppliers + probe supplier hash
    struct Result {
        int32_t s_suppkey;
        int32_t row_id;  // for varlen access
        double  total_revenue;
    };
    std::vector<Result> results;

    {
        MQO_TIME_PHASE("Q15_filter_probe");
        for (int32_t k = 1; k < ARRAY_SIZE; ++k) {
            if (has_data[k] && revenue[k] == max_revenue) {
                const auto* e = supp.probe(k);
                if (e) {
                    results.push_back({e->s_suppkey, e->row_id, revenue[k]});
                }
            }
        }
    }

    // --- Step 6: Sort by s_suppkey ASC (typically 1 row, trivial)
    std::sort(results.begin(), results.end(),
              [](const Result& a, const Result& b) { return a.s_suppkey < b.s_suppkey; });

    // --- Step 7: Output CSV
    {
        MQO_TIME_PHASE("Q15_output");
        std::string path = ctx.output_dir + "/q15.csv";
        FILE* fp = std::fopen(path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q15] Cannot open %s\n", path.c_str());
            std::free(revenue);
            std::free(has_data);
            return;
        }

        std::fprintf(fp, "s_suppkey,s_name,s_address,s_phone,total_revenue\n");
        for (const auto& r : results) {
            auto name = supp.get_name(r.row_id);
            auto addr = supp.get_address(r.row_id);
            auto phone = supp.get_phone(r.row_id);
            std::fprintf(fp, "%d,%.*s,%.*s,%.*s,%.2f\n",
                         r.s_suppkey,
                         static_cast<int>(name.size()), name.data(),
                         static_cast<int>(addr.size()), addr.data(),
                         static_cast<int>(phone.size()), phone.data(),
                         r.total_revenue);
        }
        std::fclose(fp);
    }

    std::free(revenue);
    std::free(has_data);
}

}  // namespace mqo::tails
