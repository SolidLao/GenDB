// Q14 tail — Promotion Effect
// Shared inputs: scan_lineitem_full, hash_part_by_partkey
// Operators: residual date filter → hash probe (part) → scalar aggregate → finalize

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/hash_part_by_partkey.hpp"

#include <cstdio>
#include <cstring>
#include <string>

namespace mqo { namespace tails {

void run_Q14(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q14_tail");

    // -- Fetch shared inputs --
    const auto& li  = mqo::shared::scan_lineitem_full::get();
    const auto& pt  = mqo::shared::hash_part_by_partkey::get();

    // -- Load p_type dictionary and pre-compute PROMO% mask --
    bool is_promo[256] = {};
    {
        MQO_TIME_PHASE("Q14_load_dict");
        mqo::io::Dictionary dict;
        dict.load(ctx.gendb_dir + "/part/p_type_dict.bin");
        for (uint32_t i = 0; i < dict.count && i < 256; ++i) {
            if (dict.entries[i].size() >= 5 &&
                std::memcmp(dict.entries[i].data(), "PROMO", 5) == 0) {
                is_promo[i] = true;
            }
        }
    }

    // -- Date filter constants (days since epoch 1970-01-01) --
    constexpr int32_t DATE_LO = 9374;  // 1995-09-01
    constexpr int32_t DATE_HI = 9404;  // 1995-10-01 (exclusive)

    const size_t n = li.n_rows;
    const int32_t* __restrict__ shipdate = li.l_shipdate;
    const int32_t* __restrict__ partkey  = li.l_partkey;
    const double*  __restrict__ price    = li.l_extendedprice;
    const double*  __restrict__ disc     = li.l_discount;
    const int32_t* __restrict__ pk_idx   = pt.pk_index;
    const uint8_t* __restrict__ ptype    = pt.p_type;

    double promo_sum = 0.0;
    double total_sum = 0.0;

    {
        MQO_TIME_PHASE("Q14_main_scan");

        #pragma omp parallel for schedule(static) reduction(+:promo_sum,total_sum)
        for (size_t i = 0; i < n; ++i) {
            int32_t sd = shipdate[i];
            if (sd < DATE_LO || sd >= DATE_HI) continue;

            double revenue = price[i] * (1.0 - disc[i]);
            total_sum += revenue;

            int32_t pk = partkey[i];
            int32_t row_id = pk_idx[pk];
            if (row_id >= 0 && is_promo[ptype[row_id]]) {
                promo_sum += revenue;
            }
        }
    }

    // -- Output --
    {
        MQO_TIME_PHASE("Q14_output");
        double promo_revenue = (total_sum != 0.0) ? 100.0 * promo_sum / total_sum : 0.0;

        std::string path = ctx.output_dir + "/q14.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(f, "promo_revenue\n");
        std::fprintf(f, "%.2f\n", promo_revenue);
        std::fclose(f);
    }
}

}} // namespace mqo::tails
