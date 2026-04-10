// Q14 tail — Promotion Effect
// Consumes: scan_lineitem_shipdate_1995_1996 (shared)
// Local scan: part (p_partkey, p_type dict codes)
// Operators: build_direct_array, residual_filter, fused_join_aggregate, finalize

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_shipdate_1995_1996.hpp"

#include <bitset>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace mqo::tails {

void run_Q14(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q14_tail");

    // ---- Date constants (epoch days) ----
    static constexpr int32_t DATE_LO = mqo::io::to_epoch_days(1995, 9, 1);   // >= 1995-09-01
    static constexpr int32_t DATE_HI = mqo::io::to_epoch_days(1995, 10, 1);  // <  1995-10-01

    // ---- Step 1: Build direct array for p_type and promo bitset ----
    uint8_t* type_array = nullptr;
    std::bitset<256> promo_bits;  // 150 entries, 256 for alignment
    {
        MQO_TIME_PHASE("Q14_build_direct_array");

        const std::string part_dir = ctx.gendb_dir + "/part";
        const size_t n_parts = 2000000;

        // Load p_partkey (int32) and p_type (uint8 dict codes)
        const int32_t* p_partkey = mqo::io::mmap_column<int32_t>(part_dir + "/p_partkey.bin", n_parts);
        const uint8_t* p_type   = mqo::io::mmap_column<uint8_t>(part_dir + "/p_type.bin", n_parts);

        // Allocate direct array indexed by p_partkey [0..2000000]
        type_array = static_cast<uint8_t*>(std::calloc(2000001, sizeof(uint8_t)));

        for (size_t i = 0; i < n_parts; ++i) {
            type_array[p_partkey[i]] = p_type[i];
        }

        // Pre-computed promo dict codes from plan
        static constexpr int promo_codes[] = {
            0, 5, 7, 15, 20, 26, 27, 39, 54, 56, 62, 63, 66, 75, 78,
            79, 82, 96, 113, 118, 123, 131, 132, 134, 145
        };
        for (int c : promo_codes) {
            promo_bits.set(c);
        }
    }

    // ---- Steps 2+3: Residual filter + fused join aggregate ----
    double promo_sum = 0.0;
    double total_sum = 0.0;
    {
        MQO_TIME_PHASE("Q14_scan_aggregate");

        const auto& scan = mqo::shared::scan_lineitem_shipdate_1995_1996::get();
        const size_t n = scan.n_rows;

        const int32_t* __restrict__ shipdate = scan.l_shipdate;
        const int32_t* __restrict__ partkey  = scan.l_partkey;
        const double*  __restrict__ extprice = scan.l_extendedprice;
        const double*  __restrict__ discount = scan.l_discount;

        #pragma omp parallel reduction(+:promo_sum, total_sum)
        {
            #pragma omp for schedule(static)
            for (size_t i = 0; i < n; ++i) {
                const int32_t sd = shipdate[i];
                if (sd >= DATE_LO && sd < DATE_HI) {
                    const double revenue = extprice[i] * (1.0 - discount[i]);
                    total_sum += revenue;
                    if (promo_bits.test(type_array[partkey[i]])) {
                        promo_sum += revenue;
                    }
                }
            }
        }
    }

    // ---- Step 4: Finalize and output ----
    {
        MQO_TIME_PHASE("Q14_output");

        const double promo_revenue = 100.0 * promo_sum / total_sum;

        std::string outpath = ctx.output_dir + "/q14.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q14] Cannot open output: %s\n", outpath.c_str());
            std::exit(1);
        }
        std::fprintf(fp, "promo_revenue\n");
        std::fprintf(fp, "%.2f\n", promo_revenue);
        std::fclose(fp);
    }

    std::free(type_array);
}

}  // namespace mqo::tails
