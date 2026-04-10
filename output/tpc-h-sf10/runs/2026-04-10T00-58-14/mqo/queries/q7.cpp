// Q7 tail — Volume Shipping
// Consumes: scan_lineitem_shipdate_1995_1996, hash_supplier_by_suppkey,
//           hash_orders_by_orderkey, hash_customer_by_custkey
// Operators: nation lookup, probe+filter (supplier→orders→customer), aggregate, sort, output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_shipdate_1995_1996.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <algorithm>

namespace mqo::tails {

void run_Q7(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q7_tail");

    // -----------------------------------------------------------------------
    // Step 0: Load nation lookup — find FRANCE and GERMANY nationkeys
    // -----------------------------------------------------------------------
    int32_t france_key = -1, germany_key = -1;
    std::string nation_names[25];
    {
        MQO_TIME_PHASE("Q7_load_nation");
        const std::string ndir = ctx.gendb_dir + "/nation/";
        const size_t n_nations = mqo::io::read_row_count(ndir + "meta.txt");
        const int32_t* nk = mqo::io::mmap_column<int32_t>(ndir + "n_nationkey.bin", n_nations);

        // n_name is dict-encoded: n_name.bin (uint8 codes), n_name_dict.bin (dictionary)
        auto dict = mqo::io::read_dictionary(ndir + "n_name_dict.bin");
        const uint8_t* name_codes = mqo::io::mmap_column<uint8_t>(ndir + "n_name.bin", n_nations);

        for (size_t i = 0; i < n_nations; ++i) {
            nation_names[nk[i]] = dict[name_codes[i]];
            if (dict[name_codes[i]] == "FRANCE")  france_key = nk[i];
            if (dict[name_codes[i]] == "GERMANY") germany_key = nk[i];
        }
    }

    // -----------------------------------------------------------------------
    // Step 1-3: Fused probe → filter → aggregate
    // Aggregation: 2 nation pairs × 2 years = 4 buckets
    //   pair 0 = (FRANCE, GERMANY), pair 1 = (GERMANY, FRANCE)
    //   year_offset = l_year - 1995
    //   index = pair * 2 + year_offset
    // -----------------------------------------------------------------------
    const auto& scan = mqo::shared::scan_lineitem_shipdate_1995_1996::get();
    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
    const auto& ord  = mqo::shared::hash_orders_by_orderkey::get();
    const auto& cust = mqo::shared::hash_customer_by_custkey::get();

    const size_t n_rows = scan.n_rows;

    // Date boundaries for year extraction
    static constexpr int32_t DATE_1996 = mqo::io::to_epoch_days(1996, 1, 1); // first day of 1996

    // Thread-local accumulators, merged after
    double global_acc[4] = {0.0, 0.0, 0.0, 0.0};

    {
        MQO_TIME_PHASE("Q7_main_scan");

        #pragma omp parallel
        {
            double local_acc[4] = {0.0, 0.0, 0.0, 0.0};

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_rows; ++i) {
                const int32_t l_suppkey = scan.l_suppkey[i];

                // Probe supplier (L3-resident, ~5.5MB)
                const auto* se = supp.probe(l_suppkey);
                if (!se) continue;
                const int32_t s_nk = se->s_nationkey;

                // Early filter: supplier nation must be FRANCE or GERMANY
                if (s_nk != france_key && s_nk != germany_key) continue;

                // Probe orders
                const int32_t l_orderkey = scan.l_orderkey[i];
                const auto* oe = ord.probe(l_orderkey);
                if (!oe) continue;

                // Probe customer
                const int32_t o_custkey = oe->o_custkey;
                const auto* ce = cust.probe(o_custkey);
                if (!ce) continue;
                const int32_t c_nk = ce->c_nationkey;

                // Early filter: customer nation must be FRANCE or GERMANY
                if (c_nk != france_key && c_nk != germany_key) continue;

                // Cross-filter: nations must be different
                if (s_nk == c_nk) continue;

                // Compute aggregation key
                // pair 0 = (FRANCE→supp, GERMANY→cust), pair 1 = (GERMANY→supp, FRANCE→cust)
                const int pair_id = (s_nk == france_key) ? 0 : 1;
                const int year_offset = (scan.l_shipdate[i] < DATE_1996) ? 0 : 1;
                const int idx = pair_id * 2 + year_offset;

                // Compute volume
                const double volume = scan.l_extendedprice[i] * (1.0 - scan.l_discount[i]);
                local_acc[idx] += volume;
            }

            // Merge
            #pragma omp critical
            {
                for (int k = 0; k < 4; ++k) global_acc[k] += local_acc[k];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Output — already in sorted order by construction
    // pair 0 = (FRANCE, GERMANY): years 1995, 1996
    // pair 1 = (GERMANY, FRANCE): years 1995, 1996
    // This is lexicographic order: FRANCE < GERMANY
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q7_output");
        const std::string out_path = ctx.output_dir + "/q7.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "supp_nation,cust_nation,l_year,revenue\n");

        // Output non-zero groups in sorted order
        const char* pair_supp[] = {"FRANCE", "GERMANY"};
        const char* pair_cust[] = {"GERMANY", "FRANCE"};
        const int years[] = {1995, 1996};

        for (int p = 0; p < 2; ++p) {
            for (int y = 0; y < 2; ++y) {
                int idx = p * 2 + y;
                if (global_acc[idx] != 0.0) {
                    std::fprintf(fp, "%s,%s,%d,%.2f\n",
                                 pair_supp[p], pair_cust[p], years[y], global_acc[idx]);
                }
            }
        }

        std::fclose(fp);
    }
}

}  // namespace mqo::tails
