// Q7 tail — Volume Shipping query
// Shared inputs: scan_lineitem_full, scan_orders_full, hash_supplier_by_suppkey, hash_customer_by_custkey
// Tail ops: load nation, build nation-filtered suppkey/custkey arrays,
//           scan lineitem with date+suppkey+custkey+cross-nation filters,
//           aggregate into 4-element array, output CSV.

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"
#include "../shared/hash_customer_by_custkey.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q7(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q7_tail");

    // ---- Constants ----
    // FRANCE nationkey=6, GERMANY nationkey=7 (verified from nation table)
    static constexpr int32_t FRANCE_NK = 6;
    static constexpr int32_t GERMANY_NK = 7;
    // l_shipdate range: 1995-01-01 to 1996-12-31 as days since 1970-01-01
    static constexpr int32_t DATE_LO = 9131;   // 1995-01-01
    static constexpr int32_t DATE_HI = 9861;   // 1996-12-31
    static constexpr int32_t YEAR_BASE = 1995;

    // ---- Fetch shared data ----
    const auto& li  = mqo::shared::scan_lineitem_full::get();
    const auto& ord = mqo::shared::scan_orders_full::get();
    const auto& sup = mqo::shared::hash_supplier_by_suppkey::get();
    const auto& cus = mqo::shared::hash_customer_by_custkey::get();

    // ---- Step 0: Build orderkey → custkey direct array from orders ----
    // orders_pk_index maps orderkey → row_id. We build custkey_by_ok[orderkey] directly.
    int32_t max_orderkey = 0;
    {
        MQO_TIME_PHASE("Q7_build_ok_custkey");
        // Find max orderkey from last row (ordered by orderkey)
        if (ord.n_rows > 0) {
            max_orderkey = ord.o_orderkey[ord.n_rows - 1];
        }
        // We'll just use orders pk_index + o_custkey for lookups inline
    }

    // ---- Step 1: Build suppkey → nationkey direct array (only FRANCE/GERMANY) ----
    // suppkey range [1..100000], use direct array of int8_t, -1 = not matched
    static constexpr int32_t SUPP_MAX = 100001;
    std::vector<int8_t> supp_nk(SUPP_MAX, -1);
    {
        MQO_TIME_PHASE("Q7_build_supp_filter");
        for (size_t i = 0; i < sup.n_rows; ++i) {
            int32_t nk = sup.s_nationkey[i];
            if (nk == FRANCE_NK || nk == GERMANY_NK) {
                int32_t sk = sup.s_suppkey[i];
                if (sk >= 0 && sk < SUPP_MAX) {
                    supp_nk[sk] = static_cast<int8_t>(nk);
                }
            }
        }
    }

    // ---- Step 2: Build custkey → nationkey direct array (only FRANCE/GERMANY) ----
    static constexpr int32_t CUST_MAX = 1500001;
    std::vector<int8_t> cust_nk(CUST_MAX, -1);
    {
        MQO_TIME_PHASE("Q7_build_cust_filter");
        for (size_t i = 0; i < cus.n_rows; ++i) {
            int32_t nk = cus.c_nationkey[i];
            if (nk == FRANCE_NK || nk == GERMANY_NK) {
                int32_t ck = cus.c_custkey[i];
                if (ck >= 0 && ck < CUST_MAX) {
                    cust_nk[ck] = static_cast<int8_t>(nk);
                }
            }
        }
    }

    // ---- Step 3: Build orderkey → custkey direct lookup ----
    // Using orders pk_index for O(1) lookup during scan
    // orders_pk_index: orderkey → row_id. Pre-build custkey_by_orderkey for cache efficiency.
    // max orderkey = 60000000, so 60M+1 entries × 4B = ~240MB. Acceptable.
    static constexpr int32_t OK_MAX = 60000001;
    std::vector<int32_t> ok_to_ck(OK_MAX, -1);
    {
        MQO_TIME_PHASE("Q7_build_ok_to_ck");
        for (size_t i = 0; i < ord.n_rows; ++i) {
            int32_t ok = ord.o_orderkey[i];
            if (ok >= 0 && ok < OK_MAX) {
                ok_to_ck[ok] = ord.o_custkey[i];
            }
        }
    }

    // ---- Steps 3-9: Main scan with fused filters + aggregation ----
    // Aggregation: 4 groups indexed as:
    //   0 = (FRANCE, GERMANY, 1995)
    //   1 = (FRANCE, GERMANY, 1996)
    //   2 = (GERMANY, FRANCE, 1995)
    //   3 = (GERMANY, FRANCE, 1996)
    // Index = nation_pair_idx * 2 + (year - 1995)
    //   nation_pair_idx: supp=FRANCE(6),cust=GERMANY(7) → 0; supp=GERMANY(7),cust=FRANCE(6) → 1

    double agg[4] = {0.0, 0.0, 0.0, 0.0};

    {
        MQO_TIME_PHASE("Q7_main_scan");

        const size_t n = li.n_rows;
        const int32_t* __restrict__ shipdate = li.l_shipdate;
        const int32_t* __restrict__ suppkey  = li.l_suppkey;
        const int32_t* __restrict__ orderkey = li.l_orderkey;
        const double*  __restrict__ extprice = li.l_extendedprice;
        const double*  __restrict__ discount = li.l_discount;
        const int8_t*  __restrict__ supp_arr = supp_nk.data();
        const int8_t*  __restrict__ cust_arr = cust_nk.data();
        const int32_t* __restrict__ ok2ck    = ok_to_ck.data();

        // Helper to extract year from days-since-epoch
        // For the narrow range [9131..9861], year is 1995 or 1996.
        // 1996-01-01 = 9131 + 365 = 9496
        static constexpr int32_t DATE_1996 = 9496;

        for (size_t i = 0; i < n; ++i) {
            int32_t sd = shipdate[i];
            if (sd < DATE_LO || sd > DATE_HI) continue;

            int32_t sk = suppkey[i];
            if (sk < 0 || sk >= SUPP_MAX) continue;
            int8_t s_nk = supp_arr[sk];
            if (s_nk < 0) continue;

            int32_t ok = orderkey[i];
            if (ok < 0 || ok >= OK_MAX) continue;
            int32_t ck = ok2ck[ok];
            if (ck < 0 || ck >= CUST_MAX) continue;
            int8_t c_nk = cust_arr[ck];
            if (c_nk < 0) continue;

            // Cross-nation filter: must be (FRANCE,GERMANY) or (GERMANY,FRANCE)
            if (s_nk == c_nk) continue;  // same nation → reject (both are F or G)

            double volume = extprice[i] * (1.0 - discount[i]);

            // nation_pair_idx: FRANCE-supp,GERMANY-cust=0; GERMANY-supp,FRANCE-cust=1
            int pair_idx = (s_nk == GERMANY_NK) ? 1 : 0;
            int year_idx = (sd >= DATE_1996) ? 1 : 0;
            agg[pair_idx * 2 + year_idx] += volume;
        }
    }

    // ---- Step 10: Output (already in correct sort order) ----
    {
        MQO_TIME_PHASE("Q7_output");
        std::string outpath = ctx.output_dir + "/q7.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "supp_nation,cust_nation,l_year,revenue\n");

        // Group order: FRANCE<GERMANY lexicographically
        // idx 0: FRANCE, GERMANY, 1995
        // idx 1: FRANCE, GERMANY, 1996
        // idx 2: GERMANY, FRANCE, 1995
        // idx 3: GERMANY, FRANCE, 1996
        static const char* names[4][2] = {
            {"FRANCE", "GERMANY"},
            {"FRANCE", "GERMANY"},
            {"GERMANY", "FRANCE"},
            {"GERMANY", "FRANCE"}
        };
        static const int years[4] = {1995, 1996, 1995, 1996};

        for (int g = 0; g < 4; ++g) {
            if (agg[g] != 0.0) {  // Only output groups with data
                std::fprintf(f, "%s,%s,%d,%.2f\n", names[g][0], names[g][1], years[g], agg[g]);
            }
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
