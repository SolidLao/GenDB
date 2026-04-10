// Q5 tail: Local Supplier Volume
// Shared inputs: scan_lineitem_full, hash_orders_by_orderkey,
//                hash_customer_by_custkey, hash_supplier_by_suppkey
// Tail ops: precompute dim filter (region/nation), fused probe pipeline,
//           direct-array aggregation by nationkey, sort, output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_customer_by_custkey.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo::tails {

void run_Q5(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q5_tail");

    // Date constants: 1994-01-01 .. 1995-01-01
    static constexpr int32_t DATE_LO = mqo::io::to_epoch_days(1994, 1, 1);
    static constexpr int32_t DATE_HI = mqo::io::to_epoch_days(1995, 1, 1);

    // ---------------------------------------------------------------
    // Step 0: Precompute dimension filter — ASIA nation bitset + name lookup
    // ---------------------------------------------------------------
    bool asia_nation[25] = {};
    std::string nation_names[25];

    {
        MQO_TIME_PHASE("Q5_dim_filter");
        const std::string rb = ctx.gendb_dir + "/region/";
        const size_t nr = mqo::io::read_row_count(rb + "meta.txt");
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rb + "r_regionkey.bin", nr);
        // r_name is dict-encoded: r_name.bin = uint8 codes, r_name_dict.bin = dictionary
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(rb + "r_name.bin", nr);
        auto r_dict = mqo::io::read_dictionary(rb + "r_name_dict.bin");

        // Find ASIA regionkey
        int32_t asia_rk = -1;
        for (size_t i = 0; i < nr; ++i) {
            if (r_dict[r_name_codes[i]] == "ASIA") {
                asia_rk = r_regionkey[i];
                break;
            }
        }

        // Scan nation, filter by asia regionkey
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t nn = mqo::io::read_row_count(nb + "meta.txt");
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", nn);
        const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(nb + "n_regionkey.bin", nn);
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", nn);
        auto n_dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");

        for (size_t i = 0; i < nn; ++i) {
            if (n_regionkey[i] == asia_rk) {
                int32_t nk = n_nationkey[i];
                asia_nation[nk] = true;
                nation_names[nk] = n_dict[n_name_codes[i]];
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 1+2: Fused probe pipeline + aggregation
    // ---------------------------------------------------------------
    // Use thread-local aggregation arrays
    const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const auto& ht_ord = mqo::shared::hash_orders_by_orderkey::get();
    const auto& ht_sup = mqo::shared::hash_supplier_by_suppkey::get();
    const auto& ht_cus = mqo::shared::hash_customer_by_custkey::get();

    const size_t n_rows = li.n_rows;
    const int32_t* l_orderkey      = li.l_orderkey;
    const int32_t* l_suppkey       = li.l_suppkey;
    const double*  l_extendedprice = li.l_extendedprice;
    const double*  l_discount      = li.l_discount;

    // Global aggregation array (only 25 entries, merge is trivial)
    double revenue[25] = {};

    {
        MQO_TIME_PHASE("Q5_main_scan");

        #pragma omp parallel
        {
            double local_rev[25] = {};

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_rows; ++i) {
                // Probe orders
                const int32_t ok = l_orderkey[i];
                const auto* oe = ht_ord.probe(ok);
                if (!oe) continue;

                // Filter orderdate [1994-01-01, 1995-01-01)
                const int32_t od = oe->o_orderdate;
                if (od < DATE_LO || od >= DATE_HI) continue;

                // Probe supplier
                const int32_t sk = l_suppkey[i];
                const auto* se = ht_sup.probe(sk);
                if (!se) continue;

                // Filter ASIA nation
                const int32_t s_nk = se->s_nationkey;
                if (s_nk < 0 || s_nk >= 25 || !asia_nation[s_nk]) continue;

                // Probe customer
                const int32_t ck = oe->o_custkey;
                const auto* ce = ht_cus.probe(ck);
                if (!ce) continue;

                // Filter local supplier: c_nationkey == s_nationkey
                if (ce->c_nationkey != s_nk) continue;

                // Accumulate revenue
                local_rev[s_nk] += l_extendedprice[i] * (1.0 - l_discount[i]);
            }

            // Merge thread-local into global
            #pragma omp critical
            {
                for (int k = 0; k < 25; ++k) {
                    revenue[k] += local_rev[k];
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Finalize — collect non-zero, sort by revenue DESC, output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q5_output");

        struct Result {
            std::string name;
            double rev;
        };
        std::vector<Result> results;
        results.reserve(5);
        for (int k = 0; k < 25; ++k) {
            if (asia_nation[k] && revenue[k] != 0.0) {
                results.push_back({nation_names[k], revenue[k]});
            }
        }
        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.rev > b.rev; });

        std::string path = ctx.output_dir + "/q5.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", path.c_str());
            std::exit(1);
        }
        std::fprintf(f, "n_name,revenue\n");
        for (const auto& r : results) {
            std::fprintf(f, "%s,%.2f\n", r.name.c_str(), r.rev);
        }
        std::fclose(f);
    }
}

}  // namespace mqo::tails
