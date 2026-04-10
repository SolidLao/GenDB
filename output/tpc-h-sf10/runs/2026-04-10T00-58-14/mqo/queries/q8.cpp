// Q8 tail: National Market Share
// Shared inputs: scan_lineitem_full, hash_orders_by_orderkey,
//                hash_customer_by_custkey, hash_supplier_by_suppkey
// Operators: build dim lookups (part filter, america bitset, nation lookup),
//            probe chain on lineitem, direct-array aggregation, project+output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_customer_by_custkey.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <bitset>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_set>

namespace mqo::tails {

void run_Q8(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q8_tail");

    // ---------------------------------------------------------------
    // Date constants
    // ---------------------------------------------------------------
    static constexpr int32_t DATE_1995_01_01 = mqo::io::to_epoch_days(1995, 1, 1);
    static constexpr int32_t DATE_1996_12_31 = mqo::io::to_epoch_days(1996, 12, 31);

    // ---------------------------------------------------------------
    // Step 0a: Build part filter set (p_type = 'ECONOMY ANODIZED STEEL')
    // ---------------------------------------------------------------
    std::unordered_set<int32_t> part_filter_set;
    {
        MQO_TIME_PHASE("Q8_part_filter");
        const std::string pb = ctx.gendb_dir + "/part/";
        const size_t pn = mqo::io::read_row_count(pb + "meta.txt");

        // Read p_type dictionary to find code for 'ECONOMY ANODIZED STEEL'
        auto p_type_dict = mqo::io::read_dictionary(pb + "p_type_dict.bin");
        uint8_t target_code = 255;
        for (uint32_t i = 0; i < p_type_dict.size(); i++) {
            if (p_type_dict[i] == "ECONOMY ANODIZED STEEL") {
                target_code = static_cast<uint8_t>(i);
                break;
            }
        }

        if (target_code != 255) {
            const uint8_t* p_type = mqo::io::mmap_column<uint8_t>(pb + "p_type.bin", pn);
            const int32_t* p_partkey = mqo::io::mmap_column<int32_t>(pb + "p_partkey.bin", pn);
            part_filter_set.reserve(16384);
            for (size_t i = 0; i < pn; i++) {
                if (p_type[i] == target_code) {
                    part_filter_set.insert(p_partkey[i]);
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 0b: Build america nation bitset + nation name lookup
    // ---------------------------------------------------------------
    std::bitset<32> america_nations;
    // nation_name dict codes indexed by nationkey
    uint8_t nation_name_code[25] = {};
    uint8_t brazil_dict_code = 255;
    {
        MQO_TIME_PHASE("Q8_dim_lookups");
        const std::string rb = ctx.gendb_dir + "/region/";
        const size_t rn = mqo::io::read_row_count(rb + "meta.txt");

        // Find region key for 'AMERICA'
        auto r_name_dict = mqo::io::read_dictionary(rb + "r_name_dict.bin");
        uint8_t america_code = 255;
        for (uint32_t i = 0; i < r_name_dict.size(); i++) {
            if (r_name_dict[i] == "AMERICA") {
                america_code = static_cast<uint8_t>(i);
                break;
            }
        }

        const uint8_t* r_name = mqo::io::mmap_column<uint8_t>(rb + "r_name.bin", rn);
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rb + "r_regionkey.bin", rn);
        int32_t america_regionkey = -1;
        for (size_t i = 0; i < rn; i++) {
            if (r_name[i] == america_code) {
                america_regionkey = r_regionkey[i];
                break;
            }
        }

        // Nation: find nations in AMERICA region
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t nn = mqo::io::read_row_count(nb + "meta.txt");
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", nn);
        const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(nb + "n_regionkey.bin", nn);
        const uint8_t* n_name = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", nn);

        // Nation name dictionary - find BRAZIL code
        auto n_name_dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");
        for (uint32_t i = 0; i < n_name_dict.size(); i++) {
            if (n_name_dict[i] == "BRAZIL") {
                brazil_dict_code = static_cast<uint8_t>(i);
                break;
            }
        }

        for (size_t i = 0; i < nn; i++) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < 25) {
                nation_name_code[nk] = n_name[i];
            }
            if (n_regionkey[i] == america_regionkey) {
                if (nk >= 0 && nk < 32) {
                    america_nations.set(static_cast<size_t>(nk));
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 1-3: Scan lineitem, probe chain, aggregate
    // ---------------------------------------------------------------
    // Direct array aggregation: index 0 = year 1995, index 1 = year 1996
    struct AggBucket {
        double brazil_volume = 0.0;
        double total_volume = 0.0;
    };

    // Get shared components
    const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const auto& orders_ht = mqo::shared::hash_orders_by_orderkey::get();
    const auto& cust_ht = mqo::shared::hash_customer_by_custkey::get();
    const auto& supp_ht = mqo::shared::hash_supplier_by_suppkey::get();

    const size_t n_rows = li.n_rows;
    AggBucket global_agg[2] = {};

    {
        MQO_TIME_PHASE("Q8_main_scan");

        #pragma omp parallel
        {
            AggBucket local_agg[2] = {};

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_rows; i++) {
                // Probe 1: part filter (most selective ~0.67%)
                const int32_t partkey = li.l_partkey[i];
                if (part_filter_set.find(partkey) == part_filter_set.end())
                    continue;

                // Probe 2: orders hash (extract o_orderdate, o_custkey)
                const int32_t orderkey = li.l_orderkey[i];
                const auto* oe = orders_ht.probe(orderkey);
                if (!oe) continue;

                // Probe 3: date filter on o_orderdate
                const int32_t odate = oe->o_orderdate;
                if (odate < DATE_1995_01_01 || odate > DATE_1996_12_31)
                    continue;

                // Probe 4: customer hash (extract c_nationkey)
                const int32_t custkey = oe->o_custkey;
                const auto* ce = cust_ht.probe(custkey);
                if (!ce) continue;

                // Probe 5: america bitset filter on c_nationkey
                const int32_t c_nk = ce->c_nationkey;
                if (c_nk < 0 || c_nk >= 32 || !america_nations.test(static_cast<size_t>(c_nk)))
                    continue;

                // Probe 6: supplier hash (extract s_nationkey)
                const int32_t suppkey = li.l_suppkey[i];
                const auto* se = supp_ht.probe(suppkey);
                if (!se) continue;

                // Probe 7: nation name lookup
                const int32_t s_nk = se->s_nationkey;

                // Compute expressions
                int y, m, d;
                mqo::io::from_epoch_days(odate, y, m, d);
                const int year_idx = y - 1995;
                if (year_idx < 0 || year_idx > 1) continue;  // safety

                const double volume = li.l_extendedprice[i] * (1.0 - li.l_discount[i]);

                local_agg[year_idx].total_volume += volume;
                if (s_nk >= 0 && s_nk < 25 && nation_name_code[s_nk] == brazil_dict_code) {
                    local_agg[year_idx].brazil_volume += volume;
                }
            }

            // Merge thread-local into global
            #pragma omp critical
            {
                for (int j = 0; j < 2; j++) {
                    global_agg[j].brazil_volume += local_agg[j].brazil_volume;
                    global_agg[j].total_volume  += local_agg[j].total_volume;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 4: Output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q8_output");
        std::string out_path = ctx.output_dir + "/q8.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot write: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "o_year,mkt_share\n");
        for (int j = 0; j < 2; j++) {
            int year = 1995 + j;
            double mkt_share = (global_agg[j].total_volume > 0.0)
                ? global_agg[j].brazil_volume / global_agg[j].total_volume
                : 0.0;
            std::fprintf(fp, "%d,%.17g\n", year, mkt_share);
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
