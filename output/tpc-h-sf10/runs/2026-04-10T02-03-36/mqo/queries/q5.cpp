// Q5 tail: Local Supplier Volume (TPC-H Q5)
// Shared inputs: scan_lineitem_full, scan_orders_full, hash_customer_by_custkey, hash_supplier_by_suppkey
// Local scans: region (5 rows), nation (25 rows)

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/hash_customer_by_custkey.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"
#include "../shared/mqo_io.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q5(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q5_tail");

    static constexpr int NUM_NATIONS = 25;
    static constexpr int32_t MAX_SUPPKEY = 100001;
    static constexpr int32_t DATE_LO = 8766;   // 1994-01-01
    static constexpr int32_t DATE_HI = 9131;    // 1995-01-01

    // ---------------------------------------------------------------
    // Step 1: Build nation/region filter (5+25 rows)
    // ---------------------------------------------------------------
    uint8_t asia_nationkeys[NUM_NATIONS] = {};
    uint8_t nationkey_to_name_code[NUM_NATIONS];
    std::memset(nationkey_to_name_code, 0xFF, sizeof(nationkey_to_name_code));
    mqo::io::Dictionary n_name_dict;

    {
        MQO_TIME_PHASE("Q5_nation_region_filter");

        // Find ASIA region key
        std::string rdir = ctx.gendb_dir + "/region";
        size_t r_nrows = mqo::io::read_row_count(rdir);
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rdir + "/r_regionkey.bin", r_nrows);
        mqo::io::Dictionary r_name_dict;
        r_name_dict.load(rdir + "/r_name_dict.bin");
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(rdir + "/r_name.bin", r_nrows);

        int asia_regionkey = -1;
        for (size_t i = 0; i < r_nrows; ++i) {
            if (r_name_dict.get(r_name_codes[i]) == "ASIA") {
                asia_regionkey = r_regionkey[i];
                break;
            }
        }

        // Scan nation, filter by asia_regionkey, build lookup structures
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t n_nrows = mqo::io::read_row_count(ndir);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_nrows);
        const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(ndir + "/n_regionkey.bin", n_nrows);
        n_name_dict.load(ndir + "/n_name_dict.bin");
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_nrows);

        for (size_t i = 0; i < n_nrows; ++i) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < NUM_NATIONS) {
                nationkey_to_name_code[nk] = n_name_codes[i];
                if (n_regionkey[i] == asia_regionkey) {
                    asia_nationkeys[nk] = 1;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Filter suppliers by ASIA nations → suppkey_to_nationkey
    // ---------------------------------------------------------------
    std::vector<int8_t> suppkey_to_nationkey(MAX_SUPPKEY, -1);
    {
        MQO_TIME_PHASE("Q5_filter_suppliers");
        const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
        for (size_t i = 0; i < supp.n_rows; ++i) {
            int32_t nk = supp.s_nationkey[i];
            if (nk >= 0 && nk < NUM_NATIONS && asia_nationkeys[nk]) {
                int32_t sk = supp.s_suppkey[i];
                if (sk >= 0 && sk < MAX_SUPPKEY) {
                    suppkey_to_nationkey[sk] = static_cast<int8_t>(nk);
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3+4: Filter orders by date [1994-01-01, 1995-01-01),
    //           probe customer for ASIA nation, build orderkey→c_nationkey hash
    // ---------------------------------------------------------------
    // Open-addressing hash table for orderkey -> c_nationkey
    static constexpr size_t HT_SIZE = 1 << 20; // 1M slots for ~428K entries
    static constexpr int32_t EMPTY_KEY = -1;
    struct HTSlot {
        int32_t key = EMPTY_KEY;
        int8_t  val = -1;
    };
    std::vector<HTSlot> ok_ht(HT_SIZE);

    {
        MQO_TIME_PHASE("Q5_orders_customer_filter");
        const auto& ord = mqo::shared::scan_orders_full::get();
        const auto& cust = mqo::shared::hash_customer_by_custkey::get();

        for (size_t i = 0; i < ord.n_rows; ++i) {
            int32_t odate = ord.o_orderdate[i];
            if (odate < DATE_LO || odate >= DATE_HI) continue;

            int32_t ck = ord.o_custkey[i];
            if (ck < 0 || ck > cust.max_key) continue;
            int32_t row_id = cust.pk_index[ck];
            if (row_id < 0) continue;

            int32_t c_nk = cust.c_nationkey[row_id];
            if (c_nk < 0 || c_nk >= NUM_NATIONS || !asia_nationkeys[c_nk]) continue;

            // Insert into hash table
            int32_t okey = ord.o_orderkey[i];
            uint32_t h = static_cast<uint32_t>(okey) * 2654435761u;
            size_t slot = h & (HT_SIZE - 1);
            while (ok_ht[slot].key != EMPTY_KEY) {
                slot = (slot + 1) & (HT_SIZE - 1);
            }
            ok_ht[slot].key = okey;
            ok_ht[slot].val = static_cast<int8_t>(c_nk);
        }
    }

    // ---------------------------------------------------------------
    // Step 5: Stream 60M lineitem rows, probe two maps, aggregate revenue
    // ---------------------------------------------------------------
    double revenue[NUM_NATIONS] = {};

    {
        MQO_TIME_PHASE("Q5_lineitem_probe_agg");
        const auto& li = mqo::shared::scan_lineitem_full::get();
        const size_t n = li.n_rows;
        const int32_t* l_orderkey      = li.l_orderkey;
        const int32_t* l_suppkey       = li.l_suppkey;
        const double*  l_extendedprice = li.l_extendedprice;
        const double*  l_discount      = li.l_discount;

        #pragma omp parallel
        {
            double local_rev[NUM_NATIONS] = {};

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n; ++i) {
                // Probe orderkey hash table
                int32_t okey = l_orderkey[i];
                uint32_t h = static_cast<uint32_t>(okey) * 2654435761u;
                size_t slot = h & (HT_SIZE - 1);
                int8_t c_nk = -1;
                while (true) {
                    int32_t k = ok_ht[slot].key;
                    if (k == EMPTY_KEY) break;
                    if (k == okey) { c_nk = ok_ht[slot].val; break; }
                    slot = (slot + 1) & (HT_SIZE - 1);
                }
                if (c_nk < 0) continue;

                // Check supplier nationkey matches customer nationkey
                int32_t sk = l_suppkey[i];
                if (sk < 0 || sk >= MAX_SUPPKEY) continue;
                int8_t s_nk = suppkey_to_nationkey[sk];
                if (s_nk != c_nk) continue;

                local_rev[c_nk] += l_extendedprice[i] * (1.0 - l_discount[i]);
            }

            // Merge thread-local results
            for (int j = 0; j < NUM_NATIONS; ++j) {
                if (local_rev[j] != 0.0) {
                    #pragma omp atomic
                    revenue[j] += local_rev[j];
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 6+7: Collect results, sort by revenue DESC, write CSV
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q5_sort_output");
        struct Result {
            std::string name;
            double revenue;
        };
        std::vector<Result> results;
        results.reserve(5);
        for (int i = 0; i < NUM_NATIONS; ++i) {
            if (asia_nationkeys[i] && revenue[i] != 0.0) {
                results.push_back({n_name_dict.get(nationkey_to_name_code[i]), revenue[i]});
            }
        }

        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.revenue > b.revenue; });

        std::string outpath = ctx.output_dir + "/q5.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(fp, "n_name,revenue\n");
        for (const auto& r : results) {
            std::fprintf(fp, "%s,%.2f\n", r.name.c_str(), r.revenue);
        }
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
