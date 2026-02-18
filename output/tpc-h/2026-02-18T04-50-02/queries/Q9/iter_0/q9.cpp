/*
 * Q9: Product Type Profit Measure
 *
 * SQL:
 *   SELECT nation, o_year, SUM(amount) AS sum_profit
 *   FROM (
 *     SELECT n_name AS nation,
 *            EXTRACT(YEAR FROM o_orderdate) AS o_year,
 *            l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
 *     FROM part, supplier, lineitem, partsupp, orders, nation
 *     WHERE s_suppkey = l_suppkey
 *       AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
 *       AND p_partkey = l_partkey
 *       AND o_orderkey = l_orderkey
 *       AND s_nationkey = n_nationkey
 *       AND p_name LIKE '%green%'
 *   ) AS profit
 *   GROUP BY nation, o_year
 *   ORDER BY nation, o_year DESC;
 *
 * === LOGICAL PLAN ===
 * Step 1: Single-table predicates & cardinalities
 *   - part (2M rows): p_name LIKE '%green%' => ~48K rows (2.4%)
 *   - nation (25 rows): no filter, all rows
 *   - supplier (100K rows): no filter, all rows
 *   - partsupp (8M rows): no filter, join with part_green
 *   - orders (15M rows): no filter, need o_orderkey -> year
 *   - lineitem (60M rows): main fact table, probe all dimensions
 *
 * Step 2: Join ordering (smallest filtered result first)
 *   1. Filter part -> part_green set (~48K partkeys)
 *   2. Build nation array: nationkey -> name (25 entries, direct array)
 *   3. Build supplier array: suppkey -> nationkey (100K entries, direct array)
 *   4. Build partsupp map: (partkey,suppkey) -> supplycost (filtered by part_green ~192K entries)
 *   5. Build orders array: orderkey -> year (15M entries, direct array indexed by orderkey)
 *   6. Scan lineitem (60M rows): probe (partkey,suppkey), then orderkey->year, then suppkey->nationkey->nation
 *
 * Step 3: Aggregation
 *   - GROUP BY (nation_name, year): 25 nations x ~7 years = ~175 groups
 *   - Use flat array [nation_idx][year_idx] for O(1) access
 *
 * === PHYSICAL PLAN ===
 * - part: mmap p_name.bin + p_name_dict.txt -> scan dict for "green" entries -> bitset of partkeys
 * - nation: mmap n_nationkey.bin + n_name.bin + n_name_dict.txt -> array[nationkey] = name_str
 * - supplier: mmap s_suppkey.bin + s_nationkey.bin -> array[suppkey] = nationkey
 * - partsupp: mmap ps_partkey.bin + ps_suppkey.bin + ps_supplycost.bin
 *             -> CompactHashMapPair<int64_t> keyed by (partkey,suppkey), filtered by part_green
 * - orders: mmap o_orderkey.bin + o_orderdate.bin
 *           -> std::vector<int16_t> year_arr (indexed by orderkey since o_orderkey <= 15M)
 * - lineitem: mmap all needed cols, parallel OpenMP scan
 *             probe (l_partkey,l_suppkey) in partsupp_map
 *             lookup year = order_year[l_orderkey]
 *             lookup nation_idx = supp_nation[l_suppkey] -> nation_name
 *             accumulate profit[nation_idx][year-1992]
 * - Output: sort by nation name, year DESC, write CSV
 *
 * === PARALLELISM ===
 * - lineitem scan: OpenMP parallel_for with thread-local aggregation arrays
 * - Merge thread-local into global after scan
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================================
    // Phase 1: Dimension table setup
    // =========================================================================
    // We store:
    //   part_green[partkey] = true if p_name contains 'green' (bitset via bool array)
    //   nation_name[nationkey] = string name
    //   supp_nationkey[suppkey] = nationkey (0-24)
    //   orderkey_year[orderkey] = year (e.g. 1992..1998) stored as int16_t
    //   partsupp_map: (partkey,suppkey) -> supplycost (int64_t)

    // Max known values
    // NOTE: TPC-H SF10 orderkeys are sparse, go up to 4*num_orders = 60M
    static constexpr int MAX_PARTKEY   = 2000001;
    static constexpr int MAX_SUPPKEY   = 100001;
    static constexpr int MAX_ORDERKEY  = 60000001;
    static constexpr int MIN_YEAR      = 1992;
    static constexpr int MAX_YEAR      = 1998;
    static constexpr int NUM_YEARS     = MAX_YEAR - MIN_YEAR + 1; // 7
    static constexpr int NUM_NATIONS   = 25;

    // -------------------------------------------------------------------------
    // 1a. Load nation names
    // -------------------------------------------------------------------------
    std::string nation_names[NUM_NATIONS]; // indexed by nationkey

    {
        GENDB_PHASE("dim_filter");

        // Load nation: n_nationkey, n_name (dict-encoded)
        gendb::MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
        gendb::MmapColumn<int32_t> n_name_col(gendb_dir + "/nation/n_name.bin");

        // Load n_name dictionary
        std::vector<std::string> n_name_dict;
        {
            std::ifstream f(gendb_dir + "/nation/n_name_dict.txt");
            std::string line;
            while (std::getline(f, line)) n_name_dict.push_back(line);
        }

        for (size_t i = 0; i < n_nationkey.count; i++) {
            int32_t nk = n_nationkey.data[i];
            int32_t nc = n_name_col.data[i];
            if (nk >= 0 && nk < NUM_NATIONS) {
                nation_names[nk] = n_name_dict[nc];
            }
        }

        // -------------------------------------------------------------------------
        // 1b. Load supplier -> nationkey mapping
        // -------------------------------------------------------------------------
        gendb::MmapColumn<int32_t> s_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey(gendb_dir + "/supplier/s_nationkey.bin");

        // Direct array: suppkey is 1-based, up to 100000
        // We'll use a flat vector indexed by suppkey
        std::vector<int8_t> supp_nationkey(MAX_SUPPKEY, -1);
        for (size_t i = 0; i < s_suppkey.count; i++) {
            int32_t sk = s_suppkey.data[i];
            int32_t nk = s_nationkey.data[i];
            if (sk < MAX_SUPPKEY) supp_nationkey[sk] = (int8_t)nk;
        }

        // -------------------------------------------------------------------------
        // 1c. Filter part by p_name LIKE '%green%'
        //     Build bool array part_green[partkey]
        // -------------------------------------------------------------------------
        gendb::MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");

        // Load p_name dictionary and find which dict codes contain 'green'
        std::vector<bool> name_has_green;
        {
            std::ifstream f(gendb_dir + "/part/p_name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                name_has_green.push_back(line.find("green") != std::string::npos);
            }
        }

        // Build part_green bitset
        std::vector<bool> part_green(MAX_PARTKEY, false);
        size_t green_count = 0;
        for (size_t i = 0; i < p_partkey.count; i++) {
            int32_t pk = p_partkey.data[i];
            int32_t nc = p_name_col.data[i];
            if (nc >= 0 && nc < (int32_t)name_has_green.size() && name_has_green[nc]) {
                if (pk < MAX_PARTKEY) {
                    part_green[pk] = true;
                    green_count++;
                }
            }
        }

        // -------------------------------------------------------------------------
        // 1d. Build partsupp map: (partkey,suppkey) -> supplycost
        //     Filtered by part_green
        // -------------------------------------------------------------------------
        gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");

        // Estimate ~4 suppliers per green part => ~192K entries
        gendb::CompactHashMapPair<int64_t> partsupp_map(green_count * 4 + 1024);

        for (size_t i = 0; i < ps_partkey.count; i++) {
            int32_t ppk = ps_partkey.data[i];
            if (ppk < MAX_PARTKEY && part_green[ppk]) {
                int32_t psk = ps_suppkey.data[i];
                int64_t psc = ps_supplycost.data[i];
                partsupp_map.insert({ppk, psk}, psc);
            }
        }

        // -------------------------------------------------------------------------
        // 1e. Build orders: orderkey -> year
        //     orderkey up to 15M, store as int16_t for cache efficiency
        // -------------------------------------------------------------------------
        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");

        std::vector<int16_t> order_year(MAX_ORDERKEY, 0);

        for (size_t i = 0; i < o_orderkey.count; i++) {
            int32_t ok = o_orderkey.data[i];
            int32_t od = o_orderdate.data[i];
            if (ok < MAX_ORDERKEY) {
                order_year[ok] = (int16_t)gendb::extract_year(od);
            }
        }

        // =========================================================================
        // Phase 2: Main scan - lineitem with parallel aggregation
        // =========================================================================
        // Aggregation key: (nation_idx 0..24, year_idx 0..6)
        // Result: sum_profit[nation_idx][year_idx]
        // Precision: accumulate as int64_t scaled x100*100 = x10000
        // amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity
        //        = (ep * (100 - disc)) / 100 - (sc * qty) / 100
        // All in int64_t: ep, disc, sc, qty scaled by 100
        // So: amount_scaled = ep*(100-disc) - sc*qty  (scaled by 100*100 = 10000)
        // Final output: divide by 10000, format with 4 decimal places

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int32_t> l_partkey(gendb_dir + "/lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity(gendb_dir + "/lineitem/l_quantity.bin");

        l_orderkey.advise_sequential();
        l_partkey.advise_sequential();
        l_suppkey.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();
        l_quantity.advise_sequential();

        const size_t N = l_orderkey.count;
        const int num_threads = omp_get_max_threads();

        // Thread-local aggregation buffers
        // [thread][nation_idx][year_idx]
        std::vector<std::vector<std::array<int64_t, NUM_YEARS>>> tl_profit(
            num_threads,
            std::vector<std::array<int64_t, NUM_YEARS>>(NUM_NATIONS, std::array<int64_t, NUM_YEARS>{})
        );

        // Initialize to zero
        for (int t = 0; t < num_threads; t++)
            for (int n = 0; n < NUM_NATIONS; n++)
                for (int y = 0; y < NUM_YEARS; y++)
                    tl_profit[t][n][y] = 0;

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel for schedule(dynamic, 65536)
            for (size_t i = 0; i < N; i++) {
                int32_t lp = l_partkey.data[i];
                if (lp >= MAX_PARTKEY || !part_green[lp]) continue;

                int32_t ls = l_suppkey.data[i];
                int64_t* psc_ptr = partsupp_map.find({lp, ls});
                if (!psc_ptr) continue;

                int32_t lok = l_orderkey.data[i];
                if (lok >= MAX_ORDERKEY) continue;
                int yr = order_year[lok];
                if (yr < MIN_YEAR || yr > MAX_YEAR) continue;

                int32_t nation_idx = (ls < MAX_SUPPKEY) ? (int32_t)supp_nationkey[ls] : -1;
                if (nation_idx < 0 || nation_idx >= NUM_NATIONS) continue;

                int64_t ep  = l_extendedprice.data[i];  // scaled x100
                int64_t disc = l_discount.data[i];       // scaled x100
                int64_t qty  = l_quantity.data[i];       // scaled x100
                int64_t sc   = *psc_ptr;                 // scaled x100

                // amount_scaled: ep*(100-disc) is scale-100*unitless=scale-100
                // sc*qty is scale-100*scale-100=scale-10000
                // Multiply first term by 100 to match: both become scale-10000
                int64_t amount_scaled = ep * (100 - disc) * 100 - sc * qty;

                int tid = omp_get_thread_num();
                int y_idx = yr - MIN_YEAR;
                tl_profit[tid][nation_idx][y_idx] += amount_scaled;
            }
        }

        // =========================================================================
        // Phase 3: Merge thread-local results and output
        // =========================================================================
        {
            GENDB_PHASE("output");

            // Global aggregation: [nation_idx][year_idx]
            std::vector<std::array<int64_t, NUM_YEARS>> global_profit(
                NUM_NATIONS, std::array<int64_t, NUM_YEARS>{}
            );
            for (int n = 0; n < NUM_NATIONS; n++)
                for (int y = 0; y < NUM_YEARS; y++)
                    global_profit[n][y] = 0;

            for (int t = 0; t < num_threads; t++)
                for (int n = 0; n < NUM_NATIONS; n++)
                    for (int y = 0; y < NUM_YEARS; y++)
                        global_profit[n][y] += tl_profit[t][n][y];

            // Build result rows: (nation_name, year, sum_profit)
            struct ResultRow {
                std::string nation;
                int year;
                int64_t profit_scaled; // x10000
            };
            std::vector<ResultRow> rows;
            rows.reserve(NUM_NATIONS * NUM_YEARS);

            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < NUM_YEARS; y++) {
                    int64_t p = global_profit[n][y];
                    if (p != 0) {
                        rows.push_back({nation_names[n], MIN_YEAR + y, p});
                    }
                }
            }

            // Sort: ORDER BY nation ASC, o_year DESC
            std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
                if (a.nation != b.nation) return a.nation < b.nation;
                return a.year > b.year; // DESC
            });

            // Write CSV
            std::string out_path = results_dir + "/Q9.csv";
            FILE* f = std::fopen(out_path.c_str(), "w");
            if (!f) {
                std::fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
                return;
            }
            std::fprintf(f, "nation,o_year,sum_profit\n");
            for (const auto& row : rows) {
                // profit_scaled is x10000: format as decimal with 4 places
                int64_t p = row.profit_scaled;
                bool neg = (p < 0);
                if (neg) p = -p;
                int64_t integer_part = p / 10000;
                int64_t frac_part    = p % 10000;
                if (neg)
                    std::fprintf(f, "%s,%d,-%lld.%04lld\n",
                        row.nation.c_str(), row.year,
                        (long long)integer_part, (long long)frac_part);
                else
                    std::fprintf(f, "%s,%d,%lld.%04lld\n",
                        row.nation.c_str(), row.year,
                        (long long)integer_part, (long long)frac_part);
            }
            std::fclose(f);
        }

    } // end dim_filter phase (which wraps everything for RAII ordering)
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
