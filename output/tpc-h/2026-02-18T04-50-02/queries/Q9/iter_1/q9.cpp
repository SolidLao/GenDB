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
 * === CORRECTNESS NOTE ===
 * All monetary/decimal columns are stored scaled by 100 (int64_t).
 * amount_real = ep_real*(1-disc_real) - sc_real*qty_real
 * ep*(100-disc) = (ep_real*100)*(100 - disc_real*100)
 *              = ep_real*(1-disc_real)*10000
 * sc*qty = sc_real*qty_real*10000
 * => amount_scaled (x10000) = ep*(100-disc) - sc*qty
 * Output: amount_real = amount_scaled / 10000
 *
 * === PHYSICAL PLAN ===
 * Phase 1 (sequential, small tables):
 *   - nation: array[nationkey] = name_str
 *   - supplier: array[suppkey] = nationkey
 *   - part: bool array[partkey] = has_green (scan p_name dict)
 * Phase 2 (parallel via threads):
 *   - Thread A: build partsupp_map (partkey,suppkey)->supplycost [filtered by part_green]
 *   - Thread B: build order_year[orderkey] = year (int16_t flat array)
 * Phase 3 (OpenMP parallel scan):
 *   - lineitem: probe part_green, partsupp_map, order_year, supp_nationkey
 *   - Thread-local flat agg[25][7], merge after
 * Phase 4: sort and output
 *
 * === PARALLELISM ===
 * - Phases 2A+2B: std::thread (overlap I/O for two large tables)
 * - Phase 3: OpenMP parallel scan with thread-local [25][7] accumulators
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <fstream>
#include <thread>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    static constexpr int MAX_PARTKEY  = 2000001;
    static constexpr int MAX_SUPPKEY  = 100001;
    static constexpr int MAX_ORDERKEY = 60000001;
    static constexpr int MIN_YEAR     = 1992;
    static constexpr int MAX_YEAR     = 1998;
    static constexpr int NUM_YEARS    = MAX_YEAR - MIN_YEAR + 1; // 7
    static constexpr int NUM_NATIONS  = 25;

    // =========================================================================
    // Phase 1: Small dimension tables (sequential, fast)
    // =========================================================================
    {
        GENDB_PHASE("dim_filter");

        std::string nation_names[NUM_NATIONS];

        // 1a. Nation: nationkey -> name string
        {
            gendb::MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
            gendb::MmapColumn<int32_t> n_name_col(gendb_dir + "/nation/n_name.bin");
            std::vector<std::string> n_name_dict;
            {
                std::ifstream f(gendb_dir + "/nation/n_name_dict.txt");
                std::string line;
                while (std::getline(f, line)) n_name_dict.push_back(line);
            }
            for (size_t i = 0; i < n_nationkey.count; i++) {
                int32_t nk = n_nationkey.data[i];
                if (nk >= 0 && nk < NUM_NATIONS)
                    nation_names[nk] = n_name_dict[n_name_col.data[i]];
            }
        }

        // 1b. Supplier: suppkey -> nationkey (flat array, 1-based up to 100K)
        std::vector<int8_t> supp_nationkey(MAX_SUPPKEY, -1);
        {
            gendb::MmapColumn<int32_t> s_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
            gendb::MmapColumn<int32_t> s_nationkey(gendb_dir + "/supplier/s_nationkey.bin");
            for (size_t i = 0; i < s_suppkey.count; i++) {
                int32_t sk = s_suppkey.data[i];
                if (sk < MAX_SUPPKEY) supp_nationkey[sk] = (int8_t)s_nationkey.data[i];
            }
        }

        // 1c. Part: filter p_name LIKE '%green%' -> bool array[partkey]
        std::vector<bool> part_green(MAX_PARTKEY, false);
        size_t green_count = 0;
        {
            gendb::MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");
            gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
            std::vector<bool> name_has_green;
            {
                std::ifstream f(gendb_dir + "/part/p_name_dict.txt");
                std::string line;
                while (std::getline(f, line))
                    name_has_green.push_back(line.find("green") != std::string::npos);
            }
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
        }

        // =========================================================================
        // Phase 2: Build partsupp_map and order_year in parallel threads
        // =========================================================================
        // Thread A: partsupp (8M rows) -> (partkey,suppkey)->supplycost, filtered by green
        // Thread B: orders (15M rows) -> orderkey->year flat array

        gendb::CompactHashMapPair<int64_t> partsupp_map(green_count * 4 + 1024);
        // order_year: flat array indexed by orderkey (sparse up to ~60M, stored as int16_t)
        std::vector<int16_t> order_year(MAX_ORDERKEY, 0);

        {
            // Thread A: build partsupp_map
            std::thread tA([&]() {
                gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
                gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
                gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
                for (size_t i = 0; i < ps_partkey.count; i++) {
                    int32_t ppk = ps_partkey.data[i];
                    if (ppk < MAX_PARTKEY && part_green[ppk]) {
                        partsupp_map.insert({ppk, ps_suppkey.data[i]}, ps_supplycost.data[i]);
                    }
                }
            });

            // Thread B: build order_year
            std::thread tB([&]() {
                gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
                gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
                for (size_t i = 0; i < o_orderkey.count; i++) {
                    int32_t ok = o_orderkey.data[i];
                    if (ok < MAX_ORDERKEY)
                        order_year[ok] = (int16_t)gendb::extract_year(o_orderdate.data[i]);
                }
            });

            tA.join();
            tB.join();
        }

        // =========================================================================
        // Phase 3: Main lineitem scan — parallel OpenMP with thread-local agg
        // =========================================================================
        // amount_scaled (x10000) = ep*(100-disc) - sc*qty
        // Accumulate as int64_t: agg[nation_idx][year_idx]

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

        // Thread-local accumulators: [thread][nation_idx][year_idx]
        std::vector<std::array<int64_t, NUM_NATIONS * NUM_YEARS>> tl_profit(
            num_threads, std::array<int64_t, NUM_NATIONS * NUM_YEARS>{}
        );
        for (int t = 0; t < num_threads; t++)
            tl_profit[t].fill(0);

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel for schedule(static, 65536)
            for (size_t i = 0; i < N; i++) {
                int32_t lp = l_partkey.data[i];
                if (lp >= MAX_PARTKEY || !part_green[lp]) continue;

                int32_t ls = l_suppkey.data[i];
                int64_t* psc_ptr = partsupp_map.find({lp, ls});
                if (!psc_ptr) continue;

                int32_t nation_idx = (ls < MAX_SUPPKEY) ? (int32_t)supp_nationkey[ls] : -1;
                if (nation_idx < 0 || nation_idx >= NUM_NATIONS) continue;

                int32_t lok = l_orderkey.data[i];
                if (lok >= MAX_ORDERKEY) continue;
                int yr = order_year[lok];
                if (yr < MIN_YEAR || yr > MAX_YEAR) continue;

                int64_t ep   = l_extendedprice.data[i]; // scaled x100
                int64_t disc = l_discount.data[i];       // scaled x100
                int64_t qty  = l_quantity.data[i];       // scaled x100
                int64_t sc   = *psc_ptr;                 // scaled x100

                // amount_real = ep_real*(1-disc_real) - sc_real*qty_real
                // amount_scaled(x10000) = ep*(100-disc) - sc*qty
                int64_t amount_scaled = ep * (100 - disc) - sc * qty;

                int tid = omp_get_thread_num();
                int y_idx = yr - MIN_YEAR;
                tl_profit[tid][nation_idx * NUM_YEARS + y_idx] += amount_scaled;
            }
        }

        // =========================================================================
        // Phase 4: Merge thread-local results and output
        // =========================================================================
        {
            GENDB_PHASE("output");

            // Global aggregation
            std::array<int64_t, NUM_NATIONS * NUM_YEARS> global_profit{};
            global_profit.fill(0);

            for (int t = 0; t < num_threads; t++)
                for (int k = 0; k < NUM_NATIONS * NUM_YEARS; k++)
                    global_profit[k] += tl_profit[t][k];

            // Build and sort result rows
            struct ResultRow {
                const char* nation;
                int year;
                int64_t profit_scaled; // x10000
            };
            std::vector<ResultRow> rows;
            rows.reserve(NUM_NATIONS * NUM_YEARS);

            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < NUM_YEARS; y++) {
                    int64_t p = global_profit[n * NUM_YEARS + y];
                    if (p != 0) {
                        rows.push_back({nation_names[n].c_str(), MIN_YEAR + y, p});
                    }
                }
            }

            // ORDER BY nation ASC, o_year DESC
            std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
                int cmp = strcmp(a.nation, b.nation);
                if (cmp != 0) return cmp < 0;
                return a.year > b.year;
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
                int64_t p = row.profit_scaled;
                bool neg = (p < 0);
                if (neg) p = -p;
                int64_t int_part  = p / 10000;
                int64_t frac_part = p % 10000;
                if (neg)
                    std::fprintf(f, "%s,%d,-%lld.%04lld\n",
                        row.nation, row.year,
                        (long long)int_part, (long long)frac_part);
                else
                    std::fprintf(f, "%s,%d,%lld.%04lld\n",
                        row.nation, row.year,
                        (long long)int_part, (long long)frac_part);
            }
            std::fclose(f);
        }

    } // end dim_filter phase
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
