/*
 * Q9: Product Type Profit Measure
 *
 * SQL:
 *   SELECT nation, o_year, SUM(amount) AS sum_profit
 *   FROM (
 *     SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
 *            l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
 *     FROM part, supplier, lineitem, partsupp, orders, nation
 *     WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
 *       AND ps_partkey = l_partkey AND p_partkey = l_partkey
 *       AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
 *       AND p_name LIKE '%green%'
 *   ) AS profit
 *   GROUP BY nation, o_year
 *   ORDER BY nation, o_year DESC
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Predicate pushdown / filtered cardinalities:
 *   part:     filter p_name LIKE '%green%'  → ~200K-400K parts (dict scan)
 *   supplier: no filter                     → 100K rows
 *   nation:   no filter                     → 25 rows
 *   partsupp: no filter                     → 8M rows
 *   orders:   no filter                     → 15M rows
 *   lineitem: no filter, but semi-join on p_partkey → 59M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier → part(filtered) → partsupp → lineitem → orders
 *
 * Step 3 — Physical plan:
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *   Phase 2: Load supplier (100K) → direct array: s_suppkey → s_nationkey (1-indexed, max=100000)
 *   Phase 3: Scan part name dict; find all dict codes containing "green";
 *            then scan p_name.bin to collect set of matching p_partkey values
 *            → CompactHashSet<int32_t> green_parts (~400K entries)
 *   Phase 4: Scan partsupp (8M), filter ps_partkey in green_parts
 *            → CompactHashMapPair<int64_t> ps_map: (ps_partkey,ps_suppkey) → ps_supplycost
 *   Phase 5: Scan orders (15M) → CompactHashMap<int32_t,int16_t>: o_orderkey → year
 *   Phase 6: Scan lineitem (60M) in parallel with OpenMP:
 *            - filter l_partkey in green_parts (fast hash set probe)
 *            - lookup ps_map[(l_partkey, l_suppkey)] → ps_supplycost
 *            - lookup supp_nationkey[l_suppkey] → nationkey
 *            - lookup order_year[l_orderkey] → year
 *            - compute amount = l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity
 *              (all scaled int64 arithmetic, divide by 10000 for output)
 *            - accumulate into thread-local agg[nation_idx][year-1992]
 *   Phase 7: Merge thread-local aggregations, sort, output CSV
 *
 * Aggregation structure: 25 nations × 7 years (1992-1998) = 175 groups
 *   → flat 2D array: agg[25][10] (indexed by n_nationkey and year-1992)
 *   → atomic int64 adds with #pragma omp atomic
 *
 * Arithmetic (all scaled integers, scale=100):
 *   l_extendedprice ∈ int64 (×100), l_discount ∈ int64 (×100)
 *   l_extendedprice * (100 - l_discount) → scale = 100*100 = 10000
 *   ps_supplycost ∈ int64 (×100), l_quantity ∈ int64 (×100)
 *   ps_supplycost * l_quantity → scale = 100*100 = 10000
 *   amount = (l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity) / 10000
 *   For output: divide by 10000 and print 4 decimal places
 * ============================================================
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ─── Aggregation grid: 25 nations × 10 years (1992-2001 covers all TPC-H data) ───
static const int N_NATIONS = 25;
static const int YEAR_BASE  = 1992;
static const int N_YEARS    = 10;  // 1992-2001

// ─── nation name lookup by nationkey (0-24) ───
static std::string g_nation_names[N_NATIONS];

// ─── supplier nationkey lookup by suppkey (1-indexed, max 100000) ───
// We'll use a flat vector indexed by s_suppkey (1-based, so size = 100001)
static std::vector<int8_t> g_supp_nationkey; // int8_t: 0-24 fits

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ─── Phase 1: Load nation ───────────────────────────────────────────────
    {
        GENDB_PHASE("dim_filter");

        // nation n_name (25 rows, int32_t codes)
        gendb::MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
        gendb::MmapColumn<int32_t> n_name_col(gendb_dir + "/nation/n_name.bin");

        // Load nation name dictionary (line N = code N)
        std::vector<std::string> nation_dict;
        {
            std::ifstream f(gendb_dir + "/nation/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                nation_dict.push_back(line);
            }
        }
        for (size_t i = 0; i < n_nationkey.count; i++) {
            int32_t nk = n_nationkey[i];
            int32_t nc = n_name_col[i];
            if (nk >= 0 && nk < N_NATIONS && nc >= 0 && nc < (int32_t)nation_dict.size()) {
                g_nation_names[nk] = nation_dict[nc];
            }
        }

        // ─── Load supplier: suppkey → nationkey ────────────────────────────
        gendb::MmapColumn<int32_t> s_suppkey_col(gendb_dir + "/supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey_col(gendb_dir + "/supplier/s_nationkey.bin");
        size_t n_suppliers = s_suppkey_col.count;
        g_supp_nationkey.resize(100001, -1);
        for (size_t i = 0; i < n_suppliers; i++) {
            int32_t sk = s_suppkey_col[i];
            int32_t nk = s_nationkey_col[i];
            if (sk >= 0 && sk <= 100000) {
                g_supp_nationkey[sk] = (int8_t)nk;
            }
        }
    }

    // ─── Phase 2: Find green parts ─────────────────────────────────────────
    gendb::CompactHashSet<int32_t> green_parts(500000);
    {
        GENDB_PHASE("dim_filter_part");

        // Load p_name dict and find codes containing "green"
        std::vector<bool> green_code;
        {
            std::ifstream f(gendb_dir + "/part/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                // Check if the part name contains "green"
                green_code.push_back(line.find("green") != std::string::npos);
            }
        }

        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;

        for (size_t i = 0; i < n_parts; i++) {
            int32_t code = p_name_col[i];
            if (code >= 0 && code < (int32_t)green_code.size() && green_code[code]) {
                green_parts.insert(p_partkey_col[i]);
            }
        }
    }

    // ─── Phase 3: Build partsupp hash map (partkey,suppkey) → supplycost ───
    // Only keep entries where ps_partkey is a green part
    gendb::CompactHashMapPair<int64_t> ps_map(green_parts.size() * 4 + 1000);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        size_t n_ps = ps_partkey_col.count;

        for (size_t i = 0; i < n_ps; i++) {
            int32_t pk = ps_partkey_col[i];
            if (!green_parts.contains(pk)) continue;
            int32_t sk = ps_suppkey_col[i];
            int64_t sc = ps_supplycost_col[i];
            ps_map.insert({pk, sk}, sc);
        }
    }

    // ─── Phase 4: Build orders hash map: o_orderkey → year ─────────────────
    // Use compact hash map: o_orderkey (int32_t) → year offset from 1992 (int8_t)
    // Max orderkey ~ 60M, but only 15M orders; use CompactHashMap
    gendb::CompactHashMap<int32_t, int16_t> order_year_map(16000000);
    {
        GENDB_PHASE("build_joins_orders");

        gendb::MmapColumn<int32_t> o_orderkey_col(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        size_t n_orders = o_orderkey_col.count;

        for (size_t i = 0; i < n_orders; i++) {
            int32_t od = o_orderdate_col[i];
            int16_t yr = (int16_t)gendb::extract_year(od);
            order_year_map.insert(o_orderkey_col[i], yr);
        }
    }

    // ─── Phase 5: Scan lineitem — parallel aggregation ──────────────────────
    // Aggregation: agg[nation_idx][year - YEAR_BASE] accumulates scaled amount
    // Each thread has its own agg buffer to avoid contention
    const int num_threads = omp_get_max_threads();
    // Thread-local aggregation: [thread][nation][year]
    // Flattened: thread_agg[thread * N_NATIONS * N_YEARS + nation * N_YEARS + year]
    std::vector<int64_t> thread_agg(num_threads * N_NATIONS * N_YEARS, 0LL);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_partkey_col(gendb_dir + "/lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey_col(gendb_dir + "/lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice_col(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount_col(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity_col(gendb_dir + "/lineitem/l_quantity.bin");
        gendb::MmapColumn<int32_t> l_orderkey_col(gendb_dir + "/lineitem/l_orderkey.bin");

        int64_t n_lineitem = (int64_t)l_partkey_col.count;

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            // Pointer to this thread's aggregation slice
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(dynamic, 200000)
            for (int64_t i = 0; i < n_lineitem; i++) {
                int32_t lk = l_partkey_col[i];

                // Fast semi-join filter: must be a green part
                if (!green_parts.contains(lk)) continue;

                int32_t sk = l_suppkey_col[i];

                // Lookup partsupp
                const int64_t* psc = ps_map.find({lk, sk});
                if (!psc) continue;

                // Lookup order year
                const int16_t* yr_ptr = order_year_map.find(l_orderkey_col[i]);
                if (!yr_ptr) continue;

                int yr = *yr_ptr;
                int yi = yr - YEAR_BASE;
                if (yi < 0 || yi >= N_YEARS) continue;

                // Supplier nationkey
                if (sk < 0 || sk > 100000) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if (ni >= N_NATIONS) continue;

                // Compute amount in scaled int:
                // l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity
                // All scaled by 100, so result scaled by 100*100 = 10000
                int64_t ep  = l_extendedprice_col[i];
                int64_t dis = l_discount_col[i];
                int64_t qty = l_quantity_col[i];
                int64_t amount = ep * (100LL - dis) - (*psc) * qty;

                local_agg[ni * N_YEARS + yi] += amount;
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregations ──────────────────────────
    // Final global agg [nation][year]
    std::vector<int64_t> global_agg(N_NATIONS * N_YEARS, 0LL);

    for (int t = 0; t < num_threads; t++) {
        const int64_t* ta = thread_agg.data() + t * N_NATIONS * N_YEARS;
        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                global_agg[ni * N_YEARS + yi] += ta[ni * N_YEARS + yi];
            }
        }
    }

    // ─── Phase 7: Output ────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // Build result rows: (nation_name, year, sum_profit)
        struct ResultRow {
            std::string nation;
            int year;
            int64_t sum_profit_scaled; // scaled by 10000
        };
        std::vector<ResultRow> rows;
        rows.reserve(N_NATIONS * N_YEARS);

        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                int64_t v = global_agg[ni * N_YEARS + yi];
                if (v == 0) continue;
                int yr = YEAR_BASE + yi;
                rows.push_back({g_nation_names[ni], yr, v});
            }
        }

        // Sort: nation ASC, year DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) {
            std::cerr << "Cannot open output file: " << out_path << std::endl;
            return;
        }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& row : rows) {
            // sum_profit_scaled is in units of 1/10000
            // Output with 4 decimal places
            int64_t val = row.sum_profit_scaled;
            int64_t whole = val / 10000LL;
            int64_t frac  = val % 10000LL;
            if (frac < 0) { whole--; frac += 10000LL; }
            fprintf(fp, "%s,%d,%lld.%04lld\n",
                    row.nation.c_str(), row.year,
                    (long long)whole, (long long)frac);
        }
        fclose(fp);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
