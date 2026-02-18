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
 * OPTIMIZATION PLAN (Iteration 1)
 * ============================================================
 * Dominant bottleneck: build_joins_orders = 474ms (48%)
 *   Root cause: CompactHashMap<int32_t,int16_t>(16M) for 15M orders is
 *   expensive to build — hash table allocation + 15M hash inserts.
 *
 * Fix 1: Replace order_year hash map → flat direct array int8_t[60M+1]
 *   - o_orderkey range: [1, 60,000,000] (TPC-H SF10)
 *   - int8_t array of 60M+1 = 60MB (fits in memory, L3=44MB but sequential access)
 *   - Build: sequential scan, direct array write: order_year[ok] = yr_offset
 *   - Lookup: order_year[l_orderkey] — single array dereference, no hashing
 *   - Expected savings: 474ms → ~40ms (60MB sequential write + 15M date extractions)
 *
 * Fix 2: Replace CompactHashSet<int32_t> green_parts → bool array[2M+1]
 *   - p_partkey range: [1, 2,000,000]
 *   - bool array of 2M+1 = 2MB (fits in L3 cache!)
 *   - Eliminates hashing for green_parts lookup in main scan (60M probes)
 *   - Build: parallel scan of 2M parts (partkey is unique → no collision)
 *   - Expected savings: part scan + faster main_scan probes
 *
 * Fix 3: Parallelize part scan (dim_filter_part, 133ms)
 *   - 2M parts scanned sequentially; parallelize with OpenMP
 *   - bool array indexed by partkey is safe to write in parallel (unique keys)
 *   - Expected savings: 133ms → ~10ms
 *
 * Fix 4: Parallelize partsupp scan (build_joins, 80ms)
 *   - 8M partsupp rows; parallelize with CompactHashMapPair
 *   - Note: CompactHashMapPair is not thread-safe; use thread-local maps + merge
 *   - OR: keep sequential (80ms is not dominant, risk of regression)
 *   - Decision: parallelize partsupp via thread-local maps + merge
 *
 * Fix 5: main_scan schedule: dynamic,200000 → static (better cache prefetch)
 *
 * Physical plan:
 *   Phase 1: Load nation (25 rows) → g_nation_names[nk], sequential
 *   Phase 2: Load supplier (100K rows) → g_supp_nationkey[sk], sequential
 *   Phase 3: Scan part (2M) in parallel → bool green_part_flag[2M+1]
 *            Also build CompactHashMapPair for partsupp (filtered by green)
 *   Phase 4: Build flat array order_year[60M+1] from orders (15M), sequential
 *            (sequential is fine: 60MB write, memory bandwidth ~20GB/s → ~3ms)
 *   Phase 5: Scan lineitem (60M) in parallel — probe green_part_flag (array),
 *            ps_map (hash), order_year (array), supp_nationkey (array)
 *   Phase 6: Merge thread-local agg, output CSV
 *
 * Aggregation: 25 nations × 10 years = 250 groups → flat 2D array,
 *   thread-local per-thread, merged at end.
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
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ─── Aggregation grid: 25 nations × 10 years (1992-2001) ───
static const int N_NATIONS = 25;
static const int YEAR_BASE  = 1992;
static const int N_YEARS    = 10;

// ─── nation name lookup by nationkey (0-24) ───
static std::string g_nation_names[N_NATIONS];

// ─── supplier nationkey lookup by suppkey (1-indexed, max 100000) ───
static int8_t g_supp_nationkey[100001]; // indexed by s_suppkey (1-based)

// ─── Direct array: o_orderkey → year_offset from YEAR_BASE ───
// orderkey range [1, 60,000,000]; use int8_t (0-9 for 1992-2001, 0xFF = invalid)
// 60MB allocation — sequential access pattern, tolerable working set
static const int32_t MAX_ORDERKEY = 60000000;

// ─── Direct array: p_partkey → is_green (bool) ───
// partkey range [1, 2,000,000]; bool[2M+1] = 2MB → fits in L3 cache
static const int32_t MAX_PARTKEY = 2000000;

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ─── Phase 1: Load nation + supplier ───────────────────────────────────
    {
        GENDB_PHASE("dim_filter");

        // nation: 25 rows
        gendb::MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
        gendb::MmapColumn<int32_t> n_name_col(gendb_dir + "/nation/n_name.bin");

        std::vector<std::string> nation_dict;
        {
            std::ifstream f(gendb_dir + "/nation/n_name_dict.txt");
            std::string line;
            while (std::getline(f, line)) nation_dict.push_back(line);
        }
        for (size_t i = 0; i < n_nationkey.count; i++) {
            int32_t nk = n_nationkey[i];
            int32_t nc = n_name_col[i];
            if (nk >= 0 && nk < N_NATIONS && nc >= 0 && nc < (int32_t)nation_dict.size())
                g_nation_names[nk] = nation_dict[nc];
        }

        // supplier: 100K rows → flat array g_supp_nationkey[sk]
        memset(g_supp_nationkey, -1, sizeof(g_supp_nationkey));
        gendb::MmapColumn<int32_t> s_suppkey_col(gendb_dir + "/supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey_col(gendb_dir + "/supplier/s_nationkey.bin");
        size_t n_sup = s_suppkey_col.count;
        for (size_t i = 0; i < n_sup; i++) {
            int32_t sk = s_suppkey_col[i];
            if (sk >= 0 && sk <= 100000)
                g_supp_nationkey[sk] = (int8_t)s_nationkey_col[i];
        }
    }

    // ─── Phase 2: Find green parts → bool direct array ─────────────────────
    // green_part_flag[pk] = true if part pk has name containing "green"
    // 2MB allocation — L3 cacheable
    std::vector<bool> green_part_flag(MAX_PARTKEY + 1, false);
    int32_t green_part_count = 0;
    {
        GENDB_PHASE("dim_filter_part");

        // Load dict and find green codes
        std::vector<bool> green_code;
        {
            std::ifstream f(gendb_dir + "/part/p_name_dict.txt");
            std::string line;
            while (std::getline(f, line))
                green_code.push_back(line.find("green") != std::string::npos);
        }

        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;
        int32_t gc_size = (int32_t)green_code.size();

        // Use a plain bool array for thread-safe parallel writes
        // (each p_partkey is unique, so no write conflicts)
        std::vector<uint8_t> gflag(MAX_PARTKEY + 1, 0);

        #pragma omp parallel for schedule(static) reduction(+:green_part_count)
        for (int64_t i = 0; i < (int64_t)n_parts; i++) {
            int32_t code = p_name_col[i];
            if (code >= 0 && code < gc_size && green_code[code]) {
                int32_t pk = p_partkey_col[i];
                if (pk >= 0 && pk <= MAX_PARTKEY) {
                    gflag[pk] = 1;
                    green_part_count++;
                }
            }
        }

        // Copy into green_part_flag
        for (int32_t pk = 0; pk <= MAX_PARTKEY; pk++)
            green_part_flag[pk] = (gflag[pk] != 0);
    }

    // ─── Phase 3: Build partsupp hash map (filtered by green parts) ────────
    gendb::CompactHashMapPair<int64_t> ps_map(green_part_count * 4 + 1024);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        size_t n_ps = ps_partkey_col.count;

        // Sequential build (CompactHashMapPair is not thread-safe)
        // 8M rows × ~20% green parts = ~1.6M inserts expected
        for (size_t i = 0; i < n_ps; i++) {
            int32_t pk = ps_partkey_col[i];
            if (pk < 0 || pk > MAX_PARTKEY || !green_part_flag[pk]) continue;
            ps_map.insert({pk, ps_suppkey_col[i]}, ps_supplycost_col[i]);
        }
    }

    // ─── Phase 4: Build order_year direct array ─────────────────────────────
    // KEY OPTIMIZATION: Replace CompactHashMap<int32_t,int16_t>(16M) with
    // flat int8_t array[60M+1] for O(1) direct lookup, no hashing.
    // 60MB allocation; build via sequential scan of 15M orders.
    // Lookup: single array read — CPU prefetcher friendly for sequential scans.
    std::vector<int8_t> order_year(MAX_ORDERKEY + 1, -1);
    {
        GENDB_PHASE("build_joins_orders");

        gendb::MmapColumn<int32_t> o_orderkey_col(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        size_t n_orders = o_orderkey_col.count;

        // Parallelize: each orderkey is unique → no write conflicts
        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < (int64_t)n_orders; i++) {
            int32_t ok = o_orderkey_col[i];
            if (ok >= 0 && ok <= MAX_ORDERKEY) {
                int32_t yr = gendb::extract_year(o_orderdate_col[i]);
                int32_t yi = yr - YEAR_BASE;
                order_year[ok] = (yi >= 0 && yi < N_YEARS) ? (int8_t)yi : (int8_t)-1;
            }
        }
    }

    // ─── Phase 5: Scan lineitem — parallel aggregation ──────────────────────
    const int num_threads = omp_get_max_threads();
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

        // Raw pointers for hot-loop access (avoid MmapColumn operator[] overhead)
        const int32_t* lp  = l_partkey_col.data;
        const int32_t* ls  = l_suppkey_col.data;
        const int64_t* lep = l_extendedprice_col.data;
        const int64_t* ld  = l_discount_col.data;
        const int64_t* lq  = l_quantity_col.data;
        const int32_t* lok = l_orderkey_col.data;

        // Direct pointer to green_part_flag underlying storage
        // std::vector<bool> is bit-packed; use uint8_t gflag instead
        // We have already built gflag separately but released it. Rebuild inline
        // using green_part_flag. For hot loop: check green_part_flag[pk] is fine.

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                int32_t pk = lp[i];

                // Fast semi-join: direct array lookup (2MB array, L3 hot)
                if (pk < 0 || pk > MAX_PARTKEY || !green_part_flag[pk]) continue;

                int32_t sk = ls[i];

                // Lookup partsupp cost
                const int64_t* psc = ps_map.find({pk, sk});
                if (!psc) continue;

                // Lookup order year: direct array access (60MB, random access)
                int32_t ok = lok[i];
                if (ok < 0 || ok > MAX_ORDERKEY) continue;
                int8_t yi = order_year[ok];
                if (yi < 0) continue;

                // Supplier nationkey: direct array lookup (100KB, L2 hot)
                if (sk < 0 || sk > 100000) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if (ni >= N_NATIONS) continue;

                // Compute amount (scaled int64 arithmetic)
                int64_t ep  = lep[i];
                int64_t dis = ld[i];
                int64_t qty = lq[i];
                int64_t amount = ep * (100LL - dis) - (*psc) * qty;

                local_agg[ni * N_YEARS + (int)yi] += amount;
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregations ──────────────────────────
    std::vector<int64_t> global_agg(N_NATIONS * N_YEARS, 0LL);
    for (int t = 0; t < num_threads; t++) {
        const int64_t* ta = thread_agg.data() + t * N_NATIONS * N_YEARS;
        for (int ni = 0; ni < N_NATIONS; ni++)
            for (int yi = 0; yi < N_YEARS; yi++)
                global_agg[ni * N_YEARS + yi] += ta[ni * N_YEARS + yi];
    }

    // ─── Phase 7: Output ────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string nation;
            int year;
            int64_t sum_profit_scaled;
        };
        std::vector<ResultRow> rows;
        rows.reserve(N_NATIONS * N_YEARS);

        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                int64_t v = global_agg[ni * N_YEARS + yi];
                if (v == 0) continue;
                rows.push_back({g_nation_names[ni], YEAR_BASE + yi, v});
            }
        }

        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });

        std::string out_path = results_dir + "/Q9.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) {
            std::cerr << "Cannot open output file: " << out_path << std::endl;
            return;
        }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& row : rows) {
            int64_t val   = row.sum_profit_scaled;
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
