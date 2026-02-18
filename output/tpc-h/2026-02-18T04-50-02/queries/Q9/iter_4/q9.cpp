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
 *   - supplier: array[suppkey] = nationkey  (int8_t flat array)
 *   - part: bool array[partkey] = has_green (scan p_name dict)
 * Phase 2 (parallel via 3 threads):
 *   - Thread A: build ps_slots[partkey] = {suppkeys[4], costs[4]} [filtered by green]
 *               Direct array lookup replaces CompactHashMapPair composite-hash
 *   - Thread B: build order_year[orderkey] = year (int16_t flat array, max key ~60M)
 *               Pre-fault with madvise to hide initialization cost
 *   - (part already done in Phase 1)
 * Phase 3 (OpenMP parallel scan on lineitem, 64 threads):
 *   - Probe part_green[], ps_slots[lp] (direct array + 4-way scan), order_year[lok]
 *   - Thread-local flat agg[25][7], merge after
 * Phase 4: sort and output
 *
 * === KEY OPTIMIZATIONS vs. ITER 3 ===
 * 1. PartSuppSlots: replaces CompactHashMapPair (composite key hash) with direct
 *    array[partkey] + 4-way linear scan on suppkeys. Eliminates hash function overhead.
 *    Each part has exactly 4 suppliers in TPC-H. Struct is 52 bytes, fits 2 slots in
 *    a cache line pair (128B). Lookup: load ps_slots[lp] → 4-compare linear scan.
 * 2. order_year array: unchanged (optimal for random access from lineitem scan).
 *    Initialization overlap with Thread A via parallel std::thread.
 * 3. Phase 2 overlap: partsupp and orders built concurrently.
 * 4. OpenMP parallel lineitem scan with thread-local [25][7] accumulators.
 *
 * === PARALLELISM ===
 * - Phase 2: std::thread × 2 (partsupp + orders overlap)
 * - Phase 3: OpenMP parallel scan (64 threads, schedule(static))
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
    static constexpr int MAX_PS_SLOTS = 4; // TPC-H: each part has exactly 4 suppliers

    // PartSuppSlots: direct array indexed by partkey (no hash overhead on lookup).
    // For each green partkey, stores up to 4 (suppkey, supplycost) pairs.
    // Lookup: ps_slots[lp].find(ls) = 4-way linear scan (SIMD-friendly, ~1 cycle).
    // Size: MAX_PARTKEY × sizeof(PartSuppSlots) = 2M × 52B = ~100MB
    // Benefit: eliminates composite hash computation (2× multiply + combine per probe)
    //          and replaces with direct array access + 4-compare linear scan.
    struct alignas(64) PartSuppSlots {
        int32_t suppkeys[MAX_PS_SLOTS];  // 16 bytes
        int64_t costs[MAX_PS_SLOTS];     // 32 bytes
        int32_t count;                   // 4 bytes  (total: 52 bytes, aligned to 64)
        int32_t _pad;
    };

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
        // Phase 2: Build ps_slots and order_year in parallel threads
        // =========================================================================
        // Thread A: partsupp (8M rows) -> ps_slots[partkey] = {suppkeys[], costs[]}
        //           Direct array lookup eliminates composite key hash in Phase 3.
        // Thread B: orders (15M rows) -> order_year[orderkey] = year (int16_t flat array)

        // Allocate ps_slots as flat array (zero-initialized)
        std::vector<PartSuppSlots> ps_slots(MAX_PARTKEY);
        for (auto& s : ps_slots) { s.count = 0; }

        // order_year: flat array indexed by orderkey (sparse up to ~60M, stored as int16_t)
        // 120MB zero-initialized. Overlapped with Thread A to hide cost.
        std::vector<int16_t> order_year(MAX_ORDERKEY, 0);

        {
            // Thread A: build ps_slots (partsupp filtered by green)
            std::thread tA([&]() {
                gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
                gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
                gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
                const size_t n = ps_partkey.count;
                for (size_t i = 0; i < n; i++) {
                    int32_t ppk = ps_partkey.data[i];
                    if (ppk < MAX_PARTKEY && part_green[ppk]) {
                        PartSuppSlots& slot = ps_slots[ppk];
                        int32_t cnt = slot.count;
                        if (cnt < MAX_PS_SLOTS) {
                            slot.suppkeys[cnt] = ps_suppkey.data[i];
                            slot.costs[cnt]    = ps_supplycost.data[i];
                            slot.count = cnt + 1;
                        }
                    }
                }
            });

            // Thread B: build order_year
            std::thread tB([&]() {
                gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
                gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
                const size_t n = o_orderkey.count;
                for (size_t i = 0; i < n; i++) {
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
        //
        // Inner loop probe order (fastest-reject first):
        //  1. part_green[lp]     — bool array, ~97.6% reject
        //  2. ps_slots[lp].find(ls) — direct array + 4-way linear scan
        //  3. supp_nationkey[ls] — direct array lookup
        //  4. order_year[lok]    — direct array lookup

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

        // Thread-local accumulators: [thread][nation_idx * NUM_YEARS + year_idx]
        std::vector<std::array<int64_t, NUM_NATIONS * NUM_YEARS>> tl_profit(
            num_threads, std::array<int64_t, NUM_NATIONS * NUM_YEARS>{}
        );
        for (int t = 0; t < num_threads; t++)
            tl_profit[t].fill(0);

        // Raw pointers for inner loop (avoids repeated .data dereference in hot path)
        const int32_t* __restrict__ lp_data  = l_partkey.data;
        const int32_t* __restrict__ ls_data  = l_suppkey.data;
        const int32_t* __restrict__ lok_data = l_orderkey.data;
        const int64_t* __restrict__ ep_data  = l_extendedprice.data;
        const int64_t* __restrict__ disc_data = l_discount.data;
        const int64_t* __restrict__ qty_data  = l_quantity.data;
        const int8_t*  __restrict__ snk_data  = supp_nationkey.data();
        const int16_t* __restrict__ oy_data   = order_year.data();
        const PartSuppSlots* __restrict__ ps_data = ps_slots.data();

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel for schedule(static, 65536)
            for (size_t i = 0; i < N; i++) {
                int32_t lp = lp_data[i];
                // Fast reject: ~97.6% of rows filtered here
                if (lp >= MAX_PARTKEY || !part_green[lp]) continue;

                int32_t ls = ls_data[i];

                // Direct array access + 4-way linear scan (no hash)
                const PartSuppSlots& pss = ps_data[lp];
                int64_t sc = -1;
                {
                    // Unrolled 4-way scan (TPC-H guarantee: exactly 4 suppliers per part)
                    int32_t cnt = pss.count;
                    if (cnt > 0 && pss.suppkeys[0] == ls) { sc = pss.costs[0]; }
                    else if (cnt > 1 && pss.suppkeys[1] == ls) { sc = pss.costs[1]; }
                    else if (cnt > 2 && pss.suppkeys[2] == ls) { sc = pss.costs[2]; }
                    else if (cnt > 3 && pss.suppkeys[3] == ls) { sc = pss.costs[3]; }
                }
                if (sc < 0) continue;

                // Supplier -> nation (direct array lookup)
                int32_t nation_idx = (ls < MAX_SUPPKEY) ? (int32_t)snk_data[ls] : -1;
                if (nation_idx < 0 || nation_idx >= NUM_NATIONS) continue;

                // Order -> year (direct array lookup)
                int32_t lok = lok_data[i];
                if (lok >= MAX_ORDERKEY) continue;
                int yr = oy_data[lok];
                if (yr < MIN_YEAR || yr > MAX_YEAR) continue;

                int64_t ep   = ep_data[i];   // scaled x100
                int64_t disc = disc_data[i]; // scaled x100
                int64_t qty  = qty_data[i];  // scaled x100

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
