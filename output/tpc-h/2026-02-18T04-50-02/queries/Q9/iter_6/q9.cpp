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
 * === PHYSICAL PLAN (iter_6) ===
 *
 * Bottleneck analysis (iter_5):
 *   dim_filter = 338ms total; main_scan (lineitem, OpenMP) = 42ms.
 *   => Phase 1+2 = ~296ms (sequential/2-thread).
 *   Orders scan (15M rows) and partsupp scan (8M rows) are the critical path.
 *   Only 2 cores used for Phase 2 (one thread each). 64 cores available.
 *
 * Key optimizations (iter_6):
 *  1. Parallelize orders scan fully: std::thread tB uses OpenMP internally
 *     (omp_set_num_threads inside thread, or use omp_set_nested). Since
 *     each orderkey is unique, parallel writes to order_year[ok] are safe
 *     without atomics. Use partitioned approach: divide 15M rows into chunks,
 *     each chunk processed by one OMP thread writing to disjoint key space
 *     isn't guaranteed — use atomic_store or just parallel for with no conflict.
 *     Actually orderkeys ARE unique so concurrent writes to different indices
 *     are safe. Use #pragma omp parallel for with schedule(static).
 *
 *  2. Parallelize partsupp scan: 8M rows, thread-local CompactHashMaps,
 *     merge after. Only ~192K green entries total -> merge is trivial.
 *     Split: launch tA with OMP parallel inside.
 *
 *  3. Restructure: Run partsupp then orders builds sequentially, each using
 *     ALL 64 OMP cores. No nested OMP inside std::threads (avoids 128-thread
 *     contention). Sequential phases each get full core count.
 *
 *  4. For partsupp parallel build with thread-local vectors:
 *     - Each OMP thread collects (key, cost) pairs in thread-local vector
 *     - After parallel region, merge ~192K entries sequentially (fast)
 *     - Expected speedup: ~32-64x on scan phase
 *
 *  5. order_year zero-init: use calloc for OS-level zero-page optimization.
 *     calloc uses anonymous mmap which gets zero pages lazily from OS.
 *     Avoids memset over 60MB upfront.
 *
 *  6. Part filter (2M rows) parallelized with OMP parallel for.
 *
 * Phase 1: Sequential (nation 25 rows, supplier 100K rows) — negligible
 * Phase 1b: OMP parallel (part green filter, 2M rows)
 * Phase 2A: OMP parallel partsupp build (8M rows, thread-local pairs, merge)
 * Phase 2B: OMP parallel orders build (15M rows, direct array write, unique keys)
 * Phase 3: OMP parallel lineitem scan (60M rows, 64 threads, thread-local agg)
 * Phase 4: Merge + sort + output
 */

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <fstream>
#include <thread>
#include <atomic>
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
    // Phase 1: Small dimension tables (sequential, negligible cost)
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

        // 1c. Part: filter p_name LIKE '%green%' -> uint8_t array[partkey]
        // Use uint8_t instead of std::vector<bool> to avoid bit-packing in hot scan
        // Allocate with calloc for OS zero-page benefit
        uint8_t* part_green_raw = (uint8_t*)calloc(MAX_PARTKEY, sizeof(uint8_t));
        std::vector<uint8_t> part_green; // will be assigned ownership via swap trick
        // Use raw pointer for fast access, wrapped in a deleter
        struct FreeOnExit { uint8_t* p; ~FreeOnExit() { free(p); } } _pg_guard{part_green_raw};

        std::atomic<size_t> green_count_atomic{0};
        {
            gendb::MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");
            gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
            std::vector<uint8_t> name_has_green;
            {
                std::ifstream f(gendb_dir + "/part/p_name_dict.txt");
                std::string line;
                while (std::getline(f, line))
                    name_has_green.push_back(line.find("green") != std::string::npos ? 1 : 0);
            }

            // Parallel part scan: 2M rows, parallel OMP writes to disjoint positions
            // (partkeys are unique, so no write conflicts)
            const size_t Np = p_partkey.count;
            const int32_t* __restrict__ pk_data  = p_partkey.data;
            const int32_t* __restrict__ pn_data  = p_name_col.data;
            const uint8_t* __restrict__ nhg_data = name_has_green.data();
            const int32_t nhg_size = (int32_t)name_has_green.size();
            uint8_t* __restrict__ pg_raw = part_green_raw;
            size_t local_green = 0;

            #pragma omp parallel for schedule(static) reduction(+:local_green)
            for (size_t i = 0; i < Np; i++) {
                int32_t pk = pk_data[i];
                int32_t nc = pn_data[i];
                if (nc >= 0 && nc < nhg_size && nhg_data[nc]) {
                    if ((uint32_t)pk < (uint32_t)MAX_PARTKEY) {
                        pg_raw[pk] = 1;
                        local_green++;
                    }
                }
            }
            green_count_atomic.store(local_green);
        }

        size_t green_count = green_count_atomic.load();

        // =========================================================================
        // Phase 2: Build partsupp_map and order_year using ALL 64 OMP cores
        // =========================================================================
        // Phase 2A (partsupp, 8M rows):
        //   - OMP parallel for, thread-local (key,cost) pair collectors
        //   - Filter by part_green first (only ~192K entries pass green filter)
        //   - Merge into global partsupp_map sequentially (~192K entries, fast)
        //
        // Phase 2B (orders, 15M rows):
        //   - OMP parallel for over 15M rows, all 64 cores
        //   - Direct write to order_year[ok] — safe: o_orderkey is PK (unique)
        //   - order_year allocated with calloc (OS zero pages, lazy fault)

        // order_year: use calloc for lazy OS zero-page allocation (faster than memset)
        // stores (year - MIN_YEAR + 1): range 1..7, 0 = absent
        int8_t* order_year_raw = (int8_t*)calloc(MAX_ORDERKEY, sizeof(int8_t));
        struct FreeOrderYear { int8_t* p; ~FreeOrderYear() { free(p); } } _oy_guard{order_year_raw};

        // partsupp key: partkey * 100001LL + suppkey (unique composite key)
        // Pre-size: green_count * 4 (4 suppliers per part on average)
        gendb::CompactHashMap<int64_t, int64_t> partsupp_map(green_count * 4 + 1024);

        {
            const int nthreads = omp_get_max_threads();

            // ---------------------------------------------------------------
            // Phase 2A: Parallel partsupp build (8M rows -> ~192K green entries)
            // Each OMP thread collects matching (key,cost) pairs in thread-local
            // vectors, then we merge sequentially into the global hash map.
            // This avoids lock contention during insert.
            // ---------------------------------------------------------------
            {
                gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
                gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
                gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
                const size_t M = ps_partkey.count;

                const int32_t* __restrict__ ppk_data = ps_partkey.data;
                const int32_t* __restrict__ psk_data = ps_suppkey.data;
                const int64_t* __restrict__ psc_data = ps_supplycost.data;
                const uint8_t* __restrict__ pg_raw   = part_green_raw;

                // Thread-local (key, cost) collectors
                std::vector<std::vector<std::pair<int64_t,int64_t>>> local_pairs(nthreads);
                size_t reserve_per = (green_count * 4 + nthreads - 1) / nthreads + 64;
                for (auto& lp : local_pairs) lp.reserve(reserve_per);

                #pragma omp parallel for schedule(static)
                for (size_t i = 0; i < M; i++) {
                    int32_t ppk = ppk_data[i];
                    if ((uint32_t)ppk < (uint32_t)MAX_PARTKEY && pg_raw[ppk]) {
                        int64_t key = (int64_t)ppk * 100001LL + psk_data[i];
                        int tid = omp_get_thread_num();
                        local_pairs[tid].emplace_back(key, psc_data[i]);
                    }
                }

                // Sequential merge into global partsupp_map (~192K entries total)
                for (int t = 0; t < nthreads; t++) {
                    for (auto& [k, v] : local_pairs[t]) {
                        partsupp_map.insert(k, v);
                    }
                }
            }

            // ---------------------------------------------------------------
            // Phase 2B: Parallel orders scan (15M rows)
            // Orderkeys are globally unique (PK), so parallel writes to
            // order_year[ok] at distinct indices are safe without atomics.
            // ---------------------------------------------------------------
            {
                gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
                gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
                const size_t M = o_orderkey.count;

                const int32_t* __restrict__ ok_data = o_orderkey.data;
                const int32_t* __restrict__ od_data = o_orderdate.data;
                int8_t* __restrict__ oy_raw = order_year_raw;

                #pragma omp parallel for schedule(static)
                for (size_t i = 0; i < M; i++) {
                    int32_t ok = ok_data[i];
                    if ((uint32_t)ok < (uint32_t)MAX_ORDERKEY) {
                        int yr = gendb::extract_year(od_data[i]);
                        if (yr >= MIN_YEAR && yr <= MAX_YEAR)
                            oy_raw[ok] = (int8_t)(yr - MIN_YEAR + 1);
                    }
                }
            }
        }

        // =========================================================================
        // Phase 3: Main lineitem scan — parallel OpenMP with thread-local agg
        // =========================================================================
        // amount_scaled (x100) = ep*(100-disc)/100 - sc*qty/100
        // Accumulate as int64_t: agg[nation_idx * NUM_YEARS + year_idx]

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
        // NUM_NATIONS * NUM_YEARS = 175 int64_t = 1400B — fits in L1 per thread
        static constexpr int AGG_SIZE = NUM_NATIONS * NUM_YEARS;
        std::vector<std::array<int64_t, AGG_SIZE>> tl_profit(num_threads);
        for (int t = 0; t < num_threads; t++)
            tl_profit[t].fill(0);

        // Cache raw pointers for hot loop — avoids repeated member dereference
        const int32_t* __restrict__ lp_data   = l_partkey.data;
        const int32_t* __restrict__ ls_data   = l_suppkey.data;
        const int32_t* __restrict__ lok_data  = l_orderkey.data;
        const int64_t* __restrict__ ep_data   = l_extendedprice.data;
        const int64_t* __restrict__ disc_data = l_discount.data;
        const int64_t* __restrict__ qty_data  = l_quantity.data;
        const uint8_t* __restrict__ pg_data   = part_green_raw;
        const int8_t*  __restrict__ sn_data   = supp_nationkey.data();
        const int8_t*  __restrict__ oy_data   = order_year_raw;

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel for schedule(static, 65536)
            for (size_t i = 0; i < N; i++) {
                int32_t lp = lp_data[i];
                // Fast: uint8_t lookup instead of vector<bool> bit-unpack
                if ((uint32_t)lp >= (uint32_t)MAX_PARTKEY || !pg_data[lp]) continue;

                int32_t ls = ls_data[i];
                // Check nation early (before expensive hash probe)
                int32_t nation_idx = ((uint32_t)ls < (uint32_t)MAX_SUPPKEY) ? (int32_t)sn_data[ls] : -1;
                if (nation_idx < 0) continue;

                // Encode (partkey, suppkey) as single int64_t key
                int64_t ps_key = (int64_t)lp * 100001LL + ls;
                int64_t* psc_ptr = partsupp_map.find(ps_key);
                if (!psc_ptr) continue;

                int32_t lok = lok_data[i];
                if ((uint32_t)lok >= (uint32_t)MAX_ORDERKEY) continue;
                int8_t yr_off = oy_data[lok];
                if (yr_off == 0) continue; // not in orders or out of year range

                int64_t ep   = ep_data[i];
                int64_t disc = disc_data[i];
                int64_t qty  = qty_data[i];
                int64_t sc   = *psc_ptr;

                // amount_scaled(x100) = ep*(100-disc)/100 - sc*qty/100
                int64_t amount_scaled = (ep * (100 - disc)) / 100 - (sc * qty) / 100;

                int tid = omp_get_thread_num();
                int y_idx = (int)yr_off - 1; // back to 0-based year index
                tl_profit[tid][nation_idx * NUM_YEARS + y_idx] += amount_scaled;
            }
        }

        // =========================================================================
        // Phase 4: Merge thread-local results and output
        // =========================================================================
        {
            GENDB_PHASE("output");

            std::array<int64_t, AGG_SIZE> global_profit{};
            global_profit.fill(0);

            for (int t = 0; t < num_threads; t++)
                for (int k = 0; k < AGG_SIZE; k++)
                    global_profit[k] += tl_profit[t][k];

            struct ResultRow {
                const char* nation;
                int year;
                int64_t profit_scaled; // x100
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
                int64_t int_part  = p / 100;
                int64_t frac_part = p % 100;
                if (neg)
                    std::fprintf(f, "%s,%d,-%lld.%02lld\n",
                        row.nation, row.year,
                        (long long)int_part, (long long)frac_part);
                else
                    std::fprintf(f, "%s,%d,%lld.%02lld\n",
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
