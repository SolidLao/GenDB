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
 * === PHYSICAL PLAN (iter 3) ===
 *
 * Step 1: 4 parallel build threads:
 *   T0: nation (25 rows) + supplier (100K) -> supp_nationkey[suppkey]
 *   T1: part (2M) -> part_green bool array + green_suppkeys set
 *   T2: partsupp (8M) -> flat ps_entries[partkey] (up to 4 slots per green part)
 *         Keyed by partkey, linear-search suppkey among ≤4 entries.
 *         Avoids composite hash, replaces with O(1) partkey lookup + 4-slot scan.
 *   T3: orders (15M) -> order_year_compact[orderkey] as uint8_t (year-1992+1, 0=missing)
 *         Cuts memory from 120MB (int16_t×60M) to 60MB (uint8_t×60M).
 *
 * Step 2 (OpenMP parallel): lineitem scan (60M rows)
 *   - Check part_green[l_partkey]
 *   - Lookup ps_entries[l_partkey] for suppkey match → get supplycost
 *   - Lookup supp_nationkey[l_suppkey] → nation_idx
 *   - Lookup order_year_compact[l_orderkey] → year_idx
 *   - Accumulate into thread-local flat array [25 nations × 7 years]
 *
 * Step 3: Merge + output
 *
 * === PARALLELISM ===
 * - Build phases: 4 std::threads (max I/O parallelism on HDD)
 * - Main scan: OpenMP parallel (64 cores) with thread-local [25][7] accumulators
 *
 * === KEY DATA STRUCTURES ===
 * - part_green: vector<bool>[2M] — green part filter
 * - ps_entries: flat array[2M] of PsSlots (4×{suppkey,cost}) — replaces composite hash map
 * - supp_nationkey: uint8_t[100K] — suppkey → nationkey
 * - order_year_compact: uint8_t[60M] — orderkey → (year-MIN_YEAR+1), 0=absent
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
#include <atomic>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Flat partsupp storage per green partkey.
// TPC-H guarantees exactly 4 suppliers per part (SF×4 entries in partsupp).
// Store up to 4 (suppkey, supplycost) slots directly in a flat array[MAX_PARTKEY].
// ---------------------------------------------------------------------------
struct alignas(64) PsSlots {
    static constexpr int MAX_SLOTS = 4;
    int32_t suppkey[MAX_SLOTS];
    int64_t cost[MAX_SLOTS];
    int8_t  count;  // number of valid slots

    PsSlots() : count(0) {
        suppkey[0] = suppkey[1] = suppkey[2] = suppkey[3] = 0;
        cost[0] = cost[1] = cost[2] = cost[3] = 0;
    }

    // Insert a (suppkey, cost) pair (up to MAX_SLOTS)
    void insert(int32_t sk, int64_t c) {
        if (count < MAX_SLOTS) {
            suppkey[count] = sk;
            cost[count] = c;
            count++;
        }
    }

    // Find cost for a given suppkey; returns INT64_MIN if not found
    __attribute__((always_inline))
    int64_t find(int32_t sk) const {
        // Branchless scan over 4 slots
        for (int i = 0; i < count; i++) {
            if (suppkey[i] == sk) return cost[i];
        }
        return INT64_MIN;
    }
};

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
    // Allocate shared lookup structures up front (before threads launch)
    // =========================================================================
    std::string nation_names[NUM_NATIONS];
    std::vector<uint8_t> supp_nationkey(MAX_SUPPKEY, 255); // 255 = invalid
    std::vector<bool>    part_green(MAX_PARTKEY, false);

    // Flat ps_entries: one PsSlots per partkey (only green parts populated)
    // ~2M × 64B = 128MB — fits well in 376GB RAM
    std::vector<PsSlots> ps_entries(MAX_PARTKEY);

    // order_year_compact: uint8_t per orderkey, value = (year - MIN_YEAR + 1), 0 = absent
    // 60M × 1B = 60MB (vs 120MB for int16_t)
    std::vector<uint8_t> order_year_compact(MAX_ORDERKEY, 0);

    // =========================================================================
    // Phase 1: 4 parallel build threads
    // =========================================================================
    {
        GENDB_PHASE("dim_filter");

        // Thread 0: nation + supplier (tiny tables, fast)
        std::thread t0([&]() {
            // Nation
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
            // Supplier
            {
                gendb::MmapColumn<int32_t> s_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
                gendb::MmapColumn<int32_t> s_nationkey(gendb_dir + "/supplier/s_nationkey.bin");
                for (size_t i = 0; i < s_suppkey.count; i++) {
                    int32_t sk = s_suppkey.data[i];
                    if (sk < MAX_SUPPKEY)
                        supp_nationkey[sk] = (uint8_t)s_nationkey.data[i];
                }
            }
        });

        // Thread 1: part — build part_green array from dictionary
        std::thread t1([&]() {
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
                if (pk < MAX_PARTKEY && nc >= 0 && nc < (int32_t)name_has_green.size()
                    && name_has_green[nc]) {
                    part_green[pk] = true;
                }
            }
        });

        // Thread 2: partsupp — build flat ps_entries (wait for t1 to set part_green first)
        // We join t1 before t2 uses part_green, but to keep full parallelism:
        // t2 filters during build using part_green which t1 sets concurrently.
        // Safe because each partkey slot is unique to t2, and part_green writes (t1)
        // to different partkey slots happen-before t2 reads them IF we sync.
        // Solution: t2 reads part_green AFTER joining t1. Launch t2 after t1 joins.
        // We'll handle this by a phased approach:
        //   Phase A: t0, t1, t3 run concurrently
        //   Phase B: t2 runs after t1 (needs part_green)
        // This is still better than sequential: t0/t1/t3 all overlap.

        // Thread 3: orders — build order_year_compact
        std::thread t3([&]() {
            gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
            gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
            for (size_t i = 0; i < o_orderkey.count; i++) {
                int32_t ok = o_orderkey.data[i];
                if (ok < MAX_ORDERKEY) {
                    int yr = gendb::extract_year(o_orderdate.data[i]);
                    if (yr >= MIN_YEAR && yr <= MAX_YEAR)
                        order_year_compact[ok] = (uint8_t)(yr - MIN_YEAR + 1);
                }
            }
        });

        // Join t1 first (part_green needed by partsupp thread)
        t1.join();

        // Now build ps_entries in a dedicated thread (part_green is ready)
        std::thread t2([&]() {
            gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
            gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
            gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
            for (size_t i = 0; i < ps_partkey.count; i++) {
                int32_t ppk = ps_partkey.data[i];
                if (ppk < MAX_PARTKEY && part_green[ppk]) {
                    ps_entries[ppk].insert(ps_suppkey.data[i], ps_supplycost.data[i]);
                }
            }
        });

        t0.join();
        t2.join();
        t3.join();

    } // end dim_filter phase

    // =========================================================================
    // Phase 2: Main lineitem scan — parallel OpenMP with thread-local agg
    // =========================================================================
    gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int32_t> l_partkey(gendb_dir + "/lineitem/l_partkey.bin");
    gendb::MmapColumn<int32_t> l_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> l_quantity(gendb_dir + "/lineitem/l_quantity.bin");

    // Prefetch all lineitem columns into page cache asynchronously
    mmap_prefetch_all(l_orderkey, l_partkey, l_suppkey,
                      l_extendedprice, l_discount, l_quantity);

    l_orderkey.advise_sequential();
    l_partkey.advise_sequential();
    l_suppkey.advise_sequential();
    l_extendedprice.advise_sequential();
    l_discount.advise_sequential();
    l_quantity.advise_sequential();

    const size_t N = l_orderkey.count;
    const int num_threads = omp_get_max_threads();

    // Thread-local accumulators: flat [NUM_NATIONS * NUM_YEARS]
    std::vector<std::array<int64_t, NUM_NATIONS * NUM_YEARS>> tl_profit(
        num_threads, std::array<int64_t, NUM_NATIONS * NUM_YEARS>{}
    );
    for (int t = 0; t < num_threads; t++)
        tl_profit[t].fill(0);

    // Cache raw pointers for inner-loop performance (avoid member dereference overhead)
    const int32_t* __restrict__ lp_data  = l_partkey.data;
    const int32_t* __restrict__ ls_data  = l_suppkey.data;
    const int32_t* __restrict__ lok_data = l_orderkey.data;
    const int64_t* __restrict__ ep_data  = l_extendedprice.data;
    const int64_t* __restrict__ disc_data= l_discount.data;
    const int64_t* __restrict__ qty_data = l_quantity.data;

    // Read-only lookup tables as raw pointers
    const PsSlots* __restrict__  ps_ptr   = ps_entries.data();
    const uint8_t* __restrict__  sn_ptr   = supp_nationkey.data();
    const uint8_t* __restrict__  oy_ptr   = order_year_compact.data();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(static, 65536)
        for (size_t i = 0; i < N; i++) {
            int32_t lp = lp_data[i];
            if ((uint32_t)lp >= (uint32_t)MAX_PARTKEY) continue;
            if (!part_green[lp]) continue;

            int32_t ls = ls_data[i];

            // Flat array lookup by partkey, then linear scan over ≤4 slots
            int64_t sc = ps_ptr[lp].find(ls);
            if (sc == INT64_MIN) continue;

            // Supplier → nation
            uint8_t nation_idx = ((uint32_t)ls < (uint32_t)MAX_SUPPKEY) ? sn_ptr[ls] : 255;
            if (nation_idx >= (uint8_t)NUM_NATIONS) continue;

            // Order → year
            int32_t lok = lok_data[i];
            if ((uint32_t)lok >= (uint32_t)MAX_ORDERKEY) continue;
            uint8_t yr_enc = oy_ptr[lok];
            if (yr_enc == 0) continue;  // no order year stored

            int64_t ep   = ep_data[i];
            int64_t disc = disc_data[i];
            int64_t qty  = qty_data[i];

            // amount_scaled(x10000) = ep*(100-disc) - sc*qty
            int64_t amount_scaled = ep * (100 - disc) - sc * qty;

            int tid    = omp_get_thread_num();
            int y_idx  = (int)yr_enc - 1;  // 0-based year index
            tl_profit[tid][(int)nation_idx * NUM_YEARS + y_idx] += amount_scaled;
        }
    }

    // =========================================================================
    // Phase 3: Merge thread-local results and output
    // =========================================================================
    {
        GENDB_PHASE("output");

        std::array<int64_t, NUM_NATIONS * NUM_YEARS> global_profit{};
        global_profit.fill(0);

        for (int t = 0; t < num_threads; t++)
            for (int k = 0; k < NUM_NATIONS * NUM_YEARS; k++)
                global_profit[k] += tl_profit[t][k];

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
