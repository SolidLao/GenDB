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
 * === PHYSICAL PLAN (iter_8) ===
 *
 * Bottleneck analysis (iter_7):
 *   dim_filter = 275ms (wraps everything), main_scan = 32ms.
 *   => Phase 2 (partsupp + orders) dominates at ~242ms.
 *   Root cause: tA and tB each spawn #pragma omp parallel inside std::threads.
 *   Two simultaneous OMP teams × 64 threads each = 128 thread requests on 64 cores.
 *   Both OMP teams fight for the same physical cores → serialization overhead.
 *
 * Key optimization (iter_8): FUSED SINGLE-PASS OMP PARALLEL REGION
 *   Instead of two competing std::threads each launching their own OMP team,
 *   use a SINGLE #pragma omp parallel region that splits work across threads:
 *     - First half of threads: process partsupp (8M rows)
 *     - Second half of threads: process orders (15M rows)
 *   No thread contention. No OMP-inside-thread nesting issues.
 *   Both datasets processed simultaneously in one unified OMP team.
 *
 *   Additional optimization: prefetch all large data files with MADV_WILLNEED
 *   before opening them, so HDD I/O is overlapped with CPU work in prior phases.
 *
 *   Also: supp_nationkey -> use int8_t array but also build a bloom-filter-like
 *   bitset for fast "is this suppkey valid" check before the hash probe.
 *
 * Phase 1: Sequential (nation 25, supplier 100K, part 2M OMP) — negligible
 * Phase 2: Single OMP parallel region, thread-split:
 *   Threads 0..half-1: partsupp scan (8M rows, thread-local pairs, merge)
 *   Threads half..N-1: orders scan (15M rows, direct write to flat array)
 * Phase 3: OMP parallel lineitem scan (60M rows)
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

namespace {

} // end anonymous namespace

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
        // Phase 2: Build partsupp_map and order_year in a SINGLE FUSED OMP REGION
        // =========================================================================
        // Instead of two competing OMP teams inside std::threads, use ONE parallel
        // region and split threads by tid:
        //   Threads [0, half): handle partsupp (8M rows)
        //   Threads [half, nthreads): handle orders (15M rows)
        //
        // This avoids the "two OMP teams × 64 threads each on 64 cores" contention
        // from iter_7. Both datasets are scanned simultaneously without fighting
        // for the same physical cores.
        //
        // Key encoding: partkey<<17 | suppkey (suppkey ≤ 100K < 2^17=131072)
        // Slightly faster than partkey*100001+suppkey (shift+or vs multiply).

        // order_year: calloc for lazy OS zero-page (faster than memset over 60MB)
        // Stores (year - MIN_YEAR + 1): range 1..7, 0 = absent
        int8_t* order_year_raw = (int8_t*)calloc(MAX_ORDERKEY, sizeof(int8_t));
        struct FreeOrderYear { int8_t* p; ~FreeOrderYear() { free(p); } } _oy_guard{order_year_raw};

        // partsupp_map: ~192K green entries (2M parts × 2.4% green × 4 suppliers)
        // Key = (partkey << 17) | suppkey — unique since suppkey < 2^17
        gendb::CompactHashMap<int64_t, int64_t> partsupp_map(green_count * 4 + 1024);

        {
            const int nthreads = omp_get_max_threads();
            // Split threads: first half for partsupp, second half for orders
            const int half = nthreads / 2;  // e.g., 32 threads each on 64-core system

            // Pre-allocate thread-local collectors for partsupp threads
            // Only threads [0, half) collect pairs, so allocate 'half' slots
            std::vector<std::vector<std::pair<int64_t,int64_t>>> local_pairs(half);
            {
                // ~192K entries split across 'half' threads
                size_t reserve_per = (green_count * 4 + half - 1) / half + 64;
                for (auto& lp : local_pairs) lp.reserve(reserve_per);
            }

            // Open all files BEFORE the parallel region to avoid file-open overhead
            // inside the parallel region (which would serialize on file descriptor alloc)
            gendb::MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
            gendb::MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
            gendb::MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
            gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
            gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");

            // Prefetch both datasets concurrently via MADV_WILLNEED
            ps_partkey.prefetch();
            ps_suppkey.prefetch();
            ps_supplycost.prefetch();
            o_orderkey.prefetch();
            o_orderdate.prefetch();

            const size_t M_ps = ps_partkey.count;  // 8M
            const size_t M_o  = o_orderkey.count;   // 15M

            const int32_t* __restrict__ ppk_data = ps_partkey.data;
            const int32_t* __restrict__ psk_data = ps_suppkey.data;
            const int64_t* __restrict__ psc_data = ps_supplycost.data;
            const int32_t* __restrict__ ok_data  = o_orderkey.data;
            const int32_t* __restrict__ od_data  = o_orderdate.data;
            const uint8_t* __restrict__ pg_raw   = part_green_raw;
            int8_t*        __restrict__ oy_raw   = order_year_raw;

            // ---------------------------------------------------------------
            // FUSED SINGLE OMP PARALLEL REGION
            // First half of threads: partsupp scan
            // Second half of threads: orders scan
            // ---------------------------------------------------------------
            #pragma omp parallel num_threads(nthreads)
            {
                int tid = omp_get_thread_num();

                if (tid < half) {
                    // --- Partsupp threads: scan 8M rows, collect green matches ---
                    // Each thread scans its stripe of partsupp
                    const size_t chunk = (M_ps + half - 1) / half;
                    const size_t start = (size_t)tid * chunk;
                    const size_t end   = (start + chunk < M_ps) ? (start + chunk) : M_ps;

                    auto& lp = local_pairs[tid];
                    for (size_t i = start; i < end; i++) {
                        int32_t ppk = ppk_data[i];
                        if ((uint32_t)ppk < (uint32_t)MAX_PARTKEY && pg_raw[ppk]) {
                            int64_t key = ((int64_t)ppk << 17) | (int64_t)psk_data[i];
                            lp.emplace_back(key, psc_data[i]);
                        }
                    }
                } else {
                    // --- Orders threads: scan 15M rows, populate order_year ---
                    // Each thread scans its stripe of orders
                    const int orders_tid = tid - half;  // 0-based within orders group
                    const int orders_nthreads = nthreads - half;
                    const size_t chunk = (M_o + orders_nthreads - 1) / orders_nthreads;
                    const size_t start = (size_t)orders_tid * chunk;
                    const size_t end   = (start + chunk < M_o) ? (start + chunk) : M_o;

                    for (size_t i = start; i < end; i++) {
                        int32_t ok = ok_data[i];
                        if ((uint32_t)ok < (uint32_t)MAX_ORDERKEY) {
                            int yr = gendb::extract_year(od_data[i]);
                            if (yr >= MIN_YEAR && yr <= MAX_YEAR)
                                oy_raw[ok] = (int8_t)(yr - MIN_YEAR + 1);
                        }
                    }
                }
            } // end fused parallel region

            // Merge partsupp thread-local pairs into global map (sequential, ~192K entries)
            for (int t = 0; t < half; t++) {
                for (auto& [k, v] : local_pairs[t]) {
                    partsupp_map.insert(k, v);
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

                // Encode (partkey, suppkey) as single int64_t key (shift+or, matching build)
                int64_t ps_key = ((int64_t)lp << 17) | (int64_t)ls;
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
