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
 * === PHYSICAL PLAN (iter_10) ===
 *
 * Bottleneck analysis (iter_8):
 *   dim_filter = 268ms (wraps everything), main_scan = 36ms.
 *   Phase 1c bottleneck: p_name_dict.txt is 67MB with ~2M lines.
 *   Current code uses std::getline into std::string + string::find("green")
 *   for each of 2M dictionary entries → 2M heap allocations + string ops ≈ 100-150ms!
 *
 * Key optimization (iter_10):
 *   1. MMAP p_name_dict.txt as raw bytes + memmem() per-line scan:
 *      Zero string allocations. Scan 67MB of bytes sequentially.
 *      Replace: while(getline) {str.find("green")} → raw byte scan with memmem().
 *      Expected savings: ~100-150ms → brings dim_filter overhead from 232ms to ~80-130ms.
 *
 *   2. REBALANCED thread split for fused OMP region:
 *      Old: half=32 threads to partsupp (8M), half=32 to orders (15M)
 *      → orders threads process 15M/32=469K rows each, partsupp 8M/32=250K rows.
 *      → orders is the straggler (1.88x more rows per thread).
 *      New: partition threads proportionally to row counts:
 *        orders_frac = 15/(15+8) ≈ 0.652 → ~42 threads for orders, ~22 for partsupp
 *        → orders: 15M/42=357K rows/thread, partsupp: 8M/22=364K rows/thread → balanced!
 *      Expected savings: orders stall time × (1 - 357/469) ≈ 24% of Phase 2 time.
 *
 *   3. EARLY PREFETCH of lineitem files:
 *      Issue MADV_WILLNEED for all 6 lineitem columns before Phase 2 begins.
 *      On HDD, this gives the OS ~200ms of head start before Phase 3 scan.
 *      lineitem total: 6 columns × ~240MB = ~1.4GB → HDD benefits greatly.
 *
 * Phase 1: Sequential (nation 25, supplier 100K), part 2M OMP
 *   → Part dict: raw mmap of p_name_dict.txt, memmem scan, no string allocs
 * Phase 2: Single OMP parallel region, thread-split:
 *   Threads 0..ps_threads-1: partsupp scan (8M rows, thread-local pairs, merge)
 *   Threads ps_threads..N-1: orders scan (15M rows, direct write to flat array)
 *   Thread split: ps_threads ≈ N*8/(8+15) ≈ N*8/23
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
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
        //
        // KEY OPTIMIZATION: Instead of std::getline into std::string + string::find()
        // (2M heap allocations + 2M string searches on a 67MB file), we:
        //   1. mmap p_name_dict.txt as raw bytes (zero-copy)
        //   2. Scan linearly with memmem() to find "green" per newline-delimited entry
        //   3. Build name_has_green as uint8_t array indexed by dict code
        //   4. No string allocations whatsoever — pure byte scan
        //
        // This avoids 2M std::string constructions and the per-string heap overhead,
        // replacing them with a single sequential mmap read + memmem calls.

        uint8_t* part_green_raw = (uint8_t*)calloc(MAX_PARTKEY, sizeof(uint8_t));
        struct FreeOnExit { uint8_t* p; ~FreeOnExit() { free(p); } } _pg_guard{part_green_raw};

        // Open and mmap p_name_dict.txt for zero-copy raw byte scanning
        std::string dict_path = gendb_dir + "/part/p_name_dict.txt";
        int dict_fd = ::open(dict_path.c_str(), O_RDONLY);
        if (dict_fd < 0) {
            std::fprintf(stderr, "Cannot open %s\n", dict_path.c_str());
            return;
        }
        struct stat dict_st;
        fstat(dict_fd, &dict_st);
        size_t dict_size = dict_st.st_size;

        // Advise sequential: file is ~67MB, will be read front-to-back
        const char* dict_bytes = nullptr;
        if (dict_size > 0) {
            void* ptr = mmap(nullptr, dict_size, PROT_READ, MAP_PRIVATE, dict_fd, 0);
            if (ptr == MAP_FAILED) {
                ::close(dict_fd);
                std::fprintf(stderr, "Cannot mmap %s\n", dict_path.c_str());
                return;
            }
            madvise(ptr, dict_size, MADV_SEQUENTIAL);
            dict_bytes = static_cast<const char*>(ptr);
        }
        ::close(dict_fd);

        // Count dict entries and build name_has_green via raw byte scan
        // Simultaneously: count lines (= dict entries) and check for "green"
        static const char GREEN[] = "green";
        static const size_t GREEN_LEN = 5;

        std::vector<uint8_t> name_has_green;
        // Pre-reserve: we know there are ~2M entries
        name_has_green.reserve(2000000);

        if (dict_bytes && dict_size > 0) {
            const char* p = dict_bytes;
            const char* end = dict_bytes + dict_size;
            while (p < end) {
                // Find next newline
                const char* nl = (const char*)memchr(p, '\n', end - p);
                const char* line_end = nl ? nl : end;
                size_t line_len = line_end - p;
                // Check if "green" appears in this line using memmem
                bool has_green = (line_len >= GREEN_LEN) &&
                                 (memmem(p, line_len, GREEN, GREEN_LEN) != nullptr);
                name_has_green.push_back(has_green ? 1 : 0);
                p = nl ? nl + 1 : end;
            }
            munmap((void*)dict_bytes, dict_size);
        }

        // Early prefetch of lineitem columns (HDD: ~1.4GB total, start I/O now
        // so it overlaps with Phase 2 CPU work before Phase 3 scan begins)
        gendb::MmapColumn<int32_t> l_orderkey_pre(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int32_t> l_partkey_pre(gendb_dir + "/lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey_pre(gendb_dir + "/lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice_pre(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount_pre(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity_pre(gendb_dir + "/lineitem/l_quantity.bin");
        // Issue MADV_WILLNEED now to let the OS start prefetching lineitem from HDD
        // while we finish Phase 1 and run Phase 2
        l_orderkey_pre.prefetch();
        l_partkey_pre.prefetch();
        l_suppkey_pre.prefetch();
        l_extendedprice_pre.prefetch();
        l_discount_pre.prefetch();
        l_quantity_pre.prefetch();

        // Now scan part table in parallel to build part_green_raw
        std::atomic<size_t> green_count_atomic{0};
        {
            gendb::MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");
            gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");

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
        // REBALANCED thread split: proportional to row counts
        //   partsupp = 8M rows, orders = 15M rows, total = 23M
        //   ps_threads = nthreads * 8/23 ≈ nthreads * 0.348
        //   orders_threads = nthreads - ps_threads ≈ nthreads * 0.652
        //
        // Old: 32:32 → orders had 1.88x more rows per thread (straggler)
        // New: ~22:42 → roughly equal rows per thread → no straggler
        //
        // Key encoding: partkey<<17 | suppkey (suppkey ≤ 100K < 2^17=131072)

        // order_year: calloc for lazy OS zero-page (faster than memset over 60MB)
        // Stores (year - MIN_YEAR + 1): range 1..7, 0 = absent
        int8_t* order_year_raw = (int8_t*)calloc(MAX_ORDERKEY, sizeof(int8_t));
        struct FreeOrderYear { int8_t* p; ~FreeOrderYear() { free(p); } } _oy_guard{order_year_raw};

        // partsupp_map: ~192K green entries (2M parts × 2.4% green × 4 suppliers)
        gendb::CompactHashMap<int64_t, int64_t> partsupp_map(green_count * 4 + 1024);

        {
            const int nthreads = omp_get_max_threads();

            // Proportional split: ps_threads processes partsupp (8M rows),
            // remaining threads process orders (15M rows).
            // Optimal: ps_threads/orders_threads = 8/15 => ps_threads = nthreads*8/23
            // We round, ensuring at least 1 thread per group.
            const int ps_threads = std::max(1, std::min(nthreads - 1,
                                            (int)((int64_t)nthreads * 8 / 23)));
            const int orders_threads = nthreads - ps_threads;

            // Pre-allocate thread-local collectors for partsupp threads
            std::vector<std::vector<std::pair<int64_t,int64_t>>> local_pairs(ps_threads);
            {
                size_t reserve_per = (green_count * 4 + ps_threads - 1) / ps_threads + 64;
                for (auto& lp : local_pairs) lp.reserve(reserve_per);
            }

            // Open all files BEFORE the parallel region to avoid file-open overhead
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
            // FUSED SINGLE OMP PARALLEL REGION — rebalanced thread split
            // Threads [0, ps_threads): handle partsupp (8M rows)
            // Threads [ps_threads, nthreads): handle orders (15M rows)
            // Each group gets proportional share of threads to balance workload.
            // ---------------------------------------------------------------
            #pragma omp parallel num_threads(nthreads)
            {
                int tid = omp_get_thread_num();

                if (tid < ps_threads) {
                    // --- Partsupp threads: scan 8M rows, collect green matches ---
                    const size_t chunk = (M_ps + ps_threads - 1) / ps_threads;
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
                    const int orders_tid = tid - ps_threads;  // 0-based within orders group
                    const size_t chunk = (M_o + orders_threads - 1) / orders_threads;
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

            // Merge partsupp thread-local pairs into global map (sequential, ~768K entries)
            for (int t = 0; t < ps_threads; t++) {
                for (auto& [k, v] : local_pairs[t]) {
                    partsupp_map.insert(k, v);
                }
            }
        }

        // =========================================================================
        // Phase 3: Main lineitem scan — parallel OpenMP with thread-local agg
        // =========================================================================
        // Reuse already-opened lineitem mmap columns (prefetched above)
        // amount_scaled (x100) = ep*(100-disc)/100 - sc*qty/100
        // Accumulate as int64_t: agg[nation_idx * NUM_YEARS + year_idx]

        const size_t N = l_orderkey_pre.count;
        const int num_threads = omp_get_max_threads();

        // Thread-local accumulators: [thread][nation_idx * NUM_YEARS + year_idx]
        // NUM_NATIONS * NUM_YEARS = 175 int64_t = 1400B — fits in L1 per thread
        static constexpr int AGG_SIZE = NUM_NATIONS * NUM_YEARS;
        std::vector<std::array<int64_t, AGG_SIZE>> tl_profit(num_threads);
        for (int t = 0; t < num_threads; t++)
            tl_profit[t].fill(0);

        // Cache raw pointers for hot loop — avoids repeated member dereference
        const int32_t* __restrict__ lp_data   = l_partkey_pre.data;
        const int32_t* __restrict__ ls_data   = l_suppkey_pre.data;
        const int32_t* __restrict__ lok_data  = l_orderkey_pre.data;
        const int64_t* __restrict__ ep_data   = l_extendedprice_pre.data;
        const int64_t* __restrict__ disc_data = l_discount_pre.data;
        const int64_t* __restrict__ qty_data  = l_quantity_pre.data;
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
