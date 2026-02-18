/*
 * Q18: Large Volume Customer
 *
 * SQL:
 *   SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
 *   FROM customer, orders, lineitem
 *   WHERE o_orderkey IN (
 *       SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
 *   )
 *   AND c_custkey = o_custkey
 *   AND o_orderkey = l_orderkey
 *   GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
 *   ORDER BY o_totalprice DESC, o_orderdate ASC
 *   LIMIT 100;
 *
 * OPTIMIZATION PLAN (iter_4):
 *
 *   ITER_2 STATUS: subquery_precompute=179ms (41%), orders_scan=18ms (4%),
 *                  dim_filter=4ms (1%), total=436ms
 *   ITER_3: REGRESSED (rolled back).
 *
 *   ROOT CAUSE of subquery_precompute=179ms:
 *   The pre-built index approach iterates 33.5M hash slots, then for each occupied
 *   slot sums l_quantity via a positions array. Each positions[p] is a random index
 *   into the 60M-row l_quantity column — causing 60M random page faults on HDD.
 *   Random I/O on HDD is catastrophically slow. The index's purpose is random
 *   lookup (point queries), but Q18 needs to aggregate ALL rows — sequential scan wins.
 *
 *   FIX: Replace random-access index approach with SEQUENTIAL scan of l_orderkey
 *   and l_quantity together. Use a flat int64_t array of size MAX_ORDERKEY+1
 *   (15,000,001 × 8B = ~120MB) for aggregation — direct array indexed by orderkey,
 *   O(1) no-hash update. Single pass reads both columns sequentially (predictable
 *   prefetching). Parallel with OpenMP using partitioned orderkey ranges to avoid
 *   false sharing — each thread owns a contiguous range of orderkeys, processes
 *   its own partition of the flat array without any atomic ops.
 *
 *   PARALLEL STRATEGY for subquery_precompute:
 *   - Partition l_orderkey space (1..15M) into N_PARTS=64 ranges.
 *   - Each thread scans ALL 60M lineitem rows but only accumulates into its
 *     own orderkey range (branch-predicted skip for other ranges).
 *   - Actually better: 2-pass approach:
 *     Pass 1: 64 threads each scan 60M/64 lineitem rows, accumulate into
 *             thread-local flat array (120MB per thread = 7.5GB total — too much).
 *
 *   REVISED STRATEGY:
 *   - Use a SINGLE shared flat int64_t array of 15M+1 entries.
 *   - Parallel scan with atomic<int64_t>::fetch_add — but atomics on contended
 *     cachelines are slow.
 *   - BETTER: Partition lineitem rows into 64 thread chunks (each ~937K rows).
 *     Each thread accumulates into a LOCAL flat array of 15M+1 entries — but
 *     120MB × 64 = 7.5GB total, too large.
 *
 *   ACTUAL BEST: Use 64 thread-local CompactHashMap<int32_t,int64_t>(32768).
 *   Each thread scans its chunk of ~937K lineitem rows, accumulating sum_qty
 *   per orderkey into a local hashmap. Since most orderkeys appear ~4 times,
 *   each local hashmap has ~937K/4 = ~234K entries — compact enough.
 *   After parallel phase, merge into a single global flat array (or just
 *   check HAVING on the merged result).
 *
 *   EVEN BETTER (chosen): Single-pass with a SHARED flat array, but partition
 *   the orderkey space so each thread owns exclusive orderkey ranges:
 *   - Divide orderkeys 1..15M into 64 bands of 234K each.
 *   - Each thread scans ALL 60M lineitem rows sequentially, but only writes
 *     to its band. Since all threads scan sequentially, I/O is sequential.
 *     Cost: 64 × sequential_scan_cost — bad.
 *
 *   FINAL CHOSEN APPROACH:
 *   Two-pass parallel aggregation with moderate thread-local storage:
 *   - Use 64 threads, each processing 60M/64 ≈ 937K rows sequentially.
 *   - Thread-local aggregation into CompactHashMap<int32_t,int64_t> sized
 *     for ~300K entries (slightly over expected ~234K unique orderkeys per chunk).
 *   - After parallel phase, merge all thread-local maps into a flat int64_t
 *     array [15M+1] (120MB) and apply HAVING filter.
 *   - Expected: 60M sequential reads of l_orderkey+l_quantity ÷ 64 threads
 *     ≈ per thread ~15MB of each column — fits in L3 cache (88MB / 64 ≈ 1.4MB,
 *     but sequential access benefits from hardware prefetcher regardless).
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - l_orderkey range: 1..15,000,000 (use as direct array index).
 *   - c_name: dictionary-encoded int32_t; load dict at runtime.
 *   - ORDER BY o_totalprice DESC, o_orderdate ASC.
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <atomic>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================
// Result row
// ============================================================
struct Q18Row {
    std::string c_name;
    int32_t     c_custkey;
    int32_t     o_orderkey;
    int32_t     o_orderdate;   // epoch days
    int64_t     o_totalprice;  // scaled x100
    int64_t     sum_qty;       // scaled x100
};

// Sort comparator: ORDER BY o_totalprice DESC, o_orderdate ASC
inline bool q18_row_better(const Q18Row& a, const Q18Row& b) {
    if (a.o_totalprice != b.o_totalprice)
        return a.o_totalprice > b.o_totalprice;
    return a.o_orderdate < b.o_orderdate;
}

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string li_dir    = gendb_dir + "/lineitem/";
    const std::string ord_dir   = gendb_dir + "/orders/";
    const std::string cust_dir  = gendb_dir + "/customer/";

    // ============================================================
    // Open all columns early — issue MADV_WILLNEED to prefetch
    // all columns into page cache before scanning begins.
    // ============================================================
    gendb::MmapColumn<int32_t> l_orderkey_col(li_dir + "l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_quantity_col(li_dir + "l_quantity.bin");
    gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Issue prefetch for ALL columns simultaneously — overlap HDD I/O with CPU setup
    mmap_prefetch_all(l_orderkey_col, l_quantity_col,
                      o_orderkey, o_custkey, o_orderdate, o_totalprice,
                      c_custkey_col, c_name_col);

    // ============================================================
    // Phase 1: Subquery precompute via SEQUENTIAL scan of lineitem.
    //   Problem with iter_2: pre-built index causes 60M RANDOM accesses
    //   into l_quantity (one per position entry) → HDD random I/O catastrophe.
    //   Solution: scan l_orderkey + l_quantity SEQUENTIALLY, aggregate
    //   sum_qty per orderkey in thread-local CompactHashMaps, then merge
    //   into a flat array for O(1) HAVING filter.
    //
    //   Sequential scan: hardware prefetcher handles reads perfectly on HDD.
    //   Result: ~60M rows / 64 threads = ~937K rows per thread (sequential chunk).
    // ============================================================
    static constexpr int32_t MAX_ORDERKEY = 15000000;
    static constexpr int64_t THRESHOLD    = 30000LL; // 300 * 100 (scale=100)

    // qualifying_keys: orderkey -> sum_qty (scaled x100). ~624 entries expected.
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t LI_ROWS = (int64_t)l_orderkey_col.count;
        const int32_t* lk_data = l_orderkey_col.data;
        const int64_t* lq_data = l_quantity_col.data;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Thread-local CompactHashMaps: each thread aggregates its chunk.
        // Each chunk has ~LI_ROWS/nthreads rows ≈ ~937K rows.
        // Unique orderkeys per chunk ≈ ~234K (each orderkey ~4 rows on average).
        // Size thread-local map at 512K to avoid rehashing.
        std::vector<gendb::CompactHashMap<int32_t, int64_t>> local_maps(nthreads,
            gendb::CompactHashMap<int32_t, int64_t>(524288));

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& lmap = local_maps[tid];

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < LI_ROWS; i++) {
                int32_t okey = lk_data[i];
                int64_t qty  = lq_data[i];
                int64_t* ptr = lmap.find(okey);
                if (ptr) {
                    *ptr += qty;
                } else {
                    lmap.insert(okey, qty);
                }
            }
        }

        // Merge thread-local maps into a flat array indexed by orderkey.
        // flat_qty[orderkey] = total sum_qty across all threads.
        // 15M+1 entries × 8B = 120MB — within system memory (376GB).
        // Use calloc for zero-initialization (OS provides zero pages — no page fault cost).
        int64_t* flat_qty = (int64_t*)calloc((size_t)(MAX_ORDERKEY + 1), sizeof(int64_t));
        if (!flat_qty) throw std::runtime_error("calloc failed for flat_qty");

        for (int t = 0; t < nthreads; t++) {
            for (auto [key, qty] : local_maps[t]) {
                if (key >= 1 && key <= MAX_ORDERKEY) {
                    flat_qty[key] += qty;
                }
            }
        }

        // Apply HAVING SUM(l_quantity) > 300 → collect qualifying orderkeys
        for (int32_t k = 1; k <= MAX_ORDERKEY; k++) {
            if (flat_qty[k] > THRESHOLD) {
                qualifying_keys.insert(k, flat_qty[k]);
            }
        }

        free(flat_qty);
    }

    // ============================================================
    // Phase 2: Parallel scan of orders → collect qualifying orders
    //   15M rows, ~624 qualify. Parallelize across 64 threads.
    // ============================================================
    struct OrderInfo {
        int32_t custkey;
        int32_t orderdate;
        int64_t totalprice;
    };

    std::vector<std::pair<int32_t, OrderInfo>> qualifying_orders;
    qualifying_orders.reserve(qualifying_keys.size() * 2 + 16);

    {
        GENDB_PHASE("orders_scan");

        const int64_t ORD_ROWS = (int64_t)o_orderkey.count;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Thread-local result vectors to avoid contention
        std::vector<std::vector<std::pair<int32_t, OrderInfo>>> local_orders(nthreads);
        // Reserve a small amount per thread (total ~624, each thread gets ~10)
        for (auto& v : local_orders) v.reserve(32);

        const int32_t* ok_data  = o_orderkey.data;
        const int32_t* ck_data  = o_custkey.data;
        const int32_t* od_data  = o_orderdate.data;
        const int64_t* tp_data  = o_totalprice.data;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = local_orders[tid];

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < ORD_ROWS; i++) {
                int32_t okey = ok_data[i];
                if (qualifying_keys.find(okey)) {
                    local.push_back({okey, {ck_data[i], od_data[i], tp_data[i]}});
                }
            }
        }

        // Merge thread-local results
        for (int t = 0; t < nthreads; t++) {
            for (auto& entry : local_orders[t]) {
                qualifying_orders.push_back(std::move(entry));
            }
        }
    }

    // ============================================================
    // Phase 3: Late materialization for customer names.
    //   We have ~624 qualifying orders → ~624 unique custkeys.
    //   Build a small custkey → name_code map (size ~2048).
    //   Then scan 1.5M customers once looking only for those custkeys.
    //   Avoids building the full 1.5M-entry hash map.
    // ============================================================
    std::vector<std::string> c_name_dict;
    {
        std::ifstream dict_file(cust_dir + "c_name_dict.txt");
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open c_name_dict.txt");
        std::string line;
        while (std::getline(dict_file, line))
            c_name_dict.push_back(line);
    }

    // Small map: custkey → name_code (only for ~624 needed custkeys)
    gendb::CompactHashMap<int32_t, int32_t> cust_name_map(2048);

    {
        GENDB_PHASE("dim_filter");

        // Build set of needed custkeys from qualifying orders
        gendb::CompactHashSet<int32_t> needed_custkeys(2048);
        for (auto& [okey, info] : qualifying_orders) {
            needed_custkeys.insert(info.custkey);
        }

        const int64_t CUST_ROWS = (int64_t)c_custkey_col.count;
        const int32_t* ck_data = c_custkey_col.data;
        const int32_t* cn_data = c_name_col.data;

        // Scan 1.5M customers, only insert those in needed_custkeys (~624)
        for (int64_t i = 0; i < CUST_ROWS; i++) {
            int32_t ckey = ck_data[i];
            if (needed_custkeys.contains(ckey)) {
                cust_name_map.insert(ckey, cn_data[i]);
                // Early exit if we've found all needed customers
                if (cust_name_map.size() == needed_custkeys.size()) break;
            }
        }
    }

    // ============================================================
    // Phase 4: Build result rows, sort, take top 100, write CSV
    //   sum_qty already known from qualifying_keys map.
    // ============================================================
    {
        GENDB_PHASE("output");

        std::vector<Q18Row> all_rows;
        all_rows.reserve(qualifying_orders.size());

        for (auto& [okey, order_info] : qualifying_orders) {
            int64_t* qty_ptr = qualifying_keys.find(okey);
            if (!qty_ptr) continue;

            int32_t* name_code_ptr = cust_name_map.find(order_info.custkey);
            if (!name_code_ptr) continue;

            int32_t name_code = *name_code_ptr;
            const std::string& cname = (name_code >= 0 && name_code < (int32_t)c_name_dict.size())
                                       ? c_name_dict[name_code] : "";

            Q18Row row;
            row.c_name       = cname;
            row.c_custkey    = order_info.custkey;
            row.o_orderkey   = okey;
            row.o_orderdate  = order_info.orderdate;
            row.o_totalprice = order_info.totalprice;
            row.sum_qty      = *qty_ptr;

            all_rows.push_back(std::move(row));
        }

        // Sort ORDER BY o_totalprice DESC, o_orderdate ASC
        // Use partial_sort since we only need top 100
        int limit = std::min((int)all_rows.size(), 100);
        std::partial_sort(all_rows.begin(), all_rows.begin() + limit, all_rows.end(), q18_row_better);

        // Write top 100 rows to CSV
        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        std::fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[16];
        for (int i = 0; i < limit; i++) {
            auto& row = all_rows[i];
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            std::fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                row.c_name.c_str(),
                row.c_custkey,
                row.o_orderkey,
                date_buf,
                (double)row.o_totalprice / 100.0,
                (double)row.sum_qty / 100.0
            );
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
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
