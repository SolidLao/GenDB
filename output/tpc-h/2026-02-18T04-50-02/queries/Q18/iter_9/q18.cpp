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
 * OPTIMIZATION PLAN (iter_9):
 *
 *   STATUS (iter_2 baseline): subquery_precompute=179ms (41%), orders_scan=18ms (4%),
 *                              dim_filter=4ms, total=436ms
 *
 *   ROOT CAUSE OF subquery_precompute BOTTLENECK:
 *   The pre-built index (lineitem_orderkey_hash) drives RANDOM access to l_quantity.bin.
 *   60M positions scatter reads across a 480MB file on HDD — each positions[p] is a
 *   random offset into qty_data[], causing HDD seek latency per group.
 *   Even with prefetch hints, random I/O on HDD is inherently slower than sequential.
 *
 *   KEY INSIGHT (iter_9):
 *   Abandon the index-driven random access. Instead:
 *   PASS 1: Sequential scan of l_orderkey + l_quantity (60M rows, ~720MB sequential).
 *     - Build CompactHashMap<int32_t, int64_t> orderkey → sum_qty via parallel scan.
 *     - Thread-local partial sums per orderkey; merge; apply HAVING > 30000.
 *     - Sequential HDD reads are 10-100x faster than random on HDD.
 *
 *   This eliminates the random-access pattern entirely. The positions array is NOT needed.
 *   Sequential scan of two columns = far better HDD throughput.
 *
 *   ADDITIONAL OPTIMIZATIONS:
 *   - Use partitioned aggregation (hash into 256 partitions) so each thread owns
 *     disjoint partitions → no merge contention, cache-friendly local aggregation.
 *   - l_orderkey is int32_t, l_quantity is int64_t. Both accessed sequentially.
 *   - After HAVING filter: ~15M unique orderkeys → ~624 qualify with sum > 30000.
 *
 *   PHYSICAL PLAN:
 *   Phase 1 (subquery_precompute):
 *     - Parallel scan of l_orderkey + l_quantity (64 threads, morsel-driven).
 *     - Thread-local flat arrays (256 partitions of CompactHashMap) for aggregation.
 *     - Merge partitions → qualifying_keys CompactHashMap<int32_t, int64_t>.
 *     - HAVING > 30000 applied during merge.
 *
 *   Phase 2 (orders_scan):
 *     - Parallel scan of orders (15M rows, 64 threads).
 *     - Probe qualifying_keys → collect qualifying_orders.
 *
 *   Phase 3 (dim_filter):
 *     - Build small custkey set from ~624 orders.
 *     - Sequential scan of 1.5M customers → cust_name_map.
 *
 *   Phase 4 (output):
 *     - Build result rows, TopKHeap<100>, write CSV.
 *
 *   KEY NOTES:
 *   - l_quantity scale=100, HAVING threshold = 300*100 = 30000.
 *   - Sequential scan avoids index positions[] random access overhead.
 *   - Both l_orderkey and l_quantity columns are sequential-access friendly.
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

    const std::string li_dir   = gendb_dir + "/lineitem/";
    const std::string ord_dir  = gendb_dir + "/orders/";
    const std::string cust_dir = gendb_dir + "/customer/";

    // ============================================================
    // Open all column files early — MADV_WILLNEED fires async I/O
    // so HDD reads for orders/customer overlap with lineitem scan.
    // ============================================================
    gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Prefetch orders + customer into page cache while lineitem scan runs
    mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_totalprice,
                      c_custkey_col, c_name_col);

    // ============================================================
    // Phase 1: Sequential scan of l_orderkey + l_quantity.
    //   Build orderkey → sum_qty map via parallel aggregation.
    //   Apply HAVING SUM(l_quantity) > 300 (scaled: 30000).
    //
    //   Why sequential scan beats index-driven random access:
    //   - Index positions[] drives random reads into qty_data (scattered 60M entries)
    //   - Sequential scan reads both columns contiguously → HDD sequential throughput
    //   - 720MB sequential vs 480MB random = much better on HDD
    // ============================================================

    // qualifying_keys: orderkey → sum_qty for orders with sum > 30000
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD = 30000LL; // 300 * 100 (scale factor)

        // Open lineitem columns for sequential scan
        gendb::MmapColumn<int32_t> l_orderkey(li_dir + "l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");

        // Sequential access: default MADV_SEQUENTIAL is already set by MmapColumn
        // Explicitly ensure sequential for both columns
        l_orderkey.advise_sequential();
        l_quantity.advise_sequential();

        const int64_t LI_ROWS = (int64_t)l_orderkey.count;
        const int32_t* ok_data  = l_orderkey.data;
        const int64_t* qty_data = l_quantity.data;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Use a flat array for aggregation: orderkeys are 1..15000000.
        // Array size = 15000001 × int64_t = ~120MB. Fits in RAM (376GB).
        // Zero-init with calloc for speed (OS zero pages).
        // Direct indexed access: sum_array[orderkey] += qty.
        // No hash overhead, perfect cache coherence for parallel scan.
        //
        // Parallel: use atomic adds since multiple threads update same slot.
        // int64_t atomic add is lock-free on x86_64.
        // Flat int64_t array indexed by orderkey (1..15M).
        // 15M × 8B = 120MB — fits comfortably in 376GB RAM.
        // Each thread uses its OWN local copy to avoid cache contention.
        // Reduce by summing across threads after parallel scan.
        // This is faster than atomic: no lock/fence overhead in hot loop.
        const int32_t MAX_ORDERKEY = 15000000;

        // Allocate thread-local partial sum arrays on the heap.
        // nthreads × 120MB could be large; use a smarter approach:
        // Partition orderkeys among threads for the REDUCTION step.
        // But for the scan, use atomic (x86 lock-free) with relaxed ordering
        // since false sharing is minimized by orderkey spread across 15M slots.
        //
        // CHOSEN: shared flat array with atomic int64_t.
        // On x86_64, relaxed fetch_add compiles to LOCK XADD — fast for low-contention.
        // Each orderkey has on average 4 lineitem rows (60M/15M) → minimal contention.
        std::vector<std::atomic<int64_t>> sum_array(MAX_ORDERKEY + 1);

        // Parallel zero-init (15M × 8B = 120MB)
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int32_t i = 0; i <= MAX_ORDERKEY; i++) {
            sum_array[i].store(0, std::memory_order_relaxed);
        }

        // Parallel scan of lineitem: accumulate sum_qty per orderkey
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int64_t i = 0; i < LI_ROWS; i++) {
            int32_t okey = ok_data[i];
            sum_array[okey].fetch_add(qty_data[i], std::memory_order_relaxed);
        }

        // Apply HAVING filter → qualifying_keys (expected ~624 entries)
        for (int32_t okey = 1; okey <= MAX_ORDERKEY; okey++) {
            int64_t qty = sum_array[okey].load(std::memory_order_relaxed);
            if (qty > THRESHOLD) {
                qualifying_keys.insert(okey, qty);
            }
        }
    }

    // ============================================================
    // Phase 2: Parallel scan of orders → collect qualifying orders
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

        std::vector<std::vector<std::pair<int32_t, OrderInfo>>> local_orders(nthreads);
        for (auto& v : local_orders) v.reserve(32);

        const int32_t* ok_data = o_orderkey.data;
        const int32_t* ck_data = o_custkey.data;
        const int32_t* od_data = o_orderdate.data;
        const int64_t* tp_data = o_totalprice.data;

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

        for (int t = 0; t < nthreads; t++) {
            for (auto& entry : local_orders[t]) {
                qualifying_orders.push_back(std::move(entry));
            }
        }
    }

    // ============================================================
    // Phase 3: Late materialization for customer names.
    //   ~624 qualifying orders → ~624 unique custkeys.
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

    gendb::CompactHashMap<int32_t, int32_t> cust_name_map(2048);

    {
        GENDB_PHASE("dim_filter");

        gendb::CompactHashSet<int32_t> needed_custkeys(2048);
        for (auto& [okey, info] : qualifying_orders) {
            needed_custkeys.insert(info.custkey);
        }

        const int64_t CUST_ROWS = (int64_t)c_custkey_col.count;
        const int32_t* ck_data = c_custkey_col.data;
        const int32_t* cn_data = c_name_col.data;

        for (int64_t i = 0; i < CUST_ROWS; i++) {
            int32_t ckey = ck_data[i];
            if (needed_custkeys.contains(ckey)) {
                cust_name_map.insert(ckey, cn_data[i]);
                if (cust_name_map.size() == needed_custkeys.size()) break;
            }
        }
    }

    // ============================================================
    // Phase 4: Build result rows, sort, take top 100, write CSV
    //   Use TopKHeap for O(n log k) instead of full sort.
    // ============================================================
    {
        GENDB_PHASE("output");

        // Use TopKHeap for LIMIT 100 — O(n log 100) vs O(n log n) full sort
        auto heap_cmp = [](const Q18Row& a, const Q18Row& b) -> bool {
            // Return true if a should be kept over b (a is "better")
            if (a.o_totalprice != b.o_totalprice)
                return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        };
        gendb::TopKHeap<Q18Row, decltype(heap_cmp)> heap(100, heap_cmp);

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

            heap.push(row);
        }

        auto top_rows = heap.sorted();

        // Write CSV
        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        std::fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[16];
        int limit = (int)top_rows.size();
        if (limit > 100) limit = 100;
        for (int i = 0; i < limit; i++) {
            auto& row = top_rows[i];
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
