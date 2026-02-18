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
 * OPTIMIZATION PLAN (iter_7):
 *
 *   ITER_2 STATUS: subquery_precompute=179ms (41%), orders_scan=18ms (4%),
 *                  dim_filter=4ms (1%), total=436ms
 *
 *   ROOT CAUSE OF BOTTLENECK:
 *   subquery_precompute used the pre-built lineitem_orderkey_hash index to drive
 *   RANDOM ACCESS into l_quantity.bin via a positions array (60M random reads).
 *   On HDD-backed mmap, random access causes repeated page faults / TLB misses,
 *   which is catastrophic for throughput (179ms = 41% of total time).
 *
 *   FIX: Replace index-driven random access with SEQUENTIAL SCAN of l_orderkey +
 *   l_quantity columns. Sequential I/O enables OS read-ahead; avoids random page
 *   faults entirely. Aggregate into a flat int64_t array indexed by orderkey
 *   (direct array lookup, O(1), no hash overhead). Use #pragma omp atomic for
 *   concurrent writes — contention is negligible (15M unique keys, avg 4 rows each).
 *
 *   PHYSICAL PLAN (iter_7):
 *
 *   Phase 1 (subquery_precompute):
 *     - Sequential mmap of l_orderkey.bin (60M × int32 = 240MB) and
 *       l_quantity.bin (60M × int64 = 480MB) with MADV_SEQUENTIAL.
 *     - Flat array qty_sum[15000001] (int64_t, 120MB), zero-initialized.
 *     - Parallel scan (64 threads, #pragma omp for schedule(static)):
 *         qty_sum[l_orderkey[i]] += l_quantity[i]  (#pragma omp atomic)
 *     - Atomic contention: 15M unique keys, avg 4 rows each → P(two threads
 *       same key simultaneously) ≈ 0.003% → essentially zero contention.
 *     - After parallel phase: sequential scan of qty_sum[] (15M × 8B = 120MB)
 *       to find qualifying orderkeys (sum > 30000). ~624 qualifying.
 *     - Insert qualifying entries into CompactHashMap<int32_t,int64_t> (size 2048).
 *
 *   Phase 2 (orders_scan): Parallel scan of 15M orders → qualifying_orders (~624).
 *   Phase 3 (dim_filter): Late materialization for customer names (~624 custkeys).
 *   Phase 4 (output): Build result, partial_sort top-100, write CSV.
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - orderkey range: 1..15000000; flat array of size 15000001 → direct indexing.
 *   - Sequential I/O: l_orderkey + l_quantity read sequentially → OS prefetch works.
 *   - Flat array replaces hash table aggregation → O(1) direct index, no hash probe.
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
    // Early prefetch: open and MADV_WILLNEED orders + customer columns
    // so I/O overlaps with Phase 1 (subquery_precompute).
    // ============================================================
    gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Issue prefetch for orders + customer columns — overlap HDD I/O with Phase 1
    mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_totalprice,
                      c_custkey_col, c_name_col);

    // ============================================================
    // Phase 1: Subquery precompute via SEQUENTIAL scan of lineitem.
    //
    //   Core change from iter_2: instead of index-driven random access into
    //   l_quantity.bin (60M random reads → HDD page faults), we do a direct
    //   sequential scan of l_orderkey + l_quantity columns. The OS prefetcher
    //   handles sequential reads efficiently; no random page faults.
    //
    //   Aggregation into a flat int64_t array (direct orderkey indexing):
    //   - qty_sum[orderkey] accumulates l_quantity for each orderkey.
    //   - O(1) write, no hash computation, no collision resolution.
    //   - #pragma omp atomic for safe concurrent writes (negligible contention
    //     with 15M unique keys and only 4 rows/key on average).
    //   - After scan: sequential pass over qty_sum[] (120MB) to find qualifiers.
    // ============================================================

    // qualifying_keys: orderkey -> sum_qty (scaled x100). ~624 entries expected.
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD   = 30000LL;   // 300 * 100 (l_quantity scale=100)
        const int32_t MAX_ORDERKEY = 15000000;

        // Open l_orderkey and l_quantity with sequential access hints for OS prefetch
        gendb::MmapColumn<int32_t> l_orderkey(li_dir + "l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");
        l_orderkey.advise_sequential();
        l_quantity.advise_sequential();

        const int64_t  LI_ROWS  = (int64_t)l_orderkey.count;
        const int32_t* ok_data  = l_orderkey.data;
        const int64_t* qty_data = l_quantity.data;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Flat aggregation array: qty_sum[k] = total l_quantity for orderkey k.
        // Size: 15000001 × 8B = 120MB. Fits comfortably in 376GB RAM.
        // Direct index access: no hash, no collision, O(1) per row.
        std::vector<int64_t> qty_sum(MAX_ORDERKEY + 1, 0LL);
        int64_t* qs = qty_sum.data();

        // Parallel sequential scan with atomic accumulation.
        // Each thread processes a static stripe of ~937K rows (60M / 64 threads).
        // Writes are to indices determined by l_orderkey values (spread across 0-15M),
        // so contention is extremely low (P ≈ 4/15M per pair of threads per row).
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int64_t i = 0; i < LI_ROWS; i++) {
            int32_t key = ok_data[i];
            int64_t qty = qty_data[i];
            #pragma omp atomic
            qs[key] += qty;
        }

        // Sequential scan of qty_sum[] to identify qualifying orderkeys.
        // This is a 120MB sequential read — very cache/prefetch friendly.
        for (int32_t k = 1; k <= MAX_ORDERKEY; k++) {
            if (qs[k] > THRESHOLD) {
                qualifying_keys.insert(k, qs[k]);
            }
        }
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
    //   ~624 qualifying orders → ~624 unique custkeys.
    //   Scan 1.5M customers with tiny probe set (~624).
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
        const int32_t* ck_data  = c_custkey_col.data;
        const int32_t* cn_data  = c_name_col.data;

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
