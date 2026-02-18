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
 * OPTIMIZATION PLAN (iter_8):
 *
 *   ITER_2 STATUS: subquery_precompute=179ms (41%), orders_scan=18ms (4%),
 *                  dim_filter=4ms (1%), total=436ms
 *
 *   ITER_3-7: All failed/regressed — using index random access approach.
 *
 *   ROOT CAUSE of 179ms: The pre-built lineitem_orderkey_hash index approach
 *   forces RANDOM ACCESS on l_quantity via positions[] array:
 *     - Index slots: 33.5M × 12B = ~402MB sequential (ok)
 *     - Positions array: 60M × 4B = 240MB sequential (ok)
 *     - l_quantity: 60M × 8B accessed in RANDOM order (BAD on HDD) = dominant bottleneck
 *   Even with software prefetch, random 60M accesses on HDD is slow.
 *
 *   KEY INSIGHT: Replace random-access index approach with SEQUENTIAL SCAN:
 *   - Scan l_orderkey (240MB) + l_quantity (480MB) sequentially = 720MB linear reads
 *   - Sequential HDD reads are much faster than random reads
 *   - Use shared flat atomic int64_t array[16M] for lock-free parallel aggregation
 *   - No index loading (saves 642MB of I/O), no random l_quantity access
 *
 *   PHYSICAL PLAN (iter_8):
 *
 *   Phase 0 (setup): Allocate flat array qty_sum[16M+1] = 0 (128MB).
 *     Issue MADV_WILLNEED on l_orderkey + l_quantity + orders + customer columns.
 *
 *   Phase 1 (subquery_precompute): Parallel sequential scan of l_orderkey + l_quantity.
 *     - 64 threads, static partitioning of 60M rows
 *     - Each thread: atomic_fetch_add on qty_sum[l_orderkey[i]] += l_quantity[i]
 *     - After parallel phase: single-pass over qty_sum[1..15M] to find entries > 30000
 *     - Build qualifying_keys CompactHashMap<int32_t,int64_t> (~624 entries)
 *
 *   Phase 2 (orders_scan): Parallel scan of 15M orders rows (unchanged from iter_2).
 *     - Probe qualifying_keys for each o_orderkey
 *     - Thread-local result vectors → merge
 *
 *   Phase 3 (dim_filter): Late materialization for customer names (unchanged from iter_2).
 *     - Build tiny custkey set from qualifying orders
 *     - Scan 1.5M customers for only needed custkeys
 *
 *   Phase 4 (output): Build result rows, top-100 sort, emit CSV.
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - orderkey range: 1..15000000. Array size: 15000001 entries × 8B = 120MB.
 *   - Using std::atomic<int64_t> for lock-free parallel accumulation.
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

    const std::string li_dir   = gendb_dir + "/lineitem/";
    const std::string ord_dir  = gendb_dir + "/orders/";
    const std::string cust_dir = gendb_dir + "/customer/";

    // ============================================================
    // Open all columns. MmapColumn defaults to MADV_SEQUENTIAL.
    // Fire prefetch on orders + customer early so HDD I/O overlaps
    // with Phase 1 lineitem scan.
    // ============================================================
    gendb::MmapColumn<int32_t> l_orderkey(li_dir + "l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");

    gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Prefetch orders + customer so I/O overlaps with Phase 1 lineitem scan
    mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_totalprice,
                      c_custkey_col, c_name_col);
    // Also prefetch lineitem columns for sequential reads
    l_orderkey.prefetch();
    l_quantity.prefetch();

    // ============================================================
    // Phase 1: Subquery precompute via SEQUENTIAL SCAN of lineitem.
    //
    //   Strategy: flat atomic array qty_sum[orderkey] avoids random I/O.
    //   orderkeys range 1..15000000. Array = 15000001 × 8B = ~120MB.
    //   Parallel: 64 threads do atomic_fetch_add — avg 4 updates/slot,
    //   low contention, fast sequential memory access pattern.
    // ============================================================
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD = 30000LL; // 300 * 100 (scale factor)
        const int64_t LI_ROWS = (int64_t)l_orderkey.count;

        // Flat array for per-orderkey quantity sum aggregation.
        // orderkeys are 1-based, max = 15000000. Index by orderkey directly.
        constexpr int32_t MAX_ORDERKEY = 15000000;
        // Use vector of atomic int64_t for lock-free parallel accumulation.
        std::vector<std::atomic<int64_t>> qty_sum(MAX_ORDERKEY + 1);
        // Initialize to zero (atomic default-constructs to 0 in C++20, but
        // use explicit init for portability)
        for (int32_t i = 0; i <= MAX_ORDERKEY; i++)
            qty_sum[i].store(0, std::memory_order_relaxed);

        const int32_t* ok_data  = l_orderkey.data;
        const int64_t* qty_data = l_quantity.data;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Parallel sequential scan: each thread processes a contiguous stripe
        // of lineitem rows. Sequential access pattern on both l_orderkey and
        // l_quantity — optimal for HDD readahead.
        #pragma omp parallel num_threads(nthreads)
        {
            #pragma omp for schedule(static)
            for (int64_t i = 0; i < LI_ROWS; i++) {
                int32_t okey = ok_data[i];
                // orderkeys are 1-based and <= 15M
                qty_sum[okey].fetch_add(qty_data[i], std::memory_order_relaxed);
            }
        }

        // Single-pass over qty_sum to collect qualifying orderkeys (> 30000).
        // ~15M entries, sequential, fast.
        for (int32_t k = 1; k <= MAX_ORDERKEY; k++) {
            int64_t total_qty = qty_sum[k].load(std::memory_order_relaxed);
            if (total_qty > THRESHOLD) {
                qualifying_keys.insert(k, total_qty);
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
    //   Build tiny custkey set → scan 1.5M customers once.
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

        int limit = std::min((int)all_rows.size(), 100);
        std::partial_sort(all_rows.begin(), all_rows.begin() + limit, all_rows.end(), q18_row_better);

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
