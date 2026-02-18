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
 * LOGICAL PLAN:
 *   Step 1: Subquery — scan lineitem, group by l_orderkey, SUM(l_quantity),
 *           HAVING SUM > 300*100=30000 → qualifying_orderkeys (CompactHashSet, ~624 keys)
 *   Step 2: Scan orders, filter by qualifying_orderkeys → ~624 qualifying orders vector
 *   Step 3: Build customer name lookup: CompactHashMap<int32_t, int32_t> (custkey→name_code)
 *           Load c_name_dict.txt for final output
 *   Step 4: Second lineitem scan — accumulate SUM(l_quantity) per qualifying orderkey
 *           CompactHashMap<int32_t, int64_t> (orderkey → qty_sum), parallel
 *   Step 5: Build result rows, TopKHeap sort, LIMIT 100, write CSV
 *
 * PHYSICAL PLAN:
 *   - Subquery: Parallel (64 threads), thread-local CompactHashMaps, merge → filter HAVING
 *   - Orders scan: Sequential (15M rows, probe qualifying set)
 *   - Customer: Sequential scan → CompactHashMap (1.5M rows)
 *   - Final scan: Parallel thread-local qty accumulation, sequential merge
 *   - TopKHeap<Q18Row,Q18Cmp>(100) for ORDER BY + LIMIT
 *
 * KEY NOTES:
 *   - l_quantity: int64_t scale=100. Threshold = 300*100 = 30000 (HAVING SUM > 300).
 *   - o_totalprice: int64_t scale=100.
 *   - o_orderdate: int32_t epoch days.
 *   - c_name: dictionary-encoded int32_t; load dict at runtime (NEVER hardcode codes).
 *   - ORDER BY o_totalprice DESC, o_orderdate ASC (earlier date = better on tie).
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
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
// Returns true if a should come before b in the final sorted output
inline bool q18_row_better(const Q18Row& a, const Q18Row& b) {
    if (a.o_totalprice != b.o_totalprice)
        return a.o_totalprice > b.o_totalprice; // higher price first
    return a.o_orderdate < b.o_orderdate;       // earlier date first on tie
}

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string li_dir   = gendb_dir + "/lineitem/";
    const std::string ord_dir  = gendb_dir + "/orders/";
    const std::string cust_dir = gendb_dir + "/customer/";

    // Open lineitem columns (used twice: subquery + final scan)
    gendb::MmapColumn<int32_t> l_orderkey(li_dir + "l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");
    const int64_t LI_ROWS = (int64_t)l_orderkey.count;

    // Prefetch both lineitem columns early (HDD: overlap I/O with setup)
    mmap_prefetch_all(l_orderkey, l_quantity);

    // ============================================================
    // Phase 1: Subquery — SUM(l_quantity) per l_orderkey, HAVING > 300
    //          Parallel thread-local aggregation + sequential merge
    // ============================================================
    gendb::CompactHashSet<int32_t> qualifying_orderkeys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD = 30000LL; // 300 * scale_factor(100)

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        std::vector<gendb::CompactHashMap<int32_t, int64_t>> local_maps(nthreads);
        for (auto& m : local_maps) m.reserve(1 << 19); // 512K capacity per thread

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = local_maps[tid];

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < LI_ROWS; i++) {
                int32_t key = l_orderkey.data[i];
                int64_t qty = l_quantity.data[i];
                int64_t* slot = local.find(key);
                if (slot) {
                    *slot += qty;
                } else {
                    local.insert(key, qty);
                }
            }
        }

        // Sequential merge into global aggregation map
        gendb::CompactHashMap<int32_t, int64_t> global_agg(1 << 24);
        for (int t = 0; t < nthreads; t++) {
            for (auto [key, val] : local_maps[t]) {
                int64_t* slot = global_agg.find(key);
                if (slot) {
                    *slot += val;
                } else {
                    global_agg.insert(key, val);
                }
            }
        }

        // HAVING filter: SUM(l_quantity) > 300 (threshold = 30000 scaled)
        for (auto [key, val] : global_agg) {
            if (val > THRESHOLD) {
                qualifying_orderkeys.insert(key);
            }
        }
    }

    // ============================================================
    // Phase 2: Scan orders, filter by qualifying_orderkeys → collect qualifying orders
    // ============================================================
    struct OrderInfo {
        int32_t custkey;
        int32_t orderdate;
        int64_t totalprice;
    };

    std::vector<std::pair<int32_t, OrderInfo>> qualifying_orders;
    qualifying_orders.reserve(qualifying_orderkeys.size() * 2);

    {
        GENDB_PHASE("orders_scan");

        gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(ord_dir + "o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
        gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");

        const int64_t ORD_ROWS = (int64_t)o_orderkey.count;

        for (int64_t i = 0; i < ORD_ROWS; i++) {
            int32_t okey = o_orderkey.data[i];
            if (qualifying_orderkeys.contains(okey)) {
                qualifying_orders.push_back({okey, {
                    o_custkey.data[i],
                    o_orderdate.data[i],
                    o_totalprice.data[i]
                }});
            }
        }
    }

    // ============================================================
    // Phase 3: Build customer name lookup (custkey → dict code)
    //          Load c_name dictionary
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

    gendb::CompactHashMap<int32_t, int32_t> cust_name_map(1600000);
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(cust_dir + "c_custkey.bin");
        gendb::MmapColumn<int32_t> c_name(cust_dir + "c_name.bin");

        const int64_t CUST_ROWS = (int64_t)c_custkey.count;
        for (int64_t i = 0; i < CUST_ROWS; i++) {
            cust_name_map.insert(c_custkey.data[i], c_name.data[i]);
        }
    }

    // ============================================================
    // Phase 4: Main scan — SUM(l_quantity) per qualifying orderkey
    //          Parallel thread-local accumulation (only qualifying rows pass)
    // ============================================================
    gendb::CompactHashMap<int32_t, int64_t> orderkey_qty(qualifying_orderkeys.size() * 2 + 16);

    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Thread-local maps for qualifying keys only
        std::vector<gendb::CompactHashMap<int32_t, int64_t>> local_qty(nthreads);
        for (auto& m : local_qty)
            m.reserve(qualifying_orders.size() * 2 + 16);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = local_qty[tid];

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < LI_ROWS; i++) {
                int32_t key = l_orderkey.data[i];
                // Filter: only process rows matching qualifying orderkeys
                if (qualifying_orderkeys.contains(key)) {
                    int64_t* slot = local.find(key);
                    if (slot) {
                        *slot += l_quantity.data[i];
                    } else {
                        local.insert(key, l_quantity.data[i]);
                    }
                }
            }
        }

        // Sequential merge: accumulate all thread contributions
        for (int t = 0; t < nthreads; t++) {
            for (auto [key, val] : local_qty[t]) {
                int64_t* slot = orderkey_qty.find(key);
                if (slot) {
                    *slot += val;
                } else {
                    orderkey_qty.insert(key, val);
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Build all result rows, sort, take top 100, write CSV
    // Note: qualifying_orders has ~624 rows — sort is trivial
    // ============================================================
    {
        GENDB_PHASE("output");

        std::vector<Q18Row> all_rows;
        all_rows.reserve(qualifying_orders.size());

        for (auto& [okey, order_info] : qualifying_orders) {
            int64_t* qty_ptr = orderkey_qty.find(okey);
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
        std::sort(all_rows.begin(), all_rows.end(), q18_row_better);

        // Write top 100 rows to CSV
        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        std::fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[16];
        int limit = std::min((int)all_rows.size(), 100);
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
