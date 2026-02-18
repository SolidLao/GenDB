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
 * OPTIMIZATION PLAN (iter_6):
 *
 *   ITER_2 STATUS: subquery_precompute=179ms (41%), orders_scan=18ms, dim_filter=4ms, total=436ms
 *   GAP vs Umbra: 2.4x. Target: ~180ms.
 *
 *   ROOT CAUSE OF subquery_precompute (179ms):
 *   The pre-built index approach does RANDOM ACCESS into l_quantity.bin via positions[].
 *   60M random reads into a 480MB file on HDD → TLB thrashing + page faults even with prefetch.
 *
 *   KEY INSIGHT:
 *   Abandon the pre-built index for quantity aggregation. Instead:
 *   - LINEAR SCAN of l_orderkey.bin (240MB, sequential) + l_quantity.bin (480MB, sequential)
 *   - Accumulate into a FLAT ARRAY indexed by orderkey (orderkeys 1..15000000, allocate 15M+1 int64_t = 120MB)
 *   - Sequential I/O is 10-50x faster than random I/O on HDD
 *   - Flat array updates: no hash overhead, just array[orderkey] += qty
 *   - After scan: linear pass over flat array to find qualifying keys (>30000)
 *
 *   PARALLEL APPROACH (avoid 15M*64=7.7GB per-thread arrays):
 *   - Use ATOMIC int64_t array: omp parallel for, fetch_add per row
 *   - But atomic on 15M-entry array with 64 threads and 60M rows → ~4 updates/key avg → minimal contention
 *   - Alternative: N_THREADS partial arrays with partitioned orderkey ranges
 *
 *   CHOSEN: Partition orderkeys across threads — each thread owns a contiguous range of orderkeys.
 *   Thread T owns orderkeys in range [T*shard_size, (T+1)*shard_size).
 *   Each thread scans ALL 60M lineitem rows but only accumulates for its owned orderkey range.
 *   → No synchronization needed, pure sequential scan per thread, perfect cache locality for writes.
 *   → With 64 threads: each thread processes 60M rows and writes to 234K-entry shard (1.9MB — fits in L3/2)
 *   → Total work: 64 threads × 60M rows = 3.84B iterations, but sequential so bandwidth-limited
 *
 *   BETTER APPROACH — Two-pass with minimal memory:
 *   Phase 1a: SINGLE PASS linear scan of l_orderkey + l_quantity into a shared flat array
 *             using 16 threads with thread-local partial arrays for 16 partitions of orderkeys.
 *             Each partition = 15M/16 = ~937K keys → 7.5MB per partial array → 16 × 7.5MB = 120MB total
 *             Each thread i scans all rows, accumulates only keys in its partition (key % 16 == i).
 *             → Each thread scans 60M rows linearly (sequential), writes to 937K-entry array (cache-friendly)
 *
 *   SIMPLEST WINNING APPROACH:
 *   Use a single flat array of size 15,000,001, do parallel scan where each thread atomically adds.
 *   With 64 threads and avg ~4 updates per key from distinct threads, contention is low on HDD-speed data.
 *   Actually: bandwidth is the bottleneck (720MB to read), not compute. 64 threads reading 720MB = each
 *   thread reads 11.25MB → fine. Use omp parallel for with private accumulation into the shared array
 *   using __sync_fetch_and_add (for int64). Or better: use partitioned approach.
 *
 *   FINAL CHOSEN STRATEGY:
 *   16 threads, each owns a partition of orderkeys (key & (16-1) == tid).
 *   Thread scans all 60M rows linearly, accumulates only its partition into a 937K flat array.
 *   After parallel scan: merge 16 small arrays → find qualifying keys.
 *   Total memory: 16 × 937K × 8B = 120MB. Each thread: 60M sequential reads + 937K random writes (L2 cache-friendly).
 *
 *   PHASES:
 *   Phase 1 (subquery_precompute): Parallel partitioned linear scan of lineitem to get qualifying orderkeys.
 *   Phase 2 (orders_scan): Parallel scan of 15M orders → qualifying_orders.
 *   Phase 3 (dim_filter): Small custkey scan → customer name lookup.
 *   Phase 4 (output): Sort top-100, write CSV.
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - Orderkeys: range 1..15000000 (SF10). Max orderkey = 15000000.
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

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
    // Open all columns early and issue MADV_WILLNEED to overlap
    // HDD I/O with CPU setup work.
    // ============================================================
    gendb::MmapColumn<int32_t> l_orderkey_col(li_dir + "l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_quantity_col(li_dir + "l_quantity.bin");
    gendb::MmapColumn<int32_t> o_orderkey_col(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey_col(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate_col(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice_col(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Prefetch all columns for sequential access — overlap HDD I/O
    l_orderkey_col.advise_sequential();
    l_quantity_col.advise_sequential();
    mmap_prefetch_all(l_orderkey_col, l_quantity_col,
                      o_orderkey_col, o_custkey_col, o_orderdate_col, o_totalprice_col,
                      c_custkey_col, c_name_col);

    // ============================================================
    // Phase 1: Subquery — compute SUM(l_quantity) per l_orderkey,
    //   find orders where sum > 30000 (scaled 300*100).
    //
    //   Strategy: Partitioned linear scan — no random access, no index.
    //   - 16 partitions by (orderkey & PART_MASK)
    //   - Each thread owns one partition, scans ALL 60M rows sequentially
    //   - Accumulates into a flat array of size MAX_ORDERKEY+1
    //   - Fully sequential reads, small random writes (fits in L3 cache)
    //
    //   MAX_ORDERKEY = 15,000,000 for SF10.
    //   Each partition flat array = 15M/16 = ~937K entries × 8B = 7.5MB → fits in L3.
    // ============================================================

    // qualifying_keys: orderkey -> sum_qty (scaled x100). ~624 entries expected.
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD = 30000LL; // 300 * 100 (scale factor)
        const int64_t LI_ROWS   = (int64_t)l_orderkey_col.count;
        const int32_t MAX_OKEY  = 15000000; // known for SF10

        // Number of partitions: must be power of 2
        // We use N_PARTS threads (or fewer if system has less)
        int max_threads = omp_get_max_threads();
        if (max_threads > 64) max_threads = 64;

        // Use 16 partitions for good cache behavior (7.5MB shard fits in L3/thread)
        // If fewer than 16 threads available, use that many instead
        const int N_PARTS = 16;
        const int n_parts = (max_threads >= N_PARTS) ? N_PARTS : max_threads;
        const int PART_MASK = n_parts - 1; // works only if n_parts is power of 2

        // Validate n_parts is a power of 2 (fallback if not)
        // n_parts is either N_PARTS (16, pow2) or max_threads (could be non-pow2)
        // Safe: use modulo if not pow2
        const bool use_mask = ((n_parts & (n_parts - 1)) == 0);

        // Each partition owns orderkeys where (key % n_parts == part_id)
        // Partition flat array: indexed by (key / n_parts), i.e., key >> log2(n_parts)
        // For n_parts=16: shard_size = (MAX_OKEY / 16) + 1 = 937501
        const int32_t shard_size = (MAX_OKEY / n_parts) + 2; // +2 for safety

        // Allocate per-partition flat arrays on heap
        // Each: shard_size × int64_t = 7.5MB for n_parts=16
        std::vector<std::vector<int64_t>> shards(n_parts,
            std::vector<int64_t>(shard_size, 0));

        const int32_t* ok_data  = l_orderkey_col.data;
        const int64_t* qty_data = l_quantity_col.data;

        #pragma omp parallel num_threads(n_parts)
        {
            int part = omp_get_thread_num();
            int64_t* my_shard = shards[part].data();

            if (use_mask) {
                // Fast path: power-of-2 modulo via mask
                for (int64_t i = 0; i < LI_ROWS; i++) {
                    int32_t okey = ok_data[i];
                    if ((okey & PART_MASK) == part) {
                        // shard index = okey / n_parts = okey >> log2(n_parts)
                        // For n_parts=16: okey >> 4
                        // Generically: okey / n_parts
                        my_shard[okey / n_parts] += qty_data[i];
                    }
                }
            } else {
                // Slow path: general modulo
                for (int64_t i = 0; i < LI_ROWS; i++) {
                    int32_t okey = ok_data[i];
                    if ((okey % n_parts) == part) {
                        my_shard[okey / n_parts] += qty_data[i];
                    }
                }
            }
        }

        // Now gather qualifying orderkeys from all shards (sequential, ~15M iterations total)
        for (int part = 0; part < n_parts; part++) {
            const int64_t* my_shard = shards[part].data();
            for (int32_t idx = 0; idx < shard_size; idx++) {
                if (my_shard[idx] > THRESHOLD) {
                    int32_t okey = idx * n_parts + part;
                    if (okey > 0 && okey <= MAX_OKEY) {
                        qualifying_keys.insert(okey, my_shard[idx]);
                    }
                }
            }
        }
    }

    // ============================================================
    // Phase 2: Parallel scan of orders → collect qualifying orders
    //   15M rows, ~624 qualify. Parallelize across available threads.
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

        const int64_t ORD_ROWS = (int64_t)o_orderkey_col.count;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        std::vector<std::vector<std::pair<int32_t, OrderInfo>>> local_orders(nthreads);
        for (auto& v : local_orders) v.reserve(32);

        const int32_t* ok_data  = o_orderkey_col.data;
        const int32_t* ck_data  = o_custkey_col.data;
        const int32_t* od_data  = o_orderdate_col.data;
        const int64_t* tp_data  = o_totalprice_col.data;

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
    //   Build small custkey → name_code map, scan 1.5M customers once.
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
