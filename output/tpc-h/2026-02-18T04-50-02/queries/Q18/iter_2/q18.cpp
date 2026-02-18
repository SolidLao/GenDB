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
 * OPTIMIZATION PLAN (iter_2):
 *
 *   ITER_1 STATUS: subquery_precompute=166ms (31%), orders_scan=67ms (13%),
 *                  dim_filter=38ms (7%), total=531ms
 *
 *   KEY BOTTLENECKS FOR ITER_2:
 *
 *   1. orders_scan (67ms): Single-threaded sequential scan of 15M orders rows.
 *      FIX: Parallelize with OpenMP across 64 threads → thread-local result vectors →
 *      merge. Expected ~4-8x speedup (67ms → ~10-20ms).
 *
 *   2. dim_filter (38ms): Building a full 1.5M-entry CompactHashMap for customer.
 *      Only ~624 qualifying orders → ~624 unique custkeys needed.
 *      FIX: Late materialization — after orders_scan produces qualifying_orders,
 *      collect unique custkeys (~624), build a tiny CompactHashMap<int32_t,int32_t>
 *      of size 2048. Then scan customer (1.5M rows) probing only for those ~624
 *      custkeys. Avoids building the full 1.5M-entry hash map entirely.
 *
 *   3. subquery_precompute (166ms): Random l_quantity access via positions array.
 *      FIX: Add software prefetch (__builtin_prefetch) for next positions to hide
 *      memory latency for random access pattern on HDD-backed mmap pages.
 *      Also switch l_quantity to MADV_RANDOM since access is non-sequential.
 *
 *   4. I/O wait (~260ms unaccounted): HDD-backed mmap; pages not pre-loaded.
 *      FIX: Issue MADV_WILLNEED (prefetch) on all columns as early as possible,
 *      overlapping I/O with CPU-side hash build and index loading.
 *      Prefetch orders + customer columns while subquery_precompute runs.
 *
 *   PHYSICAL PLAN:
 *   Phase 1 (subquery_precompute): Load lineitem_orderkey_hash via mmap.
 *     - Parallel over index hash table slots (64 threads).
 *     - l_quantity with MADV_RANDOM + prefetch next position.
 *     - Thread-local vectors of (orderkey, sum_qty); merge → qualifying_keys map.
 *     - Apply HAVING > 30000 → qualifying_keys CompactHashMap<int32_t, int64_t>
 *
 *   Phase 2 (orders_scan): Parallel scan of 15M orders → thread-local result vectors
 *     → merge → qualifying_orders (~624 entries).
 *
 *   Phase 3 (dim_filter): Late materialization — scan 1.5M customers with tiny
 *     custkey probe set (size 2048) → collect only needed (custkey → name_code) pairs.
 *
 *   Phase 4 (output): Build result rows, top-100 sort, emit CSV.
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - lineitem_orderkey_hash layout:
 *       [uint32_t num_unique][uint32_t table_size]
 *       then table_size slots of [int32_t key, uint32_t offset, uint32_t count] (12B each)
 *       then [uint32_t total_positions][uint32_t positions...]
 *   - Occupied slot: key != 0 (empty slots have key=0) — valid orderkeys start at 1.
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

// ============================================================
// Pre-built hash index layout (lineitem_orderkey_hash)
// [uint32_t num_unique][uint32_t table_size]
// [int32_t key, uint32_t offset, uint32_t count] per slot (12B), table_size slots
// [uint32_t total_positions][uint32_t positions...]
// ============================================================
struct IndexSlot {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string li_dir    = gendb_dir + "/lineitem/";
    const std::string ord_dir   = gendb_dir + "/orders/";
    const std::string cust_dir  = gendb_dir + "/customer/";
    const std::string idx_dir   = gendb_dir + "/indexes/";

    // ============================================================
    // Early prefetch: open and MADV_WILLNEED orders + customer columns
    // so I/O overlaps with Phase 1 (subquery_precompute) on HDD.
    // ============================================================
    gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");
    gendb::MmapColumn<int32_t> c_custkey_col(cust_dir + "c_custkey.bin");
    gendb::MmapColumn<int32_t> c_name_col(cust_dir + "c_name.bin");

    // Issue prefetch for all columns — overlap HDD I/O with Phase 1 index scan
    mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_totalprice,
                      c_custkey_col, c_name_col);

    // ============================================================
    // Phase 1: Subquery + main_scan FUSED via pre-built index
    //   Load lineitem_orderkey_hash index (mmap, zero-copy).
    //   Parallel iterate hash slots, sum l_quantity per orderkey.
    //   Apply HAVING > 30000 → qualifying_keys map (orderkey -> sum_qty).
    // ============================================================

    // qualifying_keys: orderkey -> sum_qty (scaled x100). ~624 entries expected.
    gendb::CompactHashMap<int32_t, int64_t> qualifying_keys(2048);

    {
        GENDB_PHASE("subquery_precompute");

        const int64_t THRESHOLD = 30000LL; // 300 * 100 (scale factor)

        // Open l_quantity column (read-only via mmap, random access pattern)
        gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");
        // Positions are random (index-driven) — advise random access
        l_quantity.advise_random();

        // Memory-map the pre-built lineitem_orderkey_hash index
        std::string idx_path = idx_dir + "lineitem_orderkey_hash.bin";
        int idx_fd = open(idx_path.c_str(), O_RDONLY);
        if (idx_fd < 0)
            throw std::runtime_error("Cannot open lineitem_orderkey_hash.bin");

        struct stat idx_stat;
        fstat(idx_fd, &idx_stat);
        size_t idx_size = (size_t)idx_stat.st_size;

        void* idx_raw = mmap(nullptr, idx_size, PROT_READ, MAP_SHARED, idx_fd, 0);
        if (idx_raw == MAP_FAILED)
            throw std::runtime_error("mmap failed for lineitem_orderkey_hash.bin");
        close(idx_fd);

        // Advise sequential for index slots (iterated in order), willneed for positions
        madvise(idx_raw, idx_size, MADV_WILLNEED);

        const uint8_t* ptr = (const uint8_t*)idx_raw;

        // Parse header
        uint32_t num_unique  = *(const uint32_t*)(ptr);
        uint32_t table_size  = *(const uint32_t*)(ptr + 4);
        ptr += 8;

        // Hash table slots
        const IndexSlot* slots = (const IndexSlot*)ptr;
        ptr += (size_t)table_size * sizeof(IndexSlot);

        // Positions array
        uint32_t total_positions = *(const uint32_t*)ptr;
        ptr += 4;
        const uint32_t* positions = (const uint32_t*)ptr;

        (void)num_unique;
        (void)total_positions;

        const int64_t* qty_data = l_quantity.data;

        int nthreads = omp_get_max_threads();
        if (nthreads > 64) nthreads = 64;

        // Thread-local storage for qualifying (key, sum_qty) pairs
        std::vector<std::vector<std::pair<int32_t, int64_t>>> local_results(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = local_results[tid];

            #pragma omp for schedule(static)
            for (uint32_t s = 0; s < table_size; s++) {
                const IndexSlot& slot = slots[s];
                if (slot.key == 0) continue; // empty slot

                // Sum l_quantity for all lineitem rows with this orderkey.
                // Use software prefetch to hide random-access latency.
                int64_t total_qty = 0;
                uint32_t end = slot.offset + slot.count;
                uint32_t p = slot.offset;

                // Prefetch-ahead by 8 positions to pipeline memory accesses
                constexpr uint32_t PREFETCH_DIST = 8;
                uint32_t prefetch_end = (end > PREFETCH_DIST) ? end - PREFETCH_DIST : end;
                for (; p < prefetch_end; p++) {
                    __builtin_prefetch(&qty_data[positions[p + PREFETCH_DIST]], 0, 0);
                    total_qty += qty_data[positions[p]];
                }
                for (; p < end; p++) {
                    total_qty += qty_data[positions[p]];
                }

                // HAVING SUM(l_quantity) > 300 (scaled: 30000)
                if (total_qty > THRESHOLD) {
                    local.emplace_back(slot.key, total_qty);
                }
            }
        }

        // Merge thread-local results into qualifying_keys map
        for (int t = 0; t < nthreads; t++) {
            for (auto& [key, qty] : local_results[t]) {
                qualifying_keys.insert(key, qty);
            }
        }

        munmap(idx_raw, idx_size);
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
