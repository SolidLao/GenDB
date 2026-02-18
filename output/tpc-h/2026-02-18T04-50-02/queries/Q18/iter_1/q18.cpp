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
 * OPTIMIZATION PLAN (iter_1):
 *
 *   KEY BOTTLENECK (iter_0): subquery_precompute = 3740ms (92%) due to:
 *     - 64 thread-local CompactHashMaps each with 512K slots (512MB+) for 15M unique keys
 *     - Expensive sequential merge of 64 thread-local maps
 *     - TWO separate full 60M-row lineitem scans (subquery + main_scan)
 *
 *   FIX: Use the pre-built lineitem_orderkey_hash index (zero build cost via mmap).
 *     The index already groups lineitem row positions by l_orderkey.
 *     We can iterate the index's hash table once, for each unique key:
 *       - Sum l_quantity for all its positions (contiguous reads from positions array)
 *       - Apply HAVING SUM > 30000 inline
 *       - Store qualifying keys + their sum_qty
 *     This FUSES the subquery + main_scan into ONE structured pass.
 *     Eliminates the need for parallel thread-local maps and sequential merge.
 *
 *   PHYSICAL PLAN:
 *   Phase 1 (subquery_precompute): Load lineitem_orderkey_hash via mmap.
 *     - Parallel over index hash table slots (64 threads).
 *     - Each thread processes a stripe of table slots.
 *     - For each occupied slot: sum l_quantity[pos] for all positions in the group.
 *     - Thread-local vectors of (orderkey, sum_qty), merged globally.
 *     - Apply HAVING > 30000 → qualifying_keys CompactHashMap<int32_t, int64_t>
 *       (maps orderkey -> sum_qty, ~624 entries).
 *
 *   Phase 2 (orders_scan): Scan orders, probe qualifying_keys → collect ~624 orders.
 *
 *   Phase 3 (dim_filter): Build customer custkey -> name_code map (1.5M entries).
 *
 *   Phase 4 (output): Build result rows, sort, emit top 100.
 *     (No separate main_scan needed — sum_qty already known from Phase 1.)
 *
 *   KEY NOTES:
 *   - l_quantity: int64_t scale=100. HAVING threshold = 300*100 = 30000.
 *   - lineitem_orderkey_hash layout:
 *       [uint32_t num_unique][uint32_t table_size]
 *       then table_size slots of [int32_t key, uint32_t offset, uint32_t count] (12B each)
 *       then [uint32_t total_positions][uint32_t positions...]
 *   - Occupied slot: key != 0 (empty slots have key=0) — but key=0 is not a valid orderkey.
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

        // Open l_quantity column (read-only via mmap)
        gendb::MmapColumn<int64_t> l_quantity(li_dir + "l_quantity.bin");

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

        // Advise sequential+willneed for index read
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

        // Parallel scan over hash table slots.
        // Each thread processes a contiguous stripe of slots.
        // For each occupied slot (key != 0): sum l_quantity for its positions.
        // Thread-local result vectors to avoid contention.

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

                // Sum l_quantity for all lineitem rows with this orderkey
                int64_t total_qty = 0;
                uint32_t end = slot.offset + slot.count;
                for (uint32_t p = slot.offset; p < end; p++) {
                    total_qty += l_quantity.data[positions[p]];
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
    // Phase 2: Scan orders, filter by qualifying_keys → collect qualifying orders
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

        gendb::MmapColumn<int32_t> o_orderkey(ord_dir + "o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(ord_dir  + "o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(ord_dir + "o_orderdate.bin");
        gendb::MmapColumn<int64_t> o_totalprice(ord_dir + "o_totalprice.bin");

        const int64_t ORD_ROWS = (int64_t)o_orderkey.count;

        for (int64_t i = 0; i < ORD_ROWS; i++) {
            int32_t okey = o_orderkey.data[i];
            if (qualifying_keys.find(okey)) {
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
        gendb::MmapColumn<int32_t> c_name(cust_dir    + "c_name.bin");

        const int64_t CUST_ROWS = (int64_t)c_custkey.count;
        for (int64_t i = 0; i < CUST_ROWS; i++) {
            cust_name_map.insert(c_custkey.data[i], c_name.data[i]);
        }
    }

    // ============================================================
    // Phase 4: Build result rows, sort, take top 100, write CSV
    //   sum_qty is already known from qualifying_keys map (no extra lineitem scan needed).
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
