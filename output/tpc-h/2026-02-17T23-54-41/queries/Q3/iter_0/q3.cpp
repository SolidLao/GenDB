/*
 * Q3: Shipping Priority
 *
 * SQL:
 *   SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *          o_orderdate, o_shippriority
 *   FROM customer, orders, lineitem
 *   WHERE c_mktsegment = 'BUILDING'
 *     AND c_custkey = o_custkey
 *     AND l_orderkey = o_orderkey
 *     AND o_orderdate < DATE '1995-03-15'
 *     AND l_shipdate > DATE '1995-03-15'
 *   GROUP BY l_orderkey, o_orderdate, o_shippriority
 *   ORDER BY revenue DESC, o_orderdate
 *   LIMIT 10
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates & filtered cardinalities:
 *   customer: c_mktsegment = 'BUILDING' (1/5 selectivity) → ~300K rows
 *   orders:   o_orderdate < 1995-03-15  → ~50% rows → ~7.5M rows
 *   lineitem: l_shipdate > 1995-03-15   → ~50% rows → ~30M rows
 *
 * Step 2 — Join ordering (smallest filtered first):
 *   1. Filter customer → build HashSet<c_custkey> (~300K keys)
 *   2. Scan orders with zone-map pruning on o_orderdate,
 *      probe customer set; collect qualifying (o_orderkey, o_orderdate, o_shippriority)
 *      into HashSet<o_orderkey> + map for date/priority lookup → ~7.5M → ~3.5M qualifying
 *   3. Scan lineitem with zone-map pruning on l_shipdate,
 *      probe orders hash map; accumulate revenue per (l_orderkey, o_orderdate, o_shippriority)
 *
 * Step 3 — No correlated subqueries.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter
 *   - Load customer c_mktsegment (mmap), find BUILDING code (code 0 from dict)
 *   - Load customer c_custkey (mmap)
 *   - Build CompactHashSet<int32_t> of qualifying c_custkeys
 *   - Parallelism: sequential (300K rows, fast)
 *
 * Phase 2: build_joins
 *   - Load orders columns (mmap): o_orderkey, o_custkey, o_orderdate, o_shippriority
 *   - Use orders_o_orderdate zone_map to skip blocks where max < cutoff
 *   - Probe customer set; for qualifying: build CompactHashMap<o_orderkey, {o_orderdate, o_shippriority}>
 *   - Parallelism: parallel scan with thread-local vectors, merge
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap): l_orderkey, l_extendedprice, l_discount, l_shipdate
 *   - Use lineitem_l_shipdate zone_map to skip blocks where min > cutoff
 *   - Probe orders map; accumulate revenue (int64_t scaled) per o_orderkey
 *   - CompactHashMap<int32_t, int64_t> for aggregation (keyed by l_orderkey)
 *   - Parallelism: parallel scan with thread-local hash maps, merge
 *
 * Phase 4: sort_topk
 *   - Materialize aggregation results; sort by (revenue DESC, o_orderdate ASC)
 *   - Emit top 10 rows
 *
 * Phase 5: output — write CSV
 *
 * Key constants:
 *   DATE '1995-03-15' = epoch day 9204
 *   l_extendedprice * (1 - l_discount): both scaled by 100
 *     revenue_raw = l_extendedprice * (100 - l_discount) / 100  (int64_t)
 *   Output revenue = revenue_raw / 100.0 with 2 decimal places
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

#include <omp.h>

// ----------------------------------------------------------------
// Constants
// ----------------------------------------------------------------
static constexpr int32_t DATE_CUTOFF = 9204; // 1995-03-15 in epoch days

// ----------------------------------------------------------------
// Order metadata stored in the hash map
// ----------------------------------------------------------------
struct OrderMeta {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ----------------------------------------------------------------
// Aggregation result per orderkey
// ----------------------------------------------------------------
struct AggEntry {
    int64_t revenue_raw; // sum of l_extendedprice * (100 - l_discount), divide by 100 for output
    int32_t o_orderdate;
    int32_t o_shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string data_dir = gendb_dir;
    const std::string idx_dir  = gendb_dir + "/../indexes"; // indexes are in parent/indexes

    // ----------------------------------------------------------------
    // Phase 1: Filter customer → build set of qualifying c_custkeys
    // ----------------------------------------------------------------
    gendb::CompactHashSet<int32_t> cust_set(400000);
    {
        GENDB_PHASE("dim_filter");

        // Find BUILDING dictionary code
        // Dict file: one entry per line, code = line index (0-based)
        int32_t building_code = -1;
        {
            std::string dict_path = data_dir + "/customer/mktsegment_dict.txt";
            FILE* f = fopen(dict_path.c_str(), "r");
            if (!f) throw std::runtime_error("Cannot open mktsegment_dict.txt");
            char line[64];
            int32_t code = 0;
            while (fgets(line, sizeof(line), f)) {
                // strip newline
                size_t len = strlen(line);
                while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) line[--len] = '\0';
                if (strcmp(line, "BUILDING") == 0) { building_code = code; break; }
                code++;
            }
            fclose(f);
            if (building_code < 0) throw std::runtime_error("BUILDING not found in dict");
        }

        gendb::MmapColumn<int32_t> c_custkey(data_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktseg (data_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        for (size_t i = 0; i < n; i++) {
            if (c_mktseg[i] == building_code) {
                cust_set.insert(c_custkey[i]);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 2: Scan orders, filter by date + customer set
    //          Build map: o_orderkey -> OrderMeta
    // ----------------------------------------------------------------
    // Use zone map to skip blocks
    // Zone map layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(8000000);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (data_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (data_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (data_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (data_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();

        // Load zone map for orders o_orderdate
        // Try to use zone map to skip blocks
        struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t count; };
        std::vector<ZoneEntry> zones;
        uint32_t zone_block_size = 100000; // from storage guide: block size 100,000
        {
            std::string zm_path = data_dir + "/../indexes/orders_o_orderdate_zonemap.bin";
            FILE* f = fopen(zm_path.c_str(), "rb");
            if (f) {
                uint32_t num_zones = 0;
                fread(&num_zones, sizeof(uint32_t), 1, f);
                zones.resize(num_zones);
                for (uint32_t z = 0; z < num_zones; z++) {
                    fread(&zones[z].min_val, sizeof(int32_t), 1, f);
                    fread(&zones[z].max_val, sizeof(int32_t), 1, f);
                    fread(&zones[z].count, sizeof(uint32_t), 1, f);
                }
                fclose(f);
            }
        }

        // Parallel scan with thread-local vectors
        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::pair<int32_t, OrderMeta>>> local_results(nthreads);

        if (!zones.empty()) {
            // Zone-map pruned scan
            #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
            for (int z = 0; z < (int)zones.size(); z++) {
                // Skip block if max < DATE_CUTOFF impossible (all rows >= DATE_CUTOFF)
                // We want o_orderdate < DATE_CUTOFF, so skip if min >= DATE_CUTOFF
                if (zones[z].min_val >= DATE_CUTOFF) continue;

                int tid = omp_get_thread_num();
                size_t start = (size_t)z * zone_block_size;
                size_t end   = start + zones[z].count;
                if (end > n) end = n;

                for (size_t i = start; i < end; i++) {
                    if (o_orderdate[i] < DATE_CUTOFF && cust_set.contains(o_custkey[i])) {
                        local_results[tid].push_back({
                            o_orderkey[i],
                            {o_orderdate[i], o_shipprio[i]}
                        });
                    }
                }
            }
        } else {
            // Full parallel scan without zone map
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n; i++) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set.contains(o_custkey[i])) {
                    int tid = omp_get_thread_num();
                    local_results[tid].push_back({
                        o_orderkey[i],
                        {o_orderdate[i], o_shipprio[i]}
                    });
                }
            }
        }

        // Merge into order_map
        for (auto& lv : local_results) {
            for (auto& [key, meta] : lv) {
                order_map.insert(key, meta);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          probe order_map, aggregate revenue per l_orderkey
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, AggEntry> agg_map(4000000);
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey    (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice    (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount    (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate    (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();

        // Load zone map for lineitem l_shipdate
        struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t count; };
        std::vector<ZoneEntry> zones;
        uint32_t zone_block_size = 200000; // from storage guide: block size 200,000
        {
            std::string zm_path = data_dir + "/../indexes/lineitem_l_shipdate_zonemap.bin";
            FILE* f = fopen(zm_path.c_str(), "rb");
            if (f) {
                uint32_t num_zones = 0;
                fread(&num_zones, sizeof(uint32_t), 1, f);
                zones.resize(num_zones);
                for (uint32_t z = 0; z < num_zones; z++) {
                    fread(&zones[z].min_val, sizeof(int32_t), 1, f);
                    fread(&zones[z].max_val, sizeof(int32_t), 1, f);
                    fread(&zones[z].count, sizeof(uint32_t), 1, f);
                }
                fclose(f);
            }
        }

        int nthreads = omp_get_max_threads();
        // Thread-local aggregation maps to avoid contention
        // We'll use a vector of (key, revenue, orderdate, shipprio) and merge at end
        std::vector<std::vector<std::tuple<int32_t, int64_t, int32_t, int32_t>>> local_aggs(nthreads);

        if (!zones.empty()) {
            #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
            for (int z = 0; z < (int)zones.size(); z++) {
                // We want l_shipdate > DATE_CUTOFF, skip if max <= DATE_CUTOFF
                if (zones[z].max_val <= DATE_CUTOFF) continue;

                int tid = omp_get_thread_num();
                size_t start = (size_t)z * zone_block_size;
                size_t end   = start + zones[z].count;
                if (end > n) end = n;

                for (size_t i = start; i < end; i++) {
                    if (l_shipdate[i] > DATE_CUTOFF) {
                        int32_t okey = l_orderkey[i];
                        const OrderMeta* om = order_map.find(okey);
                        if (om) {
                            // revenue = l_extendedprice * (1 - l_discount)
                            // l_extprice scaled x100, l_discount scaled x100
                            // Divide by 100 immediately to keep scale¹ (x100) consistency
                            int64_t rev = (l_extprice[i] * (100LL - l_discount[i])) / 100;
                            local_aggs[tid].emplace_back(okey, rev, om->o_orderdate, om->o_shippriority);
                        }
                    }
                }
            }
        } else {
            #pragma omp parallel for schedule(static) num_threads(nthreads)
            for (size_t i = 0; i < n; i++) {
                if (l_shipdate[i] > DATE_CUTOFF) {
                    int32_t okey = l_orderkey[i];
                    const OrderMeta* om = order_map.find(okey);
                    if (om) {
                        // Divide by 100 immediately to keep scale¹ (x100) consistency
                        int64_t rev = (l_extprice[i] * (100LL - l_discount[i])) / 100;
                        int tid = omp_get_thread_num();
                        local_aggs[tid].emplace_back(okey, rev, om->o_orderdate, om->o_shippriority);
                    }
                }
            }
        }

        // Merge thread-local results into agg_map
        for (auto& lv : local_aggs) {
            for (auto& [okey, rev, odate, oprio] : lv) {
                AggEntry* entry = agg_map.find(okey);
                if (entry) {
                    entry->revenue_raw += rev;
                } else {
                    agg_map.insert(okey, {rev, odate, oprio});
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 4: Sort Top-K
    // ----------------------------------------------------------------
    struct ResultRow {
        int32_t l_orderkey;
        int64_t revenue_raw;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<ResultRow> results;
    {
        GENDB_PHASE("sort_topk");

        // Use TopKHeap for O(n log k) instead of O(n log n) sort+resize.
        // cmp(a,b)=true means "a is worse than b" (a should be evicted before b).
        // Worse = lower revenue, or same revenue but later orderdate.
        auto heap_cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue_raw != b.revenue_raw) return a.revenue_raw < b.revenue_raw;
            return a.o_orderdate > b.o_orderdate;
        };
        gendb::TopKHeap<ResultRow, decltype(heap_cmp)> heap(10, heap_cmp);

        for (auto entry : agg_map) {
            ResultRow row;
            row.l_orderkey     = entry.first;
            row.revenue_raw    = entry.second.revenue_raw;
            row.o_orderdate    = entry.second.o_orderdate;
            row.o_shippriority = entry.second.o_shippriority;
            heap.push(row);
        }

        // sorted() returns best-first (revenue DESC, o_orderdate ASC)
        results = heap.sorted();
    }

    // ----------------------------------------------------------------
    // Phase 5: Output CSV
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[16];
        for (auto& row : results) {
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            // revenue_raw is now at scale¹ (x100): divided by 100 during accumulation
            // Output: integer part and 2 decimal fraction
            int64_t rev_int  = row.revenue_raw / 100;
            int64_t rev_frac = row.revenue_raw % 100;
            // revenue_raw is always non-negative; no sign correction needed
            fprintf(f, "%d,%lld.%02lld,%s,%d\n",
                    row.l_orderkey,
                    (long long)rev_int,
                    (long long)rev_frac,
                    date_buf,
                    row.o_shippriority);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
