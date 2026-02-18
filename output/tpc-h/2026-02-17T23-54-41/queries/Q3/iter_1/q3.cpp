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
 *   ORDER BY revenue DESC, o_orderdate ASC
 *   LIMIT 10
 *
 * ============================================================
 * CORRECTNESS FIX (iter_1):
 * ============================================================
 * Bug: sort_topk used TopKHeap from hash_utils.h which is broken for k>1.
 *
 * Root cause: TopKHeap::push() uses std::push_heap to build a max-heap where
 * root = BEST element (highest revenue). The skip condition:
 *   if cmp(val, h.front()) return; // skip val
 * With cmp(a,b)='a is WORSE' (original iter_0 code): skips when val < root (max),
 * so only elements ABOVE the current maximum are ever added — essentially just tracking
 * the running max + accumulating a few random elements from the first k insertions.
 * This produces wrong top-10 with revenues far below the actual top values.
 *
 * Fix: Replace TopKHeap entirely with std::partial_sort for correct top-k.
 * For ~1.5M groups and k=10: O(N log k) ≈ 4.5M operations — well under 10ms.
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
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K keys)
 *   2. Scan orders, probe customer set; collect qualifying orders into
 *      CompactHashMap<o_orderkey, OrderMeta> → ~1.5M qualifying orders
 *   3. Scan lineitem, filter l_shipdate > cutoff, probe order_map;
 *      collect (okey, rev, odate, oprio) tuples into thread-local flat vectors.
 *      Merge sequentially with find-or-accumulate into global agg_map.
 *
 * Step 3 — No correlated subqueries.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter
 *   - Load customer c_mktsegment (mmap), find BUILDING code from dict
 *   - Build CompactHashSet<int32_t> of qualifying c_custkeys (~300K)
 *   - Sequential (fast at 1.5M rows)
 *
 * Phase 2: build_joins
 *   - Load orders columns (mmap), parallel scan with OMP
 *   - Thread-local flat vectors → merge into CompactHashMap<o_orderkey, OrderMeta>
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap), parallel OMP scan
 *   - Filter l_shipdate > DATE_CUTOFF, probe order_map
 *   - Thread-local flat vectors (cache-friendly push_back, no hash overhead in hot loop)
 *   - Sequential merge: find-or-accumulate into global agg_map
 *
 * Phase 4: sort_topk
 *   - Collect agg_map entries into vector, std::partial_sort top-10
 *   - Comparator: revenue DESC, o_orderdate ASC
 *
 * Phase 5: output — write CSV
 *
 * Key constants:
 *   DATE '1995-03-15' = epoch day 9204
 *   revenue_raw = l_extendedprice * (100 - l_discount) / 100  (int64_t, scale x100)
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
    int64_t revenue_raw; // sum of l_extendedprice * (100 - l_discount) / 100, scale x100
    int32_t o_orderdate;
    int32_t o_shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string data_dir = gendb_dir;

    // ----------------------------------------------------------------
    // Phase 1: Filter customer → build set of qualifying c_custkeys
    // ----------------------------------------------------------------
    gendb::CompactHashSet<int32_t> cust_set(400000);
    {
        GENDB_PHASE("dim_filter");

        // Find BUILDING dictionary code
        int32_t building_code = -1;
        {
            std::string dict_path = data_dir + "/customer/mktsegment_dict.txt";
            FILE* f = fopen(dict_path.c_str(), "r");
            if (!f) throw std::runtime_error("Cannot open mktsegment_dict.txt");
            char line[64];
            int32_t code = 0;
            while (fgets(line, sizeof(line), f)) {
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
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(8000000); // 1.5M entries, roomy pre-sizing
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (data_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (data_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (data_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (data_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        int nthreads = omp_get_max_threads();

        // Thread-local flat vectors: cache-friendly inner loop
        std::vector<std::vector<std::pair<int32_t, OrderMeta>>> local_results(nthreads);

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

        // Merge into order_map (sequential, ~1.5M entries total)
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
    // Strategy: parallel scan with thread-local flat vectors (cache-friendly push_back
    // in hot loop), then sequential merge with find-or-accumulate into global agg_map.
    gendb::CompactHashMap<int32_t, AggEntry> agg_map(4000000); // ~1.5M groups, roomy pre-sizing
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();
        int nthreads = omp_get_max_threads();

        // Thread-local flat vectors: O(1) push_back in hot loop, no hash overhead
        std::vector<std::vector<std::tuple<int32_t, int64_t, int32_t, int32_t>>> local_aggs(nthreads);

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t i = 0; i < n; i++) {
            if (l_shipdate[i] > DATE_CUTOFF) {
                int32_t okey = l_orderkey[i];
                const OrderMeta* om = order_map.find(okey);
                if (om) {
                    // revenue_raw = extprice * (100 - discount) / 100
                    // extprice and discount both scaled x100
                    // product is x10000, /100 brings to x100 scale
                    int64_t rev = (l_extprice[i] * (100LL - l_discount[i])) / 100LL;
                    int tid = omp_get_thread_num();
                    local_aggs[tid].emplace_back(okey, rev, om->o_orderdate, om->o_shippriority);
                }
            }
        }

        // Sequential merge: find-or-accumulate per entry into global agg_map
        // Multiple tuples with the same okey are correctly summed via find+accumulate
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

        // CORRECTNESS FIX: TopKHeap in hash_utils.h is broken for k>1 (it only tracks
        // elements >= current running max, not true top-k). Use std::partial_sort instead.
        //
        // SQL ORDER BY revenue DESC, o_orderdate ASC:
        //   row a comes before row b if a.revenue > b.revenue
        //   OR same revenue AND a.o_orderdate < b.o_orderdate (earlier date = ASC ordering)
        //
        // Complexity: O(N log k) for N=1.5M groups, k=10 → ~4.5M ops, well under 10ms.

        // Collect all aggregation results into a flat vector
        std::vector<ResultRow> all_rows;
        all_rows.reserve(agg_map.size());
        for (auto [okey, entry] : agg_map) {
            all_rows.push_back({okey, entry.revenue_raw, entry.o_orderdate, entry.o_shippriority});
        }

        // partial_sort: first k elements are sorted top-k in O(N log k)
        constexpr size_t K = 10;
        size_t k = std::min(K, all_rows.size());
        auto row_cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_raw != b.revenue_raw) return a.revenue_raw > b.revenue_raw;
            return a.o_orderdate < b.o_orderdate;  // earlier date first (ASC order)
        };
        std::partial_sort(all_rows.begin(), all_rows.begin() + (ptrdiff_t)k, all_rows.end(), row_cmp);
        results.assign(all_rows.begin(), all_rows.begin() + (ptrdiff_t)k);
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
            // revenue_raw is at scale x100: divide by 100 for output
            int64_t rev_int  = row.revenue_raw / 100LL;
            int64_t rev_frac = row.revenue_raw % 100LL;
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
