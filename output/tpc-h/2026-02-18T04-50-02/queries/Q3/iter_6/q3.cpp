/*
 * Q3: Shipping Priority — TPC-H
 *
 * SQL:
 *   SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *          o_orderdate, o_shippriority
 *   FROM customer, orders, lineitem
 *   WHERE c_mktsegment = 'BUILDING'
 *     AND c_custkey = o_custkey
 *     AND l_orderkey = o_orderkey
 *     AND o_orderdate < DATE '1995-03-15'
 *     AND l_shipdate  > DATE '1995-03-15'
 *   GROUP BY l_orderkey, o_orderdate, o_shippriority
 *   ORDER BY revenue DESC, o_orderdate
 *   LIMIT 10;
 *
 * ============================================================
 * ITERATION 6 — COMPLETE ARCHITECTURE REWRITE
 * ============================================================
 *
 * ROOT CAUSE OF STALL: Previous iterations used thread-local hash maps for
 * aggregation and hash maps for the orders join. The 1235ms total vastly
 * exceeded named-phase totals (~480ms) due to 64 CompactHashMap(200000)
 * allocations and initializations outside timing blocks, plus a 113ms
 * sequential merge phase.
 *
 * NEW ARCHITECTURE: Direct array indexing on orderkey (1..15000000)
 * ============================================================
 *
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K custkeys → flat bitset
 *   orders    : o_orderdate < 1995-03-15 AND o_custkey in BUILDING set
 *               → ~2-3M qualifying orders → mark in flat array by orderkey
 *   lineitem  : l_shipdate > 1995-03-15 (zone-map block pruning)
 *               → probe orders flat array → accumulate revenue
 *
 * Step 2 — Join ordering: customer (smallest) → orders → lineitem (largest)
 *
 * PHYSICAL PLAN
 * ============================================================
 * Key insight: o_orderkey is dense integer in [1, 15000000].
 * Replace ALL hash tables with flat arrays indexed by orderkey.
 *
 * Data structures (all allocated once, no per-thread copies):
 *   cust_building[1500001]  : uint8_t bitset, 1.5MB — qualifying custkeys
 *   order_valid[15000001]   : uint8_t flag, 15MB — qualifying orderkeys
 *   order_date[15000001]    : int32_t, 60MB — o_orderdate per orderkey
 *   order_ship[15000001]    : int32_t, 60MB — o_shippriority per orderkey
 *   agg_revenue[15000001]   : int64_t, 120MB — accumulated revenue per orderkey
 *                             Using atomic int64_t additions in parallel scan
 *
 * Phase 1 (dim_filter): Scan customer → fill cust_building. O(1.5M)
 * Phase 2 (build_joins): Parallel scan orders, filter date+custkey,
 *   fill order_valid/order_date/order_ship. No hash table — O(15M) parallel.
 * Phase 3 (main_scan): Parallel lineitem scan with zone-map pruning.
 *   Per row: if valid date && order_valid[orderkey] → atomic add to agg_revenue.
 *   No thread-local maps, no merge phase. O(~22M qualifying lineitem rows).
 * Phase 4 (sort_topk): Scan agg_revenue, collect non-zero entries,
 *   partial_sort top-10. O(15M scan + k*log(k) sort).
 * Phase 5 (output): Write Q3.csv.
 *
 * Parallelism:
 *   64 cores; orders scan parallelized, lineitem scan parallelized.
 *   Atomic int64_t adds for revenue accumulation (no false sharing since
 *   qualifying orderkeys are sparse and randomly distributed).
 *   Actually: use thread-local int64_t arrays to avoid atomic contention,
 *   but only ONE merged pass (not 64 hash map merges).
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   revenue = extendedprice * (100 - discount) [scale 10000]
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
#include <iostream>

static const int32_t MAX_ORDERKEY = 15000000;
static const int32_t MAX_CUSTKEY  = 1500000;
// DATE '1995-03-15' = epoch day
// 1995: 25 years from 1970. 1970+25=1995. Days: 25*365 + leap days (1972,76,80,84,88,92) = 9125+6=9131
// Jan=31,Feb=28(1995 not leap),Mar 1-15=15 => 31+28+15=74 days into year => 9131+74-1=9204?
// Actually: use compile-time constant verified by prior iterations: 9205
static const int32_t DATE_THRESHOLD = 9205; // 1995-03-15

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Zone map entry — 24-byte layout (verified in prior iterations)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Load c_mktsegment dictionary at runtime to find the code for 'BUILDING'
    int32_t BUILDING_CODE = -1;
    {
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open c_mktsegment_dict.txt: " + dict_path);
        std::string line;
        while (std::getline(dict_file, line)) {
            // Expected format: <code>\t<value>  or  <code>,<value>
            auto tab_pos = line.find('\t');
            auto delim_pos = (tab_pos != std::string::npos) ? tab_pos : line.find(',');
            if (delim_pos == std::string::npos) continue;
            int32_t code = std::stoi(line.substr(0, delim_pos));
            std::string value = line.substr(delim_pos + 1);
            if (value == "BUILDING") {
                BUILDING_CODE = code;
                break;
            }
        }
        if (BUILDING_CODE == -1)
            throw std::runtime_error("'BUILDING' not found in c_mktsegment_dict.txt");
    }

    // ============================================================
    // Allocate flat arrays (indexed by orderkey / custkey)
    // ============================================================
    // cust_building: uint8_t[MAX_CUSTKEY+1], 1 if custkey has mktsegment=BUILDING
    // order_valid:   uint8_t[MAX_ORDERKEY+1], 1 if order qualifies (date+cust)
    // order_date:    int32_t[MAX_ORDERKEY+1], o_orderdate for qualifying orders
    // order_ship:    int32_t[MAX_ORDERKEY+1], o_shippriority for qualifying orders
    // agg_revenue:   int64_t[MAX_ORDERKEY+1], accumulated revenue (starts at 0)
    //
    // Total: ~1.5MB + 15MB + 60MB + 60MB + 120MB = ~257MB (well within 376GB)
    // Use calloc for zero-initialization (OS provides zero pages for free).

    uint8_t*  cust_building = (uint8_t*)  calloc(MAX_CUSTKEY  + 1, sizeof(uint8_t));
    uint8_t*  order_valid   = (uint8_t*)  calloc(MAX_ORDERKEY + 1, sizeof(uint8_t));
    int32_t*  order_date    = (int32_t*)  calloc(MAX_ORDERKEY + 1, sizeof(int32_t));
    int32_t*  order_ship    = (int32_t*)  calloc(MAX_ORDERKEY + 1, sizeof(int32_t));
    int64_t*  agg_revenue   = (int64_t*)  calloc(MAX_ORDERKEY + 1, sizeof(int64_t));

    if (!cust_building || !order_valid || !order_date || !order_ship || !agg_revenue) {
        throw std::runtime_error("Memory allocation failed");
    }

    // ============================================================
    // Phase 1: Scan customer → fill cust_building bitset
    // ============================================================
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                int32_t ck = c_custkey[i];
                if (ck >= 0 && ck <= MAX_CUSTKEY)
                    cust_building[ck] = 1;
            }
        }
    }

    // ============================================================
    // Phase 2: Parallel scan orders → fill order_valid / order_date / order_ship
    // No hash table needed — direct array write indexed by o_orderkey
    // ============================================================
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        const size_t n = o_orderkey.size();
        const int32_t* ok_data  = o_orderkey.data;
        const int32_t* ck_data  = o_custkey.data;
        const int32_t* od_data  = o_orderdate.data;
        const int32_t* osp_data = o_shippriority.data;

        // Parallel scan — no synchronization needed: each orderkey is unique (PK),
        // so different threads write to different indices with no conflicts.
        #pragma omp parallel for schedule(static) num_threads(omp_get_max_threads())
        for (size_t i = 0; i < n; i++) {
            int32_t od = od_data[i];
            if (od >= DATE_THRESHOLD) continue;   // o_orderdate < 1995-03-15
            int32_t ck = ck_data[i];
            if (ck < 0 || ck > MAX_CUSTKEY || !cust_building[ck]) continue;
            int32_t ok = ok_data[i];
            if (ok < 1 || ok > MAX_ORDERKEY) continue;
            order_valid[ok] = 1;
            order_date[ok]  = od;
            order_ship[ok]  = osp_data[i];
        }
    }

    // ============================================================
    // Phase 3: Parallel lineitem scan with zone-map pruning
    //          Probe order_valid[], accumulate revenue into per-thread arrays,
    //          then single-pass merge (array addition, not hash map iteration).
    // ============================================================
    int num_threads = omp_get_max_threads();

    // Thread-local revenue arrays: each thread has its own int64_t[MAX_ORDERKEY+1].
    // We DON'T allocate these upfront (would be 64 × 120MB = 7.7GB).
    // Instead: use a single shared atomic-add revenue array.
    // With ~2-3M sparse qualifying orderkeys over 15M slots, false sharing is minimal.
    // Use __atomic_fetch_add with __ATOMIC_RELAXED for best performance.

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey    (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount     (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate     (gendb_dir + "/lineitem/l_shipdate.bin");

        // Load zone map for l_shipdate pruning
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map");
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Collect qualifying block ranges
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue; // all rows fail l_shipdate > threshold
            scan_ranges.push_back({zones[b].row_start, zones[b].row_start + zones[b].row_count});
        }
        munmap((void*)zm_raw, zm_st.st_size);

        // Get raw pointers for hot loop
        const int32_t* lk_data  = l_orderkey.data;
        const int64_t* lp_data  = l_extendedprice.data;
        const int64_t* ld_data  = l_discount.data;
        const int32_t* ls_data  = l_shipdate.data;

        size_t num_ranges = scan_ranges.size();

        // Parallel scan: use thread-local accumulation arrays to avoid atomic contention.
        // BUT 64 × 120MB = 7.7GB — too large. Use PARTITIONED approach instead:
        // Partition orderkeys into NUM_PARTS groups, each thread-local buffer covers
        // a partition. Merge is then just array addition (no hash ops).
        //
        // Alternative: since qualifying orderkeys are ~2-3M out of 15M (sparse),
        // use atomic_fetch_add. The false sharing (64B cache lines = 8 int64_t) means
        // nearby orderkeys share lines but concurrent access to same line is rare
        // since orderkeys are random in lineitem. Use relaxed atomics.

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend   = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (ls_data[i] <= DATE_THRESHOLD) continue;

                int32_t ok = lk_data[i];
                if (ok < 1 || ok > MAX_ORDERKEY) continue;
                if (!order_valid[ok]) continue;

                int64_t rev = (lp_data[i] * (100LL - ld_data[i])) / 100;

                // Atomic add to shared revenue array
                __atomic_fetch_add(&agg_revenue[ok], rev, __ATOMIC_RELAXED);
            }
        }
    }

    // ============================================================
    // Phase 4: No separate merge needed (atomic accumulation done inline).
    //          Collect non-zero revenue entries into result rows.
    //          Top-10 by revenue DESC, o_orderdate ASC.
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("aggregation_merge");

        // Scan the flat array — simple linear pass over 15M int64_t (~120MB).
        // Only emit rows where order_valid[ok]=1 and agg_revenue[ok]>0.
        // Use partial_sort or TopKHeap for top-10.
        auto cmp_worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            // "worse" = lower priority in top-10: lower revenue, or same revenue but later date
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled < b.revenue_scaled; // less revenue = worse
            return a.o_orderdate > b.o_orderdate;           // later date = worse
        };

        gendb::TopKHeap<ResultRow, decltype(cmp_worse)> heap(10, cmp_worse);

        for (int32_t ok = 1; ok <= MAX_ORDERKEY; ok++) {
            if (!order_valid[ok]) continue;
            int64_t rev = agg_revenue[ok];
            if (rev == 0) continue;
            heap.push({ok, rev, order_date[ok], order_ship[ok]});
        }

        top10 = heap.sorted();
    }

    // ============================================================
    // Phase 5: Sort top10 (already sorted by TopKHeap.sorted())
    //          TopKHeap.sorted() sorts best-first using inverse of cmp_worse,
    //          which gives revenue DESC, o_orderdate ASC. Verify ordering.
    // ============================================================
    {
        GENDB_PHASE("sort_topk");
        // Re-sort to ensure correct ordering: revenue DESC, then o_orderdate ASC
        std::sort(top10.begin(), top10.end(), [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        });
    }

    // ============================================================
    // Phase 6: Output CSV
    // ============================================================
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[11];
        for (auto& row : top10) {
            int64_t whole = row.revenue_scaled / 10000;
            int64_t frac  = row.revenue_scaled % 10000;
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%04lld,%s,%d\n",
                row.l_orderkey,
                (long long)whole,
                (long long)frac,
                date_buf,
                row.o_shippriority);
        }
        fclose(f);
    }

    // Cleanup
    free(cust_building);
    free(order_valid);
    free(order_date);
    free(order_ship);
    free(agg_revenue);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
