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
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15  → ~54% of 15M → ~8.1M rows
 *   lineitem  : l_shipdate > 1995-03-15   → ~38% of 60M → ~22M rows
 *
 * Step 2 — Join ordering (smallest filtered result first):
 *   1. Scan customer with mktsegment='BUILDING' → flat bool array cust_ok[1..1.5M]
 *   2. Scan orders with o_orderdate < threshold, probe cust_ok bitset
 *      → build flat arrays: order_valid[], order_date[], order_shipprio[]
 *        indexed directly by o_orderkey (1..15M) → O(1) access, no hash table
 *   3. Parallel scan lineitem with l_shipdate > threshold (zone-map skip),
 *      probe order_valid[] directly by l_orderkey → O(1) array access
 *      → accumulate revenue into per-thread local flat arrays agg_rev[],
 *        then reduce with atomic adds into global flat array
 *   4. Collect non-zero entries, top-10 sort
 *
 * ============================================================
 * PHYSICAL PLAN (REWRITE — fixes stall)
 * ============================================================
 * KEY INSIGHT: o_orderkey and l_orderkey are in range 1..15M.
 *   Use flat arrays indexed by orderkey — O(1) access, no hash table needed.
 *   This eliminates the 272ms CompactHashMap build AND 113ms merge.
 *
 * customer  : sequential scan, filter mktsegment==BUILDING_CODE
 *             → flat bool array cust_ok[0..1500000]
 * orders    : sequential scan, filter o_orderdate < threshold + cust_ok probe
 *             → flat arrays order_valid/order_date/order_shipprio[0..15000000]
 *             (direct write by orderkey index — no hash table)
 * lineitem  : zone-map block pruning on l_shipdate, parallel morsel scan
 *             → probe order_valid[l_orderkey] directly (array access)
 *             → accumulate into thread-local flat revenue arrays (size 15M+1)
 *             → final merge: single pass atomic add into global revenue array
 * aggregation: single pass over order_valid entries → collect (key, rev, date, prio)
 * sort/topk : partial_sort top-10
 * output    : write Q3.csv
 *
 * Memory layout:
 *   cust_ok       : 1.5M × 1B  = 1.5MB
 *   order_valid   : 15M × 1B   = 15MB
 *   order_date    : 15M × 4B   = 60MB
 *   order_shipprio: 15M × 1B   = 15MB  (shippriority is always 0 in TPC-H SF10)
 *   agg_rev[]     : 15M × 8B   = 120MB (thread-local, but reuse single global)
 *
 * Parallelism: 64 cores
 *   Phase 3 (lineitem scan): parallel with OpenMP, thread-local revenue partial sums
 *   Merge: single global array with thread-local contribution added sequentially per block
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   MAX_ORDERKEY = 15000000
 *   MAX_CUSTKEY  = 1500000
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <fstream>
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

static constexpr int32_t MAX_CUSTKEY  = 1500000;
static constexpr int32_t MAX_ORDERKEY = 15000000;

// Zone map entry (24 bytes with natural alignment padding)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");

    // Load c_mktsegment dictionary and find the code for 'BUILDING' at runtime
    int32_t BUILDING_CODE = -1;
    {
        std::ifstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt");
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open customer/c_mktsegment_dict.txt");
        std::string line;
        int32_t code = 0;
        while (std::getline(dict_file, line)) {
            if (line == "BUILDING") { BUILDING_CODE = code; break; }
            ++code;
        }
        if (BUILDING_CODE < 0)
            throw std::runtime_error("'BUILDING' not found in c_mktsegment_dict.txt");
    }

    // ============================================================
    // Phase 1: Scan customer → flat bool array indexed by c_custkey
    // ============================================================
    // Allocate on heap to avoid stack overflow (1.5MB)
    std::vector<uint8_t> cust_ok(MAX_CUSTKEY + 1, 0);
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        const int32_t* ck = c_custkey.data;
        const int32_t* cm = c_mktsegment.data;

        for (size_t i = 0; i < n; i++) {
            if (cm[i] == BUILDING_CODE) {
                cust_ok[ck[i]] = 1;
            }
        }
    }

    // ============================================================
    // Phase 2: Scan orders → flat arrays indexed by o_orderkey
    //   order_valid[ok]    = 1 if order qualifies (date < threshold AND cust BUILDING)
    //   order_date[ok]     = o_orderdate
    //   order_shipprio[ok] = o_shippriority
    //
    // This REPLACES the CompactHashMap (was 272ms) with direct flat array writes.
    // ============================================================
    // Allocate flat arrays (15MB + 60MB + 15MB = ~90MB total)
    std::vector<uint8_t>  order_valid (MAX_ORDERKEY + 1, 0);
    std::vector<int32_t>  order_date  (MAX_ORDERKEY + 1, 0);
    std::vector<int8_t>   order_shipprio(MAX_ORDERKEY + 1, 0);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        const int32_t* ok  = o_orderkey.data;
        const int32_t* ck  = o_custkey.data;
        const int32_t* od  = o_orderdate.data;
        const int32_t* osp = o_shippriority.data;

        uint8_t*  ov  = order_valid.data();
        int32_t*  odt = order_date.data();
        int8_t*   opr = order_shipprio.data();
        const uint8_t* co = cust_ok.data();

        for (size_t i = 0; i < n; i++) {
            if (od[i] < DATE_THRESHOLD) {
                int32_t c = ck[i];
                if (c >= 0 && c <= MAX_CUSTKEY && co[c]) {
                    int32_t okey = ok[i];
                    ov[okey]  = 1;
                    odt[okey] = od[i];
                    opr[okey] = (int8_t)osp[i];
                }
            }
        }
    }
    // Free customer array — no longer needed
    { std::vector<uint8_t>().swap(cust_ok); }

    // ============================================================
    // Phase 3: Parallel lineitem scan with zone-map pruning
    //   Probe order_valid[l_orderkey] directly (flat array, O(1))
    //   Accumulate revenue into thread-local flat arrays, then merge
    //
    // Thread-local flat arrays avoid ALL hash overhead.
    // Each thread gets its own int64_t agg[MAX_ORDERKEY+1].
    // ============================================================

    // Use a single global aggregation array with atomic int64 additions.
    // Since we have ~2-3M qualifying orderkeys out of 15M, false sharing
    // is low and contention is minimal — atomics are sufficient.
    // However, 15M * 8B = 120MB is fine given 376GB RAM.
    //
    // Alternative: thread-local arrays of size MAX_ORDERKEY+1 would be 64*120MB = 7.6GB
    // — too large. Use atomic int64 instead.
    //
    // Better: use partitioned approach — assign orderkey ranges to threads.
    // With 64 threads and 15M orderkeys, each thread owns 234K orderkey range.
    // Only aggregation into "owned" range — zero contention, no atomics needed.

    // Global aggregation array
    std::vector<int64_t> agg_rev(MAX_ORDERKEY + 1, 0LL);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        // Load zone map for l_shipdate pruning
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map");
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size,
                                                       PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Build qualifying block ranges (skip blocks where max <= threshold)
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            scan_ranges.push_back({zones[b].row_start,
                                   zones[b].row_start + zones[b].row_count});
        }
        munmap((void*)zm_raw, zm_st.st_size);

        size_t num_ranges = scan_ranges.size();
        const int32_t* lok = l_orderkey.data;
        const int64_t* lep = l_extendedprice.data;
        const int64_t* lds = l_discount.data;
        const int32_t* lsd = l_shipdate.data;
        const uint8_t* ov  = order_valid.data();
        int64_t*       ar  = agg_rev.data();

        // Partitioned parallel aggregation:
        // Each thread processes a subset of zone-map blocks AND writes only
        // to its own orderkey partition in the global agg array.
        // To avoid contention: use atomic fetch_add (int64 atomics are fast on x86).
        // With ~22M qualifying rows across 64 threads, ~340K rows/thread —
        // most writes go to distinct orderkeys (2-3M unique), so contention is low.

        int num_threads = omp_get_max_threads();

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend   = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (lsd[i] <= DATE_THRESHOLD) continue;

                int32_t okey = lok[i];
                if (!ov[okey]) continue;

                int64_t rev = lep[i] * (100LL - lds[i]) / 100;
                // Atomic add to global array — fast on x86 with -O3
                __atomic_fetch_add(&ar[okey], rev, __ATOMIC_RELAXED);
            }
        }
    }

    // ============================================================
    // Phase 4: Collect results — single pass over order_valid entries
    //   No hash table merge needed — flat arrays are already complete.
    //   This replaces the 113ms aggregation_merge phase.
    // ============================================================
    std::vector<ResultRow> all_rows;
    {
        GENDB_PHASE("aggregation_merge");

        // Count qualifying orders for reserve
        // Scan order_valid to find entries with both valid flag AND revenue > 0
        // (an order may qualify but have no matching lineitem rows)
        all_rows.reserve(300000); // ~2-3M qualifying orders but most won't have matching lineitems

        const uint8_t* ov  = order_valid.data();
        const int32_t* odt = order_date.data();
        const int8_t*  opr = order_shipprio.data();
        const int64_t* ar  = agg_rev.data();

        for (int32_t okey = 1; okey <= MAX_ORDERKEY; okey++) {
            if (ov[okey] && ar[okey] > 0) {
                all_rows.push_back({okey, ar[okey],
                                    odt[okey],
                                    (int32_t)(uint8_t)opr[okey]});
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        };

        int k = (int)std::min((size_t)10, all_rows.size());
        std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(), cmp);
        top10.assign(all_rows.begin(), all_rows.begin() + k);
    }

    // ============================================================
    // Phase 6: Output CSV
    // ============================================================
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

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
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
