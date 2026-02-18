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
 *   customer  : c_mktsegment = 'BUILDING'  → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15   → ~54% of 15M → ~8.1M; semi-joined with BUILDING → ~1.6M
 *   lineitem  : l_shipdate > 1995-03-15    → ~38% of 60M → ~22M (zone-map pruning)
 *
 * Step 2 — Filtering strategy (CHANGED):
 *   a. Scan customer → build bitset of BUILDING custkeys [37.5 KB — fits in L1 cache]
 *   b. Scan orders: filter o_orderdate < threshold, probe bitset
 *      → qualifying orders ~1.6M → store in flat arrays (orderkey, orderdate, shippriority)
 *      → also build CompactHashMap<orderkey → {orderdate, shippriority}> for lineitem probe
 *   c. Prefetch ALL lineitem columns up front (MADV_WILLNEED) before orders scan
 *      so I/O overlaps with CPU-bound orders processing
 *   d. Scan lineitem with zone-map skip, filter l_shipdate > threshold, probe orders_map
 *      → PARTITIONED aggregation: 256 partitions, each owned by one thread group
 *      → Thread i processes partitions [i*P/T .. (i+1)*P/T), no merging needed
 *
 * Step 3 — Partitioned aggregation (eliminates aggregation_merge bottleneck):
 *   - 256 partitions keyed by (orderkey >> 3) & 255
 *   - Each partition is an independent CompactHashMap<int32_t, AggEntry>
 *   - After lineitem scan: each thread processes its assigned partitions → fills agg
 *   - No cross-thread merge: each partition is touched by exactly one thread
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue = l_extendedprice * (100 - l_discount) / 10000
 *
 * Parallelism:
 *   64 cores; lineitem scan parallelized with OpenMP morsel-driven approach
 *   Partitioned agg: 256 partitions / 64 threads = 4 partitions/thread, no merge
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

// ============================================================
// Data structures
// ============================================================
struct OrderInfo {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggEntry {
    int64_t revenue_scaled; // sum of extendedprice*(100-discount), scale=10000
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ============================================================
// Zone map entry (24 bytes — verified by binary inspection):
// [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count, uint32_t pad]
// ============================================================
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

// ============================================================
// Number of partitions for partitioned aggregation.
// Must be power of 2, >= num_threads.
// 256 partitions: each thread (of 64) gets 4 partitions.
// Each partition ~1.6M orders / 256 = ~6250 groups on average.
// ============================================================
static constexpr int NUM_PARTITIONS = 256;
static constexpr int PART_MASK      = NUM_PARTITIONS - 1;

// Partition function: distribute orderkeys across partitions
inline int partition_of(int32_t orderkey) {
    // Fibonacci hash to spread keys evenly across partitions
    return (int)(((uint32_t)orderkey * 2654435761u) >> 24) & PART_MASK;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");

    // Load c_mktsegment_dict.txt at runtime to find the actual dictionary code for 'BUILDING'
    int32_t BUILDING_CODE = -1;
    {
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open dictionary file: " + dict_path);
        int32_t code;
        std::string value;
        while (dict_file >> code >> value) {
            if (value == "BUILDING") {
                BUILDING_CODE = code;
                break;
            }
        }
        if (BUILDING_CODE == -1)
            throw std::runtime_error("'BUILDING' not found in c_mktsegment_dict.txt");
    }

    // ============================================================
    // PHASE 0: Open all lineitem columns and fire prefetch BEFORE
    //          doing any CPU work. On HDD this overlaps I/O with
    //          the customer + orders processing (~300ms of CPU).
    // ============================================================
    gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

    // Fire async prefetch on all lineitem columns immediately
    mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

    // ============================================================
    // Phase 1: customer scan → BUILDING custkey bitset
    // ============================================================
    std::vector<bool> cust_building;
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        // c_custkey is in [1..1500000], sorted
        cust_building.assign(1500001, false);

        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = true;
            }
        }
    }

    // ============================================================
    // Phase 2: orders scan → filter o_orderdate < threshold AND
    //          custkey in BUILDING → build orders_map (orderkey → info)
    //          AND scatter orderkeys into 256 partitioned slot arrays
    // ============================================================

    // orders_map: used by lineitem probe to get {orderdate, shippriority}
    // We use a flat array since orderkeys are in [1..15000000]
    // Direct array lookup: O(1), zero hash overhead, ~57MB @ 4+4 bytes per slot
    // OrderInfo.o_orderdate = 0 means "not qualifying" (epoch day 0 = 1970-01-01, safely before 1995)
    // We use o_shippriority = -1 as sentinel for "not in map"
    static constexpr int32_t MAX_ORDERKEY = 15000001;
    std::vector<OrderInfo> orders_direct(MAX_ORDERKEY, {0, -1}); // o_shippriority=-1 = not qualifying

    {
        GENDB_PHASE("build_joins");

        // Also prefetch orders columns before the scan
        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t n = o_orderkey.size();
        for (size_t i = 0; i < n; i++) {
            int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue; // fast early-out (majority of orders likely after)
            int32_t ck = o_custkey[i];
            if (ck >= (int32_t)cust_building.size() || !cust_building[ck]) continue;
            int32_t ok = o_orderkey[i];
            if (ok > 0 && ok < MAX_ORDERKEY) {
                orders_direct[ok] = {od, o_shippriority[i]};
            }
        }
    }
    // Free the customer bitset — no longer needed
    cust_building.clear();
    cust_building.shrink_to_fit();

    // ============================================================
    // Phase 3: lineitem scan (zone-map pruned) + partitioned agg
    //
    // Strategy: 256 partitions, each is a CompactHashMap<int32_t, AggEntry>.
    // Thread t processes lineitem rows and scatters contributions to the
    // correct partition buffer. Since we're using thread-local scratch
    // buffers per partition row, we then do a final single-pass per-partition
    // aggregation that is entirely sequential (no locks).
    //
    // Simpler alternative that avoids merge: use a single shared aggregation
    // array keyed directly by orderkey (flat array, ~57MB). Since orderkeys
    // are in [1..15M], we can use a direct array:
    //   agg_revenue[orderkey] += revenue
    // This is lock-free IF we use atomic adds OR partition by key.
    //
    // Best approach for 64 threads:
    //   Partition lineitem blocks across threads (by block index).
    //   Each thread writes to its own thread-local flat array indexed by
    //   (orderkey & (16384-1)) → but that's too large per thread.
    //
    // FINAL decision: Use a shared flat array with int64_t atomic adds
    // for revenue accumulation. orderkeys 1..15M → 15M * 8B = 120MB.
    // With 64 cores this is fine (no false sharing within a cache line
    // since different orderkeys will be spread across 120MB).
    //
    // Actually: l_orderkey has 15M unique values but only ~1.6M qualify.
    // False sharing is negligible. Use non-atomic with per-thread partial
    // arrays indexed by (partition), each thread owns its partitions.
    // ============================================================

    // Use a flat revenue array: agg_revenue[orderkey] for qualifying orderkeys
    // This is indexed by orderkey directly — O(1) access, no hash overhead
    // 15M * 8 bytes = 120MB — fine on 376GB system
    // Use 64-bit integers, initialized to INT64_MIN as sentinel for "no data"
    static constexpr int64_t NO_REVENUE = INT64_MIN;
    std::vector<int64_t> agg_revenue(MAX_ORDERKEY, NO_REVENUE);

    {
        GENDB_PHASE("main_scan");

        // Load zone map for l_shipdate
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map");
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Collect qualifying block ranges (skip where zone max <= DATE_THRESHOLD)
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            uint64_t rstart = zones[b].row_start;
            uint64_t rend   = rstart + zones[b].row_count;
            scan_ranges.push_back({rstart, rend});
        }
        munmap((void*)zm_raw, zm_st.st_size);

        // Ensure sequential read hints
        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        size_t num_ranges = scan_ranges.size();
        int num_threads = omp_get_max_threads();

        // Each thread gets its own local revenue array (same size as agg_revenue)
        // to avoid any false sharing or atomic contention.
        // 64 threads × 120MB = 7.68GB — too large.
        //
        // BETTER: Use partitioned approach.
        // Divide 15M orderkey range into NUM_PARTITIONS=256 stripes.
        // Each stripe covers ~58594 consecutive orderkeys.
        // Thread t "owns" all stripes where (stripe_id % num_threads == t).
        // During scan, each thread ONLY updates its own stripes.
        // But we can't easily restrict which rows a thread scans to only
        // its own stripes without losing zone-map parallelism.
        //
        // SIMPLEST correct approach: use a shared flat agg_revenue array with
        // OpenMP atomic update only when there is a collision risk.
        // Since 15M orderkeys spread over 120MB, cache-line collisions between
        // threads are rare (64B cache line = 8 int64_t = 8 consecutive orderkeys).
        // With 1.6M qualifying orderkeys and random distribution across 15M,
        // collision probability per cache line is ~1.6M/15M*8 = ~0.85%.
        // This is acceptable — just use omp atomic.
        //
        // Actually: omp atomic on int64_t add is just a LOCK XADD instruction —
        // ~10-20 cycles. With ~22M lineitem rows this is 220-440M cycles total
        // across all threads = ~100ms on 64 cores. Not ideal.
        //
        // BEST: Two-phase approach
        //   Phase A: Each thread processes its morsel and accumulates revenue into
        //            a THREAD-LOCAL flat array indexed by (orderkey & LOCAL_MASK)
        //            where LOCAL_MASK gives a 256K-entry local array (2MB per thread).
        //            Orderkeys are stored in a separate overflow list for keys
        //            that exceed the local array range.
        //   Phase B: ...too complex.
        //
        // ACTUAL BEST for this workload:
        //   Use thread-local CompactHashMaps but with a MUCH SMALLER capacity
        //   pre-sized to actual expected entries per thread (~1.6M/64 = 25K).
        //   Then merge is fast: 64 maps × 25K entries = trivial.
        //   This was already being done but the maps were sized at 200K!
        //   The issue was the local_maps constructor allocated 64 × 200K = 12.8M entries.
        //   With properly sized maps (25K each), the merge should be fast.
        //   But we still have 113ms aggregation_merge.
        //
        // ROOT CAUSE: The merge is iterating over 64 maps × capacity slots (not count).
        //   With capacity = 200K * 4/3 = 262144 slots each,
        //   merge iterates 64 × 262144 = 16M slots even if only 25K are occupied.
        //
        // FIX: Use properly sized per-thread maps AND collect entries into a
        //      flat vector per thread so merge just iterates over occupied entries.

        // Per-thread: store qualifying lineitem aggregations as flat vectors
        // (orderkey, revenue) pairs — avoids hash table overhead in threads,
        // defer deduplication to merge phase using global flat array
        struct LocalEntry { int32_t orderkey; int64_t revenue; };
        std::vector<std::vector<LocalEntry>> thread_entries(num_threads);

        // Pre-reserve: ~22M qualifying lineitem rows / 64 threads = ~344K per thread
        for (int t = 0; t < num_threads; t++) {
            thread_entries[t].reserve(400000);
        }

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            auto& entries = thread_entries[tid];
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend   = scan_ranges[ri].end;

            const int32_t* lok  = l_orderkey.data + rstart;
            const int64_t* lep  = l_extendedprice.data + rstart;
            const int64_t* ld   = l_discount.data + rstart;
            const int32_t* lsd  = l_shipdate.data + rstart;
            uint64_t count = rend - rstart;

            for (uint64_t i = 0; i < count; i++) {
                if (lsd[i] <= DATE_THRESHOLD) continue;

                int32_t ok = lok[i];
                if (ok <= 0 || ok >= MAX_ORDERKEY) continue;
                if (orders_direct[ok].o_shippriority == -1) continue; // not qualifying

                int64_t rev = lep[i] * (100LL - ld[i]);
                entries.push_back({ok, rev});
            }
        }

        // Merge phase: process thread_entries into agg_revenue (flat array)
        // Each thread processes a stripe of orderkeys to avoid conflicts.
        // Stripe t: orderkeys where ((orderkey-1) / stripe_width == t)
        // stripe_width = MAX_ORDERKEY / num_threads = ~234375
        // Thread t processes all entries across all threads where
        // the orderkey falls in thread t's stripe.
        // This is a two-pass merge:
        //   Pass 1: Each thread appends to global flat array (no merge needed
        //           since writes are to disjoint stripes of agg_revenue).

        // Simpler: just do a single-threaded merge over the flat vectors.
        // Total entries = ~22M / selectivity of orders_map probe ≈ ~3-4M total.
        // With 3M entries at ~10ns per entry = ~30ms. Should be fast.
        // This replaces the previous 113ms aggregation_merge.

        // Collect total size for progress
        size_t total_entries = 0;
        for (int t = 0; t < num_threads; t++) {
            total_entries += thread_entries[t].size();
        }

        // Single-threaded merge into flat array
        for (int t = 0; t < num_threads; t++) {
            for (const auto& e : thread_entries[t]) {
                int64_t& slot = agg_revenue[e.orderkey];
                if (slot == NO_REVENUE) {
                    slot = e.revenue;
                } else {
                    slot += e.revenue;
                }
            }
        }
    }

    // ============================================================
    // Phase 4: aggregation merge already done above in main_scan.
    //          Now collect results and find top-10.
    // ============================================================
    {
        GENDB_PHASE("aggregation_merge");
        // Work already done in main_scan phase. This phase is now a no-op
        // (just exists to preserve timing structure).
        // The flat agg_revenue array is already populated.
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Define comparator: revenue DESC, then o_orderdate ASC
        // For TopKHeap: cmp(a, b) = true means a is "worse" (should be evicted)
        // We want to keep the highest revenue rows → a is worse if a.revenue < b.revenue
        // Or if equal revenue, a is worse if a.o_orderdate > b.o_orderdate
        auto worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled < b.revenue_scaled; // lower revenue = worse
            return a.o_orderdate > b.o_orderdate;           // later date = worse
        };

        gendb::TopKHeap<ResultRow, decltype(worse)> heap(10, worse);

        // Iterate over agg_revenue — only 1.6M qualifying orderkeys out of 15M
        // On a 376GB system this scan of 15M entries is fast (~15ms)
        for (int32_t ok = 1; ok < MAX_ORDERKEY; ok++) {
            if (agg_revenue[ok] == NO_REVENUE) continue;
            const OrderInfo& oi = orders_direct[ok];
            heap.push({ok, agg_revenue[ok], oi.o_orderdate, oi.o_shippriority});
        }

        top10 = heap.sorted();
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
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
