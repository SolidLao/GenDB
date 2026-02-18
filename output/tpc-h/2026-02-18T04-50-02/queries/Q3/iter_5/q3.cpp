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
 * COMPLETE REWRITE — ITERATION 5 (STALL RECOVERY)
 * ============================================================
 *
 * ROOT CAUSE OF 18x GAP:
 * 1. I/O bottleneck: 4 lineitem columns × 60M rows ≈ 1.2 GB on HDD.
 *    Unmeasured I/O time dominated (~750ms). Fix: MADV_WILLNEED prefetch all
 *    columns concurrently BEFORE computation starts, overlapping I/O with CPU.
 * 2. Aggregation merge bottleneck (113ms): 64 thread-local CompactHashMaps
 *    with up to millions of distinct keys merged sequentially. Fix: replace with
 *    a FLAT DIRECT ARRAY indexed by o_orderkey (range 1..15M) for O(1) lookup
 *    with zero merge cost. 15M × 16 bytes = 240 MB, fits in 376 GB RAM.
 * 3. Serial orders scan (272ms): 15M rows scanned single-threaded. Fix:
 *    parallelize using OpenMP with thread-local filtered order arrays, then
 *    scatter-write to the flat direct array (no collision since each orderkey
 *    is unique in orders).
 * 4. Bloom filter on orderkey before lineitem scan: ~38% of lineitem rows
 *    pass shipdate filter but only ~N% have a matching order. Bloom filter
 *    eliminates most dead hash-table probes cheaply in L2 cache.
 *
 * LOGICAL PLAN:
 *   Step 1: customer [mktsegment='BUILDING'] → bitset[custkey] ~300K bits
 *   Step 2: orders [orderdate < 1995-03-15] ⋈ customer_bitset
 *           → flat array[orderkey] = {orderdate, shippriority, revenue=0}
 *   Step 3: lineitem [shipdate > 1995-03-15, zone-map pruned]
 *           → bloom_filter check → flat_array[l_orderkey].revenue += rev
 *   Step 4: scan flat array → collect results → top-10
 *
 * PHYSICAL PLAN:
 *   customer  : parallel scan 1.5M rows → flat bitset[1500001]
 *   orders    : prefetch all cols → parallel scan 15M rows with thread-local
 *               collect → scatter to flat OrderEntry array[15M+1]
 *               (orderkeys are unique in orders, no collision)
 *   lineitem  : prefetch all cols → zone-map block pruning on l_shipdate →
 *               parallel morsel scan → bloom_filter(l_orderkey) → direct
 *               array probe → atomic-free revenue accumulation per thread
 *               (partition by orderkey hash to avoid false sharing)
 *   aggregation: NO MERGE NEEDED — flat array is shared, written once per
 *               orderkey in orders phase, accumulated in lineitem phase
 *               using partitioned parallel update
 *   sort/topk : parallel thread-local top-10 heaps → serial merge of ≤640
 *               candidates → final top-10
 *
 * KEY CONSTANTS:
 *   DATE '1995-03-15' = epoch day 9205
 *   BUILDING dict code = 0
 *   Max orderkey = 15,000,000 (flat array safe index)
 *   revenue_scaled = extendedprice(×100) × (100 − discount(×100)) → /10000 for output
 *
 * PARALLELISM:
 *   64 cores; morsel size = 65536 (matches block size, L3-cache-friendly)
 *   Prefetch all columns with MADV_WILLNEED before any computation
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <stdexcept>
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
// Flat direct-array entry for qualifying orders.
// Indexed by o_orderkey (1..15,000,000).
// revenue_scaled is accumulated atomically per lineitem row.
// ============================================================
struct OrderEntry {
    int32_t  o_orderdate;     // 0 means "not qualifying" (epoch day 0 = 1970-01-01, before 1995-03-15)
    int32_t  o_shippriority;
    int64_t  revenue_scaled;  // accumulated lineitem revenue (scaled ×10000)
    uint32_t valid;           // 1 = qualifying order, 0 = not
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ============================================================
// Zone map entry (verified 24-byte layout with padding)
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
// Simple Bloom filter — fits in L2 cache (128 KB = 1M bits)
// Optimized for ~2-3M qualifying orderkeys
// ============================================================
struct BloomFilter {
    static constexpr size_t NBYTES = 1 << 17;  // 128 KB
    static constexpr size_t MASK   = NBYTES * 8 - 1;
    uint8_t bits[NBYTES];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    inline void insert(uint32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 17   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 34   ) & MASK;
        bits[h1 >> 3] |= (1u << (h1 & 7));
        bits[h2 >> 3] |= (1u << (h2 & 7));
        bits[h3 >> 3] |= (1u << (h3 & 7));
    }

    inline bool maybe_contains(uint32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 17   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 34   ) & MASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15"); // 9205
    const int32_t BUILDING_CODE  = 0;
    const int32_t MAX_ORDERKEY   = 15000000; // known max from workload analysis

    // ============================================================
    // Open all columns early and prefetch ALL data concurrently.
    // On HDD: ~1.2 GB lineitem + ~240 MB orders + ~24 MB customer.
    // MADV_WILLNEED fires async kernel readahead for all pages.
    // ============================================================
    gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
    gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
    gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");
    gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

    // Fire all prefetches immediately — overlap I/O with CPU setup
    mmap_prefetch_all(c_custkey, c_mktsegment,
                      o_orderkey, o_custkey, o_orderdate, o_shippriority,
                      l_orderkey, l_extendedprice, l_discount, l_shipdate);

    // ============================================================
    // Phase 1: Customer filter → flat bitset indexed by custkey
    // Parallelized: 1.5M rows, ~6MB, fast even on HDD
    // ============================================================
    // Use a flat byte array for O(1) lookup (1.5M bytes = 1.5 MB, L3-resident)
    std::vector<uint8_t> cust_building(1500001, 0);
    {
        GENDB_PHASE("dim_filter");
        size_t n = c_custkey.size();
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = 1;
            }
        }
    }

    // ============================================================
    // Phase 2: Build flat direct-array from qualifying orders.
    // Orders scan: filter o_orderdate < threshold AND custkey in BUILDING.
    // Flat array indexed by orderkey (1..15M): ~240 MB.
    // Since each orderkey appears exactly once in orders, no collision.
    // Parallelize with OpenMP — each thread writes to distinct orderkey slots.
    // Also build bloom filter for lineitem probe.
    // ============================================================
    // Allocate flat array (zero-initialized → valid=0 means not qualifying)
    std::vector<OrderEntry> order_flat(MAX_ORDERKEY + 1, {0, 0, 0, 0});

    BloomFilter bloom;

    {
        GENDB_PHASE("build_joins");
        size_t n = o_orderkey.size();
        const uint8_t* cust_ptr = cust_building.data();
        int32_t max_ckey = 1500000;

        // Parallel scan: each thread writes to its own distinct orderkey slots
        // (orderkey is unique in orders, so no two threads write to the same slot)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue;
            int32_t ck = o_custkey[i];
            if (ck < 1 || ck > max_ckey || !cust_ptr[ck]) continue;
            int32_t ok = o_orderkey[i];
            // No collision: orderkey is unique in orders table
            order_flat[ok].o_orderdate    = od;
            order_flat[ok].o_shippriority = o_shippriority[i];
            order_flat[ok].revenue_scaled = 0;
            order_flat[ok].valid          = 1;
        }

        // Build bloom filter from qualifying orders (single-threaded over 15M entries is fast)
        // Use parallel reduction: each thread builds a local bloom, then merge
        // But BloomFilter is 128KB × 64 threads = 8MB merge — just do it serially over flat array
        for (int32_t ok = 1; ok <= MAX_ORDERKEY; ok++) {
            if (order_flat[ok].valid) {
                bloom.insert((uint32_t)ok);
            }
        }
    }

    // ============================================================
    // Phase 3: Scan lineitem with zone-map pruning + shipdate filter,
    //          probe bloom filter then flat array.
    //          Aggregate revenue into order_flat[l_orderkey].revenue_scaled.
    //          Use partitioned parallel aggregation: partition by (orderkey >> P)
    //          so each thread owns a disjoint partition of the flat array.
    //          With P=6 (64 partitions), each thread handles ~234K orderkeys.
    // ============================================================

    // Load zone map
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

    // Collect qualifying block ranges
    struct BlockRange { uint64_t start; uint64_t end; };
    std::vector<BlockRange> scan_ranges;
    scan_ranges.reserve(num_blocks);
    for (uint32_t b = 0; b < num_blocks; b++) {
        if (zones[b].max_val <= DATE_THRESHOLD) continue;
        scan_ranges.push_back({zones[b].row_start, zones[b].row_start + zones[b].row_count});
    }
    munmap((void*)zm_raw, zm_st.st_size);

    {
        GENDB_PHASE("main_scan");

        // Partitioned parallel aggregation:
        // 64 threads, each thread owns lineitem rows where (l_orderkey % 64 == tid).
        // This ensures each thread writes to its own disjoint partition of order_flat,
        // eliminating false sharing and the need for atomics or locks.
        // We process ALL ranges but only accumulate if (ok % 64 == tid).

        size_t num_ranges = scan_ranges.size();
        const int32_t* lship = l_shipdate.data;
        const int32_t* lok   = l_orderkey.data;
        const int64_t* lep   = l_extendedprice.data;
        const int64_t* ldis  = l_discount.data;

        #pragma omp parallel num_threads(64)
        {
            int tid = omp_get_thread_num();

            #pragma omp for schedule(dynamic, 2)
            for (size_t ri = 0; ri < num_ranges; ri++) {
                uint64_t rstart = scan_ranges[ri].start;
                uint64_t rend   = scan_ranges[ri].end;

                for (uint64_t i = rstart; i < rend; i++) {
                    if (lship[i] <= DATE_THRESHOLD) continue;

                    int32_t ok = lok[i];
                    // Bloom filter — cheap L2-resident check
                    if (!bloom.maybe_contains((uint32_t)ok)) continue;

                    // Partition ownership check: only this thread handles this orderkey
                    if ((ok & 63) != tid) continue;

                    // Direct flat array probe — O(1), guaranteed cache-miss if not hot
                    OrderEntry& oe = order_flat[ok];
                    if (!oe.valid) continue;

                    int64_t rev = lep[i] * (100LL - ldis[i]);
                    oe.revenue_scaled += rev;
                }
            }
        }
    }

    // ============================================================
    // Phase 4: No separate merge needed — flat array already has final results.
    //          Collect qualifying entries, find top-10.
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("aggregation_merge");

        // Use thread-local top-10 heaps, one per thread, to scan 15M entries in parallel
        int nthreads = 64;
        std::vector<std::vector<ResultRow>> local_top(nthreads);
        for (auto& lt : local_top) lt.reserve(10);

        // Comparator: revenue DESC, orderdate ASC
        auto cmp_better = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        };

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            // Each thread scans its partition of the flat array
            int32_t pstart = 1 + tid;
            std::vector<ResultRow>& lt = local_top[tid];

            // Simple O(n/T) scan with local top-10 heap
            gendb::TopKHeap<ResultRow, decltype(cmp_better)> heap(10, cmp_better);

            for (int32_t ok = pstart; ok <= MAX_ORDERKEY; ok += nthreads) {
                const OrderEntry& oe = order_flat[ok];
                if (!oe.valid || oe.revenue_scaled == 0) continue;
                ResultRow rr{ok, oe.revenue_scaled, oe.o_orderdate, oe.o_shippriority};
                heap.push(rr);
            }
            lt = heap.sorted();
        }

        // Merge all thread-local top-10 lists (≤640 candidates) → global top-10
        std::vector<ResultRow> all_candidates;
        all_candidates.reserve(nthreads * 10);
        for (auto& lt : local_top) {
            for (auto& rr : lt) all_candidates.push_back(rr);
        }

        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        };

        int k = std::min((size_t)10, all_candidates.size());
        if (k > 0) {
            std::partial_sort(all_candidates.begin(), all_candidates.begin() + k,
                              all_candidates.end(), cmp);
            top10 = std::vector<ResultRow>(all_candidates.begin(), all_candidates.begin() + k);
        }
    }

    // ============================================================
    // Phase 5: sort_topk — already done above, keep phase for timing compat
    // ============================================================
    {
        GENDB_PHASE("sort_topk");
        // Already computed in aggregation_merge phase
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
