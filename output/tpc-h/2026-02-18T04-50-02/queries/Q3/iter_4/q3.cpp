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
 * Step 1 — Single-table predicates & cardinalities:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15 + semi-join BUILDING custkeys
 *               → ~8.1M × (300K/1.5M) ≈ ~1.6M qualifying orders
 *   lineitem  : l_shipdate > 1995-03-15 + zone-map pruning → ~22M rows (38% of 60M)
 *
 * Step 2 — Join ordering (smallest filtered result → build side):
 *   1. customer  filter → bitset[custkey] (37.5 KB, fits L2 cache)
 *   2. orders    filter (date + bitset probe) → qualifying orders CompactHashMap
 *      orderkey → {orderdate, shippriority}
 *   3. lineitem  zone-map pruned scan → probe orders_map → aggregate revenue
 *      GROUP BY l_orderkey (same key as join key) in partitioned hash tables
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer : sequential scan, filter mktsegment==BUILDING_CODE
 *            → flat bool array[1..1500000] (37.5 KB)
 *
 * orders   : PARALLEL scan with OpenMP (15M rows → ~1.6M qualifying)
 *            Phase A: 64 threads each build thread-local vector<QualOrder>
 *            Phase B: count total qualifying → single pre-sized CompactHashMap
 *            Phase C: sequential fill of CompactHashMap (avoids concurrent write hazard)
 *            BUILD Bloom filter from all qualifying orderkeys simultaneously
 *
 * lineitem : zone-map block pruning on l_shipdate (skip blocks where max<=threshold)
 *            PARALLEL scan, per-row: bloom-filter probe → orders_map probe → aggregate
 *            PARTITIONED AGGREGATION: NUM_PARTS partitions by orderkey hash
 *            Each thread owns its partitions → atomic-free → zero merge cost
 *
 * agg merge: With partitioned agg, merge only across partition boundaries once
 *            (one pass over all thread-local results per partition)
 *
 * sort/topk: partial_sort top-10 with comparator (revenue DESC, o_orderdate ASC)
 *            Uses TopKHeap from hash_utils.h
 *
 * Bloom filter sizing:
 *   ~1.6M qualifying orders → 1.6M × 10 bits = 20Mbit = 2.5MB (fits L3 cache 88MB)
 *   Expected false positive rate: ~1%
 *   Expected lineitem rows skipped: ~60-70% of non-matching rows
 *
 * Parallelism:
 *   64 cores; all major phases parallelized with OpenMP
 *   Partitioned aggregation: NUM_PARTS=64 partitions, each thread owns 1+ partitions
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
// Bloom Filter — fits in L3 cache, ~1% FP rate for 1.6M keys
// 4MB = 32Mbit → ~20 bits/key → FP rate < 0.1%
// ============================================================
struct BloomFilter {
    static constexpr size_t NBYTES = 1 << 22;  // 4MB
    static constexpr size_t MASK   = NBYTES * 8 - 1;
    uint8_t bits[NBYTES];

    void clear() { memset(bits, 0, NBYTES); }

    inline void insert(uint64_t h) {
        uint32_t h1 = (uint32_t)(h)         & MASK;
        uint32_t h2 = (uint32_t)(h >> 22)   & MASK;
        uint32_t h3 = (uint32_t)(h >> 44)   & MASK;
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(uint64_t h) const {
        uint32_t h1 = (uint32_t)(h)         & MASK;
        uint32_t h2 = (uint32_t)(h >> 22)   & MASK;
        uint32_t h3 = (uint32_t)(h >> 44)   & MASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

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

// Zone map entry: 20 bytes per storage guide layout
// Verified: actual on-disk layout is 24 bytes (padding to align uint64_t)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

// Qualifying order record (temporary, for parallel orders collection)
struct QualOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");
    // BUILDING = dict code 0 (first entry in c_mktsegment_dict.txt)
    const int32_t BUILDING_CODE = 0;

    // ============================================================
    // Phase 1: customer filter → flat bitset[custkey]
    // ~1.5M rows, 5 distinct segments, fast sequential scan
    // ============================================================
    std::vector<bool> cust_building;
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktseg(gendb_dir  + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        // custkey is 1-based, max is 1500000
        cust_building.assign(1500001, false);
        for (size_t i = 0; i < n; i++) {
            if (c_mktseg[i] == BUILDING_CODE) {
                int32_t ck = c_custkey[i];
                if (ck > 0 && ck <= 1500000)
                    cust_building[ck] = true;
            }
        }
    }

    // ============================================================
    // Phase 2: orders → parallel scan + build hash map + bloom filter
    // Strategy:
    //   (A) Parallel scan: each thread collects qualifying rows into thread-local vector
    //   (B) Count total → pre-size orders_map and bloom filter
    //   (C) Sequential insert into CompactHashMap + bloom filter
    // This eliminates concurrent write hazard while still parallelising the scan
    // ============================================================
    gendb::CompactHashMap<int32_t, OrderInfo> orders_map(2000000);
    static BloomFilter bloom; // static to avoid stack overflow (4MB)
    bloom.clear();

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (gendb_dir + "/orders/o_shippriority.bin");

        // Prefetch all orders columns for sequential scan
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shipprio);

        const size_t n = o_orderkey.size(); // 15M
        const int num_threads = omp_get_max_threads();

        // Thread-local qualifying order vectors
        std::vector<std::vector<QualOrder>> thread_results(num_threads);
        // Reserve ~1.6M / num_threads per thread
        const size_t reserve_per_thread = 1700000 / num_threads + 1000;
        for (auto& v : thread_results) v.reserve(reserve_per_thread);

        // Parallel scan: filter and collect qualifying rows
        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_results[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n; i++) {
                if (o_orderdate[i] >= DATE_THRESHOLD) continue;
                int32_t ck = o_custkey[i];
                if (ck < 1 || ck > 1500000 || !cust_building[ck]) continue;
                local.push_back({o_orderkey[i], o_orderdate[i], o_shipprio[i]});
            }
        }

        // Count total qualifying orders
        size_t total_qual = 0;
        for (auto& v : thread_results) total_qual += v.size();

        // Re-size orders_map to fit all qualifying orders
        orders_map.reserve(total_qual + total_qual / 4 + 1024);

        // Sequential fill of hash map + bloom filter (avoids concurrent hash collision)
        for (auto& v : thread_results) {
            for (auto& qo : v) {
                uint64_t h = gendb::hash_int(qo.orderkey);
                orders_map.insert(qo.orderkey, {qo.orderdate, qo.shippriority});
                bloom.insert(h);
            }
        }
    }

    // ============================================================
    // Phase 3: lineitem zone-map pruned parallel scan
    //          with bloom filter pre-check and partitioned aggregation
    //
    // PARTITIONED AGGREGATION strategy (eliminates aggregation_merge bottleneck):
    //   - Use NUM_PARTS = num_threads partitions
    //   - Each thread scans all lineitem blocks but writes ONLY to its own partition
    //     based on: orderkey % NUM_PARTS == thread_id
    //   - Each partition is a single CompactHashMap — no merging needed!
    //   - Result: zero synchronization, zero merge phase, full parallelism
    // ============================================================
    const int num_threads = omp_get_max_threads();
    const int NUM_PARTS = num_threads;

    // Pre-size per-partition aggregation maps
    // ~1.6M unique qualifying orderkeys / NUM_PARTS each
    const size_t part_size = orders_map.size() / NUM_PARTS + 1024;
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> part_agg(
        NUM_PARTS, gendb::CompactHashMap<int32_t, AggEntry>(part_size));

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        // Prefetch lineitem columns
        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        // Load zone map for l_shipdate
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map: " + zm_path);
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size,
                                                      PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Collect qualifying block ranges (skip blocks where max <= threshold)
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

        const size_t num_ranges = scan_ranges.size();

        // Partitioned parallel aggregation:
        // Each thread processes ALL blocks but only aggregates rows where
        // (orderkey % NUM_PARTS == tid), ensuring each partition map is thread-private.
        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto& my_agg = part_agg[tid];

            #pragma omp for schedule(dynamic, 8)
            for (size_t ri = 0; ri < num_ranges; ri++) {
                uint64_t rstart = scan_ranges[ri].start;
                uint64_t rend   = scan_ranges[ri].end;

                for (uint64_t i = rstart; i < rend; i++) {
                    // Per-row shipdate check (partial blocks may have early dates)
                    if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                    int32_t ok = l_orderkey[i];

                    // Bloom filter: skip ~70% of non-matching orderkeys cheaply
                    uint64_t h = gendb::hash_int(ok);
                    if (!bloom.maybe_contains(h)) continue;

                    // Partition check: only process rows belonging to this thread's partition
                    if ((ok & (NUM_PARTS - 1)) != tid) continue;

                    // Hash map probe
                    OrderInfo* oi = orders_map.find(ok);
                    if (!oi) continue;

                    // Accumulate revenue: extendedprice*(100-discount)/100, scale=100
                    int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]) / 100;

                    AggEntry* ae = my_agg.find(ok);
                    if (ae) {
                        ae->revenue_scaled += rev;
                    } else {
                        my_agg.insert(ok, {rev, oi->o_orderdate, oi->o_shippriority});
                    }
                }
            }
        }
    }

    // ============================================================
    // Phase 4: Collect results from all partitions (no merging needed —
    //          each orderkey exists in exactly one partition)
    // ============================================================
    std::vector<ResultRow> all_rows;
    {
        GENDB_PHASE("aggregation_merge");

        size_t total_groups = 0;
        for (int p = 0; p < NUM_PARTS; p++) total_groups += part_agg[p].size();
        all_rows.reserve(total_groups);

        for (int p = 0; p < NUM_PARTS; p++) {
            for (const auto [key, val] : part_agg[p]) {
                all_rows.push_back({key, val.revenue_scaled, val.o_orderdate, val.o_shippriority});
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Comparator: "better" = higher revenue, then earlier orderdate
        // For TopKHeap: cmp(a,b) returns true if a is "worse" (should be evicted)
        // We want top by revenue DESC, so "worse" = lower revenue (or same rev + later date)
        auto worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled < b.revenue_scaled; // a is worse if lower revenue
            return a.o_orderdate > b.o_orderdate;           // a is worse if later date
        };

        gendb::TopKHeap<ResultRow, decltype(worse)> heap(10, worse);
        for (auto& row : all_rows) heap.push(row);
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
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
