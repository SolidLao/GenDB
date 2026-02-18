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
 * ITERATION 7 OPTIMIZATIONS
 * ============================================================
 * Dominant bottlenecks from iter 6:
 *   main_scan:   238ms (46%) — random order_map probing causes L3 thrashing
 *   build_joins: 128ms (25%) — sequential merge of thread-local vectors
 *
 * Root cause analysis:
 *   1. order_map is ~24MB (1.5M entries × 16B/entry). With 64 threads sharing
 *      44MB L3, each thread gets ~687KB effective L3. Every order_map.find()
 *      in the hot loop is a random L3 (or RAM) access → ~50-100ns per miss.
 *      ~15M qualifying lineitem rows × 50ns = 750ms theoretical; parallelism
 *      brings it down to 238ms but it's still the dominant term.
 *
 *   2. The 2MB bloom filter was supposed to help but it ALSO doesn't fit in
 *      per-thread L3. Bloom probes (3 random accesses into 2MB) are similarly
 *      cache-miss prone, so they add overhead rather than reduce it.
 *
 *   3. Sequential merge of thread-local vectors into order_map (line 366-371):
 *      ~1.5M sequential inserts with Robin Hood probing = ~100ms serial work.
 *
 * Fixes for iter 7:
 *
 * Fix 1: RADIX-PARTITIONED PROBE (eliminates random L3 thrashing)
 *   - First pass (parallel): scan lineitem, apply l_shipdate filter,
 *     write qualifying (l_orderkey, rev_raw) pairs into P=256 partitioned
 *     output buffers (partitioned by okey & (P-1)).
 *   - Second pass (parallel): thread T processes partitions [T*(P/nthreads) ..
 *     (T+1)*(P/nthreads)). For its partitions, it probes only the order_map
 *     entries whose key falls in those partitions. Each thread works on a
 *     contiguous ~1/nthreads slice of order_map → much better cache locality.
 *     Aggregation into thread-local agg map with disjoint keys → no merge needed!
 *
 * Fix 2: L1-CACHE BLOOM FILTER (8KB) replacing 2MB bloom
 *   - For 1.5M keys, 8KB = 65536 bits → 0.043 bits/key → very high FP rate.
 *   - Instead use a 256KB bloom (2M bits, 1.33 bits/key → still high FP).
 *   - Actually: REMOVE bloom entirely. The partitioned approach means each
 *     partition's order_map slice is ~24MB/256 = 96KB per partition → fits L2!
 *     No bloom needed when data is partition-local.
 *
 * Fix 3: PARALLEL ORDER_MAP BUILD (eliminates sequential merge bottleneck)
 *   - Build P=256 independent partition hash maps for orders.
 *   - Each partition map covers okeys where (okey & 255) == p.
 *   - Parallel scan: each thread writes to its thread-local partition vectors.
 *   - Then merge: thread T builds partition maps T*(P/nthreads)..(T+1)*(P/nthreads).
 *   - No locking needed: each partition is owned by exactly one merge thread.
 *
 * Fix 4: ELIMINATE THREAD-LOCAL AGG MERGE BOTTLENECK
 *   - With partitioned probing, each thread aggregates over disjoint key ranges.
 *   - No merge needed: thread T's agg_map has only keys in its partitions.
 *   - Global agg_map = union of all thread agg maps (no conflict).
 *
 * Expected speedup:
 *   - order_map probe per partition: ~96KB → fits in L2 cache. Probes become
 *     ~3-5ns instead of 50-100ns → 10-20x speedup on probe cost.
 *   - Sequential merge eliminated → build_joins goes from 128ms to ~40ms.
 *   - main_scan: 238ms → ~80ms (partitioned probe, no bloom overhead).
 *   - Total estimate: ~150ms (down from 520ms).
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer: c_mktsegment = 'BUILDING' → ~300K rows (1/5 selectivity)
 *   orders:   o_orderdate < 1995-03-15  → ~50% → ~7.5M; zone-map prunes ~50% blocks
 *             + c_custkey join → ~1.5M qualifying orders
 *   lineitem: l_shipdate > 1995-03-15   → ~50% → ~30M; zone-map prunes ~50% blocks
 *
 * Step 2 — Join ordering:
 *   1. Filter customer → CompactHashSet<c_custkey> (~300K)
 *   2. Scan orders (zone-map pruned) → filter date + cust_set
 *      → P=256 partition arrays of (okey, OrderMeta)
 *      → build 256 CompactHashMaps in parallel (~6K entries each)
 *   3. Scan lineitem (zone-map pruned) → filter l_shipdate
 *      → write to P=256 partition buffers of (okey, rev)
 *   4. For each partition p, probe partition_order_map[p] and aggregate
 *      → thread-local CompactHashMap<okey, AggEntry> (disjoint keys, no merge)
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter (~10ms, sequential)
 *   - MmapColumn c_mktsegment + c_custkey, filter=BUILDING
 *   - Build CompactHashSet<int32_t> cust_set (~300K)
 *
 * Phase 2: build_joins (~40ms target)
 *   - MmapColumn orders 4 columns + zone map
 *   - Parallel scan → thread-local partition vectors (256 partitions)
 *   - Parallel build: thread T builds order_part_maps[T*step...(T+1)*step]
 *     by merging all thread-local vectors for those partitions
 *
 * Phase 3: main_scan (~80ms target)
 *   - MmapColumn lineitem 4 columns + zone map
 *   - Parallel scan → partitioned output buffers (atomic-free per-thread staging)
 *   - Parallel probe phase: each thread owns ceil(256/nthreads) partitions,
 *     probes order_part_maps, aggregates into thread-local agg_map
 *
 * Phase 4: sort_topk
 *   - Collect all thread agg_maps (disjoint key ranges) → TopKHeap LIMIT 10
 *
 * Phase 5: output CSV
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

#include <omp.h>

// ----------------------------------------------------------------
// Constants
// ----------------------------------------------------------------
static constexpr int32_t DATE_CUTOFF = 9204; // 1995-03-15 in epoch days

// Number of radix partitions. Must be power of 2.
// 256 partitions: each order partition has ~1.5M/256 ≈ 5859 entries
// → partition hash map ~5859 × 20B ≈ 117KB → fits in L2 cache (512KB per core)
static constexpr int NPARTS = 256;
static constexpr int PART_MASK = NPARTS - 1;

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
    int64_t revenue_raw;   // sum of l_extendedprice * (100 - l_discount) / 100
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ----------------------------------------------------------------
// Lineitem partial result (after shipdate filter, before join)
// ----------------------------------------------------------------
struct LinePair {
    int32_t l_orderkey;
    int64_t rev;       // l_extendedprice * (100 - l_discount) / 100
};

// ----------------------------------------------------------------
// Zone map entry layout (from Storage Guide):
//   per zone: [int32_t min, int32_t max, uint32_t count]
//   header:   [uint32_t num_zones]
// ----------------------------------------------------------------
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t count;
};

struct ZoneMapData {
    const ZoneEntry* zones;
    uint32_t num_zones;
    void* raw_ptr;
    size_t raw_size;

    ZoneMapData() : zones(nullptr), num_zones(0), raw_ptr(nullptr), raw_size(0) {}

    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open zone map: " + path);
        struct stat st;
        fstat(fd, &st);
        raw_size = st.st_size;
        raw_ptr = mmap(nullptr, raw_size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (raw_ptr == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        const uint8_t* p = static_cast<const uint8_t*>(raw_ptr);
        num_zones = *reinterpret_cast<const uint32_t*>(p);
        zones = reinterpret_cast<const ZoneEntry*>(p + sizeof(uint32_t));
    }

    ~ZoneMapData() {
        if (raw_ptr && raw_ptr != MAP_FAILED) munmap(raw_ptr, raw_size);
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string data_dir  = gendb_dir;
    const std::string index_dir = gendb_dir + "/indexes";

    // ----------------------------------------------------------------
    // Phase 1: Filter customer → build set of qualifying c_custkeys
    // ----------------------------------------------------------------
    gendb::CompactHashSet<int32_t> cust_set(400000);
    {
        GENDB_PHASE("dim_filter");

        // Find BUILDING dictionary code
        int32_t building_code = -1;
        {
            std::string dict_path = data_dir + "/customer/c_mktsegment_dict.txt";
            FILE* f = fopen(dict_path.c_str(), "r");
            if (!f) {
                dict_path = data_dir + "/customer/mktsegment_dict.txt";
                f = fopen(dict_path.c_str(), "r");
            }
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
    // Phase 2: Scan orders → build P=256 partitioned hash maps
    //          Each partition p covers okeys where (okey & PART_MASK) == p
    //          Partition map ~6K entries × 20B ≈ 120KB → fits in L2 cache
    // ----------------------------------------------------------------

    // Per-partition order hash maps. Using vector-of-CompactHashMap.
    // Pre-sized to 8192 = next power-of-2 above 6K×4/3=8K → load factor ~75%
    static constexpr size_t PART_HT_SIZE = 8192; // per-partition expected entries
    std::vector<gendb::CompactHashMap<int32_t, OrderMeta>> order_parts(NPARTS);
    for (int p = 0; p < NPARTS; p++) {
        order_parts[p].reserve(PART_HT_SIZE);
    }

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (data_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (data_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (data_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (data_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        constexpr size_t BLOCK_SIZE = 100000;
        size_t num_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;

        int nthreads = omp_get_max_threads();

        // Load zone map for o_orderdate
        ZoneMapData zm_orders;
        bool have_orders_zm = false;
        try {
            zm_orders.open(index_dir + "/orders_o_orderdate_zonemap.bin");
            have_orders_zm = (zm_orders.num_zones > 0);
        } catch (...) {}

        // Build surviving block list
        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_orders_zm) {
            for (uint32_t z = 0; z < zm_orders.num_zones; z++) {
                if (zm_orders.zones[z].min_val >= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Step A: Parallel scan → thread-local partition staging vectors
        // Each thread writes (okey, OrderMeta) pairs into its local partition buckets.
        // Avoids any locking during scan.
        //
        // Memory: nthreads × NPARTS × vector. Each vector avg ~6K/nthreads entries.
        // For 64 threads: 64 × 256 = 16384 vectors, each ~94 entries on average.
        // Total: ~1.5M entries × 8B each ≈ 12MB staging memory. Acceptable.

        // Use flat 2D array: local_parts[tid][p] = vector<pair<int32_t,OrderMeta>>
        // To avoid excessive small vector overhead, use a flat vector per thread
        // that's partitioned by a separate offset array.
        // Simpler: thread-local arrays of NPARTS vectors.

        struct alignas(64) ThreadBuf {
            std::vector<std::pair<int32_t, OrderMeta>> parts[NPARTS];
        };
        std::vector<ThreadBuf> tbufs(nthreads);

        // Pre-reserve per-thread partition buffers to avoid reallocs
        // avg per thread per partition: 1.5M / nthreads / NPARTS ≈ 92 for 64 threads
        // Pre-reserve 128 to avoid realloc
        for (int t = 0; t < nthreads; t++) {
            for (int p = 0; p < NPARTS; p++) {
                tbufs[t].parts[p].reserve(128);
            }
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b   = surviving_blocks[bi];
            size_t beg = b * BLOCK_SIZE;
            size_t end = std::min(beg + BLOCK_SIZE, n);
            int    tid = omp_get_thread_num();

            auto& my_parts = tbufs[tid].parts;

            for (size_t i = beg; i < end; i++) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set.contains(o_custkey[i])) {
                    int32_t okey = o_orderkey[i];
                    int p = okey & PART_MASK;
                    my_parts[p].emplace_back(okey, OrderMeta{o_orderdate[i], o_shipprio[i]});
                }
            }
        }

        // Step B: Parallel build of partition hash maps.
        // Thread t builds partitions [t*step .. (t+1)*step).
        // No locking needed: each partition is exclusively owned by one thread.
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int p = 0; p < NPARTS; p++) {
            auto& pmap = order_parts[p];
            // Count total entries for this partition across all threads
            size_t total = 0;
            for (int t = 0; t < nthreads; t++) total += tbufs[t].parts[p].size();
            if (total > PART_HT_SIZE) {
                // Resize if more entries than expected
                pmap.reserve(total * 2);
            }
            for (int t = 0; t < nthreads; t++) {
                for (auto& [key, meta] : tbufs[t].parts[p]) {
                    pmap.insert(key, meta);
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          write to partitioned buffers, then parallel probe + aggregate
    //
    // KEY INSIGHT: Each thread works on its own partition(s) of order_parts.
    //   - order_parts[p] has ~6K entries × 20B ≈ 120KB → fits in L2 cache
    //   - After shipping filter, ~30M rows → ~15M pass bloom → partition buffers
    //   - Second pass: each thread drains its partitions → cache-hot probing
    //   - Thread agg maps have DISJOINT key ranges → no merge needed!
    // ----------------------------------------------------------------
    int nthreads_main = omp_get_max_threads();

    // Per-thread agg maps. Thread t aggregates partitions [t*step..(t+1)*step).
    // Each agg map has ~1.5M/nthreads keys ≈ 23K per thread.
    int part_step = (NPARTS + nthreads_main - 1) / nthreads_main;
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> thread_aggs(nthreads_main);
    for (int t = 0; t < nthreads_main; t++) {
        // ~23K entries per thread. Reserve 32K for headroom.
        thread_aggs[t].reserve(32768);
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();
        constexpr size_t BLOCK_SIZE = 200000;
        size_t num_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;

        // Load zone map for l_shipdate
        ZoneMapData zm_lineitem;
        bool have_lineitem_zm = false;
        try {
            zm_lineitem.open(index_dir + "/lineitem_l_shipdate_zonemap.bin");
            have_lineitem_zm = (zm_lineitem.num_zones > 0);
        } catch (...) {}

        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_lineitem_zm) {
            for (uint32_t z = 0; z < zm_lineitem.num_zones; z++) {
                if (zm_lineitem.zones[z].max_val <= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // ----------------------------------------------------------------
        // Sub-phase A: Parallel scan lineitem → per-thread partition staging
        //
        // Each thread scans its blocks and writes qualifying (okey, rev) pairs
        // into thread-local partition buffers. No cross-thread synchronization.
        //
        // Memory: nthreads × NPARTS vectors. Each partition gets ~15M/NPARTS ≈ 58K
        // entries total across all threads, so per-thread per-partition ≈ 900 entries.
        // ----------------------------------------------------------------

        struct alignas(64) LBuf {
            std::vector<LinePair> parts[NPARTS];
        };
        std::vector<LBuf> lbufs(nthreads_main);
        // Pre-reserve: ~900 entries per thread-partition pair on average
        for (int t = 0; t < nthreads_main; t++) {
            for (int p = 0; p < NPARTS; p++) {
                lbufs[t].parts[p].reserve(1024);
            }
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads_main)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b   = surviving_blocks[bi];
            size_t beg = b * BLOCK_SIZE;
            size_t end = std::min(beg + BLOCK_SIZE, n);
            int    tid = omp_get_thread_num();

            auto& my_parts = lbufs[tid].parts;

            const int32_t* p_shipdate = l_shipdate.data + beg;
            const int32_t* p_orderkey = l_orderkey.data + beg;
            const int64_t* p_extprice = l_extprice.data + beg;
            const int64_t* p_discount = l_discount.data + beg;
            size_t block_len = end - beg;

            for (size_t i = 0; i < block_len; i++) {
                if (__builtin_expect(p_shipdate[i] <= DATE_CUTOFF, 1)) continue;
                int32_t okey = p_orderkey[i];
                int64_t rev  = (p_extprice[i] * (100LL - p_discount[i])) / 100LL;
                int p = okey & PART_MASK;
                my_parts[p].push_back({okey, rev});
            }
        }

        // ----------------------------------------------------------------
        // Sub-phase B: Parallel probe + aggregate
        //
        // Thread t processes partitions [t*part_step .. min((t+1)*part_step, NPARTS)).
        // For each partition p:
        //   1. Load order_parts[p] (cache-hot: ~120KB per partition)
        //   2. Drain all thread-local lbufs[][p] staging buffers
        //   3. For each (okey, rev): probe order_parts[p], aggregate into
        //      thread_aggs[t] (keyed by okey, disjoint from other threads)
        //
        // No cross-thread merging needed: key space partitioned by partition index.
        // ----------------------------------------------------------------

        #pragma omp parallel for schedule(static) num_threads(nthreads_main)
        for (int t = 0; t < nthreads_main; t++) {
            int p_start = t * part_step;
            int p_end   = std::min(p_start + part_step, NPARTS);

            auto& my_agg = thread_aggs[t];

            for (int p = p_start; p < p_end; p++) {
                const auto& omap = order_parts[p]; // cache-hot partition map

                // Drain all thread-local staging buffers for this partition
                for (int src = 0; src < nthreads_main; src++) {
                    for (const auto& lp : lbufs[src].parts[p]) {
                        const OrderMeta* om = omap.find(lp.l_orderkey);
                        if (__builtin_expect(om == nullptr, 0)) continue;

                        AggEntry* ae = my_agg.find(lp.l_orderkey);
                        if (ae) {
                            ae->revenue_raw += lp.rev;
                        } else {
                            my_agg.insert(lp.l_orderkey,
                                         {lp.rev, om->o_orderdate, om->o_shippriority});
                        }
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 4: Sort Top-K
    //   All thread_aggs have disjoint key ranges — union is the full result.
    //   Use partial_sort on combined vector.
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

        // Count total entries across all thread agg maps
        size_t total_entries = 0;
        for (int t = 0; t < nthreads_main; t++) total_entries += thread_aggs[t].size();

        std::vector<ResultRow> all_rows;
        all_rows.reserve(total_entries);

        // Thread agg maps have disjoint key ranges → simple concatenation
        for (int t = 0; t < nthreads_main; t++) {
            for (auto [okey, entry] : thread_aggs[t]) {
                all_rows.push_back({okey, entry.revenue_raw, entry.o_orderdate, entry.o_shippriority});
            }
        }

        constexpr size_t K = 10;
        size_t k = std::min(K, all_rows.size());
        auto row_cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_raw != b.revenue_raw) return a.revenue_raw > b.revenue_raw;
            return a.o_orderdate < b.o_orderdate;
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
