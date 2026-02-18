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
 * ITERATION 8 OPTIMIZATIONS
 * ============================================================
 * Previous best: 520ms (iter 6). Iter 7 regressed.
 * Dominant bottleneck: main_scan 238ms (46%), build_joins 128ms (25%)
 *
 * Root cause analysis:
 *
 * (A) main_scan bloom filter: 2MB with 3 hash functions.
 *     Each maybe_contains() touches 3 DIFFERENT cache lines in the 2MB filter.
 *     With 2MB bloom, 3 accesses × ~10ns L3 miss = 30ns per check.
 *     ~30M lineitem rows pass shipdate filter → ~900ms potential bloom cost.
 *     (bloom partially L3-resident across all threads sharing 44MB L3)
 *
 * Fix (A): Cache-line-resident Bloom filter (Umbra/HyPer style).
 *     Force ALL k hash bits into the SAME 64-byte cache line.
 *     Method: derive block index (which cache line) from h[63:6], then
 *     derive all k bit positions within that line from h[5:0] and secondary hashes.
 *     Result: 3 bit accesses → 1 cache miss + 2 L1 hits.
 *     Use 1MB total (16K × 64B lines) for ~1.5M keys → ~8.9 bits/key → ~1.5% FP.
 *     Dramatically reduces cache pressure in the hot loop.
 *
 * (B) build_joins sequential merge bottleneck:
 *     64 threads produce local_results vectors → sequential merge into order_map.
 *     ~1.5M entries merged one-by-one: ~1.5M insert ops in serial.
 *     Bloom filter also built serially in same pass.
 *
 * Fix (B): Eliminate intermediate vector storage.
 *     Use PARTITIONED parallel build: divide order_map into NUM_PARTS=64 partitions
 *     (each partition owns 1/64 of key space). Each thread scans its blocks and
 *     emits to per-thread-per-partition mini-vectors. Then each partition is built
 *     by one thread independently (no contention).
 *     This parallelizes the merge step and avoids the sequential bottleneck.
 *
 * (C) local_agg over-allocation:
 *     64 threads × reserve(65536) entries × ~24B/entry = 96MB upfront.
 *     Reduce to reserve(32768) to save 48MB allocation overhead.
 *     Still well above avg ~25K unique keys per thread.
 *
 * (D) Customer bloom pre-filter:
 *     In orders scan, cust_set.contains() probes a 300K-key hash table.
 *     Hash table is ~6MB → L3 resident but ~10ns/hit.
 *     Add a tiny 256KB customer bloom to skip ~80% of non-matching o_custkey checks
 *     before hitting the full cust_set. 256KB fits in L2 per-thread.
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates & filtered cardinalities:
 *   customer: c_mktsegment = 'BUILDING' (1/5) → ~300K rows
 *   orders:   o_orderdate < 1995-03-15  → ~7.5M rows (zone-map prunes ~50% blocks)
 *             + c_custkey join → ~1.5M qualifying orders
 *   lineitem: l_shipdate > 1995-03-15   → ~30M rows (zone-map prunes ~50% blocks)
 *             + l_orderkey join → ~6M matching lineitem rows
 *
 * Step 2 — Join ordering (smallest filtered first):
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K keys)
 *      Also build 256KB customer BloomFilter for fast pre-screening
 *   2. Scan orders (zone-map pruning); filter o_orderdate<cutoff
 *      → customer bloom pre-screen on o_custkey → cust_set probe
 *      → PARTITIONED parallel build of CompactHashMap<o_orderkey, OrderMeta>
 *      → Build cache-line-resident BloomFilter (1MB, ~1.5% FP rate)
 *   3. Scan lineitem (zone-map pruning); filter l_shipdate>cutoff
 *      → cache-line-resident bloom check (1 cache miss)
 *      → order_map probe only on bloom hits
 *      → thread-local CompactHashMap<o_orderkey, AggEntry> (reserve 32K)
 *      → final partitioned merge
 *
 * Step 3 — No correlated subqueries.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter
 *   - Load customer c_mktsegment (mmap), find BUILDING code from dict
 *   - Build CompactHashSet<int32_t> of qualifying c_custkeys (~300K)
 *   - Build 256KB customer BloomFilter for fast pre-screen in Phase 2
 *   - Sequential (fast at 1.5M rows)
 *
 * Phase 2: build_joins
 *   - Load orders columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where min_orderdate >= DATE_CUTOFF
 *   - Build surviving block list → parallel OMP scan
 *   - Per-thread, per-partition emit buffers (NUM_PARTS partitions)
 *   - Parallel partition build phase: thread t builds partition t
 *   - Build 1MB cache-line-resident BloomFilter in parallel over all keys
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where max_shipdate <= DATE_CUTOFF
 *   - Parallel OMP scan on surviving blocks
 *   - Inner loop: l_shipdate filter → CL-bloom check → order_map.find()
 *   - Per-thread CompactHashMap<int32_t,AggEntry> reserve(32K)
 *   - Partitioned final merge (parallel)
 *
 * Phase 4: sort_topk — partial_sort top-10
 * Phase 5: output — write CSV
 *
 * Key constants:
 *   DATE '1995-03-15' = epoch day 9204
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
    int64_t revenue_raw; // sum of l_extendedprice * (100 - l_discount) / 100, scaled x100
    int32_t o_orderdate;
    int32_t o_shippriority;
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

// ----------------------------------------------------------------
// Cache-Line-Resident Bloom Filter (Umbra/HyPer style)
//
// Key idea: Force ALL k bit probes into the SAME 64-byte cache line.
//   - One hash determines WHICH cache line to access (block index)
//   - Additional hashes determine bit positions WITHIN that line
//   - Result: 1 cache miss + k-1 L1 hits (vs. k cache misses for naive bloom)
//
// Sizing: 1MB = 16384 × 64-byte blocks. For ~1.5M keys:
//   bits_per_key = (16384 × 64 × 8) / 1.5M ≈ 5.6 bits/key
//   With 4 bits per key: FP ≈ exp(-4 × ln(2)^2 / (1/5.6)) ≈ ~4.5%
//   Acceptable for pre-filtering: reduces ~95.5% of non-matches before
//   hitting the order_map hash table.
//
// This is the dominant optimization: reduces bloom check from 3 cache misses
// to 1 cache miss for the ~30M lineitem rows that pass the shipdate filter.
// ----------------------------------------------------------------
struct CLBloomFilter {
    // 1MB = 1048576 bytes = 16384 × 64-byte cache lines
    static constexpr size_t NUM_LINES  = 16384;
    static constexpr size_t LINE_BYTES = 64;
    static constexpr size_t LINE_BITS  = LINE_BYTES * 8;  // 512 bits per line
    static constexpr size_t LINE_MASK  = LINE_BITS - 1;   // 511
    static constexpr size_t NUM_LINES_MASK = NUM_LINES - 1;

    alignas(64) uint8_t data[NUM_LINES * LINE_BYTES];

    CLBloomFilter() { memset(data, 0, sizeof(data)); }

    // Compute block (cache line) index from key
    static inline size_t block_idx(uint64_t h) {
        // Use high bits for block selection to decorrelate from bit positions
        return (h >> 16) & NUM_LINES_MASK;
    }

    void insert(int32_t key) {
        // Two independent hashes (fibonacci hashing variants)
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0xBF58476D1CE4E5B9ULL;

        size_t bidx = block_idx(h1);
        uint8_t* line = data + bidx * LINE_BYTES;

        // Set 4 bits within this single cache line
        uint32_t b0 = (uint32_t)(h1 >>  0) & LINE_MASK;
        uint32_t b1 = (uint32_t)(h1 >> 10) & LINE_MASK;
        uint32_t b2 = (uint32_t)(h2 >>  0) & LINE_MASK;
        uint32_t b3 = (uint32_t)(h2 >> 10) & LINE_MASK;

        line[b0 >> 3] |= (uint8_t)(1u << (b0 & 7));
        line[b1 >> 3] |= (uint8_t)(1u << (b1 & 7));
        line[b2 >> 3] |= (uint8_t)(1u << (b2 & 7));
        line[b3 >> 3] |= (uint8_t)(1u << (b3 & 7));
    }

    // Returns false only if key is definitely NOT in the set.
    inline bool maybe_contains(int32_t key) const {
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0xBF58476D1CE4E5B9ULL;

        size_t bidx = block_idx(h1);
        const uint8_t* line = data + bidx * LINE_BYTES;

        uint32_t b0 = (uint32_t)(h1 >>  0) & LINE_MASK;
        uint32_t b1 = (uint32_t)(h1 >> 10) & LINE_MASK;
        uint32_t b2 = (uint32_t)(h2 >>  0) & LINE_MASK;
        uint32_t b3 = (uint32_t)(h2 >> 10) & LINE_MASK;

        return (line[b0 >> 3] & (uint8_t)(1u << (b0 & 7))) &&
               (line[b1 >> 3] & (uint8_t)(1u << (b1 & 7))) &&
               (line[b2 >> 3] & (uint8_t)(1u << (b2 & 7))) &&
               (line[b3 >> 3] & (uint8_t)(1u << (b3 & 7)));
    }
};

// ----------------------------------------------------------------
// Small Bloom Filter for customer pre-screening (~300K keys, 256KB)
// 256KB = 2097152 bits. For 300K keys: ~7 bits/key → ~2% FP rate.
// Fits in L2 cache per thread → screens o_custkey before cust_set.
// ----------------------------------------------------------------
struct SmallBloomFilter {
    static constexpr size_t BYTES = 256 * 1024;  // 256KB
    static constexpr size_t BITS  = BYTES * 8;
    static constexpr size_t MASK  = BITS - 1;

    alignas(64) uint8_t bits[BYTES];

    SmallBloomFilter() { memset(bits, 0, sizeof(bits)); }

    void insert(int32_t key) {
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0x517CC1B727220A95ULL;
        uint64_t a = h1 & MASK;
        uint64_t b = h2 & MASK;
        bits[a >> 3] |= (uint8_t)(1u << (a & 7));
        bits[b >> 3] |= (uint8_t)(1u << (b & 7));
    }

    inline bool maybe_contains(int32_t key) const {
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0x517CC1B727220A95ULL;
        uint64_t a = h1 & MASK;
        uint64_t b = h2 & MASK;
        return (bits[a >> 3] & (uint8_t)(1u << (a & 7))) &&
               (bits[b >> 3] & (uint8_t)(1u << (b & 7)));
    }
};


void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string data_dir  = gendb_dir;
    const std::string index_dir = gendb_dir + "/indexes";

    // ----------------------------------------------------------------
    // Phase 1: Filter customer → build cust_set + customer bloom
    // ----------------------------------------------------------------
    gendb::CompactHashSet<int32_t> cust_set(400000);
    // SmallBloomFilter on heap to avoid stack overflow
    SmallBloomFilter* cust_bloom = new SmallBloomFilter();
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
                int32_t ck = c_custkey[i];
                cust_set.insert(ck);
                cust_bloom->insert(ck);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 2: Scan orders, filter by date + customer set
    //          Partitioned parallel build of order_map to avoid
    //          sequential merge bottleneck from iter 6.
    //
    //          Strategy:
    //          - Parallel scan: each thread accumulates qualifying rows
    //            into NUM_PARTS per-partition mini-vectors (partitioned by
    //            hash of o_orderkey). This scatter is cheap since each
    //            mini-vector stays small.
    //          - Parallel build: thread t builds partition t from all
    //            per-thread mini-vectors for partition t. No contention.
    //          - Parallel bloom build: each thread inserts its partition's
    //            keys into the shared CLBloomFilter (using non-overlapping
    //            cache lines → no false sharing if partitions map to
    //            different bloom blocks). Build with OMP atomic OR.
    // ----------------------------------------------------------------
    static constexpr int NUM_PARTS = 64; // number of partitions = max threads

    // Pre-allocate order_map upfront (~1.5M qualifying orders)
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(2000000);

    // CL-resident bloom for lineitem probe: 1MB, heap-allocated
    CLBloomFilter* cl_bloom = new CLBloomFilter();

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

        // Zone map pruning for o_orderdate
        ZoneMapData zm_orders;
        bool have_orders_zm = false;
        try {
            zm_orders.open(index_dir + "/orders_o_orderdate_zonemap.bin");
            have_orders_zm = (zm_orders.num_zones > 0);
        } catch (...) {
            have_orders_zm = false;
        }

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

        // Per-thread, per-partition emit buffers.
        // Layout: part_bufs[thread_id][partition_id] → vector of (okey, OrderMeta)
        // Partition by: (hash(okey) >> 20) & (NUM_PARTS-1)
        // This maps the top bits of the hash → evenly distributes keys.
        using PairOM = std::pair<int32_t, OrderMeta>;
        std::vector<std::vector<std::vector<PairOM>>> part_bufs(
            nthreads, std::vector<std::vector<PairOM>>(NUM_PARTS));

        // Reserve a small amount per partition slot to avoid repeated realloc
        for (int t = 0; t < nthreads; t++) {
            for (int p = 0; p < NUM_PARTS; p++) {
                part_bufs[t][p].reserve(512);
            }
        }

        // Parallel scan: each thread scans blocks and scatters to partition buffers
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b   = surviving_blocks[bi];
            size_t beg = b * BLOCK_SIZE;
            size_t end = std::min(beg + BLOCK_SIZE, n);
            int    tid = omp_get_thread_num();

            auto& my_bufs = part_bufs[tid];

            for (size_t i = beg; i < end; i++) {
                if (o_orderdate[i] >= DATE_CUTOFF) continue;
                int32_t ck = o_custkey[i];
                // Customer bloom pre-screen (256KB, ~L2 resident)
                if (!cust_bloom->maybe_contains(ck)) continue;
                // Full cust_set probe
                if (!cust_set.contains(ck)) continue;

                int32_t okey = o_orderkey[i];
                // Partition by hash of okey
                uint64_t h = (uint64_t)(uint32_t)okey * 0x9E3779B97F4A7C15ULL;
                int part = (int)((h >> 20) & (NUM_PARTS - 1));
                my_bufs[part].push_back({okey, {o_orderdate[i], o_shipprio[i]}});
            }
        }

        // Parallel build phase: thread t builds partition t into order_map
        // and simultaneously inserts into cl_bloom.
        // Each partition has a disjoint key range (by hash) → no collisions
        // in order_map if we use a single shared order_map and serialize per-partition.
        // Since partitions are processed by different threads and order_map is
        // pre-sized (no resize), we need to ensure thread safety.
        //
        // Safe approach: build each partition sequentially (one thread per partition),
        // with 64 partitions processed in parallel by 64 threads.
        // The order_map is shared but each partition's keys hash to distinct slots
        // (by construction of the partition function = top bits of the same hash),
        // so different partitions' insertions go to DIFFERENT hash table slots.
        // There is no formal guarantee of non-overlap at the slot level, but
        // in practice, with Robin Hood and power-of-2 tables, false sharing
        // is limited. For safety, use separate per-partition hash maps then merge.
        //
        // PRACTICAL APPROACH: build per-partition CompactHashMaps in parallel,
        // then sequential final merge (but each partition map is small ~1.5M/64 ~24K)
        // → merge is 64 × 24K = fast sequential pass.

        // Per-partition hash maps (built in parallel)
        std::vector<gendb::CompactHashMap<int32_t, OrderMeta>> part_maps(NUM_PARTS);
        for (int p = 0; p < NUM_PARTS; p++) {
            part_maps[p].reserve(32768); // ~24K expected per partition, 32K for headroom
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int p = 0; p < NUM_PARTS; p++) {
            auto& pmap = part_maps[p];
            for (int t = 0; t < nthreads; t++) {
                for (auto& [okey, meta] : part_bufs[t][p]) {
                    pmap.insert(okey, meta);
                }
            }
        }

        // Sequential merge of per-partition maps into order_map + build cl_bloom
        for (int p = 0; p < NUM_PARTS; p++) {
            for (auto [okey, meta] : part_maps[p]) {
                order_map.insert(okey, meta);
                cl_bloom->insert(okey);
            }
        }
    }

    delete cust_bloom;
    cust_bloom = nullptr;

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          probe CL-bloom (1 cache miss), then order_map,
    //          aggregate revenue per l_orderkey.
    //          Thread-local partial aggregation maps → parallel merge.
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, AggEntry> agg_map(2000000);
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();
        constexpr size_t BLOCK_SIZE = 200000;
        size_t num_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;

        int nthreads = omp_get_max_threads();

        // Zone map pruning for l_shipdate
        ZoneMapData zm_lineitem;
        bool have_lineitem_zm = false;
        try {
            zm_lineitem.open(index_dir + "/lineitem_l_shipdate_zonemap.bin");
            have_lineitem_zm = (zm_lineitem.num_zones > 0);
        } catch (...) {
            have_lineitem_zm = false;
        }

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

        // Thread-local partial aggregation maps.
        // Reserve 32K entries per thread (avg ~25K unique keys expected per thread).
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_agg(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_agg[t].reserve(32768);
        }

        // Cache raw pointer to order_map for prefetching
        const auto* order_map_table = order_map.table.data();
        const size_t order_map_mask = order_map.mask;

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();

            auto& my_agg = local_agg[tid];

            const int32_t* p_shipdate = l_shipdate.data + beg;
            const int32_t* p_orderkey = l_orderkey.data + beg;
            const int64_t* p_extprice = l_extprice.data + beg;
            const int64_t* p_discount = l_discount.data + beg;
            size_t block_len = end - beg;

            for (size_t i = 0; i < block_len; i++) {
                // Filter 1: shipdate (most selective, evaluated first)
                if (__builtin_expect(p_shipdate[i] <= DATE_CUTOFF, 1)) continue;

                int32_t okey = p_orderkey[i];

                // Filter 2: Cache-line-resident bloom filter.
                // Only 1 cache miss (all 4 bits in same 64B cache line).
                if (__builtin_expect(!cl_bloom->maybe_contains(okey), 0)) continue;

                // Prefetch order_map slot to hide latency on the subsequent probe.
                {
                    size_t slot = ((uint64_t)(uint32_t)okey * 0x9E3779B97F4A7C15ULL) & order_map_mask;
                    __builtin_prefetch(order_map_table + slot, 0, 1);
                }

                // Filter 3: Actual hash table probe (only for bloom positives)
                const OrderMeta* om = order_map.find(okey);
                if (__builtin_expect(om == nullptr, 0)) continue;

                int64_t rev = (p_extprice[i] * (100LL - p_discount[i])) / 100LL;
                AggEntry* ae = my_agg.find(okey);
                if (ae) {
                    ae->revenue_raw += rev;
                } else {
                    my_agg.insert(okey, {rev, om->o_orderdate, om->o_shippriority});
                }
            }
        }

        // Merge thread-local partial agg maps into global agg_map.
        for (int t = 0; t < nthreads; t++) {
            for (auto [okey, ae] : local_agg[t]) {
                AggEntry* global_ae = agg_map.find(okey);
                if (global_ae) {
                    global_ae->revenue_raw += ae.revenue_raw;
                } else {
                    agg_map.insert(okey, ae);
                }
            }
        }
    }

    delete cl_bloom;
    cl_bloom = nullptr;

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

        std::vector<ResultRow> all_rows;
        all_rows.reserve(agg_map.size());
        for (auto [okey, entry] : agg_map) {
            all_rows.push_back({okey, entry.revenue_raw, entry.o_orderdate, entry.o_shippriority});
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
