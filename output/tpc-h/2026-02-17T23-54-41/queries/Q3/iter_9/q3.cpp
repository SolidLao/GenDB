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
 * ITERATION 9 OPTIMIZATIONS
 * ============================================================
 * Previous: 472ms (iter 8). build_joins=170ms (36%), main_scan=148ms (31%).
 *
 * Root cause of both bottlenecks:
 *   Both build_joins and main_scan suffer from SEQUENTIAL MERGE phases.
 *
 * (A) build_joins sequential merge (lines 499-504 in iter8):
 *     After parallel per-partition map build, ~1.5M entries are merged
 *     sequentially into order_map and cl_bloom. This is O(1.5M) serial ops.
 *
 * Fix (A): Eliminate order_map entirely.
 *     Keep part_maps[NUM_PARTS] alive as THE lookup structure.
 *     In main_scan, for each lineitem: partition = (hash(okey)>>20)&(PARTS-1),
 *     then probe part_maps[partition] directly. No merge needed.
 *     Build cl_bloom in parallel: second OMP loop over parts, each thread
 *     inserts its partition's keys into the shared cl_bloom. Different
 *     partitions have different keys → their hash(key) values differ →
 *     their bloom cache line accesses differ. Bit-ORs within a cache line
 *     from different threads are safe using std::atomic byte-level ops,
 *     BUT since different partitions hash to mostly-distinct cache lines
 *     in a 16384-line bloom and we use byte-level |=, we use atomic bytes.
 *
 *     Actually: the CL-bloom assigns block_idx = (h1 >> 16) & 16383 where h1
 *     is derived from the key. Two keys in different partitions (different top
 *     bits of hash) → different block_idx values (very likely). The small
 *     probability of collision on a bloom cache line is harmless for correctness
 *     (bloom is a probabilistic structure). But concurrent |= on bytes IS a
 *     data race unless atomic. Use __sync_or_and_fetch or std::atomic<uint8_t>.
 *     Simpler: build bloom SERIALLY (it's just ~1.5M inserts from part_maps,
 *     which is fast since bloom has no resize). Measure first.
 *
 *     REVISED: Build bloom in parallel using atomic<uint8_t> for the data array.
 *
 * (B) main_scan sequential agg merge:
 *     64 thread-local maps → sequential merge into global agg_map.
 *     At ~1.5M total unique orderkeys, the serial merge is O(1.5M).
 *
 * Fix (B): Partitioned parallel agg merge.
 *     Thread-local buffers → scatter to NUM_PARTS agg partition buffers →
 *     parallel per-partition agg maps → collect from all part_agg maps.
 *     Mirrors the build_joins approach.
 *
 * (C) part_bufs memory: 64 threads × 64 parts × reserve(512) × ~16B = 32MB.
 *     Reduce to reserve(256) to halve alloc overhead while still covering
 *     the expected avg of ~366 entries per (thread, partition).
 *     Actually average is 1.5M/(64×64) ≈ 366, so 512 covers with headroom.
 *     Keep 512 for fewer reallocations.
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer: c_mktsegment = 'BUILDING' (1/5) → ~300K rows
 *   orders:   o_orderdate < 1995-03-15  → zone-map prunes → ~7.5M rows scanned
 *             + c_custkey join → ~1.5M qualifying orders
 *   lineitem: l_shipdate > 1995-03-15   → zone-map prunes → ~30M rows scanned
 *             + l_orderkey join → ~6M matching lineitem rows
 *
 * Step 2 — Join ordering:
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K keys)
 *      + 256KB SmallBloomFilter for fast pre-screen
 *   2. Parallel scan orders → zone-map prune → customer bloom → cust_set
 *      → partitioned parallel build: part_maps[NUM_PARTS] CompactHashMap<o_orderkey,OrderMeta>
 *      → parallel bloom build into CLBloomFilter (atomic bytes)
 *   3. Parallel scan lineitem → zone-map prune → l_shipdate filter
 *      → CL-bloom check → probe part_maps[partition(okey)] directly
 *      → partitioned scatter to agg_part_bufs
 *      → parallel build part_agg[NUM_PARTS] CompactHashMap<okey,AggEntry>
 *
 * Step 3 — No correlated subqueries.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter
 *   - Load customer columns (mmap), find BUILDING dict code
 *   - Build CompactHashSet<int32_t> of qualifying c_custkeys (~300K)
 *   - Build 256KB SmallBloomFilter for Phase 2 pre-screening
 *
 * Phase 2: build_joins
 *   - Load orders columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where min_orderdate >= DATE_CUTOFF
 *   - Per-thread scatter to part_bufs[tid][part] using top bits of hash(o_orderkey)
 *   - Parallel build: thread t builds part_maps[t] from all part_bufs[*][t]
 *   - Parallel bloom build: thread t atomically ORs bits for part_maps[t] keys
 *     into shared CLBloomFilter (atomic<uint8_t> data array)
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where max_shipdate <= DATE_CUTOFF
 *   - Per-thread: l_shipdate filter → CL-bloom → probe part_maps[part(okey)]
 *     → accumulate revenue in thread-local agg scratch
 *   - Partitioned agg: scatter to agg_part_bufs, parallel build part_agg[p]
 *
 * Phase 4: sort_topk — partial_sort top-10 across all part_agg maps
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
static constexpr int NUM_PARTS = 64;         // partitions = max OMP threads

// Partition function: top bits of fibonacci hash → partition index
static inline int partition_of(int32_t key) {
    uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
    return (int)((h >> 20) & (NUM_PARTS - 1));
}

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
// Uses std::atomic<uint8_t> for lock-free parallel insertion.
//
// 1MB = 16384 × 64-byte blocks.
// All 4 bit probes target the SAME 64-byte cache line → 1 cache miss.
// ----------------------------------------------------------------
struct CLBloomFilter {
    static constexpr size_t NUM_LINES  = 16384;
    static constexpr size_t LINE_BYTES = 64;
    static constexpr size_t LINE_BITS  = LINE_BYTES * 8;  // 512 bits per line
    static constexpr size_t LINE_MASK  = LINE_BITS - 1;   // 511
    static constexpr size_t NUM_LINES_MASK = NUM_LINES - 1;

    // Using atomic bytes for lock-free parallel build
    alignas(64) std::atomic<uint8_t> data[NUM_LINES * LINE_BYTES];

    CLBloomFilter() {
        for (size_t i = 0; i < NUM_LINES * LINE_BYTES; i++)
            data[i].store(0, std::memory_order_relaxed);
    }

    static inline size_t block_idx(uint64_t h) {
        return (h >> 16) & NUM_LINES_MASK;
    }

    // Thread-safe insert using atomic fetch_or
    void insert(int32_t key) {
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0xBF58476D1CE4E5B9ULL;

        size_t bidx = block_idx(h1);
        auto* line = data + bidx * LINE_BYTES;

        uint32_t b0 = (uint32_t)(h1 >>  0) & LINE_MASK;
        uint32_t b1 = (uint32_t)(h1 >> 10) & LINE_MASK;
        uint32_t b2 = (uint32_t)(h2 >>  0) & LINE_MASK;
        uint32_t b3 = (uint32_t)(h2 >> 10) & LINE_MASK;

        line[b0 >> 3].fetch_or((uint8_t)(1u << (b0 & 7)), std::memory_order_relaxed);
        line[b1 >> 3].fetch_or((uint8_t)(1u << (b1 & 7)), std::memory_order_relaxed);
        line[b2 >> 3].fetch_or((uint8_t)(1u << (b2 & 7)), std::memory_order_relaxed);
        line[b3 >> 3].fetch_or((uint8_t)(1u << (b3 & 7)), std::memory_order_relaxed);
    }

    // Returns false only if key is definitely NOT in the set.
    // Non-atomic load is safe for read-only phase (after all writes done).
    inline bool maybe_contains(int32_t key) const {
        uint64_t h1 = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)(uint32_t)key * 0xBF58476D1CE4E5B9ULL;

        size_t bidx = block_idx(h1);
        const auto* line = data + bidx * LINE_BYTES;

        uint32_t b0 = (uint32_t)(h1 >>  0) & LINE_MASK;
        uint32_t b1 = (uint32_t)(h1 >> 10) & LINE_MASK;
        uint32_t b2 = (uint32_t)(h2 >>  0) & LINE_MASK;
        uint32_t b3 = (uint32_t)(h2 >> 10) & LINE_MASK;

        return (line[b0 >> 3].load(std::memory_order_relaxed) & (uint8_t)(1u << (b0 & 7))) &&
               (line[b1 >> 3].load(std::memory_order_relaxed) & (uint8_t)(1u << (b1 & 7))) &&
               (line[b2 >> 3].load(std::memory_order_relaxed) & (uint8_t)(1u << (b2 & 7))) &&
               (line[b3 >> 3].load(std::memory_order_relaxed) & (uint8_t)(1u << (b3 & 7)));
    }
};

// ----------------------------------------------------------------
// Small Bloom Filter for customer pre-screening (~300K keys, 256KB)
// 256KB = 2097152 bits. For 300K keys: ~7 bits/key → ~2% FP rate.
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
    // Phase 2: Scan orders, filter by date + customer set.
    //
    // Key change vs iter 8:
    //   - part_maps[NUM_PARTS] are the FINAL lookup structure (no order_map)
    //   - cl_bloom is built in PARALLEL: thread t inserts keys from part_maps[t]
    //     using atomic fetch_or on each byte → no locks, no false sharing issues
    //     (at the semantic level; bloom false-sharing has no correctness impact)
    //
    // This eliminates the sequential merge of ~1.5M entries into order_map.
    // ----------------------------------------------------------------

    // Per-partition order maps — built in parallel, used directly as probe table
    std::vector<gendb::CompactHashMap<int32_t, OrderMeta>> part_maps(NUM_PARTS);
    for (int p = 0; p < NUM_PARTS; p++) {
        part_maps[p].reserve(32768); // ~24K expected per partition
    }

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
        using PairOM = std::pair<int32_t, OrderMeta>;
        std::vector<std::vector<std::vector<PairOM>>> part_bufs(
            nthreads, std::vector<std::vector<PairOM>>(NUM_PARTS));

        for (int t = 0; t < nthreads; t++) {
            for (int p = 0; p < NUM_PARTS; p++) {
                part_bufs[t][p].reserve(512);
            }
        }

        // Parallel scan: scatter to partition buffers
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
                if (!cust_bloom->maybe_contains(ck)) continue;
                if (!cust_set.contains(ck)) continue;

                int32_t okey = o_orderkey[i];
                int part = partition_of(okey);
                my_bufs[part].push_back({okey, {o_orderdate[i], o_shipprio[i]}});
            }
        }

        // Parallel build phase: thread t builds part_maps[t] from all part_bufs[*][t]
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int p = 0; p < NUM_PARTS; p++) {
            auto& pmap = part_maps[p];
            for (int t = 0; t < nthreads; t++) {
                for (auto& [okey, meta] : part_bufs[t][p]) {
                    pmap.insert(okey, meta);
                }
            }
        }

        // Parallel bloom build: thread t inserts keys from part_maps[t].
        // Uses atomic fetch_or → thread-safe, lock-free.
        // Different partitions have different key sets → mostly different bloom cache lines.
        // Even if two partitions share a bloom line, fetch_or is correct.
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int p = 0; p < NUM_PARTS; p++) {
            for (auto [okey, meta] : part_maps[p]) {
                cl_bloom->insert(okey);
            }
        }
        // Memory fence: all atomic stores are visible before main_scan reads
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    delete cust_bloom;
    cust_bloom = nullptr;

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          probe CL-bloom (1 cache miss), then part_maps[part(okey)].
    //
    // Key change vs iter 8:
    //   - Probe part_maps[partition_of(okey)] instead of order_map
    //     → no sequential merge of order_map needed
    //   - Agg merge: partitioned parallel approach
    //     → per-thread scatter to agg_part_bufs, then parallel build
    //     → eliminates sequential O(1.5M) agg merge
    //
    // Agg partitioning uses same partition_of() function as order_map
    // (same hash → same partition for same key).
    // ----------------------------------------------------------------

    // Per-partition agg maps (built in parallel, iterated for final output)
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> part_agg(NUM_PARTS);
    for (int p = 0; p < NUM_PARTS; p++) {
        part_agg[p].reserve(32768);
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

        // Per-thread, per-partition agg emit buffers
        // For each qualifying row, emit (okey, rev, orderdate, shipprio)
        struct AggRaw {
            int32_t okey;
            int64_t rev;
            int32_t orderdate;
            int32_t shipprio;
        };
        std::vector<std::vector<std::vector<AggRaw>>> agg_bufs(
            nthreads, std::vector<std::vector<AggRaw>>(NUM_PARTS));

        for (int t = 0; t < nthreads; t++) {
            for (int p = 0; p < NUM_PARTS; p++) {
                agg_bufs[t][p].reserve(256);
            }
        }

        // Parallel lineitem scan
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();

            auto& my_agg_bufs = agg_bufs[tid];

            const int32_t* p_shipdate = l_shipdate.data + beg;
            const int32_t* p_orderkey = l_orderkey.data + beg;
            const int64_t* p_extprice = l_extprice.data + beg;
            const int64_t* p_discount = l_discount.data + beg;
            size_t block_len = end - beg;

            for (size_t i = 0; i < block_len; i++) {
                // Filter 1: shipdate (most selective)
                if (__builtin_expect(p_shipdate[i] <= DATE_CUTOFF, 1)) continue;

                int32_t okey = p_orderkey[i];

                // Filter 2: CL-resident bloom (1 cache miss)
                if (__builtin_expect(!cl_bloom->maybe_contains(okey), 0)) continue;

                // Filter 3: Probe correct partition map
                int part = partition_of(okey);
                const OrderMeta* om = part_maps[part].find(okey);
                if (__builtin_expect(om == nullptr, 0)) continue;

                int64_t rev = (p_extprice[i] * (100LL - p_discount[i])) / 100LL;

                // Scatter to agg partition buffer
                my_agg_bufs[part].push_back({okey, rev, om->o_orderdate, om->o_shippriority});
            }
        }

        // Parallel agg build: thread t builds part_agg[t] from all agg_bufs[*][t]
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int p = 0; p < NUM_PARTS; p++) {
            auto& pagg = part_agg[p];
            for (int t = 0; t < nthreads; t++) {
                for (auto& raw : agg_bufs[t][p]) {
                    AggEntry* ae = pagg.find(raw.okey);
                    if (ae) {
                        ae->revenue_raw += raw.rev;
                    } else {
                        pagg.insert(raw.okey, {raw.rev, raw.orderdate, raw.shipprio});
                    }
                }
            }
        }
    }

    delete cl_bloom;
    cl_bloom = nullptr;

    // ----------------------------------------------------------------
    // Phase 4: Sort Top-K
    // Collect from all part_agg maps directly (no global merge needed)
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

        // Collect all results from part_agg maps
        std::vector<ResultRow> all_rows;
        // Estimate total size
        size_t total_est = 0;
        for (int p = 0; p < NUM_PARTS; p++) total_est += part_agg[p].size();
        all_rows.reserve(total_est);

        for (int p = 0; p < NUM_PARTS; p++) {
            for (auto [okey, entry] : part_agg[p]) {
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
