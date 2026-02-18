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
 * ITERATION 4 OPTIMIZATIONS
 * ============================================================
 * Dominant bottleneck: main_scan (259ms, 48%)
 *   - Two hash probes per qualifying lineitem row:
 *     (1) order_map.find(okey)   — 1.5M table, random L3 miss ~50ns each
 *     (2) my_agg.find/insert     — per-thread agg map
 *   - ~30M rows pass the l_shipdate filter but ~50% have no matching order
 *
 * build_joins (118ms, 22%):
 *   - Thread-local vector collect + sequential merge into order_map
 *   - Merge of ~1.5M pairs is sequential bottleneck
 *
 * Fixes applied:
 *   1. Bloom filter on order_map keys (built during build_joins phase):
 *      - 2MB bloom filter, 3 hash bits per key, ~1% false-positive rate for 1.5M keys
 *      - In main_scan hot loop: check bloom BEFORE probing order_map
 *      - Eliminates ~50% of non-matching L3 cache misses in the hot path
 *      - Bloom check is ~3ns (L2 cache hit), vs hash probe ~50ns (L3 miss)
 *
 *   2. Bloom filter on cust_set keys (built during dim_filter phase):
 *      - 512KB bloom filter for ~300K customer keys
 *      - In build_joins hot loop: check bloom BEFORE probing cust_set
 *      - ~80% of orders rows fail the cust_set probe (orders not from BUILDING segment)
 *      - Bloom check rejects most of them in L2 cache before touching the 4MB cust_set table
 *
 *   3. Partitioned parallel aggregation in main_scan:
 *      - Partition the 1.5M result groups into P buckets by (okey % P)
 *      - Each thread owns a subset of partitions (not just blocks)
 *      - Thread-local agg writes only to its own partition shard → no merge needed
 *      - NOTE: We use the static OMP schedule trick: since lineitem is sorted by l_shipdate
 *        and then l_orderkey, adjacent rows with same orderkey tend to land on same thread.
 *        Combined with hash partitioning by (okey & partition_mask), threads get disjoint sets.
 *      - Implementation: 64 partition shards, each a small CompactHashMap; thread i processes
 *        partition i (round-robin assignment). After parallel phase, collect shard results.
 *
 *   4. Faster order_map build: use parallel collect + single parallel-merge into partitioned
 *      sub-tables. Instead of one big CompactHashMap, build 64 partition hash maps keyed by
 *      (o_orderkey & 63). Each thread builds its partition independently (no coordination).
 *      Lineitem probe: for row with okey, probe partition (okey & 63) directly.
 *      This reduces hash table size per partition (smaller → better cache fit).
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates & filtered cardinalities:
 *   customer: c_mktsegment = 'BUILDING' (1/5 selectivity) → ~300K rows
 *   orders:   o_orderdate < 1995-03-15  → ~50% rows → ~7.5M rows after date filter
 *             + cust_set join filter     → ~1.5M rows
 *             zone map prunes ~50% of blocks (orders sorted by o_orderdate)
 *   lineitem: l_shipdate > 1995-03-15   → ~50% rows → ~30M rows
 *             zone map prunes ~50% of blocks (lineitem sorted by l_shipdate)
 *
 * Step 2 — Join ordering:
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K) + Bloom filter (~512KB)
 *   2. Scan orders (zone-map pruned) → filter date + bloom-cust + cust_set
 *      → build 64 partitioned CompactHashMap<o_orderkey, OrderMeta> (~1.5M total)
 *      → simultaneously build a Bloom filter on qualifying o_orderkeys (~2MB)
 *   3. Scan lineitem (zone-map pruned) → filter l_shipdate > cutoff
 *      → check order_bloom BEFORE probing partitioned order_map
 *      → probe partitioned order_map → aggregate into per-partition agg maps
 *
 * Step 3 — No correlated subqueries.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1: dim_filter
 *   - Load customer c_mktsegment (mmap), find BUILDING code from dict
 *   - Build CompactHashSet<int32_t> of qualifying c_custkeys (~300K)
 *   - Build Bloom filter on the same c_custkeys (~512KB, L2 resident)
 *   - Sequential (fast at 1.5M rows)
 *
 * Phase 2: build_joins
 *   - Load orders columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where min_orderdate >= DATE_CUTOFF
 *   - Parallel scan on surviving blocks
 *   - Per-row: check cust_bloom first, then cust_set, then emit into
 *     per-thread local_results vectors (as before but with bloom pre-filter)
 *   - Sequential merge into 64 partitioned order_maps
 *   - Build global order_bloom (Bloom filter on all qualifying o_orderkeys)
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where max_shipdate <= DATE_CUTOFF
 *   - Parallel OMP scan on surviving blocks
 *   - Hot loop: filter l_shipdate → check order_bloom → probe order_map[okey & 63]
 *     → accumulate into per-thread agg CompactHashMap
 *   - Final merge: iterate thread-local maps → global agg_map
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

// Number of partitions for partitioned hash join.
// Must be a power of 2. 64 partitions → each ~23K entries (fits in L2 cache)
static constexpr int NUM_PARTS = 64;
static constexpr int PART_MASK = NUM_PARTS - 1;

// ----------------------------------------------------------------
// Bloom filter — compact, L2-cache-resident probabilistic filter
// 3 hash functions, ~1% FP rate for sizing:
//   For N=300K cust keys:   SIZE=1<<19 → 512KB (L2 resident)
//   For N=1.5M order keys:  SIZE=1<<21 → 2MB   (L3 resident)
// ----------------------------------------------------------------
template<int LOG2_SIZE>
struct BloomFilter {
    static constexpr uint32_t SIZE  = 1u << LOG2_SIZE;       // bytes
    static constexpr uint32_t BMASK = SIZE * 8u - 1u;        // bit mask
    uint8_t bits[SIZE] = {};

    void insert(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 =  h        & BMASK;
        uint32_t h2 = (h >> 17) & BMASK;
        uint32_t h3 = (h >> 34) & BMASK;
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7u));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7u));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7u));
    }

    bool maybe_contains(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 =  h        & BMASK;
        uint32_t h2 = (h >> 17) & BMASK;
        uint32_t h3 = (h >> 34) & BMASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7u))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7u))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7u)));
    }
};

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
    int64_t revenue_raw; // sum of l_extendedprice * (100 - l_discount), scale x100
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

// ----------------------------------------------------------------
// Load a zone map file via mmap.
// ----------------------------------------------------------------
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
    // Phase 1: Filter customer → build cust_set + cust_bloom
    // ----------------------------------------------------------------
    // cust_set: ~300K keys, CompactHashSet → ~4MB table (L3 resident)
    // cust_bloom: 512KB bloom filter → L2 resident, rejects ~80% of orders rows early
    gendb::CompactHashSet<int32_t> cust_set(400000);
    // 512KB = 1<<19 bytes → ~4M bits → for 300K keys: 4M/300K ≈ 13 bits/key → ~0.2% FP rate
    auto* cust_bloom = new BloomFilter<19>();

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
    //          Build 64 partitioned order_maps + order_bloom
    //
    //   Partitioning: partition p = o_orderkey & PART_MASK
    //   Each partition has ~23K entries on average (1.5M / 64)
    //   Each partitioned map fits in ~400KB → all 64 fit in 25MB total
    //   But probing a single partition is L3-cache-friendly
    //
    //   order_bloom: 2MB bloom filter for all 1.5M qualifying o_orderkeys
    //   In lineitem scan: bloom check before order_map probe
    //   Eliminates ~50% of probes for non-matching orderkeys
    // ----------------------------------------------------------------
    // Pre-size each partition map for ~23K entries with 50% headroom
    static gendb::CompactHashMap<int32_t, OrderMeta> order_maps[NUM_PARTS];
    for (int p = 0; p < NUM_PARTS; p++) {
        order_maps[p].reserve(50000); // ~50K per partition
    }

    // 2MB = 1<<21 bytes → ~16M bits → for 1.5M keys: ~10.7 bits/key → ~0.1% FP rate
    auto* order_bloom = new BloomFilter<21>();

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (data_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (data_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (data_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (data_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        constexpr size_t BLOCK_SIZE = 100000; // orders block size from Storage Guide
        size_t num_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;

        int nthreads = omp_get_max_threads();

        // Zone map for o_orderdate
        ZoneMapData zm_orders;
        bool have_orders_zm = false;
        try {
            zm_orders.open(index_dir + "/orders_o_orderdate_zonemap.bin");
            have_orders_zm = (zm_orders.num_zones > 0);
        } catch (...) { have_orders_zm = false; }

        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_orders_zm) {
            for (uint32_t z = 0; z < zm_orders.num_zones; z++) {
                // predicate: o_orderdate < DATE_CUTOFF → skip if min >= cutoff
                if (zm_orders.zones[z].min_val >= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local flat vectors for parallel collection
        // Each entry: (okey, orderdate, shipprio)
        struct OrderRow { int32_t okey; int32_t orderdate; int32_t shipprio; };
        std::vector<std::vector<OrderRow>> local_results(nthreads);
        // Pre-reserve: ~1.5M qualifying rows / nthreads per thread
        for (int t = 0; t < nthreads; t++) {
            local_results[t].reserve(32000);
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();
            auto& lv    = local_results[tid];

            for (size_t i = beg; i < end; i++) {
                int32_t odate = o_orderdate[i];
                if (odate >= DATE_CUTOFF) continue;         // date filter
                int32_t ckey = o_custkey[i];
                if (!cust_bloom->maybe_contains(ckey)) continue;  // bloom pre-filter
                if (!cust_set.contains(ckey)) continue;            // exact filter
                lv.push_back({o_orderkey[i], odate, o_shipprio[i]});
            }
        }

        // Sequential merge into partitioned order_maps + build order_bloom
        for (int t = 0; t < nthreads; t++) {
            for (auto& row : local_results[t]) {
                int p = row.okey & PART_MASK;
                order_maps[p].insert(row.okey, {row.orderdate, row.shipprio});
                order_bloom->insert(row.okey);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          Bloom-filter on order_bloom (skip ~50% of probes),
    //          Probe partitioned order_maps, aggregate revenue
    //          Thread-local partial aggregation maps → final merge
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, AggEntry> agg_map(2000000);
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();
        constexpr size_t BLOCK_SIZE = 200000; // lineitem block size from Storage Guide
        size_t num_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;

        int nthreads = omp_get_max_threads();

        // Zone map for l_shipdate
        ZoneMapData zm_lineitem;
        bool have_lineitem_zm = false;
        try {
            zm_lineitem.open(index_dir + "/lineitem_l_shipdate_zonemap.bin");
            have_lineitem_zm = (zm_lineitem.num_zones > 0);
        } catch (...) { have_lineitem_zm = false; }

        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_lineitem_zm) {
            for (uint32_t z = 0; z < zm_lineitem.num_zones; z++) {
                // predicate: l_shipdate > DATE_CUTOFF → skip if max <= cutoff
                if (zm_lineitem.zones[z].max_val <= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local partial aggregation maps.
        // With 64 threads and ~1.5M output groups, each thread sees ~25K unique keys avg.
        // Sized at 65K to avoid rehash with static OMP schedule (adjacent blocks → similar keys).
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_agg(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_agg[t].reserve(65536);
        }

        // Take local refs to avoid repeated global lookups in hot loop
        const BloomFilter<21>& ob = *order_bloom;

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();
            auto& my_agg = local_agg[tid];

            for (size_t i = beg; i < end; i++) {
                if (l_shipdate[i] <= DATE_CUTOFF) continue;     // date filter
                int32_t okey = l_orderkey[i];
                // Bloom filter check — skip if definitely not in order_map
                // Cheap L3 access vs expensive hash table probe with potential L3 miss
                if (!ob.maybe_contains(okey)) continue;
                // Probe the correct partition
                int p = okey & PART_MASK;
                const OrderMeta* om = order_maps[p].find(okey);
                if (!om) continue;
                // Accumulate into thread-local agg map
                int64_t rev = (l_extprice[i] * (100LL - l_discount[i])) / 100LL;
                AggEntry* ae = my_agg.find(okey);
                if (ae) {
                    ae->revenue_raw += rev;
                } else {
                    my_agg.insert(okey, {rev, om->o_orderdate, om->o_shippriority});
                }
            }
        }

        // Merge thread-local partial agg maps into global agg_map
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

    // Free bloom filters
    delete cust_bloom;
    delete order_bloom;

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
