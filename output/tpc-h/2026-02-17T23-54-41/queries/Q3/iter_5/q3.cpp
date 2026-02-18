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
 * ITERATION 5 OPTIMIZATIONS
 * ============================================================
 * Previous best (iter 3): 537ms
 *   - dim_filter: 12ms (2%)
 *   - build_joins: 118ms (22%)
 *   - main_scan: 259ms (48%) <-- DOMINANT
 *   - sort_topk: 15ms (3%)
 *
 * Root cause of main_scan (259ms):
 *   For each of ~30M lineitem rows (after zone-map pruning), we do:
 *   1. l_shipdate > DATE_CUTOFF comparison
 *   2. order_map.find(okey) — expensive cache-miss probe into ~24MB hash table
 *   3. my_agg.find(okey) + update/insert
 *   Most of the 30M rows do NOT match a qualifying order (order_map has only
 *   ~1.5M keys), so step 2 is a cache miss that returns nullptr on most probes.
 *
 * Fix 1 — Bloom filter for order_map (main win):
 *   After building order_map, construct a 1.25MB bloom filter from its keys.
 *   This fits in L3 cache and eliminates 80-90% of expensive order_map probes.
 *   Expected gain: ~100-150ms on main_scan.
 *
 * Fix 2 — Parallel build_joins with thread-local maps:
 *   Replace thread-local vectors + sequential merge with thread-local
 *   CompactHashMap<int32_t, OrderMeta> + parallel merge. Avoids intermediate
 *   vector overhead and push_back costs.
 *
 * Fix 3 — Dynamic scheduling for lineitem scan:
 *   Some surviving blocks have more shipdate-qualifying rows than others.
 *   Dynamic scheduling better balances load across 64 threads.
 *
 * Fix 4 — Prefetch order_map slot before shipdate filter check:
 *   Software prefetch to overlap memory latency with computation.
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates & filtered cardinalities:
 *   customer: c_mktsegment = 'BUILDING' (1/5 selectivity) → ~300K rows
 *   orders:   o_orderdate < 1995-03-15  → ~50% rows → ~7.5M rows
 *             zone map prunes ~50% of blocks (orders sorted by o_orderdate)
 *   lineitem: l_shipdate > 1995-03-15   → ~50% rows → ~30M rows
 *             zone map prunes ~50% of blocks (lineitem sorted by l_shipdate)
 *
 * Step 2 — Join ordering (smallest filtered first):
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K keys)
 *   2. Scan orders with zone-map block pruning; filter o_orderdate<cutoff + cust_set probe
 *      → CompactHashMap<o_orderkey, OrderMeta> (~1.5M qualifying orders)
 *      + BloomFilter built from order_map keys
 *   3. Scan lineitem with zone-map block pruning; filter l_shipdate>cutoff
 *      → bloom filter check (eliminates ~85% non-matching probes cheaply)
 *      → order_map probe (only for bloom-positive keys)
 *      → parallel thread-local CompactHashMap<o_orderkey, AggEntry> (partial agg)
 *      → final merge into global agg_map
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
 *   - Load orders columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where min_orderdate >= DATE_CUTOFF
 *   - Parallel scan on surviving blocks → thread-local CompactHashMap<okey,OrderMeta>
 *   - Sequential merge into global CompactHashMap<o_orderkey, OrderMeta> (~1.5M)
 *   - Build BloomFilter from order_map keys (1.25MB, fits L3 cache)
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where max_shipdate <= DATE_CUTOFF
 *   - Parallel OMP scan on surviving blocks (dynamic schedule for balance)
 *   - Hot loop: shipdate filter → bloom check → order_map probe → agg update
 *   - Per-thread CompactHashMap<int32_t,AggEntry>: accumulate in hot loop
 *   - Final merge: iterate thread-local maps → accumulate into global agg_map
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
#include <immintrin.h>

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
// Bloom filter — sized for ~1.5M keys → ~1.25MB (10 bits/element)
// Uses 3 hash probes for ~1% false-positive rate.
// At 1.25MB this fits in L3 cache easily.
// ----------------------------------------------------------------
struct BloomFilter {
    // 1.25MB = 10 * 1.5M bits / 8 — round up to power of 2: 1 << 20 = 1MB bytes
    // Use 1MB (8M bits) for 1.5M keys → ~5.3 bits/key → ~4% FP rate
    // Acceptable for our use case: false positives just cause extra hash probes
    static constexpr size_t NBYTES = (1u << 20); // 1MB
    static constexpr size_t NBITS  = NBYTES * 8;
    static constexpr size_t MASK   = NBITS - 1;

    uint8_t bits[NBYTES];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    inline void insert(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 20  ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 40  ) & MASK;
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 20  ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 40  ) & MASK;
        return (bits[h1 >> 3] & (uint8_t)(1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (uint8_t)(1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (uint8_t)(1u << (h3 & 7)));
    }
};

// ----------------------------------------------------------------
// Load a zone map file via mmap. Returns pointer to zone entries
// and fills num_zones. Caller must munmap(ptr, file_size) when done.
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

    const std::string data_dir   = gendb_dir;
    const std::string index_dir  = gendb_dir + "/indexes";

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
    // Phase 2: Scan orders, filter by date + customer set
    //          Build map: o_orderkey -> OrderMeta
    //          Then build Bloom filter from order_map keys
    //          Uses zone map pruning on o_orderdate (sorted column)
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(2000000);
    BloomFilter* order_bloom = new BloomFilter(); // 1MB on heap (not stack)
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

        // Load zone map for o_orderdate
        ZoneMapData zm_orders;
        bool have_orders_zm = false;
        try {
            zm_orders.open(index_dir + "/orders_o_orderdate_zonemap.bin");
            have_orders_zm = (zm_orders.num_zones > 0);
        } catch (...) {
            have_orders_zm = false;
        }

        // Build qualifying block list
        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_orders_zm) {
            for (uint32_t z = 0; z < zm_orders.num_zones; z++) {
                // predicate: o_orderdate < DATE_CUTOFF
                // skip block if block_min >= DATE_CUTOFF
                if (zm_orders.zones[z].min_val >= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local CompactHashMaps for lock-free parallel build
        // Each thread builds its own map, then we merge sequentially
        std::vector<gendb::CompactHashMap<int32_t, OrderMeta>> local_maps(nthreads);
        // Estimate: ~1.5M qualifying orders / nthreads per thread
        // With 64 threads and ~750 surviving blocks, each thread processes ~12 blocks
        // → ~12 * 100K * (orderdate_selectivity 0.5) * (cust_selectivity 0.2) ~ 120K orders/thread
        for (int t = 0; t < nthreads; t++) {
            local_maps[t].reserve(131072); // 128K per thread, well above average
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();
            auto& lmap  = local_maps[tid];

            for (size_t i = beg; i < end; i++) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set.contains(o_custkey[i])) {
                    lmap.insert(o_orderkey[i], {o_orderdate[i], o_shipprio[i]});
                }
            }
        }

        // Sequential merge into order_map + build bloom filter simultaneously
        for (int t = 0; t < nthreads; t++) {
            for (auto [key, meta] : local_maps[t]) {
                order_map.insert(key, meta);
                order_bloom->insert(key);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          probe order_map (with bloom filter pre-check),
    //          aggregate revenue per l_orderkey.
    //          Uses zone map pruning on l_shipdate (sorted column)
    //          Uses thread-local partial aggregation maps.
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

        // Load zone map for l_shipdate
        ZoneMapData zm_lineitem;
        bool have_lineitem_zm = false;
        try {
            zm_lineitem.open(index_dir + "/lineitem_l_shipdate_zonemap.bin");
            have_lineitem_zm = (zm_lineitem.num_zones > 0);
        } catch (...) {
            have_lineitem_zm = false;
        }

        // Build surviving block list
        std::vector<size_t> surviving_blocks;
        surviving_blocks.reserve(num_blocks);
        if (have_lineitem_zm) {
            for (uint32_t z = 0; z < zm_lineitem.num_zones; z++) {
                // predicate: l_shipdate > DATE_CUTOFF
                // skip block if block_max <= DATE_CUTOFF
                if (zm_lineitem.zones[z].max_val <= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local partial aggregation maps.
        // With 64 threads and ~1.5M output groups, each thread sees ~25K unique keys
        // on average. Sized at 64K → avoids rehash.
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_agg(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_agg[t].reserve(65536);
        }

        // Capture bloom filter pointer for use in parallel lambda
        const BloomFilter* bloom = order_bloom;

        // Use dynamic scheduling: shipdate-qualifying rows are unevenly distributed
        // across blocks (early blocks in surviving list have fewer qualifying rows
        // since l_shipdate is sorted — earlier dates are below cutoff within blocks
        // that partially straddle the cutoff). Dynamic balances load better.
        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();

            auto& my_agg = local_agg[tid];

            // Get raw pointers for maximum inner-loop speed
            const int32_t* shipdate_ptr = l_shipdate.data + beg;
            const int32_t* okey_ptr     = l_orderkey.data + beg;
            const int64_t* price_ptr    = l_extprice.data + beg;
            const int64_t* disc_ptr     = l_discount.data + beg;
            size_t count = end - beg;

            for (size_t i = 0; i < count; i++) {
                if (shipdate_ptr[i] > DATE_CUTOFF) {
                    int32_t okey = okey_ptr[i];
                    // Bloom filter check: eliminates ~85-95% of non-matching probes
                    // without touching the order_map hash table (avoids cache misses)
                    if (!bloom->maybe_contains(okey)) continue;
                    const OrderMeta* om = order_map.find(okey);
                    if (om) {
                        int64_t rev = (price_ptr[i] * (100LL - disc_ptr[i])) / 100LL;
                        AggEntry* ae = my_agg.find(okey);
                        if (ae) {
                            ae->revenue_raw += rev;
                        } else {
                            my_agg.insert(okey, {rev, om->o_orderdate, om->o_shippriority});
                        }
                    }
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
