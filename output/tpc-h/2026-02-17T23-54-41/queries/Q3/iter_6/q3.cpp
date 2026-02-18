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
 * ITERATION 6 OPTIMIZATIONS
 * ============================================================
 * Dominant bottleneck: main_scan 259ms (48%)
 *
 * Root cause: In the main_scan hot loop, every lineitem row that
 * passes the l_shipdate filter (roughly half of surviving blocks)
 * performs an order_map.find() lookup. The order_map has ~1.5M
 * entries = ~24MB, which does NOT fit in per-thread L3 cache on
 * a 64-core machine (44MB L3 shared). This causes frequent cache
 * misses. Only ~10-15% of lineitem rows that pass the shipdate
 * filter actually have a matching order (because orders table after
 * date+customer filter has ~1.5M out of 15M total orders, and
 * lineitem has ~4 rows per order → ~6M matching lineitem rows out
 * of ~30M surviving the shipdate filter).
 *
 * Fix: Bloom filter pre-screening before order_map.find().
 *
 * Bloom filter sized at 2MB (16M bits for ~1.5M keys → ~11 bits/key
 * → ~0.5% false positive rate). Fits comfortably in L3 cache and is
 * shared read-only across all threads → hot path for each thread.
 *
 * Expected savings: ~85% of non-matching probe rows skip the
 * expensive order_map hash table lookup entirely. order_map.find()
 * cost per miss: ~50-100ns (L3 cache miss). With ~24M non-matching
 * rows, bloom filter eliminates ~20M of those expensive probes.
 *
 * Additional optimization: prefetch order_map slot after bloom hit
 * to hide memory latency on the (now rarer) actual table probes.
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
 *      → BloomFilter over qualifying o_orderkeys (2MB, ~0.5% FP rate)
 *   3. Scan lineitem with zone-map block pruning; filter l_shipdate>cutoff
 *      → BloomFilter pre-screen (fast L3-resident check, eliminates ~85% of misses)
 *      → order_map probe only on bloom hits
 *      → parallel thread-local CompactHashMap<o_orderkey, AggEntry> (partial aggregation)
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
 *   - Parallel scan on surviving blocks → thread-local vectors
 *   - Sequential merge into CompactHashMap<o_orderkey, OrderMeta> (~1.5M)
 *   - Build BloomFilter over all inserted o_orderkeys (2MB, 3 hash functions)
 *
 * Phase 3: main_scan
 *   - Load lineitem columns (mmap) + zone map index
 *   - Zone-map pruning: skip blocks where max_shipdate <= DATE_CUTOFF
 *   - Parallel OMP scan on surviving blocks
 *   - Inner loop: l_shipdate filter → BloomFilter check → order_map.find()
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

// ----------------------------------------------------------------
// Bloom filter for semi-join pre-screening.
// Sized at 2MB (16M bits) for ~1.5M keys:
//   16M / 1.5M ≈ 10.7 bits/key → ~0.5% false positive rate
// Three independent hash functions derived from fibonacci hashing.
// Read-only after build → safely shared across threads with no sync.
// ----------------------------------------------------------------
struct BloomFilter {
    // 2MB = 2 * 1024 * 1024 bytes = 16,777,216 bits
    static constexpr size_t BYTES = 2 * 1024 * 1024;
    static constexpr size_t BITS  = BYTES * 8;
    static constexpr size_t MASK  = BITS - 1;

    alignas(64) uint8_t bits[BYTES];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    // Three hash functions using different fibonacci multipliers
    static inline uint64_t h1(int32_t key) {
        return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
    }
    static inline uint64_t h2(int32_t key) {
        return (uint64_t)(uint32_t)key * 0x517CC1B727220A95ULL;
    }
    static inline uint64_t h3(int32_t key) {
        // Derived by mixing h1 and h2
        uint64_t v = h1(key);
        v ^= (v >> 17);
        v *= 0xBF58476D1CE4E5B9ULL;
        return v;
    }

    void insert(int32_t key) {
        uint64_t a = h1(key) & MASK;
        uint64_t b = h2(key) & MASK;
        uint64_t c = h3(key) & MASK;
        bits[a >> 3] |= (uint8_t)(1u << (a & 7));
        bits[b >> 3] |= (uint8_t)(1u << (b & 7));
        bits[c >> 3] |= (uint8_t)(1u << (c & 7));
    }

    // Returns false only if the key is definitely NOT in the set.
    // May return true for keys not in the set (false positive ~0.5%).
    inline bool maybe_contains(int32_t key) const {
        uint64_t a = h1(key) & MASK;
        uint64_t b = h2(key) & MASK;
        uint64_t c = h3(key) & MASK;
        return (bits[a >> 3] & (uint8_t)(1u << (a & 7))) &&
               (bits[b >> 3] & (uint8_t)(1u << (b & 7))) &&
               (bits[c >> 3] & (uint8_t)(1u << (c & 7)));
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
                // Try alternate path
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
    //          Build Bloom filter over qualifying o_orderkeys
    //          Uses zone map pruning on o_orderdate (sorted column)
    // ----------------------------------------------------------------
    // Qualifying orders: ~1.5M (300K customers × ~5 orders each after date filter)
    // Right-sized to 2M (not 8M) → 4× less allocation
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(2000000);

    // Bloom filter: 2MB, ~0.5% FP rate for 1.5M keys.
    // Allocated on heap to avoid stack overflow (2MB is too large for stack).
    BloomFilter* bloom = new BloomFilter();

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

        // Try to load zone map for o_orderdate
        // Zone map: skip blocks where block_min >= DATE_CUTOFF (predicate: o_orderdate < DATE_CUTOFF)
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
                // skip block if block_min >= DATE_CUTOFF (all rows in block fail predicate)
                if (zm_orders.zones[z].min_val >= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local flat vectors for cache-friendly parallel collection
        std::vector<std::vector<std::pair<int32_t, OrderMeta>>> local_results(nthreads);

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();

            for (size_t i = beg; i < end; i++) {
                if (o_orderdate[i] < DATE_CUTOFF && cust_set.contains(o_custkey[i])) {
                    local_results[tid].push_back({
                        o_orderkey[i],
                        {o_orderdate[i], o_shipprio[i]}
                    });
                }
            }
        }

        // Sequential merge into order_map + build bloom filter simultaneously
        for (auto& lv : local_results) {
            for (auto& [key, meta] : lv) {
                order_map.insert(key, meta);
                bloom->insert(key);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem, filter l_shipdate > DATE_CUTOFF,
    //          probe bloom filter (fast, L3-resident), then order_map,
    //          aggregate revenue per l_orderkey
    //          Uses zone map pruning on l_shipdate (sorted column)
    //          Uses thread-local partial aggregation maps to avoid
    //          collecting/merging 30M raw tuples
    // ----------------------------------------------------------------
    // ~1.5M distinct orderkeys qualify → right-size agg_map to 2M
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

        // Try to load zone map for l_shipdate
        // Predicate: l_shipdate > DATE_CUTOFF
        // Skip block if block_max <= DATE_CUTOFF (all rows in block fail predicate)
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
                // skip block if block_max <= DATE_CUTOFF (all rows fail predicate)
                if (zm_lineitem.zones[z].max_val <= DATE_CUTOFF) continue;
                surviving_blocks.push_back(z);
            }
        } else {
            for (size_t b = 0; b < num_blocks; b++) surviving_blocks.push_back(b);
        }

        // Thread-local partial aggregation maps.
        // Each thread builds its own CompactHashMap<okey, AggEntry> during the hot loop.
        // This avoids collecting 30M raw tuples and large sequential merge.
        // With 64 threads and ~1.5M output groups, each thread sees ~25K unique keys on average
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_agg(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_agg[t].reserve(65536); // 64K → well above avg, avoids rehash
        }

        // Cache raw pointer to order_map table data for direct prefetch
        const auto* order_map_table = order_map.table.data();
        const size_t order_map_mask = order_map.mask;

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t bi = 0; bi < surviving_blocks.size(); bi++) {
            size_t b    = surviving_blocks[bi];
            size_t beg  = b * BLOCK_SIZE;
            size_t end  = std::min(beg + BLOCK_SIZE, n);
            int    tid  = omp_get_thread_num();

            auto& my_agg = local_agg[tid];

            // Get raw pointers for cache-friendly sequential access
            const int32_t* p_shipdate = l_shipdate.data + beg;
            const int32_t* p_orderkey = l_orderkey.data + beg;
            const int64_t* p_extprice = l_extprice.data + beg;
            const int64_t* p_discount = l_discount.data + beg;
            size_t block_len = end - beg;

            for (size_t i = 0; i < block_len; i++) {
                // Filter 1: shipdate predicate (most selective first)
                if (__builtin_expect(p_shipdate[i] <= DATE_CUTOFF, 1)) continue;

                int32_t okey = p_orderkey[i];

                // Filter 2: Bloom filter pre-screen before expensive hash table probe.
                // Bloom is 2MB, shared read-only. Eliminates ~85% of non-matching probes.
                if (__builtin_expect(!bloom->maybe_contains(okey), 0)) continue;

                // Prefetch order_map slot to hide cache-miss latency on the hash table probe.
                // We compute the initial hash slot index and prefetch it for reads.
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
        // Each map has ~25K unique keys → total merge is ~1.5M find/insert ops
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

    delete bloom;

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
