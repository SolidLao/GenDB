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
 * ITERATION 2 OPTIMIZATIONS
 * ============================================================
 *
 * Key changes from iter_1:
 *
 * 1. Zone-map pruning on orders (o_orderdate < 9204):
 *    orders is sorted by o_orderdate → many tail blocks entirely >= cutoff.
 *    Skip those blocks entirely using the orders_o_orderdate_zonemap.bin index.
 *    Expected: cut orders scan from 15M → ~7.5M rows (50% skip).
 *
 * 2. Zone-map pruning on lineitem (l_shipdate > 9204):
 *    lineitem is sorted by l_shipdate, l_orderkey → many head blocks entirely
 *    <= cutoff. Skip those using lineitem_l_shipdate_zonemap.bin.
 *    Expected: cut lineitem scan from 60M → ~30M rows (50% skip).
 *
 * 3. Thread-local agg maps + parallel merge (main_scan):
 *    Instead of collecting 15M tuples into thread-local vectors and then
 *    sequentially merging all into a single agg_map, each thread maintains
 *    its own CompactHashMap<int32_t, AggEntry> for partial aggregation.
 *    This reduces the sequential merge cost dramatically: each thread-local
 *    map has ~(1.5M / nthreads) entries already aggregated. Final merge
 *    iterates over ~1.5M groups total (not 15M tuples).
 *
 * 4. Thread-local order_map build (build_joins):
 *    Instead of collecting pairs then sequentially inserting ~1.5M entries,
 *    build thread-local CompactHashMaps for the order filtering pass and
 *    merge into a single global map. For 1.5M total qualifying orders /
 *    nthreads, each thread-local map is small → merge is fast.
 *
 * 5. Bloom filter on lineitem probe:
 *    After building order_map (~1.5M keys from ~15M orders), build a bloom
 *    filter over those keys. In the lineitem scan hot loop, bloom-check
 *    l_orderkey BEFORE the order_map hash lookup. Since lineitem has ~60M
 *    rows and order_map only has ~1.5M distinct keys (~10% of lineitem orderkeys
 *    match), the bloom filter rejects most non-matches at L2-cache speed.
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates & filtered cardinalities:
 *   customer: c_mktsegment = 'BUILDING' → ~300K rows (1/5 selectivity)
 *   orders:   o_orderdate < 9204 (1995-03-15) → ~7.5M rows (zone-map pruned)
 *   lineitem: l_shipdate > 9204 → ~30M rows (zone-map pruned)
 *
 * Step 2 — Join ordering (smallest filtered first):
 *   1. Filter customer → build CompactHashSet<c_custkey> (~300K keys)
 *   2. Scan orders (zone-map skip ~50% blocks), probe customer set;
 *      build CompactHashMap<o_orderkey, OrderMeta> → ~1.5M entries
 *   3. Build bloom filter over order_map keys
 *   4. Scan lineitem (zone-map skip ~50% blocks), filter l_shipdate > cutoff,
 *      bloom-check l_orderkey, probe order_map;
 *      thread-local partial agg maps → merge to global agg_map (~1.5M groups)
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Phase 1 (dim_filter):
 *   - Sequential scan customer (1.5M rows — fast)
 *   - Build CompactHashSet<int32_t> cust_set (~300K)
 *
 * Phase 2 (build_joins):
 *   - Load orders zone map → compute block skip mask
 *   - Parallel OMP scan over qualifying blocks only
 *   - Thread-local CompactHashMap<int32_t, OrderMeta> per thread
 *   - Sequential merge thread-local maps into global order_map
 *   - Build bloom filter from order_map keys
 *
 * Phase 3 (main_scan):
 *   - Load lineitem zone map → compute block skip mask
 *   - Parallel OMP scan over qualifying blocks only
 *   - Thread-local CompactHashMap<int32_t, AggEntry> for partial agg
 *   - Sequential merge thread-local agg maps into global agg_map
 *
 * Phase 4 (sort_topk):
 *   - std::partial_sort top-10 from agg_map entries
 *
 * Phase 5 (output): write CSV
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
// Bloom filter — 1MB, ~8M bits, for ~1.5M keys → ~0.3% false positives
// Fits in L3 cache (44MB available). 3-probe variant.
// ----------------------------------------------------------------
struct BloomFilter {
    static constexpr size_t NBYTES = 1 << 20; // 1MB = 8M bits
    static constexpr size_t MASK   = NBYTES * 8 - 1;
    uint8_t bits[NBYTES];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    inline void insert(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 21   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 42   ) & MASK;
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 21   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 42   ) & MASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

// ----------------------------------------------------------------
// Zone map entry (from Storage Guide):
// [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
// ----------------------------------------------------------------
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t count;
};

// Load zone map from file, return pointer and num_zones via mmap
// Returns raw mmap ptr (caller must munmap), sets num_zones.
struct ZoneMapData {
    const ZoneEntry* zones;
    uint32_t         num_zones;
    void*            mmap_ptr;
    size_t           mmap_size;
};

static ZoneMapData load_zonemap(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open zonemap: " + path);
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (ptr == MAP_FAILED) throw std::runtime_error("mmap failed for " + path);

    // Layout: [uint32_t num_zones][ZoneEntry * num_zones]
    uint32_t nz = *reinterpret_cast<const uint32_t*>(ptr);
    const ZoneEntry* zones = reinterpret_cast<const ZoneEntry*>(
        reinterpret_cast<const uint8_t*>(ptr) + sizeof(uint32_t));
    return {zones, nz, ptr, sz};
}

static void free_zonemap(ZoneMapData& zm) {
    if (zm.mmap_ptr) {
        munmap(zm.mmap_ptr, zm.mmap_size);
        zm.mmap_ptr = nullptr;
    }
}

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
            std::string dict_path = data_dir + "/customer/mktsegment_dict.txt";
            FILE* f = fopen(dict_path.c_str(), "r");
            if (!f) {
                // try alternate name
                dict_path = data_dir + "/customer/c_mktsegment_dict.txt";
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
    // Phase 2: Scan orders (zone-map pruned), filter by date + customer set.
    //          Build map: o_orderkey -> OrderMeta.
    //          Then build bloom filter over orderkeys.
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, OrderMeta> order_map(8000000);
    BloomFilter* bf = new BloomFilter(); // heap-allocate 1MB
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (data_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (data_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (data_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shipprio  (data_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        int nthreads = omp_get_max_threads();

        // --- Zone-map pruning for orders (o_orderdate < DATE_CUTOFF) ---
        // orders sorted by o_orderdate → blocks near the end have min > DATE_CUTOFF → skip
        // Try to load orders zone map; fall back to full scan if unavailable
        bool use_orders_zm = false;
        ZoneMapData orders_zm{};
        try {
            orders_zm = load_zonemap(index_dir + "/orders_o_orderdate_zonemap.bin");
            use_orders_zm = true;
        } catch (...) {
            use_orders_zm = false;
        }

        // Identify qualifying row ranges (block-level)
        // We collect (start, end) row intervals that pass zone-map skip
        std::vector<std::pair<size_t,size_t>> order_ranges;
        if (use_orders_zm) {
            size_t row_offset = 0;
            for (uint32_t z = 0; z < orders_zm.num_zones; z++) {
                uint32_t cnt = orders_zm.zones[z].count;
                int32_t  zmin = orders_zm.zones[z].min_val;
                // Predicate: o_orderdate < DATE_CUTOFF
                // Skip block if: zmin >= DATE_CUTOFF (all rows in block fail predicate)
                if (zmin < DATE_CUTOFF) {
                    order_ranges.emplace_back(row_offset, row_offset + cnt);
                }
                row_offset += cnt;
            }
            free_zonemap(orders_zm);
        } else {
            // Fall back: whole table
            order_ranges.emplace_back(0, n);
        }

        // Thread-local CompactHashMaps for order entries
        std::vector<gendb::CompactHashMap<int32_t, OrderMeta>> local_maps(nthreads,
            gendb::CompactHashMap<int32_t, OrderMeta>(200000)); // pre-size per thread

        // Parallel scan over qualifying blocks
        size_t num_ranges = order_ranges.size();

        #pragma omp parallel for schedule(dynamic,1) num_threads(nthreads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            size_t start = order_ranges[ri].first;
            size_t end   = order_ranges[ri].second;
            auto& lmap   = local_maps[tid];
            for (size_t i = start; i < end; i++) {
                int32_t odate = o_orderdate[i];
                if (odate < DATE_CUTOFF) {
                    int32_t ocust = o_custkey[i];
                    if (cust_set.contains(ocust)) {
                        lmap.insert(o_orderkey[i], {odate, o_shipprio[i]});
                    }
                }
            }
        }

        // Sequential merge thread-local order maps into global order_map
        // Total entries ~1.5M across all threads
        for (auto& lmap : local_maps) {
            for (auto [key, meta] : lmap) {
                order_map.insert(key, meta);
            }
        }

        // Build bloom filter over order_map keys
        for (auto [key, meta] : order_map) {
            bf->insert(key);
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Scan lineitem (zone-map pruned), filter l_shipdate > DATE_CUTOFF,
    //          bloom + order_map probe, thread-local partial aggregation.
    // ----------------------------------------------------------------
    gendb::CompactHashMap<int32_t, AggEntry> agg_map(4000000);
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey (data_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice (data_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount (data_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate (data_dir + "/lineitem/l_shipdate.bin");

        size_t n = l_orderkey.size();
        int nthreads = omp_get_max_threads();

        // --- Zone-map pruning for lineitem (l_shipdate > DATE_CUTOFF) ---
        // lineitem sorted by l_shipdate, l_orderkey → head blocks have max <= DATE_CUTOFF → skip
        bool use_line_zm = false;
        ZoneMapData line_zm{};
        try {
            line_zm = load_zonemap(index_dir + "/lineitem_l_shipdate_zonemap.bin");
            use_line_zm = true;
        } catch (...) {
            use_line_zm = false;
        }

        std::vector<std::pair<size_t,size_t>> line_ranges;
        if (use_line_zm) {
            size_t row_offset = 0;
            for (uint32_t z = 0; z < line_zm.num_zones; z++) {
                uint32_t cnt  = line_zm.zones[z].count;
                int32_t  zmax = line_zm.zones[z].max_val;
                // Predicate: l_shipdate > DATE_CUTOFF
                // Skip block if: zmax <= DATE_CUTOFF (all rows fail predicate)
                if (zmax > DATE_CUTOFF) {
                    line_ranges.emplace_back(row_offset, row_offset + cnt);
                }
                row_offset += cnt;
            }
            free_zonemap(line_zm);
        } else {
            line_ranges.emplace_back(0, n);
        }

        size_t num_ranges = line_ranges.size();

        // Thread-local partial aggregation maps
        // Each thread owns a CompactHashMap → no synchronization in hot loop
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_aggs(nthreads,
            gendb::CompactHashMap<int32_t, AggEntry>(200000));

        #pragma omp parallel for schedule(dynamic,1) num_threads(nthreads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid  = omp_get_thread_num();
            size_t start = line_ranges[ri].first;
            size_t end   = line_ranges[ri].second;
            auto& lagg   = local_aggs[tid];

            for (size_t i = start; i < end; i++) {
                if (l_shipdate[i] > DATE_CUTOFF) {
                    int32_t okey = l_orderkey[i];
                    // Bloom filter check before expensive hash lookup
                    if (!bf->maybe_contains(okey)) continue;
                    const OrderMeta* om = order_map.find(okey);
                    if (om) {
                        int64_t rev = (l_extprice[i] * (100LL - l_discount[i])) / 100LL;
                        // Partial agg: find-or-insert into thread-local map
                        AggEntry* ae = lagg.find(okey);
                        if (ae) {
                            ae->revenue_raw += rev;
                        } else {
                            lagg.insert(okey, {rev, om->o_orderdate, om->o_shippriority});
                        }
                    }
                }
            }
        }

        // Sequential merge of thread-local agg maps into global agg_map
        // Each local map has ~(1.5M / nthreads) entries → total iteration = 1.5M
        for (auto& lagg : local_aggs) {
            for (auto [okey, ae] : lagg) {
                AggEntry* gae = agg_map.find(okey);
                if (gae) {
                    gae->revenue_raw += ae.revenue_raw;
                } else {
                    agg_map.insert(okey, ae);
                }
            }
        }
    }

    delete bf;

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
