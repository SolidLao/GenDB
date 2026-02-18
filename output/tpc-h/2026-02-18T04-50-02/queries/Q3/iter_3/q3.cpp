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
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15  → ~54% of 15M → ~8.1M rows
 *               + semi-join on cust_building bitset → ~1.6M qualifying
 *   lineitem  : l_shipdate > 1995-03-15   → ~38% of 60M → ~22M rows (zone-map pruning)
 *
 * Step 2 — Join ordering:
 *   1. Scan customer → build 1-based bitset for BUILDING custkeys
 *   2. Parallel scan orders → qualify on date + bitset → partitioned into P=64 buckets
 *      (hash o_orderkey into bucket, build local OrderInfo maps per bucket)
 *   3. Lineitem parallel scan (zone-map, morsel-driven):
 *      - Per-row: shipdate filter, then Bloom filter, then probe orders bucket map
 *      - Accumulate revenue into per-thread per-partition aggregation arrays
 *   4. Parallel merge by partition: each partition's thread-local agg maps merged in parallel
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer  : sequential scan, filter mktsegment==BUILDING → flat bitset[0..1500000]
 * orders    : parallel scan (64 threads), filter date+bitset → partitioned CompactHashMap array
 *             (NPARTS=64 buckets keyed by orderkey%NPARTS, each owned exclusively per partition)
 * lineitem  : zone-map block pruning, parallel morsel scan
 *             → per-thread per-partition AggEntry accumulation
 *             → parallel final merge (each partition merged by dedicated thread)
 * Bloom filter on qualifying orders orderkeys → skip ~50%+ non-matching lineitem rows cheaply
 * sort/topk : partial_sort top-10
 * output    : Q3.csv
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   NPARTS = 64 (partition count for parallel merge, power of 2 for cheap modulo)
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <stdexcept>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"
#include <iostream>

// ============================================================
// Aggregation structures
// ============================================================
struct OrderInfo {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggEntry {
    int64_t revenue_scaled;
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
// Zone map layout (24-byte entries verified by inspection)
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
// Bloom filter: 256KB, targets ~2M keys → ~1% false positive rate
// ============================================================
struct BloomFilter {
    static constexpr size_t NBYTES = 1 << 18; // 256KB
    static constexpr size_t MASK   = NBYTES * 8 - 1;
    uint8_t bits[NBYTES];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    inline void insert(int32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        & MASK);
        uint32_t h2 = (uint32_t)((h >> 19) & MASK);
        uint32_t h3 = (uint32_t)((h >> 38) & MASK);
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(int32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        & MASK);
        uint32_t h2 = (uint32_t)((h >> 19) & MASK);
        uint32_t h3 = (uint32_t)((h >> 38) & MASK);
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");

    // Load c_mktsegment dictionary at runtime and find code for 'BUILDING'
    int32_t BUILDING_CODE = -1;
    {
        std::ifstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt");
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open c_mktsegment_dict.txt");
        std::string line;
        int32_t code = 0;
        while (std::getline(dict_file, line)) {
            if (line == "BUILDING") { BUILDING_CODE = code; break; }
            ++code;
        }
        if (BUILDING_CODE == -1)
            throw std::runtime_error("'BUILDING' not found in c_mktsegment_dict.txt");
    }

    // Number of partitions for parallel aggregation merge
    // Power of 2 so we can use bit-masking
    static constexpr int NPARTS = 64;

    // ============================================================
    // Phase 1: customer → bitset of BUILDING custkeys
    // ============================================================
    std::vector<bool> cust_building;
    int32_t max_ckey = 0;
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        // max custkey = 1500000 (known from storage guide)
        max_ckey = 1500000;
        cust_building.assign(max_ckey + 1, false);

        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = true;
            }
        }
    }

    // ============================================================
    // Phase 2: Parallel orders scan → NPARTS partitioned hash maps
    //          + Bloom filter over all qualifying orderkeys
    //
    // Strategy: divide orders rows into thread-local segments, each thread
    // builds its own per-partition mini-maps, then we merge per-partition
    // across threads (parallel: each partition merged by its index % nthreads).
    //
    // orders has 15M rows; ~54% pass date filter → ~8.1M; ~20% of those
    // have BUILDING custkey → ~1.6M qualifying orders total.
    // ============================================================
    int num_threads = omp_get_max_threads();

    // NPARTS partitioned orders maps (one map per partition, built sequentially per partition
    // after parallel collection phase to avoid contention)
    // We use thread-local vectors to collect (orderkey, orderdate, shippriority) per partition,
    // then build CompactHashMap per partition in parallel.

    // Per-thread, per-partition: collect qualifying rows
    // Layout: thread_rows[tid][part] = vector of (ok, od, sp)
    struct ORow { int32_t ok, od, sp; };

    std::vector<std::vector<std::vector<ORow>>> thread_rows(
        num_threads, std::vector<std::vector<ORow>>(NPARTS));

    BloomFilter bloom;
    // We build the bloom filter after collecting, so we need a temp store first
    // Actually build after we know all qualifying orderkeys

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        // Prefetch all orders columns (HDD: overlap I/O with setup)
        o_orderkey.prefetch();
        o_custkey.prefetch();
        o_orderdate.prefetch();
        o_shippriority.prefetch();

        const size_t n = o_orderkey.size();

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto& my_rows = thread_rows[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n; i++) {
                if (o_orderdate[i] >= DATE_THRESHOLD) continue;
                int32_t ck = o_custkey[i];
                if (ck < 0 || ck > max_ckey || !cust_building[ck]) continue;
                int32_t ok = o_orderkey[i];
                int part = (int)((uint32_t)((uint64_t)ok * 0x9E3779B97F4A7C15ULL >> 58)); // top 6 bits → 64 parts
                my_rows[part].push_back({ok, o_orderdate[i], o_shippriority[i]});
            }
        }
    }

    // Build NPARTS CompactHashMaps in parallel, and populate bloom filter
    // Estimate total qualifying: ~1.6M → ~25K per partition on avg
    std::vector<gendb::CompactHashMap<int32_t, OrderInfo>> orders_parts(NPARTS);

    {
        // Parallel build: each partition is independent
        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int part = 0; part < NPARTS; part++) {
            // Count total rows for this partition across all threads
            size_t total = 0;
            for (int t = 0; t < num_threads; t++) {
                total += thread_rows[t][part].size();
            }
            orders_parts[part] = gendb::CompactHashMap<int32_t, OrderInfo>(std::max(total + 1, (size_t)16));
            for (int t = 0; t < num_threads; t++) {
                for (auto& r : thread_rows[t][part]) {
                    orders_parts[part].insert(r.ok, {r.od, r.sp});
                }
            }
        }

        // Build bloom filter (single-threaded, iterating all parts)
        for (int part = 0; part < NPARTS; part++) {
            for (auto [key, val] : orders_parts[part]) {
                bloom.insert(key);
            }
        }
    }

    // Free thread_rows memory early
    { decltype(thread_rows) tmp; tmp.swap(thread_rows); }

    // ============================================================
    // Phase 3: Lineitem scan — zone-map pruning, parallel morsel scan
    //          probe: bloom filter → per-partition orders map → aggregate
    //
    // Aggregation: per-thread, per-partition CompactHashMap<int32_t, AggEntry>
    // Merge: parallel by partition (each partition merged by dedicated work)
    // ============================================================

    // Thread-local per-partition agg maps
    // Layout: local_agg[tid][part] = CompactHashMap<orderkey, AggEntry>
    // We'll allocate lazily to avoid huge upfront memory cost
    // ~2M qualifying lineitem rows / NPARTS = ~31K per partition on avg
    std::vector<std::vector<gendb::CompactHashMap<int32_t, AggEntry>>> local_agg(
        num_threads, std::vector<gendb::CompactHashMap<int32_t, AggEntry>>(NPARTS));

    // Initialize each partition map with estimated capacity
    // orders_parts[part].size() gives the build-side count for that partition
    for (int t = 0; t < num_threads; t++) {
        for (int part = 0; part < NPARTS; part++) {
            size_t cap = std::max(orders_parts[part].size() / 2 + 1, (size_t)16);
            local_agg[t][part] = gendb::CompactHashMap<int32_t, AggEntry>(cap);
        }
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey    (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount     (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate     (gendb_dir + "/lineitem/l_shipdate.bin");

        // Prefetch all lineitem columns
        l_orderkey.prefetch();
        l_extendedprice.prefetch();
        l_discount.prefetch();
        l_shipdate.prefetch();

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

        // Build qualifying block ranges
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            scan_ranges.push_back({zones[b].row_start, zones[b].row_start + zones[b].row_count});
        }
        munmap((void*)zm_raw, zm_st.st_size);

        size_t num_ranges = scan_ranges.size();

        // Cache raw data pointers to avoid operator[] overhead in hot loop
        const int32_t* ok_ptr  = l_orderkey.data;
        const int64_t* ep_ptr  = l_extendedprice.data;
        const int64_t* disc_ptr = l_discount.data;
        const int32_t* sd_ptr  = l_shipdate.data;

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend   = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (sd_ptr[i] <= DATE_THRESHOLD) continue;

                int32_t ok = ok_ptr[i];

                // Bloom filter: skip rows that can't possibly match
                if (!bloom.maybe_contains(ok)) continue;

                // Determine partition
                int part = (int)((uint32_t)((uint64_t)ok * 0x9E3779B97F4A7C15ULL >> 58));

                // Probe partition orders map
                OrderInfo* oi = orders_parts[part].find(ok);
                if (!oi) continue;

                int64_t rev = (ep_ptr[i] * (100LL - disc_ptr[i])) / 100LL;

                auto& lmap = local_agg[tid][part];
                AggEntry* ae = lmap.find(ok);
                if (ae) {
                    ae->revenue_scaled += rev;
                } else {
                    lmap.insert(ok, {rev, oi->o_orderdate, oi->o_shippriority});
                }
            }
        }
    }

    // ============================================================
    // Phase 4: Parallel aggregation merge by partition
    //          Each partition merged independently — no cross-partition contention
    // ============================================================
    // Final global agg: one CompactHashMap per partition (reuse orders_parts slots
    // or build a new per-partition map)
    // We merge into per-partition AggEntry maps, then flatten for sort
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> part_agg(NPARTS);

    {
        GENDB_PHASE("aggregation_merge");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int part = 0; part < NPARTS; part++) {
            // Estimate total groups for this partition
            size_t total = 0;
            for (int t = 0; t < num_threads; t++) {
                total += local_agg[t][part].size();
            }
            part_agg[part] = gendb::CompactHashMap<int32_t, AggEntry>(std::max(total + 1, (size_t)16));

            for (int t = 0; t < num_threads; t++) {
                for (auto [key, val] : local_agg[t][part]) {
                    AggEntry* ge = part_agg[part].find(key);
                    if (ge) {
                        ge->revenue_scaled += val.revenue_scaled;
                    } else {
                        part_agg[part].insert(key, val);
                    }
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Collect all result rows across partitions
        std::vector<ResultRow> all_rows;
        size_t total_groups = 0;
        for (int part = 0; part < NPARTS; part++) total_groups += part_agg[part].size();
        all_rows.reserve(total_groups);

        for (int part = 0; part < NPARTS; part++) {
            for (auto [key, val] : part_agg[part]) {
                all_rows.push_back({key, val.revenue_scaled, val.o_orderdate, val.o_shippriority});
            }
        }

        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        };

        int k = (int)std::min((size_t)10, all_rows.size());
        std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(), cmp);
        top10 = std::vector<ResultRow>(all_rows.begin(), all_rows.begin() + k);
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
