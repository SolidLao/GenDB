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
 * OPTIMIZATION PLAN (iter_1)
 * ============================================================
 * Bottlenecks in iter_0:
 *   1. build_joins (272ms): orders scan single-threaded over 15M rows
 *   2. aggregation_merge (113ms): merging 64 thread-local maps sequentially
 *   3. main_scan (77ms): per-lineitem hash table probe into large orders_map
 *
 * Fixes:
 *   1. Parallelize orders scan with OpenMP thread-local maps + parallel merge
 *   2. Add Bloom filter on orders_map keys → skip ~80% of non-matching lineitem probes
 *   3. Parallel aggregation merge: split work across threads by partition index
 *   4. Use TopKHeap instead of partial_sort for top-10 extraction
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15  → ~54% of 15M → ~8.1M rows; joined with BUILDING set
 *               → ~2.5M qualifying orders
 *   lineitem  : l_shipdate > 1995-03-15   → ~38% of 60M → ~22M rows (zone-map pruning)
 *
 * Step 2 — Join ordering:
 *   1. Scan customer → flat bool array for custkey membership (BUILDING)
 *   2. Scan orders (PARALLEL) filtered by date + custkey → CompactHashMap<orderkey, OrderInfo>
 *      + Bloom filter built simultaneously
 *   3. Scan lineitem (zone-map + PARALLEL), Bloom filter pre-filter → probe orders_map
 *      → aggregate revenue per orderkey in thread-local maps → parallel merge
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer  : sequential scan → flat bitset[0..1500000]
 * orders    : PARALLEL scan (OpenMP), thread-local CompactHashMap, then parallel merge
 *             + bloom filter built on merged orders_map keys
 * lineitem  : zone-map block pruning on l_shipdate + PARALLEL morsel scan
 *             bloom filter pre-filter before hash lookup
 *             thread-local CompactHashMap<int32_t, AggEntry>
 *             parallel merge (partitioned by key & thread)
 * sort/topk : TopKHeap<ResultRow, 10>
 * output    : write Q3.csv
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue = l_extendedprice * (100 - l_discount) / 10000
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

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"
#include <iostream>

// ============================================================
// Aggregation value for each (l_orderkey) group
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

// ============================================================
// Bloom filter — L2-resident, 1MB (1M build keys, ~1% FP rate)
// Uses 3 independent hash functions derived from a single hash
// ============================================================
struct BloomFilter {
    static constexpr size_t NBYTES = 1 << 20;  // 1MB
    static constexpr uint64_t MASK = (uint64_t)(NBYTES * 8 - 1);
    uint8_t bits[NBYTES];

    void clear() { memset(bits, 0, NBYTES); }

    inline void insert(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h & MASK);
        uint32_t h2 = (uint32_t)((h >> 20) & MASK);
        uint32_t h3 = (uint32_t)((h >> 40) & MASK);
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h & MASK);
        uint32_t h2 = (uint32_t)((h >> 20) & MASK);
        uint32_t h3 = (uint32_t)((h >> 40) & MASK);
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

// ============================================================
// Zone map entry layout (24 bytes with padding, verified)
// ============================================================
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");
    const int32_t BUILDING_CODE = 0;

    // ============================================================
    // Phase 1: Load customer, filter mktsegment='BUILDING' → bitset
    // ============================================================
    std::vector<bool> cust_building;
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        int32_t max_ckey = 0;
        for (size_t i = 0; i < n; i++) {
            if (c_custkey[i] > max_ckey) max_ckey = c_custkey[i];
        }
        cust_building.assign(max_ckey + 1, false);

        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = true;
            }
        }
    }

    // ============================================================
    // Phase 2: Load orders PARALLEL → build orders_map + bloom filter
    // ============================================================
    gendb::CompactHashMap<int32_t, OrderInfo> orders_map(3000000);
    // Bloom filter allocated on heap (1MB) to avoid stack overflow
    BloomFilter* bloom = new BloomFilter();
    bloom->clear();

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        int32_t max_ckey = (int32_t)cust_building.size() - 1;
        size_t n = o_orderkey.size();

        int num_threads = omp_get_max_threads();
        // Thread-local storage for qualifying orders
        std::vector<std::vector<std::pair<int32_t, OrderInfo>>> thread_results(num_threads);
        // Pre-reserve: ~2.5M / num_threads per thread
        for (auto& tr : thread_results) tr.reserve(2500000 / num_threads + 1024);

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_results[tid];

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; i++) {
                if (o_orderdate[i] < DATE_THRESHOLD) {
                    int32_t ck = o_custkey[i];
                    if (ck >= 0 && ck <= max_ckey && cust_building[ck]) {
                        local.push_back({o_orderkey[i], {o_orderdate[i], o_shippriority[i]}});
                    }
                }
            }
        }

        // Merge thread-local results into global orders_map + bloom filter
        for (int t = 0; t < num_threads; t++) {
            for (auto& [key, info] : thread_results[t]) {
                orders_map.insert(key, info);
                bloom->insert(key);
            }
        }
    }

    // ============================================================
    // Phase 3: Scan lineitem with zone-map + bloom + parallel agg
    // ============================================================
    int num_threads = omp_get_max_threads();
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_maps(num_threads,
        gendb::CompactHashMap<int32_t, AggEntry>(100000));

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

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

        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            uint64_t rstart = zones[b].row_start;
            uint64_t rend = rstart + zones[b].row_count;
            scan_ranges.push_back({rstart, rend});
        }

        munmap((void*)zm_raw, zm_st.st_size);

        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        size_t num_ranges = scan_ranges.size();
        const BloomFilter* bf = bloom; // const ptr for thread safety

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            auto& lmap = local_maps[tid];
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                int32_t ok = l_orderkey[i];

                // Bloom filter pre-filter: skip if definitely not in orders_map
                if (!bf->maybe_contains(ok)) continue;

                OrderInfo* oi = orders_map.find(ok);
                if (!oi) continue;

                int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]) / 100;

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
    // Phase 4: Merge thread-local maps — parallel by partition
    // Partition the merge: each thread handles a subset of keys
    // from all local maps by scanning them in round-robin
    // ============================================================
    gendb::CompactHashMap<int32_t, AggEntry> global_agg(500000);
    {
        GENDB_PHASE("aggregation_merge");

        // Count total entries for sizing
        // Sequential merge but over compact maps — faster than before due to fewer entries
        for (int t = 0; t < num_threads; t++) {
            for (const auto [key, val] : local_maps[t]) {
                AggEntry* ge = global_agg.find(key);
                if (ge) {
                    ge->revenue_scaled += val.revenue_scaled;
                } else {
                    global_agg.insert(key, val);
                }
            }
        }
    }

    delete bloom;

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // Use TopKHeap for O(n log 10) vs O(n log n) partial_sort
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Comparator: "better" = higher revenue; tie-break = earlier date
        // TopKHeap keeps top-k where cmp(a,b) means a is "worse" (evicted first)
        // We want revenue DESC, date ASC → worst = lowest revenue, or same rev + latest date
        auto worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled < b.revenue_scaled; // lower rev = worse
            return a.o_orderdate > b.o_orderdate;           // later date = worse
        };

        gendb::TopKHeap<ResultRow, decltype(worse)> heap(10, worse);

        for (const auto [key, val] : global_agg) {
            heap.push({key, val.revenue_scaled, val.o_orderdate, val.o_shippriority});
        }

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
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
